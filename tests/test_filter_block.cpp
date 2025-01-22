// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "gtest/gtest.h"
#include "sstable/filter_block.h"
#include "sstable/filter_policy.h"

using namespace ozonedb;

inline void EncodeFixed32(char* dst, uint32_t value) {
  auto* const buffer = reinterpret_cast<uint8_t*>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

// For testing: emit an array with one hash value per key
class TestHashFilter : public FilterPolicy {
 public:
  char const* name() const override { return "TestHashFilter"; }

  void createFilter(std::string const* keys, int n, std::string* dst) const override {
    for (int i = 0; i < n; i++) {
      uint32_t h = Hash(keys[i].data(), keys[i].size(), 1);
      PutFixed32(dst, h);
    }
  }

  bool keyMayMatch(std::string const& key, std::string const& filter) const override {
    uint32_t h = Hash(key.data(), key.size(), 1);
    for (size_t i = 0; i + 4 <= filter.size(); i += 4) {
      if (h == DecodeFixed32(filter.data() + i)) {
        return true;
      }
    }
    return false;
  }
};

class FilterBlockTest : public testing::Test {
 public:
  TestHashFilter policy_;
};

TEST_F(FilterBlockTest, EmptyBuilder) {
  FilterBlockBuilder builder(&policy_);
  FilterBlock* block = new FilterBlock(builder.finish());
  ASSERT_EQ(11, block->lg_base());
  filterBlockReader reader(&policy_, block);
  ASSERT_TRUE(reader.keyMayMatch(0, "foo"));
  ASSERT_TRUE(reader.keyMayMatch(100000, "foo"));
}

TEST_F(FilterBlockTest, SingleChunk) {
  FilterBlockBuilder builder(&policy_);
  builder.startBlock(100);
  builder.addKey("foo");
  builder.addKey("bar");
  builder.addKey("box");
  builder.startBlock(200);
  builder.addKey("box");
  builder.startBlock(300);
  builder.addKey("hello");
  FilterBlock* block = new FilterBlock(builder.finish());
  filterBlockReader reader(&policy_, block);
  ASSERT_TRUE(reader.keyMayMatch(100, "foo"));
  ASSERT_TRUE(reader.keyMayMatch(100, "bar"));
  ASSERT_TRUE(reader.keyMayMatch(100, "box"));
  ASSERT_TRUE(reader.keyMayMatch(100, "hello"));
  ASSERT_TRUE(reader.keyMayMatch(100, "foo"));
  ASSERT_TRUE(!reader.keyMayMatch(100, "missing"));
  ASSERT_TRUE(!reader.keyMayMatch(100, "other"));
}

TEST_F(FilterBlockTest, MultiChunk) {
  FilterBlockBuilder builder(&policy_);

  // First filter
  builder.startBlock(0);
  builder.addKey("foo");
  builder.startBlock(2000);
  builder.addKey("bar");

  // Second filter
  builder.startBlock(3100);
  builder.addKey("box");

  // Third filter is empty

  // Last filter
  builder.startBlock(9000);
  builder.addKey("box");
  builder.addKey("hello");
  FilterBlock* block = new FilterBlock(builder.finish());
  filterBlockReader reader(&policy_, block);

  // Check first filter
  ASSERT_TRUE(reader.keyMayMatch(0, "foo"));
  ASSERT_TRUE(reader.keyMayMatch(2000, "bar"));
  ASSERT_TRUE(!reader.keyMayMatch(0, "box"));
  ASSERT_TRUE(!reader.keyMayMatch(0, "hello"));

  // Check second filter
  ASSERT_TRUE(reader.keyMayMatch(3100, "box"));
  ASSERT_TRUE(!reader.keyMayMatch(3100, "foo"));
  ASSERT_TRUE(!reader.keyMayMatch(3100, "bar"));
  ASSERT_TRUE(!reader.keyMayMatch(3100, "hello"));

  // Check third filter (empty)
  ASSERT_TRUE(!reader.keyMayMatch(4100, "foo"));
  ASSERT_TRUE(!reader.keyMayMatch(4100, "bar"));
  ASSERT_TRUE(!reader.keyMayMatch(4100, "box"));
  ASSERT_TRUE(!reader.keyMayMatch(4100, "hello"));

  // Check last filter
  ASSERT_TRUE(reader.keyMayMatch(9000, "box"));
  ASSERT_TRUE(reader.keyMayMatch(9000, "hello"));
  ASSERT_TRUE(!reader.keyMayMatch(9000, "foo"));
  ASSERT_TRUE(!reader.keyMayMatch(9000, "bar"));
}

TEST_F(FilterBlockTest, MultiChunk1) {
  // when block size > filter base size
  FilterBlockBuilder builder(&policy_);

  // First filter
  builder.startBlock(0);
  for (int i = 0; i < 1024; i++) {
    builder.addKey("fooo");
  }

  // Second filter
  builder.startBlock(4096);
  builder.addKey("box");

  FilterBlock* block = new FilterBlock(builder.finish());
  filterBlockReader reader(&policy_, block);

  // Check first filter
  ASSERT_TRUE(reader.keyMayMatch(0, "fooo"));
  ASSERT_TRUE(reader.keyMayMatch(4096, "box"));
}
