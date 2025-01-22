// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sstable/block_handler.h"
#include "protobuf_serializer.h"
#include <algorithm>
#include <cassert>
namespace ozonedb {
BlockBuilder::BlockBuilder(int block_restart_interval)
    : comparator_(newBytewiseComparator()),  // default for now
      block_restart_interval(block_restart_interval),
      accumulated_size_(0),
      counter_(0),
      finished_(false) {
  assert(block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::reset() {
  block_.Clear();
  accumulated_size_ = 0;
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::currentSizeEstimate() const {
  return (accumulated_size_ +                   //
          restarts_.size() * sizeof(int32_t) +  // Restart array
          sizeof(int32_t));                     // Restart array length
}

BlockData& BlockBuilder::finish() {
  // Append restart array
  for (auto restart : restarts_) {
    block_.add_restarts(restart);
  }
  block_.set_num_restarts(restarts_.size());
  finished_ = true;
  return block_;
}

void BlockBuilder::add(std::string const& key, std::string const& value) {
  std::string last_key_piece = last_key_;
  assert(!finished_);
  assert(counter_ <= block_restart_interval);
  assert(block_.entries_size() == 0  // No values yet?
         || comparator_->compare(key, last_key_) > 0);

  size_t shared = 0;
  if (counter_ < block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    char const* a = last_key_piece.data();
    char const* b = key.data();
    while ((shared < min_length) && (a[shared] == b[shared])) {
      ++shared;
    }
  } else {
    // Restart compression
    restarts_.push_back(block_.entries_size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  BlockEntry* block_entry = block_.add_entries();
  block_entry->set_shared_bytes(shared);
  block_entry->set_unshared_bytes(non_shared);

  // Add string delta to buffer_ followed by value
  block_entry->set_key_delta(key.data() + shared, non_shared);
  block_entry->set_value(value.data(), value.size());

  accumulated_size_ += block_entry->ByteSizeLong();

  // Update state
  // shrink last_key_ to the shared length
  last_key_ = last_key_.substr(0, shared);
  // last_key_.shrink(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(last_key_ == key);
  counter_++;
}

Status readBlock(Storage* storage,
                 std::string const& fileName,
                 BlockIdentifier const& identifier,
                 BlockData*& result) {
  // Read the block contents
  // See table_builder.cc for the code that built this structure.
  size_t n = identifier.length();
  unsigned char* block_data_contents = nullptr;
  Status status = storage->read(fileName, block_data_contents, identifier.offset(), n);
  if (status != Status::kSuccess) {
    return status;
  }
  std::vector<google::protobuf::Message*> block_data;
  status = protobuf::deserializeMessages(block_data_contents, n, block_data, []() {
    return new BlockData();
  });
  delete[] block_data_contents;
  block_data_contents = nullptr;
  if (status != Status::kSuccess) {
    return status;
  }
  result = static_cast<BlockData*>(block_data[0]);
  return status;
}
}  // namespace ozonedb