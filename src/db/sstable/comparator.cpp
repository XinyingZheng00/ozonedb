// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sstable/comparator.h"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
namespace ozonedb {
Comparator::~Comparator() = default;

class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  char const* name() const override {
    return "leveldb.BytewiseComparator";
  }

  int compare(std::string const& a, std::string const& b) const override {
    return a.compare(b);
  }

  void findShortestSeparator(
      std::string* start,
      std::string const& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      auto diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(compare(*start, limit) < 0);
      }
    }
  }

  void findShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};

static Comparator* bytewise = nullptr;

// static void InitModule() {
//   bytewise = new BytewiseComparatorImpl;
// }

Comparator* newBytewiseComparator() {
  return new BytewiseComparatorImpl;
}

void deleteBytewiseComparator() {
  if (bytewise == nullptr) return;
  delete bytewise;
  bytewise = nullptr;
}

}  // namespace ozonedb