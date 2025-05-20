// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sstable/filter_block.h"
#include "protobuf_serializer.h"
#include "sstable/filter_policy.h"
#include <cassert>

namespace ozonedb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
/*
If block size < kFilterBase, multiple blocks will share the same filter.
If block size > kFilterBase, each block will have its own filter and an empty filter will be added to the block.
*/
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(FilterPolicy const* policy)
    : policy_(policy) {}

void FilterBlockBuilder::startBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= block_.filters_size());
  while (filter_index > block_.filters_size()) {
    generateFilter();
  }
}

void FilterBlockBuilder::addKey(std::string const& key) {
  tmp_keys_.push_back(key);
}

FilterBlock& FilterBlockBuilder::finish() {
  if (!tmp_keys_.empty()) {
    generateFilter();
  }
  block_.set_lg_base(kFilterBaseLg);  // Save encoding parameter in result
  return block_;
}

void FilterBlockBuilder::generateFilter() {
  const size_t num_keys = tmp_keys_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    block_.add_filters("");
    return;
  }

  // Generate filter for current set of keys and append to result_.
  std::string filter;
  policy_->createFilter(tmp_keys_.data(), static_cast<int>(num_keys), &filter);
  block_.add_filters(filter);

  tmp_keys_.clear();
}

filterBlockReader::filterBlockReader(FilterPolicy const* policy, FilterBlock* block)
    : policy_(policy), block_(block) {
}

bool filterBlockReader::keyMayMatch(uint64_t block_offset, std::string const& key) {
  uint64_t index = block_offset >> block_->lg_base();
  if (index < block_->filters_size()) {
    std::string filter = block_->filters(index);
    return policy_->keyMayMatch(key, filter);
  }
  return true;  // Errors are treated as potential matches
}

Status readFilterBlock(FileStorage* storage,
                       std::string const& fileName,
                       BlockIdentifier const& identifier,
                       FilterBlock*& result) {
  // Read the block contents
  // See table_builder.cc for the code that built this structure.
  size_t n = identifier.length();
  std::vector<google::protobuf::Message*> block_data;
  Status status = storage->read(
      fileName, identifier.offset(), identifier.offset() + n, []() {
        return new FilterBlock();
      },
      block_data);
  if (status != Status::kSuccess) {
    return status;
  }
  result = static_cast<FilterBlock*>(block_data[0]);
  return status;
}

}  // namespace ozonedb
