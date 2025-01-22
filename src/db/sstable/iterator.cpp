// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sstable/iterator.h"
namespace ozonedb {
BlockIter::BlockIter(Comparator* comparator,
                     google::protobuf::RepeatedPtrField<BlockEntry> const& entries,
                     google::protobuf::RepeatedField<uint32_t> const& restarts,
                     uint32_t num_restarts)
    : comparator_(comparator),
      entries_(entries),
      restarts_(restarts),
      num_restarts_(num_restarts),
      current_entry_index(0),
      restart_index_(0),
      status_() {
  assert(num_restarts_ > 0);
}

uint32_t BlockIter::nextEntryIndex() const {
  return current_entry_index + 1;
}

uint32_t BlockIter::getRestartPoint(uint32_t index) {
  assert(index < num_restarts_);
  return restarts_[index];
}

void BlockIter::seekToRestartPoint(uint32_t index) {
  key_.clear();
  assert(index < num_restarts_);
  restart_index_ = index;
  current_entry_index = getRestartPoint(index);
}

bool BlockIter::valid() const {
  return (current_entry_index != -1) && (restart_index_ != -1);
}

Status const& BlockIter::status() const {
  return status_;
}

std::string BlockIter::key() const {
  assert(valid());
  return key_;
}

std::string const& BlockIter::value() const {
  assert(valid());
  return *value_ptr_;
}

void BlockIter::next() {
  assert(valid());
  current_entry_index++;
  parseCurrentKey();
}

void BlockIter::prev() {
  assert(valid());

  // Scan backwards to a restart point before current_
  const uint32_t original = current_entry_index;
  while (getRestartPoint(restart_index_) >= original) {
    if (restart_index_ == 0) {
      current_entry_index = -1;
      restart_index_ = -1;
      return;
    }
    restart_index_--;
  }
  seekToRestartPoint(restart_index_);
  do {
    current_entry_index++;
  } while (parseCurrentKey() && nextEntryIndex() < original);
}

void BlockIter::seek(std::string const& target) {
  uint32_t left = 0;
  uint32_t right = num_restarts_ - 1;
  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = getRestartPoint(mid);
    uint32_t shared;
    uint32_t non_shared;

    BlockEntry entry = entries_[region_offset];
    shared = entry.shared_bytes();
    non_shared = entry.unshared_bytes();
    char const* key_ptr = entry.key_delta().data();
    if (key_ptr == NULL || (shared != 0)) {  // restart point key must have shared
      corruptionError();
      return;
    }
    std::string mid_key(key_ptr, non_shared);
    if (comparator_->compare(mid_key, target) < 0) {
      // Key at "mid" is smaller than "target".  Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else {
      // Key at "mid" is >= "target".  Therefore all blocks at or
      // after "mid" are uninteresting.
      // if key at mid is equal to target, the linear search will find it, todo: change it.
      right = mid - 1;
    }
  }

  // linear search from left point
  seekToRestartPoint(left);
  while (true) {
    if (!parseCurrentKey()) {
      return;
    }
    if (comparator_->compare(key_, target) >= 0) {
      return;
    }
    current_entry_index++;
  }
}

void BlockIter::seekToFirst() {
  seekToRestartPoint(0);
  parseCurrentKey();
}

void BlockIter::seekToLast() {
  seekToRestartPoint(num_restarts_ - 1);
  while (parseCurrentKey() && nextEntryIndex() < entries_.size()) {
    current_entry_index++;
  }
}

void BlockIter::corruptionError() {
  current_entry_index = -1;
  restart_index_ = -1;
  status_ = Status::kFailure;
  // std::cout << "Corruption Error" << std::endl;
  key_.clear();
  value_ptr_ = nullptr;
}

bool BlockIter::parseCurrentKey() {
  if (current_entry_index >= entries_.size()) {
    corruptionError();
    return false;
  }

  BlockEntry const& entry = entries_.Get(current_entry_index);
  const uint32_t shared = entry.shared_bytes();
  const uint32_t non_shared = entry.unshared_bytes();
  std::string const& delta = entry.key_delta();
  char const* p = delta.data();

  if (p == nullptr || key_.size() < shared) {
    corruptionError();
    return false;
  } else {
    key_.resize(shared);
    key_.append(p, non_shared);
    value_ptr_ = &entry.value();
    while (restart_index_ + 1 < num_restarts_ && getRestartPoint(restart_index_ + 1) < current_entry_index) {
      ++restart_index_;
    }
    return true;
  }
}

Iterator* newIterator(BlockData* block_data, Comparator* cmp) {
  Iterator* iter = new BlockIter(newBytewiseComparator(), block_data->entries(), block_data->restarts(), block_data->num_restarts());
  iter->block_data_ = block_data;
  return iter;
}
}  // namespace ozonedb