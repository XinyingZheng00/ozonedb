// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef STORAGE_OZONEDB_INCLUDE_ITERATOR_H_
#define STORAGE_OZONEDB_INCLUDE_ITERATOR_H_

#include "comparator.h"
#include "protobuf/sstable.pb.h"
#include "storage/storage.h"
#include <iostream>
namespace ozonedb {

class Iterator {
 public:
  BlockData* block_data_ = nullptr;  // this is just used to delete the block_data
  Iterator() = default;
  virtual ~Iterator() = default;

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void seekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void seekToLast() = 0;

  // Position at the first key in the source that at or past target
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void seek(std::string const& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual std::string key() const = 0;

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: !AtEnd() && !AtStart()
  virtual std::string const& value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status const& status() const = 0;
};

class BlockIter : public Iterator {
 private:
  Comparator* comparator_;
  google::protobuf::RepeatedPtrField<BlockEntry> const& entries_;  // Direct reference to the repeated BlockEntry
  google::protobuf::RepeatedField<uint32_t> const& restarts_;
  const uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  uint32_t current_entry_index;  // Offset in entries_ of current entry. -1 if !Valid
  uint32_t restart_index_;       // Index of restart block in which current_ falls, -1 if !Valid
  std::string key_;
  std::string const* value_ptr_;
  Status status_;

  uint32_t nextEntryIndex() const;
  uint32_t getRestartPoint(uint32_t index);
  void seekToRestartPoint(uint32_t index);
  void corruptionError();
  bool parseCurrentKey();

 public:
  BlockIter(Comparator* comparator,
            google::protobuf::RepeatedPtrField<BlockEntry> const& entries_,
            google::protobuf::RepeatedField<uint32_t> const& restarts_,
            uint32_t num_restarts);
  ~BlockIter() { delete block_data_; };

  virtual bool valid() const override;
  virtual Status const& status() const override;
  virtual std::string key() const override;
  virtual std::string const& value() const override;
  virtual void next() override;
  virtual void prev() override;
  virtual void seek(std::string const& target) override;
  virtual void seekToFirst() override;
  virtual void seekToLast() override;
};

Iterator* newIterator(BlockData* block_data, ozonedb::Comparator* cmp);
}  // namespace ozonedb
#endif  // STORAGE_OZONEDB_INCLUDE_ITERATOR_H_
