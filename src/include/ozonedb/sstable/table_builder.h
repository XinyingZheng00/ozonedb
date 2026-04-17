// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_OZONEDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_OZONEDB_INCLUDE_TABLE_BUILDER_H_

#include "sstable/block_handler.h"
#include "storage.h"
#include <stdint.h>

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
namespace ozonedb {

class LRUCache;

class TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in file.
  TableBuilder(Storage* storage, std::string file);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~TableBuilder();

  // Optional: register an LRU cache to receive each data block's
  // records as a write-through cache entry. When set, every data
  // block published via flush() is also inserted into the cache so
  // readers immediately after compaction hit the cache instead of
  // paying a cold S3 GET. Safe to leave unset; tests and abandon()
  // paths then skip the write-through entirely.
  void setLRUCache(LRUCache* cache);

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void add(std::string const& key, Record const& record);

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void flush();

  // Return non-ok iff some error has been detected.
  Status status() const;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status finish();

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void abandon();

  // Number of calls to Add() so far.
  uint64_t numEntries() const;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t fileSize() const;

  // get the file name
  std::string fileName() const;

 private:
  struct Rep;
  Rep* rep_;

  bool ok() const { return status() == Status::kSuccess; }
  void writeBlock(google::protobuf::Message const& block, BlockIdentifier*& identifier);
};
}  // namespace ozonedb
#endif  // STORAGE_OZONEDB_INCLUDE_TABLE_BUILDER_H_
