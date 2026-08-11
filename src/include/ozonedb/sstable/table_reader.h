// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_OZONEDB_INCLUDE_TABLE_H_
#define STORAGE_OZONEDB_INCLUDE_TABLE_H_

#include "comparator.h"
#include "protobuf/sstable.pb.h"
#include "sstable/iterator.h"
#include "storage.h"
#include <memory>
#include <stdint.h>
namespace ozonedb {
class LRUCache;
// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to NULL and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status open(Storage* storage,
                     std::string const& fileName,
                     Table*& table);
  Status get(std::string const& key, std::shared_ptr<Record>& record);
  // Status getBlockPosition(std::string const& key, std::string& index_value);
  // Returns freshly-allocated shared_ptr<Record>s for every key in the
  // table. Caller (compaction) co-owns; cache is not involved.
  std::unordered_map<std::string, std::shared_ptr<Record>> getAll();

  ~Table();

  void setCache(LRUCache* cache);
  Iterator* blockReader(Table* table, std::string const& index_value);
  // for testing
  // set rep_ filter reader to nullptr
  void setFilterReaderToNull();

 private:
  struct Rep;
  Rep* rep_ = nullptr;

  explicit Table(Rep* rep) : rep_(rep) {}
  void readMeta(FooterBlock const& footer);
  void readFilter(std::string const& filter_handle_value);
  void readFileFilter(std::string const& filter_handle_value);
};
}  // namespace ozonedb
#endif  // STORAGE_OZONEDB_INCLUDE_TABLE_H_
