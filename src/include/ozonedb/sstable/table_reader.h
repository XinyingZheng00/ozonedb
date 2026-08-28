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
#include <unordered_map>
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
  // Every record in the table, for compaction. Caller co-owns the
  // Records; the cache is not involved.
  //
  // The data section is read in ranged reads of at most max_read_bytes
  // each, and the blocks are sliced out of every chunk and parsed. A
  // chunk is a run of consecutive index entries, so no block is ever
  // split across two reads; a block larger than the limit gets its own
  // read. With the default, one SSTable is one read. Reading one block
  // per storage call cost 16,384 GETs per 64 MiB file of 4 KiB blocks on
  // S3 (bench/PLAN-compaction-range-read.md).
  //
  // The index is validated before the first read: offsets ascending and
  // non-overlapping, every block inside the file. A bad index, a failed
  // read, or a block or record that does not parse returns kFailure (or
  // the storage status) and leaves `out` empty. Nothing here dereferences
  // a null iterator: the point-read path (blockReader) is unchanged.
  static constexpr size_t kDefaultScanReadBytes = 64u << 20;
  Status getAll(std::unordered_map<std::string, std::shared_ptr<Record>>& out,
                size_t max_read_bytes = kDefaultScanReadBytes);

  ~Table();

  void setCache(LRUCache* cache);
  Iterator* blockReader(Table* table, std::string const& index_value);
  // for testing
  // set rep_ filter reader to nullptr
  void setFilterReaderToNull();
  // for testing: pretend the file is `size` bytes long so getAll's index
  // validation can be exercised without a hand-built corrupt index.
  void setFileSizeForTesting(uint64_t size);

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
