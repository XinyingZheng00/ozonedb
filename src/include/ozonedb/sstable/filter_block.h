// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_OZONEDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_OZONEDB_TABLE_FILTER_BLOCK_H_

#include "protobuf/sstable.pb.h"
#include "storage/file_storage.h"
#include <string>
#include <vector>

namespace ozonedb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(FilterPolicy const*);
  void startBlock(uint64_t block_offset);
  void addKey(std::string const& key);
  FilterBlock& finish();

 private:
  void generateFilter();

  FilterPolicy const* policy_;
  std::vector<std::string> tmp_keys_;  // policy_->CreateFilter() argument
  FilterBlock block_;
};

class filterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  filterBlockReader(FilterPolicy const* policy, FilterBlock* block);
  bool keyMayMatch(uint64_t block_offset, std::string const& key);
  ~filterBlockReader() { delete block_; };

 private:
  FilterPolicy const* policy_;
  FilterBlock* block_;
};

Status readFilterBlock(FileStorage* storage, std::string const& fileName,
                       BlockIdentifier const& identifier,
                       FilterBlock*& result);

}  // namespace ozonedb

#endif  // STORAGE_OZONEDB_TABLE_FILTER_BLOCK_H_
