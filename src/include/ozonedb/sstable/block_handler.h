// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_OZONEDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_OZONEDB_TABLE_BLOCK_BUILDER_H_

#include "comparator.h"
#include "protobuf/sstable.pb.h"
#include "storage/file_storage.h"
#include <string>
#include <vector>
#include <stdint.h>
namespace ozonedb {

class BlockBuilder {
 public:
  explicit BlockBuilder(int block_restart_interval);

  // Reset the contents as if the BlockBuilder was just constructed.
  void reset();

  // REQUIRES: Finish() has not been callled since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void add(std::string const& key, std::string const& value);

  // Finish building the block and return a Block that refers to the
  // block contents.  The returned Block will remain valid for the
  // lifetime of this builder or until Reset() is called.
  BlockData& finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t currentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return block_.entries_size() == 0;
  }

 private:
  Comparator* comparator_;
  int block_restart_interval;       // Restart interval
  BlockData block_;                 // Destination block
  int accumulated_size_;            // Current size of blockEntry_buffer_
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Number of entries emitted since restart
  bool finished_;

 public:  // Has Finish() been called?
  std::string last_key_;
};

Status readBlock(FileStorage* storage,
                 std::string const& fileName,
                 //  ReadOptions const& options,
                 BlockIdentifier const& identifier,
                 BlockData*& result);
}  // namespace ozonedb
#endif  // STORAGE_OZONEDB_TABLE_BLOCK_BUILDER_H_
