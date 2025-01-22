// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sstable/table_builder.h"
#include "protobuf_serializer.h"
#include "sstable/block_handler.h"
#include "sstable/comparator.h"
#include "sstable/filter_block.h"
#include "sstable/filter_policy.h"
#include <cassert>
namespace ozonedb {
#define BLOCK_SIZE 4096  // approximate size of a block

struct TableBuilder::Rep {
  Storage* storage;
  std::string fileName;
  size_t offset = 0;
  Status status = Status::kSuccess;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries = 0;
  bool closed = false;  // Either Finish() or Abandon() has been called.
  Comparator const* comparator;
  FilterPolicy const* filter_policy = nullptr;
  FilterBlockBuilder* filter_block = nullptr;
  FilterBlockBuilder* filter_block_for_file = nullptr;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry = false;
  BlockIdentifier* pending_index_identifier = nullptr;  // Handle to add to index block

  Rep(Storage* storage, std::string fileName) : storage(storage),
                                                fileName(std::move(fileName)),
                                                data_block(8),                           // default to 8, can be changed later
                                                index_block(1),                          // cannot be changed
                                                comparator(newBytewiseComparator()),     // default for now
                                                filter_policy(newBloomFilterPolicy(10))  // default for now
                                                {};
};

TableBuilder::TableBuilder(Storage* storage, std::string file)
    : rep_(new Rep(storage, std::move(file))) {
  if (rep_->filter_policy != nullptr) {
    rep_->filter_block = new FilterBlockBuilder(rep_->filter_policy);
    rep_->filter_block_for_file = new FilterBlockBuilder(rep_->filter_policy);
    rep_->filter_block->startBlock(0);
    rep_->filter_block_for_file->startBlock(0);
  }
}
TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_->filter_block_for_file;
  delete rep_->filter_policy;
  if (rep_->comparator != nullptr) {
    deleteBytewiseComparator();
  }
  delete rep_;
};

void TableBuilder::add(std::string const& key, Record const& record) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->comparator->compare(key, r->last_key) > 0);
  }
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    r->comparator->findShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    assert(r->pending_index_identifier->has_offset());
    assert(r->pending_index_identifier->has_length());
    r->index_block.add(r->last_key, r->pending_index_identifier->SerializeAsString());
    delete r->pending_index_identifier;
    r->pending_index_identifier = nullptr;
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    r->filter_block->addKey(key);
  }
  if (r->filter_block_for_file != nullptr) {
    r->filter_block_for_file->addKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  std::string value = record.SerializeAsString();
  r->data_block.add(key, value);

  const size_t estimated_block_size = r->data_block.currentSizeEstimate();
  if (estimated_block_size >= BLOCK_SIZE) {
    flush();
  }
}

void TableBuilder::flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  writeBlock(r->data_block.finish(), r->pending_index_identifier);
  r->data_block.reset();
  if (ok()) {
    r->pending_index_entry = true;
    // r->status = r->storage->flush(r->fileName);
  }
  if (r->filter_block != nullptr) {
    r->filter_block->startBlock(r->offset);
  }
}

void TableBuilder::writeBlock(google::protobuf::Message const& block, BlockIdentifier*& identifier) {
  assert(ok());
  Rep* r = rep_;
  int block_data_vector_size;
  unsigned char* block_data_vector = protobuf::serializeMessage(block, block_data_vector_size);
  // write block data
  identifier = new BlockIdentifier();
  identifier->set_offset(r->offset);
  identifier->set_length(block_data_vector_size);
  r->status = r->storage->appendNoFlush(r->fileName, block_data_vector, block_data_vector_size);  // change
  delete[] block_data_vector;
  block_data_vector = nullptr;
  if (ok()) {
    r->offset += block_data_vector_size;
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

Status TableBuilder::finish() {
  Rep* r = rep_;
  flush();
  assert(!r->closed);
  r->closed = true;

  BlockIdentifier* filter_block_identifier = nullptr;
  BlockIdentifier* filter_block_for_file_identifier = nullptr;
  BlockIdentifier* metaindex_block_identifier = nullptr;
  BlockIdentifier* index_block_identifier = nullptr;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    writeBlock(r->filter_block->finish(), filter_block_identifier);
  }

  if (ok() && r->filter_block_for_file != nullptr) {
    writeBlock(r->filter_block_for_file->finish(), filter_block_for_file_identifier);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(8);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->filter_policy->name());
      meta_index_block.add(key, filter_block_identifier->SerializeAsString());
    }
    if (r->filter_block_for_file != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filterFile.";
      key.append(r->filter_policy->name());
      meta_index_block.add(key, filter_block_for_file_identifier->SerializeAsString());
    }
    writeBlock(meta_index_block.finish(), metaindex_block_identifier);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->comparator->findShortSuccessor(&r->last_key);
      r->index_block.add(r->last_key, r->pending_index_identifier->SerializeAsString());
      delete r->pending_index_identifier;
      r->pending_index_entry = false;
    }
    writeBlock(r->index_block.finish(), index_block_identifier);
  }

  // Write footer
  if (ok()) {
    FooterBlock footer;
    footer.set_allocated_metaindex_identifier(metaindex_block_identifier);
    footer.set_allocated_index_identifier(index_block_identifier);
    footer.set_magic(0xdb4775248b80fb57);
    int footer_encoding_size;
    auto* footer_encoding = protobuf::serializeMessage(footer, footer_encoding_size);
    r->status = r->storage->appendNoFlush(r->fileName, footer_encoding, footer_encoding_size);
    delete[] footer_encoding;
    footer_encoding = nullptr;
    if (ok()) {
      r->offset += footer_encoding_size;
    }
  }
  r->storage->flush(r->fileName); //flush at last
  // r->storage->seal(r->fileName);
  delete filter_block_identifier;
  delete filter_block_for_file_identifier;
  // delete metaindex_block_identifier;
  // delete index_block_identifier; this is already deleted by the footer
  return r->status;
}

void TableBuilder::abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::numEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::fileSize() const {
  return rep_->offset + rep_->data_block.currentSizeEstimate();
}

std::string TableBuilder::fileName() const {
  return rep_->fileName;
}

}  // namespace ozonedb