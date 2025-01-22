// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sstable/table_reader.h"
#include "cache.h"
#include "protobuf/sstable.pb.h"
#include "protobuf_serializer.h"
#include "sstable/block_handler.h"
#include "sstable/filter_block.h"
#include "sstable/filter_policy.h"
namespace ozonedb {
#define FOOTER_BLOCK_SIZE 50
struct Table::Rep {
  Status status = Status::kSuccess;
  Storage* storage = nullptr;
  LRUCache* lru_cache = nullptr;
  Comparator* comparator = nullptr;
  std::string fileName;
  Iterator* index_iter = nullptr;
  FilterPolicy const* filter_policy = nullptr;
  filterBlockReader* filter_block_reader = nullptr;
  filterBlockReader* filter_block_for_file_reader = nullptr;
};

void Table::setCache(LRUCache* cache) {
  rep_->lru_cache = cache;
}

Status Table::open(Storage* storage,
                   std::string const& fileName,
                   Table*& table) {
  // read the footer
  size_t size = storage->size(fileName);//tobe changed.
  unsigned char* footer_vector = nullptr;
  Status s = storage->read(fileName, footer_vector, size - FOOTER_BLOCK_SIZE, FOOTER_BLOCK_SIZE);
  if (s != Status::kSuccess) return s;
  // Deserialize the footer
  std::vector<google::protobuf::Message*> footer;
  s = protobuf::deserializeMessages(footer_vector, FOOTER_BLOCK_SIZE, footer, []() { return new FooterBlock(); });
  if (s != Status::kSuccess) return s;
  delete[] footer_vector;
  footer_vector = nullptr;
  auto* footer_block = static_cast<FooterBlock*>(footer[0]);
  // read index block based on the index identifier in the footer
  BlockData* index_block;
  s = readBlock(storage, fileName, footer_block->index_identifier(), index_block);
  if (s == Status::kSuccess) {
    Rep* rep = new Table::Rep;
    rep->storage = storage;
    rep->fileName = fileName;
    rep->comparator = newBytewiseComparator();  // set to default for now
    rep->index_iter = newIterator(index_block, rep->comparator);
    rep->filter_policy = newBloomFilterPolicy(10);
    table = new Table(rep);
    table->readMeta(*footer_block);
    delete footer_block;
  }

  return s;
}

void Table::readMeta(FooterBlock const& footer) {
  if (rep_->filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  BlockData* meta_index_block;
  Status s = readBlock(rep_->storage, rep_->fileName, footer.metaindex_identifier(), meta_index_block);
  if (s != Status::kSuccess) {
    // do not propagate the error, since meta index block is optional
    return;
  }
  Iterator* miiter = newIterator(meta_index_block, rep_->comparator);
  std::string key = "filter.";
  key.append(rep_->filter_policy->name());
  miiter->seek(key);
  if (miiter->valid() && miiter->key() == std::string(key)) {
    readFilter(miiter->value());
  }
  key = "filterFile.";
  key.append(rep_->filter_policy->name());
  miiter->seek(key);
  if (miiter->valid() && miiter->key() == std::string(key)) {
    readFileFilter(miiter->value());
  }
  delete miiter;
}

void Table::readFilter(std::string const& filter_handle_value) {
  BlockIdentifier identifier;
  bool s = identifier.ParseFromString(filter_handle_value);
  if (s) {
    FilterBlock* filter_block;
    readFilterBlock(rep_->storage, rep_->fileName, identifier, filter_block);
    rep_->filter_block_reader = new filterBlockReader(rep_->filter_policy, filter_block);
  }
}

void Table::readFileFilter(std::string const& filter_handle_value) {
  BlockIdentifier identifier;
  bool s = identifier.ParseFromString(filter_handle_value);
  if (s) {
    FilterBlock* filter_block;
    readFilterBlock(rep_->storage, rep_->fileName, identifier, filter_block);
    rep_->filter_block_for_file_reader = new filterBlockReader(rep_->filter_policy, filter_block);
  }
}

Table::~Table() {
  delete rep_->index_iter;
  delete rep_->filter_policy;
  delete rep_->filter_block_reader;
  delete rep_->filter_block_for_file_reader;
  deleteBytewiseComparator();
  delete rep_;
}

void Table::setFilterReaderToNull() {
  rep_->filter_block_reader = nullptr;
  rep_->filter_block_for_file_reader = nullptr;
}

// Convert an index iterator value (i.e., an encoded BlockIdentifier)
// into an iterator over the contents of the corresponding block.
Iterator* Table::blockReader(Table* table,
                             std::string const& index_value) {
  BlockIdentifier identiifer;
  bool s = identiifer.ParseFromString(index_value);

  BlockData* block_data = nullptr;
  Iterator* iter = nullptr;
  if (s) {
    Status status = readBlock(table->rep_->storage, table->rep_->fileName, identiifer, block_data);
    if (status == Status::kSuccess) {
      if (block_data != NULL) {
        iter = newIterator(block_data, table->rep_->comparator);
      }
    }
  }
  return iter;
}

Status Table::get(std::string const& key, Record*& record) {
  // check file filter first
  if (rep_->filter_block_for_file_reader != nullptr && !rep_->filter_block_for_file_reader->keyMayMatch(0, key)) {
    return Status::kNotFound;
  }
  Status s;
  this->rep_->index_iter->seek(key);
  if (this->rep_->index_iter->valid()) {
    std::string identifier_value = this->rep_->index_iter->value();
    BlockIdentifier identifier;
    bool s = identifier.ParseFromString(identifier_value);
    if (rep_->filter_block_reader != nullptr && !rep_->filter_block_reader->keyMayMatch(identifier.offset(), key)) {
      return Status::kNotFound;
    }
    Status status;
    if (s) {
      bool read_more = true;
      this->rep_->lru_cache->needReadBlock(rep_->fileName, read_more, identifier_value);
      if (read_more) {
        this->rep_->lru_cache->readDataBlocks(rep_->fileName, identifier_value);
      }
      this->rep_->lru_cache->get(rep_->fileName, key, record, identifier_value);
      if (record != nullptr) {
        return Status::kSuccess;
      }
    }
  }
  return Status::kNotFound;
}

/*
Status Table::getBlockPosition(std::string const& key, std::string& index_value) {
  // check file filter first
  if (rep_->filter_block_for_file_reader != nullptr && !rep_->filter_block_for_file_reader->keyMayMatch(0, key)) {
    return Status::kNotFound;
  }
  Status s;
  this->rep_->index_iter->seek(key);
  if (this->rep_->index_iter->valid()) {
    index_value = this->rep_->index_iter->value();
    BlockIdentifier identifier;
    bool s = identifier.ParseFromString(index_value);
    if (rep_->filter_block_reader != nullptr && !rep_->filter_block_reader->keyMayMatch(identifier.offset(), key)) {
      return Status::kNotFound;
    }
  }
  return Status::kSuccess;
}
*/

std::unordered_map<std::string, Record*> Table::getAll() {
  std::unordered_map<std::string, Record*> result;
  assert(this->rep_ != nullptr);
  assert(this->rep_->index_iter != nullptr);
  this->rep_->index_iter->seekToFirst();
  Iterator* block_iter = nullptr;
  while (this->rep_->index_iter->valid()) {
    block_iter = blockReader(this, this->rep_->index_iter->value());
    block_iter->seekToFirst();
    while (block_iter->valid()) {
      std::string const& value = block_iter->value();
      auto* record = new Record();
      (*record).ParseFromString(value);
      result[block_iter->key()] = record;
      block_iter->next();
    }
    this->rep_->index_iter->next();
    delete block_iter;
    block_iter = nullptr;
  }
  return result;
}

}  // namespace ozonedb