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
#include <vector>
namespace ozonedb {
#define FOOTER_BLOCK_SIZE 50
struct Table::Rep {
  Status status = Status::kSuccess;
  Storage* storage = nullptr;
  LRUCache* lru_cache = nullptr;
  Comparator* comparator = nullptr;
  std::string fileName;
  uint64_t file_size = 0;  // as reported by storage->size() at open; bounds getAll's reads
  // The index block. index_iter owns it (BlockIter deletes its
  // BlockData); this alias lets scanBlocks copy it into a private
  // iterator, because index_iter is shared with every reader of the
  // Table and a seek from one thread would corrupt a scan on another.
  BlockData* index_block = nullptr;
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
  // A missing, empty or truncated object would underflow the offset
  // below and read from an absurd address.
  if (size < FOOTER_BLOCK_SIZE) return Status::kFailure;
  unsigned char* footer_vector = nullptr;
  Status s = storage->read(fileName, footer_vector, size - FOOTER_BLOCK_SIZE, FOOTER_BLOCK_SIZE);
  if (s != Status::kSuccess) return s;
  // Deserialize the footer
  std::vector<google::protobuf::Message*> footer;
  s = protobuf::deserializeMessages(footer_vector, FOOTER_BLOCK_SIZE, footer, []() { return new FooterBlock(); });
  delete[] footer_vector;
  footer_vector = nullptr;
  if (s != Status::kSuccess) return s;
  if (footer.empty()) return Status::kFailure;
  auto* footer_block = static_cast<FooterBlock*>(footer[0]);
  // read index block based on the index identifier in the footer
  BlockData* index_block = nullptr;
  s = readBlock(storage, fileName, footer_block->index_identifier(), index_block);
  if (s != Status::kSuccess || index_block == nullptr) {
    // The footer owns the index identifier; free it on the failure path
    // too, or every failed open leaks one FooterBlock.
    delete footer_block;
    return s == Status::kSuccess ? Status::kFailure : s;
  }
  {
    Rep* rep = new Table::Rep;
    rep->storage = storage;
    rep->fileName = fileName;
    rep->file_size = size;
    rep->comparator = newBytewiseComparator();  // set to default for now
    rep->index_block = index_block;
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

Status Table::get(std::string const& key, std::shared_ptr<Record>& record) {
  // index_iter is built from the index block in Table::open. A Table
  // that reached a reader without one cannot answer anything.
  if (rep_ == nullptr || rep_->index_iter == nullptr || rep_->lru_cache == nullptr) {
    return Status::kNotFound;
  }
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
        this->rep_->lru_cache->readDataBlocks(rep_->fileName, identifier_value, this);
      }
      this->rep_->lru_cache->get(rep_->fileName, key, record, identifier_value);
      if (record) {
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

void Table::setFileSizeForTesting(uint64_t size) {
  rep_->file_size = size;
}

Status Table::scanBlocks(size_t max_read_bytes,
                         std::function<Status(std::string const&, Iterator*)> const& visit) {
  if (rep_ == nullptr || rep_->index_block == nullptr || rep_->storage == nullptr) {
    return Status::kFailure;
  }

  // 1. Collect the block identifiers in index order and validate them
  //    before touching storage: ascending, non-overlapping, inside the
  //    file. TableBuilder::writeBlock lays data blocks out contiguously,
  //    so a violation means a corrupt or foreign index, not a layout we
  //    should try to serve. The index entry's raw value is kept as well:
  //    it is the cache key Table::get and TableBuilder::flush use.
  //    A private iterator over a copy of the index block, because
  //    rep_->index_iter is shared with concurrent Table::get callers.
  std::vector<BlockIdentifier> blocks;
  std::vector<std::string> index_values;
  {
    Iterator* index = newIterator(new BlockData(*rep_->index_block), rep_->comparator);
    uint64_t prev_end = 0;
    for (index->seekToFirst(); index->valid(); index->next()) {
      BlockIdentifier id;
      if (!id.ParseFromString(index->value()) || id.length() == 0 || id.offset() < prev_end ||
          id.offset() + id.length() > rep_->file_size) {
        delete index;
        return Status::kFailure;
      }
      prev_end = id.offset() + id.length();
      blocks.push_back(id);
      index_values.push_back(index->value());
    }
    delete index;
  }

  // 2. Read block-aligned chunks of at most max_read_bytes and slice the
  //    blocks out of each. Every parsed block goes through the same
  //    deserializer and iterator as readBlock/blockReader.
  size_t i = 0;
  while (i < blocks.size()) {
    uint64_t const base = blocks[i].offset();
    size_t j = i + 1;
    while (j < blocks.size() && blocks[j].offset() + blocks[j].length() - base <= max_read_bytes) {
      ++j;
    }
    size_t const len = static_cast<size_t>(blocks[j - 1].offset() + blocks[j - 1].length() - base);
    unsigned char* buf = nullptr;
    Status s = rep_->storage->read(rep_->fileName, buf, static_cast<size_t>(base), len);
    if (s != Status::kSuccess || buf == nullptr) {
      delete[] buf;
      return s == Status::kSuccess ? Status::kFailure : s;
    }
    for (size_t k = i; k < j; ++k) {
      std::vector<google::protobuf::Message*> parsed;
      s = protobuf::deserializeMessages(buf + (blocks[k].offset() - base), blocks[k].length(), parsed,
                                        []() { return new BlockData(); });
      // A block is one serialized BlockData; anything past [0] is
      // unexpected and freed, as readBlock does implicitly.
      for (size_t m = 1; m < parsed.size(); ++m) delete parsed[m];
      if (s != Status::kSuccess || parsed.empty() || parsed[0] == nullptr) {
        if (!parsed.empty()) delete parsed[0];
        delete[] buf;
        return Status::kFailure;
      }
      // The iterator owns the BlockData (BlockIter::~BlockIter deletes it).
      Iterator* block_iter = newIterator(static_cast<BlockData*>(parsed[0]), rep_->comparator);
      Status v = visit(index_values[k], block_iter);
      delete block_iter;
      if (v != Status::kSuccess) {
        delete[] buf;
        return v;
      }
    }
    delete[] buf;
    i = j;
  }
  return Status::kSuccess;
}

Status Table::getAll(std::unordered_map<std::string, std::shared_ptr<Record>>& out,
                     size_t max_read_bytes) {
  out.clear();
  Status s = scanBlocks(max_read_bytes, [&out](std::string const&, Iterator* block_iter) {
    for (block_iter->seekToFirst(); block_iter->valid(); block_iter->next()) {
      auto record = std::make_shared<Record>();
      if (!record->ParseFromString(block_iter->value())) return Status::kFailure;
      out[block_iter->key()] = std::move(record);
    }
    return Status::kSuccess;
  });
  if (s != Status::kSuccess) out.clear();
  return s;
}

Status Table::warm(LRUCache* cache, size_t max_read_bytes, size_t& blocks, size_t& bytes) {
  blocks = 0;
  bytes = 0;
  if (cache == nullptr || rep_ == nullptr) return Status::kFailure;
  std::string const& file_name = rep_->fileName;
  return scanBlocks(max_read_bytes, [&](std::string const& index_value, Iterator* block_iter) {
    // Same shape as TableBuilder::flush: a heap map the cache owns, sized
    // by Record::ByteSizeLong, keyed by the index entry's bytes.
    auto* records = new std::unordered_map<std::string, std::shared_ptr<Record>>();
    size_t size = 0;
    for (block_iter->seekToFirst(); block_iter->valid(); block_iter->next()) {
      auto record = std::make_shared<Record>();
      if (!record->ParseFromString(block_iter->value())) {
        delete records;
        return Status::kFailure;
      }
      size += record->ByteSizeLong();
      (*records)[block_iter->key()] = std::move(record);
    }
    cache->putSSTableRecords(file_name, records, index_value, size, /*warmed=*/true);
    ++blocks;
    bytes += size;
    return Status::kSuccess;
  });
}

}  // namespace ozonedb