#include "cache.h"
#include "db.h"
#include "helper.h"
#include "protobuf_serializer.h"

namespace ozonedb {
// Function to move a key to the front of the LRU list
void LRUCache::updateLRU(std::string const& file_name, std::string const& index_value) {
  if (file_to_entry_map[file_name].lru_itr.find(index_value) != file_to_entry_map[file_name].lru_itr.end()) {
    auto index_value_itr = file_to_entry_map[file_name].lru_itr[index_value];
    lru_list.erase(index_value_itr);
  }
  lru_list.push_front({file_name, index_value});
  file_to_entry_map[file_name].lru_itr[index_value] = lru_list.begin();
}

// Function to evict the least recently used item
void LRUCache::evict() {
  while (current_size > block_cache_capacity) {
    auto it = lru_list.rbegin();
    current_size -= file_to_entry_map[it->first].block_size[it->second];
    auto block_records = file_to_entry_map[it->first].block_records[it->second];
    for (auto& record : *block_records) {
      delete record.second;
    }
    delete block_records;
    file_to_entry_map[it->first].block_records.erase(it->second);
    file_to_entry_map[it->first].block_size.erase(it->second);
    file_to_entry_map[it->first].lru_itr.erase(it->second);
    if (file_to_entry_map[it->first].block_records.empty()) {
      file_to_entry_map.erase(it->first);
    }
    lru_list.pop_back();
  }
}
void LRUCache::updateLRULog(std::string const& file_name) {
  if (file_to_entry_map.find(file_name) != file_to_entry_map.end()) {
    auto& lru_itr_log = file_to_entry_map[file_name].lru_itr_log;
    lru_list_log.erase(lru_itr_log);
    lru_list_log.push_front(file_name);
    file_to_entry_map[file_name].lru_itr_log = lru_list_log.begin();
  } else {
    lru_list_log.push_front(file_name);
  }
}

// Function to evict the least recently used item
void LRUCache::evictLog() {
  while (log_num > log_num_limit) {
    auto it = lru_list_log.back();
    log_num -= 1;
    auto records = file_to_entry_map[it].records;
    for (auto& record : records) {
      delete record.second;
    }
    file_to_entry_map.erase(it);
    lru_list_log.pop_back();
  }
}
void LRUCache::shouldReadMoreLog(std::string const& file_name, bool& read_more, size_t& cached_offset, size_t& size) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it != file_to_entry_map.end()) {
    if (it->second.sealed) {
      read_more = false;
      return;
    } else {
      cached_offset = it->second.offset;
    }
  }
  size = latest_view->getFileSize(file_name);
  if (size <= cached_offset) {
    read_more = false;
  }
}

// void LRUCache::readDataLog(std::string const& file_name, size_t cached_offset, size_t size) {
// std::unordered_map<std::string, Record*> records_map;
// bool sealed;
// std::vector<google::protobuf::Message*> messages;
// this->log_storage->read(
//     file_name, cached_offset, size,
//     [&]() -> google::protobuf::Message* {
//       return new Record();
//     },
//     messages);
// for (auto* msg : messages) {
//   auto* rec = static_cast<Record*>(msg);
//   records_map[rec->key()] = rec;
// }
// sealed = file_name != latest_view->current_log_tail;
// putLogRecords(file_name, records_map, size, sealed);
// }

void LRUCache::getSSTable(std::string const& file_name, Table*& table) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it == file_to_entry_map.end()) {
    lock.unlock();
    Status status = Table::open(this->sst_storage, file_name, table);
    table->setCache(this);  // fix this
    if (status != Status::kSuccess) {
      std::cout << "open table failed" << std::endl;
      return;
    }
    putSSTableMeta(file_name, table);
  } else {
    table = it->second.table;
  }
}

void LRUCache::needReadBlock(std::string const& file_name, bool& read_more, std::string const& index_value) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it->second.block_records.find(index_value) != it->second.block_records.end()) {
    read_more = false;
  }
}

void LRUCache::readDataBlocks(std::string const& file_name, std::string const& index_value) {
  Table* table = file_to_entry_map[file_name].table;
  Iterator* iter = table->blockReader(table, index_value);
  iter->seekToFirst();
  std::unordered_map<std::string, Record*>* records_tmp = new std::unordered_map<std::string, Record*>();
  size_t size = 0;
  while (iter->valid()) {
    std::string const& value = iter->value();
    auto* record = new Record();
    (*record).ParseFromArray(value.data(), value.size());
    (*records_tmp)[iter->key()] = record;
    size += record->ByteSizeLong();
    iter->next();
  }
  delete iter;
  putSSTableRecords(file_name, records_tmp, index_value, size);
}

// Function to get the latest records for a given filename, return records and offset
void LRUCache::get(std::string const& file_name, std::string const& key, Record*& record, std::string const& index_value) {
  std::unique_lock lock(mutex);
  if (file_name.find("log") != std::string::npos) {
    if (file_to_entry_map.find(file_name) == file_to_entry_map.end()) {
      // the latest tail may not be created yet
      record = nullptr;
      return;
    }
    auto& records = file_to_entry_map[file_name].records;
    updateLRULog(file_name);
    auto record_it = records.find(key);
    if (record_it != records.end()) {
      record = record_it->second;
    } else {
      record = nullptr;
    }
  } else {
    auto& records = file_to_entry_map[file_name].block_records[index_value];
    updateLRU(file_name, index_value);
    auto record_it = records->find(key);
    if (record_it != records->end()) {
      record = record_it->second;
    } else {
      record = nullptr;
    }
  }
}

void LRUCache::putLogRecords(std::string const& key, std::unordered_map<std::string, Record*> const& records, size_t offset, bool sealed) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(key);
  if (it != file_to_entry_map.end()) {
    // Key exists, merge the records and update the offset if necessary
    if (offset > it->second.offset) {
      for (auto const& record : records) {
        it->second.records[record.first] = record.second;
      }
      it->second.offset = offset;
      it->second.sealed = sealed;
    }
    updateLRULog(key);
  } else {
    // Key does not exist, insert new entry
    log_num += 1;
    if (log_num > log_num_limit) {
      evictLog();
    }
    updateLRULog(key);
    CacheEntry entry(records, offset, sealed);
    file_to_entry_map[key] = entry;
    file_to_entry_map[key].lru_itr_log = lru_list_log.begin();
  }
}

void LRUCache::putSSTableMeta(std::string const& key, Table* table) {
  std::unique_lock lock(mutex);
  CacheEntry entry(table);
  file_to_entry_map[key] = entry;
}

void LRUCache::putSSTableRecords(std::string const& key, std::unordered_map<std::string, Record*>* records, std::string const& index_value, size_t size) {
  std::unique_lock lock(mutex);
  file_to_entry_map[key].block_records[index_value] = records;
  file_to_entry_map[key].block_size[index_value] = size;
  current_size += size;
  if (current_size > block_cache_capacity) {
    evict();
  }
  updateLRU(key, index_value);
}

}  // namespace ozonedb
