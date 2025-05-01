#include "cache.h"
#include "db.h"
#include "metadata_log_handler.h"
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
  while (current_size > capacity) {
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
void LRUCache::checkReadMoreLog(std::string const& file_name, bool& read_more, size_t& cached_offset, size_t& size) {
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

void LRUCache::readDataLog(std::string const& file_name, size_t cached_offset, size_t size) {
  std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
  std::unique_lock file_lock(file_mutex);
  unsigned char* buffer = nullptr;
  this->storage->read(file_name, buffer, cached_offset, size - cached_offset);
  file_lock.unlock();
  std::vector<google::protobuf::Message*> messages;
  protobuf::deserializeMessages(buffer, size - cached_offset, messages, []() -> google::protobuf::Message* {
    return new Record();
  });
  delete[] buffer;
  buffer = nullptr;
  std::unordered_map<std::string, Record*> records_tmp;
  for (auto* msg : messages) {
    auto* rec = static_cast<Record*>(msg);
    records_tmp[rec->key()] = rec;
  }
  bool sealed = file_name != latest_view->current_log_tail;
  putLogRecords(file_name, records_tmp, size, sealed);
}

void LRUCache::getSSTable(std::string const& file_name, Table*& table) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it == file_to_entry_map.end()) {
    lock.unlock();
    std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
    std::unique_lock file_lock(file_mutex);
    Status status = Table::open(this->storage, file_name, table);
    file_lock.unlock();
    table->setCache(this);
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
  std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
  std::unique_lock file_lock(file_mutex);
  Table* table = file_to_entry_map[file_name].table;
  Iterator* iter = table->blockReader(table, index_value);
  file_lock.unlock();
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
  std::shared_lock lock(mutex);
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
  if (current_size > capacity) {
    evict();
  }
  updateLRU(key, index_value);
}

/*
LRUCacheForSharedLog::CacheEntry::CacheEntry(std::unordered_map<std::string, Record*> records, size_t offset, bool sealed)
    : records(std::move(records)), offset(offset), sealed(sealed) {}

LRUCacheForSharedLog::LRUCacheForSharedLog(size_t capacity, Storage* storage)
    : storage(storage),
      capacity(capacity),
      log_num_limit(2),
      current_size(0),
      log_num(0),
      file_mutex_manager(nullptr),
      latest_view(nullptr) {}

LRUCacheForSharedLog::~LRUCacheForSharedLog() {
  for (auto& entry : file_to_entry_map) {
    for (auto& record : entry.second.records) {
      delete record.second;
    }
  }
}

void LRUCacheForSharedLog::checkReadMoreSharedLog(const std::string& file_name, bool& read_more, size_t& cached_offset, size_t& size) {
  std::shared_lock lock(mutex);
  if (file_to_entry_map.find(file_name) == file_to_entry_map.end()) {
    read_more = true;
    cached_offset = 0;
    size = 0;
    return;
  }

  const CacheEntry& entry = file_to_entry_map[file_name];
  cached_offset = entry.offset;

  SharedLogStorage log(file_name);
  size = log.size();

  read_more = (cached_offset < size);
}

void LRUCacheForSharedLog::readSharedLog(const std::string& file_name, size_t cached_offset, size_t size) {
  std::vector<std::string> entries;
  SharedLogStorage log(file_name);
  Status s = log.read(entries, cached_offset, size);
  if (!s.ok()) {
    std::cerr << "[LRUCacheForSharedLog] Read failed for " << file_name << std::endl;
    return;
  }

  std::unordered_map<std::string, Record*> records;
  size_t new_offset = cached_offset;

  for (const auto& entry : entries) {
    Record* record = new Record();
    record->parseFromString(entry);  // assumes this method exists
    records[record->key()] = record;
    ++new_offset;
  }

  putLogRecords(file_name, records, new_offset, new_offset == log.size());
}

void LRUCacheForSharedLog::putLogRecords(const std::string& key, const std::unordered_map<std::string, Record*>& records, size_t offset, bool sealed) {
  std::unique_lock lock(mutex);

  if (file_to_entry_map.find(key) == file_to_entry_map.end()) {
    file_to_entry_map[key] = CacheEntry();
    lru_list_log.push_front(key);
    file_to_entry_map[key].lru_itr_log = lru_list_log.begin();
  }

  auto& entry = file_to_entry_map[key];
  for (auto& kv : records) {
    entry.records[kv.first] = kv.second;
  }

  entry.offset = offset;
  entry.sealed = sealed;

  updateLRULog(key);
}

void LRUCacheForSharedLog::get(const std::string& file_name, const std::string& key, Record*& record, const std::string& index_value) {
  std::shared_lock lock(mutex);
  if (file_to_entry_map.find(file_name) == file_to_entry_map.end()) {
    record = nullptr;
    return;
  }

  auto& entry = file_to_entry_map[file_name];
  auto it = entry.records.find(key);
  if (it != entry.records.end()) {
    record = it->second;
    updateLRU(file_name, index_value);
  } else {
    record = nullptr;
  }
}

void LRUCacheForSharedLog::updateLRULog(const std::string& file_name) {
  auto& lru_itr = file_to_entry_map[file_name].lru_itr_log;
  lru_list_log.erase(lru_itr);
  lru_list_log.push_front(file_name);
  file_to_entry_map[file_name].lru_itr_log = lru_list_log.begin();
}

void LRUCacheForSharedLog::updateLRU(const std::string& file_name, const std::string& index_value) {
  // No-op in current version, can be added later
}

void LRUCacheForSharedLog::setFileMutexManager(FileMutexManager* manager) {
  file_mutex_manager = manager;
}

void LRUCacheForSharedLog::setLatestView(View* view) {
  latest_view = view;
}*/
}  // namespace ozonedb
