#include "cache.h"
#include "db.h"
#include "metadata_log_handler.h"
#include "protobuf_serializer.h"

namespace ozonedb {
// Function to move a key to the front of the LRU list
void LRUCache::updateLRU(std::string const& file_name, std::string const& index_value) {
  lru_list.erase(file_to_entry_map[file_name].lru_itr[index_value]);
  lru_list.push_front({file_name, index_value});
  file_to_entry_map[file_name].lru_itr[index_value] = lru_list.begin();
}

// Function to evict the least recently used item
void LRUCache::evict() {
  while (current_size > capacity) {
    auto it = lru_list.rbegin();
    current_size -= file_to_entry_map[it->first].block_size[it->second];
    file_to_entry_map[it->first].block_records.erase(it->second);
    file_to_entry_map[it->first].block_size.erase(it->second);
    file_to_entry_map[it->first].lru_itr.erase(it->second);
    lru_list.pop_back();
  }
}
void LRUCache::checkReadMoreLog(std::string const& file_name, bool& read_more, size_t& cached_offset, size_t& size) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it != file_to_entry_map.end()) {
    // Key found in the cache
    // updateLRU(file_name);
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
  bool sealed = file_name!=latest_view->current_log_tail;
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
    // updateLRU(file_name);
  }
}

void LRUCache::needReadBlock(std::string const& file_name, bool& read_more, std::string const& index_value) {
  std::unique_lock lock(mutex);
  // updateLRU(file_name);
  auto it = file_to_entry_map.find(file_name);
  if (it->second.block_records.find(index_value) != it->second.block_records.end()) {
    read_more = false;
  }
}

void LRUCache::readDataBlocks(std::string const& file_name, std::string const& index_value) {
  std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
  std::unique_lock file_lock(file_mutex);
  // if sstable not in cache, read from storage and put it in cache
  Table* table = file_to_entry_map[file_name].table;
  // read block from storage
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
  // Get the records from the cache
  // if it is log file:
  std::shared_lock lock(mutex);
  if (file_name.find("log") != std::string::npos) {
    auto& records = file_to_entry_map[file_name].records;
    // Find the record corresponding to the key
    auto record_it = records.find(key);
    if (record_it != records.end()) {
      // Key found, assign the record
      record = record_it->second;
    } else {
      record = nullptr;  // empty record
    }
  } else {
    auto& records = file_to_entry_map[file_name].block_records[index_value];
    updateLRU(file_name, index_value);
    // Find the record corresponding to the key
    auto record_it = records->find(key);
    if (record_it != records->end()) {
      // Key found, assign the record
      record = record_it->second;
    } else {
      record = nullptr;  // empty record
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
    // updateLRU(key);
  } else {
    // Key does not exist, insert new entry
    // if (lru_list.size() >= capacity) {
    //   evict();
    // }
    // lru_list.push_front(key);
    CacheEntry entry(records, offset, sealed);
    file_to_entry_map[key] = entry;
  }
}

void LRUCache::putSSTableMeta(std::string const& key, Table* table) {
  std::unique_lock lock(mutex);
  // if (lru_list.size() >= capacity) {
  //   evict();
  // }
  // lru_list.push_front(key);
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
  lru_list.push_front({key, index_value});
  file_to_entry_map[key].lru_itr[index_value] = lru_list.begin();
}

}  // namespace ozonedb