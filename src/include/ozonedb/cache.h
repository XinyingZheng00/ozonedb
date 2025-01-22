#ifndef OZONEDB_CACHE_H
#define OZONEDB_CACHE_H
#include "sstable/block_handler.h"
#include "sstable/table_reader.h"
#include "storage.h"
#include <list>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <mutex>
namespace ozonedb {

class View;
class FileMutexManager;
class TailCache {
 public:
  // mutex for tail_cache
  std::shared_mutex mutex;
  TailCache() = default;
  ~TailCache() = default;
  // Update the cache with a key, record, and offset
  void updateCache(std::string key, Record* record, std::string new_tail, std::string old_tail) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (record != nullptr) {
      cache_[key] = std::make_pair(record, new_tail);
    } else {
      cache_[key].second = new_tail;
    }

    if (!old_tail.empty() && tail_to_key_map.find(old_tail) != tail_to_key_map.end()) {  // maybe the old tail is already expired
      auto& old_vec = tail_to_key_map[old_tail];
      old_vec.erase(std::remove(old_vec.begin(), old_vec.end(), key), old_vec.end());
      if (old_vec.empty()) {
        tail_to_key_map.erase(old_tail);
      }
    }
    tail_to_key_map[new_tail].push_back(key);
  }
  // get cache_
  std::unordered_map<std::string, std::pair<Record*, std::string>>& getKeyToTailMap() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return cache_;
  }
  // get file_to_key_map
  std::unordered_map<std::string, std::vector<std::string>>& getTailToKeyMap() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return tail_to_key_map;
  }

  // Retrieve the latest record for a given key up to a specified offset
  bool getLatestRecord(std::string key, std::pair<Record*, std::string>& entry) {
    if (cache_.find(key) != cache_.end()) {
      entry = cache_[key];
      return true;
    } else {
      return false;
    }
  }

 private:
  std::unordered_map<std::string, std::pair<Record*, std::string>> cache_;
  std::unordered_map<std::string, std::vector<std::string>> tail_to_key_map;
};

class LRUCache {
 private:
  struct CacheEntry {
    std::unordered_map<std::string, Record*> records;
    std::unordered_map<std::string, std::unordered_map<std::string, Record*>> block_records;  // index_value_for_block -> records
    std::unordered_map<std::string, size_t> block_size;                                       // index_value_for_block -> tail
    std::unordered_map<std::string, std::list<std::pair<std::string, std::string>>::iterator> lru_itr;

    Table* table = nullptr;
    size_t offset = 0;    // Offset indicating the cached records until this offset
    bool sealed = false;  // Indicates whether the file is sealed (fully cached)
    // Default constructor
    CacheEntry() {}

    // contructor for records
    CacheEntry(std::unordered_map<std::string, Record*> records, size_t offset, bool sealed)
        : records(records), table(nullptr), offset(offset), sealed(sealed) {}
    // contructor for table
    CacheEntry(Table* table) : table(table), offset(), sealed(true) {}
  };
  Storage* storage = nullptr;
  size_t capacity = 33554432;
  size_t current_size = 0;
  std::shared_mutex mutex;  // this is to protect file_to_records_map and lru_list
  std::unordered_map<std::string, CacheEntry> file_to_entry_map;
  std::list<std::pair<std::string, std::string>> lru_list;
  // std::unordered_map<std::string, std::shared_mutex> file_to_mutex_map;
  // std::shared_mutex map_mutex;
  FileMutexManager* file_mutex_manager = nullptr;
  View* latest_view = nullptr;

  // Function to move a key to the front of the LRU list
  void updateLRU(std::string const& file_name, std::string const& index_value);

  // Function to evict the least recently used item
  void evict();

 public:
  LRUCache(int capacity, Storage* storage) : capacity(capacity), storage(storage) {}

  ~LRUCache() {
    for (auto& entry : file_to_entry_map) {
      for (auto& record : entry.second.records) {
        delete record.second;
      }
      for (auto& block : entry.second.block_records) {
        for (auto& record : block.second) {
          delete record.second;
        }
      }
      if (entry.second.table != nullptr) {
        delete entry.second.table;
      }
    }
  }
  // only for testing
  std::unordered_map<std::string, CacheEntry> getCacheMap() {
    return file_to_entry_map;
  }

  // set file_mutex_manager
  void setFileMutexManager(FileMutexManager* file_mutex_manager) {
    this->file_mutex_manager = file_mutex_manager;
  }

  void checkReadMoreLog(std::string const& file_name, bool& read_more, size_t& cached_offset, size_t& size);
  void readDataLog(std::string const& file_name, size_t cached_offset, size_t size);
  void getSSTable(std::string const& file_name, Table*& table);
  void needReadBlock(std::string const& file_name, bool& read_more, std::string const& index_value);
  void readDataBlocks(std::string const& file_name, std::string const& index_value);

  void get(std::string const& file_name, std::string const& key, Record*& record, std::string const& index_value = "");
  // Function to update the cache with a file_name, records, offset, and sealed status
  void putLogRecords(std::string const& key, std::unordered_map<std::string, Record*> const& records, size_t offset, bool sealed);
  void putSSTableMeta(std::string const& key, Table* table);
  void putSSTableRecords(std::string const& key, std::unordered_map<std::string, Record*> const& records, std::string const& index_value, size_t size);

  // set latest view
  void setLatestView(View* view) {
    latest_view = view;
  }
};
}  // namespace ozonedb
#endif