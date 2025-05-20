#ifndef OZONEDB_CACHE_H
#define OZONEDB_CACHE_H
#include "sstable/block_handler.h"
#include "sstable/table_reader.h"
#include "storage/shared_log_storage.h"
#include "storage/storage.h"
#include "view.h"
#include <condition_variable>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
namespace ozonedb {

class TailCache {
 public:
  // mutex for tail_cache
  std::shared_mutex mutex;
  TailCache() = default;
  ~TailCache() = default;
  // Update the cache with a key, record, and offset
  void updateCache(std::string key, Record* record, std::string new_tail, std::string old_tail) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (old_tail.empty()) {
      old_tail = cache_[key].second;
    }
    if (record != nullptr) {
      cache_[key] = std::make_pair(record, new_tail);
    } else {
      cache_[key].second = new_tail;
    }
  }

  // add tail change
  void addTailChange(std::string old_tail, std::string new_tail) {
    tail_to_tail_map[old_tail] = new_tail;
  }

  // Retrieve the latest record for a given key up to a specified offset
  // also check if the tail has changed
  bool getLatestRecord(std::string key, std::pair<Record*, std::string>& entry) {
    if (cache_.find(key) != cache_.end()) {
      std::string old_tail = cache_[key].second;
      while (tail_to_tail_map.find(old_tail) != tail_to_tail_map.end()) {
        old_tail = tail_to_tail_map[old_tail];
      }
      if (old_tail != entry.second) {
        entry.second = old_tail;
      }
      entry = cache_[key];
      return true;
    } else {
      return false;
    }
  }

 private:
  std::unordered_map<std::string, std::pair<Record*, std::string>> cache_;
  std::unordered_map<std::string, std::string> tail_to_tail_map;  // old tail -> new tail
};

class LRUCache {
 private:
  struct CacheEntry {
    std::unordered_map<std::string, Record*> records;
    std::list<std::string>::iterator lru_itr_log;

    std::unordered_map<std::string, std::unordered_map<std::string, Record*>*> block_records;  // index_value_for_block -> records
    std::unordered_map<std::string, size_t> block_size;                                        // index_value_for_block -> tail
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
  Storage* log_storage = nullptr;
  FileStorage* sst_storage = nullptr;

  int log_num_limit = 2;
  int log_num = 0;

  size_t block_cache_capacity = 33554432;
  size_t current_size = 0;
  std::shared_mutex mutex;                                        // this is to protect file_to_records_map and lru_list
  std::unordered_map<std::string, CacheEntry> file_to_entry_map;  // to sharedlog storage, the file_name is the sharedlog/start_index:end_index
  std::list<std::pair<std::string, std::string>> lru_list;
  std::list<std::string> lru_list_log;
  View* latest_view = nullptr;

  // Function to move a key to the front of the LRU list
  void updateLRU(std::string const& file_name, std::string const& index_value);
  void updateLRULog(std::string const& file_name);
  // Function to evict the least recently used item
  void evict();
  void evictLog();

 public:
  std::mutex inflight_mu_;
  std::unordered_map<std::string, std::shared_ptr<std::condition_variable>> inflight_;

  LRUCache(int log_num_limit, int capacity, Storage* log_storage, FileStorage* sst_storage)
      : log_num_limit(log_num_limit), block_cache_capacity(capacity), log_storage(log_storage), sst_storage(sst_storage) {}

  ~LRUCache() {
    for (auto& entry : file_to_entry_map) {
      for (auto& record : entry.second.records) {
        delete record.second;
      }
      // std::unordered_map<std::string, std::unordered_map<std::string, Record*>*> block_records;
      for (auto block : entry.second.block_records) {
        for (auto record : *block.second) {
          delete record.second;
        }
        delete block.second;
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

  void shouldReadMoreLog(std::string const& file_name, bool& read_more, size_t& cached_offset, size_t& size);
  // void readDataLog(std::string const& file_name, size_t cached_offset, size_t size);
  void getSSTable(std::string const& file_name, Table*& table);
  void needReadBlock(std::string const& file_name, bool& read_more, std::string const& index_value);
  void readDataBlocks(std::string const& file_name, std::string const& index_value);

  void get(std::string const& file_name, std::string const& key, Record*& record, std::string const& index_value = "");
  // Function to update the cache with a file_name, records, offset, and sealed status
  void putLogRecords(std::string const& key, std::unordered_map<std::string, Record*> const& records, size_t offset, bool sealed);
  void putSSTableMeta(std::string const& key, Table* table);
  void putSSTableRecords(std::string const& key, std::unordered_map<std::string, Record*>* records, std::string const& index_value, size_t size);

  void setLatestView(View* view) {
    latest_view = view;
  }
};

}  // namespace ozonedb
#endif