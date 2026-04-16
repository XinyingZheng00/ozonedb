#ifndef OZONEDB_CACHE_H
#define OZONEDB_CACHE_H
#include "sstable/block_handler.h"
#include "sstable/table_reader.h"
#include "storage.h"
#include <atomic>
#include <cstdint>
#include <future>
#include <list>
#include <memory>
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
  // Disable the cache for multi-writer backends (e.g. Corfu) where
  // entries can go stale from remote writes. When disabled, all
  // lookups miss and the read path falls through to the storage layer.
  void disable() { enabled_ = false; }
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
    if (!enabled_) return false;
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
  bool enabled_ = true;
  std::unordered_map<std::string, std::pair<Record*, std::string>> cache_;
  std::unordered_map<std::string, std::string> tail_to_tail_map;  // old tail -> new tail
};

class LRUCache {
 private:
  struct CacheEntry {
    std::unordered_map<std::string, Record*> records;
    std::unordered_map<std::string, std::unordered_map<std::string, Record*>*> block_records;  // index_value_for_block -> records
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
  // Separate pointers for log vs sstable files. In a backward-compat
  // single-backend configuration the two pointers alias; in a split
  // configuration (e.g. Corfu log + S3 sstables) they point at different
  // Storage instances and the per-method routing below keeps reads on the
  // right backend. Paper §3.5 motivation.
  Storage* log_storage_ = nullptr;
  Storage* sstable_storage_ = nullptr;
  size_t capacity = 33554432;
  size_t current_size = 0;
  std::shared_mutex mutex;  // this is to protect file_to_records_map and lru_list
  std::unordered_map<std::string, CacheEntry> file_to_entry_map;
  std::list<std::pair<std::string, std::string>> lru_list;
  // std::unordered_map<std::string, std::shared_mutex> file_to_mutex_map;
  // std::shared_mutex map_mutex;
  FileMutexManager* file_mutex_manager = nullptr;
  View* latest_view = nullptr;

  // Singleflight for concurrent block/table loads. Without these, N
  // readers that race on the same cold block each issue an S3 GET and
  // each call putSSTableRecords — double-counting current_size, leaking
  // the parsed records map, and triggering spurious eviction that
  // kicks out hot blocks. See putSSTableRecords docstring below.
  std::unordered_map<std::string, std::shared_future<void>> block_inflight_;
  std::unordered_map<std::string, std::shared_future<void>> table_inflight_;

  // Steady-state counters for diagnosing cache behavior. Printed at
  // LRUCache destruction. A healthy zipfian workload should show
  // hits >> misses once the hot set fits in `capacity`.
  std::atomic<uint64_t> sstable_cache_hits_{0};
  std::atomic<uint64_t> sstable_cache_misses_{0};

  // Function to move a key to the front of the LRU list
  void updateLRU(std::string const& file_name, std::string const& index_value);

  // Function to evict the least recently used item
  void evict();

 public:
  // Legacy single-storage constructor — both log and sstable reads go
  // through the same backend. Kept for backward-compat with tests and
  // configs that don't split storage.
  LRUCache(size_t capacity, Storage* storage)
      : log_storage_(storage), sstable_storage_(storage), capacity(capacity) {}

  // Split-storage constructor — log reads go through log_storage and
  // sstable reads go through sstable_storage. Used when the config sets
  // sstable_backend separately from the main backend.
  LRUCache(size_t capacity, Storage* log_storage, Storage* sstable_storage)
      : log_storage_(log_storage),
        sstable_storage_(sstable_storage),
        capacity(capacity) {}

  ~LRUCache() {
    for (auto& entry : file_to_entry_map) {
      for (auto& record : entry.second.records) {
        delete record.second;
      }
      //std::unordered_map<std::string, std::unordered_map<std::string, Record*>*> block_records; 
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
  // Insert or overwrite a single Record for a log file. Cache takes
  // ownership of the Record*. If a prior Record* existed at the same
  // key+file, it is deleted. Used by LogHandler to push freshly-written
  // records into the cache so LogKeyIndex can safely borrow the pointer.
  void putLogRecordSingle(std::string const& file_name, Record* record);
  // Copy the per-file records map (pointers only) into `out`. Cache
  // retains ownership of Record*. Used by LogKeyIndex::warm to seed
  // itself from already-cached log files on startup.
  void snapshotLogFileRecords(std::string const& file_name,
                              std::unordered_map<std::string, Record*>& out);
  void putSSTableMeta(std::string const& key, Table* table);
  void putSSTableRecords(std::string const& key, std::unordered_map<std::string, Record*>* records, std::string const& index_value, size_t size);

  // set latest view
  void setLatestView(View* view) {
    latest_view = view;
  }

  // Dump steady-state cache counters to stderr. Safe to call at shutdown.
  void printCacheStats() const;
};
}  // namespace ozonedb
#endif