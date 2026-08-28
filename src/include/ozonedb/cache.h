#ifndef OZONEDB_CACHE_H
#define OZONEDB_CACHE_H
#include "sstable/block_handler.h"
#include "sstable/table_reader.h"
#include "storage.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <future>
#include <list>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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
    // Log + SSTable block records: shared_ptr ownership so concurrent
    // readers (DB::get fast path, LogHandler::tryIndexLookup, the
    // multi-file scan in readRecord, Table::get) keep the Record
    // alive past invalidateLogFile, putLogRecordSingle overwrites,
    // and LRU eviction. The cache holds the canonical reference; any
    // borrower keeps an additional refcount until they release.
    std::unordered_map<std::string, std::shared_ptr<Record>> records;
    std::unordered_map<std::string, std::unordered_map<std::string, std::shared_ptr<Record>>*> block_records;  // index_value_for_block -> records
    std::unordered_map<std::string, size_t> block_size;                                       // index_value_for_block -> tail
    std::unordered_map<std::string, std::list<std::pair<std::string, std::string>>::iterator> lru_itr;
    // Blocks published by the warm worker (part B of
    // bench/PLAN-compaction-cache.md) that no reader has touched yet. A
    // get() on a marked block counts one warm hit and clears the mark, so
    // warm_hits_ / warm_blocks_ is the fraction of warmed blocks that
    // were worth the read. Evict and invalidate drop the mark with the
    // block.
    std::unordered_set<std::string> warmed_blocks;

    Table* table = nullptr;
    size_t offset = 0;    // Offset indicating the cached records until this offset
    bool sealed = false;  // Indicates whether the file is sealed (fully cached)
    // Default constructor
    CacheEntry() {}

    // contructor for records
    CacheEntry(std::unordered_map<std::string, std::shared_ptr<Record>> records, size_t offset, bool sealed)
        : records(std::move(records)), table(nullptr), offset(offset), sealed(sealed) {}
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
  View const* latest_view = nullptr;

  // Memoized fenced size of each SEALED log file.
  //
  // The read bound for a sealed log used to come from the View, whose
  // file_size for that file is whatever the rolling writer stamped into
  // OperationRecord::sealed_input_bytes — a number that writer read from
  // its own UNFENCED view. Peer records appended just before the seal sit
  // above that bound and stay unreadable forever, in default mode and
  // under linearizable_reads alike. The bound has to come from a fenced
  // storage->size().
  //
  // Memoized because a sealed file never grows, so one fence per file per
  // process is enough. Guarded by its own mutex, never by `mutex`: the
  // storage->size() call fences on the Corfu tailer, and the tailer's
  // listener takes `mutex` — holding it across the fence is the lock
  // inversion that checkReadMoreLog documents.
  std::mutex sealed_size_mtx_;
  std::unordered_map<std::string, size_t> sealed_size_;
  size_t sealedLogSize(std::string const& file_name);

  // Singleflight for concurrent block/table loads. Without these, N
  // readers that race on the same cold block each issue an S3 GET and
  // each call putSSTableRecords — double-counting current_size, leaking
  // the parsed records map, and triggering spurious eviction that
  // kicks out hot blocks. See putSSTableRecords docstring below.
  std::unordered_map<std::string, std::shared_future<void>> block_inflight_;
  std::unordered_map<std::string, std::shared_future<void>> table_inflight_;

  // Tables of removed files, kept alive for a grace period. getSSTable
  // hands out raw Table* and Table::get runs on them without holding
  // `mutex`, so invalidateLogFile (a peer compaction's REMOVE, applied
  // by the tailer) must not delete the object under a reader in
  // flight: that was a use-after-free whose visible symptom was a null
  // `table` in readDataBlocks (SIGSEGV, one crash per compaction that
  // removed a file being read). A read finishes in milliseconds; the
  // grace period is seconds. Entries older than kRetiredTableGraceMs
  // are freed on the next invalidateLogFile and in the destructor.
  static constexpr int64_t kRetiredTableGraceMs = 30000;
  std::deque<std::pair<std::chrono::steady_clock::time_point, Table*>> retired_tables_;

  // Steady-state counters for diagnosing cache behavior. Printed at
  // LRUCache destruction. A healthy zipfian workload should show
  // hits >> misses once the hot set fits in `capacity`.
  std::atomic<uint64_t> sstable_cache_hits_{0};
  std::atomic<uint64_t> sstable_cache_misses_{0};
  // The same hits and misses split by SSTable level (slot = the level
  // parsed from "sstable<L>/<name>", slot 0 = any other name). Which
  // level misses is what tells a write-heavy cell apart from a
  // read-only one (bench/PLAN-compaction-cache.md, part C).
  static constexpr int kLevelSlots = 16;
  std::atomic<uint64_t> level_hits_[kLevelSlots] = {};
  std::atomic<uint64_t> level_misses_[kLevelSlots] = {};
  // Warm-worker counters (part B). All zero until the worker exists.
  std::atomic<uint64_t> warm_files_{0};
  std::atomic<uint64_t> warm_blocks_{0};
  std::atomic<uint64_t> warm_bytes_{0};
  std::atomic<uint64_t> warm_hits_{0};
  std::atomic<uint64_t> warm_skipped_disabled_{0};
  std::atomic<uint64_t> warm_skipped_level_{0};
  std::atomic<uint64_t> warm_skipped_budget_{0};
  std::atomic<uint64_t> warm_skipped_affinity_{0};
  std::atomic<uint64_t> warm_skipped_gone_{0};
  std::atomic<uint64_t> warm_skipped_built_{0};
  std::atomic<uint64_t> warm_dropped_{0};

  // Level of an SSTable name "sstable<L>/<name>", 0 for any other name
  // or a level outside [1, kLevelSlots). A plain scan, not std::regex:
  // needReadBlock runs on every read and getSSTLayerNumber builds a
  // regex per call.
  static int levelSlot(std::string const& file_name);

  // Function to move a key to the front of the LRU list
  void updateLRU(std::string const& file_name, std::string const& index_value);

  // Function to evict the least recently used item
  void evict();

 public:
  // One consistent reading of every counter, taken under the cache
  // mutex. dead_* cover blocks whose file is not in latest_view (a
  // compacted-away SSTable that nobody dropped); after part A they must
  // be 0. printCacheStats formats exactly this.
  struct Stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t level_hits[kLevelSlots] = {};
    uint64_t level_misses[kLevelSlots] = {};
    size_t capacity = 0;
    size_t current_size = 0;
    size_t files = 0;
    size_t sstable_files = 0;  // entries that hold a Table* or blocks
    size_t dead_bytes = 0;
    size_t dead_blocks = 0;
    size_t dead_files = 0;
    size_t retired_tables = 0;
    uint64_t warm_files = 0;
    uint64_t warm_blocks = 0;
    uint64_t warm_bytes = 0;
    uint64_t warm_hits = 0;
    uint64_t warm_skipped_disabled = 0;
    uint64_t warm_skipped_level = 0;
    uint64_t warm_skipped_budget = 0;
    uint64_t warm_skipped_affinity = 0;
    uint64_t warm_skipped_gone = 0;
    uint64_t warm_skipped_built = 0;
    uint64_t warm_dropped = 0;
  };
  Stats stats();

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
      // Log + block records are shared_ptr — destructors handle them.
      // Each block_records entry is a heap-allocated map we still own.
      for (auto block : entry.second.block_records) {
        delete block.second;
      }
      if (entry.second.table != nullptr) {
        delete entry.second.table;
      }
    }
    for (auto& retired : retired_tables_) {
      delete retired.second;
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
  // caller_table: the Table the caller is already reading through
  // (Table::get passes `this`). Used when the file's cache entry has
  // no Table* — populated by the compaction write-through before any
  // reader opened the file, or erased by a REMOVE applied between
  // needReadBlock and this call. A block that cannot be read is a
  // miss (get() then returns no record), never a crash.
  void readDataBlocks(std::string const& file_name, std::string const& index_value, Table* caller_table = nullptr);

  // SSTable variant — returns a shared_ptr so the Record stays alive
  // past a concurrent evict(). Updates LRU order on hit.
  void get(std::string const& file_name, std::string const& key, std::shared_ptr<Record>& record, std::string const& index_value = "");
  // Log variant — returns a shared_ptr so the Record stays alive past
  // a concurrent invalidateLogFile or a same-key overwrite from
  // putLogRecordSingle. Used by LogHandler reads and the LogKeyIndex.
  void getLog(std::string const& file_name, std::string const& key, std::shared_ptr<Record>& record);
  // Function to update the cache with a file_name, records, offset, and sealed status
  void putLogRecords(std::string const& key, std::unordered_map<std::string, std::shared_ptr<Record>> records, size_t offset, bool sealed);
  // Insert or overwrite a single Record for a log file. Cache takes
  // shared ownership of the Record. If a prior Record existed at the
  // same key+file, its shared_ptr is dropped (the Record only frees
  // when the last borrower releases it). Used by LogHandler to push
  // freshly-written records into the cache so LogKeyIndex can safely
  // share the pointer.
  void putLogRecordSingle(std::string const& file_name, std::shared_ptr<Record> record);
  // Drop a log file's cache entry. Records are released via shared_ptr —
  // they only deallocate once every borrower (readers, the LogKeyIndex)
  // has released them. Called when a log file is compacted away.
  void invalidateLogFile(std::string const& file_name);
  // Copy the per-file records map into `out`. Cache and `out` co-own
  // each Record via shared_ptr. Used by LogKeyIndex::warm to seed
  // itself from already-cached log files on startup.
  void snapshotLogFileRecords(std::string const& file_name,
                              std::unordered_map<std::string, std::shared_ptr<Record>>& out);
  void putSSTableMeta(std::string const& key, Table* table);
  void putSSTableRecords(std::string const& key, std::unordered_map<std::string, std::shared_ptr<Record>>* records, std::string const& index_value, size_t size);

  // set latest view
  void setLatestView(View const* view) {
    latest_view = view;
  }

  // Dump the counters to stderr at DB close. Two lines: the original
  // `[lru_cache] sstable hits=…` line, unchanged because the extractor
  // (bench/scripts/extract_cost_coefficients.py) parses it, and a
  // `[lru_cache] levels …` line with the per-level, dead-block and warm
  // counters.
  void printCacheStats();
};
}  // namespace ozonedb
#endif