#ifndef DB_H
#define DB_H

#include "cache.h"
#include "compaction.h"
#include "log_handler.h"
#include "protobuf/record.pb.h"
#include "sstable/sstable_handler.h"
#include <memory>
#include <thread>
#include <assert.h>

namespace ozonedb {
class FileMutexManager {
  std::unordered_map<std::string, std::shared_mutex> file_to_mutex_map;
  std::shared_mutex map_mutex;

 public:
  std::shared_mutex& getMutexForFile(std::string const& filename) {
    std::unique_lock<std::shared_mutex> lock(map_mutex);
    return file_to_mutex_map[filename];
  }
};

class EventListener {
 public:
  virtual void onLogCompactionStart(){};
  virtual void onLogCompactionCompletion(int time){};
  virtual void onSSTableCompactionStart(){};
  virtual void onSSTableCompactionCompletion(int time, int level){};
  virtual void onViewUpdate(){};
  virtual void onNewTail(){};
};

enum class Mode {
  Singleton,
  MultipleProcesses,
};
class LogTrimmer;
class DiskCacheStorage;
class DB {
 private:
  /**
   * @brief the state of the database
   *
   */
  std::atomic<bool> active;
  Mode mode;
  int compaction_per_operation = 10;
  int counter = 0;

  /**
   * @brief different modules of the database
   *
   * log_storage holds append-coordinated files (log layer, metadata log,
   * task log). sstable_storage holds SSTables. When the config does not
   * set a separate sstable_backend the two pointers alias and the
   * destructor's guard prevents a double-free. Paper §3.5 split.
   */
  Storage* log_storage = nullptr;
  Storage* sstable_storage = nullptr;
  // The disk tier in front of sstable_storage when disk_cache_dir is set;
  // then sstable_storage IS this object and owns the backing store. Non-owning.
  DiskCacheStorage* disk_cache = nullptr;
  Metadata* metadata;
  CompactionWatcher* watcher = nullptr;
  // Only when Metadata::log_trim_enabled and the log backend is Corfu:
  // this process checkpoints the log to sstable_storage and trims behind
  // the checkpoint (PLAN-trimming.md). Stopped in closeDB, which also runs
  // its final cycle, and deleted first in ~DB because it uses both stores.
  LogTrimmer* trimmer = nullptr;
  std::string fingerprint;
  // Holds an immutable snapshot of the metadata-log view. Refreshed
  // once per DB::get via std::atomic_load on the handler's internal
  // shared_ptr — the shared_ptr extends the View's lifetime across
  // the get's child-handler dispatch without deep-copying three
  // hashmaps on every call. The raw pointer each child handler
  // stores (setLatestView) aliases into this snapshot.
  std::shared_ptr<View const> latest_view_snapshot;
  std::mutex db_mutex;

  /**
   * @brief different handlers for read-write
   *
   */
  MetadataLogHandler* metadata_log = nullptr;
  LogHandler* log_handler = nullptr;
  SSTableHandler* sstable_handler = nullptr;
  TailCache* tail_cache = nullptr;
  LRUCache* lru_cache = nullptr;
  FileMutexManager* file_mutex_manager = nullptr;
  ThreadPool* thread_pool = nullptr;
  EventListener* event_listener = nullptr;

  double compaction_rate = 0;

  DB(std::string const& shared_config_path);
  ~DB();

 public:
  /**
   * @brief For testing purpose only
   *
   */
  CompactionWatcher* getWatcher() {
    return watcher;
  }
  std::atomic<bool>* getActive() {
    return &active;
  }
  TailCache* getCache() {
    return tail_cache;
  }

  //set event listener
  void setEventListener(EventListener* event_listener) {
    this->event_listener = event_listener;
    this->watcher->setEventListener(event_listener);
    this->metadata_log->setEventListener(event_listener);
  }

  /**
   * @brief open the database
   *
   * read metadata, create the handlers for RW, open the compaction watcher
   *
   * @param db the database object
   * @param shared_config_path  the path to the shared config file
   * @return Status  the status of the operation
   */
  static Status openDB(DB*& db, std::string const& shared_config_path);

  /**
   * @brief to close the database
   *
   * @param db the database object
   * @return Status the status of the operation
   */
  static Status closeDB(DB*& db);

  /**
   * @brief put the key value pair in the database
   *
   * @param key
   * @param value
   * @return Status
   */
  Status put(std::string const& key, std::string const& value);

  /**
   * @brief delete the key from the database
   *
   * @param key
   * @return Status
   */
  Status remove(std::string const& key);

  /**
   * @brief get the value of the key from the database
   *
   * `value` aliases bytes inside `guard`'s Record. The caller MUST keep
   * `guard` alive for as long as it dereferences `value`, otherwise a
   * concurrent compaction or LRU eviction can free the bytes mid-read.
   *
   * @param key
   * @param value  output: pointer to the value bytes
   * @param guard  output: shared_ptr keeping the value alive
   * @return Status
   */
  Status get(std::string const& key, std::string const*& value,
             std::shared_ptr<Record>& guard);

  /**
   * @brief Batch fence for strict reads (Metadata::linearizable_reads).
   *
   * sync() takes ONE storage fence on the calling thread; every get()
   * on this thread until clearSync() reuses it instead of fencing per
   * call, so each of those reads linearizes at the sync() point — the
   * analog of cr-sqlite's per-tick /barrier, and the amortization for
   * read batches (poll loops, scans over many keys). The caller must
   * pair every sync() with a clearSync(): a leaked token silently
   * pins later reads on this thread to a stale fence. No-ops on
   * non-fencing backends and in non-strict mode (a token is only
   * consulted by the fenced read path).
   */
  Status sync();
  Status clearSync();
};
}  // namespace ozonedb
#endif  // DB_H

// Concurrent put get.