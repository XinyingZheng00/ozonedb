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
  Metadata* metadata;
  CompactionWatcher* watcher = nullptr;
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
   * @brief read the value and version of a key for read-modify-write
   *
   * The version is the global log address of the key's last accepted
   * write, suitable as compareAndPut's expected_version. Fences on the
   * shared log tail before reading, so the pair reflects every write
   * acked before the call. version is -1 when the key has never been
   * written (or the backend doesn't track versions).
   *
   * @param key
   * @param value    output: copy of the value bytes
   * @param version  output: the key's version, -1 if unwritten
   * @return Status  kNotFound when the key is absent or deleted
   */
  Status getVersioned(std::string const& key, std::string& value,
                      int64_t& version);

  /**
   * @brief conditionally put: succeeds only if the key's version is
   * still expected_version at the write's position in the shared log
   *
   * The condition is evaluated deterministically by every replica's
   * apply loop, so a successful compareAndPut is totally ordered
   * against all concurrent writes to the key — a get/compareAndPut
   * retry loop is an atomic read-modify-write. Requires a shared-log
   * backend (Corfu); other backends return kFailure. Concurrent blind
   * put()s to the same key are not ordered by the check (a put always
   * wins); keys managed via CAS should be written only via CAS after
   * the initial seed.
   *
   * @param key
   * @param expected_version  version from getVersioned, -1 = key unwritten
   * @param value
   * @param new_version  output: the key's version after a successful put
   * @return Status kSuccess | kCasConflict | kFailure
   */
  Status compareAndPut(std::string const& key, int64_t expected_version,
                       std::string const& value, int64_t& new_version);

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