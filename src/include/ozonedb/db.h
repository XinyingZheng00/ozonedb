#ifndef DB_H
#define DB_H

#include "cache.h"
#include "compaction.h"
#include "log_handler.h"
#include "protobuf/record.pb.h"
#include "sstable/sstable_handler.h"
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>
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
class Transaction;
class DB {
 private:
  friend class Transaction;
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
             std::shared_ptr<Record>& guard, bool force_strict = false);

  /**
   * @brief read the value and version of a key for read-modify-write
   *
   * The version is the global log address of the key's last accepted
   * write, suitable as compareAndPut's expected_version. Takes ONE
   * storage fence (reusing a caller-held DB::sync() token if any) and
   * reads strictly — regardless of linearizable_reads — so the pair
   * reflects every write acked before the call and the value never
   * lags the version. version is -1 when the key has never been
   * written. Requires Metadata::track_versions (kFailure otherwise).
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
   * @brief start a serializable transaction (see Transaction)
   *
   * Takes ONE fence (DB::sync()) on the calling thread; every get()
   * inside the transaction reuses it. The Transaction must be used and
   * finished (commit or abort) on this same thread — the fence token is
   * thread-local. Requires Metadata::track_versions: without it the
   * returned Transaction is closed and every call on it fails.
   *
   * @param validate_read_only  commit() of a transaction with no writes
   *        appends a read-only validation record (one append) so the
   *        reads are proven serializable; false skips the append and
   *        such a commit always succeeds.
   */
  Transaction begin(bool validate_read_only = true);

  /**
   * @brief commit a read set and a write set as one atomic log record
   *
   * The building block under Transaction::commit, exposed for callers
   * that collect the read set themselves (the JNI binding pairs it
   * with sync()/getVersioned()/clearSync()). Semantics are
   * Storage::appendTransaction's: accepted iff every read-set version
   * still holds at the record's log position, then the whole write set
   * is applied atomically; write-set keys absent from the read set are
   * blind writes. Does not touch the calling thread's fence token.
   *
   * @param version  output: the commit record's log address (every
   *                 written key's new version); -1 when nothing was
   *                 appended (both sets empty)
   * @return Status kSuccess | kCasConflict | kFailure
   */
  Status commitTransaction(std::vector<ReadVersion> const& read_set,
                           std::vector<Record> const& write_set,
                           int64_t& version);

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

/**
 * @brief serializable optimistic transaction on the shared log
 *
 * Obtained from DB::begin(). get() records {key, version} in the read
 * set (write-buffered keys are served from the write set instead);
 * put()/remove() buffer records; commit() appends ONE conditional
 * entry carrying both sets (Storage::appendTransaction) and returns
 * the shared log's verdict — kSuccess with the commit record's address
 * as the new version of every written key, kCasConflict if any read
 * key was written since it was read, kFailure otherwise.
 *
 * Isolation: the commit record's log position is the linearization
 * point; because every read-set version equals the version at that
 * position, the transaction is equivalent to one that ran alone there
 * (strictly serializable). Reads are NOT a snapshot — a later get()
 * may observe a newer state than an earlier one — but validation
 * rejects such a transaction, so this only ever costs an abort. Keys
 * written but never read are blind writes and are not validated. There
 * is no range read, so phantoms cannot arise.
 *
 * Lifetime: a Transaction that goes out of scope still open aborts,
 * releasing the fence token begin() took on this thread (a leaked
 * token would pin later reads on the thread to a stale fence). Not
 * copyable; movable. Single-threaded: use it only on the thread that
 * called begin().
 */
class Transaction {
 public:
  Transaction(Transaction&& other) noexcept;
  Transaction(Transaction const&) = delete;
  Transaction& operator=(Transaction const&) = delete;
  Transaction& operator=(Transaction&&) = delete;
  ~Transaction();

  /**
   * @brief read a key; adds {key, observed version} to the read set
   *
   * @return kSuccess, kNotFound (absent or deleted — still recorded, so
   *         the commit validates that the key stayed unwritten),
   *         kFailure (transaction not open, or track_versions off)
   */
  Status get(std::string const& key, std::string& value);
  void put(std::string const& key, std::string const& value);
  void remove(std::string const& key);
  /**
   * @brief append the commit record and close the transaction
   *
   * With an empty write set: appends a read-only validation record when
   * begin(validate_read_only=true) and the read set is non-empty, else
   * appends nothing and returns kSuccess (version = -1).
   *
   * @return kSuccess | kCasConflict | kFailure (also for a closed txn)
   */
  Status commit(int64_t& version);
  void abort();

  bool isOpen() const { return open_; }
  std::vector<ReadVersion> const& readSet() const { return read_set_; }
  std::vector<Record> const& writeSet() const { return write_set_; }

 private:
  friend class DB;
  Transaction(DB* db, bool validate_read_only);
  void putRecord(std::string const& key, std::string const* value);
  void close();

  DB* db_;
  bool open_ = false;
  bool validate_read_only_;
  std::vector<ReadVersion> read_set_;
  std::unordered_map<std::string, size_t> read_index_;
  std::vector<Record> write_set_;
  std::unordered_map<std::string, size_t> write_index_;
};
}  // namespace ozonedb
#endif  // DB_H

// Concurrent put get.