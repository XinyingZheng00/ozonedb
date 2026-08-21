#include "db.h"
#include "helper.h"
#include "thread_pool.h"
#ifdef OZONEDB_ENABLE_CORFU
#include "corfu_storage.h"
#endif
#ifdef OZONEDB_ENABLE_S3
#include "s3_storage.h"
#endif
#include <algorithm>
#include <cmath>
#include <optional>
namespace ozonedb {

namespace {
// Build a Storage instance for one of the supported backend kinds.
// Used twice from DB::DB — once for the log layer (always required) and
// optionally a second time for SSTable storage if the config picks a
// separate backend per paper §3.5.
Storage* makeStorage(BackendKind kind, Metadata const& md, bool for_sstables) {
  switch (kind) {
    case BackendKind::kLocal:
      return new FileStorage(for_sstables && !md.sstable_dir.empty()
                                 ? md.sstable_dir
                                 : md.DBpath);
    case BackendKind::kAzure:
      return new AzureBlobStorage(
          "DefaultEndpointsProtocol=https;AccountName=ozonedbstorage;AccountKey=RRKjiP5iHjd+8lC36H6+IKf1F1WO7M8F3g5VgIqDT1NTbAQXX19xNY2pipUGtGJU9f1/j17jsmtD+AStPt3y4A==;EndpointSuffix=core.windows.net",
          for_sstables ? md.sstable_container_name : md.container_name,
          md.DBpath);
    case BackendKind::kCorfu:
#ifdef OZONEDB_ENABLE_CORFU
      return new CorfuDBStorage(md.corfu_endpoint, md.corfu_jar_path,
                                md.corfu_jvm_opts, md.corfu_stream_name,
                                md.DBpath);
#else
      throw std::runtime_error(
          "OzoneDB was built without CorfuDB support (rebuild with "
          "-DOZONEDB_ENABLE_CORFU=ON)");
#endif
    case BackendKind::kS3:
#ifdef OZONEDB_ENABLE_S3
      return new S3Storage(md.s3_endpoint, md.s3_region, md.s3_bucket,
                           md.s3_access_key, md.s3_secret_key,
                           md.s3_use_path_style,
                           for_sstables ? md.sstable_dir : md.DBpath);
#else
      throw std::runtime_error(
          "OzoneDB was built without S3 support (rebuild with "
          "-DOZONEDB_ENABLE_S3=ON)");
#endif
  }
  throw std::runtime_error("makeStorage: unhandled BackendKind");
}
}  // namespace

DB::DB(std::string const& shared_config_path) {
  this->metadata = new Metadata(shared_config_path);
  if (this->metadata->mode == 0) {
    this->mode = Mode::Singleton;
  } else {
    this->mode = Mode::MultipleProcesses;
  }

  this->log_storage = makeStorage(this->metadata->backend_kind,
                                  *this->metadata, /*for_sstables=*/false);
  if (this->metadata->sstable_backend_set) {
    this->sstable_storage = makeStorage(this->metadata->sstable_backend_kind,
                                        *this->metadata, /*for_sstables=*/true);
  } else {
    // Backward-compat: SSTables share the main backend.
    this->sstable_storage = this->log_storage;
  }

  this->tail_cache = new TailCache();
  // TailCache is safe for now even under multi-writer Corfu — the
  // write-side population path (db.cpp:190-194) is commented out, so
  // the cache is only updated by addTailChange during compaction
  // replay (file-name remaps, not key-value pairs). If the write path
  // is ever re-enabled, add tail_cache->disable() here for kCorfu.
  this->lru_cache = new LRUCache(this->metadata->lru_cache_bytes,
                                 this->log_storage, this->sstable_storage);
  this->metadata_log = new MetadataLogHandler(this->metadata->metadata_log, this->log_storage, this->tail_cache);
  this->metadata_log->setLRUCache(this->lru_cache);
  this->metadata_log->setMetadata(this->metadata);
  // Enable the in-memory key index on all backends. On Corfu, the
  // storage layer's remote-append listener (see CorfuDBStorage and
  // LogHandler::onRemoteAppend) keeps the index in sync with peer
  // writes; on non-shared backends the writer path is the only
  // updater, so the listener hook defaults to a no-op.
  this->log_handler = new LogHandler(
      this->metadata->log_file_size_limit, this->metadata->log_prefix,
      this->log_storage, lru_cache, metadata_log, /*enable_key_index=*/true,
      /*key_index_capacity=*/1000000,
      /*trust_background_tail=*/this->metadata->trust_background_tail,
      /*linearizable_reads=*/this->metadata->linearizable_reads);
  this->sstable_handler = new SSTableHandler(this->sstable_storage, metadata_log, this->metadata->sstable_level_prefix, lru_cache);
  this->sstable_handler->setMaxLevel(this->metadata->max_level);
  this->thread_pool = new ThreadPool(std::thread::hardware_concurrency());
  this->log_handler->setThreadPool(this->thread_pool);
  this->sstable_handler->setThreadPool(this->thread_pool);
  std::string fingerprint = generateFingerprint();
  this->watcher = new CompactionWatcher(this->metadata, this->log_storage, this->log_handler, metadata_log, this->sstable_handler, fingerprint);
  this->watcher->setSSTableStorage(this->sstable_storage);
  this->watcher->setLRUCache(this->lru_cache);
  this->watcher->setTaskLogHandler(new TaskLogHandler(this->metadata->task_log, this->log_storage));
  this->watcher->setMode(this->mode);
  this->file_mutex_manager = new FileMutexManager();
  this->lru_cache->setFileMutexManager(this->file_mutex_manager);
  this->watcher->setFileMutexManager(this->file_mutex_manager);
  this->watcher->setThreadPool(this->thread_pool);
  srand(std::hash<std::string>{}(fingerprint));
};

DB::~DB() {
  delete this->watcher;
  delete this->sstable_handler;
  delete this->log_handler;
  delete this->lru_cache;
  delete this->metadata_log;
  delete this->tail_cache;
  // Guard against double-free when SSTable storage isn't split off the
  // log backend (the legacy single-storage case where both pointers
  // alias). When split, the SSTable backend is destroyed first to flush
  // any pending PutObject requests before the log backend tears down.
  if (this->sstable_storage != this->log_storage) {
    delete this->sstable_storage;
  }
  delete this->log_storage;
  delete this->metadata;
  delete this->thread_pool;
  delete this->file_mutex_manager;
}

Status DB::openDB(DB*& db, std::string const& shared_config_path) {
  // init DB Logic
  db = new DB(shared_config_path);
  db->active = true;
  db->metadata_log->rollForwardMetadataLog();
  db->metadata_log->initSSTMetadata();
  // Seed the log key index from any log files already materialized in
  // the cache by rollforward. Set latest_view on both log_handler and
  // lru_cache first — warmKeyIndex calls cache->checkReadMoreLog which
  // dereferences the cache's latest_view pointer. Pin the snapshot on
  // the DB so the raw pointers handed to children stay valid past
  // openDB.
  db->latest_view_snapshot = db->metadata_log->latestViewSnapshot();
  View const* view_ptr = db->latest_view_snapshot.get();
  db->log_handler->setLatestView(view_ptr);
  db->lru_cache->setLatestView(view_ptr);
  db->log_handler->warmKeyIndex();
  if (db->metadata->compaction_policy == CompactionPolicy::kHoAl) {
    // only use in the case of HoAl and HeAl
    db->watcher->startCompactionWatcher(&(db->active));
  }
  db->metadata_log->startViewUpdate(&(db->active));
  return Status::kSuccess;
}

Status DB::closeDB(DB*& db) {
  db->active = false;
  if (db->metadata->compaction_policy == CompactionPolicy::kHoAl) {
    db->watcher->stopCompactionWatcher();
  }
  // db->thread_pool->waitForCompletion();
  db->metadata_log->stopViewUpdate();
  db->lru_cache->printCacheStats();
  delete db;
  return Status::kSuccess;
}

Status DB::put(std::string const& key, std::string const& value) {
  Record record;
  record.set_key(key);
  record.set_value(value);
  record.set_type(kTypeValue);
  log_handler->addRecord(record);
  if (this->metadata->compaction_policy == CompactionPolicy::kHoSe) {
    counter++;
    if (counter < this->compaction_per_operation) {
      return Status::kSuccess;
    }
    counter = 0;

    auto random = static_cast<double>(rand() * 1.0 / RAND_MAX);
    double score = 0;
    metadata_log->getLatestScore(score);
    if (score == 0) {
      return Status::kSuccess;
    }
    double k = 1;
    double b = (this->metadata->max_level + 1) * 1.0;
    this->compaction_rate = (std::exp(k * score) - std::exp(k)) / (std::exp(k * b) - std::exp(k));
    if (random < this->compaction_rate) {
      bool has_worked_on_compaction = false;
      Compaction* compaction = nullptr;
      TaskRecord* task_record = nullptr;
      this->watcher->pickCompaction(compaction, task_record, has_worked_on_compaction);
      if (has_worked_on_compaction) {
        // push the task to thread pool
        this->thread_pool->enqueue([this, compaction, task_record]() {
          this->watcher->startCompaction(compaction, task_record);
          delete compaction;
          delete task_record;
        });
      }
    }
  }
  return Status::kSuccess;
}

Status DB::remove(std::string const& key) {
  Record record;
  record.set_key(key);
  record.set_type(kTypeDeletion);
  this->log_handler->addRecord(record);
  return Status::kSuccess;
}

Status DB::get(std::string const& key, std::string const*& value,
               std::shared_ptr<Record>& guard) {
  // Fast path: probe the log key index before refreshing the view.
  // When trust_background_tail is enabled, the index is authoritative
  // for log reads — kept current by local addRecord and, on Corfu, by
  // the tailer's onRemoteAppend callback. A hit here avoids the O(N)
  // View deep-copy at getLatestView, the tail_cache lock, and the
  // setLatestView calls below. Tombstones still terminate the read.
  if (this->metadata->trust_background_tail) {
    std::shared_ptr<Record> fast_hit;
    if (this->log_handler->tryIndexLookup(key, fast_hit)) {
      if (fast_hit->type() == kTypeDeletion) return Status::kFailure;
      value = &(fast_hit->value());
      guard = std::move(fast_hit);
      return Status::kSuccess;
    }
  }

  // Strict mode (linearizable_reads): ONE fence per get, taken here.
  // SyncScope samples the global log tail and waits for the tailer
  // once (the linearization point); it records a thread-local fence
  // token, so every fenced storage call below on this thread —
  // syncView's metadata-log reads, the tail probe in checkReadMoreLog,
  // the data-log reads (run inline in strict mode, see readRecord),
  // and the post-scan validation — reuses the token instead of paying
  // its own sequencer round-trip. After the fence, everything
  // sequenced before this get began is in local state, so those reads
  // need no further freshness. Without any of this the view lags by
  // the background thread's ~100ms cycle, which both hides a peer's
  // freshly rolled tail file and, during a peer compaction, opens a
  // window where a key is temporarily in neither the (removed) input
  // log file nor the (not-yet-visible) output SSTable.
  //
  // The post-scan check closes that compaction window against ops
  // sequenced *after* our fence: the tailer applies entries in address
  // order, so if it removed a log file under our scan, the COMPACT
  // that precedes the REMOVE has already grown the LOCAL metadata log
  // past view_offset — the (token-fenced, i.e. local) size() below
  // sees it and we retry against the fresh view. Ops not yet applied
  // locally cannot have disturbed a scan that only read local state,
  // which is also why retries do NOT re-fence: they only need to
  // ingest what the tailer already applied, and the original fence
  // remains a valid linearization point inside this get's window.
  // Metadata ops are rare (log rolls and compactions), so retries are
  // too; the cap only guards against a pathological storm, serving the
  // last result instead of spinning.
  bool const strict = this->metadata->linearizable_reads;
  std::optional<Storage::SyncScope> fence;
  if (strict) {
    fence.emplace(*log_storage);
  }
  constexpr int kStrictMaxAttempts = 5;
  for (int attempt = 0;; ++attempt) {
    size_t view_offset = 0;
    if (strict) {
      view_offset = metadata_log->syncView();
    }

    // Atomic snapshot of the metadata-log view — no deep copy, no lock.
    // Held locally so children's raw latest_view pointers stay valid
    // until this frame returns. Refresh publishes happen only on log
    // rollforward / tail-size refresh, not per get.
    this->latest_view_snapshot = metadata_log->latestViewSnapshot();
    View const* view_ptr = this->latest_view_snapshot.get();
    if (view_ptr == nullptr) {
      return Status::kFailure;
    }
    this->log_handler->setLatestView(view_ptr);
    this->sstable_handler->setLatestView(view_ptr);
    this->lru_cache->setLatestView(view_ptr);

    // tail_cache path removed — TailCache::getLatestRecord was dead
    // code: the write side that populated it (db.cpp:~190 in put)
    // was commented out, so every call was a shared-lock + empty
    // hashmap lookup. The `offset` we used to feed into readRecord
    // and readRecordFromAllLevel came from that dead cache and was
    // always empty, so drop both the call and the condition.

    std::shared_ptr<Record> log_record;
    std::string const empty_offset;
    std::string latest_offset;
    log_handler->readRecord(key, log_record, empty_offset, latest_offset);

    std::shared_ptr<Record> latest_record;
    if (log_record) {
      latest_record = std::move(log_record);
    } else {
      std::shared_ptr<Record> sstable_record;
      sstable_handler->readRecordFromAllLevel(key, sstable_record, empty_offset);
      if (sstable_record) {
        latest_record = std::move(sstable_record);
      }
    }

    if (strict && attempt + 1 < kStrictMaxAttempts &&
        log_storage->size(this->metadata->metadata_log) != view_offset) {
      continue;
    }

    if (latest_record) {
      if (latest_record->type() == kTypeDeletion) {
        return Status::kFailure;
      }
      value = &(latest_record->value());
      guard = std::move(latest_record);
      return Status::kSuccess;
    }
    return Status::kFailure;
  }
}
}  // namespace ozonedb
