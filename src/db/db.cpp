#include "db.h"
#include "helper.h"
#include "thread_pool.h"
#include <algorithm>
#include <cmath>
namespace ozonedb {

DB::DB(std::string const& shared_config_path) {
  this->metadata = new Metadata(shared_config_path);

  if (this->metadata->storage_type == StorageType::kFileStorage) {
    FileStorage* fs = new FileStorage(this->metadata->DBdir);
    this->log_storage = fs;
    this->sstable_storage = fs;
    this->metadatalog_storage = fs;
    this->tasklog_storage = fs;
  } else if (this->metadata->storage_type == StorageType::kAzureBlobStorage) {
    // this->storage = new AzureBlobStorage("DefaultEndpointsProtocol=https;AccountName=ozonedbstorage;AccountKey=vp7eifiiqeHobq0nFpHv6MOI/J53UXgOKYxg0xIwOQj0NHe2cbOcVmdtgh6KE/9cu2UU9z3oPjvI+AStoe1A2Q==;EndpointSuffix=core.windows.net", this->metadata->container_name, this->metadata->DBdir);
  } else if (this->metadata->storage_type == StorageType::kSharedLogStorage) {
    this->log_storage = new SharedLogStorage("datalog");
    this->sstable_storage = new FileStorage(this->metadata->DBdir);
    this->metadatalog_storage = new SharedLogStorage("metadatalog");
    this->tasklog_storage = new SharedLogStorage("tasklog");
  } else {
    std::cerr << "Invalid storage type" << std::endl;
    exit(1);
  }
  this->thread_pool = new ThreadPool(this->metadata->log_file_number_limit);  // think about the number of threads in the embedded case
  this->lru_cache = new LRUCache(this->metadata->log_file_number_limit, this->metadata->block_cache_capacity,
                                 this->log_storage, this->sstable_storage);
  this->tail_cache = new TailCache();

  this->metadata_log_handler = new MetadataLogHandler(this->metadata->metadata_log_path, this->metadatalog_storage,
                                                      this->log_storage, this->sstable_storage, this->tail_cache, this->lru_cache);
  if (this->metadata->storage_type == StorageType::kSharedLogStorage) {
    this->data_log_handler = new DataLogHandler(this->lru_cache, this->thread_pool, this->metadata->storage_type);
  } else {
    this->data_log_handler = new DataLogHandler(this->metadata->DBdir, this->metadata->log_file_size_limit, this->metadata->data_log_prefix,
                                                this->lru_cache, this->metadata_log_handler, this->thread_pool, this->metadata->storage_type);
  }
  this->sstable_handler = new SSTableHandler(this->metadata->sstable_level_prefix, this->lru_cache, this->metadata->max_level);

  std::string fingerprint = generateFingerprint();

  this->watcher = new CompactionWatcher(this->metadata, this->log_storage, new FileStorage(this->metadata->DBdir), this->data_log_handler,
                                        this->metadata_log_handler, this->sstable_handler, fingerprint, this->tasklog_storage);

  srand(std::hash<std::string>{}(fingerprint));
};

DB::~DB() {
  delete this->watcher;
  delete this->sstable_handler;
  delete this->data_log_handler;
  delete this->lru_cache;
  delete this->metadata_log_handler;
  delete this->tail_cache;
  if (this->metadata->storage_type == StorageType::kFileStorage) {
    delete this->sstable_storage;
    this->log_storage = nullptr;
    this->metadatalog_storage = nullptr;
    this->tasklog_storage = nullptr;
  } else if (this->metadata->storage_type == StorageType::kAzureBlobStorage) {
    // delete this->storage;
  } else if (this->metadata->storage_type == StorageType::kSharedLogStorage) {
    delete this->log_storage;
    delete this->sstable_storage;
    delete this->metadatalog_storage;
    delete this->tasklog_storage;
  }
  delete this->metadata;
  delete this->thread_pool;
}

Status DB::openDB(DB*& db, std::string const& shared_config_path) {
  // init DB Logic
  db = new DB(shared_config_path);
  db->active = true;
  db->metadata_log_handler->rollForwardMetadataLog();
  db->metadata_log_handler->initSSTMetadata();
  if (db->metadata->compaction_policy == CompactionPolicy::kHoAl) {
    // only use in the case of HoAl and HeAl
    db->watcher->startCompactionWatcher(&(db->active));
  }
  db->metadata_log_handler->startViewUpdate(&(db->active));
  return Status::kSuccess;
}

Status DB::closeDB(DB*& db) {
  db->active = false;
  if (db->metadata->compaction_policy == CompactionPolicy::kHoAl) {
    db->watcher->stopCompactionWatcher();
  }
  // db->thread_pool->waitForCompletion();
  // db->metadata_log->stopViewUpdate();
  delete db;
  return Status::kSuccess;
}

Status DB::put(std::string const& key, std::string const& value) {
  Record record;
  record.set_key(key);
  record.set_value(value);
  record.set_type(kTypeValue);
  data_log_handler->addRecord(record);
  // if (this->metadata->compaction_policy == CompactionPolicy::kHoSe) {
  //   counter++;
  //   if (counter < this->compaction_per_operation) {
  //     return Status::kSuccess;
  //   }
  //   counter = 0;

  //   auto random = static_cast<double>(rand() * 1.0 / RAND_MAX);
  //   double score = 0;
  //   metadata_log->getLatestScore(score);
  //   if (score == 0) {
  //     return Status::kSuccess;
  //   }
  //   double k = 1;
  //   double b = (this->metadata->max_level + 1) * 1.0;
  //   this->compaction_rate = (std::exp(k * score) - std::exp(k)) / (std::exp(k * b) - std::exp(k));
  //   if (random < this->compaction_rate) {
  //     bool has_worked_on_compaction = false;
  //     Compaction* compaction = nullptr;
  //     TaskRecord* task_record = nullptr;
  //     this->watcher->pickCompaction(compaction, task_record, has_worked_on_compaction);
  //     if (has_worked_on_compaction) {
  //       // push the task to thread pool
  //       this->thread_pool->enqueue([this, compaction, task_record]() {
  //         this->watcher->startCompaction(compaction, task_record);
  //         delete compaction;
  //         delete task_record;
  //       });
  //     }
  //   }
  // }
  return Status::kSuccess;
}

Status DB::remove(std::string const& key) {
  Record record;
  record.set_key(key);
  record.set_type(kTypeDeletion);
  this->data_log_handler->addRecord(record);
  return Status::kSuccess;
}

Status DB::get(std::string const& key, std::string const*& value) {
  // Step1: get the latest view and cache state from metadata log
  // metadata_log->rollForwardMetadataLog();
  metadata_log_handler->getLatestView(this->latest_view);
  this->data_log_handler->setLatestView(&this->latest_view);
  this->sstable_handler->setLatestView(&this->latest_view);
  this->lru_cache->setLatestView(&this->latest_view);
  std::pair<Record*, std::string> entry;
  bool hit = tail_cache->getLatestRecord(key, entry);  // go to loghandler

  // Step2: get the latest record from log, sstable
  Record* latest_record = nullptr;

  Record* cached_record = nullptr;
  std::string offset;
  bool updated = false;
  if (hit) {
    cached_record = entry.first;
    offset = entry.second;
    // std::cout << "db cache hit! Latest offset is " << offset << std::endl;
  }
  latest_record = cached_record;

  // Read from log, from new to old.
  Record* log_record = nullptr;
  std::string latest_offset;
  data_log_handler->readRecord(key, log_record, offset, latest_offset);
  if (log_record) {
    latest_record = log_record;
    updated = true;
  }

  // If the latest file is in sstable and no record in log layer,
  // read from sstable, from new to old.
  Record* sstable_record = nullptr;
  std::string prefix = this->metadata->sstable_level_prefix;
  if ((cached_record == nullptr || offset.substr(0, prefix.size()) == prefix) && log_record == nullptr) {
    sstable_handler->readRecordFromAllLevel(key, sstable_record, offset);
    if (sstable_record) {
      updated = true;
      latest_record = sstable_record;
    }
  }

  // if (updated) {
  //   // Perform cache update asynchronously to avoid blocking the critical path
  //   this->thread_pool->enqueue([this, key, latest_record, latest_offset, offset]() {
  //       tail_cache->updateCache(key, latest_record, latest_offset, offset);
  //   });
  // }

  if (latest_record) {
    if (latest_record->type() == kTypeDeletion) {
      return Status::kFailure;
    }
    value = &(latest_record->value());
    return Status::kSuccess;
  }
  return Status::kFailure;
}
}  // namespace ozonedb