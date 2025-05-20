#ifndef DB_H
#define DB_H

#include "cache.h"
#include "compaction.h"
#include "data_log_handler.h"
#include "event_listener.h"
#include "protobuf/record.pb.h"
#include "sstable/sstable_handler.h"
#include <thread>
#include <assert.h>

namespace ozonedb {

class DB {
 private:
  std::atomic<bool> active;
  int compaction_per_operation = 10;
  int counter = 0;

  Storage* log_storage;
  FileStorage* sstable_storage;
  Storage* metadatalog_storage;
  Storage* tasklog_storage;

  Metadata* metadata;
  CompactionWatcher* watcher = nullptr;
  View latest_view;
  std::mutex db_mutex;

  MetadataLogHandler* metadata_log_handler = nullptr;
  DataLogHandler* data_log_handler = nullptr;
  SSTableHandler* sstable_handler = nullptr;
  TailCache* tail_cache = nullptr;
  LRUCache* lru_cache = nullptr;
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

  // set event listener
  void setEventListener(EventListener* event_listener) {
    this->event_listener = event_listener;
    this->watcher->setEventListener(event_listener);
    this->metadata_log_handler->setEventListener(event_listener);
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
   * @param key
   * @param value
   * @return Status
   */
  Status get(std::string const& key, std::string const*& value);
};
}  // namespace ozonedb
#endif  // DB_H

// Concurrent put get.