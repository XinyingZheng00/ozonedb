#ifndef DB_H
#define DB_H

#include "cache.h"
#include "compaction.h"
#include "log_handler.h"
#include "protobuf/record.pb.h"
#include "sstable/sstable_handler.h"
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
  View latest_view;
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
   * @param key
   * @param value
   * @return Status
   */
  Status get(std::string const& key, std::string const*& value);
};
}  // namespace ozonedb
#endif  // DB_H

// Concurrent put get.