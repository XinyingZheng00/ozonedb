#ifndef LOG_HANDLER_H
#define LOG_HANDLER_H

#include "cache.h"
#include "metadata.h"
#include "metadata_log_handler.h"
#include "protobuf/record.pb.h"
#include "protobuf_serializer.h"
#include "storage.h"
#include "thread_pool.h"
#include <list>
#include <string>
#include <vector>

namespace ozonedb {
class LogHandler {
 private:
  /**
   * @brief prefix for each file
   * for datalog, it is in "/tmp/db/datafile/datalog"
   * for leveled sstable, it is "/tmp/db/datafile/level_x/"
   * for task, it is "/tmp/db/task"
   */
  std::string prefix;
  std::string active_unit;
  View* latest_view = nullptr;
  size_t file_size_limit;
  Storage* storage = nullptr;
#ifdef SHARED_LOG
  SharedLogStorage* sharedlog_storage = nullptr;

 public:
  void setSharedLogStorage(SharedLogStorage* sharedlog_storage) {
    this->sharedlog_storage = sharedlog_storage;
  }

 private:
#endif
  LRUCache* cache = nullptr;
  MetadataLogHandler* metadata_log = nullptr;
  ThreadPool* thread_pool = nullptr;

  /**
   * @brief create a new unit in this layer
   *
   * if reached the end of the file or the file is already sealed, go to next log file
   *
   * @return Status
   */
  Status newTail();

 public:
  /**
   * @brief Construct a new Handler object
   *
   * @param file_size_limit
   * @param log_prefix
   * @param storage
   */
  LogHandler(uint64_t file_size_limit, std::string log_prefix, Storage* storage, LRUCache* global_cache, MetadataLogHandler* metadata_log = nullptr)
      : file_size_limit(file_size_limit), prefix(std::move(log_prefix)), storage(storage) {
    if (storage != nullptr) {
      storage->createDirectory(prefix);
    }
    cache = global_cache;
    this->metadata_log = metadata_log;
  };

  ~LogHandler(){};

  // set latest view
  void setLatestView(View* view) { latest_view = view; }

  /**
   * @brief write record into this level
   *
   * @param record
   * @return Status
   */
  Status addRecord(Record const& record);

  /**
   * @brief read record from this level, end to the offset
   *
   * @param key
   * @param value
   * @return Status
   */
  Status readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset);

  // set thread pool
  void setThreadPool(ThreadPool* thread_pool) { this->thread_pool = thread_pool; }
  // after seal the file, we may create metadata for the file
};
}  // namespace ozonedb
#endif  // LOG_H