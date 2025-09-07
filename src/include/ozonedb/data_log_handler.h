#ifndef LOG_HANDLER_H
#define LOG_HANDLER_H

#include "cache.h"
#include "event_listener.h"
#include "metadata.h"
#include "metadata_log_handler.h"
#include "ozonedb_common.h"
#include "protobuf/record.pb.h"
#include "protobuf_serializer.h"
#include "storage/storage.h"
#include "thread_pool.h"
#include <list>
#include <string>
#include <vector>

namespace ozonedb {

Storage* getThreadLocalStorage(std::string const& DBdir, StorageType storage_type);

class RecordAppender {
 public:
  virtual Status append(Record const& record) = 0;
  virtual ~RecordAppender() = default;
};

class SharedLogRecordAppender : public RecordAppender {
  inline static thread_local std::unique_ptr<SharedLogStorage> log_storage = nullptr;

 public:
  SharedLogRecordAppender(){};

  Status append(Record const& record) override {
    if (!log_storage) {
      log_storage = std::make_unique<SharedLogStorage>("datalog");
    }
    size_t size;
    return log_storage->append("", record, size);  // Simple append
  }
};

class FileRecordAppender : public RecordAppender {
  std::string DBdir;
  inline static thread_local std::unique_ptr<FileStorage> log_storage = nullptr;
  MetadataLogHandler* metadata_log_handler;
  std::string log_prefix;
  size_t file_size_limit;
  std::string active_unit;

 public:
  FileRecordAppender(std::string const& DBdir,
                     MetadataLogHandler* metadata_log_handler,
                     std::string log_prefix,
                     size_t file_size_limit)
      : DBdir(DBdir),
        metadata_log_handler(metadata_log_handler),
        log_prefix(std::move(log_prefix)),
        file_size_limit(file_size_limit) {
  }
  ~FileRecordAppender() {
    this->log_storage.reset();
  }

  Status newTail() {
    if (!this->active_unit.empty()) {
      this->log_storage->seal(this->active_unit);
    }

    View view;
    this->metadata_log_handler->getLatestView(view);
    std::string current_tail = view.current_log_tail;

    if (!current_tail.empty() && view.getFileSize(current_tail) < this->file_size_limit) {
      this->active_unit = current_tail;
      return Status::kSuccess;
    }
    int log_number = current_tail.empty() ? 0 : std::stoi(getSuffix(current_tail));

    std::string name = this->log_prefix + "/" + std::to_string(log_number + 1);
    OperationRecord record;
    record.set_op_type(OperationRecord::LOGCREATE);
    record.add_input_files(current_tail);
    record.add_output_file(name);
    this->metadata_log_handler->appendToMetadataLog(record);
    view = this->metadata_log_handler->rollForwardMetadataLog();
    this->active_unit = view.current_log_tail;

    if (this->metadata_log_handler->event_listener) {
      this->metadata_log_handler->event_listener->onNewTail();
    }

    return Status::kSuccess;
  }

 public:
  Status append(Record const& record) override {
    if (!this->log_storage) {
      this->log_storage = std::make_unique<FileStorage>(this->DBdir);
      this->log_storage->createDirectory(this->log_prefix);
    }
    if (this->active_unit.empty()) {
      newTail();
    }
    size_t size;
    while (this->log_storage->append(this->active_unit, record, size) == Status::kSealed) {
      newTail();
    }

    View view;
    metadata_log_handler->rollForwardMetadataLog();
    metadata_log_handler->getLatestView(view);

    while (view.getFileSize(this->active_unit) >= this->file_size_limit || view.current_log_tail != this->active_unit) {
      newTail();
      metadata_log_handler->getLatestView(view);
    }

    return Status::kSuccess;
  }
};

class DataLogHandler {
 private:
  std::string DBdir;
  std::string log_prefix;
  uint64_t file_size_limit;
  StorageType storage_type;
  MetadataLogHandler* metadata_log_handler;

  View* latest_view = nullptr;
  LRUCache* cache = nullptr;
  ThreadPool* thread_pool = nullptr;
  std::unique_ptr<RecordAppender> record_appender;

 public:
  DataLogHandler(std::string const& DBdir, uint64_t file_size_limit, std::string log_prefix, LRUCache* global_cache,
                 MetadataLogHandler* metadata_log_handler, ThreadPool* thread_pool, StorageType storage_type) {
    this->DBdir = DBdir;
    this->cache = global_cache;
    this->log_prefix = log_prefix;
    this->file_size_limit = file_size_limit;
    this->metadata_log_handler = metadata_log_handler;
    this->thread_pool = thread_pool;
    this->storage_type = storage_type;
    if (storage_type == StorageType::kSharedLogStorage) {
      this->record_appender = std::make_unique<SharedLogRecordAppender>();
    } else {
      this->record_appender = std::make_unique<FileRecordAppender>(DBdir, metadata_log_handler, log_prefix, file_size_limit);
    }
  };

  DataLogHandler(LRUCache* global_cache, ThreadPool* thread_pool, StorageType storage_type) {
    this->cache = global_cache;
    this->thread_pool = thread_pool;
    this->storage_type = storage_type;
  };

  ~DataLogHandler(){};

  void setLatestView(View* view) { latest_view = view; }

  Status addRecord(Record const& record);
  Status readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset);

 private:
  bool shouldReadMoreLog(std::string const& file_name, size_t& cached_offset, size_t& size);
  void fetchLogToCache(std::string const& file_name, size_t cached_offset, size_t size);
  Storage* getThreadLocalStorage(std::string const& DBdir, StorageType storage_type);
  // after seal the file, we may create metadata for the file
};

}  // namespace ozonedb
#endif  // LOG_H