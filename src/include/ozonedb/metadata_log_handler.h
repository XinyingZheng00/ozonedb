#ifndef METADATA_LOG_HANDLER_H
#define METADATA_LOG_HANDLER_H
#include "cache.h"
#include "helper.h"
#include "metadata.h"
#include "protobuf/sstable.pb.h"
#include "protobuf_serializer.h"
#include "storage/file_storage.h"
#include "storage/shared_log_storage.h"
#include <queue>
#include <string>
#include <vector>

namespace ozonedb {
class EventListener;

// Strategy pattern for updating view
class LogTailUpdater {
 public:
  virtual void updateViewTail(View& latest_view) = 0;
  virtual ~LogTailUpdater() = default;
};

class SharedLogLogTailUpdater : public LogTailUpdater {
 public:
  SharedLogStorage* log_storage;
  int predefined_shared_log_segment_size;  // todo fix predefine size to be put in storage layer
  SharedLogLogTailUpdater(Storage* s, int predefined_shared_log_segment_size)
      : predefined_shared_log_segment_size(predefined_shared_log_segment_size) {
    this->log_storage = dynamic_cast<SharedLogStorage*>(s);
    if (!this->log_storage) {
      throw std::runtime_error("Storage pointer is not a SharedLogStorage");
    }
  }

  void updateViewTail(View& latest_view) override {
    int log_size = log_storage->size("");
    if (log_size == 0) return;

    int start_offset = 0;
    int end_offset = 0;
    if (!latest_view.current_log_tail.empty()) {
      start_offset = getStartOffset(latest_view.current_log_tail);
      end_offset = getEndOffset(latest_view.current_log_tail);
      latest_view.file_size[latest_view.current_log_tail] = std::min(predefined_shared_log_segment_size, log_size - start_offset);
    }

    while (log_size > end_offset) {
      std::string name = "sharedlog/" + std::to_string(end_offset) + ":" + std::to_string(end_offset + predefined_shared_log_segment_size);
      std::cout << "push file name: " << name << std::endl;
      latest_view.current_log_tail = name;
      latest_view.storage_layout["sharedlog"].push_back(name);
      latest_view.file_size[name] =
          std::min(predefined_shared_log_segment_size, log_size - end_offset);
      end_offset += predefined_shared_log_segment_size;
    }
  }
};

class FileSystemLogTailUpdater : public LogTailUpdater {
 public:
  FileStorage* log_storage;
  explicit FileSystemLogTailUpdater(Storage* s) {
    this->log_storage = dynamic_cast<FileStorage*>(s);
    if (!this->log_storage) {
      throw std::runtime_error("Storage pointer is not a FileStorage");
    }
  }

  void updateViewTail(View& latest_view) override {
    if (!latest_view.current_log_tail.empty()) {
      latest_view.file_size[latest_view.current_log_tail] =
          log_storage->size(latest_view.current_log_tail);
    }
  }
};

class MetadataLogHandler {
 private:
  size_t offset;
  std::string active_unit;
  Storage* metadata_log_storage = nullptr;
  Storage* log_storage = nullptr;
  FileStorage* sst_storage = nullptr;
  LogTailUpdater* log_tail_updater = nullptr;

  TailCache* tail_cache = nullptr;
  LRUCache* lru_cache = nullptr;
  std::thread* update_view_thread = nullptr;
  View latest_view_w;
  View latest_view_r;

  Metadata* metadata = nullptr;
  std::shared_mutex view_mutex_w;
  std::shared_mutex view_mutex_r;
  std::shared_mutex read_mutex;
  std::unordered_map<std::string, std::priority_queue<std::pair<int, OperationRecord*>, std::vector<std::pair<int, OperationRecord*>>, std::greater<>>> buffer;

  std::vector<OperationRecord*> readMetadataLog();
  std::string rollforwardSingleOperationRecord(OperationRecord* record);

 public:
  EventListener* event_listener = nullptr;
  MetadataLogHandler(std::string metadata_log, Storage* metadata_log_storage, Storage* log_storage, FileStorage* sst_storage, TailCache* tail_cache, LRUCache* lru_cache) {
    this->offset = 0;
    this->metadata_log_storage = metadata_log_storage;
    this->log_storage = log_storage;
    this->sst_storage = sst_storage;
    this->active_unit = metadata_log;
    this->tail_cache = tail_cache;
    this->lru_cache = lru_cache;
    if (metadata_log.find("sharedlog") != std::string::npos) {
      this->log_tail_updater = new SharedLogLogTailUpdater(log_storage, 64);  // need to be changed to be a parameter
    } else {
      this->log_tail_updater = new FileSystemLogTailUpdater(log_storage);
    }
  };
  void setEventListener(EventListener* event_listener) { this->event_listener = event_listener; }

  ~MetadataLogHandler(){};

  void rollForwardMetadataLogPeriodically(std::atomic<bool> const* active);

  View rollForwardMetadataLog();

  void initSSTMetadata();

  void getLatestView(View& view);

  void flushLatestView();

  // void getLatestScore(double& score);

  // double getLevelScore(std::string level_prefix);

  // Serialize a OperationRecord to a file
  void appendToMetadataLog(OperationRecord const& record);

  // Start a thread to roll forward the metadata log periodically
  Status startViewUpdate(std::atomic<bool> const* active);

  Status stopViewUpdate();
};

}  // namespace ozonedb
#endif