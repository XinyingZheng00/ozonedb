
#ifndef METADATA_LOG_HANDLER_H
#define METADATA_LOG_HANDLER_H
#include "cache.h"
#include "metadata.h"
#include "protobuf/sstable.pb.h"
#include "protobuf_serializer.h"
#include "storage.h"
#include <queue>
#include <string>
#include <vector>

namespace ozonedb {
class EventListener;
class View {
 public:
  std::unordered_map<std::string, std::deque<std::string>> storage_layout;
  std::unordered_map<std::string, std::pair<std::string, std::string>> key_range;
  std::unordered_map<std::string, size_t> file_size;
  std::string current_log_tail;

  int getFileNumber(std::string const& prefix);

  std::deque<std::string> getWithPrefix(std::string const& prefix);

  std::pair<std::string, std::string> getKeyRange(std::string const& file_name);

  size_t getFileSize(std::string const& file_name);

  std::string getCurrentLogTail() { return current_log_tail; }
};

class MetadataLogHandler {
 private:
  size_t offset;
  std::string active_unit;
  Storage* storage = nullptr;  // still using storage in sharedlog case for reading sstable
#ifdef SHARED_LOG
  size_t predefined_shared_log_segment_size = 32768;
  SharedLogStorage* metadata_sharedlog_storage = nullptr;
  SharedLogStorage* data_sharedlog_storage = nullptr;

 public:
  void setMetadataSharedLogStorage(SharedLogStorage* metadata_sharedlog_storage) {
    this->metadata_sharedlog_storage = metadata_sharedlog_storage;
  }
  void setDataSharedLogStorage(SharedLogStorage* data_sharedlog_storage) {
    this->data_sharedlog_storage = data_sharedlog_storage;
  }
  void setPredefinedSharedLogSegmentSize(size_t predefined_shared_log_segment_size) {
    this->predefined_shared_log_segment_size = predefined_shared_log_segment_size;
  }

 private:
#endif
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

  // Deserialize OperationRecords from a file
  std::vector<OperationRecord*> readMetadataLog();
  std::string rollforwardSingleOperationRecord(OperationRecord* record);

 public:
  EventListener* event_listener = nullptr;
  MetadataLogHandler(std::string metadata_log, Storage* storage, TailCache* tail_cache) {
    this->offset = 0;
    this->storage = storage;
    this->active_unit = metadata_log;
    this->tail_cache = tail_cache;
  };
  void setEventListener(EventListener* event_listener) { this->event_listener = event_listener; }

  void setLRUCache(LRUCache* lru_cache) { this->lru_cache = lru_cache; }

  void setMetadata(Metadata* metadata) { this->metadata = metadata; }

  ~MetadataLogHandler(){};

  void rollForwardMetadataLogPeriodically(std::atomic<bool> const* active);

  View rollForwardMetadataLog();

  void initSSTMetadata();

  void getLatestView(View& view);

  void flushLatestView();

  void getLatestScore(double& score);

  // double getLevelScore(std::string level_prefix);
  // Serialize a OperationRecord to a file
  void appendToMetadataLog(OperationRecord const& record);

  // Start a thread to roll forward the metadata log periodically
  Status startViewUpdate(std::atomic<bool> const* active);

  Status stopViewUpdate();
};
}  // namespace ozonedb
#endif