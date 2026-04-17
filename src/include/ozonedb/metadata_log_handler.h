
#ifndef METADATA_LOG_HANDLER_H
#define METADATA_LOG_HANDLER_H
#include "cache.h"
#include "metadata.h"
#include "protobuf/sstable.pb.h"
#include "protobuf_serializer.h"
#include "storage.h"
#include <atomic>
#include <memory>
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
  size_t tail_size = 0;

  // All accessors are const and non-mutating so a View can be held
  // immutably behind a shared_ptr<View const> and read concurrently
  // from many threads without copying. Prior versions used
  // operator[] which silently inserted default entries, making the
  // View unsafe to share across threads.
  int getFileNumber(std::string const& prefix) const;

  // Returns a reference into the map (no copy). On miss returns a
  // reference to a shared static empty deque so callers can still
  // write `auto const& files = view.getWithPrefix(p);` without
  // special-casing the not-found path. Only valid as long as the
  // caller holds a reference to the View (i.e. the enclosing
  // shared_ptr is alive).
  std::deque<std::string> const& getWithPrefix(std::string const& prefix) const;

  std::pair<std::string, std::string> getKeyRange(std::string const& file_name) const;

  size_t getFileSize(std::string const& file_name) const;

  size_t getTailSize() const { return tail_size; }

  std::string const& getCurrentLogTail() const { return current_log_tail; }
};

class MetadataLogHandler {
 private:
  size_t offset;
  std::string active_unit;
  Storage* storage = nullptr;
  TailCache* tail_cache = nullptr;
  LRUCache* lru_cache = nullptr;
  std::thread* update_view_thread = nullptr;
  View latest_view;
  // Immutable snapshot of latest_view, swapped atomically by
  // publishSnapshotLocked after each mutation. Readers (DB::get hot
  // path) atomically load this instead of deep-copying latest_view
  // under a shared_lock per call — converts an O(files) per-get copy
  // into a refcount bump. std::atomic free functions on
  // std::shared_ptr are used; C++20's atomic<shared_ptr> isn't
  // required.
  std::shared_ptr<View const> latest_snapshot_;
  Metadata* metadata = nullptr;
  std::shared_mutex view_mutex;
  std::shared_mutex read_mutex;
  std::unordered_map<std::string, std::priority_queue<std::pair<int, OperationRecord*>, std::vector<std::pair<int, OperationRecord*>>, std::greater<>>> buffer;

  // Deserialize OperationRecords from a file
  std::vector<OperationRecord*> readMetadataLog();
  std::string rollforwardSingleOperationRecord(OperationRecord* record);
  // Refresh latest_view.tail_size without holding view_mutex across
  // storage->size() — keeps the Corfu fence out of the critical section.
  void refreshTailSizeUnlocked();
  // Publish a fresh shared_ptr<View const> snapshot from the current
  // latest_view. Caller must hold view_mutex as a unique_lock.
  void publishSnapshotLocked();

 public:
  EventListener* event_listener = nullptr;
  MetadataLogHandler(std::string metadata_log, Storage* storage, TailCache* tail_cache) {
    this->offset = 0;
    this->storage = storage;
    this->active_unit = metadata_log;
    this->tail_cache = tail_cache;
  };
  // set event listener
  void setEventListener(EventListener* event_listener) { this->event_listener = event_listener; }
  
  // set lru cache
  void setLRUCache(LRUCache* lru_cache) { this->lru_cache = lru_cache; }

  // set metadata
  void setMetadata(Metadata* metadata) { this->metadata = metadata; }

  ~MetadataLogHandler(){};
  // get view mutex
  std::shared_mutex& getViewMutex() { return view_mutex; }

  // used by get module
  void rollForwardMetadataLogPeriodically(std::atomic<bool> const* active);

  // used by compaction module and put module
  View rollForwardMetadataLog();

  void initSSTMetadata();

  void getLatestView(View& view);

  // Atomic load of the latest View snapshot. Returns a shared_ptr by
  // value; the refcount bump keeps the View alive for the caller's
  // use regardless of subsequent mutations. Preferred over
  // getLatestView for read-only hot paths — no lock, no copy.
  std::shared_ptr<View const> latestViewSnapshot() const;

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