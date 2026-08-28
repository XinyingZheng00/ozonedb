
#ifndef METADATA_LOG_HANDLER_H
#define METADATA_LOG_HANDLER_H
#include "cache.h"
#include "metadata.h"
#include "protobuf/sstable.pb.h"
#include "protobuf_serializer.h"
#include "storage.h"
#include <atomic>
#include <map>
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
  // Contiguous metadata-log prefix whose records have been APPLIED into
  // latest_view; guarded by view_mutex. Distinct from `offset` (guarded
  // by read_mutex), which readMetadataLog commits BEFORE its caller
  // applies the batch — in that window the view lags the offset, so
  // syncView must gate on this, not on `offset`, to guarantee the
  // published snapshot covers its fence target. Batches can apply out
  // of order (reader A commits [0,L1), is preempted, reader B applies
  // [L1,L2) first); applied_gaps_ parks such ranges until the watermark
  // reaches them, so the watermark never overclaims an unapplied hole.
  size_t applied_offset_ = 0;
  std::map<size_t, size_t> applied_gaps_;
  // Record that [start, end) has been applied into latest_view and
  // advance applied_offset_ over any now-contiguous parked ranges.
  // Caller must hold view_mutex as a unique_lock.
  void markAppliedLocked(size_t start, size_t end);
  Metadata* metadata = nullptr;
  std::shared_mutex view_mutex;
  std::shared_mutex read_mutex;
  std::unordered_map<std::string, std::priority_queue<std::pair<int, OperationRecord*>, std::vector<std::pair<int, OperationRecord*>>, std::greater<>>> buffer;

  // Deserialize OperationRecords from a file. When `observed_size` /
  // `batch_start` are non-null they receive the metadata-log length
  // this call saw (0 when the log does not exist yet) and the offset
  // the returned batch begins at — i.e. the batch spans
  // [batch_start, observed_size). On Corfu that size() is fenced on the
  // global tail, so the first observation doubles as syncView's fence.
  std::vector<OperationRecord*> readMetadataLog(size_t* observed_size = nullptr,
                                                size_t* batch_start = nullptr);
  std::string rollforwardSingleOperationRecord(OperationRecord* record);
  // Refresh latest_view.tail_size without holding view_mutex across
  // storage->size() — keeps the Corfu fence out of the critical section.
  void refreshTailSizeUnlocked();
  // Publish a fresh shared_ptr<View const> snapshot from the current
  // latest_view. Caller must hold view_mutex as a unique_lock.
  void publishSnapshotLocked();

  // Cache work owed for COMPACT records applied to the view
  // (bench/PLAN-compaction-cache.md, parts A and B). The apply runs under
  // unique_lock<view_mutex>, and the cache mutex must never be taken
  // there: the Corfu tailer's listener takes the cache mutex, and a
  // foreground thread may hold it while fencing on the tailer. So both
  // COMPACT branches queue an event (guarded by view_mutex) and the two
  // callers drain the queue once their lock scope has ended. The atomic
  // flag lets syncView, which strict reads call per get, skip the
  // unique_lock when there is nothing to drain.
  struct CompactionEvent {
    std::vector<std::string> inputs;
    std::vector<std::string> outputs;
    std::vector<size_t> output_bytes;
    int dest_level = 0;
  };
  std::vector<CompactionEvent> pending_cache_events_;
  std::atomic<bool> has_pending_cache_events_{false};
  void queueCacheEventLocked(OperationRecord const* record);
  void drainCacheEvents();

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

  // Synchronous rollforward with no View copy: ingests every
  // LOGCREATE/COMPACT the storage layer can currently see, closing the
  // ~100ms window of the background view thread. Fencing is the
  // storage layer's business: with no fence token on the calling
  // thread, readMetadataLog's storage calls each fence on the global
  // sequencer tail; under a Storage::SyncScope (the strict DB::get
  // path) they reuse the caller's single fence and read local state.
  // Either way the resulting view covers everything sequenced before
  // the caller's fence point. This is the per-get hook for
  // Metadata::linearizable_reads; unlike rollForwardMetadataLog it
  // avoids the O(files) return-by-value copy (a known hot-path
  // regression) and skips the tail-size refresh (the strict read path
  // re-fences the tail itself in checkReadMoreLog).
  //
  // Loops until the committed offset covers the log length sampled at
  // entry: readMetadataLog's offset-CAS means a racing caller can lose
  // its batch to a concurrent winner and come back empty-handed while
  // the view is still short of this caller's fence — tolerable for the
  // background thread, not for a linearizable get. Returns the
  // committed offset the published view reflects; DB::get compares it
  // against a post-scan fenced log length to detect a compaction that
  // landed mid-scan.
  size_t syncView();

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