// src/include/ozonedb/disk_cache_storage.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "frequency_sketch.h"
#include "storage.h"

namespace ozonedb {

// A read-through tier for immutable SSTable objects on a local disk, in
// front of the object store that holds them (bench/PLAN-disk-cache.md).
//
// Entries are whole files: "<dir>/<name>" is a complete local copy,
// "<dir>/<name>.part" is a write-through in progress, "<dir>/<name>.fillpart"
// is a background fill in progress. The fill never touches the write-through's
// stream or its .part file (and vice versa) so the two populators can never
// interleave bytes or race a discard/rename into a truncated "complete" file
// (found in review of PLAN-disk-cache T3). Only names with `prefix` are
// cached; everything else passes straight through to the backing store
// (checkpoint/LATEST is mutable and must never be cached).
//
// Population: the builder's write-through (append* + flush), a background
// fill on the first ranged-read miss (chunked ranged reads of the backing),
// and reconcile() at open. Eviction: LRU by file under capacity_bytes.
// Invalidation: remove() (this process compacted) and invalidate() (a peer
// compacted; wired to the COMPACT apply in MetadataLogHandler).
//
// A fill that is already in flight may publish a file a peer's COMPACT has
// just removed from the view. That is deliberate and harmless: SSTable
// content is immutable and a name is never reused, so the copy is either
// correct or dead, and a dead one is dropped by the LRU or by the next
// reconcile() at open. Nothing reads a file the view no longer names.
//
// Lock order and what each mutex covers:
//   * mtx_        - index_, lru_, current_bytes_ (the budget). Held across an
//                   unlink in eviction/erase, never across a backing-store
//                   call or any other file I/O.
//   * parts_mtx_  - parts_ (the write-through streams) and poisoned_parts_.
//                   Deliberately held across the part's create_directories,
//                   ofstream open, write, flush and remove, because those are
//                   what serialise two threads on one .part; never held across
//                   a backing-store call, and never while mtx_ is held (the
//                   publish path drops it before calling publishPartFile).
//   * fill_mtx_   - fill_queue_, queued_, the fill_* flags and fill_cv_. Never
//                   held across the fill's I/O: fillOne() runs unlocked.
//   * fill_lifecycle_mtx_ - serialises startFillWorker/stopFillWorker; held
//                   across fill_thread_.join(), which is why it is the
//                   outermost of the four.
//   * sketch_mtx_ - sketch_ (the TinyLFU counters). A leaf: taken under mtx_
//                   by mayTakeLocked(), and alone by recordAccess(). Never
//                   held across any I/O.
// Order: fill_lifecycle_mtx_ -> fill_mtx_ (start/stop take both, in that
// order). parts_mtx_ and mtx_ are never nested, in either direction, and
// neither is ever taken under fill_mtx_. mtx_ -> sketch_mtx_ (leaf).
// Nothing here takes a cache or view mutex; the DB calls the tier only
// outside view_mutex.
//
// The constructor throws std::runtime_error when `dir` cannot be created or
// is not writable: a tier configured onto an unusable path would otherwise
// degrade into a pure pass-through that still charges the SSD's cost.
class DiskCacheStorage : public Storage {
 public:
  enum class Admission { kAlways, kFrequency };

  struct Options {
    std::string dir;                 // local directory, "/" appended if missing
    uint64_t capacity_bytes = 0;     // 0 = cache nothing (pure pass-through)
    std::string prefix = "sstable";  // names starting with this are cached
    size_t chunk_bytes = 64u << 20;  // ranged-read size of the fill worker
    bool drop_pages = true;          // POSIX_FADV_DONTNEED after each tier read
    size_t max_queue = 256;          // fill queue bound; the oldest is dropped
    Admission admission = Admission::kAlways;  // kFrequency: TinyLFU contest for non-free budget
    uint64_t admit_window = 0;                 // sketch aging window in samples; 0 = 8 x the entries the budget holds
  };

  struct Stats {
    uint64_t hits = 0, misses = 0, hit_bytes = 0, miss_bytes = 0, passthrough = 0;
    uint64_t fills = 0, fill_bytes = 0, fill_gets = 0;
    uint64_t fill_skipped_budget = 0, fill_skipped_present = 0, fill_gone = 0, fill_failed = 0, fill_dropped = 0;
    uint64_t writethrough_files = 0, evictions = 0, evicted_bytes = 0, invalidated = 0;
    uint64_t files = 0, bytes = 0, capacity = 0;
    uint64_t fetch_bytes = 0, admit_rejected = 0;
  };

  // Throws std::runtime_error when options.dir cannot be created or is not
  // writable (probed with a create + unlink).
  DiskCacheStorage(std::unique_ptr<Storage> backing, Options options);
  ~DiskCacheStorage() override;

  // Storage. Every method forwards to the backing store; the cacheable ones
  // also maintain the local copy as documented in the .cpp.
  void createDirectory(std::string name) override;
  Status append(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendInBatch(std::string const& fileName, unsigned char* const& data, int length) override;
  Status flush(std::string const& fileName) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t& size) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override;
  size_t size(std::string fileName) override;
  void seal(std::string fileName) override;
  bool isSealed(std::string fileName) override;
  void remove(std::string fileName) override;
  bool exist(std::string fileName) override;
  void setRemoteAppendListener(RemoteAppendListener listener) override;
  long lastAppendAddressForThread() const override;
  void sync() override;
  void clearSync() override;
  bool hasSyncToken() const override;

  // Tier API.
  bool cacheable(std::string const& name) const;
  // Drops the local copy (complete or in progress); the object stays.
  void invalidate(std::string const& name);
  // Scans dir: deletes .part and .fillpart files and complete files for which
  // live(name, bytes) is false, admits the rest oldest first. Returns the
  // number deleted.
  size_t reconcile(std::function<bool(std::string const&, size_t)> const& live);
  void startFillWorker();
  void stopFillWorker();
  void waitFillIdle();
  Stats stats();
  void printStats();
  Storage* backing() { return backing_.get(); }
  Options const& options() const { return options_; }

 private:
  struct Entry {
    size_t bytes = 0;
    std::list<std::string>::iterator lru;
  };

  // How a candidate takes budget that is not free (bench/PLAN-disk-cache-2.md, Task 2).
  //   kForce    evict until it fits: the round-1 behaviour. reconcile(), and every
  //             path under Admission::kAlways.
  //   kContest  only if the candidate's sketch frequency is strictly above the
  //             victim's: fills under Admission::kFrequency.
  //   kFreeOnly never: write-throughs under Admission::kFrequency, because a
  //             compaction output has no access history to contest with.
  // Free budget is always taken without a contest. A refusal can be counted
  // twice when the budget changes between a fill's check and its publish; that
  // is accepted, not fixed.
  enum class AdmitMode { kForce, kContest, kFreeOnly };
  AdmitMode fillMode() const { return options_.admission == Admission::kFrequency ? AdmitMode::kContest : AdmitMode::kForce; }
  AdmitMode writeThroughMode() const { return options_.admission == Admission::kFrequency ? AdmitMode::kFreeOnly : AdmitMode::kForce; }
  void recordAccess(std::string const& key);
  uint32_t frequency(std::string const& key);  // 0 without a sketch
  // Under mtx_. Decides whether `bytes` more for `key` is allowed under `how`,
  // given `victim` (empty = none). Counts a refusal in admit_rejected_.
  bool mayTakeLocked(std::string const& key, size_t bytes, std::string const& victim, AdmitMode how);
  // backing_->size() memoised per cacheable name (objects are immutable);
  // dropped by eraseLocked/invalidate. Used by the fill path only: the public
  // size() keeps its round-1 behaviour for a file the builder is appending.
  size_t sizeOf(std::string const& name);

  std::string localPath(std::string const& name) const { return options_.dir + name; }
  std::string partPath(std::string const& name) const { return options_.dir + name + ".part"; }
  // The fill's own temp file: never shared with parts_/writePart/discardPart,
  // so a peer invalidate() or a concurrent write-through can't truncate or
  // interleave into a fill in progress.
  std::string fillPartPath(std::string const& name) const { return options_.dir + name + ".fillpart"; }
  // Returns true and moves the entry to the LRU front when a complete copy exists.
  bool touch(std::string const& name);
  bool present(std::string const& name);
  // pread of [a, a+length) from the local copy into a new buffer; false on any short read.
  bool readLocal(std::string const& name, unsigned char*& data, size_t a, size_t length);
  // Appends to the .part stream (opened on first use). A write failure poisons
  // the name; a poisoned name is skipped, because reopening with trunc would
  // leave the tail of the file looking like a whole one.
  void writePart(std::string const& name, unsigned char const* data, size_t length);
  // Closes the .part, renames it into place and admits it; discards it on any
  // failure. On a poisoned name it removes the part, clears the poison and
  // returns false: nothing is published for that name, and the tier is free to
  // fill the file from the backing later.
  bool publishPart(std::string const& name);
  // Drops a write-through in progress and poisons the name, so the appends
  // that follow this discard cannot be published as a complete copy.
  void discardPart(std::string const& name);
  // Shared publish tail for a finished file at `part_path`: verifies its size
  // (against `expected_size` when nonzero), checks the budget, renames into
  // place and admits it. False (and the part removed) on any failure,
  // including the part having vanished or a size mismatch.
  bool publishPartFile(std::string const& name, std::string const& part_path, size_t expected_size, AdmitMode how);
  // Under mtx_: evicts until `bytes` fits, then inserts. False when bytes > capacity.
  bool admit(std::string const& name, size_t bytes, AdmitMode how);
  void evictToFitLocked(size_t bytes);
  void eraseLocked(std::string const& name, bool count_as_invalidated);
  void enqueueFill(std::string const& name);
  void fillLoop();
  void fillOne(std::string const& name);

  std::unique_ptr<Storage> backing_;
  Options options_;

  std::mutex mtx_;
  std::unordered_map<std::string, Entry> index_;
  std::list<std::string> lru_;  // front = most recently used
  uint64_t current_bytes_ = 0;
  std::unordered_map<std::string, size_t> sizes_;  // under mtx_

  // Leaf lock for sketch_: taken under mtx_ by mayTakeLocked(), and alone by
  // recordAccess(). Never held across any I/O.
  std::mutex sketch_mtx_;
  std::unique_ptr<FrequencySketch> sketch_;

  std::mutex parts_mtx_;
  std::unordered_map<std::string, std::unique_ptr<std::ofstream>> parts_;
  std::unordered_set<std::string> poisoned_parts_;

  std::mutex fill_mtx_;
  // Serialises startFillWorker/stopFillWorker across the join: held for the
  // whole body of each so two stops can't both pass the fill_started_ guard
  // and both join() the same thread, and so a start can't race a stop's join.
  std::mutex fill_lifecycle_mtx_;
  std::condition_variable fill_cv_;
  std::deque<std::string> fill_queue_;
  std::unordered_set<std::string> queued_;
  bool fill_started_ = false;
  bool fill_stop_ = false;
  bool fill_stopping_ = false;  // set for the duration of a stopFillWorker() call
  bool fill_busy_ = false;
  std::thread fill_thread_;

  std::atomic<uint64_t> hits_{0}, misses_{0}, hit_bytes_{0}, miss_bytes_{0}, passthrough_{0};
  std::atomic<uint64_t> fills_{0}, fill_bytes_{0}, fill_gets_{0};
  std::atomic<uint64_t> fill_skipped_budget_{0}, fill_skipped_present_{0}, fill_gone_{0}, fill_failed_{0}, fill_dropped_{0};
  std::atomic<uint64_t> writethrough_files_{0}, evictions_{0}, evicted_bytes_{0}, invalidated_{0};
  std::atomic<uint64_t> fetch_bytes_{0}, admit_rejected_{0};
};

}  // namespace ozonedb
