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

#include "storage.h"

namespace ozonedb {

// A read-through tier for immutable SSTable objects on a local disk, in
// front of the object store that holds them (bench/PLAN-disk-cache.md).
//
// Entries are whole files: "<dir>/<name>" is a complete local copy,
// "<dir>/<name>.part" is one in progress. Only names with `prefix` are
// cached; everything else passes straight through to the backing store
// (checkpoint/LATEST is mutable and must never be cached).
//
// Population: the builder's write-through (append* + flush), a background
// fill on the first ranged-read miss (chunked ranged reads of the backing),
// and reconcile() at open. Eviction: LRU by file under capacity_bytes.
// Invalidation: remove() (this process compacted) and invalidate() (a peer
// compacted; wired to the COMPACT apply in MetadataLogHandler).
//
// Lock order: mtx_ (index, LRU, budget) and fill_mtx_ (queue, worker) are
// never held across a backing-store call or file I/O other than unlink.
// Nothing here takes a cache or view mutex; the DB calls the tier only
// outside view_mutex.
class DiskCacheStorage : public Storage {
 public:
  struct Options {
    std::string dir;                 // local directory, "/" appended if missing
    uint64_t capacity_bytes = 0;     // 0 = cache nothing (pure pass-through)
    std::string prefix = "sstable";  // names starting with this are cached
    size_t chunk_bytes = 64u << 20;  // ranged-read size of the fill worker
    bool drop_pages = true;          // POSIX_FADV_DONTNEED after each tier read
    size_t max_queue = 256;          // fill queue bound; the oldest is dropped
  };

  struct Stats {
    uint64_t hits = 0, misses = 0, hit_bytes = 0, miss_bytes = 0, passthrough = 0;
    uint64_t fills = 0, fill_bytes = 0, fill_gets = 0;
    uint64_t fill_skipped_budget = 0, fill_skipped_present = 0, fill_gone = 0, fill_failed = 0, fill_dropped = 0;
    uint64_t writethrough_files = 0, evictions = 0, evicted_bytes = 0, invalidated = 0;
    uint64_t files = 0, bytes = 0, capacity = 0;
  };

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
  // Scans dir: deletes .part files and complete files for which live(name,
  // bytes) is false, admits the rest oldest first. Returns the number deleted.
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

  std::string localPath(std::string const& name) const { return options_.dir + name; }
  std::string partPath(std::string const& name) const { return options_.dir + name + ".part"; }
  // Returns true and moves the entry to the LRU front when a complete copy exists.
  bool touch(std::string const& name);
  bool present(std::string const& name);
  // pread of [a, a+length) from the local copy into a new buffer; false on any short read.
  bool readLocal(std::string const& name, unsigned char*& data, size_t a, size_t length);
  // Appends to the .part stream (opened on first use). Failures poison the part.
  void writePart(std::string const& name, unsigned char const* data, size_t length);
  // Closes the .part, renames it into place and admits it; discards it on any failure.
  bool publishPart(std::string const& name);
  void discardPart(std::string const& name);
  // Under mtx_: evicts until `bytes` fits, then inserts. False when bytes > capacity.
  bool admit(std::string const& name, size_t bytes);
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

  std::mutex parts_mtx_;
  std::unordered_map<std::string, std::unique_ptr<std::ofstream>> parts_;
  std::unordered_set<std::string> poisoned_parts_;

  std::mutex fill_mtx_;
  std::condition_variable fill_cv_;
  std::deque<std::string> fill_queue_;
  std::unordered_set<std::string> queued_;
  bool fill_started_ = false;
  bool fill_stop_ = false;
  bool fill_busy_ = false;
  std::thread fill_thread_;

  std::atomic<uint64_t> hits_{0}, misses_{0}, hit_bytes_{0}, miss_bytes_{0}, passthrough_{0};
  std::atomic<uint64_t> fills_{0}, fill_bytes_{0}, fill_gets_{0};
  std::atomic<uint64_t> fill_skipped_budget_{0}, fill_skipped_present_{0}, fill_gone_{0}, fill_failed_{0}, fill_dropped_{0};
  std::atomic<uint64_t> writethrough_files_{0}, evictions_{0}, evicted_bytes_{0}, invalidated_{0};
};

}  // namespace ozonedb
