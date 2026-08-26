#ifndef OZONEDB_LOG_KEY_INDEX_H
#define OZONEDB_LOG_KEY_INDEX_H

#include "protobuf/record.pb.h"
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace ozonedb {

// Per-LogHandler in-memory key -> latest Record index. Lets readRecord
// short-circuit the multi-file scan (and, on Corfu, the storage fence).
// Records are co-owned with the LRUCache via shared_ptr — the index
// keeps a reference so a cache invalidate (compaction, REMOVE) cannot
// free the Record out from under a reader that just lifted it out of
// the index.
// Position of a record in the shared log's global order.
//
// `addr` is the Corfu global address of the entry that carried the
// record. `sub` orders records inside one entry, because a single
// batched APPEND carries many records and the later ones are newer.
//
// An unordered source (a local backend with no global order, or a
// backfill scan) uses addr = -1, which never displaces a ranked entry
// and is always displaced by one.
//
// At namespace scope, not nested in LogKeyIndex: a nested type's default
// member initializers are not usable in a default argument of the
// enclosing class, which is still incomplete at that point.
struct LogRank {
  long addr = -1;
  uint32_t sub = 0;

  bool operator<(LogRank const& o) const {
    if (addr != o.addr) return addr < o.addr;
    return sub < o.sub;
  }
};

class LogKeyIndex {
 public:
  using Rank = LogRank;

  explicit LogKeyIndex(size_t capacity) : capacity_(capacity) {}

  std::shared_ptr<Record> lookup(std::string const& key);
  // Insert or replace the entry for `key`. A record whose Rank is BELOW
  // the stored one is dropped: the index is a newest-wins cache, and
  // the tailer can deliver a peer's older record after the local writer
  // has already indexed a newer one for the same key. The old
  // last-writer-by-arrival-time rule let that stale record win, and
  // nothing ever corrected it.
  void upsert(std::string const& key, std::shared_ptr<Record> rec,
              std::string const& source_file, LogRank rank = LogRank{});
  void invalidateFile(std::string const& file);
  size_t size() const;

 private:
  struct Entry {
    std::shared_ptr<Record> rec;
    std::string source_file;
    LogRank rank;
    std::list<std::string>::iterator lru_it;
  };

  void evictOneLocked();

  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, Entry> map_;
  std::list<std::string> lru_;
  size_t capacity_;
};

}  // namespace ozonedb
#endif
