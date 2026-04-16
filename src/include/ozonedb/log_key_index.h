#ifndef OZONEDB_LOG_KEY_INDEX_H
#define OZONEDB_LOG_KEY_INDEX_H

#include "protobuf/record.pb.h"
#include <cstddef>
#include <list>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace ozonedb {

// Per-LogHandler in-memory key -> latest Record* index. Lets readRecord
// short-circuit the multi-file scan (and, on Corfu, the storage fence).
// Record memory is owned by the LRUCache's per-file records map; the
// index borrows pointers and never frees. Log files are invalidated on
// compaction so stale pointers don't outlive the cache entry that owns
// the Record.
class LogKeyIndex {
 public:
  explicit LogKeyIndex(size_t capacity) : capacity_(capacity) {}

  Record* lookup(std::string const& key);
  void upsert(std::string const& key, Record* rec, std::string const& source_file);
  void invalidateFile(std::string const& file);
  size_t size() const;

 private:
  struct Entry {
    Record* rec;
    std::string source_file;
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
