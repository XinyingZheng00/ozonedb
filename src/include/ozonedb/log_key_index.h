#ifndef OZONEDB_LOG_KEY_INDEX_H
#define OZONEDB_LOG_KEY_INDEX_H

#include "protobuf/record.pb.h"
#include <cstddef>
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
class LogKeyIndex {
 public:
  explicit LogKeyIndex(size_t capacity) : capacity_(capacity) {}

  std::shared_ptr<Record> lookup(std::string const& key);
  void upsert(std::string const& key, std::shared_ptr<Record> rec, std::string const& source_file);
  void invalidateFile(std::string const& file);
  size_t size() const;

 private:
  struct Entry {
    std::shared_ptr<Record> rec;
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
