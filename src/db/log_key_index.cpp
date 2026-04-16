#include "log_key_index.h"

#include <mutex>

namespace ozonedb {

Record* LogKeyIndex::lookup(std::string const& key) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end()) return nullptr;
  return it->second.rec;
}

void LogKeyIndex::upsert(std::string const& key, Record* rec,
                         std::string const& source_file) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it != map_.end()) {
    it->second.rec = rec;
    it->second.source_file = source_file;
    lru_.splice(lru_.begin(), lru_, it->second.lru_it);
    return;
  }
  lru_.push_front(key);
  map_[key] = Entry{rec, source_file, lru_.begin()};
  while (capacity_ > 0 && map_.size() > capacity_) {
    evictOneLocked();
  }
}

void LogKeyIndex::invalidateFile(std::string const& file) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  for (auto it = map_.begin(); it != map_.end();) {
    if (it->second.source_file == file) {
      lru_.erase(it->second.lru_it);
      it = map_.erase(it);
    } else {
      ++it;
    }
  }
}

size_t LogKeyIndex::size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return map_.size();
}

void LogKeyIndex::evictOneLocked() {
  if (lru_.empty()) return;
  std::string key = lru_.back();
  lru_.pop_back();
  map_.erase(key);
}

}  // namespace ozonedb
