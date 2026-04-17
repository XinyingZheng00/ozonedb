#include "cache.h"
#include "db.h"
#include "metadata_log_handler.h"
#include "protobuf_serializer.h"

#include <functional>
#include <iostream>

namespace ozonedb {
// Function to move a key to the front of the LRU list
void LRUCache::updateLRU(std::string const& file_name, std::string const& index_value) {
  lru_list.erase(file_to_entry_map[file_name].lru_itr[index_value]);
  lru_list.push_front({file_name, index_value});
  file_to_entry_map[file_name].lru_itr[index_value] = lru_list.begin();
}

// Function to evict the least recently used item
void LRUCache::evict() {
  while (current_size > capacity) {
    auto it = lru_list.rbegin();
    current_size -= file_to_entry_map[it->first].block_size[it->second];
    file_to_entry_map[it->first].block_records.erase(it->second);
    file_to_entry_map[it->first].block_size.erase(it->second);
    file_to_entry_map[it->first].lru_itr.erase(it->second);
    lru_list.pop_back();
  }
}
void LRUCache::checkReadMoreLog(std::string const& file_name, bool& read_more, size_t& cached_offset, size_t& size) {
  // Sample cache state under a shared lock, then release before any
  // storage->size() call. Holding LRUCache::mutex across the Corfu
  // fence inverted locks against the tailer's remote-append listener
  // (which calls putLogRecordSingle → LRUCache::mutex): the tailer
  // could not advance past an entry it was trying to publish, and
  // this thread could not wake because last_applied_addr_ was stuck
  // behind that same entry. The sampled state is eventually
  // consistent — a stale (low) cached_offset at worst triggers one
  // extra no-op readDataLog on the next tick; sealed only flips once
  // so a stale "unsealed" resolves on the next call.
  bool is_tail = false;
  {
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto it = file_to_entry_map.find(file_name);
    if (it != file_to_entry_map.end()) {
      if (it->second.sealed) {
        read_more = false;
        return;
      }
      cached_offset = it->second.offset;
    }
    is_tail = (file_name == latest_view->current_log_tail);
  }
  // latest_view->getFileSize lags behind the storage layer for the active
  // tail whenever the writer uses appendInBatch: the view is refreshed on
  // a ~100ms cycle and ignores the local cached_file_ buffer. Ask storage
  // directly for the tail so the read covers not-yet-flushed bytes too.
  // Sealed files never grow, so the view size is authoritative.
  if (is_tail) {
    size = log_storage_->size(file_name);
  } else {
    size = latest_view->getFileSize(file_name);
  }
  if (size <= cached_offset) {
    read_more = false;
  }
}

void LRUCache::readDataLog(std::string const& file_name, size_t cached_offset, size_t size) {
  std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
  std::shared_lock file_lock(file_mutex);
  unsigned char* buffer = nullptr;
  this->log_storage_->read(file_name, buffer, cached_offset, size - cached_offset);
  file_lock.unlock();
  std::vector<google::protobuf::Message*> messages;
  protobuf::deserializeMessages(buffer, size - cached_offset, messages, []() -> google::protobuf::Message* {
    return new Record();
  });
  delete[] buffer;
  buffer = nullptr;
  std::unordered_map<std::string, Record*> records_tmp;
  for (auto* msg : messages) {
    auto* rec = static_cast<Record*>(msg);
    records_tmp[rec->key()] = rec;
  }
  bool sealed = file_name!=latest_view->current_log_tail;
  putLogRecords(file_name, records_tmp, size, sealed);
}

void LRUCache::getSSTable(std::string const& file_name, Table*& table) {
  // Singleflight: only one thread opens a given file; concurrent
  // readers wait on the same future. Without this, N readers that hit
  // a cold file each issue 4–5 S3 GETs in Table::open and the later
  // putSSTableMeta calls leak all but the last Table*.
  std::shared_future<void> waiter;
  std::shared_ptr<std::promise<void>> my_promise;

  {
    std::unique_lock<std::shared_mutex> lock(mutex);
    auto it = file_to_entry_map.find(file_name);
    if (it != file_to_entry_map.end() && it->second.table != nullptr) {
      table = it->second.table;
      return;
    }
    auto in_it = table_inflight_.find(file_name);
    if (in_it != table_inflight_.end()) {
      waiter = in_it->second;
    } else {
      my_promise = std::make_shared<std::promise<void>>();
      table_inflight_[file_name] = my_promise->get_future().share();
    }
  }

  if (waiter.valid()) {
    waiter.wait();
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto it = file_to_entry_map.find(file_name);
    table = (it != file_to_entry_map.end()) ? it->second.table : nullptr;
    return;
  }

  // Leader path. Any exception from Table::open / setCache must not
  // leave a dangling entry in table_inflight_ or an unset promise —
  // doing so would wedge every future reader of this file on a
  // broken_promise future and the block would never be refetched.
  // The scope guard fires on both normal and exceptional exit.
  Table* opened = nullptr;
  bool cleanup_done = false;
  auto cleanup = [&]() {
    if (cleanup_done) return;
    cleanup_done = true;
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      if (opened != nullptr) {
        // Merge: a prior compaction write-through may have populated
        // block_records before any reader opened the file. Only the
        // Table* needs setting; wiping the CacheEntry wholesale would
        // throw away those warm blocks.
        file_to_entry_map[file_name].table = opened;
      }
      table_inflight_.erase(file_name);
    }
    try { my_promise->set_value(); } catch (...) {}
  };
  struct Guard {
    std::function<void()>& f;
    ~Guard() { f(); }
  } guard{cleanup};

  try {
    std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
    std::shared_lock<std::shared_mutex> file_lock(file_mutex);
    Status status = Table::open(this->sstable_storage_, file_name, opened);
    if (status != Status::kSuccess) {
      std::cout << "open table failed" << std::endl;
      opened = nullptr;
    }
  } catch (...) {
    opened = nullptr;
  }
  if (opened != nullptr) {
    try {
      opened->setCache(this);
    } catch (...) {
      // Table* is still usable; just log.
      std::cerr << "[lru_cache] setCache threw for " << file_name << "\n";
    }
  }
  table = opened;
}

void LRUCache::needReadBlock(std::string const& file_name, bool& read_more, std::string const& index_value) {
  std::shared_lock<std::shared_mutex> lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it != file_to_entry_map.end() &&
      it->second.block_records.find(index_value) != it->second.block_records.end()) {
    read_more = false;
    sstable_cache_hits_.fetch_add(1, std::memory_order_relaxed);
  }
}

void LRUCache::readDataBlocks(std::string const& file_name, std::string const& index_value) {
  // Singleflight on (file, block). Without this, concurrent readers on
  // a cold block each S3-GET it AND each call putSSTableRecords —
  // double-counting current_size, leaking prior records maps, and
  // evicting hot blocks immediately. Separator must never appear in
  // either component of the key; '\0' is safe for both file paths and
  // serialized BlockIdentifier protos.
  std::string inflight_key = file_name;
  inflight_key.push_back('\0');
  inflight_key.append(index_value);

  std::shared_future<void> waiter;
  std::shared_ptr<std::promise<void>> my_promise;
  Table* table = nullptr;

  {
    std::unique_lock<std::shared_mutex> lock(mutex);
    auto it = file_to_entry_map.find(file_name);
    // Re-check under the write lock: a leader may have published
    // between Table::get's needReadBlock and this call.
    if (it != file_to_entry_map.end() &&
        it->second.block_records.find(index_value) != it->second.block_records.end()) {
      sstable_cache_hits_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    auto in_it = block_inflight_.find(inflight_key);
    if (in_it != block_inflight_.end()) {
      waiter = in_it->second;
    } else {
      my_promise = std::make_shared<std::promise<void>>();
      block_inflight_[inflight_key] = my_promise->get_future().share();
      table = (it != file_to_entry_map.end()) ? it->second.table : nullptr;
      sstable_cache_misses_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  if (waiter.valid()) {
    waiter.wait();
    return;
  }

  // Leader path. Any exception from blockReader / iterator / parse
  // must not leave block_inflight_ populated with an unset promise;
  // if that happened, all subsequent readers for this block would
  // wait on a broken_promise future and then return without fetching,
  // wedging the block permanently. Under multi-writer Corfu loads
  // these errors do happen, so we guard with a scope that always
  // runs the publish-or-drop cleanup.
  Iterator* iter = nullptr;
  std::unordered_map<std::string, Record*>* records_tmp = nullptr;
  size_t size = 0;
  bool fetch_ok = false;
  bool cleanup_done = false;
  auto cleanup = [&]() {
    if (cleanup_done) return;
    cleanup_done = true;
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      if (fetch_ok) {
        auto& entry = file_to_entry_map[file_name];
        auto existing = entry.block_records.find(index_value);
        if (existing != entry.block_records.end()) {
          // Lost a race with another publisher (shouldn't happen
          // given singleflight, but defensive). Drop our clone.
          if (records_tmp) {
            for (auto& kv : *records_tmp) delete kv.second;
            delete records_tmp;
            records_tmp = nullptr;
          }
        } else {
          entry.block_records[index_value] = records_tmp;
          entry.block_size[index_value] = size;
          current_size += size;
          lru_list.push_front({file_name, index_value});
          entry.lru_itr[index_value] = lru_list.begin();
          if (current_size > capacity) {
            evict();
          }
        }
      } else if (records_tmp) {
        for (auto& kv : *records_tmp) delete kv.second;
        delete records_tmp;
        records_tmp = nullptr;
      }
      block_inflight_.erase(inflight_key);
    }
    try { my_promise->set_value(); } catch (...) {}
  };
  struct Guard {
    std::function<void()>& f;
    ~Guard() { f(); }
  } guard{cleanup};

  try {
    {
      std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
      std::shared_lock<std::shared_mutex> file_lock(file_mutex);
      iter = table->blockReader(table, index_value);
    }
    iter->seekToFirst();
    records_tmp = new std::unordered_map<std::string, Record*>();
    while (iter->valid()) {
      std::string const& value = iter->value();
      auto* record = new Record();
      record->ParseFromArray(value.data(), value.size());
      (*records_tmp)[iter->key()] = record;
      size += record->ByteSizeLong();
      iter->next();
    }
    fetch_ok = true;
  } catch (...) {
    std::cerr << "[lru_cache] readDataBlocks threw for " << file_name << "\n";
    fetch_ok = false;
  }
  if (iter) delete iter;
  // cleanup() publishes + erases + signals the promise; Guard
  // guarantees it fires exactly once even if anything below throws.
}

// Function to get the latest records for a given filename, return records and offset
void LRUCache::get(std::string const& file_name, std::string const& key, Record*& record, std::string const& index_value) {
  // Log reads are read-only against the cache; sstable reads touch the
  // LRU list on every hit so they need a write lock. A shared_lock
  // here with updateLRU below was a data race that corrupted lru_list
  // under concurrent zipfian reads, compounding the singleflight bug.
  if (file_name.find("log") != std::string::npos) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto& records = file_to_entry_map[file_name].records;
    auto record_it = records.find(key);
    record = (record_it != records.end()) ? record_it->second : nullptr;
  } else {
    std::unique_lock<std::shared_mutex> lock(mutex);
    auto& records = file_to_entry_map[file_name].block_records[index_value];
    updateLRU(file_name, index_value);
    auto record_it = records->find(key);
    record = (record_it != records->end()) ? record_it->second : nullptr;
  }
}

void LRUCache::putLogRecords(std::string const& key, std::unordered_map<std::string, Record*> const& records, size_t offset, bool sealed) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(key);
  if (it != file_to_entry_map.end()) {
    // Key exists, merge the records and update the offset if necessary
    if (offset > it->second.offset) {
      for (auto const& record : records) {
        it->second.records[record.first] = record.second;
      }
      it->second.offset = offset;
      it->second.sealed = sealed;
    }
    // updateLRU(key);
  } else {
    // Key does not exist, insert new entry
    // if (lru_list.size() >= capacity) {
    //   evict();
    // }
    // lru_list.push_front(key);
    CacheEntry entry(records, offset, sealed);
    file_to_entry_map[key] = entry;
  }
}

void LRUCache::putLogRecordSingle(std::string const& file_name, Record* record) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it == file_to_entry_map.end()) {
    // No cache entry yet — create one with offset=0 so a later
    // checkReadMoreLog will still trigger a full readDataLog. The index
    // gets a valid borrowed pointer either way.
    CacheEntry entry;
    entry.records[record->key()] = record;
    entry.offset = 0;
    entry.sealed = false;
    file_to_entry_map[file_name] = std::move(entry);
    return;
  }
  // Overwrite the prior pointer without freeing it — matches the
  // existing putLogRecords merge contract (cache.cpp:161-163) and
  // avoids a use-after-free for readers that borrowed the old pointer
  // from the cache or the key index. The old pointer leaks until the
  // cache is destroyed; this is the existing invariant and a broader
  // fix belongs in a separate change.
  it->second.records[record->key()] = record;
}

void LRUCache::snapshotLogFileRecords(std::string const& file_name,
                                     std::unordered_map<std::string, Record*>& out) {
  std::shared_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it == file_to_entry_map.end()) return;
  out = it->second.records;
}

void LRUCache::putSSTableMeta(std::string const& key, Table* table) {
  std::unique_lock lock(mutex);
  // if (lru_list.size() >= capacity) {
  //   evict();
  // }
  // lru_list.push_front(key);
  CacheEntry entry(table);
  file_to_entry_map[key] = entry;
}

void LRUCache::putSSTableRecords(std::string const& key, std::unordered_map<std::string, Record*>* records, std::string const& index_value, size_t size) {
  // Defensive: the singleflight path in readDataBlocks now handles
  // publishing. If this is still called directly by older callers and
  // races publish the same block twice, drop the duplicate instead of
  // double-counting current_size and leaking the prior records map.
  std::unique_lock<std::shared_mutex> lock(mutex);
  auto& entry = file_to_entry_map[key];
  auto existing = entry.block_records.find(index_value);
  if (existing != entry.block_records.end()) {
    for (auto& kv : *records) delete kv.second;
    delete records;
    return;
  }
  entry.block_records[index_value] = records;
  entry.block_size[index_value] = size;
  current_size += size;
  lru_list.push_front({key, index_value});
  entry.lru_itr[index_value] = lru_list.begin();
  if (current_size > capacity) {
    evict();
  }
}

void LRUCache::printCacheStats() const {
  auto hits = sstable_cache_hits_.load(std::memory_order_relaxed);
  auto misses = sstable_cache_misses_.load(std::memory_order_relaxed);
  auto total = hits + misses;
  double hit_rate = total ? (100.0 * hits / static_cast<double>(total)) : 0.0;
  std::cerr << "[lru_cache] sstable hits=" << hits
            << " misses=" << misses
            << " hit_rate=" << hit_rate << "%"
            << " capacity=" << capacity
            << " current_size=" << current_size
            << " files=" << file_to_entry_map.size()
            << std::endl;
}

}  // namespace ozonedb