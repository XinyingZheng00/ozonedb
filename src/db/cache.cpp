#include "cache.h"
#include "db.h"
#include "metadata_log_handler.h"
#include "protobuf_serializer.h"

#include <functional>
#include <iostream>
#include <stdexcept>

namespace ozonedb {
int LRUCache::levelSlot(std::string const& file_name) {
  static constexpr char kPrefix[] = "sstable";
  static constexpr size_t kPrefixLen = sizeof(kPrefix) - 1;
  if (file_name.compare(0, kPrefixLen, kPrefix) != 0) return 0;
  size_t i = kPrefixLen;
  int level = 0;
  bool digits = false;
  for (; i < file_name.size() && file_name[i] >= '0' && file_name[i] <= '9'; ++i) {
    level = level * 10 + (file_name[i] - '0');
    digits = true;
    if (level >= kLevelSlots) return 0;
  }
  if (!digits || i >= file_name.size() || file_name[i] != '/') return 0;
  return level;
}

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
    auto& entry = file_to_entry_map[it->first];
    current_size -= entry.block_size[it->second];
    // block_records values are heap-allocated maps; the inner map's
    // shared_ptr<Record> values release on map destruction. A reader
    // that lifted a shared_ptr out of get() keeps its Record alive
    // independently — eviction no longer races with use.
    auto block_it = entry.block_records.find(it->second);
    if (block_it != entry.block_records.end()) {
      delete block_it->second;
      entry.block_records.erase(block_it);
    }
    entry.block_size.erase(it->second);
    entry.lru_itr.erase(it->second);
    entry.warmed_blocks.erase(it->second);
    lru_list.pop_back();
  }
}
// Fenced size of a sealed log file, memoized. Never call this while
// holding `mutex` — see the sealed_size_ comment in cache.h.
size_t LRUCache::sealedLogSize(std::string const& file_name) {
  {
    std::lock_guard<std::mutex> lk(sealed_size_mtx_);
    auto it = sealed_size_.find(file_name);
    if (it != sealed_size_.end()) return it->second;
  }
  size_t fenced = log_storage_->size(file_name);
  if (fenced == 0) {
    // The file is not applied locally yet (or is already removed). Fall
    // back to the View rather than memoizing a zero that would suppress
    // every later read of this file.
    return latest_view != nullptr ? latest_view->getFileSize(file_name) : 0;
  }
  std::lock_guard<std::mutex> lk(sealed_size_mtx_);
  sealed_size_[file_name] = fenced;
  return fenced;
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
  //
  // A sealed file never grows, but the View is still the wrong source for
  // its size: that number is the emitter's stale sealed_input_bytes, so
  // peer records written just before the seal fall outside the bound and
  // never become readable. Fence once per sealed file and memoize.
  if (is_tail) {
    size = log_storage_->size(file_name);
  } else {
    size = sealedLogSize(file_name);
  }
  if (size <= cached_offset) {
    read_more = false;
  }
}

void LRUCache::readDataLog(std::string const& file_name, size_t cached_offset, size_t size) {
  std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
  std::shared_lock file_lock(file_mutex);
  unsigned char* buffer = nullptr;
  Status read_status = this->log_storage_->read(
      file_name, buffer, cached_offset, size - cached_offset);
  file_lock.unlock();
  // Race window: between our caller's checkReadMoreLog and this read,
  // a peer's compaction can emit REMOVE for `file_name`. The local
  // tailer applies the REMOVE, read returns kNotFound, buffer stays
  // null. Bail out — there's nothing left to deserialize and the
  // metadata-log rollforward will eventually drop this file from view.
  if (read_status != Status::kSuccess || buffer == nullptr) {
    delete[] buffer;
    return;
  }
  std::vector<google::protobuf::Message*> messages;
  protobuf::deserializeMessages(buffer, size - cached_offset, messages, []() -> google::protobuf::Message* {
    return new Record();
  });
  delete[] buffer;
  buffer = nullptr;
  std::unordered_map<std::string, std::shared_ptr<Record>> records_tmp;
  for (auto* msg : messages) {
    // Wrap each freshly-parsed Record in a shared_ptr so the cache and
    // any future borrowers (LogKeyIndex, readers) share ownership.
    std::shared_ptr<Record> rec(static_cast<Record*>(msg));
    auto const& k = rec->key();
    records_tmp[k] = std::move(rec);
  }
  bool sealed = file_name!=latest_view->current_log_tail;
  putLogRecords(file_name, std::move(records_tmp), size, sealed);
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
  // Store the callable by value — a reference member can't bind to
  // the lambda-to-std::function rvalue conversion. The held
  // std::function still references the surrounding locals (lambda
  // captures are [&]); Guard is in the same scope, so it is
  // destroyed before those captures go out of scope.
  struct Guard {
    std::function<void()> f;
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
    level_hits_[levelSlot(file_name)].fetch_add(1, std::memory_order_relaxed);
  }
}

void LRUCache::readDataBlocks(std::string const& file_name, std::string const& index_value, Table* caller_table) {
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
      level_hits_[levelSlot(file_name)].fetch_add(1, std::memory_order_relaxed);
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
      level_misses_[levelSlot(file_name)].fetch_add(1, std::memory_order_relaxed);
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
  std::unordered_map<std::string, std::shared_ptr<Record>>* records_tmp = nullptr;
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
          // given singleflight, but defensive). Drop our clone — the
          // shared_ptr destructors free Records cleanly.
          delete records_tmp;
          records_tmp = nullptr;
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
        delete records_tmp;
        records_tmp = nullptr;
      }
      block_inflight_.erase(inflight_key);
    }
    try { my_promise->set_value(); } catch (...) {}
  };
  // See getSSTable above for why this is by value, not by reference.
  struct Guard {
    std::function<void()> f;
    ~Guard() { f(); }
  } guard{cleanup};

  try {
    // No Table* in the entry: the compaction write-through created the
    // entry before any reader opened the file, or a REMOVE erased it
    // between needReadBlock and here. Fall back to the caller's Table;
    // without one the block is unreadable and this is a miss.
    if (table == nullptr) table = caller_table;
    if (table == nullptr) {
      throw std::runtime_error("readDataBlocks: no Table for " + file_name);
    }
    {
      std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(file_name);
      std::shared_lock<std::shared_mutex> file_lock(file_mutex);
      iter = table->blockReader(table, index_value);
    }
    // blockReader returns null when the storage read fails, which is
    // what a GET on a just-removed object looks like.
    if (iter == nullptr) {
      throw std::runtime_error("readDataBlocks: block read failed for " + file_name);
    }
    iter->seekToFirst();
    records_tmp = new std::unordered_map<std::string, std::shared_ptr<Record>>();
    while (iter->valid()) {
      std::string const& value = iter->value();
      auto record = std::make_shared<Record>();
      record->ParseFromArray(value.data(), value.size());
      size += record->ByteSizeLong();
      (*records_tmp)[iter->key()] = std::move(record);
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

// SSTable variant — returns a shared_ptr copy so the Record stays
// alive past a concurrent evict() that drops the block.
void LRUCache::get(std::string const& file_name, std::string const& key, std::shared_ptr<Record>& record, std::string const& index_value) {
  std::unique_lock<std::shared_mutex> lock(mutex);
  // Lookups only, no operator[]: after a failed readDataBlocks (file
  // removed under the reader) the block is absent, and creating an
  // entry with a null records map here would crash the next reader.
  record = nullptr;
  auto file_it = file_to_entry_map.find(file_name);
  if (file_it == file_to_entry_map.end()) return;
  auto block_it = file_it->second.block_records.find(index_value);
  if (block_it == file_it->second.block_records.end() || block_it->second == nullptr) return;
  updateLRU(file_name, index_value);
  // First read of a block the warm worker published: one warm hit, and
  // the mark goes so the block counts once.
  if (!file_it->second.warmed_blocks.empty() && file_it->second.warmed_blocks.erase(index_value) != 0) {
    warm_hits_.fetch_add(1, std::memory_order_relaxed);
  }
  auto record_it = block_it->second->find(key);
  if (record_it != block_it->second->end()) record = record_it->second;
}

// Log variant — returns a shared_ptr copy so the Record is kept alive
// past a concurrent invalidateLogFile or a same-key overwrite. Holds
// only a shared_lock; no LRU bookkeeping.
void LRUCache::getLog(std::string const& file_name, std::string const& key,
                      std::shared_ptr<Record>& record) {
  std::shared_lock<std::shared_mutex> lock(mutex);
  auto file_it = file_to_entry_map.find(file_name);
  if (file_it == file_to_entry_map.end()) {
    record.reset();
    return;
  }
  auto record_it = file_it->second.records.find(key);
  record = (record_it != file_it->second.records.end()) ? record_it->second
                                                        : nullptr;
}

void LRUCache::putLogRecords(std::string const& key, std::unordered_map<std::string, std::shared_ptr<Record>> records, size_t offset, bool sealed) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(key);
  if (it != file_to_entry_map.end()) {
    // Key exists, merge the records and update the offset if necessary
    if (offset > it->second.offset) {
      for (auto& record : records) {
        it->second.records[record.first] = std::move(record.second);
      }
      it->second.offset = offset;
      it->second.sealed = sealed;
    }
    // Lost the offset race: drop the local map. shared_ptr destructors
    // release any peer borrowers' refcounts safely.
  } else {
    // Key does not exist, insert new entry
    file_to_entry_map.emplace(key, CacheEntry(std::move(records), offset, sealed));
  }
}

void LRUCache::putLogRecordSingle(std::string const& file_name, std::shared_ptr<Record> record) {
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it == file_to_entry_map.end()) {
    // No cache entry yet — create one with offset=0 so a later
    // checkReadMoreLog will still trigger a full readDataLog. The index
    // gets shared ownership either way.
    CacheEntry entry;
    auto key = record->key();
    entry.records[std::move(key)] = std::move(record);
    entry.offset = 0;
    entry.sealed = false;
    file_to_entry_map[file_name] = std::move(entry);
    return;
  }
  // Overwrite is now safe under shared_ptr semantics: any borrower
  // (LogKeyIndex, an in-flight reader) keeps the prior Record alive
  // via their own shared_ptr until they release it. The Record only
  // deallocates when the last refcount drops.
  auto key = record->key();
  it->second.records[std::move(key)] = std::move(record);
}

void LRUCache::invalidateLogFile(std::string const& file_name) {
  {
    // Drop the memoized fenced size: the file is being compacted away,
    // and a later file could reuse the name.
    std::lock_guard<std::mutex> lk(sealed_size_mtx_);
    sealed_size_.erase(file_name);
  }
  std::unique_lock lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it == file_to_entry_map.end()) return;
  // Log records release via shared_ptr — deallocation is deferred
  // until the last borrower drops its reference. Concurrent readers
  // that already lifted a pointer out of LogKeyIndex stay safe.
  // Log entries shouldn't carry sstable state, but clean up defensively.
  for (auto& block : it->second.block_records) {
    delete block.second;
    auto itr = it->second.lru_itr.find(block.first);
    if (itr != it->second.lru_itr.end()) lru_list.erase(itr->second);
    auto size_it = it->second.block_size.find(block.first);
    if (size_it != it->second.block_size.end() &&
        current_size >= size_it->second) {
      current_size -= size_it->second;
    }
  }
  // Never delete the Table here: a reader that took the raw Table* from
  // getSSTable may be inside Table::get on it right now (the reader
  // holds no lock across the block read). Retire it and free it after
  // the grace period, see retired_tables_ in cache.h.
  auto const now = std::chrono::steady_clock::now();
  if (it->second.table != nullptr) retired_tables_.emplace_back(now, it->second.table);
  file_to_entry_map.erase(it);
  freeRetiredLocked(now);
}

void LRUCache::freeRetiredLocked(std::chrono::steady_clock::time_point now) {
  while (!retired_tables_.empty() &&
         std::chrono::duration_cast<std::chrono::milliseconds>(now - retired_tables_.front().first).count() > kRetiredTableGraceMs) {
    delete retired_tables_.front().second;
    retired_tables_.pop_front();
  }
}

size_t LRUCache::invalidateSSTable(std::string const& file_name) {
  std::unique_lock lock(mutex);
  auto const now = std::chrono::steady_clock::now();
  freeRetiredLocked(now);
  auto it = file_to_entry_map.find(file_name);
  if (it == file_to_entry_map.end()) return 0;
  CacheEntry& entry = it->second;
  if (entry.table == nullptr && entry.block_records.empty()) return 0;  // a log entry
  size_t blocks = 0;
  for (auto& block : entry.block_records) {
    delete block.second;
    ++blocks;
    auto itr = entry.lru_itr.find(block.first);
    if (itr != entry.lru_itr.end()) lru_list.erase(itr->second);
    auto size_it = entry.block_size.find(block.first);
    if (size_it != entry.block_size.end() && current_size >= size_it->second) {
      current_size -= size_it->second;
    }
  }
  // Same rule as invalidateLogFile: a reader that took the raw Table*
  // from getSSTable may be inside Table::get on it right now.
  if (entry.table != nullptr) retired_tables_.emplace_back(now, entry.table);
  file_to_entry_map.erase(it);
  return blocks;
}

void LRUCache::snapshotLogFileRecords(std::string const& file_name,
                                     std::unordered_map<std::string, std::shared_ptr<Record>>& out) {
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

void LRUCache::putSSTableRecords(std::string const& key, std::unordered_map<std::string, std::shared_ptr<Record>>* records, std::string const& index_value, size_t size, bool warmed) {
  // Defensive: the singleflight path in readDataBlocks now handles
  // publishing. If this is still called directly by older callers and
  // races publish the same block twice, drop the duplicate instead of
  // double-counting current_size. shared_ptr destructors free the
  // duplicate Records cleanly.
  std::unique_lock<std::shared_mutex> lock(mutex);
  auto& entry = file_to_entry_map[key];
  auto existing = entry.block_records.find(index_value);
  if (existing != entry.block_records.end()) {
    delete records;
    return;
  }
  entry.block_records[index_value] = records;
  entry.block_size[index_value] = size;
  current_size += size;
  lru_list.push_front({key, index_value});
  entry.lru_itr[index_value] = lru_list.begin();
  if (warmed) entry.warmed_blocks.insert(index_value);
  if (current_size > capacity) {
    evict();
  }
}

void LRUCache::markSSTableComplete(std::string const& file_name) {
  std::unique_lock<std::shared_mutex> lock(mutex);
  auto it = file_to_entry_map.find(file_name);
  if (it != file_to_entry_map.end()) it->second.complete = true;
}

// ---- Warm worker ----

LRUCache::WarmSkip LRUCache::warmDecision(WarmPolicy const& policy, int level, size_t bytes, size_t capacity, size_t input_blocks) {
  if (!policy.enabled) return WarmSkip::kDisabled;
  if (level > policy.max_level) return WarmSkip::kLevel;
  if (static_cast<double>(bytes) > policy.max_fraction * static_cast<double>(capacity)) return WarmSkip::kBudget;
  if (input_blocks < policy.min_input_blocks) return WarmSkip::kAffinity;
  return WarmSkip::kNone;
}

void LRUCache::setWarmPolicy(WarmPolicy const& policy) {
  std::lock_guard<std::mutex> lk(warm_mtx_);
  warm_policy_ = policy;
}

void LRUCache::setLiveFileCheck(std::function<bool(std::string const&)> check) {
  std::lock_guard<std::mutex> lk(warm_mtx_);
  live_file_check_ = std::move(check);
}

void LRUCache::onCompactionApplied(std::vector<std::string> const& outputs, std::vector<size_t> const& output_bytes, int dest_level, size_t cached_input_blocks, bool log_inputs) {
  {
    // Before startWarmWorker (openDB's replay of the metadata log offers
    // every historical COMPACT) or after stop: count as disabled, not as
    // a policy skip, so the per-rule counters describe the run only.
    std::lock_guard<std::mutex> lk(warm_mtx_);
    if (!warm_started_ || warm_stop_) {
      warm_skipped_disabled_.fetch_add(outputs.size(), std::memory_order_relaxed);
      return;
    }
  }
  size_t affinity = cached_input_blocks;
  if (log_inputs) {
    int const slot = (dest_level > 0 && dest_level < kLevelSlots) ? dest_level : 0;
    uint64_t const now = level_hits_[slot].load(std::memory_order_relaxed) + level_misses_[slot].load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(warm_mtx_);
    uint64_t& last = level_lookups_at_last_log_event_[slot];
    affinity = static_cast<size_t>(now >= last ? now - last : 0);
    last = now;
  }
  for (size_t i = 0; i < outputs.size(); ++i) {
    size_t const bytes = i < output_bytes.size() ? output_bytes[i] : 0;
    switch (warmDecision(warm_policy_, dest_level, bytes, capacity, affinity)) {
      case WarmSkip::kDisabled: warm_skipped_disabled_.fetch_add(1, std::memory_order_relaxed); continue;
      case WarmSkip::kLevel: warm_skipped_level_.fetch_add(1, std::memory_order_relaxed); continue;
      case WarmSkip::kBudget: warm_skipped_budget_.fetch_add(1, std::memory_order_relaxed); continue;
      case WarmSkip::kAffinity: warm_skipped_affinity_.fetch_add(1, std::memory_order_relaxed); continue;
      case WarmSkip::kNone: break;
    }
    {
      std::lock_guard<std::mutex> lk(warm_mtx_);
      if (!warm_started_ || warm_stop_) {
        warm_skipped_disabled_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      while (warm_queue_.size() >= warm_policy_.max_queue) {
        warm_queue_.pop_front();
        warm_dropped_.fetch_add(1, std::memory_order_relaxed);
      }
      warm_queue_.push_back(outputs[i]);
    }
    warm_cv_.notify_all();
  }
}

void LRUCache::startWarmWorker() {
  std::lock_guard<std::mutex> lk(warm_mtx_);
  if (warm_started_) return;
  warm_stop_ = false;
  warm_busy_ = false;
  warm_started_ = true;
  warm_thread_ = std::thread(&LRUCache::warmLoop, this);
}

void LRUCache::stopWarmWorker() {
  {
    std::lock_guard<std::mutex> lk(warm_mtx_);
    if (!warm_started_) return;
    warm_stop_ = true;
  }
  warm_cv_.notify_all();
  if (warm_thread_.joinable()) warm_thread_.join();
  std::lock_guard<std::mutex> lk(warm_mtx_);
  warm_started_ = false;
  warm_queue_.clear();
}

void LRUCache::waitWarmIdle() {
  std::unique_lock<std::mutex> lk(warm_mtx_);
  warm_cv_.wait(lk, [this] { return !warm_started_ || warm_stop_ || (warm_queue_.empty() && !warm_busy_); });
}

void LRUCache::warmLoop() {
  for (;;) {
    std::string file_name;
    {
      std::unique_lock<std::mutex> lk(warm_mtx_);
      warm_cv_.wait(lk, [this] { return warm_stop_ || !warm_queue_.empty(); });
      if (warm_stop_) return;
      file_name = std::move(warm_queue_.front());
      warm_queue_.pop_front();
      warm_busy_ = true;
    }
    try {
      warmOne(file_name);
    } catch (...) {
      std::cerr << "[lru_cache] warm threw for " << file_name << "\n";
      warm_skipped_gone_.fetch_add(1, std::memory_order_relaxed);
    }
    {
      std::lock_guard<std::mutex> lk(warm_mtx_);
      warm_busy_ = false;
    }
    warm_cv_.notify_all();
  }
}

void LRUCache::warmOne(std::string const& file_name) {
  std::function<bool(std::string const&)> live;
  size_t read_bytes = 0;
  {
    std::lock_guard<std::mutex> lk(warm_mtx_);
    live = live_file_check_;
    read_bytes = warm_policy_.read_bytes;
  }
  // Replaced by a later compaction while it waited in the queue.
  if (live && !live(file_name)) {
    warm_skipped_gone_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  {
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto it = file_to_entry_map.find(file_name);
    if (it != file_to_entry_map.end() && it->second.complete) {
      warm_skipped_built_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
  }
  // The open every peer pays on its first read of the file anyway
  // (singleflight with those readers).
  Table* table = nullptr;
  getSSTable(file_name, table);
  if (table == nullptr) {
    warm_skipped_gone_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  size_t blocks = 0;
  size_t bytes = 0;
  Status s = table->warm(this, read_bytes, blocks, bytes);
  warm_blocks_.fetch_add(blocks, std::memory_order_relaxed);
  warm_bytes_.fetch_add(bytes, std::memory_order_relaxed);
  if (s != Status::kSuccess) {
    // A removed object mid-read: what was published stays valid.
    warm_skipped_gone_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  warm_files_.fetch_add(1, std::memory_order_relaxed);
}

LRUCache::Stats LRUCache::stats() {
  Stats s;
  s.hits = sstable_cache_hits_.load(std::memory_order_relaxed);
  s.misses = sstable_cache_misses_.load(std::memory_order_relaxed);
  for (int i = 0; i < kLevelSlots; ++i) {
    s.level_hits[i] = level_hits_[i].load(std::memory_order_relaxed);
    s.level_misses[i] = level_misses_[i].load(std::memory_order_relaxed);
  }
  s.warm_files = warm_files_.load(std::memory_order_relaxed);
  s.warm_blocks = warm_blocks_.load(std::memory_order_relaxed);
  s.warm_bytes = warm_bytes_.load(std::memory_order_relaxed);
  s.warm_hits = warm_hits_.load(std::memory_order_relaxed);
  s.warm_skipped_disabled = warm_skipped_disabled_.load(std::memory_order_relaxed);
  s.warm_skipped_level = warm_skipped_level_.load(std::memory_order_relaxed);
  s.warm_skipped_budget = warm_skipped_budget_.load(std::memory_order_relaxed);
  s.warm_skipped_affinity = warm_skipped_affinity_.load(std::memory_order_relaxed);
  s.warm_skipped_gone = warm_skipped_gone_.load(std::memory_order_relaxed);
  s.warm_skipped_built = warm_skipped_built_.load(std::memory_order_relaxed);
  s.warm_dropped = warm_dropped_.load(std::memory_order_relaxed);

  // Liveness comes from a fresh View snapshot when DB gave us the check
  // (setLiveFileCheck): the raw latest_view pointer is refreshed only by
  // DB::get, so at the end of a pure load it is the open-time View and
  // every file built since would count as dead. Tests without a DB set
  // latest_view directly.
  std::function<bool(std::string const&)> live;
  {
    std::lock_guard<std::mutex> lk(warm_mtx_);
    live = live_file_check_;
  }
  std::shared_lock<std::shared_mutex> lock(mutex);
  s.capacity = capacity;
  s.current_size = current_size;
  s.files = file_to_entry_map.size();
  s.retired_tables = retired_tables_.size();
  for (auto const& [name, entry] : file_to_entry_map) {
    if (entry.table == nullptr && entry.block_records.empty()) continue;  // a log file
    ++s.sstable_files;
    if (entry.block_records.empty()) continue;
    if (live) {
      if (live(name)) continue;
    } else {
      if (latest_view == nullptr) continue;
      if (latest_view->key_range.find(name) != latest_view->key_range.end()) continue;
    }
    ++s.dead_files;
    for (auto const& [index_value, size] : entry.block_size) {
      s.dead_bytes += size;
      ++s.dead_blocks;
    }
  }
  return s;
}

void LRUCache::printCacheStats() {
  Stats s = stats();
  auto total = s.hits + s.misses;
  double hit_rate = total ? (100.0 * s.hits / static_cast<double>(total)) : 0.0;
  std::cerr << "[lru_cache] sstable hits=" << s.hits
            << " misses=" << s.misses
            << " hit_rate=" << hit_rate << "%"
            << " capacity=" << s.capacity
            << " current_size=" << s.current_size
            << " files=" << s.files
            << std::endl;
  // Levels print as "<level>:<count>" pairs up to the highest level that
  // saw a hit or a miss (slot 0 = names that are not "sstable<L>/…").
  int top = 0;
  for (int i = 0; i < kLevelSlots; ++i) {
    if (s.level_hits[i] != 0 || s.level_misses[i] != 0) top = i;
  }
  std::cerr << "[lru_cache] levels hits=";
  for (int i = 0; i <= top; ++i) std::cerr << (i ? "," : "") << i << ":" << s.level_hits[i];
  std::cerr << " misses=";
  for (int i = 0; i <= top; ++i) std::cerr << (i ? "," : "") << i << ":" << s.level_misses[i];
  std::cerr << " sstable_files=" << s.sstable_files
            << " dead_files=" << s.dead_files
            << " dead_blocks=" << s.dead_blocks
            << " dead_bytes=" << s.dead_bytes
            << " retired=" << s.retired_tables
            << " warm files=" << s.warm_files
            << " blocks=" << s.warm_blocks
            << " bytes=" << s.warm_bytes
            << " skipped_disabled=" << s.warm_skipped_disabled
            << " skipped_level=" << s.warm_skipped_level
            << " skipped_budget=" << s.warm_skipped_budget
            << " skipped_affinity=" << s.warm_skipped_affinity
            << " skipped_gone=" << s.warm_skipped_gone
            << " skipped_built=" << s.warm_skipped_built
            << " dropped=" << s.warm_dropped
            << " warm_hits=" << s.warm_hits
            << std::endl;
}

}  // namespace ozonedb