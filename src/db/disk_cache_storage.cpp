// src/db/disk_cache_storage.cpp
#include "disk_cache_storage.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <vector>

namespace ozonedb {

namespace fs = std::filesystem;

DiskCacheStorage::DiskCacheStorage(std::unique_ptr<Storage> backing, Options options)
    : Storage(options.dir), backing_(std::move(backing)), options_(std::move(options)) {
  if (!options_.dir.empty() && options_.dir.back() != '/') options_.dir += '/';
  storage_path = options_.dir;
  options_.max_queue = std::max<size_t>(1, options_.max_queue);
  // An unusable tier directory is a configuration error, not something to
  // limp on: the tier would silently become a pure pass-through while the
  // run still claims to measure an SSD. Fail the open instead, the way
  // Metadata fails an inconsistent config.
  std::error_code ec;
  fs::create_directories(options_.dir, ec);
  if (ec && !fs::is_directory(options_.dir)) {
    throw std::runtime_error("[disk_cache] cannot create " + options_.dir + ": " + ec.message());
  }
  if (!fs::is_directory(options_.dir)) {
    throw std::runtime_error("[disk_cache] not a directory: " + options_.dir);
  }
  // create_directories succeeds on a read-only parent that already holds the
  // directory, so probe the write we actually need.
  std::string const probe = options_.dir + ".ozonedb_disk_cache_probe";
  {
    std::ofstream out(probe, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      throw std::runtime_error("[disk_cache] not writable: " + options_.dir);
    }
  }
  fs::remove(probe, ec);
  if (options_.admission == Admission::kFrequency) {
    // Sized from the entries the budget holds: a file-mode entry is about one
    // SSTable, taken as 64 MiB here (Task 4 uses entry_bytes in chunk mode).
    uint64_t const unit = 64u << 20;
    uint64_t const expected = std::max<uint64_t>(16, options_.capacity_bytes / unit);
    uint64_t const window = options_.admit_window ? options_.admit_window : 8 * expected;
    sketch_ = std::make_unique<FrequencySketch>(static_cast<size_t>(std::min<uint64_t>(4 * expected, 1u << 22)), window);
  }
}

DiskCacheStorage::~DiskCacheStorage() {
  stopFillWorker();
  std::lock_guard<std::mutex> lk(parts_mtx_);
  parts_.clear();
}

bool DiskCacheStorage::cacheable(std::string const& name) const {
  return options_.capacity_bytes > 0 && name.size() >= options_.prefix.size() && name.compare(0, options_.prefix.size(), options_.prefix) == 0;
}

void DiskCacheStorage::createDirectory(std::string name) {
  backing_->createDirectory(name);
  if (cacheable(name)) {
    std::error_code ec;
    fs::create_directories(localPath(name), ec);
  }
}

Status DiskCacheStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  Status s = backing_->append(fileName, data, length);
  if (cacheable(fileName)) {
    if (s == Status::kSuccess) {
      writePart(fileName, data, static_cast<size_t>(length));
      if (publishPart(fileName)) writethrough_files_.fetch_add(1);  // append() leaves the object complete
    } else {
      discardPart(fileName);
    }
  }
  return s;
}

Status DiskCacheStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  Status s = backing_->appendNoFlush(fileName, data, length);
  if (cacheable(fileName) && s == Status::kSuccess) writePart(fileName, data, static_cast<size_t>(length));
  return s;
}

Status DiskCacheStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  Status s = backing_->appendInBatch(fileName, data, length);
  if (cacheable(fileName) && s == Status::kSuccess) writePart(fileName, data, static_cast<size_t>(length));
  return s;
}

Status DiskCacheStorage::flush(std::string const& fileName) {
  Status s = backing_->flush(fileName);
  if (!cacheable(fileName)) return s;
  if (s == Status::kSuccess) {
    if (publishPart(fileName)) writethrough_files_.fetch_add(1);
  } else {
    discardPart(fileName);
  }
  return s;
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  if (!cacheable(fileName)) {
    passthrough_.fetch_add(1);
    return backing_->read(fileName, data, size);
  }
  recordAccess(fileName);
  if (touch(fileName)) {
    std::error_code ec;
    auto n = fs::file_size(localPath(fileName), ec);
    if (!ec && readLocal(fileName, data, 0, static_cast<size_t>(n))) {
      size = static_cast<size_t>(n);
      hits_.fetch_add(1);
      hit_bytes_.fetch_add(size);
      return Status::kSuccess;
    }
    invalidate(fileName);
  }
  Status s = backing_->read(fileName, data, size);
  misses_.fetch_add(1);
  if (s == Status::kSuccess) {
    miss_bytes_.fetch_add(size);
    fetch_bytes_.fetch_add(size);
    enqueueFill(fileName);
  }
  return s;
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  if (!cacheable(fileName)) {
    passthrough_.fetch_add(1);
    return backing_->read(fileName, data, a, length);
  }
  if (options_.mode == Mode::kChunk) return readChunked(fileName, data, a, length);
  recordAccess(fileName);
  if (touch(fileName)) {
    if (readLocal(fileName, data, a, length)) {
      hits_.fetch_add(1);
      hit_bytes_.fetch_add(length);
      return Status::kSuccess;
    }
    invalidate(fileName);  // the copy is short or unreadable: drop it
  }
  Status s = backing_->read(fileName, data, a, length);
  misses_.fetch_add(1);
  if (s == Status::kSuccess) {
    miss_bytes_.fetch_add(length);
    fetch_bytes_.fetch_add(length);
    enqueueFill(fileName);
  }
  return s;
}

size_t DiskCacheStorage::size(std::string fileName) {
  if (cacheable(fileName)) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (options_.mode == Mode::kChunk) {
      // chunks_ never holds a file under construction, so a builder appending
      // to a name still sees the backing's growing size.
      auto it = chunks_.find(fileName);
      if (it != chunks_.end()) return it->second.size;
    } else {
      auto it = index_.find(fileName);
      if (it != index_.end()) return it->second.bytes;
    }
  }
  return backing_->size(fileName);
}

void DiskCacheStorage::seal(std::string fileName) { backing_->seal(fileName); }
bool DiskCacheStorage::isSealed(std::string fileName) { return backing_->isSealed(fileName); }
void DiskCacheStorage::remove(std::string fileName) {
  backing_->remove(fileName);
  if (cacheable(fileName)) invalidate(fileName);
}
bool DiskCacheStorage::exist(std::string fileName) { return backing_->exist(fileName); }
void DiskCacheStorage::setRemoteAppendListener(RemoteAppendListener listener) { backing_->setRemoteAppendListener(std::move(listener)); }
long DiskCacheStorage::lastAppendAddressForThread() const { return backing_->lastAppendAddressForThread(); }
void DiskCacheStorage::sync() { backing_->sync(); }
void DiskCacheStorage::clearSync() { backing_->clearSync(); }
bool DiskCacheStorage::hasSyncToken() const { return backing_->hasSyncToken(); }

bool DiskCacheStorage::touch(std::string const& name) {
  std::lock_guard<std::mutex> lk(mtx_);
  auto it = index_.find(name);
  if (it == index_.end()) return false;
  lru_.splice(lru_.begin(), lru_, it->second.lru);
  return true;
}

bool DiskCacheStorage::present(std::string const& name) {
  std::lock_guard<std::mutex> lk(mtx_);
  return index_.count(name) != 0;
}

bool DiskCacheStorage::readLocal(std::string const& name, unsigned char*& data, size_t a, size_t length) {
  int fd = ::open(localPath(name).c_str(), O_RDONLY);
  if (fd < 0) return false;
  unsigned char* buf = new unsigned char[length];
  size_t done = 0;
  while (done < length) {
    ssize_t n = ::pread(fd, buf + done, length - done, static_cast<off_t>(a + done));
    if (n <= 0) break;
    done += static_cast<size_t>(n);
  }
  if (options_.drop_pages) ::posix_fadvise(fd, static_cast<off_t>(a), static_cast<off_t>(length), POSIX_FADV_DONTNEED);
  ::close(fd);
  if (done != length) {
    delete[] buf;
    return false;
  }
  data = buf;
  return true;
}

void DiskCacheStorage::writePart(std::string const& name, unsigned char const* data, size_t length) {
  std::lock_guard<std::mutex> lk(parts_mtx_);
  if (poisoned_parts_.count(name)) return;
  auto it = parts_.find(name);
  if (it == parts_.end()) {
    std::error_code ec;
    fs::create_directories(fs::path(partPath(name)).parent_path(), ec);
    auto out = std::make_unique<std::ofstream>(partPath(name), std::ios::binary | std::ios::trunc);
    if (!out->is_open()) {
      poisoned_parts_.insert(name);
      return;
    }
    it = parts_.emplace(name, std::move(out)).first;
  }
  it->second->write(reinterpret_cast<char const*>(data), static_cast<std::streamsize>(length));
  if (!*it->second) {
    parts_.erase(it);
    poisoned_parts_.insert(name);
    std::error_code ec;
    fs::remove(partPath(name), ec);
  }
}

bool DiskCacheStorage::publishPart(std::string const& name) {
  {
    std::lock_guard<std::mutex> lk(parts_mtx_);
    if (poisoned_parts_.erase(name)) {
      std::error_code ec;
      fs::remove(partPath(name), ec);
      return false;
    }
    auto it = parts_.find(name);
    if (it == parts_.end()) return false;
    it->second->flush();
    bool ok = static_cast<bool>(*it->second);
    parts_.erase(it);  // closes the stream
    if (!ok) {
      std::error_code ec;
      fs::remove(partPath(name), ec);
      return false;
    }
  }
  return publishPartFile(name, partPath(name), /*expected_size=*/0, writeThroughMode());
}

bool DiskCacheStorage::publishPartFile(std::string const& name, std::string const& part_path, size_t expected_size, AdmitMode how) {
  std::error_code ec;
  auto bytes = fs::file_size(part_path, ec);
  if (ec) return false;  // the part vanished from under us (raced a discard/invalidate/rename)
  if (expected_size != 0 && static_cast<size_t>(bytes) != expected_size) {
    fs::remove(part_path, ec);
    return false;
  }
  if (bytes > options_.capacity_bytes) {
    fs::remove(part_path, ec);
    fill_skipped_budget_.fetch_add(1);
    return false;
  }
  fs::rename(part_path, localPath(name), ec);
  if (ec) {
    fs::remove(part_path, ec);
    return false;
  }
  if (!admit(name, static_cast<size_t>(bytes), how)) {
    fs::remove(localPath(name), ec);  // refused: nothing indexes the copy, so it must not stay
    return false;
  }
  return true;  // the caller counts a write-through or a fill
}

void DiskCacheStorage::discardPart(std::string const& name) {
  std::lock_guard<std::mutex> lk(parts_mtx_);
  std::error_code ec;
  // Poison rather than forget, whenever a write-through was actually in
  // flight. The builder may still be appending: without the poison the next
  // writePart would reopen the .part with `trunc` and the following flush
  // would publish the tail of the file as a complete copy (final review of
  // PLAN-disk-cache, finding 7). publishPart clears the poison and publishes
  // nothing, so the set only ever holds the write-throughs that were
  // interrupted and is emptied by their own flush. Discarding a name with no
  // part in flight -- which is what invalidate() does on every peer COMPACT
  // input -- leaves nothing behind: a write-through that starts after this
  // point writes the whole file from byte 0 and is safe to publish.
  bool const had_stream = parts_.erase(name) != 0;
  bool const had_file = fs::remove(partPath(name), ec);  // true only if it was there
  if (had_stream || had_file) poisoned_parts_.insert(name);
}

void DiskCacheStorage::recordAccess(std::string const& key) {
  if (!sketch_) return;
  std::lock_guard<std::mutex> lk(sketch_mtx_);
  sketch_->record(key);
}

uint32_t DiskCacheStorage::frequency(std::string const& key) {
  if (!sketch_) return 0;
  std::lock_guard<std::mutex> lk(sketch_mtx_);
  return sketch_->estimate(key);
}

bool DiskCacheStorage::mayTakeLocked(std::string const& key, size_t bytes, std::string const& victim, AdmitMode how) {
  if (current_bytes_ + bytes <= options_.capacity_bytes) return true;  // free budget: no contest
  if (how == AdmitMode::kForce) return true;
  bool const allowed = how == AdmitMode::kContest && !victim.empty() && frequency(key) > frequency(victim);
  if (!allowed) admit_rejected_.fetch_add(1);
  return allowed;
}

size_t DiskCacheStorage::sizeOf(std::string const& name) {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = sizes_.find(name);
    if (it != sizes_.end()) return it->second;
  }
  size_t const n = backing_->size(name);
  if (n > 0) {
    std::lock_guard<std::mutex> lk(mtx_);
    sizes_.emplace(name, n);
  }
  return n;
}

bool DiskCacheStorage::admit(std::string const& name, size_t bytes, AdmitMode how) {
  std::lock_guard<std::mutex> lk(mtx_);
  if (bytes > options_.capacity_bytes) return false;
  auto old = index_.find(name);
  if (old != index_.end()) {  // re-created name: replace the accounting
    current_bytes_ -= old->second.bytes;
    lru_.erase(old->second.lru);
    index_.erase(old);
  }
  if (!mayTakeLocked(name, bytes, lru_.empty() ? std::string() : lru_.back(), how)) return false;
  evictToFitLocked(bytes);
  lru_.push_front(name);
  index_[name] = Entry{bytes, lru_.begin()};
  current_bytes_ += bytes;
  return true;
}

void DiskCacheStorage::evictToFitLocked(size_t bytes) {
  while (current_bytes_ + bytes > options_.capacity_bytes && !lru_.empty()) {
    std::string victim = lru_.back();
    evictions_.fetch_add(1);
    evicted_bytes_.fetch_add(index_[victim].bytes);
    eraseLocked(victim, /*count_as_invalidated=*/false);
  }
}

void DiskCacheStorage::eraseLocked(std::string const& name, bool count_as_invalidated) {
  sizes_.erase(name);
  auto it = index_.find(name);
  if (it == index_.end()) return;
  current_bytes_ -= it->second.bytes;
  lru_.erase(it->second.lru);
  index_.erase(it);
  std::error_code ec;
  fs::remove(localPath(name), ec);
  if (count_as_invalidated) invalidated_.fetch_add(1);
}

void DiskCacheStorage::invalidate(std::string const& name) {
  // The peer path (a COMPACT apply) hands us every input of the compaction,
  // which includes datalog* names the tier never cached. Poisoning one of
  // those would be harmless but pointless bookkeeping that never gets
  // cleared, since publishPart only runs for cacheable names.
  if (!cacheable(name)) return;
  discardPart(name);
  std::lock_guard<std::mutex> lk(mtx_);
  if (options_.mode == Mode::kChunk) {
    // dropChunkFileLocked subtracts only the present chunks: the writer of
    // each pending chunk subtracts its own bytes when it finds no entry.
    dropChunkFileLocked(name, /*unlink=*/true, /*count_as_invalidated=*/true);
    return;
  }
  eraseLocked(name, /*count_as_invalidated=*/true);
}

void DiskCacheStorage::enqueueFill(std::string const& name) {
  std::lock_guard<std::mutex> lk(fill_mtx_);
  if (queued_.count(name)) return;
  while (fill_queue_.size() >= options_.max_queue && !fill_queue_.empty()) {
    queued_.erase(fill_queue_.front());
    fill_queue_.pop_front();
    fill_dropped_.fetch_add(1);
  }
  fill_queue_.push_back(name);
  queued_.insert(name);
  fill_cv_.notify_all();
}

void DiskCacheStorage::startFillWorker() {
  if (options_.mode == Mode::kChunk) return;  // the miss path is the fill
  std::lock_guard<std::mutex> life(fill_lifecycle_mtx_);  // serialised against a concurrent stop's join
  std::lock_guard<std::mutex> lk(fill_mtx_);
  if (fill_started_) return;
  fill_started_ = true;
  fill_stop_ = false;
  fill_thread_ = std::thread([this] { fillLoop(); });
}

void DiskCacheStorage::stopFillWorker() {
  std::lock_guard<std::mutex> life(fill_lifecycle_mtx_);  // only one stop (or start) proceeds at a time
  {
    std::lock_guard<std::mutex> lk(fill_mtx_);
    if (!fill_started_) return;  // a racing stop already finished under the lifecycle lock
    fill_stop_ = true;
    fill_stopping_ = true;
    fill_cv_.notify_all();  // wakes a waitFillIdle() parked on a non-empty queue
  }
  fill_thread_.join();
  {
    std::lock_guard<std::mutex> lk(fill_mtx_);
    fill_started_ = false;
    fill_stopping_ = false;
  }
  fill_cv_.notify_all();  // wakes anyone whose predicate only became true once fill_started_ cleared
}

void DiskCacheStorage::waitFillIdle() {
  std::unique_lock<std::mutex> lk(fill_mtx_);
  fill_cv_.wait(lk, [this] { return !fill_started_ || fill_stopping_ || (fill_queue_.empty() && !fill_busy_); });
}

void DiskCacheStorage::fillLoop() {
  for (;;) {
    std::string name;
    {
      std::unique_lock<std::mutex> lk(fill_mtx_);
      fill_cv_.wait(lk, [this] { return fill_stop_ || !fill_queue_.empty(); });
      if (fill_stop_) return;
      name = fill_queue_.front();
      fill_queue_.pop_front();
      queued_.erase(name);
      fill_busy_ = true;
    }
    fillOne(name);
    {
      std::lock_guard<std::mutex> lk(fill_mtx_);
      fill_busy_ = false;
    }
    fill_cv_.notify_all();
  }
}

void DiskCacheStorage::fillOne(std::string const& name) {
  if (present(name)) {
    fill_skipped_present_.fetch_add(1);
    return;
  }
  {
    std::lock_guard<std::mutex> lk(parts_mtx_);
    if (parts_.count(name)) return;  // the builder is writing it through right now
  }
  size_t const total = sizeOf(name);
  if (total == 0) {
    fill_gone_.fetch_add(1);
    return;
  }
  if (total > options_.capacity_bytes) {
    fill_skipped_budget_.fetch_add(1);
    return;
  }
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!mayTakeLocked(name, total, lru_.empty() ? std::string() : lru_.back(), fillMode())) return;  // refused before a byte moves
  }
  // The fill owns this stream outright: it never touches parts_/writePart/
  // discardPart, so a concurrent write-through or a peer invalidate() (which
  // only ever acts on partPath(name)) can neither truncate nor interleave
  // into it (review finding, PLAN-disk-cache T3).
  std::string const tmp = fillPartPath(name);
  std::error_code ec;
  fs::create_directories(fs::path(tmp).parent_path(), ec);
  std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    fill_failed_.fetch_add(1);
    return;
  }
  for (size_t off = 0; off < total; off += options_.chunk_bytes) {
    size_t const len = std::min(options_.chunk_bytes, total - off);
    unsigned char* buf = nullptr;
    Status s = backing_->read(name, buf, off, len);
    fill_gets_.fetch_add(1);
    if (s != Status::kSuccess || buf == nullptr) {
      out.close();
      fs::remove(tmp, ec);
      fill_failed_.fetch_add(1);
      return;
    }
    fetch_bytes_.fetch_add(len);
    out.write(reinterpret_cast<char const*>(buf), static_cast<std::streamsize>(len));
    delete[] buf;
  }
  out.flush();
  bool const write_ok = static_cast<bool>(out);
  out.close();
  if (!write_ok) {
    fs::remove(tmp, ec);
    fill_failed_.fetch_add(1);
    return;
  }
  // Verify the part's byte count against the backing's before publishing: a
  // mismatch (or the part having vanished) fails closed instead of admitting
  // a truncated file as complete.
  if (publishPartFile(name, tmp, total, fillMode())) {
    fills_.fetch_add(1);
    fill_bytes_.fetch_add(total);
  } else {
    fill_failed_.fetch_add(1);
  }
}

size_t DiskCacheStorage::reconcile(std::function<bool(std::string const&, size_t)> const& live) {
  size_t removed = 0;
  std::error_code ec;
  if (!fs::exists(options_.dir, ec)) return 0;
  if (options_.mode == Mode::kChunk) {
    // Which chunks of a leftover sparse file hold data is not recorded, so a
    // warm restart is not possible: start cold and drop every local file.
    {
      std::lock_guard<std::mutex> lk(mtx_);
      chunks_.clear();
      ring_.clear();
      hand_file_ = hand_chunk_ = 0;
      present_chunks_ = 0;
      current_bytes_ = 0;
      sizes_.clear();
    }
    for (auto it = fs::recursive_directory_iterator(options_.dir, ec); !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
      if (!it->is_regular_file(ec)) continue;
      fs::remove(it->path(), ec);
      ++removed;
    }
    return removed;
  }
  auto ends_with = [](std::string const& s, std::string const& suffix) {
    return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
  };
  struct Found {
    std::string name;
    size_t bytes;
    fs::file_time_type mtime;
  };
  std::vector<Found> keep;
  for (auto it = fs::recursive_directory_iterator(options_.dir, ec); !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
    if (!it->is_regular_file(ec)) continue;
    std::string const rel = fs::relative(it->path(), options_.dir, ec).generic_string();
    if (ec) continue;
    // Both the write-through's ".part" and the fill's own ".fillpart" are
    // in-progress leftovers from before the last close and are dropped
    // unconditionally, without asking `live` (Task 3 review decision).
    bool const is_part = ends_with(rel, ".part") || ends_with(rel, ".fillpart");
    size_t const bytes = static_cast<size_t>(it->file_size(ec));
    if (is_part || !cacheable(rel) || !live(rel, bytes)) {
      fs::remove(it->path(), ec);
      ++removed;
      continue;
    }
    keep.push_back({rel, bytes, it->last_write_time(ec)});
  }
  std::sort(keep.begin(), keep.end(), [](Found const& a, Found const& b) { return a.mtime < b.mtime; });
  for (Found const& k : keep) {
    if (!admit(k.name, k.bytes, AdmitMode::kForce)) {  // larger than the capacity
      fs::remove(localPath(k.name), ec);
      ++removed;
    }
  }
  return removed;
}

DiskCacheStorage::Stats DiskCacheStorage::stats() {
  Stats s;
  s.hits = hits_.load();
  s.misses = misses_.load();
  s.hit_bytes = hit_bytes_.load();
  s.miss_bytes = miss_bytes_.load();
  s.passthrough = passthrough_.load();
  s.fills = fills_.load();
  s.fill_bytes = fill_bytes_.load();
  s.fill_gets = fill_gets_.load();
  s.fill_skipped_budget = fill_skipped_budget_.load();
  s.fill_skipped_present = fill_skipped_present_.load();
  s.fill_gone = fill_gone_.load();
  s.fill_failed = fill_failed_.load();
  s.fill_dropped = fill_dropped_.load();
  s.writethrough_files = writethrough_files_.load();
  s.evictions = evictions_.load();
  s.evicted_bytes = evicted_bytes_.load();
  s.invalidated = invalidated_.load();
  s.fetch_bytes = fetch_bytes_.load();
  s.admit_rejected = admit_rejected_.load();
  s.punch_failed = punch_failed_.load();
  std::lock_guard<std::mutex> lk(mtx_);
  if (options_.mode == Mode::kChunk) {
    s.files = chunks_.size();
    s.entries = present_chunks_;
  } else {
    s.files = index_.size();
    s.entries = index_.size();
  }
  s.bytes = current_bytes_;
  s.capacity = options_.capacity_bytes;
  return s;
}

void DiskCacheStorage::printStats() {
  Stats s = stats();
  std::cerr << "[disk_cache] hits=" << s.hits << " misses=" << s.misses
            << " hit_bytes=" << s.hit_bytes << " miss_bytes=" << s.miss_bytes
            << " passthrough=" << s.passthrough << " fills=" << s.fills
            << " fill_bytes=" << s.fill_bytes << " fill_gets=" << s.fill_gets
            << " fill_skipped_budget=" << s.fill_skipped_budget
            << " fill_skipped_present=" << s.fill_skipped_present
            << " fill_gone=" << s.fill_gone << " fill_failed=" << s.fill_failed
            << " fill_dropped=" << s.fill_dropped
            << " writethrough_files=" << s.writethrough_files
            << " evictions=" << s.evictions << " evicted_bytes=" << s.evicted_bytes
            << " invalidated=" << s.invalidated << " files=" << s.files
            << " bytes=" << s.bytes << " capacity=" << s.capacity
            << " fetch_bytes=" << s.fetch_bytes << " admit_rejected=" << s.admit_rejected
            << " entries=" << s.entries
            << " mode=" << (options_.mode == Mode::kChunk ? "chunk" : "file")
            << " entry_bytes=" << (options_.mode == Mode::kChunk ? options_.entry_bytes : 0u)
            << " punch_failed=" << s.punch_failed << "\n";
}

Status DiskCacheStorage::readChunked(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  size_t const E = options_.entry_bytes;
  recordAccess(chunkKey(fileName, a / E));
  bool hit = false;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = chunks_.find(fileName);
    if (it != chunks_.end() && a + length <= it->second.size && chunksPresentLocked(it->second, a / E, (a + length + E - 1) / E)) hit = true;
  }
  if (hit) {
    if (readLocal(fileName, data, a, length)) {
      hits_.fetch_add(1);
      hit_bytes_.fetch_add(length);
      return Status::kSuccess;
    }
    invalidate(fileName);  // the copy is unreadable: drop it
  }
  misses_.fetch_add(1);
  size_t const total = sizeOf(fileName);
  if (total == 0 || a + length > total) {  // unknown object, or a read past its end: the backing decides
    Status s = backing_->read(fileName, data, a, length);
    if (s == Status::kSuccess) {
      miss_bytes_.fetch_add(length);
      fetch_bytes_.fetch_add(length);
    }
    return s;
  }
  size_t const c0 = a / E, c1 = (a + length + E - 1) / E;
  size_t const off = c0 * E;
  size_t const len = std::min(c1 * E, total) - off;
  unsigned char* buf = nullptr;
  Status s = backing_->read(fileName, buf, off, len);
  if (s != Status::kSuccess || buf == nullptr) {
    delete[] buf;
    return s == Status::kSuccess ? Status::kFailure : s;
  }
  miss_bytes_.fetch_add(length);
  fetch_bytes_.fetch_add(len);
  data = new unsigned char[length];
  std::memcpy(data, buf + (a - off), length);

  // Reserve the absent chunks under mtx_, write them outside it, then flip
  // kPending to kPresent under mtx_ again.
  std::vector<size_t> to_write;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    ChunkFile& cf = chunks_[fileName];
    if (cf.state.empty()) {
      cf.size = total;
      cf.state.assign(chunkCount(total), 0);
      cf.ring = ring_.size();
      ring_.push_back(fileName);
    }
    for (size_t c = c0; c < c1; ++c) {
      if (cf.state[c] & (kPresent | kPending)) {
        fill_skipped_present_.fetch_add(1);
        continue;
      }
      if (reserveChunkLocked(fileName, cf, c, fillMode())) to_write.push_back(c);
    }
    if (cf.present == 0 && cf.pending == 0) dropChunkFileLocked(fileName, /*unlink=*/false, false);  // nothing kept: no empty entry
  }
  uint64_t written = 0, wrote = 0;
  for (size_t c : to_write) {
    size_t const n = chunkBytes(total, c);
    bool const ok = pwriteLocal(fileName, c * E, buf + (c * E - off), n);
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = chunks_.find(fileName);
    if (it == chunks_.end()) {  // invalidated meanwhile: the pwrite re-created a dead file
      std::error_code ec;
      fs::remove(localPath(fileName), ec);
      current_bytes_ -= n;
      continue;
    }
    ChunkFile& cf = it->second;
    cf.state[c] &= static_cast<uint8_t>(~kPending);
    --cf.pending;
    if (ok) {
      cf.state[c] |= kPresent | kReferenced;
      ++cf.present;
      ++present_chunks_;
      ++wrote;
      written += n;
    } else {
      current_bytes_ -= n;
      fill_failed_.fetch_add(1);
      if (cf.present == 0 && cf.pending == 0) dropChunkFileLocked(fileName, /*unlink=*/true, false);
    }
  }
  delete[] buf;
  fills_.fetch_add(wrote);
  fill_bytes_.fetch_add(written);
  return Status::kSuccess;
}

bool DiskCacheStorage::chunksPresentLocked(ChunkFile& cf, size_t c0, size_t c1) {
  if (c1 > cf.state.size()) return false;
  for (size_t c = c0; c < c1; ++c) {
    if (!(cf.state[c] & kPresent)) return false;
  }
  for (size_t c = c0; c < c1; ++c) cf.state[c] |= kReferenced;
  return true;
}

bool DiskCacheStorage::reserveChunkLocked(std::string const& name, ChunkFile& cf, size_t c, AdmitMode how) {
  size_t const need = chunkBytes(cf.size, c);
  if (need > options_.capacity_bytes) {
    fill_skipped_budget_.fetch_add(1);
    return false;
  }
  // Reserve first: while this chunk is kPending its file cannot be dropped
  // by an eviction below, so `cf` stays valid.
  cf.state[c] |= kPending;
  ++cf.pending;
  current_bytes_ += need;
  while (current_bytes_ > options_.capacity_bytes) {
    std::string vname;
    size_t vchunk = 0;
    if (!clockVictimLocked(vname, vchunk) || !mayTakeLocked(chunkKey(name, c), 0, chunkKey(vname, vchunk), how)) {
      cf.state[c] &= static_cast<uint8_t>(~kPending);
      --cf.pending;
      current_bytes_ -= need;
      if (vname.empty()) admit_rejected_.fetch_add(1);  // nothing evictable: counted here, mayTakeLocked did not run
      return false;
    }
    evictChunkLocked(vname, vchunk);
  }
  return true;
}

bool DiskCacheStorage::clockVictimLocked(std::string& name, size_t& chunk) {
  size_t wraps = 0;
  while (wraps < 3 && !ring_.empty()) {
    if (hand_file_ >= ring_.size()) {
      hand_file_ = 0;
      hand_chunk_ = 0;
      ++wraps;
      continue;
    }
    ChunkFile& cf = chunks_[ring_[hand_file_]];
    if (cf.present == 0) {
      ++hand_file_;
      hand_chunk_ = 0;
      continue;
    }
    for (; hand_chunk_ < cf.state.size(); ++hand_chunk_) {
      uint8_t& st = cf.state[hand_chunk_];
      if (!(st & kPresent)) continue;
      if (st & kReferenced) {
        st &= static_cast<uint8_t>(~kReferenced);
        continue;
      }
      name = ring_[hand_file_];
      chunk = hand_chunk_++;
      return true;
    }
    ++hand_file_;
    hand_chunk_ = 0;
  }
  return false;
}

void DiskCacheStorage::evictChunkLocked(std::string const& name, size_t c) {
  auto it = chunks_.find(name);
  if (it == chunks_.end()) return;
  ChunkFile& cf = it->second;
  size_t const n = chunkBytes(cf.size, c);
  if (!punchHole(name, c * options_.entry_bytes, n)) punch_failed_.fetch_add(1);  // the state byte is what says "absent"
  cf.state[c] &= static_cast<uint8_t>(~(kPresent | kReferenced));
  --cf.present;
  --present_chunks_;
  current_bytes_ -= n;
  evictions_.fetch_add(1);
  evicted_bytes_.fetch_add(n);
  if (cf.present == 0 && cf.pending == 0) dropChunkFileLocked(name, /*unlink=*/true, false);
}

void DiskCacheStorage::dropChunkFileLocked(std::string const& name, bool unlink, bool count_as_invalidated) {
  sizes_.erase(name);
  auto it = chunks_.find(name);
  if (it == chunks_.end()) return;
  ChunkFile& cf = it->second;
  for (size_t c = 0; c < cf.state.size(); ++c) {
    if (cf.state[c] & kPresent) current_bytes_ -= chunkBytes(cf.size, c);
  }
  present_chunks_ -= cf.present;
  // Swap-remove the ring slot. The hand tolerates it: at worst the moved
  // file gets one extra second chance.
  size_t const slot = cf.ring;
  size_t const last = ring_.size() - 1;
  if (slot != last) {
    ring_[slot] = ring_[last];
    chunks_[ring_[slot]].ring = slot;
  }
  ring_.pop_back();
  if (hand_file_ == slot) hand_chunk_ = 0;
  chunks_.erase(it);
  if (unlink) {
    std::error_code ec;
    fs::remove(localPath(name), ec);
  }
  if (count_as_invalidated) invalidated_.fetch_add(1);
}

bool DiskCacheStorage::pwriteLocal(std::string const& name, size_t off, unsigned char const* buf, size_t len) {
  std::error_code ec;
  fs::create_directories(fs::path(localPath(name)).parent_path(), ec);
  int fd = ::open(localPath(name).c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd < 0) return false;
  size_t done = 0;
  while (done < len) {
    ssize_t n = ::pwrite(fd, buf + done, len - done, static_cast<off_t>(off + done));
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    done += static_cast<size_t>(n);
  }
  if (options_.drop_pages) ::posix_fadvise(fd, static_cast<off_t>(off), static_cast<off_t>(len), POSIX_FADV_DONTNEED);
  ::close(fd);
  return done == len;
}

bool DiskCacheStorage::punchHole(std::string const& name, size_t off, size_t len) {
  int fd = ::open(localPath(name).c_str(), O_WRONLY);
  if (fd < 0) return false;
  int const rc = ::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, static_cast<off_t>(off), static_cast<off_t>(len));
  ::close(fd);
  return rc == 0;
}

}  // namespace ozonedb
