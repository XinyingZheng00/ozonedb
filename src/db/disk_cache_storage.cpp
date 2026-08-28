// src/db/disk_cache_storage.cpp
#include "disk_cache_storage.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <system_error>
#include <vector>

namespace ozonedb {

namespace fs = std::filesystem;

DiskCacheStorage::DiskCacheStorage(std::unique_ptr<Storage> backing, Options options)
    : Storage(options.dir), backing_(std::move(backing)), options_(std::move(options)) {
  if (!options_.dir.empty() && options_.dir.back() != '/') options_.dir += '/';
  storage_path = options_.dir;
  std::error_code ec;
  fs::create_directories(options_.dir, ec);
  if (ec) std::cerr << "[disk_cache] cannot create " << options_.dir << ": " << ec.message() << "\n";
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
    enqueueFill(fileName);
  }
  return s;
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  if (!cacheable(fileName)) {
    passthrough_.fetch_add(1);
    return backing_->read(fileName, data, a, length);
  }
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
    enqueueFill(fileName);
  }
  return s;
}

size_t DiskCacheStorage::size(std::string fileName) {
  if (cacheable(fileName)) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = index_.find(fileName);
    if (it != index_.end()) return it->second.bytes;
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
  return publishPartFile(name, partPath(name), /*expected_size=*/0);
}

bool DiskCacheStorage::publishPartFile(std::string const& name, std::string const& part_path, size_t expected_size) {
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
  return admit(name, static_cast<size_t>(bytes));  // the caller counts a write-through or a fill
}

void DiskCacheStorage::discardPart(std::string const& name) {
  std::lock_guard<std::mutex> lk(parts_mtx_);
  parts_.erase(name);
  poisoned_parts_.erase(name);
  std::error_code ec;
  fs::remove(partPath(name), ec);
}

bool DiskCacheStorage::admit(std::string const& name, size_t bytes) {
  std::lock_guard<std::mutex> lk(mtx_);
  if (bytes > options_.capacity_bytes) return false;
  auto old = index_.find(name);
  if (old != index_.end()) {  // re-created name: replace the accounting
    current_bytes_ -= old->second.bytes;
    lru_.erase(old->second.lru);
    index_.erase(old);
  }
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
  discardPart(name);
  std::lock_guard<std::mutex> lk(mtx_);
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
  size_t const total = backing_->size(name);
  if (total == 0) {
    fill_gone_.fetch_add(1);
    return;
  }
  if (total > options_.capacity_bytes) {
    fill_skipped_budget_.fetch_add(1);
    return;
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
  if (publishPartFile(name, tmp, total)) {
    fills_.fetch_add(1);
    fill_bytes_.fetch_add(total);
  } else {
    fill_failed_.fetch_add(1);
  }
}

// --- tier API stubs completed in Tasks 4-5 ---
size_t DiskCacheStorage::reconcile(std::function<bool(std::string const&, size_t)> const&) { return 0; }

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
  std::lock_guard<std::mutex> lk(mtx_);
  s.files = index_.size();
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
            << " bytes=" << s.bytes << " capacity=" << s.capacity << "\n";
}

}  // namespace ozonedb
