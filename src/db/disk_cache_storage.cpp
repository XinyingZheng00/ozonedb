// src/db/disk_cache_storage.cpp
#include "disk_cache_storage.h"

#include <fcntl.h>
#include <unistd.h>

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
  // Task 2 adds the write-through.
  return backing_->append(fileName, data, length);
}

Status DiskCacheStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  return backing_->appendNoFlush(fileName, data, length);
}

Status DiskCacheStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  return backing_->appendInBatch(fileName, data, length);
}

Status DiskCacheStorage::flush(std::string const& fileName) {
  return backing_->flush(fileName);
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  if (!cacheable(fileName)) passthrough_.fetch_add(1);
  return backing_->read(fileName, data, size);
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  if (!cacheable(fileName)) passthrough_.fetch_add(1);
  return backing_->read(fileName, data, a, length);
}

size_t DiskCacheStorage::size(std::string fileName) { return backing_->size(fileName); }
void DiskCacheStorage::seal(std::string fileName) { backing_->seal(fileName); }
bool DiskCacheStorage::isSealed(std::string fileName) { return backing_->isSealed(fileName); }
void DiskCacheStorage::remove(std::string fileName) { backing_->remove(fileName); }
bool DiskCacheStorage::exist(std::string fileName) { return backing_->exist(fileName); }
void DiskCacheStorage::setRemoteAppendListener(RemoteAppendListener listener) { backing_->setRemoteAppendListener(std::move(listener)); }
long DiskCacheStorage::lastAppendAddressForThread() const { return backing_->lastAppendAddressForThread(); }
void DiskCacheStorage::sync() { backing_->sync(); }
void DiskCacheStorage::clearSync() { backing_->clearSync(); }
bool DiskCacheStorage::hasSyncToken() const { return backing_->hasSyncToken(); }

// --- tier API stubs completed in Tasks 2-5 ---
void DiskCacheStorage::invalidate(std::string const&) {}
size_t DiskCacheStorage::reconcile(std::function<bool(std::string const&, size_t)> const&) { return 0; }
void DiskCacheStorage::startFillWorker() {}
void DiskCacheStorage::stopFillWorker() {}
void DiskCacheStorage::waitFillIdle() {}

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
