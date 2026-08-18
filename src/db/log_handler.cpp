#include "log_handler.h"
#include "db.h"
#include "helper.h"
#include <google/protobuf/message.h>
#include <future>
#include <mutex>
#include <regex>
#include <unordered_map>
#include <vector>

namespace ozonedb {
Status LogHandler::newTail() {
  if (!this->active_unit.empty()) {
    this->storage->seal(this->active_unit);
  }
  View view;
  metadata_log->getLatestView(view);
  std::string current_tail = view.current_log_tail;
  // find the number: current tail is in the format of prefix/number
  int log_number;
  if (current_tail.empty()) {
    log_number = 0;
  } else {
    if (view.getFileSize(current_tail) < this->file_size_limit) {
      // do not create new tail if the current tail is not sealed
      this->active_unit = current_tail;
      return Status::kSuccess;
    }
    log_number = std::stoi(getSuffix(current_tail));
  }
  std::string name = this->prefix + "/" + std::to_string(log_number + 1);
  OperationRecord record;
  record.set_op_type(OperationRecord::LOGCREATE);
  record.add_input_files(current_tail);
  record.add_output_file(name);
  // Ship the sealed tail's size so peer rollforward can refresh its
  // file_size map without calling storage->size(input_file) under
  // view_mutex. We use the emitter's locally cached size, which is
  // already >= file_size_limit (checked at :24). Heuristic-only:
  // feeds getLatestScore(), not correctness.
  if (!current_tail.empty()) {
    record.set_sealed_input_bytes(
        static_cast<int64_t>(view.getFileSize(current_tail)));
  }
  this->metadata_log->appendToMetadataLog(record);
  view = this->metadata_log->rollForwardMetadataLog();
  this->active_unit = view.current_log_tail;
  if (this->metadata_log->event_listener != nullptr) {
    this->metadata_log->event_listener->onNewTail();
  }
  return Status::kSuccess;
}

Status LogHandler::addRecord(Record const& record) {
  int buffer_size;
  unsigned char* buffer = protobuf::serializeMessage(record, buffer_size);
  if (this->active_unit.empty()) {
    newTail();
  }

  // Outer retry loop handles the multi-writer race where a concurrent
  // writer rolls the log (LOGCREATE) while our append is in flight.
  // When that happens our record lands on the now-stale tail whose
  // view-frozen file_size won't include it — the record becomes
  // invisible. Detect via the post-append tail check and re-issue
  // against the new active_unit. Paper §5.2 state-independent ingest.
  constexpr int kMaxRetries = 8;
  std::string final_target;
  for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
    std::string target = this->active_unit;

    while (this->storage->appendInBatch(target, buffer, buffer_size) == Status::kSealed) {
      newTail();
      target = this->active_unit;
    }

    View view;
    metadata_log->getLatestView(view);
    if (view.current_log_tail == target &&
        view.getFileSize(target) < this->file_size_limit) {
      final_target = target;
      break;
    }

    // Tail moved or the file is full. Roll forward and retry the record
    // on the new tail so the previous (now-orphaned) append doesn't
    // silently lose data.
    while (view.getFileSize(this->active_unit) >= this->file_size_limit ||
           view.current_log_tail != this->active_unit) {
      newTail();
      metadata_log->getLatestView(view);
    }
  }

  delete[] buffer;
  buffer = nullptr;

  // Push the just-written record into the cache + index so subsequent
  // readRecord calls short-circuit the fenced multi-file scan. Cache
  // and index co-own via shared_ptr — readers that lift the pointer
  // out of the index keep their copy alive even if compaction or a
  // peer REMOVE invalidates the cache entry concurrently.
  if (key_index_ && !final_target.empty()) {
    auto clone = std::make_shared<Record>(record);
    cache->putLogRecordSingle(final_target, clone);
    key_index_->upsert(record.key(), std::move(clone), final_target);
  }
  return Status::kSuccess;
}

bool LogHandler::tryIndexLookup(std::string const& key, std::shared_ptr<Record>& record) {
  if (!key_index_) return false;
  auto hit = key_index_->lookup(key);
  if (!hit) return false;
  record = std::move(hit);
  return true;
}

Status LogHandler::readRecord(std::string const& key, std::shared_ptr<Record>& record, std::string const& offset, std::string& latest_offset) {
  // Fast path: check the in-memory key index first. This skips the
  // multi-file backward scan (and, on Corfu, the per-file fenced
  // storage->size() call in checkReadMoreLog). On miss, fall through
  // to the existing scan and backfill on success. Bypassed entirely in
  // linearizable mode — see the linearizable_reads_ member comment.
  if (key_index_ && !linearizable_reads_) {
    if (auto hit = key_index_->lookup(key)) {
      record = std::move(hit);
      return Status::kSuccess;
    }
    // Trust-the-tailer mode: skip the synchronous multi-file log scan
    // (and the fenced storage->size() call it triggers on Corfu). The
    // index is kept fresh by local addRecord and the tailer's
    // onRemoteAppend callback, so a miss here is a true log miss and
    // the DB layer will fall through to SSTables.
    if (trust_background_tail_) {
      return Status::kFailure;
    }
  }
  auto const& files = this->latest_view->getWithPrefix(this->prefix);
  if (files.empty()) {
    // std::cout << "No files found with prefix " << this->prefix << std::endl;
    return Status::kFailure;
  }
  std::shared_mutex record_mutex;
  int finished_threads = 0;
  int record_file = -1;
  std::mutex cv_mutex;
  std::condition_variable cv;

  int count = 0;
  for (int i = files.size() - 1; i >= 0; i--) {
    std::string const& file_name = files[i];
    // if (i == files.size() - 1 && !this->latest_view.current_log_tail.rrent_log_tail != file_name) {
    //   if (offset == file_name) {
    //     break;
    //   }
    //   continue;  // The latest file may not be created yet
    // }
    bool read_more = true;  // not found or the file is tail(not sealed)
    size_t cached_offset = 0;
    size_t size = 0;
    this->cache->checkReadMoreLog(file_name, read_more, cached_offset, size);
    if (read_more) {
      count++;
      thread_pool->enqueue([this, file_name, cached_offset, size, key, i,
                            &record_file, &record, &record_mutex, &cv, &cv_mutex, &finished_threads]() {
        this->cache->readDataLog(file_name, cached_offset, size);
        std::shared_ptr<Record> record_tmp;
        cache->getLog(file_name, key, record_tmp);
        if (record_tmp) {
          std::unique_lock<std::shared_mutex> lock(record_mutex);
          if (i >= record_file) {
            record = std::move(record_tmp);
            record_file = i;
          }
        }
        {
          std::lock_guard<std::mutex> lock(cv_mutex);
          finished_threads++;
        }
        cv.notify_one();
      },
                           ThreadPool::Priority::High);
    } else {
      std::shared_ptr<Record> record_tmp;
      cache->getLog(file_name, key, record_tmp);
      if (record_tmp) {
        std::unique_lock<std::shared_mutex> lock(record_mutex);
        if (i >= record_file) {
          record = std::move(record_tmp);
          record_file = i;
        }
        break;
      }
    }
    if (offset == file_name) {
      break;
    }
  }

  // Wait for all threads to finish
  {
    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait(lock, [&] { return finished_threads == count; });
  }
  // Update the latest offset for the DB layer cache
  if (*files.rbegin() != offset) {
    latest_offset = *files.rbegin();
  }

  if (record) {
    // Backfill the index from the slow-path hit so the next lookup
    // for this key is O(1). Cache and index co-own via shared_ptr.
    if (key_index_ && record_file >= 0 &&
        record_file < static_cast<int>(files.size())) {
      key_index_->upsert(key, record, files[record_file]);
    }
    return Status::kSuccess;
  }
  return Status::kFailure;
}

Status LogHandler::warmKeyIndex() {
  if (!key_index_ || latest_view == nullptr) return Status::kSuccess;
  auto const& files = this->latest_view->getWithPrefix(this->prefix);
  for (auto const& file_name : files) {
    bool read_more = true;
    size_t cached_offset = 0;
    size_t size = 0;
    this->cache->checkReadMoreLog(file_name, read_more, cached_offset, size);
    if (read_more) {
      this->cache->readDataLog(file_name, cached_offset, size);
    }
    std::unordered_map<std::string, std::shared_ptr<Record>> snapshot;
    this->cache->snapshotLogFileRecords(file_name, snapshot);
    // Iterating files oldest-to-newest means the last write wins, which
    // matches readRecord's backward-scan semantics (newest file first).
    for (auto const& kv : snapshot) {
      key_index_->upsert(kv.first, kv.second, file_name);
    }
  }
  return Status::kSuccess;
}

void LogHandler::invalidateCompactedLog(std::vector<std::string> const& files) {
  for (auto const& f : files) {
    if (key_index_) key_index_->invalidateFile(f);
    if (cache) cache->invalidateLogFile(f);
  }
}

void LogHandler::onRemoteAppend(std::string const& file_name,
                                unsigned char const* data, size_t len,
                                RemoteOp op) {
  if (!key_index_) return;
  // Reject anything outside our log prefix: SSTable writes, other
  // handlers' log streams, metadata log entries. Cheap O(n) rejection
  // before we pay to parse the payload.
  if (file_name.compare(0, prefix.size(), prefix) != 0) return;

  if (op == RemoteOp::kRemove) {
    key_index_->invalidateFile(file_name);
    if (cache) cache->invalidateLogFile(file_name);
    return;
  }

  // APPEND: payload is a sequence of varint-prefixed Record protos
  // (same format produced by protobuf::serializeMessage in addRecord).
  std::vector<google::protobuf::Message*> messages;
  protobuf::deserializeMessages(const_cast<unsigned char*>(data), len, messages,
                                []() -> google::protobuf::Message* {
                                  return new Record();
                                });
  for (auto* msg : messages) {
    // Wrap in shared_ptr immediately so we don't have to hand-roll
    // cleanup if either the cache put or the index upsert throws.
    std::shared_ptr<Record> rec(static_cast<Record*>(msg));
    auto key = rec->key();
    cache->putLogRecordSingle(file_name, rec);
    key_index_->upsert(std::move(key), std::move(rec), file_name);
  }
}
}  // namespace ozonedb
