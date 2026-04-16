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
  return Status::kSuccess;
}

Status LogHandler::readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset) {
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
        Record* record_tmp = nullptr;
        cache->get(file_name, key, record_tmp);
        if (record_tmp) {
          std::unique_lock<std::shared_mutex> lock(record_mutex);
          if (i >= record_file) {
            record = record_tmp;
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
      Record* record_tmp = nullptr;
      cache->get(file_name, key, record_tmp);
      if (record_tmp) {
        std::unique_lock<std::shared_mutex> lock(record_mutex);
        if (i >= record_file) {
          record = record_tmp;
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
    return Status::kSuccess;
  }
  return Status::kFailure;
}
}  // namespace ozonedb
