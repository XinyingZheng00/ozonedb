#include "data_log_handler.h"
#include "event_listener.h"
#include "helper.h"
#include <google/protobuf/message.h>
#include <future>
#include <mutex>
#include <regex>
#include <unordered_map>
#include <vector>

namespace ozonedb {

Storage* DataLogHandler::getThreadLocalStorage(std::string const& DBdir, StorageType storage_type) {
  static thread_local std::unique_ptr<Storage> thread_local_storage;
  if (!thread_local_storage) {
    if (storage_type == StorageType::kSharedLogStorage) {
      thread_local_storage = std::make_unique<SharedLogStorage>("datalog");
    } else if (storage_type == StorageType::kFileStorage) {
      thread_local_storage = std::make_unique<FileStorage>(DBdir);
    } else {
      throw std::runtime_error("Unsupported storage type");
    }
  }
  return thread_local_storage.get();
}

Status DataLogHandler::addRecord(Record const& record) {
  return this->record_appender->append(record);
}

bool DataLogHandler::shouldReadMoreLog(std::string const& file_name, size_t& cached_offset, size_t& size) {
  bool read_more = true;
  this->cache->shouldReadMoreLog(file_name, read_more, cached_offset, size);
  return read_more;
}

void DataLogHandler::fetchLogToCache(std::string const& file_name, size_t cached_offset, size_t size) {
  // std::string inflight_key = file_name + ":" + std::to_string(cached_offset);

  // std::shared_ptr<std::condition_variable> cv;
  // {
  //   std::unique_lock<std::mutex> lock(this->cache->inflight_mu_);
  //   auto it = this->cache->inflight_.find(inflight_key);
  //   if (it != this->cache->inflight_.end()) {
  //     cv = it->second;
  //   } else {
  //     cv = std::make_shared<std::condition_variable>();
  //     this->cache->inflight_[inflight_key] = cv;
  //   }
  // }

  // if (cv.use_count() > 1) {
  //   // Already being fetched; wait for it
  //   std::mutex wait_mu;
  //   std::unique_lock<std::mutex> wait_lock(wait_mu);
  //   cv->wait(wait_lock);  // Wait until the other thread finishes
  //   return;
  // }

  std::unordered_map<std::string, Record*> records_map;
  std::vector<google::protobuf::Message*> messages;
  bool sealed;

  this->getThreadLocalStorage(this->DBdir, this->storage_type)->read(
      file_name, cached_offset, size, [&]() -> google::protobuf::Message* {
        return new Record();
      },
      messages);
  // std::cout << "thread id: " << std::this_thread::get_id() << " fetchLogToCache file_name: " << file_name << " offset: " << cached_offset << " size: " << size << std::endl;
  for (auto* msg : messages) {
    auto* rec = static_cast<Record*>(msg);
    records_map[rec->key()] = rec;
  }

  sealed = file_name != latest_view->current_log_tail;
  this->cache->putLogRecords(file_name, records_map, size, sealed);

  // Notify waiting threads
  // {
  //   std::unique_lock<std::mutex> lock(this->cache->inflight_mu_);
  //   this->cache->inflight_.erase(inflight_key);
  // }
  // cv->notify_all();
}

Status DataLogHandler::readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset) {
  auto const& files = this->latest_view->getWithPrefix(this->log_prefix);
  if (files.empty()) {
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
    size_t cached_offset = 0;
    size_t size = 0;
    bool read_more = this->shouldReadMoreLog(file_name, cached_offset, size);
    if (read_more) {
      count++;
      this->thread_pool->enqueueFetchOnceWithEndOffset(
          file_name,
          size,
          [this, file_name](size_t begin, size_t end) {
            this->fetchLogToCache(file_name, begin, end);
          },
          [this, file_name, key, i,
           &record_file, &record, &record_mutex, &cv, &cv_mutex, &finished_threads]() {
            Record* record_tmp = nullptr;
            this->cache->get(file_name, key, record_tmp);
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
          });
    } else {
      // std::cout << "thread id: " << std::this_thread::get_id() << " readmore: false" << std::endl;
      Record* record_tmp = nullptr;
      this->cache->get(file_name, key, record_tmp);
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