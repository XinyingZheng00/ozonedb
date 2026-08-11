#include "sstable/sstable_handler.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"

namespace ozonedb {

Status SSTableHandler::readRecordFromAllLevel(std::string const& key, std::shared_ptr<Record>& record, std::string const& offset) {
  // sstable prefix is level prefix + number

  for (int i = 1; i <= max_level; i++) {
    std::string prefix = this->sstable_prefix + std::to_string(i);

    // Reference into the view snapshot — avoids a per-level deque
    // copy on every get. Valid for the duration of DB::get (the
    // caller holds the shared_ptr<View const> until the frame
    // returns).
    std::deque<std::string> const& tables = this->latest_view->getWithPrefix(prefix);
    std::shared_mutex record_mutex;
    int finished_threads = 0;
    int record_file = -1;
    std::mutex cv_mutex;
    std::condition_variable cv;

    int count = 0;
    for (int j = tables.size() - 1; j >= 0; j--) {
      std::string const& file_name = tables[j];
      if (key < this->latest_view->getKeyRange(file_name).first || key > this->latest_view->getKeyRange(file_name).second) {
        continue;
      }
      Table* table = nullptr;
      this->lru_cache->getSSTable(file_name, table);
      Status status = table->get(key, record);
      if (status == Status::kSuccess) {
        return Status::kSuccess;
      }
      /*
      // currently, we don't need to use multi-thread to read the blocks from the sstable
      std::shared_mutex& file_mutex = this->lru_cache->getMutexForFile(file_name);
      std::unique_lock file_lock(file_mutex);
      Table* table = nullptr;
      this->lru_cache->getSSTable(file_name, table);
      std::string identifier_value;
      table->getBlockPosition(key, identifier_value);
      if (identifier_value.empty()) {
        continue;
      }
      bool read_more = true;
      this->lru_cache->needReadBlock(file_name, read_more, identifier_value);
      if (read_more) {
        std::cout << __FILE__ << ":" << __LINE__ << std::endl;
        count++;
        thread_pool->enqueue([this, file_name, identifier_value, key, j,
                              &record_file, &record, &record_mutex, &cv, &cv_mutex, &finished_threads]() {
          this->lru_cache->readDataBlocks(file_name, identifier_value);
          Record* record_tmp = nullptr;
          this->lru_cache->get(file_name, key, record_tmp, identifier_value);
          if (record_tmp) {
            std::unique_lock<std::shared_mutex> lock(record_mutex);
            if (j >= record_file) {
              record = record_tmp;
              record_file = j;
            }
          }
          {
            std::lock_guard<std::mutex> lock(cv_mutex);
            finished_threads++;
          }
          cv.notify_one();
        });
      } else {
        Record* record_tmp = nullptr;
        this->lru_cache->get(file_name, key, record_tmp, identifier_value);
        if (record_tmp) {
          std::unique_lock<std::shared_mutex> lock(record_mutex);
          if (j >= record_file) {
            record = record_tmp;
            record_file = j;
          }
          goto outer;
        }
      }*/
      if (offset == file_name) {
        goto outer;
      }
    }
  outer:
    // {
    //   std::unique_lock<std::mutex> lock(cv_mutex);
    //   cv.wait(lock, [&finished_threads, count] { return finished_threads == count; });
    // }
    if (record) {
      return Status::kSuccess;
    }
  }
  return Status::kFailure;
}

}  // namespace ozonedb