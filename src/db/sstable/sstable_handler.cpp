#include "sstable/sstable_handler.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include <atomic>
#include <chrono>
#include <cstdio>

namespace ozonedb {

// ---- Fig 1 investigation: per-level counters for readRecordFromAllLevel ----
// Indexed [1..max_level]; index 0 is unused.  Dumped from DB::closeDB().
constexpr int kMaxLevels = 16;
std::atomic<uint64_t> kSstLevelTimeNs[kMaxLevels];
std::atomic<uint64_t> kSstLevelEnter[kMaxLevels];     // # times the level loop body was entered
std::atomic<uint64_t> kSstLevelRangeReject[kMaxLevels];  // skipped at the key_range check
std::atomic<uint64_t> kSstLevelTableGet[kMaxLevels];  // table->get calls
std::atomic<uint64_t> kSstLevelHit[kMaxLevels];       // table->get returned kSuccess
// ---------------------------------------------------------------------------

Status SSTableHandler::readRecordFromAllLevel(std::string const& key, Record*& record, std::string const& offset) {
  using clk = std::chrono::steady_clock;
  // sstable prefix is level prefix + number

  for (int i = 1; i <= max_level; i++) {
    auto t_level_start = clk::now();
    std::string prefix = this->sstable_prefix + std::to_string(i);

    std::deque<std::string> tables = this->latest_view->getWithPrefix(prefix);
    std::shared_mutex record_mutex;
    int finished_threads = 0;
    int record_file = -1;
    std::mutex cv_mutex;
    std::condition_variable cv;

    int count = 0;
    for (int j = tables.size() - 1; j >= 0; j--) {
      std::string const& file_name = tables[j];
      kSstLevelEnter[i].fetch_add(1, std::memory_order_relaxed);
      if (key < this->latest_view->getKeyRange(file_name).first || key > this->latest_view->getKeyRange(file_name).second) {
        kSstLevelRangeReject[i].fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      Table* table = nullptr;
      this->lru_cache->getSSTable(file_name, table);
      kSstLevelTableGet[i].fetch_add(1, std::memory_order_relaxed);
      Status status = table->get(key, record);
      if (status == Status::kSuccess) {
        kSstLevelHit[i].fetch_add(1, std::memory_order_relaxed);
        kSstLevelTimeNs[i].fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - t_level_start).count(),
            std::memory_order_relaxed);
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
    kSstLevelTimeNs[i].fetch_add(
        std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - t_level_start).count(),
        std::memory_order_relaxed);
    if (record) {
      return Status::kSuccess;
    }
  }
  return Status::kFailure;
}

void SSTableHandler::dumpProfileCounters() {
  fprintf(stderr, "[OZONEDB-SST-PROFILE] level | time_total_ms | enters | range_rejects | table_gets | hits\n");
  for (int i = 1; i < kMaxLevels; ++i) {
    uint64_t enters = kSstLevelEnter[i].load(std::memory_order_relaxed);
    if (enters == 0) continue;
    uint64_t time_ns = kSstLevelTimeNs[i].load(std::memory_order_relaxed);
    uint64_t rr     = kSstLevelRangeReject[i].load(std::memory_order_relaxed);
    uint64_t tg     = kSstLevelTableGet[i].load(std::memory_order_relaxed);
    uint64_t hits   = kSstLevelHit[i].load(std::memory_order_relaxed);
    fprintf(stderr, "[OZONEDB-SST-PROFILE] L%-2d | %12.2f | %8lu | %12lu | %10lu | %6lu\n",
            i, time_ns / 1e6, (unsigned long)enters, (unsigned long)rr,
            (unsigned long)tg, (unsigned long)hits);
  }
  fflush(stderr);
  for (int i = 0; i < kMaxLevels; ++i) {
    kSstLevelTimeNs[i].store(0, std::memory_order_relaxed);
    kSstLevelEnter[i].store(0, std::memory_order_relaxed);
    kSstLevelRangeReject[i].store(0, std::memory_order_relaxed);
    kSstLevelTableGet[i].store(0, std::memory_order_relaxed);
    kSstLevelHit[i].store(0, std::memory_order_relaxed);
  }
}

}  // namespace ozonedb