// LRUCache tests for bench/PLAN-compaction-cache.md: per-level counters,
// dead blocks of files that left the View, drop on REMOVE, and the warm
// worker. Everything runs on FileStorage under /tank/test/cache_test/.
#include "cache.h"
#include "db.h"
#include "gtest/gtest.h"
#include "metadata_log_handler.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "storage.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
using namespace ozonedb;

namespace {
std::string const kDir = "/tank/test/cache_test/";

// Builds an SSTable of n records with value_bytes-long values (600 bytes
// and the 4 KiB block target give about 6 records per block) and returns
// key -> value. A write-through cache, when given, sees every block the
// way a compaction's output builder publishes it.
std::map<std::string, std::string> buildTable(Storage* storage, std::string const& file, int n,
                                              size_t value_bytes, LRUCache* write_through = nullptr) {
  std::vector<std::string> keys;
  for (int i = 0; i < n; i++) keys.push_back("key" + std::to_string(i));
  std::sort(keys.begin(), keys.end());
  std::map<std::string, std::string> expected;
  TableBuilder tb(storage, file);
  if (write_through != nullptr) tb.setLRUCache(write_through);
  for (auto const& key : keys) {
    Record record;
    record.set_key(key);
    record.set_value(std::string(value_bytes, 'v') + key);
    record.set_type(kTypeValue);
    tb.add(key, record);
    expected[key] = record.value();
  }
  EXPECT_EQ(tb.finish(), Status::kSuccess);
  return expected;
}

std::string stamp() {
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
  return std::to_string(getpid()) + "_" + std::to_string(ns);
}

struct CacheFixture {
  FileStorage storage{kDir};
  FileMutexManager mutexes;
  LRUCache cache;
  explicit CacheFixture(size_t capacity = 32u << 20) : cache(capacity, &storage) {
    std::filesystem::create_directories(kDir + "sstable1");
    std::filesystem::create_directories(kDir + "sstable2");
    cache.setFileMutexManager(&mutexes);
  }
};
}  // namespace

TEST(LRUCacheTest, LevelSlotParsesSSTableNames) {
  EXPECT_EQ(LRUCache::levelSlot("sstable1/abc.sst"), 1);
  EXPECT_EQ(LRUCache::levelSlot("sstable12/3_abc.sst"), 12);
  EXPECT_EQ(LRUCache::levelSlot("sstable/abc.sst"), 0);
  EXPECT_EQ(LRUCache::levelSlot("sstable1"), 0);
  EXPECT_EQ(LRUCache::levelSlot("sstable1x/abc.sst"), 0);
  EXPECT_EQ(LRUCache::levelSlot("datalog1"), 0);
  EXPECT_EQ(LRUCache::levelSlot("metadata.log"), 0);
  EXPECT_EQ(LRUCache::levelSlot("sstable99/abc.sst"), 0);  // beyond kLevelSlots
  EXPECT_EQ(LRUCache::levelSlot(""), 0);
}

// One miss and one hit on a level-1 file, one miss on a level-2 file:
// the per-level counters must say so. Then a View that lists only the
// level-1 file makes the level-2 block dead weight.
TEST(LRUCacheTest, PerLevelCountersAndDeadBytes) {
  CacheFixture fx;
  std::string const f1 = "sstable1/" + stamp() + ".sst";
  std::string const f2 = "sstable2/" + stamp() + ".sst";
  buildTable(&fx.storage, f1, 200, 600);
  buildTable(&fx.storage, f2, 200, 600);

  Table* t1 = nullptr;
  Table* t2 = nullptr;
  fx.cache.getSSTable(f1, t1);
  fx.cache.getSSTable(f2, t2);
  ASSERT_NE(t1, nullptr);
  ASSERT_NE(t2, nullptr);

  std::shared_ptr<Record> rec;
  ASSERT_EQ(t1->get("key0", rec), Status::kSuccess);  // level 1 miss
  ASSERT_EQ(t1->get("key0", rec), Status::kSuccess);  // level 1 hit
  ASSERT_EQ(t2->get("key0", rec), Status::kSuccess);  // level 2 miss

  LRUCache::Stats s = fx.cache.stats();
  EXPECT_EQ(s.hits, 1u);
  EXPECT_EQ(s.misses, 2u);
  EXPECT_EQ(s.level_hits[1], 1u);
  EXPECT_EQ(s.level_misses[1], 1u);
  EXPECT_EQ(s.level_hits[2], 0u);
  EXPECT_EQ(s.level_misses[2], 1u);
  EXPECT_EQ(s.level_hits[0], 0u);
  EXPECT_EQ(s.level_misses[0], 0u);
  EXPECT_EQ(s.sstable_files, 2u);
  EXPECT_GT(s.current_size, 0u);
  // No View: nothing can be called dead.
  EXPECT_EQ(s.dead_files, 0u);
  EXPECT_EQ(s.dead_bytes, 0u);

  View view;
  view.key_range[f1] = std::make_pair("key0", "key99");
  fx.cache.setLatestView(&view);
  s = fx.cache.stats();
  EXPECT_EQ(s.dead_files, 1u);
  EXPECT_EQ(s.dead_blocks, 1u);
  // The two files hold identical records, so the two cached blocks (the
  // one with key0 in each file) are the same size: the dead one is half.
  EXPECT_EQ(s.dead_bytes * 2, s.current_size);
  EXPECT_EQ(s.warm_files, 0u);
  EXPECT_EQ(s.warm_hits, 0u);
  fx.cache.printCacheStats();
  fx.cache.setLatestView(nullptr);
}
