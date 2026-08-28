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

// invalidateSSTable: the blocks leave the budget, the Table* is retired
// (not freed: a reader may hold it), the entry is gone, and the return
// value is the number of blocks that were cached.
TEST(LRUCacheTest, InvalidateSSTableDropsBlocksAndRetiresTable) {
  CacheFixture fx;
  std::string const f1 = "sstable1/" + stamp() + ".sst";
  buildTable(&fx.storage, f1, 200, 600);
  Table* t1 = nullptr;
  fx.cache.getSSTable(f1, t1);
  ASSERT_NE(t1, nullptr);
  std::shared_ptr<Record> rec;
  ASSERT_EQ(t1->get("key0", rec), Status::kSuccess);
  ASSERT_EQ(t1->get("key150", rec), Status::kSuccess);
  LRUCache::Stats before = fx.cache.stats();
  ASSERT_GT(before.current_size, 0u);
  ASSERT_EQ(before.retired_tables, 0u);

  EXPECT_EQ(fx.cache.invalidateSSTable(f1), 2u);
  LRUCache::Stats after = fx.cache.stats();
  EXPECT_EQ(after.current_size, 0u);
  EXPECT_EQ(after.sstable_files, 0u);
  EXPECT_EQ(after.retired_tables, 1u);
  EXPECT_EQ(fx.cache.getCacheMap().count(f1), 0u);
  // A second call finds nothing and drops nothing.
  EXPECT_EQ(fx.cache.invalidateSSTable(f1), 0u);
  EXPECT_EQ(fx.cache.stats().retired_tables, 1u);
  // The retired Table still answers a reader that kept the pointer.
  EXPECT_EQ(t1->get("key0", rec), Status::kSuccess);

  // A log entry (records only) is not this method's business.
  std::unordered_map<std::string, std::shared_ptr<Record>> records;
  fx.cache.putLogRecords("datalog/7", records, 10, false);
  EXPECT_EQ(fx.cache.invalidateSSTable("datalog/7"), 0u);
  EXPECT_EQ(fx.cache.getCacheMap().count("datalog/7"), 1u);
}

// A COMPACT applied to the View drops the inputs' cached blocks: the
// handler queues the event under view_mutex and drains it after the lock.
// Driven through a real metadata log on FileStorage: two LOGCREATEs, a
// log-to-L1 COMPACT, then an L1-to-L2 COMPACT whose input is cached.
TEST(LRUCacheTest, CompactApplyDropsInputBlocks) {
  std::string const dir = kDir + "mlh_" + stamp() + "/";
  std::filesystem::create_directories(dir + "sstable1");
  std::filesystem::create_directories(dir + "sstable2");
  std::filesystem::create_directories(dir + "datalog");
  FileStorage storage(dir);
  FileMutexManager mutexes;
  LRUCache cache(32u << 20, &storage);
  cache.setFileMutexManager(&mutexes);
  std::string const a = "sstable1/a.sst";
  std::string const b = "sstable2/b.sst";
  buildTable(&storage, a, 200, 600);
  buildTable(&storage, b, 200, 600);

  TailCache tail;
  MetadataLogHandler mlh("metadata.log", &storage, &tail);
  mlh.setLRUCache(&cache);

  auto logcreate = [&](std::string const& in, std::string const& out) {
    OperationRecord r;
    r.set_op_type(OperationRecord::LOGCREATE);
    r.add_input_files(in);
    r.add_output_file(out);
    r.set_sealed_input_bytes(0);
    mlh.appendToMetadataLog(r);
  };
  auto compact = [&](std::string const& in, std::string const& out) {
    OperationRecord r;
    r.set_op_type(OperationRecord::COMPACT);
    r.add_input_files(in);
    r.add_output_file(out);
    r.add_key_start("key0");
    r.add_key_end("key99");
    r.add_output_bytes(static_cast<int64_t>(storage.size(out)));
    r.set_compact_in_last_level(false);
    mlh.appendToMetadataLog(r);
  };
  logcreate("", "datalog/1");
  logcreate("datalog/1", "datalog/2");
  compact("datalog/1", a);
  mlh.rollForwardMetadataLog();
  {
    auto snap = mlh.latestViewSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_EQ(snap->getWithPrefix("sstable1").size(), 1u);
    ASSERT_EQ(snap->getWithPrefix("sstable1").front(), a);
  }

  // Cache one block of `a`, plus a log entry that must survive.
  Table* ta = nullptr;
  cache.getSSTable(a, ta);
  ASSERT_NE(ta, nullptr);
  std::shared_ptr<Record> rec;
  ASSERT_EQ(ta->get("key0", rec), Status::kSuccess);
  std::unordered_map<std::string, std::shared_ptr<Record>> records;
  cache.putLogRecords("datalog/2", records, 10, false);
  LRUCache::Stats before = cache.stats();
  ASSERT_GT(before.current_size, 0u);
  ASSERT_EQ(before.sstable_files, 1u);

  compact(a, b);
  mlh.rollForwardMetadataLog();
  auto snap = mlh.latestViewSnapshot();
  ASSERT_EQ(snap->getWithPrefix("sstable1").size(), 0u);
  ASSERT_EQ(snap->getWithPrefix("sstable2").size(), 1u);
  cache.setLatestView(snap.get());
  LRUCache::Stats after = cache.stats();
  EXPECT_EQ(after.current_size, 0u);
  EXPECT_EQ(after.sstable_files, 0u);
  EXPECT_EQ(after.retired_tables, 1u);
  EXPECT_EQ(after.dead_bytes, 0u);
  EXPECT_EQ(after.dead_files, 0u);
  EXPECT_EQ(cache.getCacheMap().count(a), 0u);
  EXPECT_EQ(cache.getCacheMap().count("datalog/2"), 1u);
  // Nothing left to drain: a second rollforward changes nothing.
  mlh.rollForwardMetadataLog();
  EXPECT_EQ(cache.stats().retired_tables, 1u);
  cache.setLatestView(nullptr);
}
