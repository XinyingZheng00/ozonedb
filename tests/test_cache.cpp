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
#include <condition_variable>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
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

// A read of a file the View lists that fails at the storage layer moves
// readFailures(): a block read on a removed file, an open of a missing
// file, a log read of a missing file. A miss on a readable file does not.
TEST(LRUCacheTest, ReadFailuresCountRemovedFiles) {
  CacheFixture fx;
  std::string const f = "sstable1/" + stamp() + ".sst";
  buildTable(&fx.storage, f, 200, 600);
  Table* t = nullptr;
  fx.cache.getSSTable(f, t);
  ASSERT_NE(t, nullptr);
  std::shared_ptr<Record> rec;
  ASSERT_EQ(t->get("key0", rec), Status::kSuccess);
  ASSERT_EQ(t->get("nokey", rec), Status::kNotFound);  // a plain miss
  EXPECT_EQ(fx.cache.readFailures(), 0u);

  // The file goes away under the open Table (a peer's REMOVE): a block
  // not yet cached cannot be read. Truncated rather than unlinked,
  // because FileStorage keeps an open stream per file and an unlinked
  // file still reads through it.
  std::filesystem::resize_file(kDir + f, 0);
  ASSERT_EQ(t->get("key150", rec), Status::kNotFound);
  EXPECT_EQ(fx.cache.readFailures(), 1u);
  // The cached block still answers, and does not count.
  ASSERT_EQ(t->get("key0", rec), Status::kSuccess);
  EXPECT_EQ(fx.cache.readFailures(), 1u);

  // An open of a file that is not there.
  Table* missing = nullptr;
  fx.cache.getSSTable("sstable1/missing_" + stamp() + ".sst", missing);
  EXPECT_EQ(missing, nullptr);
  EXPECT_EQ(fx.cache.readFailures(), 2u);

  // A log read of a file that is not there.
  fx.cache.readDataLog("datalog/404", 0, 100);
  EXPECT_EQ(fx.cache.readFailures(), 3u);
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

// The warm policy as a pure function: one case per rule, and the rule
// order (enabled, level, budget, affinity).
TEST(LRUCacheTest, WarmDecisionRules) {
  using Skip = LRUCache::WarmSkip;
  LRUCache::WarmPolicy p;
  p.enabled = true;
  p.max_level = 1;
  p.max_fraction = 0.25;
  p.min_input_blocks = 1;
  size_t const cap = 1000;
  EXPECT_EQ(LRUCache::warmDecision(p, 1, 250, cap, 1), Skip::kNone);
  EXPECT_EQ(LRUCache::warmDecision(p, 0, 0, cap, 1), Skip::kNone);
  LRUCache::WarmPolicy off = p;
  off.enabled = false;
  EXPECT_EQ(LRUCache::warmDecision(off, 1, 250, cap, 1), Skip::kDisabled);
  EXPECT_EQ(LRUCache::warmDecision(p, 2, 250, cap, 1), Skip::kLevel);
  EXPECT_EQ(LRUCache::warmDecision(p, 1, 251, cap, 1), Skip::kBudget);
  EXPECT_EQ(LRUCache::warmDecision(p, 1, 250, cap, 0), Skip::kAffinity);
  // Rule order.
  EXPECT_EQ(LRUCache::warmDecision(off, 2, 999, cap, 0), Skip::kDisabled);
  EXPECT_EQ(LRUCache::warmDecision(p, 2, 999, cap, 0), Skip::kLevel);
  EXPECT_EQ(LRUCache::warmDecision(p, 1, 999, cap, 0), Skip::kBudget);
  p.min_input_blocks = 0;
  EXPECT_EQ(LRUCache::warmDecision(p, 1, 250, cap, 0), Skip::kNone);
  // The engine defaults are the plan's: off, L1, a quarter, one block.
  LRUCache::WarmPolicy d;
  EXPECT_FALSE(d.enabled);
  EXPECT_EQ(d.max_level, 1);
  EXPECT_DOUBLE_EQ(d.max_fraction, 0.25);
  EXPECT_EQ(d.min_input_blocks, 1u);
}

// Table::warm publishes every data block: current_size equals the bytes
// it reports, every later get is a hit, and each warmed block counts one
// warm hit on its first get. A second warm changes nothing.
TEST(LRUCacheTest, WarmPublishesEveryBlock) {
  CacheFixture fx;
  std::string const f = "sstable1/" + stamp() + ".sst";
  auto expected = buildTable(&fx.storage, f, 800, 600);
  Table* t = nullptr;
  fx.cache.getSSTable(f, t);
  ASSERT_NE(t, nullptr);

  size_t blocks = 0;
  size_t bytes = 0;
  ASSERT_EQ(t->warm(&fx.cache, 10240, blocks, bytes), Status::kSuccess);  // 2.5 blocks per read
  EXPECT_GT(blocks, 100u);
  EXPECT_GT(bytes, 800u * 600u);
  LRUCache::Stats s = fx.cache.stats();
  EXPECT_EQ(s.current_size, bytes);
  EXPECT_EQ(s.misses, 0u);
  EXPECT_EQ(s.hits, 0u);

  std::shared_ptr<Record> rec;
  for (auto const& [key, value] : expected) {
    ASSERT_EQ(t->get(key, rec), Status::kSuccess) << key;
    ASSERT_EQ(rec->value(), value);
  }
  s = fx.cache.stats();
  EXPECT_EQ(s.misses, 0u);
  EXPECT_EQ(s.hits, expected.size());
  EXPECT_EQ(s.level_hits[1], expected.size());
  EXPECT_EQ(s.warm_hits, blocks);

  size_t blocks2 = 0;
  size_t bytes2 = 0;
  ASSERT_EQ(t->warm(&fx.cache, Table::kDefaultScanReadBytes, blocks2, bytes2), Status::kSuccess);
  EXPECT_EQ(blocks2, blocks);
  EXPECT_EQ(fx.cache.stats().current_size, bytes);
  // Duplicates were dropped, so no block is marked again.
  ASSERT_EQ(t->get("key0", rec), Status::kSuccess);
  EXPECT_EQ(fx.cache.stats().warm_hits, blocks);
}

// The worker: a live file is warmed, a file no longer in the View is
// skipped, a file the local builder wrote through is skipped, a level-2
// output is refused by the policy, and an offer before start (or after
// stop) counts as disabled.
TEST(LRUCacheTest, WarmWorkerQueueAndSkips) {
  CacheFixture fx;
  std::string const f_live = "sstable1/live_" + stamp() + ".sst";
  std::string const f_gone = "sstable1/gone_" + stamp() + ".sst";
  std::string const f_built = "sstable1/built_" + stamp() + ".sst";
  std::string const f_l2 = "sstable2/l2_" + stamp() + ".sst";
  auto expected = buildTable(&fx.storage, f_live, 300, 600);
  buildTable(&fx.storage, f_gone, 100, 600);
  buildTable(&fx.storage, f_built, 100, 600, &fx.cache);  // write-through + complete
  buildTable(&fx.storage, f_l2, 100, 600);
  size_t const built_bytes = fx.cache.stats().current_size;
  ASSERT_GT(built_bytes, 0u);

  LRUCache::WarmPolicy p;
  p.enabled = true;
  p.max_level = 1;
  p.max_fraction = 1.0;
  p.min_input_blocks = 1;
  p.read_bytes = 1u << 20;
  fx.cache.setWarmPolicy(p);
  std::set<std::string> const live = {f_live, f_built, f_l2};
  fx.cache.setLiveFileCheck([&live](std::string const& name) { return live.count(name) > 0; });

  auto sz = [&](std::string const& name) { return fx.storage.size(name); };
  // Offered before the worker exists: disabled, whatever the policy says
  // (a level-2 output too, which the level rule would otherwise count).
  fx.cache.onCompactionApplied({f_live, f_l2}, {sz(f_live), sz(f_l2)}, 1, 5);
  EXPECT_EQ(fx.cache.stats().warm_skipped_disabled, 2u);
  EXPECT_EQ(fx.cache.stats().warm_skipped_level, 0u);

  fx.cache.startWarmWorker();
  fx.cache.onCompactionApplied({f_live, f_gone}, {sz(f_live), sz(f_gone)}, 1, 5);
  fx.cache.onCompactionApplied({f_built}, {sz(f_built)}, 1, 5);
  fx.cache.onCompactionApplied({f_l2}, {sz(f_l2)}, 2, 5);
  fx.cache.onCompactionApplied({f_live}, {sz(f_live)}, 1, 0);  // affinity
  fx.cache.waitWarmIdle();

  LRUCache::Stats s = fx.cache.stats();
  EXPECT_EQ(s.warm_files, 1u);
  EXPECT_GT(s.warm_blocks, 0u);
  EXPECT_EQ(s.warm_skipped_gone, 1u);
  EXPECT_EQ(s.warm_skipped_built, 1u);
  EXPECT_EQ(s.warm_skipped_level, 1u);
  EXPECT_EQ(s.warm_skipped_affinity, 1u);
  EXPECT_EQ(s.warm_dropped, 0u);
  EXPECT_EQ(s.current_size, built_bytes + s.warm_bytes);
  EXPECT_EQ(s.misses, 0u);

  // Every key of the warmed file is a hit now.
  Table* t = nullptr;
  fx.cache.getSSTable(f_live, t);
  ASSERT_NE(t, nullptr);
  std::shared_ptr<Record> rec;
  for (auto const& [key, value] : expected) {
    ASSERT_EQ(t->get(key, rec), Status::kSuccess) << key;
  }
  s = fx.cache.stats();
  EXPECT_EQ(s.misses, 0u);
  EXPECT_EQ(s.hits, expected.size());
  EXPECT_EQ(s.warm_hits, s.warm_blocks);

  fx.cache.stopWarmWorker();
  fx.cache.onCompactionApplied({f_live}, {sz(f_live)}, 1, 5);
  EXPECT_EQ(fx.cache.stats().warm_skipped_disabled, 3u);
  fx.cache.stopWarmWorker();  // idempotent
}

// A log -> L1 compaction has no cached input blocks by construction; its
// affinity is the number of L1 lookups since the previous such event.
TEST(LRUCacheTest, WarmAffinityForLogInputs) {
  CacheFixture fx;
  std::string const f_out = "sstable1/out_" + stamp() + ".sst";
  std::string const f_read = "sstable1/read_" + stamp() + ".sst";
  buildTable(&fx.storage, f_out, 100, 600);
  buildTable(&fx.storage, f_read, 100, 600);
  LRUCache::WarmPolicy p;
  p.enabled = true;
  p.max_fraction = 1.0;
  p.min_input_blocks = 1;
  fx.cache.setWarmPolicy(p);
  fx.cache.startWarmWorker();
  size_t const bytes = fx.storage.size(f_out);

  // No L1 lookup yet (a pure load): refused.
  fx.cache.onCompactionApplied({f_out}, {bytes}, 1, 0, /*log_inputs=*/true);
  fx.cache.waitWarmIdle();
  EXPECT_EQ(fx.cache.stats().warm_skipped_affinity, 1u);
  EXPECT_EQ(fx.cache.stats().warm_files, 0u);

  // One L1 lookup since the last event: warmed.
  Table* t = nullptr;
  fx.cache.getSSTable(f_read, t);
  ASSERT_NE(t, nullptr);
  std::shared_ptr<Record> rec;
  ASSERT_EQ(t->get("key0", rec), Status::kSuccess);
  fx.cache.onCompactionApplied({f_out}, {bytes}, 1, 0, /*log_inputs=*/true);
  fx.cache.waitWarmIdle();
  EXPECT_EQ(fx.cache.stats().warm_files, 1u);
  EXPECT_EQ(fx.cache.stats().warm_skipped_affinity, 1u);

  // The counter is a delta: no lookup since, refused again.
  fx.cache.onCompactionApplied({f_out}, {bytes}, 1, 0, /*log_inputs=*/true);
  fx.cache.waitWarmIdle();
  EXPECT_EQ(fx.cache.stats().warm_skipped_affinity, 2u);
  fx.cache.stopWarmWorker();
}

// The queue is bounded: with max_queue 2, offering 5 files drops the 3
// oldest. Checked with the worker stopped so nothing is consumed
// (the entries are dropped by stop; only the counter matters here).
TEST(LRUCacheTest, WarmQueueIsBounded) {
  CacheFixture fx;
  LRUCache::WarmPolicy p;
  p.enabled = true;
  p.max_fraction = 1.0;
  p.min_input_blocks = 0;
  p.max_queue = 2;
  fx.cache.setWarmPolicy(p);
  // A live check that parks the worker on a flag keeps the queue full
  // while the test offers more files than the bound.
  std::mutex m;
  std::condition_variable cv;
  bool entered = false;
  bool release = false;
  fx.cache.setLiveFileCheck([&](std::string const&) {
    std::unique_lock<std::mutex> lk(m);
    entered = true;
    cv.notify_all();
    cv.wait(lk, [&] { return release; });
    return false;
  });
  fx.cache.startWarmWorker();
  fx.cache.onCompactionApplied({"sstable1/q0.sst"}, {1}, 1, 0);
  {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return entered; });  // q0 is in flight, queue empty
  }
  std::vector<std::string> names;
  std::vector<size_t> sizes;
  for (int i = 1; i <= 5; ++i) {
    names.push_back("sstable1/q" + std::to_string(i) + ".sst");
    sizes.push_back(1);
  }
  fx.cache.onCompactionApplied(names, sizes, 1, 0);
  // The queue kept 2 of the 5 (q4, q5); q1..q3 were dropped.
  {
    std::lock_guard<std::mutex> lk(m);
    release = true;
  }
  cv.notify_all();
  fx.cache.waitWarmIdle();
  LRUCache::Stats s = fx.cache.stats();
  EXPECT_EQ(s.warm_dropped, 3u);
  EXPECT_EQ(s.warm_skipped_gone, 3u);  // q0, q4, q5: the check says not live
  EXPECT_EQ(s.warm_files, 0u);
  fx.cache.stopWarmWorker();
}
