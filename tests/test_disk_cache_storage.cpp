// tests/test_disk_cache_storage.cpp
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <cstring>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "disk_cache_storage.h"
#include "metadata.h"
#include "storage.h"

#include <stdexcept>

using namespace ozonedb;

namespace {

std::string const kRoot = "/tank/test/disk_cache/";

using Mode = DiskCacheStorage::Mode;
using Adm = DiskCacheStorage::Admission;

std::string stamp() {
  auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
  return std::to_string(getpid()) + "_" + std::to_string(ns);
}

// A FileStorage that counts what the tier forwards to it.
class CountingStorage : public FileStorage {
 public:
  explicit CountingStorage(std::string const& path) : FileStorage(path) {}
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override {
    ++ranged_reads;
    last_a = a;
    last_len = length;
    return FileStorage::read(fileName, data, a, length);
  }
  Status read(std::string const& fileName, unsigned char*& data, size_t& size) override {
    ++full_reads;
    return FileStorage::read(fileName, data, size);
  }
  size_t size(std::string fileName) override {
    ++sizes;
    return FileStorage::size(fileName);
  }
  int ranged_reads = 0;
  int full_reads = 0;
  int sizes = 0;
  size_t last_a = 0;
  size_t last_len = 0;
};

// A CountingStorage whose ranged read deletes a path (the fill's private
// temp file, by construction) after its Nth call, to exercise a part
// vanishing mid-fill (review finding, PLAN-disk-cache T3 fix 1).
class PartVanishingStorage : public CountingStorage {
 public:
  PartVanishingStorage(std::string const& path, std::string vanish_path, int vanish_on_call)
      : CountingStorage(path), vanish_path_(std::move(vanish_path)), vanish_on_call_(vanish_on_call) {}
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override {
    Status s = CountingStorage::read(fileName, data, a, length);
    if (++calls_ == vanish_on_call_) std::filesystem::remove(vanish_path_);
    return s;
  }
  std::string vanish_path_;
  int vanish_on_call_;
  int calls_ = 0;
};

// A CountingStorage whose ranged read counts its own calls -- so a test can
// deterministically know the worker has begun a *specific* call instead of
// guessing from a fixed delay -- and then sleeps before delegating, so the
// test can reliably land on "the worker is mid-fill" (review finding,
// PLAN-disk-cache T3 fix 2; the counter is PLAN-disk-cache T4's fix for the
// carried-over review item: a bare sleep before start is not synchronization
// and can race the worker's first dequeue). A one-shot "entered" bool is not
// enough here: in the test below, the setup misses call this same read()
// directly on the main thread, before the worker thread exists, so a
// one-shot latch would already be tripped by the time the worker starts and
// would provide no synchronisation at all (PLAN-disk-cache T4 review round
// 1). Counting lets the test snapshot the count immediately before starting
// the worker and then wait for the worker's own call to advance it.
class SlowStorage : public CountingStorage {
 public:
  SlowStorage(std::string const& path, std::chrono::milliseconds delay) : CountingStorage(path), delay_(delay) {}
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override {
    {
      std::lock_guard<std::mutex> lk(reads_mtx_);
      ++reads_;
    }
    reads_cv_.notify_all();
    std::this_thread::sleep_for(delay_);
    return CountingStorage::read(fileName, data, a, length);
  }
  int reads() {
    std::lock_guard<std::mutex> lk(reads_mtx_);
    return reads_;
  }
  // Blocks, bounded by `timeout`, until at least `n` calls to read() above
  // have begun; false on timeout instead of hanging forever (e.g. if the
  // worker skips the read entirely via fillOne's present/gone/budget
  // early-outs -- PLAN-disk-cache T4 review round 1, finding 2).
  bool waitForReadsAtLeast(int n, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(reads_mtx_);
    return reads_cv_.wait_for(lk, timeout, [this, n] { return reads_ >= n; });
  }
  std::chrono::milliseconds delay_;

 private:
  std::mutex reads_mtx_;
  std::condition_variable reads_cv_;
  int reads_ = 0;
};

struct TierFixture {
  std::string backing_dir;
  std::string tier_dir;
  CountingStorage* backing;  // owned by the tier
  std::unique_ptr<DiskCacheStorage> tier;

  explicit TierFixture(uint64_t capacity, size_t chunk = 64u << 20, bool drop_pages = true,
                       DiskCacheStorage::Admission admission = DiskCacheStorage::Admission::kAlways,
                       DiskCacheStorage::Mode mode = DiskCacheStorage::Mode::kFile, size_t entry = 65536) {
    std::string const s = stamp();
    backing_dir = kRoot + "backing_" + s + "/";
    tier_dir = kRoot + "tier_" + s + "/";
    std::filesystem::create_directories(backing_dir + "sstable1");
    std::filesystem::create_directories(backing_dir + "checkpoint");
    std::filesystem::create_directories(tier_dir);
    auto owned = std::make_unique<CountingStorage>(backing_dir);
    backing = owned.get();
    DiskCacheStorage::Options o;
    o.dir = tier_dir;
    o.capacity_bytes = capacity;
    o.chunk_bytes = chunk;
    o.drop_pages = drop_pages;
    o.admission = admission;
    o.mode = mode;
    o.entry_bytes = entry;
    tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);
  }
  ~TierFixture() {
    tier.reset();
    std::filesystem::remove_all(backing_dir);
    std::filesystem::remove_all(tier_dir);
  }
  // Writes `n` bytes (i % 251) straight into the backing, bypassing the tier.
  std::vector<unsigned char> seed(std::string const& name, size_t n) {
    std::vector<unsigned char> bytes(n);
    for (size_t i = 0; i < n; ++i) bytes[i] = static_cast<unsigned char>(i % 251);
    std::filesystem::create_directories(std::filesystem::path(backing_dir + name).parent_path());
    std::ofstream out(backing_dir + name, std::ios::binary);
    out.write(reinterpret_cast<char const*>(bytes.data()), static_cast<std::streamsize>(n));
    return bytes;
  }
  // A 10-byte ranged read at offset 0, then wait for the fill worker.
  void touch(std::string const& name) {
    unsigned char* d = nullptr;
    ASSERT_EQ(tier->read(name, d, 0, 10), Status::kSuccess);
    delete[] d;
    tier->waitFillIdle();
  }
  // A ranged read of `n` bytes at `off`, checked against `bytes`.
  void readAt(std::string const& name, std::vector<unsigned char> const& bytes, size_t off, size_t n) {
    unsigned char* d = nullptr;
    ASSERT_EQ(tier->read(name, d, off, n), Status::kSuccess);
    EXPECT_EQ(0, memcmp(d, bytes.data() + off, n));
    delete[] d;
  }
  bool local(std::string const& name) const { return std::filesystem::exists(tier_dir + name); }
  bool part(std::string const& name) const { return std::filesystem::exists(tier_dir + name + ".part"); }
};

}  // namespace

TEST(DiskCacheStorageTest, NamesOutsideThePrefixPassThrough) {
  TierFixture f(1u << 20);
  std::vector<unsigned char> bytes = {'4', '2', '\n'};
  ASSERT_EQ(f.tier->appendNoFlush("checkpoint/LATEST", bytes.data(), 3), Status::kSuccess);
  ASSERT_EQ(f.tier->flush("checkpoint/LATEST"), Status::kSuccess);
  EXPECT_FALSE(f.local("checkpoint/LATEST"));
  EXPECT_FALSE(f.part("checkpoint/LATEST"));
  unsigned char* data = nullptr;
  size_t size = 0;
  ASSERT_EQ(f.tier->read("checkpoint/LATEST", data, size), Status::kSuccess);
  EXPECT_EQ(size, 3u);
  delete[] data;
  EXPECT_EQ(f.backing->full_reads, 1);
  EXPECT_EQ(f.tier->stats().passthrough, 1u);
  EXPECT_TRUE(f.tier->cacheable("sstable1/x.sst"));
  EXPECT_FALSE(f.tier->cacheable("checkpoint/LATEST"));
  EXPECT_FALSE(f.tier->cacheable("datalog1"));
}

TEST(DiskCacheStorageTest, ZeroCapacityCachesNothing) {
  TierFixture f(0);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 1000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 10, 100), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, bytes.data() + 10, 100));
  delete[] data;
  EXPECT_FALSE(f.tier->cacheable(name));
  EXPECT_EQ(f.tier->stats().passthrough, 1u);
  EXPECT_EQ(f.tier->stats().misses, 0u);
}

TEST(DiskCacheStorageTest, WriteThroughPublishesOnFlushAndServesReads) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> a(3000, 'a'), b(2000, 'b');
  ASSERT_EQ(f.tier->appendNoFlush(name, a.data(), 3000), Status::kSuccess);
  ASSERT_EQ(f.tier->appendNoFlush(name, b.data(), 2000), Status::kSuccess);
  EXPECT_TRUE(f.part(name));
  EXPECT_FALSE(f.local(name));
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  EXPECT_TRUE(f.local(name));
  EXPECT_FALSE(f.part(name));
  EXPECT_EQ(std::filesystem::file_size(f.tier_dir + name), 5000u);

  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 2990, 20), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, "aaaaaaaaaabbbbbbbbbb", 20));
  delete[] data;
  EXPECT_EQ(f.backing->ranged_reads, 0);
  auto s = f.tier->stats();
  EXPECT_EQ(s.hits, 1u);
  EXPECT_EQ(s.hit_bytes, 20u);
  EXPECT_EQ(s.misses, 0u);
  EXPECT_EQ(s.writethrough_files, 1u);
  EXPECT_EQ(s.files, 1u);
  EXPECT_EQ(s.bytes, 5000u);

  // size() answers locally for a complete copy.
  int const sizes_before = f.backing->sizes;
  EXPECT_EQ(f.tier->size(name), 5000u);
  EXPECT_EQ(f.backing->sizes, sizes_before);
}

TEST(DiskCacheStorageTest, MissIsServedFromTheBackingAndCounted) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 10000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 4096, 4096), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, bytes.data() + 4096, 4096));
  delete[] data;
  EXPECT_EQ(f.backing->ranged_reads, 1);
  auto s = f.tier->stats();
  EXPECT_EQ(s.misses, 1u);
  EXPECT_EQ(s.miss_bytes, 4096u);
  EXPECT_EQ(s.hits, 0u);
  // A read of an absent file fails and is not a local hit.
  // FileStorage::read returns kNotFound (not kFailure) for a genuinely
  // missing file (src/db/storage.cpp); the tier forwards it unchanged.
  EXPECT_EQ(f.tier->read("sstable1/absent.sst", data, 0, 10), Status::kNotFound);
}

TEST(DiskCacheStorageTest, ShortLocalCopyIsDroppedAndTheBackingServes) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> a(3000, 'a');
  ASSERT_EQ(f.tier->appendNoFlush(name, a.data(), 3000), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  std::filesystem::resize_file(f.tier_dir + name, 100);  // corrupt the copy
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 2000, 500), Status::kSuccess);
  EXPECT_EQ(data[0], 'a');
  delete[] data;
  EXPECT_FALSE(f.local(name));
  EXPECT_EQ(f.tier->stats().files, 0u);
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
}

TEST(DiskCacheStorageTest, FailedFlushDiscardsThePart) {
  // The backing rejects the flush: the tier must not publish the part.
  class RejectingStorage : public CountingStorage {
   public:
    explicit RejectingStorage(std::string const& path) : CountingStorage(path) {}
    Status flush(std::string const&) override { return Status::kFailure; }
  };
  std::string const s = stamp();
  std::string const backing_dir = kRoot + "backing_" + s + "/";
  std::string const tier_dir = kRoot + "tier_" + s + "/";
  std::filesystem::create_directories(backing_dir + "sstable1");
  DiskCacheStorage::Options o;
  o.dir = tier_dir;
  o.capacity_bytes = 1u << 20;
  DiskCacheStorage tier(std::make_unique<RejectingStorage>(backing_dir), o);
  std::string const name = "sstable1/x.sst";
  std::vector<unsigned char> a(10, 'a');
  ASSERT_EQ(tier.appendNoFlush(name, a.data(), 10), Status::kSuccess);
  EXPECT_EQ(tier.flush(name), Status::kFailure);
  EXPECT_FALSE(std::filesystem::exists(tier_dir + name));
  EXPECT_FALSE(std::filesystem::exists(tier_dir + name + ".part"));
  EXPECT_EQ(tier.stats().files, 0u);
  std::filesystem::remove_all(backing_dir);
  std::filesystem::remove_all(tier_dir);
}

TEST(DiskCacheStorageTest, MissQueuesAChunkedFillAndTheNextReadHits) {
  TierFixture f(1u << 20, /*chunk=*/1024);
  f.tier->startFillWorker();
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 3 * 1024 + 1);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 5, 10), Status::kSuccess);
  delete[] data;
  f.tier->waitFillIdle();
  EXPECT_TRUE(f.local(name));
  EXPECT_FALSE(f.part(name));
  auto s = f.tier->stats();
  EXPECT_EQ(s.fills, 1u);
  EXPECT_EQ(s.fill_bytes, 3u * 1024 + 1);
  EXPECT_EQ(s.fill_gets, 4u);  // 3 full chunks + 1 byte
  EXPECT_EQ(s.files, 1u);
  std::ifstream in(f.tier_dir + name, std::ios::binary);
  std::vector<unsigned char> copy((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_EQ(copy, bytes);

  int const before = f.backing->ranged_reads;
  ASSERT_EQ(f.tier->read(name, data, 2048, 100), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, bytes.data() + 2048, 100));
  delete[] data;
  EXPECT_EQ(f.backing->ranged_reads, before);
  EXPECT_EQ(f.tier->stats().hits, 1u);
}

TEST(DiskCacheStorageTest, FillSkipsFilesLargerThanTheCapacityAndFilesThatAreGone) {
  TierFixture f(2000, /*chunk=*/1024);
  f.tier->startFillWorker();
  std::string const big = "sstable1/" + stamp() + "_big.sst";
  f.seed(big, 5000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(big, data, 0, 10), Status::kSuccess);
  delete[] data;
  f.tier->waitFillIdle();
  EXPECT_FALSE(f.local(big));
  EXPECT_EQ(f.tier->stats().fill_skipped_budget, 1u);

  // Queue a name, then delete it from the backing before the worker runs.
  f.tier->stopFillWorker();
  std::string const gone = "sstable1/" + stamp() + "_gone.sst";
  f.seed(gone, 100);
  ASSERT_EQ(f.tier->read(gone, data, 0, 10), Status::kSuccess);
  delete[] data;
  std::filesystem::remove(f.backing_dir + gone);
  f.tier->startFillWorker();
  f.tier->waitFillIdle();
  EXPECT_FALSE(f.local(gone));
  EXPECT_EQ(f.tier->stats().fill_gone, 1u);
  EXPECT_EQ(f.tier->stats().fills, 0u);
}

TEST(DiskCacheStorageTest, FillQueueIsBoundedAndDeduplicated) {
  TierFixture f(1u << 20);
  DiskCacheStorage::Options o = f.tier->options();
  // Rebuild with a queue of 2 (Options are read at construction).
  f.tier.reset();
  auto owned = std::make_unique<CountingStorage>(f.backing_dir);
  f.backing = owned.get();
  o.max_queue = 2;
  f.tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);
  // The worker is not started, so the queue only grows.
  std::vector<std::string> names;
  for (int i = 0; i < 4; ++i) {
    names.push_back("sstable1/" + stamp() + "_" + std::to_string(i) + ".sst");
    f.seed(names.back(), 100);
  }
  unsigned char* data = nullptr;
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(f.tier->read(names[i], data, 0, 10), Status::kSuccess);
    delete[] data;
    ASSERT_EQ(f.tier->read(names[i], data, 0, 10), Status::kSuccess);  // duplicate
    delete[] data;
  }
  EXPECT_EQ(f.tier->stats().fill_dropped, 2u);  // 4 distinct names, bound 2
  f.tier->startFillWorker();
  f.tier->waitFillIdle();
  EXPECT_EQ(f.tier->stats().fills, 2u);
  EXPECT_FALSE(f.local(names[0]));
  EXPECT_FALSE(f.local(names[1]));
  EXPECT_TRUE(f.local(names[2]));
  EXPECT_TRUE(f.local(names[3]));
}

TEST(DiskCacheStorageTest, FillFailsCleanlyWhenItsPartVanishesMidway) {
  TierFixture f(1u << 20, /*chunk=*/1024);
  DiskCacheStorage::Options o = f.tier->options();
  f.tier.reset();
  std::string const name = "sstable1/" + stamp() + ".sst";
  f.seed(name, 3 * 1024 + 1);
  std::string const fill_part = f.tier_dir + name + ".fillpart";
  // Call 1 is the outer miss-serving read; calls 2-5 are the fill's own
  // chunk reads (4 chunks: 3 full + 1 byte). Delete the fill's private temp
  // file out from under it during its second chunk (overall call 3) --
  // the write-through's .part is untouched, so this is purely a "the fill's
  // own part vanished" race, not a shared-stream race.
  auto owned = std::make_unique<PartVanishingStorage>(f.backing_dir, fill_part, /*vanish_on_call=*/3);
  f.backing = owned.get();
  f.tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);
  f.tier->startFillWorker();

  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 0, 10), Status::kSuccess);
  delete[] data;
  f.tier->waitFillIdle();

  // No complete local file appears -- in particular not a truncated one --
  // and the failure is counted, not silently swallowed or double-counted
  // against fill_skipped_budget.
  EXPECT_FALSE(f.local(name));
  EXPECT_FALSE(std::filesystem::exists(fill_part));
  auto s = f.tier->stats();
  EXPECT_EQ(s.fill_failed, 1u);
  EXPECT_EQ(s.fills, 0u);
  EXPECT_EQ(s.fill_skipped_budget, 0u);
}

TEST(DiskCacheStorageTest, StopIsSerialisedAcrossTheJoinAndWaitFillIdleIsNeverStranded) {
  TierFixture f(1u << 20, /*chunk=*/1024);
  DiskCacheStorage::Options o = f.tier->options();
  f.tier.reset();
  auto owned = std::make_unique<SlowStorage>(f.backing_dir, std::chrono::milliseconds(150));
  SlowStorage* slow = owned.get();
  f.backing = owned.get();
  f.tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);

  std::string const name1 = "sstable1/" + stamp() + "_1.sst";
  std::string const name2 = "sstable1/" + stamp() + "_2.sst";
  f.seed(name1, 100);
  f.seed(name2, 100);

  // Two misses queue two fills before the worker runs, so both survive in
  // the queue for the worker to pick up once started.
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name1, data, 0, 10), Status::kSuccess);
  delete[] data;
  ASSERT_EQ(f.tier->read(name2, data, 0, 10), Status::kSuccess);
  delete[] data;

  // Snapshot the read count *before* starting the worker: the two misses
  // above already called SlowStorage::read() once each, directly on the main
  // thread, so the count is nonzero here. Only the worker thread can advance
  // it from this point on, which is what makes "wait for it to advance by 1"
  // below a wait on the worker's own dequeue-and-read of name1, not on
  // something that already happened.
  int const reads_before = slow->reads();
  f.tier->startFillWorker();
  // Wait, bounded, for the worker to actually dequeue name1 and enter its
  // (slow) chunk read before requesting a stop. Nothing else orders the
  // worker's fill_queue_.pop_front() ahead of stopFillWorker() setting
  // fill_stop_; a bare sleep before start races that dequeue and can leave
  // name1 unfilled. The bound turns a skipped fill (which would otherwise
  // hang this wait forever) into a clear assertion failure instead. Once the
  // wait is satisfied, the ~150ms delay inside that read holds the worker
  // there long enough for the stop and the waiter thread below to land
  // before the read returns.
  ASSERT_TRUE(slow->waitForReadsAtLeast(reads_before + 1, std::chrono::seconds(5)));
  std::thread waiter([&f] { f.tier->waitFillIdle(); });
  f.tier->stopFillWorker();
  waiter.join();  // must return promptly: this is the regression under test

  EXPECT_TRUE(f.local(name1));   // the in-flight fill was allowed to finish
  EXPECT_FALSE(f.local(name2));  // never started; still sitting in the queue

  // A second stop must not throw std::system_error from a racing join().
  EXPECT_NO_THROW(f.tier->stopFillWorker());

  // A restart drains what was left over, proving start/stop can cycle.
  f.tier->startFillWorker();
  f.tier->waitFillIdle();
  EXPECT_TRUE(f.local(name2));
}

TEST(DiskCacheStorageTest, BudgetEvictsTheLeastRecentlyReadFile) {
  TierFixture f(2500);
  auto put = [&](std::string const& name, size_t n) {
    std::vector<unsigned char> v(n, 'x');
    ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), static_cast<int>(n)), Status::kSuccess);
    ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  };
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  std::string const c = "sstable1/" + stamp() + "_c.sst";
  put(a, 1000);
  put(b, 1000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(a, data, 0, 10), Status::kSuccess);  // a is now the most recent
  delete[] data;
  put(c, 1000);  // 3000 > 2500: evict b, the least recently used
  EXPECT_TRUE(f.local(a));
  EXPECT_FALSE(f.local(b));
  EXPECT_TRUE(f.local(c));
  auto s = f.tier->stats();
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.evicted_bytes, 1000u);
  EXPECT_EQ(s.files, 2u);
  EXPECT_EQ(s.bytes, 2000u);
  // b is still on the backing: the tier evicts copies, never objects.
  EXPECT_TRUE(f.backing->exist(b));
}

TEST(DiskCacheStorageTest, RemoveDropsTheObjectAndTheCopy) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> v(100, 'x');
  ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), 100), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  ASSERT_TRUE(f.local(name));
  f.tier->remove(name);
  EXPECT_FALSE(f.local(name));
  EXPECT_FALSE(f.backing->exist(name));
  EXPECT_EQ(f.tier->stats().files, 0u);
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
}

TEST(DiskCacheStorageTest, InvalidateDropsOnlyTheCopy) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> v(100, 'x');
  ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), 100), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  f.tier->invalidate(name);
  EXPECT_FALSE(f.local(name));
  EXPECT_TRUE(f.backing->exist(name));
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
  f.tier->invalidate(name);  // absent: a no-op
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
  // A part in progress is discarded too.
  std::string const other = "sstable1/" + stamp() + "_part.sst";
  ASSERT_EQ(f.tier->appendNoFlush(other, v.data(), 100), Status::kSuccess);
  ASSERT_TRUE(f.part(other));
  f.tier->invalidate(other);
  EXPECT_FALSE(f.part(other));
  ASSERT_EQ(f.tier->flush(other), Status::kSuccess);  // the backing still flushes
  EXPECT_FALSE(f.local(other));                        // but nothing is published
}

TEST(DiskCacheStorageTest, InvalidateMidWriteThroughPoisonsThePart) {
  // A peer's COMPACT drops the file while the builder is still appending it.
  // discardPart poisons the name, so the appends that follow cannot be
  // reopened with `trunc` and published as a complete copy of the tail
  // (final review of PLAN-disk-cache, finding 7).
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> head(3000, 'a'), tail(2000, 'b');
  ASSERT_EQ(f.tier->appendNoFlush(name, head.data(), 3000), Status::kSuccess);
  ASSERT_TRUE(f.part(name));
  f.tier->invalidate(name);
  EXPECT_FALSE(f.part(name));
  ASSERT_EQ(f.tier->appendNoFlush(name, tail.data(), 2000), Status::kSuccess);
  EXPECT_FALSE(f.part(name));  // the poisoned name is not reopened
  EXPECT_EQ(f.tier->flush(name), Status::kSuccess);  // the status is the backing's
  EXPECT_FALSE(f.local(name));                       // and nothing was published
  EXPECT_FALSE(f.part(name));
  auto s = f.tier->stats();
  EXPECT_EQ(s.writethrough_files, 0u);
  EXPECT_EQ(s.files, 0u);
  EXPECT_EQ(f.backing->size(name), 5000u);  // the object itself is whole
  // The poison is cleared by that flush, so the tier is free to cache the
  // file again -- a read of it misses and is served by the backing.
  unsigned char* data = nullptr;
  size_t size = 0;
  ASSERT_EQ(f.tier->read(name, data, size), Status::kSuccess);
  EXPECT_EQ(size, 5000u);
  delete[] data;
  EXPECT_EQ(f.tier->stats().misses, 1u);
}

TEST(DiskCacheStorageTest, UnwritableDirThrows) {
  // A tier configured onto a path it cannot use must fail the open, not
  // degrade into a silent pass-through (final review of PLAN-disk-cache,
  // finding 4b). Both cases run as an ordinary user.
  std::string const base = kRoot + "unusable_" + stamp() + "/";
  std::filesystem::create_directories(base + "backing/sstable1");
  auto make = [&](std::string const& dir) {
    DiskCacheStorage::Options o;
    o.dir = dir;
    o.capacity_bytes = 1u << 20;
    return std::make_unique<DiskCacheStorage>(std::make_unique<FileStorage>(base + "backing/"), o);
  };
  // (a) The parent is a regular file, so create_directories cannot make it.
  { std::ofstream(base + "regular") << "x"; }
  EXPECT_THROW(make(base + "regular/tier"), std::runtime_error);
  // (b) The directory is there but has no write permission, so only the
  //     probe catches it: create_directories succeeds on an existing dir.
  std::string const ro = base + "ro/";
  std::filesystem::create_directories(ro);
  std::filesystem::permissions(ro, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec,
                               std::filesystem::perm_options::replace);
  EXPECT_THROW(make(ro), std::runtime_error);
  std::filesystem::permissions(ro, std::filesystem::perms::owner_all, std::filesystem::perm_options::replace);
  // A usable directory still constructs, and the probe leaves nothing behind.
  std::string const ok = base + "ok/";
  EXPECT_NO_THROW(make(ok));
  EXPECT_TRUE(std::filesystem::is_directory(ok));
  EXPECT_TRUE(std::filesystem::is_empty(ok));
  std::filesystem::remove_all(base);
}

TEST(DiskCacheStorageTest, ReconcileDropsPartsDeadFilesAndForeignNames) {
  TierFixture f(1u << 20);
  auto write = [&](std::string const& rel, size_t n) {
    std::filesystem::create_directories(std::filesystem::path(f.tier_dir + rel).parent_path());
    std::ofstream out(f.tier_dir + rel, std::ios::binary);
    std::string s(n, 'z');
    out.write(s.data(), static_cast<std::streamsize>(n));
  };
  write("sstable1/live.sst", 300);
  write("sstable1/dead.sst", 300);
  write("sstable1/wrongsize.sst", 300);
  write("sstable1/inflight.sst.part", 50);
  std::string const fillpart = "sstable1/" + stamp() + "_x.fillpart";
  write(fillpart, 50);
  write("checkpoint/LATEST", 8);  // not cacheable: a leftover of another config
  std::vector<std::string> asked;
  size_t const removed = f.tier->reconcile([&](std::string const& name, size_t bytes) {
    asked.push_back(name);
    if (name == "sstable1/live.sst") return bytes == 300;
    if (name == "sstable1/wrongsize.sst") return bytes == 999;
    return false;
  });
  EXPECT_EQ(removed, 5u);
  EXPECT_TRUE(f.local("sstable1/live.sst"));
  EXPECT_FALSE(f.local("sstable1/dead.sst"));
  EXPECT_FALSE(f.local("sstable1/wrongsize.sst"));
  EXPECT_FALSE(f.part("sstable1/inflight.sst"));
  EXPECT_FALSE(std::filesystem::exists(f.tier_dir + fillpart));
  EXPECT_FALSE(f.local("checkpoint/LATEST"));
  EXPECT_EQ(asked.size(), 3u);  // the part, the fillpart and the foreign name are not asked
  auto s = f.tier->stats();
  EXPECT_EQ(s.files, 1u);
  EXPECT_EQ(s.bytes, 300u);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read("sstable1/live.sst", data, 0, 10), Status::kSuccess);
  delete[] data;
  EXPECT_EQ(f.tier->stats().hits, 1u);
}

TEST(DiskCacheStorageTest, MetadataParsesTheDiskCacheKeys) {
  std::string const dir = kRoot + "cfg_" + stamp() + "/";
  std::filesystem::create_directories(dir);
  // parseJSON (src/util/read_json.cpp) is line-oriented: it skips a whole
  // line whenever it starts with '{' or '}', so it expects one key per
  // line with '{'/'}' each on their own line -- exactly how every real
  // config under src/config/ is pretty-printed. Writing the object as one
  // long line (as a literal `{"a": 1, "b": 2}` would) makes the entire
  // body land on a single line whose first character is '{', so the whole
  // thing is skipped and every key comes back missing. `write` below
  // wraps a body of one "key": value per line instead.
  auto write = [&](std::string const& body) {
    std::string const path = dir + "cfg.json";
    std::ofstream(path) << "{\n" << body << "\n}\n";
    return path;
  };
  // Every key Metadata's constructor requires unconditionally (copied from
  // src/config/local/shared_config_base.json), plus sstable_backend /
  // sstable_dir / lru_cache_bytes for the disk-cache keys under test.
  std::string const base =
      "\"backend\": \"local\",\n"
      "\"db_path\": \"" + dir + "db/\",\n"
      "\"mode\": 1,\n"
      "\"log_file_size_limit\": 33554432,\n"
      "\"base_file_number_limit\": 2,\n"
      "\"level_size\": \"[268435456, 2684354560]\",\n"
      "\"level_file_size_limit\": \"[67108864, 67108864]\",\n"
      "\"max_level\": 2,\n"
      "\"compaction_policy\": 0,\n"
      "\"sstable_backend\": \"local\",\n"
      "\"sstable_dir\": \"" + dir + "sst/\",\n"
      "\"lru_cache_bytes\": 1048576";
  {
    // Explicit non-default values for every disk-cache key, so this assert
    // block actually exercises the parse for disk_cache_chunk_bytes and
    // disk_cache_fill_queue too -- asserting only the 67108864 / 256
    // defaults would pass whether or not that parsing ever ran.
    Metadata md(write(base +
        ",\n\"disk_cache_dir\": \"/tank/cache/w0\",\n"
        "\"disk_cache_bytes\": \"2147483648\",\n"
        "\"disk_cache_drop_pages\": \"false\",\n"
        "\"disk_cache_chunk_bytes\": \"1048576\",\n"
        "\"disk_cache_fill_queue\": \"8\",\n"
        "\"disk_cache_mode\": \"chunk\",\n"
        "\"disk_cache_entry_bytes\": \"16384\",\n"
        "\"disk_cache_admission\": \"frequency\",\n"
        "\"disk_cache_admit_window\": \"4096\""));
    EXPECT_EQ(md.disk_cache_dir, "/tank/cache/w0/");  // slash appended
    EXPECT_EQ(md.disk_cache_bytes, 2147483648ull);
    EXPECT_FALSE(md.disk_cache_drop_pages);
    EXPECT_EQ(md.disk_cache_chunk_bytes, 1048576ull);
    EXPECT_EQ(md.disk_cache_fill_queue, 8ull);
    EXPECT_EQ(md.disk_cache_mode, "chunk");
    EXPECT_EQ(md.disk_cache_entry_bytes, 16384ull);
    EXPECT_EQ(md.disk_cache_admission, "frequency");
    EXPECT_EQ(md.disk_cache_admit_window, 4096ull);
  }
  {
    Metadata md(write(base));
    EXPECT_TRUE(md.disk_cache_dir.empty());
    EXPECT_TRUE(md.disk_cache_drop_pages);
    EXPECT_EQ(md.disk_cache_chunk_bytes, 67108864ull);
    EXPECT_EQ(md.disk_cache_fill_queue, 256ull);
    EXPECT_EQ(md.disk_cache_mode, "chunk");
    EXPECT_EQ(md.disk_cache_entry_bytes, 65536ull);
    EXPECT_EQ(md.disk_cache_admission, "frequency");
    EXPECT_EQ(md.disk_cache_admit_window, 0ull);
  }
  EXPECT_THROW(Metadata(write(base + ",\n\"disk_cache_dir\": \"/tank/cache/w0\"")),
               std::runtime_error);  // bytes missing
  std::string const tier_on = base + ",\n\"disk_cache_dir\": \"/tank/cache/w0\",\n\"disk_cache_bytes\": \"1\"";
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_mode\": \"block\"")), std::runtime_error);
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_admission\": \"lru\"")), std::runtime_error);
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_entry_bytes\": \"3000\"")), std::runtime_error);  // not a power of two
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_entry_bytes\": \"2048\"")), std::runtime_error);  // below 4096
  std::string const no_backend =
      "\"backend\": \"local\",\n"
      "\"db_path\": \"" + dir + "db/\",\n"
      "\"mode\": 1,\n"
      "\"log_file_size_limit\": 33554432,\n"
      "\"base_file_number_limit\": 2,\n"
      "\"level_size\": \"[268435456, 2684354560]\",\n"
      "\"level_file_size_limit\": \"[67108864, 67108864]\",\n"
      "\"max_level\": 2,\n"
      "\"compaction_policy\": 0,\n"
      "\"disk_cache_dir\": \"/tank/cache/w0\",\n"
      "\"disk_cache_bytes\": \"1\"";
  EXPECT_THROW(Metadata(write(no_backend)), std::runtime_error);  // no sstable_backend
  std::filesystem::remove_all(dir);
}

TEST(DiskCacheStorageTest, FrequencyAdmissionRefusesAFillThatWouldEvictAnEquallyUsedFile) {
  TierFixture f(2500, /*chunk=*/1024, /*drop_pages=*/true, DiskCacheStorage::Admission::kFrequency);
  f.tier->startFillWorker();
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  std::string const c = "sstable1/" + stamp() + "_c.sst";
  f.seed(a, 1000);
  f.seed(b, 1000);
  f.seed(c, 1000);
  f.touch(a);  // free budget: filled without a contest
  f.touch(b);
  EXPECT_TRUE(f.local(a));
  EXPECT_TRUE(f.local(b));
  f.touch(c);  // 3000 > 2500, and c (one access) is not above the LRU victim a (one access)
  EXPECT_FALSE(f.local(c));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.fill_bytes, 2000u);         // the refused fill moved no bytes
  EXPECT_EQ(s.fetch_bytes, 2000u + 30u);  // two fills plus three 10-byte misses
  EXPECT_EQ(s.files, 2u);
}

TEST(DiskCacheStorageTest, FrequencyAdmissionLetsAHotterFileDisplaceTheColdestOne) {
  TierFixture f(2500, /*chunk=*/1024, /*drop_pages=*/true, DiskCacheStorage::Admission::kFrequency);
  f.tier->startFillWorker();
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  std::string const c = "sstable1/" + stamp() + "_c.sst";
  f.seed(a, 1000);
  f.seed(b, 1000);
  f.seed(c, 1000);
  f.touch(a);
  f.touch(b);
  f.touch(c);  // c = 1, victim a = 1: refused
  f.touch(c);  // c = 2 > a = 1: admitted, a evicted
  EXPECT_TRUE(f.local(c));
  EXPECT_FALSE(f.local(a));
  EXPECT_TRUE(f.local(b));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.files, 2u);
}

TEST(DiskCacheStorageTest, WriteThroughUnderFrequencyAdmissionTakesOnlyFreeBudget) {
  TierFixture f(2500, /*chunk=*/64u << 20, /*drop_pages=*/true, DiskCacheStorage::Admission::kFrequency);
  auto put = [&](std::string const& name, size_t n) {
    std::vector<unsigned char> v(n, 'x');
    ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), static_cast<int>(n)), Status::kSuccess);
    ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  };
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  std::string const c = "sstable1/" + stamp() + "_c.sst";
  put(a, 1000);
  put(b, 1000);
  put(c, 1000);  // no free budget and no history: refused, nothing evicted
  EXPECT_TRUE(f.local(a));
  EXPECT_TRUE(f.local(b));
  EXPECT_FALSE(f.local(c));
  EXPECT_FALSE(f.part(c));
  EXPECT_TRUE(f.backing->exist(c));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.writethrough_files, 2u);
}

TEST(DiskCacheStorageTest, FillUsesTheMemoisedObjectSize) {
  TierFixture f(1u << 20, /*chunk=*/1024);
  f.tier->startFillWorker();
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  f.seed(a, 3000);
  f.touch(a);  // miss + fill: one size() on the backing
  EXPECT_EQ(f.backing->sizes, 1);
  f.touch(a);  // hit: none
  EXPECT_EQ(f.backing->sizes, 1);
  f.tier->invalidate(a);  // drops the memo with the copy
  f.touch(a);
  EXPECT_EQ(f.backing->sizes, 2);
}

TEST(DiskCacheStorageTest, ChunkMissFetchesTheCoveringChunksAndTheNextReadHits) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 3 * 1024 + 1);
  f.readAt(name, bytes, 5, 10);  // chunk 0
  EXPECT_EQ(f.backing->ranged_reads, 1);
  EXPECT_EQ(f.backing->last_a, 0u);
  EXPECT_EQ(f.backing->last_len, 1024u);
  auto s = f.tier->stats();
  EXPECT_EQ(s.misses, 1u);
  EXPECT_EQ(s.miss_bytes, 10u);
  EXPECT_EQ(s.fetch_bytes, 1024u);
  EXPECT_EQ(s.fills, 1u);
  EXPECT_EQ(s.fill_bytes, 1024u);
  EXPECT_EQ(s.fill_gets, 0u);  // the fill is the demand read
  EXPECT_EQ(s.entries, 1u);
  EXPECT_EQ(s.bytes, 1024u);
  EXPECT_EQ(s.files, 1u);
  f.readAt(name, bytes, 100, 200);  // inside chunk 0: a hit
  EXPECT_EQ(f.backing->ranged_reads, 1);
  EXPECT_EQ(f.tier->stats().hits, 1u);
  f.readAt(name, bytes, 1020, 10);  // straddles chunks 0 and 1; chunk 1 is absent
  EXPECT_EQ(f.backing->ranged_reads, 2);
  EXPECT_EQ(f.backing->last_a, 0u);  // the whole covering range, present chunk included
  EXPECT_EQ(f.backing->last_len, 2048u);
  s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.fills, 2u);
  EXPECT_EQ(s.fill_skipped_present, 1u);
  EXPECT_EQ(s.fetch_bytes, 1024u + 2048u);
  f.readAt(name, bytes, 3 * 1024, 1);  // the short last chunk
  EXPECT_EQ(f.backing->last_a, 3072u);
  EXPECT_EQ(f.backing->last_len, 1u);
  EXPECT_EQ(f.tier->stats().bytes, 2048u + 1u);
  // The local copy holds each fetched chunk at its own offset; chunk 2 is a hole.
  std::ifstream in(f.tier_dir + name, std::ios::binary);
  std::vector<unsigned char> copy((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  ASSERT_EQ(copy.size(), bytes.size());
  EXPECT_EQ(0, memcmp(copy.data(), bytes.data(), 2048));
  EXPECT_EQ(copy[3072], bytes[3072]);
}

TEST(DiskCacheStorageTest, ChunkBudgetEvictsWithClockAndTheVictimIsFetchedAgain) {
  TierFixture f(2048, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 4096);
  auto chunk = [&](size_t c) { f.readAt(name, bytes, c * 1024 + 1, 8); };
  chunk(0);
  chunk(1);  // 2048 of 2048
  EXPECT_EQ(f.tier->stats().entries, 2u);
  chunk(2);  // hand: 0 referenced -> cleared, 1 cleared, wrap, 0 evicted
  auto s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.evicted_bytes, 1024u);
  EXPECT_EQ(s.bytes, 2048u);
  EXPECT_EQ(s.punch_failed, 0u);
  int const before = f.backing->ranged_reads;
  chunk(0);  // absent again: fetched; this time chunk 1 is the victim
  EXPECT_EQ(f.backing->ranged_reads, before + 1);
  s = f.tier->stats();
  EXPECT_EQ(s.evictions, 2u);
  EXPECT_EQ(s.entries, 2u);
  chunk(2);  // still present: a hit
  EXPECT_EQ(f.backing->ranged_reads, before + 1);
  EXPECT_EQ(f.tier->stats().hits, 1u);
  EXPECT_TRUE(f.local(name));  // the sparse file stays while any chunk is present
}

TEST(DiskCacheStorageTest, ChunkEvictingTheLastChunkDropsTheFile) {
  TierFixture f(1024, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  auto ba = f.seed(a, 1024);
  auto bb = f.seed(b, 1024);
  f.readAt(a, ba, 0, 8);
  f.readAt(b, bb, 0, 8);  // evicts a's only chunk
  EXPECT_FALSE(f.local(a));
  EXPECT_TRUE(f.local(b));
  auto s = f.tier->stats();
  EXPECT_EQ(s.files, 1u);
  EXPECT_EQ(s.entries, 1u);
  EXPECT_EQ(s.bytes, 1024u);
}

TEST(DiskCacheStorageTest, ChunkInvalidateAndRemoveDropTheCopy) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 2048);
  f.readAt(name, bytes, 0, 8);
  EXPECT_TRUE(f.local(name));
  f.tier->invalidate(name);
  EXPECT_FALSE(f.local(name));
  auto s = f.tier->stats();
  EXPECT_EQ(s.invalidated, 1u);
  EXPECT_EQ(s.entries, 0u);
  EXPECT_EQ(s.bytes, 0u);
  EXPECT_TRUE(f.backing->exist(name));
  f.readAt(name, bytes, 0, 8);  // fetched again
  EXPECT_EQ(f.backing->ranged_reads, 2);
  f.tier->remove(name);
  EXPECT_FALSE(f.local(name));
  EXPECT_FALSE(f.backing->exist(name));
  EXPECT_EQ(f.tier->stats().entries, 0u);
}

TEST(DiskCacheStorageTest, ChunkModeHasNoFillWorkerAndReconcileStartsCold) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  f.tier->startFillWorker();  // a no-op in chunk mode
  f.tier->waitFillIdle();     // returns at once
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 2048);
  f.readAt(name, bytes, 0, 8);
  f.tier->stopFillWorker();
  EXPECT_EQ(f.tier->stats().fill_gets, 0u);
  // reconcile() in chunk mode deletes every local file: which chunks of a
  // leftover sparse file are data is not recorded.
  std::filesystem::create_directories(f.tier_dir + "sstable1");
  std::ofstream(f.tier_dir + "sstable1/leftover.sst") << "x";
  EXPECT_EQ(f.tier->reconcile([](std::string const&, size_t) { return true; }), 2u);
  EXPECT_FALSE(f.local(name));
  EXPECT_EQ(f.tier->stats().entries, 0u);
}

// Two threads miss on the same chunk while the first fetch is still in the
// backing. Both fetch, both return the right bytes, and exactly one of them
// writes the chunk: the other finds it kPending or kPresent and skips.
TEST(DiskCacheStorageTest, ChunkTwoConcurrentMissesFetchTwiceAndWriteOnce) {
  std::string const s = stamp();
  std::string const backing_dir = kRoot + "backing_" + s + "/";
  std::string const tier_dir = kRoot + "tier_" + s + "/";
  std::filesystem::create_directories(backing_dir + "sstable1");
  std::filesystem::create_directories(tier_dir);
  auto owned = std::make_unique<SlowStorage>(backing_dir, std::chrono::milliseconds(300));
  SlowStorage* slow = owned.get();
  DiskCacheStorage::Options o;
  o.dir = tier_dir;
  o.capacity_bytes = 1u << 20;
  o.mode = DiskCacheStorage::Mode::kChunk;
  o.entry_bytes = 1024;
  auto tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);
  std::string const name = "sstable1/" + s + ".sst";
  std::vector<unsigned char> bytes(2048);
  for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<unsigned char>(i % 251);
  std::ofstream(backing_dir + name, std::ios::binary).write(reinterpret_cast<char const*>(bytes.data()), 2048);

  unsigned char* d1 = nullptr;
  Status s1 = Status::kFailure;
  std::thread t([&] { s1 = tier->read(name, d1, 0, 8); });
  ASSERT_TRUE(slow->waitForReadsAtLeast(1, std::chrono::seconds(5)));  // the first read is inside the backing
  unsigned char* d2 = nullptr;
  ASSERT_EQ(tier->read(name, d2, 4, 8), Status::kSuccess);  // chunk 0 is not present: a second miss
  t.join();
  ASSERT_EQ(s1, Status::kSuccess);
  EXPECT_EQ(0, memcmp(d1, bytes.data(), 8));
  EXPECT_EQ(0, memcmp(d2, bytes.data() + 4, 8));
  delete[] d1;
  delete[] d2;
  EXPECT_EQ(slow->reads(), 2);
  auto st = tier->stats();
  EXPECT_EQ(st.misses, 2u);
  EXPECT_EQ(st.entries, 1u);
  EXPECT_EQ(st.bytes, 1024u);
  EXPECT_EQ(st.fills + st.fill_skipped_present, 2u);
  tier.reset();
  std::filesystem::remove_all(backing_dir);
  std::filesystem::remove_all(tier_dir);
}

TEST(DiskCacheStorageTest, ChunkWriteThroughPublishesEveryChunk) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> v(2500);
  for (size_t i = 0; i < v.size(); ++i) v[i] = static_cast<unsigned char>(i % 251);
  ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), 2500), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  auto s = f.tier->stats();
  EXPECT_EQ(s.writethrough_files, 1u);
  EXPECT_EQ(s.entries, 3u);
  EXPECT_EQ(s.bytes, 2500u);
  EXPECT_FALSE(f.part(name));
  f.readAt(name, v, 2040, 20);  // straddles chunks 1 and 2: a hit
  EXPECT_EQ(f.backing->ranged_reads, 0);
  EXPECT_EQ(f.tier->stats().hits, 1u);
}

TEST(DiskCacheStorageTest, ChunkWholeFileReadIsServedOnlyWhenComplete) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 2048);
  unsigned char* d = nullptr;
  size_t n = 0;
  ASSERT_EQ(f.tier->read(name, d, n), Status::kSuccess);  // incomplete: the backing, never admitted
  EXPECT_EQ(n, 2048u);
  EXPECT_EQ(0, memcmp(d, bytes.data(), 2048));
  delete[] d;
  EXPECT_EQ(f.backing->full_reads, 1);
  EXPECT_EQ(f.tier->stats().entries, 0u);
  f.readAt(name, bytes, 0, 8);
  f.readAt(name, bytes, 1024, 8);  // both chunks present now
  ASSERT_EQ(f.tier->read(name, d, n), Status::kSuccess);  // complete: local
  EXPECT_EQ(0, memcmp(d, bytes.data(), 2048));
  delete[] d;
  EXPECT_EQ(f.backing->full_reads, 1);
  EXPECT_EQ(f.tier->stats().hits, 1u);
}

TEST(DiskCacheStorageTest, ChunkFrequencyAdmissionKeepsTheHotterChunk) {
  TierFixture f(2048, 64u << 20, true, Adm::kFrequency, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 4096);
  auto chunk = [&](size_t c) { f.readAt(name, bytes, c * 1024 + 1, 8); };
  chunk(0);  // free budget
  chunk(1);  // free budget
  chunk(2);  // 2 (one access) is not above the victim (one access): refused, still fetched
  auto s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.fetch_bytes, 3 * 1024u);
  chunk(2);  // 2 accesses > 1: admitted, one victim evicted
  s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.admit_rejected, 1u);
  int const before = f.backing->ranged_reads;
  chunk(2);  // a hit now
  EXPECT_EQ(f.backing->ranged_reads, before);
}

TEST(DiskCacheStorageTest, ChunkWriteThroughUnderFrequencyAdmissionTakesOnlyFreeBudget) {
  TierFixture f(2048, 64u << 20, true, Adm::kFrequency, Mode::kChunk, /*entry=*/1024);
  std::string const x = "sstable1/" + stamp() + "_x.sst";
  std::string const y = "sstable1/" + stamp() + "_y.sst";
  auto bx = f.seed(x, 2048);
  f.readAt(x, bx, 0, 8);
  f.readAt(x, bx, 1024, 8);  // the budget is full
  std::vector<unsigned char> v(1000, 'y');
  ASSERT_EQ(f.tier->appendNoFlush(y, v.data(), 1000), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(y), Status::kSuccess);
  EXPECT_FALSE(f.local(y));
  EXPECT_FALSE(f.part(y));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.entries, 2u);
  EXPECT_TRUE(f.backing->exist(y));
}
