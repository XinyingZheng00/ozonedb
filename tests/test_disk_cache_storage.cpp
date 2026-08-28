// tests/test_disk_cache_storage.cpp
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "disk_cache_storage.h"
#include "metadata.h"
#include "storage.h"

using namespace ozonedb;

namespace {

std::string const kRoot = "/tank/test/disk_cache/";

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
};

struct TierFixture {
  std::string backing_dir;
  std::string tier_dir;
  CountingStorage* backing;  // owned by the tier
  std::unique_ptr<DiskCacheStorage> tier;

  explicit TierFixture(uint64_t capacity, size_t chunk = 64u << 20, bool drop_pages = true) {
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
