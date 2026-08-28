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
