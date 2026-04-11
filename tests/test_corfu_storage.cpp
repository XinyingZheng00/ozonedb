#ifdef OZONEDB_ENABLE_CORFU
#include "corfu_storage.h"
#include "gtest/gtest.h"
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

using namespace ozonedb;

namespace {

class CorfuStorageEnv {
 public:
  static std::string endpoint() {
    char const* e = std::getenv("CORFU_TEST_ENDPOINT");
    return e ? std::string(e) : std::string();
  }
  static std::string jarPath() {
    char const* j = std::getenv("CORFU_BRIDGE_JAR");
    return j ? std::string(j) : std::string();
  }
  static bool available() {
    return !endpoint().empty() && !jarPath().empty();
  }
  static std::string uniqueStream(std::string const& prefix) {
    return prefix + "_" + std::to_string(time(nullptr));
  }
};

#define SKIP_IF_NO_CORFU()                                                                  \
  do {                                                                                      \
    if (!CorfuStorageEnv::available()) {                                                    \
      GTEST_SKIP() << "CORFU_TEST_ENDPOINT / CORFU_BRIDGE_JAR not set; skipping Corfu test"; \
    }                                                                                       \
  } while (0)

CorfuDBStorage* makeStorage(std::string const& stream) {
  return new CorfuDBStorage(
      CorfuStorageEnv::endpoint(),
      CorfuStorageEnv::jarPath(),
      /*jvm_opts=*/"-Xmx512m",
      stream,
      /*db_path=*/"test/");
}

}  // namespace

TEST(CorfuStorageTest, append_and_read_back) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_rw");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  EXPECT_EQ(Status::kSuccess, storage->append(file, data.data(), data.size()));

  unsigned char* read_data = nullptr;
  size_t size = 0;
  EXPECT_EQ(Status::kSuccess, storage->read(file, read_data, size));
  EXPECT_EQ(data.size(), size);
  for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], read_data[i]);
  delete[] read_data;
  delete storage;
}

TEST(CorfuStorageTest, partial_read) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_partial");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(Status::kSuccess, storage->append(file, data.data(), data.size()));
  }
  unsigned char* read_data = nullptr;
  EXPECT_EQ(Status::kSuccess, storage->read(file, read_data, 25, 5));
  for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], read_data[i]);
  delete[] read_data;
  delete storage;
}

TEST(CorfuStorageTest, seal_returns_kSealed_on_further_append) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_seal");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3};
  EXPECT_EQ(Status::kSuccess, storage->append(file, data.data(), data.size()));
  storage->seal(file);
  EXPECT_TRUE(storage->isSealed(file));
  EXPECT_EQ(Status::kSealed, storage->append(file, data.data(), data.size()));
  // idempotent
  storage->seal(file);
  EXPECT_TRUE(storage->isSealed(file));
  delete storage;
}

TEST(CorfuStorageTest, seal_persists_across_reopen) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_seal_reopen");
  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3};

  {
    Storage* s = makeStorage(stream);
    s->append(file, data.data(), data.size());
    s->seal(file);
    delete s;
  }
  {
    Storage* s = makeStorage(stream);
    // Give the tailer a moment to replay the stream.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(s->isSealed(file));
    delete s;
  }
}

TEST(CorfuStorageTest, batch_flush_round_trip) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_batch");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(Status::kSuccess, storage->appendNoFlush(file, data.data(), data.size()));
  }
  EXPECT_EQ(Status::kSuccess, storage->flush(file));

  unsigned char* read_data = nullptr;
  size_t size = 0;
  EXPECT_EQ(Status::kSuccess, storage->read(file, read_data, size));
  EXPECT_EQ(data.size() * 10, size);
  delete[] read_data;
  delete storage;
}

TEST(CorfuStorageTest, remove_clears_file) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_remove");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3};
  storage->append(file, data.data(), data.size());
  EXPECT_TRUE(storage->exist(file));
  storage->remove(file);
  EXPECT_FALSE(storage->exist(file));
  delete storage;
}

#endif  // OZONEDB_ENABLE_CORFU
