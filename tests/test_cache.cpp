#include "cache.h"
#include "gtest/gtest.h"
#include "helper.h"
#include "protobuf/record.pb.h"
#include "status.h"
#include "storage/file_storage.h"
#include "storage/shared_log_storage.h"
#include <algorithm>
#include <thread>

using namespace ozonedb;

// LRUCache Tests
class LRUCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create test records
    for (int i = 0; i < 100; i++) {
      Record* record = new Record();
      record->set_key("key" + std::to_string(i));
      record->set_value("value" + std::to_string(i));
      record->set_type(kTypeValue);
      test_records.push_back(record);
    }
  }

  void TearDown() override {
    for (auto* record : test_records) {
      delete record;
    }
  }

  std::string getTestFileName(std::string const& prefix) {
    return prefix + "_" + std::to_string(time(nullptr));
  }

  std::vector<Record*> test_records;
};

TEST_F(LRUCacheTest, FileStorageBasicOperations) {
  FileStorage* file_storage = new FileStorage("/tank/test/cache/");
  LRUCache* file_cache = new LRUCache(2, 33554432, file_storage, file_storage);
  std::string file_name = getTestFileName("logFileStorageBasicOperations");
  for (int i = 0; i < test_records.size(); i++) {
    size_t size;
    Status status = file_storage->append(file_name, *test_records[i], size);
    EXPECT_EQ(status, Status::kSuccess);
  }
  View latest_view;
  latest_view.file_size[file_name] = file_storage->size(file_name);
  file_cache->setLatestView(&latest_view);

  bool read_more = true;
  size_t cached_offset = 0;
  size_t size = 0;
  file_cache->checkReadMoreLog(file_name, read_more, cached_offset, size);
  EXPECT_EQ(read_more, true);
  EXPECT_EQ(cached_offset, 0);
  EXPECT_EQ(size, file_storage->size(file_name));

  file_cache->readDataLog(file_name, cached_offset, size);

  for (int i = 0; i < test_records.size(); i++) {
    Record* result = nullptr;
    file_cache->get(file_name, test_records[i]->key(), result);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->key(), test_records[i]->key());
    EXPECT_EQ(result->value(), test_records[i]->value());
  }
}

TEST_F(LRUCacheTest, SharedLogStorageBasicOperations) {
  Storage* log_storage = new SharedLogStorage("datalog");
  FileStorage* sst_storage = new FileStorage("/tank/test/cache/");
  LRUCache* file_cache = new LRUCache(2, 33554432, log_storage, sst_storage);
  std::string file_name = "sharedlog/0:100";
  for (int i = 0; i < test_records.size(); i++) {
    size_t size;
    Status status = log_storage->append(file_name, *test_records[i], size);
    EXPECT_EQ(status, Status::kSuccess);
  }
  View latest_view;
  latest_view.file_size[file_name] = log_storage->size(file_name);
  file_cache->setLatestView(&latest_view);

  bool read_more = true;
  size_t cached_offset = 0;
  size_t size = 0;
  file_cache->shouldReadMoreLog(file_name, read_more, cached_offset, size);
  EXPECT_EQ(read_more, true);
  EXPECT_EQ(cached_offset, 0);
  EXPECT_EQ(size, log_storage->size(file_name));

  file_cache->readDataLog(file_name, cached_offset, size);

  for (int i = 0; i < test_records.size(); i++) {
    Record* result = nullptr;
    file_cache->get(file_name, test_records[i]->key(), result);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->key(), test_records[i]->key());
    EXPECT_EQ(result->value(), test_records[i]->value());
  }
}