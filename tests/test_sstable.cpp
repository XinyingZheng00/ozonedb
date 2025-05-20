#include "gtest/gtest.h"
#include "helper.h"
#include "protobuf/record.pb.h"
#include "protobuf/sstable.pb.h"
#include "sstable/comparator.h"
#include "sstable/sstable_handler.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "storage/file_storage.h"
#include "storage/shared_log_storage.h"
#include <algorithm>
#include <chrono>

using namespace ozonedb;

class SSTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage = new FileStorage("/tank/test/ss_table_test/");
    log_storage = nullptr;
    lru_cache = new LRUCache(2, 33554432, log_storage, storage);
  }

  void TearDown() override {
    delete storage;
    delete log_storage;
    delete lru_cache;
  }

  std::string getTestFileName(std::string const& prefix) {
    return prefix + "_" + std::to_string(time(nullptr));
  }

  FileStorage* storage;
  Storage* log_storage;
  LRUCache* lru_cache;
};

TEST_F(SSTableTest, BuildTableTest) {
  std::string fileName = getTestFileName("buildTableTest");
  TableBuilder* tb = new TableBuilder(storage, fileName);

  // Generate key value pairs
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    keys.push_back("key" + std::to_string(i));
  }
  std::sort(keys.begin(), keys.end());

  // Add key value pairs to table
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key(keys[i]);
    record.set_value("value");
    record.set_type(kTypeValue);
    tb->add(keys[i], record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
  delete tb;
}

TEST_F(SSTableTest, ReadTableTest) {
  std::string fileName = getTestFileName("ReadTableTest");
  TableBuilder* tb = new TableBuilder(storage, fileName);

  // Generate and sort keys
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    keys.push_back("key" + std::to_string(i));
  }
  std::sort(keys.begin(), keys.end());  // Add key value pairs to table
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key(keys[i]);
    record.set_value("value");
    record.set_type(kTypeValue);
    tb->add(keys[i], record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
  delete tb;

  // Read table
  Table* table = nullptr;
  lru_cache->getSSTable(fileName, table);
  ASSERT_NE(table, nullptr);
  // Verify all records
  for (int i = 0; i < 100; i++) {
    Record* record = nullptr;
    status = table->get(keys[i], record);
    ASSERT_EQ(status, Status::kSuccess);
    ASSERT_NE(record, nullptr);
    EXPECT_EQ(record->key(), keys[i]);
    EXPECT_EQ(record->value(), "value");
    EXPECT_EQ(record->type(), kTypeValue);
  }

  // Test non-existent key
  Record* record = nullptr;
  status = table->get("key100", record);
  ASSERT_EQ(status, Status::kNotFound);
}

TEST_F(SSTableTest, SSTableReadWholeTableTest) {
  std::string fileName = getTestFileName("SSTableReadWholeTableTest");
  TableBuilder* tb = new TableBuilder(storage, fileName);

  // Generate and sort keys
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    keys.push_back("key" + std::to_string(i));
  }
  std::sort(keys.begin(), keys.end());

  // Add key value pairs to table
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key(keys[i]);
    record.set_value("value");
    record.set_type(kTypeValue);
    tb->add(keys[i], record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
  delete tb;

  // Read table
  Table* table = nullptr;
  lru_cache->getSSTable(fileName, table);
  ASSERT_NE(table, nullptr);

  // Get all records and verify
  std::unordered_map<std::string, Record*> result = table->getAll();
  ASSERT_EQ(result.size(), keys.size());

  for (int i = 0; i < 100; i++) {
    auto it = result.find(keys[i]);
    ASSERT_NE(it, result.end());
    EXPECT_EQ(it->second->key(), keys[i]);
    EXPECT_EQ(it->second->value(), "value");
    EXPECT_EQ(it->second->type(), kTypeValue);
  }
}

TEST_F(SSTableTest, ReadTableWithFilterTest) {
  std::string fileName = getTestFileName("ReadTableWithFilterTest");
  TableBuilder* tb = new TableBuilder(storage, fileName);

  // Generate keys (skipping key55)
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    if (i == 55) continue;
    keys.push_back("key" + std::to_string(i));
  }
  std::sort(keys.begin(), keys.end());

  // Create large value
  std::string value;
  for (int i = 0; i < 50; ++i) {
    value += "value";
  }

  // Add key value pairs to table
  for (auto const& key : keys) {
    Record record;
    record.set_key(key);
    record.set_value(value);
    record.set_type(kTypeValue);
    tb->add(key, record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
  delete tb;

  // Read table
  Table* table = nullptr;
  lru_cache->getSSTable(fileName, table);
  ASSERT_NE(table, nullptr);

  // Verify all records
  for (auto const& key : keys) {
    Record* record = nullptr;
    status = table->get(key, record);
    ASSERT_EQ(status, Status::kSuccess);
    ASSERT_NE(record, nullptr);
    EXPECT_EQ(record->key(), key);
    EXPECT_EQ(record->value(), value);
    EXPECT_EQ(record->type(), kTypeValue);
  }

  // Test filter performance for non-existent key
  Record* record = nullptr;

  // With filter
  auto start = std::chrono::high_resolution_clock::now();
  status = table->get("key55", record);
  auto end = std::chrono::high_resolution_clock::now();
  auto filter_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  ASSERT_EQ(status, Status::kNotFound);

  // Without filter
  table->setFilterReaderToNull();
  start = std::chrono::high_resolution_clock::now();
  status = table->get("key55", record);
  end = std::chrono::high_resolution_clock::now();
  auto no_filter_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  ASSERT_EQ(status, Status::kNotFound);

  std::cout << "Time with filter: " << filter_time << "ns\n";
  std::cout << "Time without filter: " << no_filter_time << "ns\n";
  EXPECT_LT(filter_time, no_filter_time);

  // Test another non-existent key
  status = table->get("key100", record);
  ASSERT_EQ(status, Status::kNotFound);
}
