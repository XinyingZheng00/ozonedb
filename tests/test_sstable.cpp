#include "db.h"
#include "gtest/gtest.h"
#include "sstable/comparator.h"
#include "sstable/sstable_handler.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "storage.h"
using namespace ozonedb;
TEST(SSTableTest, BuildTableTest) {
  Storage* storage = new FileStorage("/tank/test/ss_table_test/");
  std::string fileName = "buildTableTest" + std::to_string(time(0));
  TableBuilder* tb = new TableBuilder(storage, fileName);

  // generate key value pairs
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    keys.push_back("key" + std::to_string(i));
  };
  std::sort(keys.begin(), keys.end());
  std::cout << "ready to add key value pairs to table" << std::endl;
  // add key value pairs to table
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key(keys[i]);
    record.set_value("value");
    record.set_type(kTypeValue);
    tb->add(keys[i], record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
}

TEST(SSTableTest, ReadTableTest) {
  Storage* storage = new FileStorage("/tank/test/ss_table_test/");
  std::string fileName = "ReadTableTest" + std::to_string(time(0));
  TableBuilder* tb = new TableBuilder(storage, fileName);
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    keys.push_back("key" + std::to_string(i));
  };
  std::sort(keys.begin(), keys.end());
  std::cout << "ready to add key value pairs to table" << std::endl;
  // add key value pairs to table
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key(keys[i]);
    record.set_value("value");
    record.set_type(kTypeValue);
    tb->add(keys[i], record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
  std::cout << "finish adding key value pairs to table" << std::endl;

  // read table
  Table* table = nullptr;
  LRUCache* lru_cache = new LRUCache(33554432, storage);
  lru_cache->setFileMutexManager(new FileMutexManager());
  lru_cache->getSSTable(fileName, table);
  ASSERT_NE(nullptr, table);
  std::cout << "ready to read key value pairs to table" << std::endl;
  for (int i = 0; i < 100; i++) {
    std::shared_ptr<Record> record;
    table->get(keys[i], record);
    ASSERT_EQ(keys[i], record->key());
    ASSERT_EQ("value", record->value());
    ASSERT_EQ(kTypeValue, record->type());
  }
  std::shared_ptr<Record> record;
  status = table->get("key100", record);
  ASSERT_EQ(status, Status::kNotFound);
}

TEST(SSTableTest, SSTableReadWholeTableTest) {
  Storage* storage = new FileStorage("/tank/test/ss_table_test/");
  std::string fileName = "SSTableReadWholeTableTest" + std::to_string(time(0));
  TableBuilder* tb = new TableBuilder(storage, fileName);

  // generate key value pairs
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    keys.push_back("key" + std::to_string(i));
  };
  std::sort(keys.begin(), keys.end());
  std::cout << "ready to add key value pairs to table" << std::endl;
  // add key value pairs to table
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key(keys[i]);
    record.set_value("value");
    record.set_type(kTypeValue);
    tb->add(keys[i], record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
  std::cout << "finish adding key value pairs to table" << std::endl;

  // read table
  Table* table = nullptr;
  LRUCache* lru_cache = new LRUCache(33554432, storage);
  lru_cache->setFileMutexManager(new FileMutexManager());
  lru_cache->getSSTable(fileName, table);
  ASSERT_NE(nullptr, table);
  assert(status == Status::kSuccess);
  std::cout << "ready to read key value pairs to table" << std::endl;
  std::unordered_map<std::string, std::shared_ptr<Record>> result = table->getAll();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(result[keys[i]]->key(), keys[i]);
    ASSERT_EQ(result[keys[i]]->value(), "value");
    ASSERT_EQ(result[keys[i]]->type(), kTypeValue);
  }
  delete table;
}

TEST(SSTableTest, ReadTableWithFilterTest) {
  Storage* storage = new FileStorage("/tank/test/ss_table_test/");
  std::string fileName = "ReadTableWithFilterTest" + std::to_string(time(0));
  TableBuilder* tb = new TableBuilder(storage, fileName);
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    if (i == 55) {
      continue;
    }
    keys.push_back("key" + std::to_string(i));
  };
  std::sort(keys.begin(), keys.end());
  std::cout << "ready to add key value pairs to table" << std::endl;
  // add key value pairs to table
  std::string value;
  for (int i = 0; i < 50; ++i) {
    value += "value";  // Append each "value" to the string
  }
  for (int i = 0; i < 99; i++) {
    Record record;
    record.set_key(keys[i]);
    record.set_value(value);
    record.set_type(kTypeValue);
    tb->add(keys[i], record);
  }
  Status status = tb->finish();
  ASSERT_EQ(status, Status::kSuccess);
  std::cout << "finish adding key value pairs to table" << std::endl;

  // read table
  Table* table = nullptr;
  LRUCache* lru_cache = new LRUCache(33554432, storage);
  lru_cache->setFileMutexManager(new FileMutexManager());
  lru_cache->getSSTable(fileName, table);
  ASSERT_NE(nullptr, table);
  std::cout << "ready to read key value pairs to table" << std::endl;
  for (int i = 0; i < 99; i++) {
    std::shared_ptr<Record> record;
    table->get(keys[i], record);
    ASSERT_EQ(keys[i], record->key());
    ASSERT_EQ(value, record->value());
    ASSERT_EQ(kTypeValue, record->type());
  }
  std::shared_ptr<Record> record;
  // count the time for the get
  auto start = std::chrono::high_resolution_clock::now();
  status = table->get("key55", record);
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_seconds = end - start;
  std::cout << "elapsed time with filter: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() << "ns\n";
  ASSERT_EQ(status, Status::kNotFound);

  table->setFilterReaderToNull();
  // count the time for the get
  start = std::chrono::high_resolution_clock::now();
  status = table->get("key55", record);
  end = std::chrono::high_resolution_clock::now();
  elapsed_seconds = end - start;
  std::cout << "elapsed time without filter: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() << "ns\n";
  ASSERT_EQ(status, Status::kNotFound);

  status = table->get("key100", record);
  ASSERT_EQ(status, Status::kNotFound);
}
