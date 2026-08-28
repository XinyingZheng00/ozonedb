#include "db.h"
#include "gtest/gtest.h"
#include "sstable/comparator.h"
#include "sstable/sstable_handler.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "storage.h"
#include <filesystem>
#include <map>
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
  std::unordered_map<std::string, std::shared_ptr<Record>> result;
  ASSERT_EQ(table->getAll(result), Status::kSuccess);
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

namespace {
// Builds an SSTable of n records whose values are value_bytes long and
// returns key -> value. With 600-byte values and the 4 KiB data block
// target, 800 records span about 120 blocks.
std::map<std::string, std::string> buildTableForScan(Storage* storage, std::string const& fileName, int n, size_t value_bytes) {
  std::vector<std::string> keys;
  for (int i = 0; i < n; i++) keys.push_back("key" + std::to_string(i));
  std::sort(keys.begin(), keys.end());
  std::map<std::string, std::string> expected;
  TableBuilder tb(storage, fileName);
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

void expectSameRecords(std::map<std::string, std::string> const& expected,
                       std::unordered_map<std::string, std::shared_ptr<Record>> const& out) {
  ASSERT_EQ(out.size(), expected.size());
  for (auto const& [key, value] : expected) {
    auto it = out.find(key);
    ASSERT_NE(it, out.end()) << key;
    ASSERT_EQ(it->second->key(), key);
    ASSERT_EQ(it->second->value(), value);
    ASSERT_EQ(it->second->type(), kTypeValue);
  }
}
}  // namespace

// getAll must return the same records whatever the chunk size: one block
// per read (1 byte forces that, so does exactly one block), 2.5 blocks per
// read (chunks end on block boundaries), and the whole file in one read.
TEST(SSTableTest, GetAllChunkSizesTest) {
  Storage* storage = new FileStorage("/tank/test/ss_table_test/");
  std::string fileName = "GetAllChunkSizesTest" + std::to_string(time(0));
  auto expected = buildTableForScan(storage, fileName, 800, 600);

  Table* table = nullptr;
  ASSERT_EQ(Table::open(storage, fileName, table), Status::kSuccess);
  ASSERT_NE(nullptr, table);
  size_t const file_size = storage->size(fileName);
  ASSERT_GT(file_size, 100 * 4096u);  // the table really spans many blocks
  for (size_t max_read_bytes : {size_t{1}, size_t{4096}, size_t{10240}, size_t{1} << 20, file_size, Table::kDefaultScanReadBytes}) {
    std::unordered_map<std::string, std::shared_ptr<Record>> out;
    ASSERT_EQ(table->getAll(out, max_read_bytes), Status::kSuccess) << "max_read_bytes=" << max_read_bytes;
    expectSameRecords(expected, out);
  }
  delete table;
  delete storage;
}

// An index entry that points past the end of the file is a corrupt
// index: getAll must fail before it reads anything, and leave `out` empty.
TEST(SSTableTest, GetAllBadIndexTest) {
  Storage* storage = new FileStorage("/tank/test/ss_table_test/");
  std::string fileName = "GetAllBadIndexTest" + std::to_string(time(0));
  auto expected = buildTableForScan(storage, fileName, 100, 600);

  Table* table = nullptr;
  ASSERT_EQ(Table::open(storage, fileName, table), Status::kSuccess);
  size_t const file_size = storage->size(fileName);
  std::unordered_map<std::string, std::shared_ptr<Record>> out;

  table->setFileSizeForTesting(file_size / 2);
  ASSERT_EQ(table->getAll(out), Status::kFailure);
  ASSERT_TRUE(out.empty());

  table->setFileSizeForTesting(file_size);
  ASSERT_EQ(table->getAll(out), Status::kSuccess);
  expectSameRecords(expected, out);
  delete table;
  delete storage;
}

// A file that shrinks under an open Table (a partial upload, or a peer's
// REMOVE on a backend that truncates) is a failed read, not a crash.
TEST(SSTableTest, GetAllTruncatedFileTest) {
  Storage* storage = new FileStorage("/tank/test/ss_table_test/");
  std::string fileName = "GetAllTruncatedFileTest" + std::to_string(time(0));
  buildTableForScan(storage, fileName, 100, 600);

  Table* table = nullptr;
  ASSERT_EQ(Table::open(storage, fileName, table), Status::kSuccess);
  size_t const file_size = storage->size(fileName);
  std::filesystem::resize_file("/tank/test/ss_table_test/" + fileName, file_size / 2);

  std::unordered_map<std::string, std::shared_ptr<Record>> out;
  ASSERT_NE(table->getAll(out), Status::kSuccess);
  ASSERT_TRUE(out.empty());
  delete table;
  delete storage;
}
