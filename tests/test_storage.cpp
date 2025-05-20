#include "gtest/gtest.h"
#include "helper.h"
#include "protobuf/record.pb.h"
#include "protobuf/sstable.pb.h"
#include "storage/file_storage.h"
#include "storage/shared_log_storage.h"
#include "test_tool.h"
#include <algorithm>  // for std::sort
#include <thread>

using namespace ozonedb;

class FileStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage = new FileStorage("/tank/test/storage/");
    // Initialize test records
    records.resize(10);
    for (int i = 0; i < 10; i++) {
      records[i].set_key("key" + std::to_string(i));
      records[i].set_value("value" + std::to_string(i));
      records[i].set_type(i % 2 == 0 ? kTypeValue : kTypeDeletion);
    }
  }

  void TearDown() override {
    delete storage;
  }

  std::string getTestFileName(std::string prefix) {
    return prefix + "_" + std::to_string(time(0));
  }

  FileStorage* storage;
  std::vector<Record> records;
};

class SharedLogStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize test records
    records.resize(10);
    for (int i = 0; i < 10; i++) {
      records[i].set_key("key" + std::to_string(i));
      records[i].set_value("value" + std::to_string(i));
      records[i].set_type(i % 2 == 0 ? kTypeValue : kTypeDeletion);
    }
  }

  void TearDown() override {
  }

  std::vector<Record> records;
};

// FileStorage Tests
TEST_F(FileStorageTest, SingleRecordWriteRead) {
  std::string fileName = getTestFileName("SingleRecordWriteRead");
  size_t written_size;

  // Write single record
  int size_before = storage->size(fileName);
  Status status = storage->append(fileName, records[0], written_size);
  ASSERT_EQ(status, Status::kSuccess);
  int size_after = storage->size(fileName);
  EXPECT_EQ(size_after, size_before + written_size);

  // Read back
  std::vector<google::protobuf::Message*> messages;
  status = storage->read(
      fileName, []() { return new Record(); }, messages);
  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), 1);

  Record* read_record = dynamic_cast<Record*>(messages[0]);
  ASSERT_NE(read_record, nullptr);
  EXPECT_EQ(read_record->key(), records[0].key());
  EXPECT_EQ(read_record->value(), records[0].value());
  EXPECT_EQ(read_record->type(), records[0].type());

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

TEST_F(FileStorageTest, MultipleRecordsWriteRead) {
  std::string fileName = getTestFileName("MultipleRecordsWriteRead");
  size_t written_size;

  // Write multiple records
  for (auto const& record : records) {
    Status status = storage->append(fileName, record, written_size);
    ASSERT_EQ(status, Status::kSuccess);
  }

  // Read all records
  std::vector<google::protobuf::Message*> messages;
  Status status = storage->read(
      fileName, []() { return new Record(); }, messages);
  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), records.size());

  // Verify records
  for (size_t i = 0; i < messages.size(); i++) {
    Record* read_record = dynamic_cast<Record*>(messages[i]);
    ASSERT_NE(read_record, nullptr);
    EXPECT_EQ(read_record->key(), records[i].key());
    EXPECT_EQ(read_record->value(), records[i].value());
    EXPECT_EQ(read_record->type(), records[i].type());
  }

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

TEST_F(FileStorageTest, PartialRead) {
  std::string fileName = getTestFileName("PartialRead");
  size_t written_size;

  // Write multiple records
  for (auto const& record : records) {
    Status status = storage->append(fileName, record, written_size);
    ASSERT_EQ(status, Status::kSuccess);
  }

  // Read partial records (from middle)
  std::vector<google::protobuf::Message*> messages;
  size_t start_offset = written_size * 3;  // Start from 4th record
  size_t length = written_size * 3;        // Read 3 records
  Status status = storage->read(
      fileName, start_offset, start_offset + length,
      []() { return new Record(); }, messages);

  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), 3);

  // Verify records
  for (size_t i = 0; i < messages.size(); i++) {
    Record* read_record = dynamic_cast<Record*>(messages[i]);
    ASSERT_NE(read_record, nullptr);
    EXPECT_EQ(read_record->key(), records[i + 3].key());
    EXPECT_EQ(read_record->value(), records[i + 3].value());
    EXPECT_EQ(read_record->type(), records[i + 3].type());
  }

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

TEST_F(FileStorageTest, SizeAndSeal) {
  std::string fileName = getTestFileName("SizeAndSeal");
  size_t written_size;

  // Write one record and check size
  Status status = storage->append(fileName, records[0], written_size);
  ASSERT_EQ(status, Status::kSuccess);

  size_t file_size = storage->size(fileName);
  EXPECT_EQ(file_size, written_size);

  // Seal the file
  storage->seal(fileName);

  status = storage->append(fileName, records[1], written_size);
  ASSERT_EQ(status, Status::kSealed);
}

void write_func(std::string fname, int idx) {
  FileStorage local_storage("/tank/test/storage/" + fname + "/");
  size_t written_size;
  Record record;
  record.set_key("key_" + std::to_string(idx));
  record.set_value("value_" + std::to_string(idx));
  record.set_type(idx % 2 == 0 ? kTypeValue : kTypeDeletion);
  Status status = local_storage.append("1", record, written_size);
  EXPECT_EQ(status, Status::kSuccess);
}

TEST_F(FileStorageTest, ConcurrentWrites) {
  std::string fileName = getTestFileName("ConcurrentWrites");
  int num_processes = 100;
  multipleProcessorTester(num_processes, write_func, fileName);

  std::vector<google::protobuf::Message*> messages;
  Storage* local_storage = new FileStorage("/tank/test/storage/" + fileName + "/");
  Status status = local_storage->read(
      "1", []() { return new Record(); }, messages);
  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), num_processes);

  for (size_t i = 0; i < messages.size(); i++) {
    Record* read_record = dynamic_cast<Record*>(messages[i]);
    ASSERT_NE(read_record, nullptr);
    std::string key = read_record->key();
    int idx = std::stoi(key.substr(key.find_last_of("_") + 1));
    EXPECT_EQ(read_record->key(), "key_" + std::to_string(idx));
    EXPECT_EQ(read_record->value(), "value_" + std::to_string(idx));
    EXPECT_EQ(read_record->type(), idx % 2 == 0 ? kTypeValue : kTypeDeletion);
  }
  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

// concurrent read
void read_func(std::string fname, int idx) {
  FileStorage local_storage("/tank/test/storage/");
  std::vector<google::protobuf::Message*> messages;
  Status status = local_storage.read(
      fname, []() { return new Record(); }, messages);
  ASSERT_EQ(status, Status::kSuccess);
  for (size_t i = 0; i < messages.size(); i++) {
    Record* read_record = dynamic_cast<Record*>(messages[i]);
    ASSERT_NE(read_record, nullptr);
    EXPECT_EQ(read_record->key(), "key_" + std::to_string(i));
    EXPECT_EQ(read_record->value(), "value_" + std::to_string(i));
    EXPECT_EQ(read_record->type(), i % 2 == 0 ? kTypeValue : kTypeDeletion);
  }
}

TEST_F(FileStorageTest, ConcurrentReads) {
  std::string fileName = getTestFileName("ConcurrentReads");
  // append 100 records
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key("key_" + std::to_string(i));
    record.set_value("value_" + std::to_string(i));
    record.set_type(i % 2 == 0 ? kTypeValue : kTypeDeletion);
    size_t written_size;
    Status status = storage->append(fileName, record, written_size);
    ASSERT_EQ(status, Status::kSuccess);
  }
  int num_processes = 100;
  multipleProcessorTester(num_processes, read_func, fileName);
}

// SharedLogStorage Tests

TEST_F(SharedLogStorageTest, BasicWriteRead) {
  size_t written_size;

  // Write single record
  Storage* storage = new SharedLogStorage("datalog");
  size_t initial_size = storage->size("");
  Status status = storage->append("", records[0], written_size);
  size_t final_size = storage->size("");
  EXPECT_EQ(final_size, initial_size + 1);
  ASSERT_EQ(status, Status::kSuccess);

  // Read back
  std::vector<google::protobuf::Message*> messages;
  status = storage->read(
      "sharedlog/0:100", initial_size, final_size,
      []() { return new Record(); }, messages);
  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_GE(messages.size(), 1);

  // Verify last record (since shared log might have other records)
  Record* read_record = dynamic_cast<Record*>(messages.back());
  ASSERT_NE(read_record, nullptr);
  EXPECT_EQ(read_record->key(), records[0].key());
  EXPECT_EQ(read_record->value(), records[0].value());
  EXPECT_EQ(read_record->type(), records[0].type());

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

TEST_F(SharedLogStorageTest, MultipleRecordsWriteRead) {
  size_t written_size;
  Storage* storage = new SharedLogStorage("datalog");
  size_t initial_size = storage->size("");

  // Write multiple records
  for (auto const& record : records) {
    Status status = storage->append("", record, written_size);
    ASSERT_EQ(status, Status::kSuccess);
  }
  size_t final_size = storage->size("");
  EXPECT_EQ(final_size, initial_size + records.size());

  // Read records
  std::vector<google::protobuf::Message*> messages;
  Status status = storage->read(
      "sharedlog/0:100", initial_size, final_size,
      []() { return new Record(); }, messages);

  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), records.size());

  // Verify records
  for (size_t i = 0; i < messages.size(); i++) {
    Record* read_record = dynamic_cast<Record*>(messages[i]);
    ASSERT_NE(read_record, nullptr);
    EXPECT_EQ(read_record->key(), records[i].key());
    EXPECT_EQ(read_record->value(), records[i].value());
    EXPECT_EQ(read_record->type(), records[i].type());
  }

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

TEST_F(SharedLogStorageTest, PartialRead) {
  size_t written_size;
  Storage* storage = new SharedLogStorage("datalog");
  size_t initial_size = storage->size("");

  // Write multiple records
  for (auto const& record : records) {
    Status status = storage->append("", record, written_size);
    ASSERT_EQ(status, Status::kSuccess);
  }

  // Read partial records
  std::vector<google::protobuf::Message*> messages;
  size_t start_offset = initial_size + 3;  // Start from 4th record
  size_t length = 3;                       // Read 3 records

  Status status = storage->read(
      "sharedlog/0:100", start_offset, start_offset + length,
      []() { return new Record(); }, messages);

  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), 3);

  // Verify records
  for (size_t i = 0; i < messages.size(); i++) {
    Record* read_record = dynamic_cast<Record*>(messages[i]);
    ASSERT_NE(read_record, nullptr);
    EXPECT_EQ(read_record->key(), records[i + 3].key());
    EXPECT_EQ(read_record->value(), records[i + 3].value());
    EXPECT_EQ(read_record->type(), records[i + 3].type());
  }

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}