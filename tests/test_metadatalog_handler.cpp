#include "gtest/gtest.h"
#include "helper.h"
#include "metadata_log_handler.h"
#include "protobuf/record.pb.h"
#include "protobuf/sstable.pb.h"
#include "storage/file_storage.h"
#include "storage/shared_log_storage.h"
#include <iostream>

using namespace ozonedb;

class MetadataLogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    metadata_log_storage = new FileStorage("/tank/test/metadata_log/");
    data_storage = new FileStorage("/tank/test/metadata_log/");
    tail_cache = new TailCache();
    meta_prefix = "test_meta_" + std::to_string(time(nullptr));
    metadata_log = new MetadataLogHandler(meta_prefix, metadata_log_storage, data_storage, data_storage, tail_cache, nullptr);
  }

  void TearDown() override {
    delete metadata_log;
    delete tail_cache;
    delete metadata_log_storage;
    delete data_storage;
  }

  void PrintView(View const& view) {
    std::cout << "Current Log Tail: " << view.current_log_tail << std::endl;
    for (auto const& [key, value] : view.storage_layout) {
      std::cout << key << ": ";
      for (auto const& file : value) {
        std::cout << file << " ";
      }
      std::cout << std::endl;
    }
  }

  Storage* metadata_log_storage;
  FileStorage* data_storage;
  TailCache* tail_cache;
  MetadataLogHandler* metadata_log;
  std::string meta_prefix;
};

TEST_F(MetadataLogTest, UnorderedLogCreationFileSystem) {
  // Test creating log files in unordered sequence
  OperationRecord record;

  // Create initial log file
  record.set_op_type(OperationRecord::LOGCREATE);
  record.add_input_files("");
  record.add_output_file("datalog/1");
  metadata_log->appendToMetadataLog(record);
  record.Clear();

  // Create log file 3 before 2 (out of order)
  record.set_op_type(OperationRecord::LOGCREATE);
  record.add_input_files("datalog/2");
  record.add_output_file("datalog/3");
  metadata_log->appendToMetadataLog(record);
  record.Clear();

  // Create log file 2 (filling the gap)
  record.set_op_type(OperationRecord::LOGCREATE);
  record.add_input_files("datalog/1");
  record.add_output_file("datalog/2");
  metadata_log->appendToMetadataLog(record);
  record.Clear();
  std::cout << "finish append" << std::endl;
  // Roll forward and verify
  View view = metadata_log->rollForwardMetadataLog();
  EXPECT_EQ(view.current_log_tail, "datalog/3");
  EXPECT_EQ(view.storage_layout["datalog"].size(), 3);
  EXPECT_EQ(view.storage_layout["datalog"][0], "datalog/1");
  EXPECT_EQ(view.storage_layout["datalog"][1], "datalog/2");
  EXPECT_EQ(view.storage_layout["datalog"][2], "datalog/3");
  PrintView(view);
}

TEST_F(MetadataLogTest, ComplexOperationsFileSystem) {
  OperationRecord record;

  // Create a chain of log files
  std::vector<std::string> log_files = {"datalog/1", "datalog/2", "datalog/3", "datalog/4", "datalog/5"};

  // Create initial log file
  record.set_op_type(OperationRecord::LOGCREATE);
  record.add_input_files("");
  record.add_output_file(log_files[0]);
  metadata_log->appendToMetadataLog(record);

  // Create subsequent log files
  for (size_t i = 1; i < log_files.size(); i++) {
    record.Clear();
    record.set_op_type(OperationRecord::LOGCREATE);
    record.add_input_files(log_files[i - 1]);
    record.add_output_file(log_files[i]);
    metadata_log->appendToMetadataLog(record);
  }

  // Verify log chain
  View view = metadata_log->rollForwardMetadataLog();
  EXPECT_EQ(view.current_log_tail, log_files.back());
  EXPECT_EQ(view.storage_layout["datalog"].size(), log_files.size());
  PrintView(view);
  // Perform compaction on files 1,2 -> level1/1
  record.Clear();
  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files(log_files[0]);
  record.add_input_files(log_files[1]);
  record.add_output_file("level1/1");
  record.add_key_start("key0");
  record.add_key_end("key9");
  metadata_log->appendToMetadataLog(record);

  // Verify after first compaction
  view = metadata_log->rollForwardMetadataLog();
  EXPECT_EQ(view.storage_layout["level1"].size(), 1);
  EXPECT_EQ(view.storage_layout["level1"][0], "level1/1");
  EXPECT_EQ(view.key_range["level1/1"].first, "key0");
  EXPECT_EQ(view.key_range["level1/1"].second, "key9");
  PrintView(view);
  // Perform compaction on files 3,4 -> level1/2
  record.Clear();
  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files(log_files[2]);
  record.add_input_files(log_files[3]);
  record.add_output_file("level1/2");
  record.add_key_start("key10");
  record.add_key_end("key19");
  metadata_log->appendToMetadataLog(record);

  // Verify final state
  view = metadata_log->rollForwardMetadataLog();
  EXPECT_EQ(view.storage_layout["level1"].size(), 2);
  EXPECT_EQ(view.storage_layout["level1"][1], "level1/2");
  EXPECT_EQ(view.key_range["level1/2"].first, "key10");
  EXPECT_EQ(view.key_range["level1/2"].second, "key19");
  PrintView(view);
}

TEST_F(MetadataLogTest, UnorderedLogCreationSharedLog) {
  Storage* metadatalog_storage = new SharedLogStorage("metadatalog");
  Storage* log_storage = new SharedLogStorage("datalog");
  FileStorage* sst_storage = new FileStorage("/tank/test/metadata_log/");
  TailCache* tail_cache = new TailCache();
  MetadataLogHandler* metadatalog_handler = new MetadataLogHandler("sharedlog/0:0", metadatalog_storage, log_storage, sst_storage, tail_cache, nullptr);

  for (int i = 0; i < 450; i++) {
    Record* record = new Record();
    record->set_key("key" + std::to_string(i));
    record->set_value("value" + std::to_string(i));
    record->set_type(kTypeValue);
    size_t size;
    Status status = log_storage->append("", *record, size);
    EXPECT_EQ(status, Status::kSuccess);
  }
  View view = metadatalog_handler->rollForwardMetadataLog();
  PrintView(view);

  // sharedlog: sharedlog/0:64 sharedlog/64:128 sharedlog/128:192 sharedlog/192:256 sharedlog/256:320 sharedlog/320:384 sharedlog/384:448
  OperationRecord record;
  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files("sharedlog/256:320");
  record.add_input_files("sharedlog/320:384");
  record.add_output_file("level1/3");
  record.add_key_start("key256");
  record.add_key_end("key383");
  metadatalog_handler->appendToMetadataLog(record);

  record.Clear();
  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files("sharedlog/128:192");
  record.add_input_files("sharedlog/192:256");
  record.add_output_file("level1/2");
  record.add_key_start("key128");
  record.add_key_end("key255");
  metadatalog_handler->appendToMetadataLog(record);

  record.Clear();
  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files("sharedlog/0:64");
  record.add_input_files("sharedlog/64:128");
  record.add_output_file("level1/1");
  record.add_key_start("key0");
  record.add_key_end("key127");
  metadatalog_handler->appendToMetadataLog(record);

  view = metadatalog_handler->rollForwardMetadataLog();
  PrintView(view);
}
