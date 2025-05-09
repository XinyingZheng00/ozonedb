#include "gtest/gtest.h"
#include "metadata_log_handler.h"
#include "shared_log_storage.h"
#include <hdr/hdr_histogram.h>
#include "log_handler.h"
#include "db.h"
#include "task_log_handler.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
// need to be run in the root mode

using namespace ozonedb;
#ifdef SHARED_LOG
TEST(SharedLogStorageTest, AppendAndSizeIncreases) {
  SharedLogStorage* storage = new SharedLogStorage();
  std::string test_data = "test_entry";
  size_t size_before = storage->size();

  EXPECT_EQ(storage->append(test_data), Status::kSuccess);

  size_t size_after = storage->size();
  std::cout << "Size before: " << size_before << ", Size after: " << size_after << std::endl;
  EXPECT_GT(size_after, size_before);  // size should increase after append
}

TEST(SharedLogStorageTest, ReadAppendedEntry) {
  SharedLogStorage* storage = new SharedLogStorage();
  std::string test_data = "test_read_entry";
  int num_entries = 10;
  for (int i = 0; i < num_entries; ++i) {
    storage->append(test_data + std::to_string(i));
  }
  size_t end = storage->size();
  std::vector<std::string> entries;
  Status status = storage->read(entries, end - num_entries, end);

  EXPECT_EQ(status, Status::kSuccess);
  ASSERT_EQ(entries.size(), num_entries);
  for (int i = 0; i < num_entries; ++i) {
    EXPECT_EQ(entries[i], test_data + std::to_string(i));
  }
}

TEST(SharedLogStorageTest, ExistAlwaysTrue) {
  SharedLogStorage* storage = new SharedLogStorage();
  EXPECT_TRUE(storage->exist());
}

using namespace std::chrono;
std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_reads;
std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_writes;

void reader_thread(int thd_id, hdr_histogram* histogram, int runtime_secs) {
  uint64_t idx = 0;
  SharedLogStorage* storage = new SharedLogStorage(Type::kDataLog, 2 * thd_id);
  auto begin = high_resolution_clock::now();

  while (duration_cast<seconds>(high_resolution_clock::now() - begin).count() < runtime_secs) {
    std::vector<std::string> entries;
    auto start = high_resolution_clock::now();
    storage->read(entries, idx, idx + 1);
    hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
    idx++;
  }

  num_requests_and_durations_reads[thd_id] = {
      idx, duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
}

void writer_thread(int thd_id, hdr_histogram* histogram, int runtime_secs) {
  uint64_t idx = 0;
  std::string data(1024, 'W');
  SharedLogStorage* storage = new SharedLogStorage(Type::kDataLog, 2 * thd_id + 1);
  auto begin = high_resolution_clock::now();

  while (duration_cast<seconds>(high_resolution_clock::now() - begin).count() < runtime_secs) {
    auto start = high_resolution_clock::now();
    storage->append(data);
    hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
    idx++;
  }

  num_requests_and_durations_writes[thd_id] = {
      idx, duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
}

double compute_throughput(std::unordered_map<int, std::pair<uint64_t, uint64_t>> const& stats) {
  double tput = 0;
  for (auto const& [_, p] : stats) {
    tput += static_cast<double>(p.first) * 1.0e9 / p.second;
  }
  return tput;
}

TEST(SharedLogStorageTest, MixedReadWriteThroughput) {
  int runtime_secs = 5;
  int thread_count = 3;

  hdr_histogram* read_hist;
  hdr_init(1, INT64_C(3600000000), 3, &read_hist);
  hdr_histogram* write_hist;
  hdr_init(1, INT64_C(3600000000), 3, &write_hist);

  std::vector<std::thread> readers, writers;

  for (int i = 0; i < thread_count; ++i) {
    writers.emplace_back(writer_thread, i, write_hist, runtime_secs);
    readers.emplace_back(reader_thread, i, read_hist, runtime_secs);
  }
  for (auto& t : writers) t.join();
  for (auto& t : readers) t.join();

  double write_tput = compute_throughput(num_requests_and_durations_writes);
  double read_tput = compute_throughput(num_requests_and_durations_reads);

  std::cout << "[MixedTest] write throughput: " << write_tput << " ops/sec\n";
  std::cout << "[MixedTest] read throughput: " << read_tput << " ops/sec\n";

  EXPECT_GT(write_tput, 0.0);
  EXPECT_GT(read_tput, 0.0);

  std::cout << "[MixedTest] read latencies:\n"
            << "\tp50: " << hdr_value_at_percentile(read_hist, 50.0) << " ns\n"
            << "\tp95: " << hdr_value_at_percentile(read_hist, 95.0) << " ns\n"
            << "\tp99: " << hdr_value_at_percentile(read_hist, 99.0) << " ns\n";

  std::cout << "[MixedTest] write latencies:\n"
            << "\tp50: " << hdr_value_at_percentile(write_hist, 50.0) << " ns\n"
            << "\tp95: " << hdr_value_at_percentile(write_hist, 95.0) << " ns\n"
            << "\tp99: " << hdr_value_at_percentile(write_hist, 99.0) << " ns\n";

  hdr_close(read_hist);
  hdr_close(write_hist);
}

// TEST(SharedLogLogHanderTest, AddAndReadRecordSuccess) {
//   LogHandlerBase* handler = new log_handler_shared_log(0);
//   Record r1;
//   r1.set_key("k1");
//   r1.set_value("v1");
//   r1.set_type(kTypeValue);
//   handler->addRecord(r1);

//   Record* out = nullptr;
//   std::string latest_offset;
//   ASSERT_EQ(handler->readRecord("k1", out, "0", latest_offset), Status::kSuccess);
//   ASSERT_NE(out, nullptr);
//   EXPECT_EQ(out->key(), "k1");
//   EXPECT_EQ(out->value(), "v1");
//   std::cout << "latest offset: " << latest_offset << std::endl;
//   delete out;
//   delete handler;
// }

// TEST(SharedLogLogHanderTest, RecordNotFound) {
//   LogHandlerBase* handler = new log_handler_shared_log(0);
//   Record* out = nullptr;
//   std::string latest_offset;
//   ASSERT_EQ(handler->readRecord("missing_key", out, "0", latest_offset), Status::kNotFound);
//   ASSERT_EQ(out, nullptr);
// }

// TEST(SharedLogLogHanderTest, OffsetEqualToAndBeyondSize) {
//   LogHandlerBase* handler = new log_handler_shared_log(0);
//   Record* out = nullptr;
//   std::string latest_offset;
//   handler->readRecord("k1", out, "1", latest_offset);

//   ASSERT_EQ(handler->readRecord("k1", out, latest_offset, latest_offset), Status::kNotFound);

//   std::string offset = std::to_string(std::stoul(latest_offset) + 1);
//   ASSERT_EQ(handler->readRecord("k1", out, offset, latest_offset), Status::kFailure);
// }

TEST(SharedLogMetadataLogTest, UnorderedRecord) {
  // better run with a clean shared log
  Storage* storage = new FileStorage("/tank/test/log/");
  std::string meta_prefix = "sharedlog:0:-1";
  MetadataLogHandler* metadata_log = new MetadataLogHandler(meta_prefix, storage, new TailCache());
  SharedLogStorage* metadata_sharedlog_storage = new SharedLogStorage(Type::kMetadataLog, 0);
  SharedLogStorage* data_sharedlog_storage = new SharedLogStorage(Type::kDataLog, 1);
  metadata_log->setMetadataSharedLogStorage(metadata_sharedlog_storage);
  metadata_log->setDataSharedLogStorage(data_sharedlog_storage);
  metadata_log->setPredefinedSharedLogSegmentSize(64);
  for (int i = 0; i < 500; i++) {
    data_sharedlog_storage->append("dataentry" + std::to_string(i));
  }
  std::cout << "finished appending data entries" << std::endl;
  View view = metadata_log->rollForwardMetadataLog();
  std::cout << view.getCurrentLogTail() << std::endl;
  for (auto const& [key, value] : view.storage_layout) {
    std::cout << key << ": ";
    for (auto const& file : value) {
      std::cout << file << " ";
    }
    std::cout << std::endl;
  }
  OperationRecord record;
  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files("sharedlog:128:192");
  record.add_input_files("sharedlog:192:256");
  record.add_output_file("sstable/2");
  record.add_key_start("2_key0");
  record.add_key_end("2_key9");
  metadata_log->appendToMetadataLog(record);
  std::cout << "append record 2" << std::endl;
  record.Clear();

  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files("sharedlog:0:64");
  record.add_input_files("sharedlog:64:128");
  record.add_output_file("sstable/1");
  record.add_key_start("1_key0");
  record.add_key_end("1_key9");
  metadata_log->appendToMetadataLog(record);
  std::cout << "append record 1" << std::endl;
  record.Clear();
  record.set_op_type(OperationRecord::COMPACT);
  record.add_input_files("sharedlog:256:320");
  record.add_input_files("sharedlog:320:384");
  record.add_output_file("sstable/3");
  record.add_key_start("3_key0");
  record.add_key_end("3_key9");
  metadata_log->appendToMetadataLog(record);
  std::cout << "append record 3" << std::endl;
  record.Clear();

  view = metadata_log->rollForwardMetadataLog();
  std::cout << view.getCurrentLogTail() << std::endl;
  for (auto const& [key, value] : view.storage_layout) {
    std::cout << key << ": ";
    for (auto const& file : value) {
      std::cout << file << " ";
    }
    std::cout << std::endl;
  }
}

TEST(ShareLogLRUCacheTest, ComplexCase) {
  MetadataLogHandler* metadata_log = new MetadataLogHandler("cache2meta", nullptr, new TailCache());
  LogHandler* log_handler = new LogHandler(1024, "logcache2", nullptr, nullptr, metadata_log);
  SharedLogStorage* data_sharedlog_storage = new SharedLogStorage(Type::kDataLog, 0);
  SharedLogStorage* metadata_sharedlog_storage = new SharedLogStorage(Type::kMetadataLog, 1);
  metadata_log->setMetadataSharedLogStorage(metadata_sharedlog_storage);
  metadata_log->setDataSharedLogStorage(data_sharedlog_storage);
  metadata_log->setPredefinedSharedLogSegmentSize(64);
  log_handler->setSharedLogStorage(data_sharedlog_storage);
  LRUCache* cache = new LRUCache(33554432, nullptr);
  cache->setFileMutexManager(new FileMutexManager());
  cache->setPredefinedSharedLogSegmentSize(64);
  cache->setSharedLogStorage(data_sharedlog_storage);
  for (size_t i = 0; i < 128; i++) {
    Record* record = new Record();
    record->set_key("key" + std::to_string(i));
    record->set_value("value" + std::to_string(i));
    record->set_type(kTypeValue);
    log_handler->addRecord(*record);
  }
  std::cout << "finished adding records" << std::endl;

  View view = metadata_log->rollForwardMetadataLog();
  cache->setLatestView(&view);
  for (auto const& [key, value] : view.storage_layout) {
    std::cout << key << ": ";
    for (auto const& file : value) {
      std::cout << file << "-" << view.file_size[file] << " ";
    }
    std::cout << std::endl;
  }
  std::deque<std::string> log_segments = view.getWithPrefix("sharedlog");

  for (auto const& log_segment : log_segments) {
    bool read_more = true;
    size_t cached_offset = 0;
    size_t size = 0;
    cache->checkReadMoreLog(log_segment, read_more, cached_offset, size);
    std::cout << "log_segment: " << log_segment 
          << ", cached_offset: " << cached_offset 
          << ", size: " << size 
          << ", readmore: " << std::boolalpha << read_more 
          << std::endl;
    
    if (read_more) {
      cache->readDataLog(log_segment, cached_offset, size);
    }
  }

  std::string fileName = "sharedlog:0:64";
  Record* dummyRecord2;
  cache->get(fileName, "key0", dummyRecord2);
  EXPECT_EQ("key0", dummyRecord2->key());
  EXPECT_EQ("value0", dummyRecord2->value());
  EXPECT_EQ(kTypeValue, dummyRecord2->type());
  fileName = "sharedlog:64:128";
  Record* dummyRecord3;
  cache->get(fileName, "key127", dummyRecord3);
  EXPECT_EQ("key127", dummyRecord3->key());
  EXPECT_EQ("value127", dummyRecord3->value());
  EXPECT_EQ(kTypeValue, dummyRecord3->type());
  delete log_handler;
  delete cache;
}

TEST(ShareLogTaskTest, ComplexCase){
  TaskLogHandler* task_log_handler = new TaskLogHandler("tasklog", nullptr);
  SharedLogStorage* task_sharedlog_storage = new SharedLogStorage(Type::kTaskLog, 0);
  task_log_handler->setTaskSharedLogStorage(task_sharedlog_storage);

  for (int i = 0; i < 100; i++) {
    TaskRecord* record = new TaskRecord();
    TaskRecord::TaskIdentifier* task_id = new TaskRecord::TaskIdentifier();
    task_id->add_input_files("input" + std::to_string(i));
    task_id->set_compactintonextlevel(true);
    task_id->set_destinationlevel(1);
    record->set_allocated_task_id(task_id);
    record->set_owner("owner" + std::to_string(i));
    record->set_status(TaskRecord::TASK_BEGIN);
    record->set_owner_generation(i);
    task_log_handler->appendToTaskLog(*record);
  }
  std::cout << "finished adding records" << std::endl;
  std::vector<TaskRecord*> records;
  task_log_handler->readTaskLog(records);
  for (int i = 0; i < records.size(); i++) {
    assert(records[i]->task_id().input_files_size() == 1);
    assert(records[i]->task_id().input_files(0) == "input" + std::to_string(i));
    assert(records[i]->task_id().compactintonextlevel() == true);
    assert(records[i]->task_id().destinationlevel() == 1);
    assert(records[i]->owner() == "owner" + std::to_string(i));
    assert(records[i]->status() == TaskRecord::TASK_BEGIN);
    assert(records[i]->owner_generation() == i);
  }
  delete task_log_handler;
}

TEST(SharedLogCompactionTest, Compaction) {
  system("rm -rf /tank/test/compaction/");
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t i = 0; i < 250; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  std::cout << "finished adding records" << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  DB::closeDB(db);
}

#endif