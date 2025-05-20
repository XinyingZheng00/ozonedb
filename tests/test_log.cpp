#include "cache.h"
#include "data_log_handler.h"
#include "metadata_log_handler.h"
#include "ozonedb_common.h"
#include "storage/file_storage.h"
#include "storage/shared_log_storage.h"
#include "storage/storage.h"
#include "thread_pool.h"
#include <gtest/gtest.h>
#include <memory>

using namespace ozonedb;

class DataLogHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string name = "/tank/test/test_datalog" + std::to_string(time(nullptr)) + "/";
    log_storage = std::make_unique<FileStorage>(name);
    metadatalog_storage = std::make_unique<FileStorage>(name);
    tail_cache = std::make_unique<TailCache>();
    cache = std::make_unique<LRUCache>(10, 0, log_storage.get(), nullptr);
    metadata_handler = std::make_unique<MetadataLogHandler>("meta.log", metadatalog_storage.get(), log_storage.get(), nullptr, tail_cache.get(), nullptr);
    thread_pool = std::make_unique<ThreadPool>(1);

    data_log_handler = std::make_unique<DataLogHandler>(name, 1024, "datalog", cache.get(),
                                                        metadata_handler.get(), thread_pool.get(), StorageType::kFileStorage);
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

  std::unique_ptr<Storage> log_storage;
  std::unique_ptr<Storage> metadatalog_storage;
  std::unique_ptr<LRUCache> cache;
  std::unique_ptr<TailCache> tail_cache;
  std::unique_ptr<MetadataLogHandler> metadata_handler;
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<DataLogHandler> data_log_handler;
};

// Test adding a record with file storage
TEST_F(DataLogHandlerTest, AddRecordFileStorage) {
  Record record;
  record.set_key("test_key");
  record.set_value("test_value");
  record.set_type(kTypeValue);

  size_t size = 0;
  Status status = data_log_handler->addRecord(record);
  EXPECT_EQ(status, Status::kSuccess);
  std::cout << "add record success" << std::endl;

  View view;
  metadata_handler->getLatestView(view);
  PrintView(view);
}
// Test adding a record with shared log storage
TEST_F(DataLogHandlerTest, AddRecordSharedLog) {
  std::unique_ptr<Storage> log_storage = std::make_unique<SharedLogStorage>("datalog");
  std::unique_ptr<Storage> metadatalog_storage = std::make_unique<SharedLogStorage>("metadatalog");
  std::unique_ptr<TailCache> tail_cache = std::make_unique<TailCache>();
  std::unique_ptr<LRUCache> cache = std::make_unique<LRUCache>(2, 0, log_storage.get(), nullptr);
  std::unique_ptr<MetadataLogHandler> metadata_handler = std::make_unique<MetadataLogHandler>("sharedlog/0:0", metadatalog_storage.get(), log_storage.get(), nullptr, tail_cache.get(), nullptr);
  std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(3);
  std::unique_ptr<DataLogHandler> data_log_handler = std::make_unique<DataLogHandler>(cache.get(), thread_pool.get(), StorageType::kSharedLogStorage);

  Record record;
  record.set_key("test_key");
  record.set_value("test_value");
  record.set_type(kTypeValue);
  Status status = data_log_handler->addRecord(record);
  EXPECT_EQ(status, Status::kSuccess);

  View view;
  metadata_handler->rollForwardMetadataLog();
  metadata_handler->getLatestView(view);
  PrintView(view);
}

TEST_F(DataLogHandlerTest, MultiThreadAddRecordFileStorage) {
  int const num_threads = 4;

  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([i, this]() {
      for (int j = 0; j < 100; j++) {
        Record record;
        record.set_key("key_" + std::to_string(i) + "_" + std::to_string(j));
        record.set_value("value_" + std::to_string(i) + "_" + std::to_string(j));
        record.set_type(kTypeValue);
        Status status = data_log_handler->addRecord(record);
        EXPECT_EQ(status, Status::kSuccess);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  View view;
  metadata_handler->rollForwardMetadataLog();
  metadata_handler->getLatestView(view);
  PrintView(view);
}

TEST_F(DataLogHandlerTest, MultiThreadAddRecordSharedLog) {
  int const num_threads = 4;

  std::unique_ptr<Storage> log_storage = std::make_unique<SharedLogStorage>("datalog");
  std::unique_ptr<Storage> metadatalog_storage = std::make_unique<SharedLogStorage>("metadatalog");
  std::unique_ptr<TailCache> tail_cache = std::make_unique<TailCache>();
  std::unique_ptr<LRUCache> cache = std::make_unique<LRUCache>(100, 0, log_storage.get(), nullptr);
  std::unique_ptr<MetadataLogHandler> metadata_handler = std::make_unique<MetadataLogHandler>("sharedlog/0:0", metadatalog_storage.get(), log_storage.get(), nullptr, tail_cache.get(), nullptr);
  std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(3);
  std::unique_ptr<DataLogHandler> data_log_handler = std::make_unique<DataLogHandler>(
      cache.get(),
      thread_pool.get(),
      StorageType::kSharedLogStorage);

  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([i, &data_log_handler]() {
      for (int j = 0; j < 100; j++) {
        Record record;
        record.set_key("shared_key_" + std::to_string(i) + "_" + std::to_string(j));
        record.set_value("shared_value_" + std::to_string(i) + "_" + std::to_string(j));
        record.set_type(kTypeValue);
        Status status = data_log_handler->addRecord(record);
        EXPECT_EQ(status, Status::kSuccess);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  View view;
  metadata_handler->rollForwardMetadataLog();
  metadata_handler->getLatestView(view);
  PrintView(view);
}
// Test reading a record
TEST_F(DataLogHandlerTest, ReadRecordFileStorage) {
  // first add some records to make log files: 3 log files
  for (int i = 0; i < 100; i++) {
    Record record;
    record.set_key("test_key" + std::to_string(i));
    record.set_value("test_value" + std::to_string(i));
    record.set_type(kTypeValue);

    size_t size = 0;
    Status status = data_log_handler->addRecord(record);
    EXPECT_EQ(status, Status::kSuccess);
  }
  View view = metadata_handler->rollForwardMetadataLog();
  data_log_handler->setLatestView(&view);
  cache->setLatestView(&view);
  PrintView(view);

  Record* found_record = nullptr;
  std::string latest_offset;
  for (int i = 0; i < 100; i++) {
    Status status = data_log_handler->readRecord("test_key" + std::to_string(i), found_record, "", latest_offset);
    EXPECT_EQ(status, Status::kSuccess);
    EXPECT_EQ(found_record->key(), "test_key" + std::to_string(i));
    EXPECT_EQ(found_record->value(), "test_value" + std::to_string(i));
    EXPECT_EQ(latest_offset, "datalog/3");
  }
}

TEST_F(DataLogHandlerTest, TestDeduplicatedReadRecordFileStorage) {
  int const num_threads = 10;
  const std::string key_prefix = "test_key_";
  // Add some records first (simulate write)
  for (int i = 0; i < 100; ++i) {
    Record record;
    record.set_key(key_prefix + std::to_string(i));
    record.set_value("val_" + std::to_string(i));
    record.set_type(kTypeValue);
    Status status = data_log_handler->addRecord(record);
    assert(status == Status::kSuccess);
  }

  // Roll metadata log forward
  View view = metadata_handler->rollForwardMetadataLog();
  data_log_handler->setLatestView(&view);
  cache->setLatestView(&view);

  // Simulate read from multiple threads
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::mutex cout_mu;

  std::mutex start_mu;
  std::condition_variable start_cv;
  bool start_flag = false;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&, i]() {
      {
        std::unique_lock<std::mutex> lock(start_mu);
        start_cv.wait(lock, [&] { return start_flag; });  // Wait until signaled
      }

      Record* out_record = nullptr;
      std::string offset = "";
      std::string latest_offset;
      Status s = data_log_handler->readRecord(key_prefix + std::to_string(i), out_record, offset, latest_offset);
      if (s == Status::kSuccess && out_record && out_record->value() == "val_" + std::to_string(i)) {
        success_count++;
      } else {
        std::lock_guard<std::mutex> lock(cout_mu);
        std::cerr << "[Thread " << i << "] failed to read expected record!\n";
      }
    });
  }

  // Sleep a short moment to ensure all threads are waiting
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Signal all threads to start
  {
    std::lock_guard<std::mutex> lock(start_mu);
    start_flag = true;
  }
  start_cv.notify_all();

  for (auto& t : threads) t.join();

  thread_pool->waitForCompletion();

  std::cout << "Read success: " << success_count.load() << " / " << num_threads << std::endl;
  assert(success_count == num_threads);
}

TEST_F(DataLogHandlerTest, TestDifferentViewFileStorage) {
  int const num_threads = 10;
  const std::string key_prefix = "test_key_";
  // Add some records first (simulate write)
  for (int i = 0; i < 100; ++i) {
    Record record;
    record.set_key(key_prefix + std::to_string(i));
    record.set_value("val_" + std::to_string(i));
    record.set_type(kTypeValue);
    Status status = data_log_handler->addRecord(record);
    assert(status == Status::kSuccess);
  }

  View old_view = metadata_handler->rollForwardMetadataLog();
  for (int i = 0; i < 10; ++i) {
    Record record;
    record.set_key(key_prefix + std::to_string(i));
    record.set_value("val_" + std::to_string(i));
    record.set_type(kTypeValue);
    Status status = data_log_handler->addRecord(record);
    assert(status == Status::kSuccess);
  }

  View new_view = metadata_handler->rollForwardMetadataLog();
  // Simulate read from multiple threads
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::mutex cout_mu;

  std::mutex start_mu;
  std::condition_variable start_cv;
  bool start_flag = false;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&, i]() {
      {
        std::unique_lock<std::mutex> lock(start_mu);
        start_cv.wait(lock, [&] { return start_flag; });  // Wait until signaled
      }
      if (i % 2 == 0) {
        data_log_handler->setLatestView(&old_view);
        cache->setLatestView(&old_view);
      } else {
        data_log_handler->setLatestView(&new_view);
        cache->setLatestView(&new_view);
      }
      Record* out_record = nullptr;
      std::string offset = "";
      std::string latest_offset;
      Status s = data_log_handler->readRecord(key_prefix + std::to_string(i), out_record, offset, latest_offset);
      if (s == Status::kSuccess && out_record && out_record->value() == "val_" + std::to_string(i)) {
        success_count++;
      } else {
        std::lock_guard<std::mutex> lock(cout_mu);
        std::cerr << "[Thread " << i << "] failed to read expected record!\n";
      }
    });
  }

  // Sleep a short moment to ensure all threads are waiting
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Signal all threads to start
  {
    std::lock_guard<std::mutex> lock(start_mu);
    start_flag = true;
  }
  start_cv.notify_all();

  for (auto& t : threads) t.join();

  thread_pool->waitForCompletion();

  std::cout << "Read success: " << success_count.load() << " / " << num_threads << std::endl;
  assert(success_count == num_threads);
}



// // Test reading a record with cache miss
// TEST_F(DataLogHandlerTest, ReadRecordCacheMiss) {
//     DataLogHandler handler(-1, "test_prefix", storage.get(), cache.get(),
//                          metadata_handler.get(), thread_pool.get());
//     handler.setLatestView(&latest_view);

//     Record* found_record = nullptr;
//     std::string latest_offset;

//     // Setup mock behavior for cache miss
//     bool read_more = true;
//     size_t cached_offset = 0;
//     size_t size = 100;

//     EXPECT_CALL(*cache, checkReadMoreLog(testing::_, testing::_, testing::_, testing::_))
//         .WillOnce(DoAll(
//             SetArgReferee<1>(read_more),
//             SetArgReferee<2>(cached_offset),
//             SetArgReferee<3>(size)
//         ));

//     // Expect readDataLog to be called due to cache miss
//     EXPECT_CALL(*cache, readDataLog("log_1", cached_offset, size))
//         .Times(1);

//     Record* cached_record = nullptr;
//     EXPECT_CALL(*cache, get("log_1", "test_key", testing::_, ""))
//         .WillOnce(SetArgReferee<2>(cached_record));

//     EXPECT_EQ(handler.readRecord("test_key", found_record, "", latest_offset), Status::kFailure);
//     EXPECT_EQ(found_record, nullptr);
// }

// // Test reading a record with empty file list
// TEST_F(DataLogHandlerTest, ReadRecordEmptyFiles) {
//     DataLogHandler handler(-1, "test_prefix", storage.get(), cache.get(),
//                          metadata_handler.get(), thread_pool.get());

//     // Setup empty view
//     View empty_view;
//     handler.setLatestView(&empty_view);

//     Record* found_record = nullptr;
//     std::string latest_offset;

//     EXPECT_EQ(handler.readRecord("test_key", found_record, "", latest_offset), Status::kFailure);
//     EXPECT_EQ(found_record, nullptr);
// }
