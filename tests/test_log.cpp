#include "db.h"
#include "gtest/gtest.h"
#include "log_handler.h"
#include "test_tool.h"
#include "thread_pool.h"
#include <thread>
using namespace ozonedb;
// TEST(LogTest, log_write_read_single_record) {
//   Storage* storage = new FileStorage("/tank/test/log/");
//   std::string prefix = "write_read_single_record_log" + std::to_string(time(0));
//   std::string meta_prefix = "write_read_single_record_log_meta" + std::to_string(time(0));
//   LRUCache* lru_cache = new LRUCache(33554432, storage);
//   lru_cache->setFileMutexManager(new FileMutexManager());
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(meta_prefix, storage, new TailCache());
//   LogHandler* log_handler = new LogHandler(1024, prefix, storage, lru_cache, metadata_log);
//   ThreadPool* thread_pool = new ThreadPool(10);
//   log_handler->setThreadPool(thread_pool);
//   Record record;
//   record.set_key("key");
//   record.set_value("value");
//   record.set_type(kTypeValue);
//   Status status = log_handler->addRecord(record);
//   EXPECT_EQ(Status::kSuccess, status);

//   std::string key = "key";
//   Record* read_record;
//   std::string offset;
//   std::string latest_offset;
//   View view = metadata_log->rollForwardMetadataLog();
//   log_handler->setLatestView(&view);
//   lru_cache->setLatestView(&view);
//   status = log_handler->readRecord(key, read_record, offset, latest_offset);

//   EXPECT_EQ(Status::kSuccess, status);
//   EXPECT_EQ("key", read_record->key());
//   EXPECT_EQ("value", read_record->value());
//   delete log_handler;
//   delete storage;
// }

// TEST(LogTest, log_write_read_mutiple_record) {
//   Storage* storage = new FileStorage("/tank/test/log/");
//   std::string prefix = "write_read_mutiple_record_log" + std::to_string(time(0));
//   std::string meta_prefix = "write_read_mutiple_record_log_meta" + std::to_string(time(0));
//   LRUCache* lru_cache = new LRUCache(33554432, storage);
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(meta_prefix, storage, new TailCache());
//   LogHandler* log_handler = new LogHandler(1024, prefix, storage, lru_cache, metadata_log);
//   lru_cache->setFileMutexManager(new FileMutexManager());
//   ThreadPool* thread_pool = new ThreadPool(10);
//   log_handler->setThreadPool(thread_pool);
//   for (size_t i = 0; i < 10; i++) {
//     Record record;
//     record.set_key("key" + std::to_string(i));
//     if (i % 2 == 0) {
//       record.set_type(kTypeValue);
//       record.set_value("value" + std::to_string(i));
//     } else {
//       record.set_type(kTypeDeletion);
//     }
//     log_handler->addRecord(record);
//   }

//   for (size_t i = 0; i < 10; i++) {
//     std::string key = "key" + std::to_string(i);
//     Record* read_record;
//     std::string offset;
//     std::string latest_offset;
//     View view = metadata_log->rollForwardMetadataLog();
//     log_handler->setLatestView(&view);
//     lru_cache->setLatestView(&view);
//     Status status = log_handler->readRecord(key, read_record, offset, latest_offset);
//     EXPECT_EQ(Status::kSuccess, status);
//     if (i % 2 == 0) {
//       EXPECT_EQ(kTypeValue, read_record->type());
//       EXPECT_EQ("value" + std::to_string(i), read_record->value());
//     } else {
//       EXPECT_EQ(kTypeDeletion, read_record->type());
//     }
//   }
//   delete log_handler;
//   delete storage;
// }

// // test write enough records to exceed the file size limit, and check if the log handler can create a new log file
// TEST(LogTest, new_log_file) {
//   Storage* storage = new FileStorage("/tank/test/log/");
//   std::string prefix = "new_log_file" + std::to_string(time(0));
//   std::string meta_prefix = "new_log_file_meta" + std::to_string(time(0));
//   LRUCache* lru_cache = new LRUCache(33554432, storage);
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(meta_prefix, storage, new TailCache());
//   LogHandler* log_handler = new LogHandler(1024, prefix, storage, lru_cache, metadata_log);
//   lru_cache->setFileMutexManager(new FileMutexManager());
//   ThreadPool* thread_pool = new ThreadPool(10);
//   log_handler->setThreadPool(thread_pool);
//   for (size_t i = 0; i < 100; i++) {
//     Record record;
//     record.set_key("key" + std::to_string(i));
//     if (i % 2 == 0) {
//       record.set_type(kTypeValue);
//       record.set_value("value" + std::to_string(i));
//     } else {
//       record.set_type(kTypeDeletion);
//     }
//     log_handler->addRecord(record);
//   }

//   for (size_t i = 0; i < 100; i++) {
//     std::string key = "key" + std::to_string(i);
//     Record* read_record;
//     std::string offset;
//     std::string latest_offset;
//     View view = metadata_log->rollForwardMetadataLog();
//     log_handler->setLatestView(&view);
//     lru_cache->setLatestView(&view);
//     Status status = log_handler->readRecord(key, read_record, offset, latest_offset);
//     EXPECT_EQ(Status::kSuccess, status);
//     if (i % 2 == 0) {
//       EXPECT_EQ(kTypeValue, read_record->type());
//       EXPECT_EQ("value" + std::to_string(i), read_record->value());
//     } else {
//       EXPECT_EQ(kTypeDeletion, read_record->type());
//     }
//   }
//   delete log_handler;
//   delete storage;
// }

// //======================================multi-client test======================================
// /*
//  Some analysis about race condition.
//  two client are trying to write to the log layer at the same time:
//  1. if current log file is not full, then both of them can write successfully.
// => only need to check that the storage support atomic append operation.

//  2. if current log file is full or at the very beginning, then both of them will create a new log file.
//  V.  append the record to the new log file;
//  I. check file status
//  II. seal the file;
//  III.  get latestsequence number;
//  IV. create new log file;

// All possible scenarios:
// a. If two clients get the latestsequence number before any of them create the new log file, then they will both get the same latest sequence number, but only
// one client will finally create the file.
// => need storage support atomic create operation.
// b. If one client get the latestsequence number after another one create the new log file, then it will get the latest sequence number of the new log file - 1.
// c. If the file is sealed by another client, then the client will also try to new a log file.

// There is 1 special case:
// 1 client is trying to append to the last position of the log, but the network is bad, so another client occupy this position and seal current file, we also need to
// handle this case.

// In extreme case, the file size may exceed the limit by a little bit, but it is acceptable.
// */

// void testFunctionMultiClientAppend(std::string prefix, int i) {
//   Storage* storage = new FileStorage("/tank/test/log/");
//   LRUCache* lru_cache = new LRUCache(33554432, storage);
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(prefix + "meta", storage, new TailCache());
//   LogHandler* log_handler = new LogHandler(1024, prefix, storage, lru_cache, metadata_log);
//   lru_cache->setFileMutexManager(new FileMutexManager());
//   ThreadPool* thread_pool = new ThreadPool(10);
//   log_handler->setThreadPool(thread_pool);
//   if (!storage || !log_handler) {
//     std::cerr << "Error: Memory allocation failed in child process " << i << std::endl;
//     delete storage;
//     delete log_handler;
//     _exit(1);
//   }

//   Record record;
//   record.set_key("key" + std::to_string(i));
//   record.set_value("value" + std::to_string(i));
//   record.set_type(kTypeValue);
//   Status status = log_handler->addRecord(record);

//   if (status != Status::kSuccess) {
//     std::cerr << "Error: Failed to add record in child process " << i << std::endl;
//     delete log_handler;
//     delete storage;
//     _exit(1);  // Exit with an error code
//   }

//   // Clean up
//   delete log_handler;
//   delete storage;
//   _exit(0);  // Exit successfully
// }

// TEST(LogTest, multiClient_append_to_log) {
//   int numProcesses = 10;
//   std::string prefix = "multiClient_append_to_log" + std::to_string(time(0));
//   multipleProcessorTester(numProcesses, testFunctionMultiClientAppend, prefix);

//   Storage* storage = new FileStorage("/tank/test/log/");
//   LRUCache* lru_cache = new LRUCache(33554432, storage);
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(prefix + "meta", storage, new TailCache());
//   LogHandler* log_handler = new LogHandler(1024, prefix, storage, lru_cache, metadata_log);
//   lru_cache->setFileMutexManager(new FileMutexManager());
//   ThreadPool* thread_pool = new ThreadPool(10);
//   log_handler->setThreadPool(thread_pool);
//   for (size_t i = 0; i < numProcesses; i++) {
//     std::string key = "key" + std::to_string(i);
//     Record* read_record;
//     std::string offset;
//     std::string latest_offset;
//     View view = metadata_log->rollForwardMetadataLog();
//     log_handler->setLatestView(&view);
//     lru_cache->setLatestView(&view);
//     Status status = log_handler->readRecord(key, read_record, offset, latest_offset);
//     EXPECT_EQ(Status::kSuccess, status);
//     EXPECT_EQ("key" + std::to_string(i), read_record->key());
//     EXPECT_EQ("value" + std::to_string(i), read_record->value());
//   }
//   std::cout << "All records have been read." << std::endl;
// }

// void testFunctionMultiClientAppendRead(std::string prefix, int i) {
//   Storage* storage = new FileStorage("/tank/test/log/");
//   LRUCache* lru_cache = new LRUCache(33554432, storage);
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(prefix + "meta", storage, new TailCache());
//   LogHandler* log_handler = new LogHandler(1024, prefix, storage, lru_cache, metadata_log);
//   lru_cache->setFileMutexManager(new FileMutexManager());
//   ThreadPool* thread_pool = new ThreadPool(10);
//   log_handler->setThreadPool(thread_pool);
//   if (!storage || !log_handler) {
//     std::cerr << "Error: Memory allocation failed in child process " << i << std::endl;
//     delete storage;
//     delete log_handler;
//     _exit(1);
//   }
//   if (i % 2 != 0) {
//     Record record;
//     record.set_key("key" + std::to_string(i));
//     record.set_value("value" + std::to_string(i));
//     record.set_type(kTypeValue);
//     Status status = log_handler->addRecord(record);
//     EXPECT_EQ(Status::kSuccess, status);
//   } else {
//     std::string key = "key";
//     Record* read_record;
//     std::string offset;
//     std::string latest_offset;
//     View view = metadata_log->rollForwardMetadataLog();
//     log_handler->setLatestView(&view);
//     lru_cache->setLatestView(&view);
//     Status status = log_handler->readRecord(key, read_record, offset, latest_offset);
//     EXPECT_EQ(Status::kSuccess, status);
//     EXPECT_EQ("key", read_record->key());
//     EXPECT_EQ("value", read_record->value());
//     EXPECT_EQ(kTypeValue, read_record->type());
//   }
//   delete log_handler;
//   delete storage;
//   _exit(0);  // Exit successfully
// }

// TEST(LogTest, multiClient_append_read_to_log) {
//   int numProcesses = 100;
//   std::string prefix = "multiClient_append_read_to_log" + std::to_string(time(0));
//   Record record;
//   record.set_key("key");
//   record.set_value("value");
//   record.set_type(kTypeValue);
//   Storage* storage = new FileStorage("/tank/test/log/");
//   LRUCache* lru_cache = new LRUCache(33554432, storage);
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(prefix + "meta", storage, new TailCache());
//   LogHandler* log_handler = new LogHandler(1024, prefix, storage, lru_cache, metadata_log);
//   lru_cache->setFileMutexManager(new FileMutexManager());
//   ThreadPool* thread_pool = new ThreadPool(10);
//   log_handler->setThreadPool(thread_pool);
//   Status status = log_handler->addRecord(record);
//   delete metadata_log;
//   delete log_handler;
//   delete storage;
//   EXPECT_EQ(Status::kSuccess, status);
//   multipleProcessorTester(numProcesses, testFunctionMultiClientAppendRead, prefix);
// }

// // TEST(LogTest, new_unit_mutithreading_test) {
// //   std::vector<std::thread> threads;
// //   std::string prefix = "new_unit_mutithreading_test" + std::to_string(time(0)) + "/";
// //   for (size_t i = 0; i < 100; i++) {
// //     threads.push_back(std::thread([i, prefix]() {
// //       FileStorage* storage = new FileStorage("/tmp/db/", 1024);
// //       Handler* log_handler = new Handler(1024, prefix, storage);
// //       if (!storage || !log_handler) {
// //         std::cout << "error" << std::endl;
// //         delete storage;
// //         delete log_handler;
// //         return;
// //       }
// //       Record record;
// //       record.set_key("key" + std::to_string(i));
// //       record.set_value("value" + std::to_string(i));
// //       record.set_type(kTypeValue);
// //       Status status = log_handler->addRecord(record);
// //       EXPECT_EQ(Status::kSuccess, status);
// //       delete log_handler;
// //       delete storage;
// //     }));
// //   }
// //   for (size_t i = 0; i < 100; i++) {
// //     threads[i].join();
// //   }
// //   FileStorage* storage = new FileStorage("/tmp/db/", 1024);
// //   Handler* log_handler = new Handler(1024, prefix, storage);
// //   for (size_t i = 0; i < 100; i++) {
// //     std::string key = "key" + std::to_string(i);
// //     Record read_record;
// //     Status status = log_handler->readRecord(key, &read_record);
// //     EXPECT_EQ(Status::kSuccess, status);
// //     EXPECT_EQ("key" + std::to_string(i), read_record.key());
// //     EXPECT_EQ("value" + std::to_string(i), read_record.value());
// //   }
// //   delete log_handler;
// //   delete storage;
// // }

// // cannot use this to mimick race condition case.
// // because this test case use different threads to mimick different client, however, the ostream and ifstream are thread-safe based on following:
// // 27.1.3 Thread safety [iostreams.thread-safety]
// // Concurrent access to a stream object [string.streams, file.streams], stream buffer object [stream.buffers], or C Library stream [c.files]
// // by multiple threads may result in a data race [intro.multithread] unless otherwise specified [iostream.objects].
// //[Note: Data races result in undefined behavior [intro.multithread]. we need to use multiple process to mimick different client.

// // test metadata log handler
// /*
// message OperationRecord {
//   enum OperationType {
//     CREATE = 0;
//     COMPACT = 1;
//   }
//   required OperationType op_type = 1;
//   required string file_name = 2;
//   repeated string input_files = 3;  // Only used for COMPACT operations
// }

// */
// TEST(LogTest, Metadata_log_test) {
//   Storage* storage = new FileStorage("/tank/test/log/");
//   MetadataLogHandler* metadata_log_handler = new MetadataLogHandler("metadata_log" + std::to_string(time(0)), storage, new TailCache());
//   for (size_t i = 0; i < 10; i++) {
//     OperationRecord record;
//     record.set_op_type(OperationRecord::LOGCREATE);
//     if (i != 0) {
//       record.add_input_files("datalog/" + std::to_string(i - 1));
//     } else {
//       record.add_input_files("");
//     }
//     record.add_output_file("datalog/" + std::to_string(i));
//     metadata_log_handler->appendToMetadataLog(record);
//   }
//   OperationRecord record;
//   record.set_op_type(OperationRecord::COMPACT);
//   record.add_output_file("level1/" + std::to_string(0));
//   record.add_key_start("0_key0");
//   record.add_key_end("0_key9");
//   record.add_input_files("datalog/0");
//   record.add_input_files("datalog/1");
//   metadata_log_handler->appendToMetadataLog(record);
//   View view = metadata_log_handler->rollForwardMetadataLog();
//   EXPECT_EQ(view.getKeyRange("level1/0").first, "0_key0");
//   EXPECT_EQ(view.getKeyRange("level1/0").second, "0_key9");

//   record.Clear();
//   record.set_op_type(OperationRecord::COMPACT);
//   record.add_output_file("level1/" + std::to_string(1));
//   record.add_key_start("1_key0");
//   record.add_key_end("1_key9");
//   record.add_input_files("datalog/2");
//   record.add_input_files("datalog/3");
//   metadata_log_handler->appendToMetadataLog(record);
//   view = metadata_log_handler->rollForwardMetadataLog();
//   EXPECT_EQ(view.getKeyRange("level1/1").first, "1_key0");
//   EXPECT_EQ(view.getKeyRange("level1/1").second, "1_key9");

//   record.Clear();
//   record.set_op_type(OperationRecord::COMPACT);
//   record.add_output_file("level2/" + std::to_string(0));
//   record.add_key_start("0_key0");
//   record.add_key_end("0_key9");
//   record.add_input_files("level1/0");
//   record.add_input_files("level1/1");
//   metadata_log_handler->appendToMetadataLog(record);

//   view = metadata_log_handler->rollForwardMetadataLog();
//   EXPECT_EQ(view.getKeyRange("level2/0").first, "0_key0");
//   EXPECT_EQ(view.getKeyRange("level2/0").second, "0_key9");

//   metadata_log_handler->getLatestView(view);
//   EXPECT_EQ(view.getWithPrefix("datalog").size(), 6);
//   EXPECT_EQ(view.getWithPrefix("level1").size(), 0);
//   EXPECT_EQ(view.getWithPrefix("level2").size(), 1);
//   EXPECT_EQ(view.getCurrentLogTail(), "datalog/9");

//   delete metadata_log_handler;
//   delete storage;
// }

TEST(LogCacheTest, PutAndGetSealedFile) {
    Storage* storage = new FileStorage("/tank/test/log/");
    LRUCache* cache = new LRUCache(33554432, storage);
    std::string fileName = "log1";
    std::unordered_map<std::string, Record*> records;
    for (size_t i = 0; i < 2; i++) {
        Record* record = new Record();
        record->set_key("key" + std::to_string(i));
        record->set_value("value" + std::to_string(i));
        record->set_type(kTypeValue);
        records[record->key()] = record;
    }
    size_t offset = 2;
    bool sealed = true;

    // Add to cache
    cache->putLogRecords(fileName, records, offset, sealed);
    // Retrieve from cache
    Record* dummyRecord;
    cache->get(fileName, "key0", dummyRecord);

    // Check if the records and offset match
    EXPECT_EQ("key0", dummyRecord->key());
    EXPECT_EQ("value0", dummyRecord->value());
    EXPECT_EQ(kTypeValue, dummyRecord->type());
    EXPECT_EQ(sealed, cache->getCacheMap()[fileName].sealed);

    // Clean up dynamically allocated Records
    for (auto record : records) {
        delete record.second;
    }
}

TEST(LogCacheTest, ComplexCase) {
    Storage* storage = new FileStorage("/tank/test/log/");
    LRUCache* lru_cache = new LRUCache(33554432, storage);
    MetadataLogHandler* metadata_log = new MetadataLogHandler("cache1meta", storage, new TailCache());
    LogHandler* log_handler = new LogHandler(1024, "logcache1", storage, lru_cache, metadata_log);
    lru_cache->setFileMutexManager(new FileMutexManager());
    ThreadPool* thread_pool = new ThreadPool(10);
    log_handler->setThreadPool(thread_pool);
    LRUCache* cache = new LRUCache(33554432, storage);
    cache->setFileMutexManager(new FileMutexManager());

    std::vector<Record*> records;
    for (size_t i = 0; i < 2; i++) {
        Record* record = new Record();
        record->set_key("key" + std::to_string(i));
        record->set_value("value" + std::to_string(i));
        record->set_type(kTypeValue);
        records.push_back(record);
    }
    // case 1: in the cache, but it is not sealed, new records are added
    log_handler->addRecord(*records[0]);
    std::string fileName = "logcache1/1";
    size_t offset = storage->size(fileName);
    bool sealed = storage->isSealed(fileName);
    cache->putLogRecords(fileName, {{records[0]->key(), records[0]}}, offset, sealed);

    Record* dummyRecord;
    cache->get(fileName, "key0", dummyRecord);
    EXPECT_EQ("key0", dummyRecord->key());
    EXPECT_EQ("value0", dummyRecord->value());
    EXPECT_EQ(kTypeValue, dummyRecord->type());

    log_handler->addRecord(*records[1]);
    
    View view = metadata_log->rollForwardMetadataLog();
    cache->setLatestView(&view);

    // Retrieve from cache
    Record* dummyRecord1;
    bool read_more = true;  // not found or the file is tail(not sealed)
    size_t cached_offset = 0;
    size_t size = 0;
    cache->checkReadMoreLog(fileName, read_more, cached_offset, size);
    if (read_more) {
        cache->readDataLog(fileName, cached_offset, size);
    }
    cache->get(fileName, "key1", dummyRecord1);
    EXPECT_EQ("key1", dummyRecord1->key());
    EXPECT_EQ("value1", dummyRecord1->value());
    EXPECT_EQ(kTypeValue, dummyRecord1->type());
    delete log_handler;
    delete cache;
    delete metadata_log;

    // records have been deleted when delete cache
    records.clear();
    for (size_t i = 0; i < 2; i++) {
        Record* record = new Record();
        record->set_key("key" + std::to_string(i));
        record->set_value("value" + std::to_string(i));
        record->set_type(kTypeValue);
        records.push_back(record);
    }
    // case 2: not in the cache
    //  Retrieve from cache
    delete lru_cache;
    lru_cache = new LRUCache(33554432, storage);
    metadata_log = new MetadataLogHandler("cache2meta", storage, new TailCache());
    log_handler = new LogHandler(1024, "logcache2", storage, lru_cache, metadata_log);
    lru_cache->setFileMutexManager(new FileMutexManager());
    cache = new LRUCache(33554432, storage);
    cache->setFileMutexManager(new FileMutexManager());
    log_handler->addRecord(*records[0]);
    log_handler->addRecord(*records[1]);
    fileName = "logcache2/1";
    
    view = metadata_log->rollForwardMetadataLog();
    cache->setLatestView(&view);
    
    Record* dummyRecord2;
    read_more = true;  // not found or the file is tail(not sealed)
    cached_offset = 0;
    size = 0;
    cache->checkReadMoreLog(fileName, read_more, cached_offset, size);
    if (read_more) {
        cache->readDataLog(fileName, cached_offset, size);
    }
    Record* record_tmp;
    cache->get(fileName, "key0", dummyRecord2);
    EXPECT_EQ("key0", dummyRecord2->key());
    EXPECT_EQ("value0", dummyRecord2->value());
    EXPECT_EQ(kTypeValue, dummyRecord2->type());
    Record* dummyRecord3;
    cache->get(fileName, "key1", dummyRecord3);
    EXPECT_EQ("key1", dummyRecord3->key());
    EXPECT_EQ("value1", dummyRecord3->value());
    EXPECT_EQ(kTypeValue, dummyRecord3->type());
    delete log_handler;
    delete cache;
}

// TEST(LogCacheTest, EvictLeastRecentlyUsed) {
//     std::string file1 = "sst1";
//     std::string file2 = "sst2";
//     std::string file3 = "sst3";
//     std::string file4 = "sst4";  // This will cause the eviction of the least recently used file

//     Storage* storage = new FileStorage("/tank/test/log/");
//     LRUCache* cache = new LRUCache(3, storage);
//     Record* record1 = new Record();
//     record1->set_key("key1");
//     record1->set_value("value1");
//     record1->set_type(kTypeValue);

//     cache->putSSTableRecords(file1, {{"1", record1}}, "index1", 1);
//     cache->putSSTableRecords(file2, {{"1", record1}}, "index2", 1);
//     cache->putSSTableRecords(file3, {{"1", record1}}, "index3", 1);

//     // Access file1 to make it most recently used
//     Record* dummyRecord;
//     cache->get(file1, "key1", dummyRecord, "index1");  // checkReadMore and get must be invoken together

//     // Adding file4 should evict file2 (least recently used)
//     cache->putSSTableRecords(file4, {{"1", record1}}, "index4", 1);

//     // Check if file2 has been evicted
//     EXPECT_EQ(cache->getCacheMap()[file2].block_records.find("index2"), cache->getCacheMap()[file2].block_records.end());

//     delete record1;
// }

// TEST(MetadataLogTest, UnorderedRecord){
//   Storage* storage = new FileStorage("/tank/test/log/");
//   std::string meta_prefix = "UnorderedRecord" + std::to_string(time(0));
//   MetadataLogHandler* metadata_log = new MetadataLogHandler(meta_prefix, storage, new TailCache());
//   OperationRecord record;
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("");
//   record.add_output_file("datalog/1");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("datalog/2");
//   record.add_output_file("datalog/3");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("datalog/1");
//   record.add_output_file("datalog/2");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();

//   View view = metadata_log->rollForwardMetadataLog();
//   //print the view
//   std::cout << view.getCurrentLogTail() << std::endl;
//   for (auto const& [key, value] : view.storage_layout) {
//     std::cout << key << ": ";
//     for (auto const& file : value) {
//       std::cout << file << " ";
//     }
//     std::cout << std::endl;
//   }
// }

// TEST(MetadataLogTest, UnorderedRecord1) {
//   Storage *storage = new FileStorage("/tank/test/log/");
//   std::string meta_prefix = "UnorderedRecord1" + std::to_string(time(0));
//   MetadataLogHandler *metadata_log =
//       new MetadataLogHandler(meta_prefix, storage, new TailCache());
//   OperationRecord record;
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("");
//   record.add_output_file("datalog/1");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("datalog/4");
//   record.add_output_file("datalog/5");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("datalog/2");
//   record.add_output_file("datalog/3");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("datalog/1");
//   record.add_output_file("datalog/2");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   record.set_op_type(OperationRecord::LOGCREATE);
//   record.add_input_files("datalog/3");
//   record.add_output_file("datalog/4");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   View view = metadata_log->rollForwardMetadataLog();
//   // print the view
//   std::cout << view.getCurrentLogTail() << std::endl;
//   for (auto const &[key, value] : view.storage_layout) {
//     std::cout << key << ": ";
//     for (auto const &file : value) {
//       std::cout << file << " ";
//     }
//     std::cout << std::endl;
//   }
//   record.set_op_type(OperationRecord::COMPACT);
//   record.add_input_files("datalog/3");
//   record.add_input_files("datalog/4");
//   record.add_output_file("level1/2");
//   record.add_key_start("2_key0");
//   record.add_key_end("2_key9");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();
//   view = metadata_log->rollForwardMetadataLog();
//   // print the view
//   std::cout << view.getCurrentLogTail() << std::endl;
//   for (auto const &[key, value] : view.storage_layout) {
//     std::cout << key << ": ";
//     for (auto const &file : value) {
//       std::cout << file << " ";
//     }
//     std::cout << std::endl;
//   }
//   record.set_op_type(OperationRecord::COMPACT);
//   record.add_input_files("datalog/1");
//   record.add_input_files("datalog/2");
//   record.add_output_file("level1/1");
//   record.add_key_start("1_key0");
//   record.add_key_end("1_key9");
//   metadata_log->appendToMetadataLog(record);
//   record.Clear();

//   view = metadata_log->rollForwardMetadataLog();
//   // print the view
//   std::cout << view.getCurrentLogTail() << std::endl;
//   for (auto const &[key, value] : view.storage_layout) {
//     std::cout << key << ": ";
//     for (auto const &file : value) {
//       std::cout << file << " ";
//     }
//     std::cout << std::endl;
//   }
// }