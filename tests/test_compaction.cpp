#include "db.h"
#include "gtest/gtest.h"
using namespace ozonedb;
TEST(CompactionTest, CompactionIntoLevel1) {
  system("rm -rf /tank/test/compaction/");
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t i = 0; i < 250; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);
}

TEST(CompactionTest, CompactionIntoMiddleLevel) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t i = 250; i < 350; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);
}

TEST(CompactionTest, CompactionInLastLevel) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t i = 350; i < 2000; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  for (size_t i = 0; i < 2000; i++) {
    std::string const* value = nullptr;
    status = db->get("key" + std::to_string(i), value);
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i), *value);
  }
  DB::closeDB(db);
}

TEST(CompactionTest, CacheModifiedAfterCompaction) {
  system("rm -rf /tank/test/compaction/");
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t i = 0; i < 108; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  // get the records from log1 and log2 to enable cache
  for (size_t i = 0; i < 108; i++) {
    std::string const* value = nullptr;
    status = db->get("key" + std::to_string(i), value);
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i), *value);
  }

  // get cache
  TailCache* cache = db->getCache();
  // print key to file map
  for (auto it = cache->getKeyToTailMap().begin(); it != cache->getKeyToTailMap().end(); it++) {
    assert(it->second.second.substr(0, 10) == "datalog/2");
  }

  // trigger compaction
  for (size_t i = 108; i < 250; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }

  for (auto it = cache->getKeyToTailMap().begin(); it != cache->getKeyToTailMap().end(); it++) {
    assert(it->second.second.substr(0, 8) == "sstable1");
  }

  DB::closeDB(db);
}

TEST(CompactionTest, ReadAfterCompaction) {
  // do not involve dbcache
  system("rm -rf /tank/test/compaction/");
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t i = 0; i < 450; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  for (size_t i = 0; i < 450; i++) {
    std::string const* value = nullptr;
    status = db->get("key" + std::to_string(i), value);
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i), *value);
  }

  DB::closeDB(db);
}

TEST(CompactionTest, ReadAfterCompaction1) {
  // involve dbcache
  system("rm -rf /tank/test/compaction/");
  DB* db = nullptr;
  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t i = 0; i < 206; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  // get the records from log4 to enable cache
  for (size_t i = 0; i < 206; i++) {
    std::string const* value = nullptr;
    status = db->get("key" + std::to_string(i), value);
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i), *value);
  }

  // trigger compaction
  for (size_t i = 0; i < 206; i++) {
    status = db->put("key" + std::to_string(i), "value_new" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  for (size_t i = 0; i < 206; i++) {
    std::string const* value = nullptr;
    status = db->get("key" + std::to_string(i), value);
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value_new" + std::to_string(i), *value);
  }
  DB::closeDB(db);
}
#include "test_tool.h"
void testFunctionMultiAltruisticClientCompact(std::string prefix, int i) {
  DB* db = nullptr;
  std::string shared_config_path = "../src/config/local/test_compaction.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t j = 0; j < 200; j++) {
    db->put(std::to_string(i) + "key" + std::to_string(j), std::to_string(i) + "value" + std::to_string(j));
  }
  std::cout << getpid() << ":sleeping for 5 seconds" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(5));
  DB::closeDB(db);
}

TEST(CompactionTest, TaskLogTestHomogenousAltruism) {
  system("rm -rf /tank/test/compaction/");
  int numProcesses = 10;
  multipleProcessorTester(numProcesses, testFunctionMultiAltruisticClientCompact, "");
  DB* db = nullptr;
  std::string shared_config_path = "../src/config/local/test_compaction.json";
  Status status = DB::openDB(db, shared_config_path);

  EXPECT_EQ(Status::kSuccess, status);

  for (size_t i = 0; i < numProcesses; i++) {
    for (size_t j = 0; j < 200; j++) {
      std::string const* value = nullptr;
      status = db->get(std::to_string(i) + "key" + std::to_string(j), value);
      EXPECT_EQ(Status::kSuccess, status);
      EXPECT_EQ(std::to_string(i) + "value" + std::to_string(j), *value);
    }
  }
  DB::closeDB(db);
}

void testFunctionMultiSelfishClientCompact(std::string prefix, int i) {
  DB* db = nullptr;
  std::string shared_config_path = "../src/config/local/test_compaction_selfish.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);
  for (size_t j = 0; j < 1000; j++) {
    if (j % (i + 1) == 0) {
      db->put(std::to_string(i) + "key" + std::to_string(j), std::to_string(i) + "value" + std::to_string(j));
    }
  }
  std::cout << getpid() << ":sleeping for 5 seconds" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(5));
  DB::closeDB(db);
}

TEST(CompactionTest, TaskLogTestHomogenousSelfish) {
  system("rm -rf /tank/test/selfish/");
  int numProcesses = 10;
  multipleProcessorTester(numProcesses, testFunctionMultiSelfishClientCompact,
                          "");
  DB* db = nullptr;
  std::string shared_config_path = "../src/config/local/test_compaction_selfish.json";
  Status status = DB::openDB(db, shared_config_path);

  EXPECT_EQ(Status::kSuccess, status);

  for (size_t i = 0; i < numProcesses; i++) {
    for (size_t j = 0; j < 1000; j++) {
      if (j % (i + 1) == 0) {
        std::string const* value = nullptr;
        status = db->get(std::to_string(i) + "key" + std::to_string(j), value);
        EXPECT_EQ(Status::kSuccess, status);
        EXPECT_EQ(std::to_string(i) + "value" + std::to_string(j), *value);
      }
    }
  }
  DB::closeDB(db);
}

/*
log1: 0-55
log2: 56-108
log3: 109-157
log4: 158-206

*/