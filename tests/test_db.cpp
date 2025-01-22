#include "db.h"
#include "gtest/gtest.h"
#include <chrono>
using namespace ozonedb;

class DBTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // This will run before each test
    system("rm -rf /tank/test/db/");
  }

  // You can also add a TearDown method if needed
  void TearDown() override {
    // Code here will be called after each test
  }
};

TEST_F(DBTest, open) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  DB::closeDB(db);
}

TEST_F(DBTest, single_put_get) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  status = db->put("key", "value");
  EXPECT_EQ(Status::kSuccess, status);
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  std::string const* value = nullptr;
  status = db->get("key", value);
  EXPECT_EQ(Status::kSuccess, status);
  EXPECT_EQ("value", *value);

  DB::closeDB(db);
}

TEST_F(DBTest, multiple_put_get) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  for (size_t i = 0; i < 10; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  for (size_t i = 0; i < 10; i++) {
    std::string const* value = nullptr;
    auto start = std::chrono::high_resolution_clock::now();
    status = db->get("key" + std::to_string(i), value);
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken to get the value " << i << " : " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() << "ns" << std::endl;
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i), *value);
  }

  DB::closeDB(db);
}

TEST_F(DBTest, put_after_put_return_new_value) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  status = db->put("key", "value");
  EXPECT_EQ(Status::kSuccess, status);

  status = db->put("key", "new_value");
  EXPECT_EQ(Status::kSuccess, status);
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  std::string const* value = nullptr;
  status = db->get("key", value);
  EXPECT_EQ(Status::kSuccess, status);
  EXPECT_EQ("new_value", *value);

  DB::closeDB(db);
}

TEST_F(DBTest, delete_after_put) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  status = db->put("key", "value");
  EXPECT_EQ(Status::kSuccess, status);
  status = db->remove("key");
  EXPECT_EQ(Status::kSuccess, status);
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  std::string const* value = nullptr;
  status = db->get("key", value);
  EXPECT_EQ(Status::kFailure, status);

  DB::closeDB(db);
}

TEST_F(DBTest, large_put) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  for (size_t i = 0; i < 100; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);
}

TEST_F(DBTest, tailcache) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  for (size_t i = 0; i < 100; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  for (size_t i = 0; i < 10; i++) {
    std::string const* value = nullptr;
    status = db->get("key" + std::to_string(i * 10), value);
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i * 10), *value);
  }
  for (size_t i = 0; i < 10; i++) {
    status = db->put("key" + std::to_string(i * 10), "value_new" + std::to_string(i * 10));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  for (size_t i = 0; i < 10; i++) {
    std::string const* value = nullptr;
    status = db->get("key" + std::to_string(i * 10), value);
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value_new" + std::to_string(i * 10), *value);
  }
  std::string const* value = nullptr;
  status = db->get("key100", value);
  EXPECT_EQ(Status::kFailure, status);

  DB::closeDB(db);
}

TEST_F(DBTest, tailcache1) {
  DB* db = nullptr;

  std::string shared_config_path = "../src/config/cloud/shared_config_rocksdb_base.json";

  Status status = DB::openDB(db, shared_config_path);
  EXPECT_EQ(Status::kSuccess, status);

  for (size_t i = 0; i < 55; i++) {
    status = db->put("key" + std::to_string(i), "value" + std::to_string(i));
    EXPECT_EQ(Status::kSuccess, status);
  }
  DB::closeDB(db);

  DB::openDB(db, shared_config_path);
  for (size_t i = 0; i < 10; i++) {
    std::string const* value = nullptr;
    auto start = std::chrono::high_resolution_clock::now();
    status = db->get("key" + std::to_string(i), value);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end - start;
    std::cout << "Time taken to get the value: " << elapsed_time.count() << "s" << std::endl;
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i), *value);
  }
  for (size_t i = 0; i < 10; i++) {
    std::string const* value = nullptr;
    auto start = std::chrono::high_resolution_clock::now();
    status = db->get("key" + std::to_string(i), value);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end - start;
    std::cout << "Time taken to get the value: " << elapsed_time.count() << "s" << std::endl;
    EXPECT_EQ(Status::kSuccess, status);
    EXPECT_EQ("value" + std::to_string(i), *value);
  }

  DB::closeDB(db);
}