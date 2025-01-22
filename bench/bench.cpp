#include "db.h"
#include "histogram.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "test_tool.h"
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/xml/domconfigurator.h>
// #include <pebblesdb/db.h>
#include <chrono>
#include <functional>  // For std::bind
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <sqlite3.h>

using namespace log4cxx;
using namespace log4cxx::helpers;

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>

//./runBench --benchmarks=fillseqsync,readseq,deleseqsync --db=sqlite,pebblesdb,rocksdb

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//
//   open          -- open a database
//   fillseqsync   -- write N values in sequential key order in sync mode
//   deleseqsync   -- delete N values in sequential key order in sync mode
//   readseq       -- read N times sequentially
//
// Initialize the basic logging configuration
LoggerPtr logger;

// Create a logger instance
static char const* FLAGS_benchmarks = "";

//"sqlite"
//"pebblesdb"
//"rocksdb"
//"ozonedb";
static char const* FLAGS_db = "";

// Number of key/values to place in database
static int FLAGS_num = 500;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

static int FLAGS_key_size = 16;  // 16 bytes

// Size of each value
static int FLAGS_value_size = 1024 * 1024 * 1;  // 0.5 MB

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

static int FLAGS_num_clients = 1;  // only used in ozonedb

static std::vector<int> FLAGS_op_each_clients;

// Use the db with the following name.
static char const* FLAGS_sqlite_path = "/tank/db/sqlite.db";
static char const* FLAGS_pebblesdb_path = "/tank/db/pebblesdb";
static char const* FLAGS_rocksdb_path = "/tank/db/rocksdb";
// ozonedb path is in shared_config.json

//========================================current unchangable settings========================================
// Print histogram of operation timings
static bool FLAGS_histogram = false;

static int FLAGS_write_buffer_size = 4 << 20;

// Page size. Default 1 KB.
static int FLAGS_sqlite_page_size = 1024;

// Number of pages.
// Default cache size = FLAGS_page_size * FLAGS_num_pages = 4 MB.
static int FLAGS_sqlite_num_pages = 4096;

// If true, we enable Write-Ahead Logging
static bool FLAGS_sqlite_WAL_enabled = true;

static std::chrono::_V2::system_clock::time_point rocksdb_start_time_;
static std::chrono::_V2::system_clock::time_point ozonedb_start_time_;

std::random_device rd;                        // Seed
std::mt19937 gen(rd());                       // Mersenne Twister engine
std::uniform_int_distribution<> dis(1, 100);  // Define a uniform distribution between 1 and 100

std::string gen_str_of_size(size_t size) {
  return std::string(size, 'x');
}

enum Order { SEQUENTIAL,
             RANDOM };
enum DBState { FRESH,
               EXISTING };

std::vector<std::pair<std::string, std::string>> load_workload(Order order, int op_count) {
  std::vector<std::pair<std::string, std::string>> workload;
  std::string value = gen_str_of_size(FLAGS_value_size);
  for (int i = 1; i <= op_count; ++i) {
    int const k = (order == SEQUENTIAL) ? i : (dis(gen) % FLAGS_num);
    char key[100];
    std::string format = "%0" + std::to_string(FLAGS_key_size) + "d";
    std::snprintf(key, sizeof(key), format.c_str(), k);
    workload.emplace_back(key, value);
    // for (int j = 0; j < 10; ++j) {
    //   workload.emplace_back(key, value);
    // }
  }
  // shuffle the workload
  if (order == RANDOM) {
    std::shuffle(workload.begin(), workload.end(), std::mt19937{std::random_device{}()});
  }
  return workload;
}

class EventListenerRocksDB : public rocksdb::EventListener {
 public:
  std::chrono::_V2::system_clock::time_point start_time_;
  void OnFlushBegin(rocksdb::DB* /*db*/,
                    rocksdb::FlushJobInfo const& /*flush_job_info*/) override {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Rocksdb Flush Started at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - rocksdb_start_time_).count());
  }

  void OnCompactionBegin(rocksdb::DB* /*db*/, rocksdb::CompactionJobInfo const& /*ci*/) override {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Rocksdb Compaction Started at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - rocksdb_start_time_).count());
  }

  void OnCompactionCompleted(rocksdb::DB* /* db */,
                             rocksdb::CompactionJobInfo const& info) override {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Rocksdb Compaction Completed at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - rocksdb_start_time_).count());
  }

  void OnFlushCompleted(rocksdb::DB* /* db */, rocksdb::FlushJobInfo const& /* info */) override {
    // Convert to time_t to get calendar time
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Rocksdb Flush Completed at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - rocksdb_start_time_).count());
  }
};

class EventListenerOzonedb : public ozonedb::EventListener {
 public:
  void onLogCompactionStart() {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Log Compaction Started at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - ozonedb_start_time_).count());
  };
  void onLogCompactionCompletion(int time) {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Log Compaction Completed at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - ozonedb_start_time_).count()
                                  << " time: " << time);
  };
  void onSSTableCompactionStart() {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  <<"SST Compaction Started at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - ozonedb_start_time_).count());
  };
  void onSSTableCompactionCompletion(int time, int source_level) {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Level "
                                  << source_level 
                                  << " SST Compaction Completed at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - ozonedb_start_time_).count()
                                  << " time: " << time);
  };

  void onViewUpdate() {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "View update Completed at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - ozonedb_start_time_).count());
  };
  void onNewTail() {
    auto now = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "New Tail Completed at "
                                  << std::chrono::duration_cast<std::chrono::nanoseconds>(now - ozonedb_start_time_).count());
  };
};

class Benchmark {
  ozonedb::DB* ozonedb_ = nullptr;
  // leveldb::DB* pebblesdb_ = nullptr;
  sqlite3* sqlite_ = nullptr;
  rocksdb::DB* rocksdb_ = nullptr;
  std::vector<std::pair<std::string, std::string>> workload;
  std::chrono::_V2::system_clock::time_point last_op_finish_;
  std::chrono::_V2::system_clock::time_point start_time_;
  std::chrono::_V2::system_clock::time_point end_time_;
  ozonedb::Histogram histogram;

  inline static void execErrorCheck(int status, char* err_msg) {
    if (status != SQLITE_OK) {
      std::fprintf(stderr, "SQL error: %s\n", err_msg);
      sqlite3_free(err_msg);
      std::exit(1);
    }
  }

  inline static void errorCheck(int status) {
    if (status != SQLITE_OK) {
      std::fprintf(stderr, "sqlite3 error: status = %d\n", status);
      std::exit(1);
    }
  }

  inline static void stepErrorCheck(int status) {
    if (status != SQLITE_DONE) {
      std::fprintf(stderr, "SQL step error: status = %d\n", status);
      std::exit(1);
    }
  }

  void finishedSingleOp(int bytes) {
    auto now = std::chrono::high_resolution_clock::now();
    auto single_op_duration = (now - last_op_finish_);
    // std::cout <<getpid() <<":" << "[Single Op Time]: " << std::chrono::duration_cast<std::chrono::nanoseconds>(single_op_duration).count() << std::endl;
    last_op_finish_ = now;
    if (FLAGS_histogram) {
      histogram.Add(std::chrono::duration_cast<std::chrono::nanoseconds>(single_op_duration).count());
    }
    auto result = bytes * 1.0 / 1024 / 1024 / std::chrono::duration_cast<std::chrono::duration<double>>(single_op_duration).count();
    if (std::string(FLAGS_db).find("rocksdb") != std::string::npos) {
      LOG4CXX_INFO(logger, getpid() << ":"
                                    << "throughput: " << result << " MB/s at " << std::chrono::duration_cast<std::chrono::nanoseconds>(now - rocksdb_start_time_).count());
    }
    if (std::string(FLAGS_db).find("ozonedb") != std::string::npos) {
      LOG4CXX_INFO(logger, getpid() << ":"
                                    << "throughput: " << result << " MB/s at " << std::chrono::duration_cast<std::chrono::nanoseconds>(now - ozonedb_start_time_).count());
    }
  }
  //=============================open the database=======================================
  void sqliteOpen() {
    assert(sqlite_ == nullptr);
    start();
    int status;
    char* err_msg = nullptr;
    // Open database
    status = sqlite3_open(FLAGS_sqlite_path, &sqlite_);
    if (status) {
      std::fprintf(stderr, "open error: %s\n", sqlite3_errmsg(sqlite_));
      std::exit(1);
    }

    // Change SQLite cache size to 4MB
    char cache_size[100];
    std::snprintf(cache_size, sizeof(cache_size), "PRAGMA cache_size = %d",
                  FLAGS_sqlite_num_pages);
    status = sqlite3_exec(sqlite_, cache_size, nullptr, nullptr, &err_msg);
    execErrorCheck(status, err_msg);

    // create table
    std::string create_stmt =
        "CREATE TABLE IF NOT EXISTS test (key text PRIMARY KEY, value text);";
    sqlite3_exec(sqlite_, create_stmt.c_str(), nullptr, nullptr, &err_msg);
    execErrorCheck(status, err_msg);
    stop("sqlite_open");
  }

  void ozonedbOpen() {
    assert(ozonedb_ == nullptr);
    start();
    ozonedb::Status status = ozonedb::DB::openDB(ozonedb_, "../src/config/local/shared_config_rocksdb_base.json");
    ozonedb_->setEventListener(new EventListenerOzonedb());
    if (status != ozonedb::Status::kSuccess) {
      std::cerr << "Failed to open OzoneDB database" << std::endl;
      return;
    }
    stop("ozonedb_open");
  }

  // void pebblesdbOpen() {
  //   assert(pebblesdb_ == nullptr);
  //   start();
  //   leveldb::Options options;
  //   options.create_if_missing = true;
  //   options.compression = leveldb::kNoCompression;
  //   options.write_buffer_size = FLAGS_write_buffer_size;
  //   leveldb::Status status = leveldb::DB::Open(options, FLAGS_pebblesdb_path, &pebblesdb_);
  //   if (!status.ok()) {
  //     std::cerr << "Failed to open PebblesDB database: " << status.ToString() << std::endl;
  //     return;
  //   }
  //   stop("pebblesdb_open");
  // }

  void rocksdbOpen() {
    assert(rocksdb_ == nullptr);
    start();
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::kNoCompression;
    // options.write_buffer_size = FLAGS_write_buffer_size;
    options.write_buffer_size = 64 << 20;  // single memtable size
    options.level_compaction_dynamic_level_bytes = 0;
    options.listeners.emplace_back(new EventListenerRocksDB());
    rocksdb::Status status = rocksdb::DB::Open(options, FLAGS_rocksdb_path, &rocksdb_);
    if (!status.ok()) {
      std::cerr << "Failed to open RocksDB database: " << status.ToString() << std::endl;
      return;
    }
    stop("rocksdb_open");
  }

  //=============================put=======================================
  void sqlitePut(bool write_sync, DBState state) {
    char* err_msg = nullptr;
    int status;

    if (state == FRESH) {
      if (FLAGS_use_existing_db) {
        std::cout << getpid() << ":"
                  << "skipping (--use_existing_db is true)" << std::endl;
        return;
      }
      system("rm -rf /tank/db/sqlite*");
    }
    sqliteOpen();
    start();
    // Check for synchronous flag in options
    std::string sync_stmt =
        (write_sync) ? "PRAGMA synchronous = FULL" : "PRAGMA synchronous = OFF";
    status = sqlite3_exec(sqlite_, sync_stmt.c_str(), nullptr, nullptr, &err_msg);
    execErrorCheck(status, err_msg);

    sqlite3_stmt* replace_stmt;
    std::string replace_str = "INSERT or REPLACE INTO test (key, value) VALUES (?, ?)";
    status = sqlite3_prepare_v2(sqlite_, replace_str.c_str(), -1, &replace_stmt,
                                nullptr);
    errorCheck(status);

    for (auto const& [key, value] : workload) {
      sqlite3_bind_text(replace_stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(replace_stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
      int rc = sqlite3_step(replace_stmt);
      finishedSingleOp(key.size() + value.size());
      stepErrorCheck(rc);
      sqlite3_reset(replace_stmt);
    }
    stop("sqlite_put");
    status = sqlite3_finalize(replace_stmt);
    errorCheck(status);
    sqlite3_close(sqlite_);
    sqlite_ = nullptr;
  }

  void ozonedbPut(bool write_sync, DBState state) {
    if (state == FRESH) {
      if (FLAGS_use_existing_db) {
        std::cout << getpid() << ":"
                  << "skipping (--use_existing_db is true)" << std::endl;
        return;
      }
      system("make clean-db");
    }
    ozonedbOpen();
    start();
    ozonedb_start_time_ = start_time_;
    for (auto const& [key, value] : workload) {
      ozonedb::Status status = ozonedb_->put(key, value);
      finishedSingleOp(key.size() + value.size());
      assert(status == ozonedb::Status::kSuccess);
    }

    stop("ozonedb_put");
    ozonedb::DB::closeDB(ozonedb_);
    ozonedb_ = nullptr;
  }

  // void pebblesdbPut(bool write_sync, DBState state) {
  //   if (state == FRESH) {
  //     if (FLAGS_use_existing_db) {
  //       std::cout << getpid() << ":"
  //                 << "skipping (--use_existing_db is true)" << std::endl;
  //       return;
  //     }
  //     system("rm -rf /tank/db/pebblesdb");
  //   }
  //   pebblesdbOpen();
  //   start();
  //   for (auto const& [key, value] : workload) {
  //     leveldb::WriteOptions write_options;
  //     write_options.sync = write_sync;
  //     leveldb::Status status = pebblesdb_->Put(write_options, key, value);
  //     finishedSingleOp(key.size() + value.size());
  //     assert(status.ok());
  //   }
  //   stop("pebblesdb_put");
  //   delete pebblesdb_;
  //   pebblesdb_ = nullptr;
  // }

  void rocksdbPut(bool write_sync, DBState state) {
    if (state == FRESH) {
      if (FLAGS_use_existing_db) {
        std::cout << getpid() << ":"
                  << "skipping (--use_existing_db is true)" << std::endl;
        return;
      }
      system("rm -rf /tank/db/rocksdb");
    }
    rocksdbOpen();
    start();
    rocksdb_start_time_ = start_time_;
    for (auto const& [key, value] : workload) {
      rocksdb::WriteOptions write_options;
      write_options.sync = write_sync;
      rocksdb::Status status = rocksdb_->Put(write_options, key, value);
      finishedSingleOp(key.size() + value.size());
      assert(status.ok());
    }
    stop("rocksdb_put");
    delete rocksdb_;
    rocksdb_ = nullptr;
  }

  //=============================get=======================================
  void sqliteGet() {
    char* err_msg = nullptr;
    int status;

    sqliteOpen();
    start();
    sqlite3_stmt* get_stmt;
    std::string get_str = "SELECT value FROM test WHERE key = ?";
    status = sqlite3_prepare_v2(sqlite_, get_str.c_str(), -1, &get_stmt, nullptr);
    errorCheck(status);

    for (auto const& [key, value] : workload) {
      sqlite3_bind_text(get_stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
      int rc = sqlite3_step(get_stmt);
      finishedSingleOp(key.size() + value.size());
      if (rc != SQLITE_ROW) {
        std::fprintf(stderr, "SQL step error: status = %d\n", rc);
        std::exit(1);
      }
      void const* result_text = sqlite3_column_text(get_stmt, 0);
      int result_size = sqlite3_column_bytes(get_stmt, 0);
      assert(result_size == value.size());
      assert(memcmp(result_text, value.c_str(), result_size) == 0);
      sqlite3_reset(get_stmt);
    }
    stop("sqlite_get");

    status = sqlite3_finalize(get_stmt);
    errorCheck(status);
    sqlite3_close(sqlite_);
    sqlite_ = nullptr;
  }

  void ozonedbGet() {
    ozonedbOpen();
    start();
    ozonedb_start_time_ = std::chrono::high_resolution_clock::now();
    for (auto const& [key, value] : workload) {
      std::string const* result = nullptr;
      ozonedb::Status s = ozonedb_->get(key, result);
      finishedSingleOp(key.size() + value.size());
      assert(s == ozonedb::Status::kSuccess);
      assert(result->compare(value) == 0);
    }
    stop("ozonedb_get");
    ozonedb::DB::closeDB(ozonedb_);
    ozonedb_ = nullptr;
  }

  // void pebblesdbGet() {
  //   pebblesdbOpen();
  //   start();
  //   for (auto const& [key, value] : workload) {
  //     std::string result;
  //     leveldb::Status status = pebblesdb_->Get(leveldb::ReadOptions(), key, &result);
  //     finishedSingleOp(key.size() + value.size());
  //     assert(status.ok());
  //     assert(result.compare(value) == 0);
  //   }
  //   stop("pebblesdb_get");
  //   delete pebblesdb_;
  //   pebblesdb_ = nullptr;
  // }

  void rocksdbGet() {
    rocksdbOpen();
    start();
    rocksdb_start_time_ = std::chrono::high_resolution_clock::now();
    for (auto const& [key, value] : workload) {
      std::string result;
      rocksdb::Status status = rocksdb_->Get(rocksdb::ReadOptions(), key, &result);
      finishedSingleOp(key.size() + value.size());
      assert(status.ok());
      assert(result.compare(value) == 0);
    }
    // std::string block_cache_usage;
    // rocksdb_->GetProperty("rocksdb.block-cache-usage", &block_cache_usage);
    // std::cout << getpid() << ":"
    //           << "Block Cache Usage: " << block_cache_usage << std::endl;
    stop("rocksdb_get");
    delete rocksdb_;
    rocksdb_ = nullptr;
  }

  void sqliteRemove(bool write_sync) {
    char* err_msg = nullptr;
    int status;
    sqliteOpen();
    start();
    // Check for synchronous flag in options
    std::string sync_stmt =
        (write_sync) ? "PRAGMA synchronous = FULL" : "PRAGMA synchronous = OFF";
    status = sqlite3_exec(sqlite_, sync_stmt.c_str(), nullptr, nullptr, &err_msg);
    execErrorCheck(status, err_msg);

    sqlite3_stmt* remove_stmt;
    std::string remove_str = "DELETE FROM test WHERE key = ?";
    status = sqlite3_prepare_v2(sqlite_, remove_str.c_str(), -1, &remove_stmt, nullptr);
    errorCheck(status);

    for (auto const& [key, value] : workload) {
      sqlite3_bind_text(remove_stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
      int rc = sqlite3_step(remove_stmt);
      finishedSingleOp(key.size() + value.size());
      sqlite3_reset(remove_stmt);
      stepErrorCheck(rc);
    }
    stop("sqlite_remove");
    status = sqlite3_finalize(remove_stmt);
    errorCheck(status);
    sqlite3_close(sqlite_);
    sqlite_ = nullptr;
  }

  void ozonedbRemove(bool write_sync) {
    ozonedbOpen();
    start();
    for (auto const& [key, value] : workload) {
      ozonedb::Status status = ozonedb_->remove(key);
      finishedSingleOp(key.size() + value.size());
      assert(status == ozonedb::Status::kSuccess);
    }
    stop("ozonedb_remove");
    ozonedb::DB::closeDB(ozonedb_);
    ozonedb_ = nullptr;
  }

  // void pebblesdbRemove(bool write_sync) {
  //   pebblesdbOpen();
  //   start();
  //   for (auto const& [key, value] : workload) {
  //     leveldb::WriteOptions write_options;
  //     write_options.sync = write_sync;
  //     leveldb::Status status = pebblesdb_->Delete(write_options, key);
  //     finishedSingleOp(key.size() + value.size());
  //     assert(status.ok());
  //   }
  //   stop("pebblesdb_remove");
  //   delete pebblesdb_;
  //   pebblesdb_ = nullptr;
  // }

  void rocksdbRemove(bool write_sync) {
    rocksdbOpen();
    start();
    for (auto const& [key, value] : workload) {
      rocksdb::WriteOptions write_options;
      write_options.sync = write_sync;
      rocksdb::Status status = rocksdb_->Delete(write_options, key);
      finishedSingleOp(key.size() + value.size());
      assert(status.ok());
    }
    stop("rocksdb_remove");
    delete rocksdb_;
    rocksdb_ = nullptr;
  }

  void stop(std::string const& operation_name) {
    end_time_ = std::chrono::high_resolution_clock::now();
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "[" << operation_name << "]");
    LOG4CXX_INFO(logger, getpid() << ":"
                                  << "Operation Count: " << FLAGS_num);
    if (operation_name.find("open") != std::string::npos) {
      LOG4CXX_INFO(logger, getpid() << ":"
                                    << "Elapsed Time: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end_time_ - start_time_).count() << " ns");
    } else {
      std::chrono::duration<double> elapsed_time = end_time_ - start_time_;
      LOG4CXX_INFO(logger, getpid() << ":"
                                    << "Elapsed Time: " << elapsed_time.count() << " s");
      LOG4CXX_INFO(logger, getpid() << ":"
                                    << "Throughput: " << (static_cast<int64_t>(FLAGS_key_size + FLAGS_value_size) * FLAGS_num) / 1048576.0 / elapsed_time.count() << " MB/s");
      LOG4CXX_INFO(logger, getpid() << ":"
                                    << "Throughput: " << (FLAGS_num) / elapsed_time.count() << " ops/s");
      if (FLAGS_histogram) {
        LOG4CXX_INFO(logger, getpid() << ":" << "Microseconds per op:" << std::endl
                                      << histogram.ToString().c_str() << std::endl);
      }
    }
    LOG4CXX_INFO(logger, getpid() << ":");
  }

  void start() {
    start_time_ = std::chrono::high_resolution_clock::now();
    last_op_finish_ = start_time_;
    histogram.Clear();
  }

  void printEnvironment() {
    std::fprintf(stderr, "SQLite:       version %s\n", SQLITE_VERSION);
    std::fprintf(stderr, "OzoneDB:      version %s\n", "1.0");
    // std::fprintf(stderr, "PebblesDB:    version %d.%d\n",
    //              leveldb::kMajorVersion, leveldb::kMinorVersion);
    std::fprintf(stderr, "Rocksdb:      version %d.%d\n",
                 rocksdb::kMajorVersion, rocksdb::kMinorVersion);
  }

  void printWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    std::fprintf(
        stdout,
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    std::fprintf(
        stdout,
        "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
  }

  void printHeader() {
    printEnvironment();
    std::fprintf(stdout, "Keys:       %d bytes each\n", FLAGS_key_size);
    std::fprintf(stdout, "Values:     %d bytes each\n", FLAGS_value_size);
    std::fprintf(stdout, "Entries:    %d\n", FLAGS_num);
    std::fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                 ((static_cast<int64_t>(FLAGS_key_size + FLAGS_value_size) * FLAGS_num) /
                  1048576.0));
    printWarnings();
    std::fprintf(stdout, "------------------------------------------------\n");
  }

 public:
  void run(int op_count) {
    printHeader();
    FLAGS_num = op_count;
    LOG4CXX_INFO(logger, getpid() << ": Op Count: " << FLAGS_num);
      
    char const* benchmarks = FLAGS_benchmarks;
    char const* db = FLAGS_db;
    std::vector<std::string> db_list;
    while (db != nullptr) {
      char const* sep = strchr(db, ',');
      std::string name;
      if (sep == nullptr) {
        name = db;
        db = nullptr;
      } else {
        name = std::string(db, sep - db);
        db = sep + 1;
      }
      db_list.push_back(name);
    }

    while (benchmarks != nullptr) {
      char const* sep = strchr(benchmarks, ',');
      std::string name;
      if (sep == nullptr) {
        name = benchmarks;
        benchmarks = nullptr;
      } else {
        name = std::string(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      bool write_sync = false;
      if (name == std::string("fillseqsync")) {
        workload = load_workload(SEQUENTIAL, FLAGS_num);
        write_sync = true;
        for (auto const& db : db_list) {
          if (db == std::string("sqlite")) {
            sqlitePut(write_sync, FRESH);
          } else if (db == std::string("pebblesdb")) {
            // pebblesdbPut(write_sync, FRESH);
          } else if (db == std::string("rocksdb")) {
            rocksdbPut(write_sync, FRESH);
          } else if (db == std::string("ozonedb")) {
            DBState state = FRESH;
            if (FLAGS_num_clients > 1) {
              state = EXISTING;
            }
            ozonedbPut(write_sync, state);
          } else {
            std::fprintf(stderr, "unknown db '%s'\n", db.c_str());
          }
        }
      } else if (name == std::string("readseq")) {
        workload = load_workload(SEQUENTIAL, FLAGS_reads);
        for (auto const& db : db_list) {
          if (db == std::string("sqlite")) {
            sqliteGet();
          } else if (db == std::string("pebblesdb")) {
            // pebblesdbGet();
          } else if (db == std::string("rocksdb")) {
            rocksdbGet();
          } else if (db == std::string("ozonedb")) {
            ozonedbGet();
          } else {
            std::fprintf(stderr, "unknown db '%s'\n", db.c_str());
          }
        }
      } else if (name == std::string("readran")) {
        workload = load_workload(RANDOM, FLAGS_reads);
        for (auto const& db : db_list) {
          if (db == std::string("sqlite")) {
            sqliteGet();
          } else if (db == std::string("pebblesdb")) {
            // pebblesdbGet();
          } else if (db == std::string("rocksdb")) {
            rocksdbGet();
          } else if (db == std::string("ozonedb")) {
            ozonedbGet();
          } else {
            std::fprintf(stderr, "unknown db '%s'\n", db.c_str());
          }
        }
      } else if (name == std::string("deleseqsync")) {
        workload = load_workload(SEQUENTIAL, FLAGS_num);
        write_sync = true;
        for (auto const& db : db_list) {
          if (db == std::string("sqlite")) {
            sqliteRemove(write_sync);
          } else if (db == std::string("pebblesdb")) {
            // pebblesdbRemove(write_sync);
          } else if (db == std::string("rocksdb")) {
            rocksdbRemove(write_sync);
          } else if (db == std::string("ozonedb")) {
            ozonedbRemove(write_sync);
          } else {
            std::fprintf(stderr, "unknown db '%s'\n", db.c_str());
          }
        }
      } else if (name == std::string("open")) {
        for (auto const& db : db_list) {
          if (db == std::string("sqlite")) {
            sqliteOpen();
          } else if (db == std::string("pebblesdb")) {
            // pebblesdbOpen();
          } else if (db == std::string("rocksdb")) {
            rocksdbOpen();
          } else if (db == std::string("ozonedb")) {
            ozonedbOpen();
          } else {
            std::fprintf(stderr, "unknown db '%s'\n", db.c_str());
          }
        }
      } else {
        if (name != std::string()) {  // No error message for empty name
          std::fprintf(stderr, "unknown benchmark '%s'\n",
                       name.c_str());
        }
      }
    }
  }
};

int main(int argc, char** argv) {
  BasicConfigurator::configure();  // Configure the logger
  logger = Logger::getLogger("bench");
  std::string default_db_path = "";
  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (std::string(argv[i]).find("--benchmarks=") != std::string::npos) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--key_size=%d%c", &n, &junk) == 1) {
      FLAGS_key_size = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (sscanf(argv[i], "--num_clients=%d%c", &n, &junk) == 1) {
      FLAGS_num_clients = n;
    } else if (std::string(argv[i]).find("--op_each_clients=") == 0) {
      std::string arg_value = argv[i] + strlen("--op_each_clients=");

      // Ensure the argument starts with '{' and ends with '}'
      if (arg_value.front() == '{' && arg_value.back() == '}') {
        // Remove the curly braces
        arg_value = arg_value.substr(1, arg_value.size() - 2);
        std::stringstream ss(arg_value);
        std::string token;
        std::vector<int> op_each_clients;

        // Split the string by commas
        while (std::getline(ss, token, ',')) {
          try {
            op_each_clients.push_back(std::stoi(token));
          } catch (std::invalid_argument const& e) {
            std::cerr << "Invalid number in --op_each_clients: " << token << std::endl;
            exit(1);
          }
        }

        FLAGS_op_each_clients = op_each_clients;
      } else {
        std::cerr << "Invalid format for --op_each_clients. Use {1,2,3,...}" << std::endl;
        exit(1);
      }
    }

    else {
      std::fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      std::exit(1);
    }
  }
  if (FLAGS_reads < 0) {
    FLAGS_reads = FLAGS_num;
  }
  if (*FLAGS_benchmarks == '\0') {
    std::fprintf(stderr, "no benchmarks specified\n");
    return 0;
  }

  if (*FLAGS_db == '\0') {
    std::fprintf(stderr, "no db specified\n");
    return 0;
  }
  if (FLAGS_num_clients > 1) {
    system("make clean-db");
    Benchmark benchmark;
    auto boundFunction = std::bind(&Benchmark::run, &benchmark, std::placeholders::_1);
    multipleProcessorBenchTester(FLAGS_num_clients, boundFunction, FLAGS_op_each_clients);
  } else {
    Benchmark benchmark;
    benchmark.run(FLAGS_num);
  }
  return 0;
}

/*
int GetFileDescriptor(std::filebuf& filebuf) {
  class my_filebuf : public std::filebuf {
   public:
    int handle() { return _M_file.fd(); }
  };

  return static_cast<my_filebuf&>(filebuf).handle();
}

#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
void write_to_file_with_mmap(std::string const& filename, std::vector<std::pair<std::string, std::string>> workload) {
  size_t file_size = 0;
  for (auto const& [key, value] : workload) {
    file_size += key.size() + value.size() + 2;  // +2 for newline characters
  }

  int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
  if (fd == -1) {
    perror("Error opening file for writing");
    return;
  }

  // Stretch the file to the required size
  if (lseek(fd, file_size - 1, SEEK_SET) == -1) {
    close(fd);
    perror("Error calling lseek() to stretch the file");
    return;
  }

  // Write a dummy byte at the end to stretch the file
  if (write(fd, "", 1) == -1) {
    close(fd);
    perror("Error writing last byte of the file");
    return;
  }

  // Map the file into memory
  char* map = (char*)mmap(0, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    close(fd);
    perror("Error mmapping the file");
    return;
  }

  // Write data to the mapped region
  size_t offset = 0;
  for (auto const& [key, value] : workload) {
    std::string line = key + " " + value + "\n";
    std::copy(line.begin(), line.end(), map + offset);
    offset += line.size();
    // Flush changes to disk
    if (msync(map, file_size, MS_SYNC) == -1) {
      perror("Error msyncing the file");
      munmap(map, file_size);
      close(fd);
      return;
    }
  }

  // Unmap and close the file
  if (munmap(map, file_size) == -1) {
    perror("Error un-mmapping the file");
  }
  close(fd);
}

void test_single_io_mmp() {
  // simply open a file write key,value and close.
  std::string filename = "test_file";
  int operation_count = 1000;
  std::ofstream file(filename, std::ios::binary);
  auto workload = load_workload(true, operation_count);
  auto start_time = std::chrono::high_resolution_clock::now();
  write_to_file_with_mmap(filename, workload);

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_time = end_time - start_time;

  std::cout <<getpid() <<":" << "[mmp Put]" << std::endl;
  std::cout <<getpid() <<":" << "Operation Count: " << operation_count << std::endl;
  std::cout <<getpid() <<":" << "Elapsed Time: " << elapsed_time.count() << " secs" << std::endl;
  std::cout <<getpid() <<":" << "Throughput: " << operation_count / elapsed_time.count() << " operation/s" << std::endl;
  std::cout <<getpid() <<":" << std::endl;
}

void test_get_size_for_file() {
  // create a random file
  std::string filename = "test_file";
#include <filesystem>
  // using fs::file_size count the time
  for (int i = 0; i < 100; i++) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::filesystem::file_size(filename);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::cout <<getpid() <<":" << "get_size_for_file: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count() << " ns" << std::endl;
  }
}

void test_single_io_standard() {
  // simply open a file write key,value and close.
  std::string filename = "test_file";
  std::ofstream file(filename, std::ios::binary);
  auto workload = load_workload(true, 1000);
  auto start_time = std::chrono::high_resolution_clock::now();
  for (auto const& [key, value] : workload) {
    std::string record = key + value;
    file.write(record.c_str(), record.size());
    file.flush();
    fsync(GetFileDescriptor(*file.rdbuf()));
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_time = end_time - start_time;

  std::cout <<getpid() <<":" << "[standardio Put]" << std::endl;
  std::cout <<getpid() <<":" << "Operation Count: " << OPERATION_COUNT << std::endl;
  std::cout <<getpid() <<":" << "Elapsed Time: " << elapsed_time.count() << " secs" << std::endl;
  std::cout <<getpid() <<":" << "Throughput: " << OPERATION_COUNT / elapsed_time.count() << " operation/s" << std::endl;
  std::cout <<getpid() <<":" << std::endl;
  file.close();

  // Test get operation
  std::ifstream read_file(filename, std::ios::binary);
  start_time = std::chrono::high_resolution_clock::now();
  for (auto const& [key, value] : workload) {
    std::string record;
    std::getline(read_file, record);
  }
}

//=============================warm cache=======================================
  void ozonedbWarmCache() {
    ozonedb::DB* db = nullptr;
    std::string shared_config_path = "../src/shared_config.json";
    ozonedb::DB::openDB(db, shared_config_path);
    for (auto const& [key, value] : workload) {
      std::string const* result = nullptr;
      db->get(key, result);
    }
    ozonedb::DB::closeDB(db);
  }

  void sqliteWarmCache() {
    sqlite3* db = nullptr;
    sqlite3_open(FLAGS_sqlite_path, &db);
    sqlite3_stmt* get_stmt;
    std::string get_str = "SELECT value FROM test WHERE key = ?";
    sqlite3_prepare_v2(db, get_str.c_str(), -1, &get_stmt, nullptr);
    for (auto const& [key, value] : workload) {
      sqlite3_bind_text(get_stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_step(get_stmt);
      sqlite3_reset(get_stmt);
    }
    sqlite3_close(db);
  }

  void pebblesdbWarmCache() {
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kNoCompression;
    options.write_buffer_size = FLAGS_write_buffer_size;
    leveldb::DB::Open(options, FLAGS_pebblesdb_path, &db);
    for (auto const& [key, value] : workload) {
      std::string result;
      leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &result);
    }
    delete db;
  }

  void rocksdbWarmCache() {
    rocksdb::DB* db = nullptr;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::kNoCompression;
    options.write_buffer_size = FLAGS_write_buffer_size;
    rocksdb::DB::Open(options, FLAGS_rocksdb_path, &db);
    for (auto const& [key, value] : workload) {
      std::string result;
      rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &result);
    }
    delete db;
  }

*/