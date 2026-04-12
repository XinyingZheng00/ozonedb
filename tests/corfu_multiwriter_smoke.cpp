// Multi-writer smoke test for OzoneDB on a Corfu + S3 backend.
//
// Usage:
//   corfu_multiwriter_smoke <path-to-shared-config.json> [keys_per_writer]
//
// Forks two child processes. Each writes a disjoint key range
// (W0: key_0..key_{N-1}, W1: key_{N}..key_{2N-1}) to the same
// Corfu stream. After both children exit, the parent reopens the DB
// and verifies all 2N keys are readable. Exercises:
//   - Phase 1: concurrent LOGCREATE / tail rollover
//   - Phase 2: cross-writer read fencing (parent sees both writers' data)
//   - Phase 4: distributed compaction claim (if compaction fires)
//   - Phase 0.5: SSTable split to S3 (if sstable_backend is set)
//
// Exits non-zero on any failure. Not a gtest — this is a minimal probe
// you can run by hand or from a script against a live corfu_server +
// optional MinIO instance.
#include "db.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

using namespace ozonedb;

static int run_writer(std::string const& config, int start, int end) {
  DB* db = nullptr;
  if (DB::openDB(db, config) != Status::kSuccess) {
    std::cerr << "[writer " << getpid() << "] openDB failed\n";
    return 2;
  }
  for (int i = start; i < end; ++i) {
    std::string k = "key_" + std::to_string(i);
    std::string v = "value_" + std::to_string(i);
    if (db->put(k, v) != Status::kSuccess) {
      std::cerr << "[writer " << getpid() << "] put failed at i=" << i << "\n";
      DB::closeDB(db);
      return 3;
    }
  }
  DB::closeDB(db);
  return 0;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <shared-config.json> [keys_per_writer=100]\n";
    return 1;
  }
  std::string config = argv[1];
  int keys_per_writer = (argc >= 3) ? std::atoi(argv[2]) : 100;
  int total_keys = keys_per_writer * 2;

  std::cout << "[multiwriter] config: " << config
            << ", keys_per_writer: " << keys_per_writer << "\n";

  auto t0 = std::chrono::steady_clock::now();

  // Fork two writer children with disjoint key ranges.
  pid_t pids[2] = {};
  for (int w = 0; w < 2; ++w) {
    pid_t pid = fork();
    if (pid < 0) {
      std::cerr << "[multiwriter] fork failed\n";
      return 1;
    }
    if (pid == 0) {
      // Child — run writer and exit.
      int start = w * keys_per_writer;
      int end = start + keys_per_writer;
      int rc = run_writer(config, start, end);
      _exit(rc);
    }
    pids[w] = pid;
  }

  // Parent — wait for both children.
  int failures = 0;
  for (int w = 0; w < 2; ++w) {
    int status = 0;
    waitpid(pids[w], &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      std::cerr << "[multiwriter] writer " << w << " (pid " << pids[w]
                << ") exited with status "
                << (WIFEXITED(status) ? WEXITSTATUS(status) : -1) << "\n";
      ++failures;
    }
  }
  if (failures > 0) {
    std::cerr << "[multiwriter] " << failures << " writer(s) failed\n";
    return 4;
  }

  auto t1 = std::chrono::steady_clock::now();
  std::cout << "[multiwriter] both writers finished in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0)
                   .count()
            << " ms\n";

  // Parent reopens and reads back all keys.
  DB* db = nullptr;
  if (DB::openDB(db, config) != Status::kSuccess) {
    std::cerr << "[multiwriter] parent openDB failed\n";
    return 5;
  }

  int ok = 0;
  int missing = 0;
  int mismatched = 0;
  for (int i = 0; i < total_keys; ++i) {
    std::string k = "key_" + std::to_string(i);
    std::string const* v = nullptr;
    if (db->get(k, v) != Status::kSuccess || v == nullptr) {
      if (missing < 10) {
        std::cerr << "[multiwriter] NOT_FOUND: " << k << "\n";
      }
      ++missing;
      continue;
    }
    std::string expected = "value_" + std::to_string(i);
    if (*v != expected) {
      if (mismatched < 10) {
        std::cerr << "[multiwriter] MISMATCH: " << k << " got '" << *v
                  << "' expected '" << expected << "'\n";
      }
      ++mismatched;
      continue;
    }
    ++ok;
  }

  DB::closeDB(db);

  auto t2 = std::chrono::steady_clock::now();
  std::cout << "[multiwriter] read phase: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1)
                   .count()
            << " ms\n";

  if (missing > 0 || mismatched > 0) {
    std::cerr << "[multiwriter] FAIL — ok=" << ok << " missing=" << missing
              << " mismatched=" << mismatched << " / " << total_keys << "\n";
    return 6;
  }

  std::cout << "[multiwriter] PASS — verified " << ok << "/" << total_keys
            << " keys from 2 concurrent writers\n";
  return 0;
}
