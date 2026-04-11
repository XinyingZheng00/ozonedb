// Standalone end-to-end smoke test for OzoneDB on a Corfu backend.
//
// Usage:
//   corfu_smoke <path-to-shared-config.json> [num_keys]
//
// Opens the DB, does num_keys put()s, closes, reopens, and get()s each key
// back to verify durability across an open/close cycle. Exits non-zero on
// any failure. Not a gtest — this is a minimal probe you can run by hand
// or from a script against a live corfu_server.
#include "db.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

using namespace ozonedb;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <shared-config.json> [num_keys=20]\n";
    return 1;
  }
  std::string config = argv[1];
  int num_keys = (argc >= 3) ? std::atoi(argv[2]) : 20;

  std::cout << "[smoke] opening DB with config: " << config << "\n";
  DB* db = nullptr;
  auto t0 = std::chrono::steady_clock::now();
  if (DB::openDB(db, config) != Status::kSuccess) {
    std::cerr << "[smoke] openDB failed\n";
    return 2;
  }
  auto t1 = std::chrono::steady_clock::now();
  std::cout << "[smoke] opened in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count()
            << " ms\n";

  std::cout << "[smoke] putting " << num_keys << " keys\n";
  for (int i = 0; i < num_keys; ++i) {
    std::string k = "key_" + std::to_string(i);
    std::string v = "value_" + std::to_string(i);
    if (db->put(k, v) != Status::kSuccess) {
      std::cerr << "[smoke] put failed at i=" << i << "\n";
      return 3;
    }
  }
  DB::closeDB(db);
  db = nullptr;
  std::cout << "[smoke] closed after writes\n";

  std::cout << "[smoke] reopening and reading keys back\n";
  if (DB::openDB(db, config) != Status::kSuccess) {
    std::cerr << "[smoke] reopenDB failed\n";
    return 4;
  }
  int ok = 0;
  for (int i = 0; i < num_keys; ++i) {
    std::string k = "key_" + std::to_string(i);
    std::string const* v = nullptr;
    if (db->get(k, v) != Status::kSuccess || v == nullptr) {
      std::cerr << "[smoke] get failed for " << k << "\n";
      return 5;
    }
    std::string expected = "value_" + std::to_string(i);
    if (*v != expected) {
      std::cerr << "[smoke] mismatch for " << k << ": got '" << *v
                << "' expected '" << expected << "'\n";
      return 6;
    }
    ++ok;
  }
  DB::closeDB(db);
  std::cout << "[smoke] PASS — verified " << ok << "/" << num_keys
            << " keys across reopen\n";
  return 0;
}
