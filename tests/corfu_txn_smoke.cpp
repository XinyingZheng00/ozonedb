// Transaction smoke test for OzoneDB on a Corfu backend.
//
// Usage:
//   corfu_txn_smoke <path-to-shared-config.json> [accounts=16] [transfers_per_writer=200]
//
// The config must set track_versions=true. The parent seeds `accounts`
// keys (acct_0..acct_{N-1}) to 100 each with blind puts, then forks two
// writer children. Each child runs `transfers_per_writer` transactions:
// read two random accounts, move a random amount from one to the
// other, commit; kCasConflict retries with a jittered backoff. After
// both children exit the parent reopens the DB and audits the sum in a
// read-only transaction (with validation): it must equal N * 100 and
// the validation record must be accepted. Exercises:
//   - T1: transactions that read blind-written (seeded) values
//   - T3: multi-key validation and atomic two-record apply
//   - read-only validation records
//
// Exits non-zero on any failure. Not a gtest — run by hand against a
// live corfu_server (+ MinIO if sstable_backend is s3).
#include "db.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>

using namespace ozonedb;

static int run_writer(std::string const& config, int writer, int accounts, int transfers) {
  DB* db = nullptr;
  if (DB::openDB(db, config) != Status::kSuccess) {
    std::cerr << "[writer " << writer << "] openDB failed\n";
    return 2;
  }
  std::mt19937 rng(static_cast<unsigned>(getpid()) * 7919u + static_cast<unsigned>(writer));
  std::uniform_int_distribution<int> pick(0, accounts - 1);
  std::uniform_int_distribution<int> amount(1, 10);
  std::uniform_int_distribution<int> jitter(0, 1000);

  long commits = 0, aborts = 0, failures = 0;
  int max_attempts = 0;
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < transfers; ++i) {
    int from = pick(rng);
    int to = pick(rng);
    while (to == from) to = pick(rng);
    int amt = amount(rng);
    std::string kf = "acct_" + std::to_string(from);
    std::string kt = "acct_" + std::to_string(to);
    int attempt = 0;
    for (;;) {
      ++attempt;
      Transaction txn = db->begin();
      if (!txn.isOpen()) {
        std::cerr << "[writer " << writer << "] begin() failed: is track_versions=true?\n";
        DB::closeDB(db);
        return 4;
      }
      std::string vf, vt;
      Status sf = txn.get(kf, vf);
      Status st = txn.get(kt, vt);
      if (sf != Status::kSuccess || st != Status::kSuccess) {
        std::cerr << "[writer " << writer << "] read failed on " << kf << "/" << kt << "\n";
        DB::closeDB(db);
        return 5;
      }
      long bf = std::atol(vf.c_str());
      long bt = std::atol(vt.c_str());
      if (bf < amt) {
        // Nothing to move; a read-only commit still validates the reads.
        int64_t v = -1;
        Status s = txn.commit(v);
        if (s == Status::kSuccess) { ++commits; break; }
        if (s == Status::kCasConflict) { ++aborts; }
        else { ++failures; break; }
      } else {
        txn.put(kf, std::to_string(bf - amt));
        txn.put(kt, std::to_string(bt + amt));
        int64_t v = -1;
        Status s = txn.commit(v);
        if (s == Status::kSuccess) { ++commits; break; }
        if (s == Status::kCasConflict) { ++aborts; }
        else { ++failures; break; }
      }
      if (attempt > max_attempts) max_attempts = attempt;
      int backoff_us = std::min(200 * (1 << std::min(attempt, 6)), 20000) + jitter(rng);
      std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
    }
    if (attempt > max_attempts) max_attempts = attempt;
  }
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0).count();
  std::cout << "[writer " << writer << "] commits=" << commits << " aborts=" << aborts
            << " failures=" << failures << " max_attempts=" << max_attempts
            << " elapsed_ms=" << ms << "\n";
  DB::closeDB(db);
  return failures == 0 ? 0 : 6;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <shared-config.json> [accounts=16] [transfers_per_writer=200]\n";
    return 1;
  }
  std::string config = argv[1];
  int accounts = (argc >= 3) ? std::atoi(argv[2]) : 16;
  int transfers = (argc >= 4) ? std::atoi(argv[3]) : 200;
  if (accounts < 2) accounts = 2;
  long const expected_sum = 100L * accounts;

  std::cout << "[txn] config: " << config << ", accounts: " << accounts
            << ", transfers_per_writer: " << transfers << "\n";

  // Seed with blind puts: the transactions below must read these
  // through the version map (inline value for every tracked write).
  {
    DB* db = nullptr;
    if (DB::openDB(db, config) != Status::kSuccess) {
      std::cerr << "[txn] parent openDB failed (seed)\n";
      return 2;
    }
    for (int i = 0; i < accounts; ++i) {
      if (db->put("acct_" + std::to_string(i), "100") != Status::kSuccess) {
        std::cerr << "[txn] seed put failed at i=" << i << "\n";
        DB::closeDB(db);
        return 3;
      }
    }
    DB::closeDB(db);
  }

  auto t0 = std::chrono::steady_clock::now();
  pid_t pids[2] = {};
  for (int w = 0; w < 2; ++w) {
    pid_t pid = fork();
    if (pid < 0) {
      std::cerr << "[txn] fork failed\n";
      return 1;
    }
    if (pid == 0) {
      _exit(run_writer(config, w, accounts, transfers));
    }
    pids[w] = pid;
  }

  int rc = 0;
  for (int w = 0; w < 2; ++w) {
    int status = 0;
    waitpid(pids[w], &status, 0);
    int code = WIFEXITED(status) ? WEXITSTATUS(status) : 128;
    if (code != 0) {
      std::cerr << "[txn] writer " << w << " exited with " << code << "\n";
      rc = code;
    }
  }
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0).count();
  std::cout << "[txn] writers done in " << ms << " ms\n";
  if (rc != 0) return rc;

  // Audit: one read-only transaction over every account, validated.
  DB* db = nullptr;
  if (DB::openDB(db, config) != Status::kSuccess) {
    std::cerr << "[txn] parent openDB failed (audit)\n";
    return 2;
  }
  long sum = 0;
  int64_t version = -1;
  Status audit;
  {
    Transaction txn = db->begin(/*validate_read_only=*/true);
    for (int i = 0; i < accounts; ++i) {
      std::string v;
      if (txn.get("acct_" + std::to_string(i), v) != Status::kSuccess) {
        std::cerr << "[txn] audit read failed at i=" << i << "\n";
        DB::closeDB(db);
        return 7;
      }
      sum += std::atol(v.c_str());
    }
    audit = txn.commit(version);
  }
  DB::closeDB(db);

  std::cout << "[txn] audit sum=" << sum << " expected=" << expected_sum
            << " validation=" << (audit == Status::kSuccess ? "accepted" : "rejected")
            << " at version " << version << "\n";
  if (sum != expected_sum) {
    std::cerr << "[txn] FAIL: money was created or lost\n";
    return 8;
  }
  if (audit != Status::kSuccess) {
    std::cerr << "[txn] FAIL: read-only validation rejected with no concurrent writer\n";
    return 9;
  }
  std::cout << "[txn] OK\n";
  return 0;
}
