#ifndef OZONEDB_LOG_TRIMMER_H
#define OZONEDB_LOG_TRIMMER_H
#ifdef OZONEDB_ENABLE_CORFU
#include "checkpoint.h"
#include "corfu_storage.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

namespace ozonedb {

/**
 * @brief Writes checkpoints of the shared log to the object store and trims
 * the Corfu stream behind them.
 *
 * One process per cluster runs a trimmer (Metadata::log_trim_enabled). Two
 * trimmers are safe -- every checkpoint is self-contained and prefixTrim is
 * monotone -- but wasteful.
 *
 * Cycle N (runCycle):
 *   1. takeSnapshot() -> C_N, the exact state at address C_N (see
 *      CorfuDBStorage::takeSnapshot for why that needs the write gate).
 *   2. checkpoint::write: file objects, manifest, then LATEST = C_N.
 *   3. prefixTrim(C_{N-1}): trim behind the PREVIOUS checkpoint, never the
 *      new one. A live member whose tailer lags by less than one cycle is
 *      then never below the trim mark, and a joiner reads LATEST, which is a
 *      full cycle above the mark.
 *   4. Delete checkpoints older than the newest `keep`.
 *
 * stop() joins the thread and runs one more cycle, so a process that exits
 * leaves the log as short as it can. That final cycle is what makes the
 * benchmark's load snapshot small.
 */
class LogTrimmer {
 public:
  struct Options {
    std::string dir = "checkpoint";     // key prefix inside the object store
    uint64_t interval_ms = 30000;       // cycle period
    uint64_t min_entries = 100000;      // entries since the last checkpoint before a cycle runs
    int keep = 2;                       // checkpoints kept; never below 2
    std::string creator;                // writer fingerprint, recorded in the manifest
  };

  LogTrimmer(CorfuDBStorage* log, Storage* store, Options options);
  ~LogTrimmer();

  void start();
  // Stops the thread, then runs one final cycle (force = true).
  void stop();

  // One cycle. force skips the min_entries check. Returns true when a new
  // checkpoint was written. Safe to call from tests without start().
  bool runCycle(bool force);

  long lastCheckpointAddr() const { return last_checkpoint_addr_.load(); }
  long lastTrimAddr() const { return last_trim_addr_.load(); }
  size_t checkpointsKept();

 private:
  void loop();
  // Seed history_ from LATEST and the prev_covered_addr chain, so a
  // restarted trimmer continues the chain and its retention.
  void loadHistory();

  CorfuDBStorage* log_;
  Storage* store_;
  Options options_;

  std::mutex cycle_mtx_;  // one cycle at a time (loop vs. stop)
  std::mutex history_mtx_;
  std::deque<long> history_;  // covered addresses of kept checkpoints, oldest first
  std::atomic<long> last_checkpoint_addr_{-1};
  std::atomic<long> last_trim_addr_{-1};

  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::atomic<bool> started_{false};
  std::mutex wake_mtx_;
  std::condition_variable wake_cv_;
};

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
#endif  // OZONEDB_LOG_TRIMMER_H
