#ifdef OZONEDB_ENABLE_CORFU
#include "log_trimmer.h"
#include <chrono>
#include <iostream>

namespace ozonedb {

LogTrimmer::LogTrimmer(CorfuDBStorage* log, Storage* store, Options options)
    : log_(log), store_(store), options_(std::move(options)) {
  if (options_.keep < 2) options_.keep = 2;
  loadHistory();
}

LogTrimmer::~LogTrimmer() {
  if (started_) stop();
}

void LogTrimmer::loadHistory() {
  // Walk LATEST -> prev_covered_addr -> ... for at most `keep` steps. The
  // walk reads manifests only, never file objects.
  long addr = -1;
  bool found = false;
  if (checkpoint::readLatestAddr(*store_, options_.dir, addr, found) != Status::kSuccess || !found) {
    return;
  }
  std::deque<long> chain;
  for (int i = 0; i < options_.keep && addr >= 0; ++i) {
    chain.push_front(addr);
    checkpoint::State st;
    if (checkpoint::read(*store_, options_.dir, addr, st, /*with_files=*/false) != Status::kSuccess) break;
    addr = st.prev_covered_addr;
  }
  std::lock_guard<std::mutex> lk(history_mtx_);
  history_ = std::move(chain);
  if (!history_.empty()) last_checkpoint_addr_ = history_.back();
}

void LogTrimmer::start() {
  if (started_.exchange(true)) return;
  stop_ = false;
  thread_ = std::thread(&LogTrimmer::loop, this);
}

void LogTrimmer::stop() {
  if (!started_.exchange(false)) return;
  stop_ = true;
  wake_cv_.notify_all();
  if (thread_.joinable()) thread_.join();
  // Final cycle: the process is leaving, so publish what it has.
  runCycle(/*force=*/true);
}

void LogTrimmer::loop() {
  while (!stop_) {
    {
      std::unique_lock<std::mutex> lk(wake_mtx_);
      wake_cv_.wait_for(lk, std::chrono::milliseconds(options_.interval_ms),
                        [this] { return stop_.load(); });
    }
    if (stop_) break;
    runCycle(/*force=*/false);
  }
  log_->detachThread();
}

bool LogTrimmer::runCycle(bool force) {
  std::lock_guard<std::mutex> cycle(cycle_mtx_);
  using clock = std::chrono::steady_clock;
  auto const t0 = clock::now();

  long last = last_checkpoint_addr_.load();
  if (!force) {
    long tail = log_->streamTail();
    if (tail < 0) return false;
    if (static_cast<uint64_t>(tail - last) < options_.min_entries) return false;
  }

  checkpoint::State snap = log_->takeSnapshot();
  auto const t1 = clock::now();
  if (snap.covered_addr < 0) {
    std::cerr << "[trimmer] snapshot unavailable (tailer behind own writes?), skipping cycle\n";
    return false;
  }
  if (snap.covered_addr <= last) return false;  // nothing new since the last checkpoint
  snap.prev_covered_addr = last;
  snap.creator = options_.creator;

  if (checkpoint::write(*store_, options_.dir, snap) != Status::kSuccess) {
    std::cerr << "[trimmer] checkpoint " << snap.covered_addr
              << " failed to write; log NOT trimmed\n";
    return false;
  }
  auto const t2 = clock::now();
  last_checkpoint_addr_ = snap.covered_addr;
  {
    std::lock_guard<std::mutex> lk(history_mtx_);
    history_.push_back(snap.covered_addr);
  }

  // Trim behind the previous checkpoint (the grace rule, see the header).
  long trim_mark = -1;
  if (last >= 0) {
    if (log_->prefixTrim(last)) {
      last_trim_addr_ = last;
      trim_mark = log_->trimMark();
    } else {
      std::cerr << "[trimmer] prefixTrim(" << last << ") failed; will retry next cycle\n";
    }
  }
  auto const t3 = clock::now();

  // Retention. Only checkpoints below the trim mark's own checkpoint are
  // ever removed, so a joiner reading LATEST always finds its objects.
  std::deque<long> to_delete;
  {
    std::lock_guard<std::mutex> lk(history_mtx_);
    while (static_cast<int>(history_.size()) > options_.keep) {
      to_delete.push_back(history_.front());
      history_.pop_front();
    }
  }
  for (long addr : to_delete) checkpoint::remove(*store_, options_.dir, addr);

  auto ms = [](clock::time_point a, clock::time_point b) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count();
  };
  std::cerr << "[trimmer] checkpoint C=" << snap.covered_addr
            << " prev=" << snap.prev_covered_addr
            << " files=" << snap.files.size()
            << " live_MB=" << (snap.liveBytes() >> 20)
            << " sealed=" << snap.sealed.size()
            << " removed=" << snap.removed.size()
            << " snapshot_ms=" << ms(t0, t1)
            << " upload_ms=" << ms(t1, t2)
            << " trim_ms=" << ms(t2, t3)
            << " trimmed_to=" << last
            << " trim_mark=" << trim_mark
            << " deleted=" << to_delete.size()
            << (force ? " final" : "") << "\n";
  return true;
}

size_t LogTrimmer::checkpointsKept() {
  std::lock_guard<std::mutex> lk(history_mtx_);
  return history_.size();
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
