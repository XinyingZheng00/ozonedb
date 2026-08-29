#ifdef OZONEDB_ENABLE_CORFU
#include "corfu_storage.h"
#include "protobuf/record.pb.h"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <random>
#include <stdexcept>

namespace ozonedb {

namespace {
constexpr int kPollTimeoutMs = 100;
// Max entries per pollBatch client round-trip. Bigger amortizes the
// per-call fixed overhead (one JNI crossing + one byte[] copy on the JNI
// client) over more entries, smaller bounds the time we spend out of the
// running_ check. 256 is a compromise: with a warm Corfu cache
// (~1 ms/entry) one batch takes ~256 ms worst case; with a cold cache
// (50-400 ms/entry) the client returns partial batches.
constexpr int kPollBatchSize = 256;
// Batch size for the open-time replay (drainInitialEntries). Larger than
// the tailer's: the replay is a bulk read of a log that already exists,
// nothing waits on per-batch latency, and the per-batch cost (one JNI
// round trip + one byte[] copy) is what made the old one-entry-per-call
// drain take ~43 us per entry — 49 s for a 1 M-entry loaded log.
constexpr int kDrainBatchSize = 4096;

CorfuClientOptions legacyOptions(std::string const& endpoint,
                                 std::string const& jar_path,
                                 std::string const& jvm_opts,
                                 std::string const& stream_name) {
  CorfuClientOptions o;
  o.endpoint = endpoint;
  o.jar_path = jar_path;
  o.jvm_opts = jvm_opts;
  o.stream_name = stream_name;
  o.client = "jni";
  return o;
}
}  // namespace

CorfuDBStorage::CorfuDBStorage(std::string const& endpoint,
                               std::string const& jar_path,
                               std::string const& jvm_opts,
                               std::string const& stream_name,
                               std::string const& db_path,
                               Storage* checkpoint_store,
                               std::string const& checkpoint_dir,
                               bool fast_ack)
    : CorfuDBStorage(legacyOptions(endpoint, jar_path, jvm_opts, stream_name),
                     db_path, checkpoint_store, checkpoint_dir, fast_ack) {}

CorfuDBStorage::CorfuDBStorage(CorfuClientOptions const& client_options,
                               std::string const& db_path,
                               Storage* checkpoint_store,
                               std::string const& checkpoint_dir,
                               bool fast_ack)
    : Storage(db_path), client_name_(client_options.client), fast_ack_(fast_ack) {
  {
    std::random_device rd;
    std::mt19937_64 rng(rd());
    client_id_ = rng();
    if (client_id_ == 0) client_id_ = 1;  // reserve 0 as "unset / legacy entry"
  }
  client_ = makeCorfuClient(client_options);
  last_commited_time_ = std::chrono::system_clock::now();
  // Synchronously bring local state up to the stream tail before returning
  // so reads issued immediately after construction (e.g. by DB::openDB ->
  // rollForwardMetadataLog) see prior state: newest checkpoint first when
  // a store is given, then the entries above it.
  bootstrap(checkpoint_store, checkpoint_dir);
  running_ = true;
  tailer_thread_ = std::thread(&CorfuDBStorage::tailerLoop, this);
  // Dispatch thread must be running by the time the tailer starts
  // enqueueing events. remote_listener_ is installed by LogHandler
  // after this constructor returns, so the tailer won't generate any
  // events until then — either start order is safe.
  dispatch_thread_ = std::thread(&CorfuDBStorage::dispatchLoop, this);
}

CorfuDBStorage::~CorfuDBStorage() {
  // Flush any pending batches so data written via appendInBatch reaches
  // Corfu before we tear down the bridge. Mirrors ~AzureBlobStorage's
  // cached_file drain in storage.h:208.
  {
    std::unique_lock<std::mutex> lock(mtx_);
    if (!cached_file_.empty()) {
      std::vector<std::pair<std::string, std::vector<unsigned char>>> drained;
      drained.reserve(cached_file_.size());
      for (auto& kv : cached_file_) {
        drained.emplace_back(kv.first, std::move(kv.second));
      }
      cached_file_.clear();
      lock.unlock();

      for (auto& kv : drained) {
        long addr = appendEntry(kv.first, ::CorfuEntry_Op_APPEND,
                                kv.second.data(),
                                static_cast<int>(kv.second.size()));
        if (addr < 0) {
          std::cerr << "[corfu] destructor flush failed for " << kv.first << "\n";
        } else {
          publishWrittenAddr(addr);
        }
      }
    }
  }

  running_ = false;
  if (tailer_thread_.joinable()) tailer_thread_.join();
  // Tailer is stopped; no more events will be enqueued. Wake the
  // dispatch thread so it can drain any remaining queued events and
  // observe running_=false to exit.
  dispatch_cv_.notify_all();
  if (dispatch_thread_.joinable()) dispatch_thread_.join();
  {
    uint64_t const n = ack_wait_batches_.load(std::memory_order_relaxed);
    std::cerr << "[corfu] ack: fast=" << fast_acks_.load(std::memory_order_relaxed)
              << " slow=" << n
              << " conflict=" << conflict_aborts_.load(std::memory_order_relaxed)
              << " spurious=" << spurious_conflicts_.load(std::memory_order_relaxed)
              << " slow_by_cause(newseq=" << slow_path_newseq_.load(std::memory_order_relaxed)
              << " overflow=" << slow_path_overflow_.load(std::memory_order_relaxed)
              << " trim=" << slow_path_trim_.load(std::memory_order_relaxed)
              << " stalled=" << slow_path_stalled_.load(std::memory_order_relaxed)
              << " other=" << slow_path_other_.load(std::memory_order_relaxed)
              << ") fast_ack=" << (fast_ack_ ? "on" : "off") << "\n";
    std::cerr << "[corfu] ack-wait: batches=" << n << " avg_us="
              << (n ? ack_wait_us_total_.load(std::memory_order_relaxed) / n : 0)
              << " max_us=" << ack_wait_us_max_.load(std::memory_order_relaxed)
              << " sealed_after_wait=" << sealed_after_wait_.load(std::memory_order_relaxed)
              << "\n";
  }
  client_->close();
}

void CorfuDBStorage::detachThread() {
  if (client_) client_->detachThread();
}

std::string CorfuDBStorage::serializeEntry(std::string const& file_name, int op,
                                           unsigned char const* data, int length) const {
  ::CorfuEntry entry;
  entry.set_file_name(file_name);
  entry.set_op(static_cast<::CorfuEntry_Op>(op));
  entry.set_client_id(client_id_);
  if (data != nullptr && length > 0) {
    entry.set_payload(data, length);
  }
  std::string serialized;
  entry.SerializeToString(&serialized);
  return serialized;
}

long CorfuDBStorage::appendEntry(std::string const& file_name, int op,
                                 unsigned char const* data, int length) {
  std::string serialized = serializeEntry(file_name, op, data, length);
  return static_cast<long>(client_->append(serialized));
}

CorfuDBStorage::CheckedAppend CorfuDBStorage::appendChecked(
    std::string const& file_name, int op,
    unsigned char const* data, int length,
    long snapshot, bool read_key, bool write_key) {
  std::string serialized = serializeEntry(file_name, op, data, length);
  std::string_view key(file_name);
  return client_->appendChecked(serialized, snapshot,
                                read_key ? &key : nullptr,
                                write_key ? &key : nullptr);
}

// SEAL and REMOVE must reach the sequencer's conflict cache, or a peer's
// fast path (submitBatchFast) is blind to them. The snapshot only has to
// pass the sequencer's sanity checks (epoch, trim mark, bootstrap tail,
// cache eviction mark); the tailer's position normally does, and the
// global tail always does.
long CorfuDBStorage::appendKeyed(std::string const& file_name, int op) {
  long snapshot = last_applied_addr_.load(std::memory_order_acquire);
  long last_abort = 0;
  for (int attempt = 0; attempt < 3; ++attempt) {
    CheckedAppend r = appendChecked(file_name, op, nullptr, 0, snapshot,
                                    /*read_key=*/false, /*write_key=*/true);
    if (r.addr >= 0) return static_cast<long>(r.addr);
    last_abort = static_cast<long>(r.abort);
    snapshot = static_cast<long>(client_->globalTail());
    if (snapshot < 0) break;
  }
  std::cerr << "[corfu] keyed op " << op << " of " << file_name
            << " refused by the sequencer (abort=" << last_abort
            << "); not appended\n";
  return -1;
}

void CorfuDBStorage::publishWrittenAddr(long addr) {
  long prev = last_written_addr_.load(std::memory_order_acquire);
  while (addr > prev &&
         !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
  }
}

bool CorfuDBStorage::applyEntry(long addr, unsigned char const* data, size_t len) {
  ::CorfuEntry entry;
  if (!entry.ParseFromArray(data, static_cast<int>(len))) {
    std::cerr << "[corfu] failed to parse entry at addr=" << addr << "\n";
    return false;
  }

  // Stage a remote-append event for the dispatch thread. We do NOT
  // invoke the listener inline here — the listener takes
  // LRUCache::mutex, and inlining that call on the tailer thread
  // creates a deadlock cycle with any foreground op that holds
  // LRUCache::mutex while fencing on this tailer via storage->size().
  // The dispatch thread runs the listener off the tailer's critical
  // path. Local APPENDs are applied but not dispatched: the writer path
  // already updates any listener-visible index itself.
  bool notify = false;
  RemoteOp notify_op = RemoteOp::kAppend;
  std::string notify_file;
  std::vector<unsigned char> notify_payload;
  RemoteAppendListener listener_snapshot;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    std::string const& fn = entry.file_name();
    switch (entry.op()) {
      case ::CorfuEntry_Op_APPEND: {
        // The tailer is the ONLY writer of file_buffers_, for our own
        // APPENDs as much as for peers'. Own entries used to be skipped
        // here because the writer thread spliced its bytes itself, right
        // after the JNI append returned. That put every file buffer in
        // ARRIVAL order rather than address order, and the two differ
        // whenever a peer's earlier entry reaches the tailer after our
        // later one was spliced. Two things broke on that: the seal rule
        // below was applied to own entries against a sealed_at_addr_ that
        // could not yet contain a peer SEAL sequenced just before (the
        // writer acked bytes every peer dropped), and the task-log election
        // read "first claim in the file" as "my claim" in every claimant
        // at once (duplicate compactions, diverging Views). Applying own
        // entries here, in address order, makes file_buffers_ the same
        // byte sequence in every process; submitBatch just waits for the
        // tailer to pass its address and reads the outcome.
        bool const own = entry.has_client_id() && entry.client_id() == client_id_;
        // An APPEND sequenced ABOVE this file's SEAL arrived after the
        // seal closed the file. It belongs to no file. Every process
        // applies this same rule, so the bytes are unreadable everywhere
        // and the writer's kSealed retry cannot duplicate the record.
        {
          auto sit = sealed_at_addr_.find(fn);
          if (sit != sealed_at_addr_.end() && sit->second < addr) {
            dropped_above_seal_.fetch_add(1, std::memory_order_relaxed);
            dropped_above_seal_bytes_.fetch_add(entry.payload().size(), std::memory_order_relaxed);
            break;
          }
        }
        // A straggler behind a REMOVE must not resurrect the file. Names
        // are never reused (log numbers grow, SSTable names carry a
        // timestamp), so an APPEND after the REMOVE is always stale.
        if (removed_files_.count(fn)) break;
        auto& buf = file_buffers_[fn];
        auto const& payload = entry.payload();
        buf.insert(buf.end(), payload.begin(), payload.end());
        // Own entries are not dispatched: the writer path already fed the
        // key index and the record cache with the record it appended.
        if (remote_listener_ && !own) {
          notify = true;
          notify_op = RemoteOp::kAppend;
          notify_file = fn;
          notify_payload.assign(payload.begin(), payload.end());
          listener_snapshot = remote_listener_;
        }
        break;
      }
      case ::CorfuEntry_Op_SEAL: {
        sealed_files_.insert(fn);
        // Keep the LOWEST seal address seen: it is the point after which
        // no append belongs to this file.
        auto sit = sealed_at_addr_.find(fn);
        if (sit == sealed_at_addr_.end() || addr < sit->second) {
          sealed_at_addr_[fn] = addr;
        }
        break;
      }
      case ::CorfuEntry_Op_REMOVE:
        file_buffers_.erase(fn);
        sealed_files_.erase(fn);
        removed_files_.insert(fn);
        sealed_at_addr_.erase(fn);
        // Settle every in-flight batch for this file instead of erasing
        // the list under its writers. Each writer co-owns its Batch by
        // shared_ptr and reads the outcome after its JNI call returns,
        // so it learns the file is gone rather than acking a lost write.
        // The old code erased pending_[fn] while a writer held a bare
        // list iterator into it.
        {
          auto pit = pending_.find(fn);
          if (pit != pending_.end()) {
            for (auto const& batch : pit->second) {
              if (batch->done) continue;
              batch->result = Status::kFailure;
              batch->done = true;
            }
            pending_.erase(pit);
          }
          auto cit = cached_batch_.find(fn);
          if (cit != cached_batch_.end()) {
            if (!cit->second->done) {
              cit->second->result = Status::kFailure;
              cit->second->done = true;
            }
            cached_batch_.erase(cit);
          }
          cached_file_.erase(fn);
        }
        if (remote_listener_) {
          notify = true;
          notify_op = RemoteOp::kRemove;
          notify_file = fn;
          listener_snapshot = remote_listener_;
        }
        break;
      case ::CorfuEntry_Op_TRIM: {
        // Bookkeeping only: no file changes. Own entries included (the
        // trimmer's own process must see its marker too), max is idempotent.
        long x = -1;
        try {
          x = std::stol(entry.payload());
        } catch (std::exception const&) {
          std::cerr << "[corfu] TRIM entry at " << addr << " has no address payload\n";
        }
        if (x > trim_marker_addr_) trim_marker_addr_ = x;
        break;
      }
    }
    last_applied_addr_.store(addr, std::memory_order_release);
    // Enqueue while still holding mtx_ so "listener check" and
    // "event enqueue" are atomic with respect to
    // setRemoteAppendListener. Without this, a concurrent clear
    // could run between the check above and the push below — the
    // listener would be cleared, then this enqueue would slip in
    // with a snapshot of the stale listener and leak past the
    // drainDispatchQueue() call in setRemoteAppendListener, causing
    // a use-after-free at LogHandler teardown. dispatch_mtx_ is
    // only ever taken here and in dispatchLoop/drainDispatchQueue,
    // none of which ever take mtx_, so no cycle.
    if (notify && listener_snapshot) {
      std::lock_guard<std::mutex> dlk(dispatch_mtx_);
      dispatch_queue_.push_back(RemoteEvent{
          notify_op, std::move(notify_file), std::move(notify_payload),
          std::move(listener_snapshot), addr});
    }
  }
  tailer_cv_.notify_all();
  // A SEAL or REMOVE can settle batches that writers are blocked on.
  batch_flushed_cv_.notify_all();
  if (notify) dispatch_cv_.notify_one();
  return true;
}

CorfuDBStorage::DrainResult CorfuDBStorage::drainInitialEntries() {
  // Same batch path as the tailer (pollBatch + applyEntry). A zero
  // timeout makes pollBatch report kIdle as soon as the stream has
  // nothing ready and the batch is empty, i.e. at the current tail.
  using clock = std::chrono::steady_clock;
  auto const t0 = clock::now();
  long drained = 0;
  long batches = 0;
  long long stream_bytes = 0;  // raw entry bytes: every entry, live or dead
  long long total_us = 0;      // inside pollBatch, sink included
  long long apply_us = 0;      // inside the sink (protobuf parse + apply under mtx_)
  bool trimmed = false;
  bool failed = false;
  auto sink = [&](int64_t addr, unsigned char const* data, size_t len) {
    auto const ta = clock::now();
    stream_bytes += static_cast<long long>(len);
    if (applyEntry(static_cast<long>(addr), data, len)) ++drained;
    apply_us += std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - ta).count();
  };
  while (true) {
    auto const tp = clock::now();
    CorfuClient::Poll r = client_->pollBatch(0, kDrainBatchSize, sink);
    total_us += std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - tp).count();
    if (r == CorfuClient::Poll::kError) {
      // A client failure other than a trimmed address (that one is the
      // marker below). The state is partial; say so.
      failed = true;
      break;
    }
    if (r == CorfuClient::Poll::kIdle) break;
    if (r == CorfuClient::Poll::kTrimmed) {
      // The next address is below the trim mark. Nothing applied from
      // this call.
      trimmed = true;
      break;
    }
    ++batches;
  }
  long long const poll_us = total_us - apply_us;  // the client's own share
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - t0).count();
  // Live fraction: what a checkpoint would carry. Top-level files (no '/'
  // in the name: metadata.log, task.log) are listed by name because they
  // grow across checkpoints and their size is the one to watch.
  size_t live_files = 0, live_bytes = 0;
  std::string top_level;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    live_files = file_buffers_.size();
    for (auto const& kv : file_buffers_) {
      live_bytes += kv.second.size();
      if (kv.first.find('/') == std::string::npos) {
        top_level += (top_level.empty() ? "" : ",") + kv.first + ":" +
                     std::to_string(kv.second.size());
      }
    }
  }
  std::cerr << "[corfu] initial replay (" << client_name_ << ") drained " << drained << " entries in "
            << batches << " batches, " << ms << " ms"
            << (ms > 0 ? " (" + std::to_string(drained * 1000 / ms) + " entries/s)" : "")
            << "; poll " << poll_us / 1000 << " ms, apply " << apply_us / 1000 << " ms"
            << "; stream_MB=" << (stream_bytes >> 20)
            << " live_files=" << live_files
            << " live_MB=" << (live_bytes >> 20)
            << " top_level={" << top_level << "}"
            << " applied_addr=" << last_applied_addr_.load(std::memory_order_acquire)
            << " dropped_above_seal=" << dropped_above_seal_.load(std::memory_order_relaxed)
            << " (" << (dropped_above_seal_bytes_.load(std::memory_order_relaxed) >> 10) << " KB)"
            << (trimmed ? " TRIMMED" : "") << (failed ? " ERROR" : "") << "\n";
  if (failed) return DrainResult::kError;
  return trimmed ? DrainResult::kTrimmed : DrainResult::kOk;
}

void CorfuDBStorage::resetStateLocked() {
  file_buffers_.clear();
  sealed_files_.clear();
  sealed_at_addr_.clear();
  removed_files_.clear();
  last_applied_addr_.store(-1, std::memory_order_release);
  loaded_from_checkpoint_ = false;
  trim_marker_addr_ = -1;
}

long CorfuDBStorage::appendTrimMarker(long addr) {
  std::string payload = std::to_string(addr);
  return appendEntry(".trim", ::CorfuEntry_Op_TRIM,
                     reinterpret_cast<unsigned char const*>(payload.data()),
                     static_cast<int>(payload.size()));
}

void CorfuDBStorage::restoreSnapshot(checkpoint::State&& state) {
  std::lock_guard<std::mutex> lk(mtx_);
  file_buffers_ = std::move(state.files);
  for (auto const& kv : state.sealed) {
    sealed_files_.insert(kv.first);
    if (kv.second >= 0) sealed_at_addr_[kv.first] = kv.second;
  }
  removed_files_ = std::move(state.removed);
  last_applied_addr_.store(state.covered_addr, std::memory_order_release);
  loaded_from_checkpoint_ = true;
}

bool CorfuDBStorage::seekPollView(long addr) {
  return client_->seek(addr);
}

// Constructor-time replay (PLAN-trimming.md §3.5).
//
// 1. Load the newest checkpoint C from the store, when there is one, and
//    seek the poll view to C + 1. Without a store, or without a
//    checkpoint, start at 0.
// 2. Drain to the tail. A TRIMMED marker means the trim mark passed our
//    start address while we were loading (two trim cycles ran); reload
//    the newest checkpoint and try again, a bounded number of times.
// 3. Two completeness checks, both per stream and both restart-proof:
//    a. A checkpoint C must not be AHEAD of the log: the stream tail the
//       sequencer reports must be >= C. Otherwise the log is an older
//       copy than the bucket (or a fresh log), and new appends would land
//       BELOW the poll view's position, invisible to this process forever.
//    b. The replay must not cross a TRIM entry with X >= start. The
//       trimmer appends TRIM(X) to the stream right before prefixTrim(X),
//       so a process that replays from S and meets X >= S has lost part
//       of its history: no checkpoint store, a wiped bucket, or a bucket
//       older than the log dir. The log unit's own trim mark is not used
//       here: it is global, so a fresh stream on a server trimmed for
//       another stream would be refused for nothing; and the sequencer's
//       per-stream mark is in memory only.
void CorfuDBStorage::bootstrap(Storage* checkpoint_store, std::string const& checkpoint_dir) {
  constexpr int kMaxAttempts = 3;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    {
      std::lock_guard<std::mutex> lk(mtx_);
      resetStateLocked();
    }
    long start = 0;  // first address this replay covers
    if (checkpoint_store != nullptr) {
      checkpoint::State state;
      bool found = false;
      Status s = checkpoint::readLatest(*checkpoint_store, checkpoint_dir, state, found);
      if (s != Status::kSuccess) {
        throw std::runtime_error("[corfu] bootstrap: the checkpoint store holds a LATEST "
                                 "that cannot be read (dir " + checkpoint_dir + ")");
      }
      if (found) {
        long tail = streamTail();
        if (tail < state.covered_addr) {
          throw std::runtime_error(
              "[corfu] bootstrap: checkpoint C=" + std::to_string(state.covered_addr) +
              " is ahead of the log (stream tail " + std::to_string(tail) +
              "): the log dir is older than the bucket, or was restored empty");
        }
        start = state.covered_addr + 1;
        std::cerr << "[corfu] restoring checkpoint C=" << state.covered_addr
                  << " files=" << state.files.size()
                  << " live_MB=" << (state.liveBytes() >> 20)
                  << " sealed=" << state.sealed.size()
                  << " removed=" << state.removed.size()
                  << " stream_tail=" << tail << "\n";
        restoreSnapshot(std::move(state));
      }
    }
    if (!seekPollView(start)) {
      throw std::runtime_error("[corfu] bootstrap: seekPollView(" + std::to_string(start) + ") failed");
    }
    DrainResult drained = drainInitialEntries();
    if (drained == DrainResult::kError) {
      throw std::runtime_error("[corfu] bootstrap: the replay from address " +
                               std::to_string(start) + " failed with a Corfu error; refusing "
                               "to serve a partial state");
    }
    if (drained == DrainResult::kTrimmed) {
      std::cerr << "[corfu] bootstrap: replay from " << start
                << " hit the trim mark (attempt " << attempt + 1 << " of "
                << kMaxAttempts << "); reloading the newest checkpoint\n";
      continue;
    }
    long marker = -1;
    {
      std::lock_guard<std::mutex> lk(mtx_);
      marker = trim_marker_addr_;
    }
    if (marker >= start) {
      throw std::runtime_error(
          "[corfu] bootstrap: the stream records a trim of every entry up to address " +
          std::to_string(marker) + " but this replay started at " + std::to_string(start) +
          ": no checkpoint covers the trimmed prefix (checkpoint store " +
          (checkpoint_store ? "present, dir " + checkpoint_dir : std::string("absent")) + ")");
    }
    return;
  }
  throw std::runtime_error("[corfu] bootstrap: the trim mark passed the newest checkpoint " +
                           std::to_string(kMaxAttempts) + " times in a row");
}

void CorfuDBStorage::failStop(char const* why) {
  long mark = trimMark();
  std::cerr << "[corfu] FAIL-STOP: " << why
            << " applied=" << last_applied_addr_.load(std::memory_order_acquire)
            << " written=" << last_written_addr_.load(std::memory_order_acquire)
            << " trim_mark=" << mark
            << ". This process fell more than one trim cycle behind the log; "
               "every write now fails and every read misses. Restart it.\n";
  {
    std::lock_guard<std::mutex> lk(mtx_);
    trimmed_out_.store(true);
    for (auto& kv : pending_) {
      for (auto const& batch : kv.second) {
        if (batch->done) continue;
        batch->result = Status::kFailure;
        batch->done = true;
      }
    }
    pending_.clear();
    for (auto& kv : cached_batch_) {
      if (kv.second->done) continue;
      kv.second->result = Status::kFailure;
      kv.second->done = true;
    }
    cached_batch_.clear();
    cached_file_.clear();
  }
  tailer_cv_.notify_all();
  batch_flushed_cv_.notify_all();
}

checkpoint::State CorfuDBStorage::takeSnapshot() {
  checkpoint::State state;
  // Exclusive gate: no local batch is between "sequenced" and "stamped"
  // (see write_gate_). Writers queue behind us for the copy only.
  std::unique_lock<std::shared_mutex> gate(write_gate_);
  std::unique_lock<std::mutex> lock(mtx_);
  if (trimmed_out_.load()) return state;
  // Every local write is already stamped and spliced; the tailer still
  // has to pass them so last_applied_addr_ names a prefix that contains
  // them. Local writes are all sequenced, so this wait is the tailer's
  // lag only, not a wait for new traffic.
  long target = last_written_addr_.load(std::memory_order_acquire);
  constexpr auto kWait = std::chrono::seconds(10);
  bool reached = tailer_cv_.wait_for(lock, kWait, [&] {
    return !running_ || trimmed_out_.load() ||
           last_applied_addr_.load(std::memory_order_acquire) >= target;
  });
  if (!reached || trimmed_out_.load()) return state;
  // Every push into pending_ happens under the shared gate, and its
  // thread settles the batch before it lets go. An unsettled batch here
  // is impossible; refuse rather than guess.
  for (auto const& kv : pending_) {
    for (auto const& batch : kv.second) {
      if (!batch->done) {
        std::cerr << "[corfu] takeSnapshot: unsettled batch under the gate for "
                  << kv.first << " at " << batch->addr << "; refusing\n";
        return state;
      }
    }
  }
  long covered = last_applied_addr_.load(std::memory_order_acquire);
  if (covered < 0) return state;
  state.covered_addr = covered;
  state.files = file_buffers_;
  for (auto const& name : sealed_files_) {
    auto sit = sealed_at_addr_.find(name);
    state.sealed[name] = sit == sealed_at_addr_.end() ? -1 : sit->second;
  }
  state.removed = removed_files_;
  return state;
}

bool CorfuDBStorage::prefixTrim(long addr) {
  if (addr < 0) return false;
  // The marker goes in BEFORE the trim, so no process can observe a
  // trimmed stream without also being able to observe the marker.
  if (appendTrimMarker(addr) < 0) {
    std::cerr << "[corfu] prefixTrim(" << addr << "): TRIM marker append failed; not trimming\n";
    return false;
  }
  return client_->prefixTrim(addr) >= 0;
}

long CorfuDBStorage::trimMark() {
  return static_cast<long>(client_->trimMark());
}

long CorfuDBStorage::streamTail() {
  return globalFenceTarget();
}

void CorfuDBStorage::tailerLoop() {
  // Diagnostic counters — 1-second rolling window. rate = entries
  // applied per second, gap = how many Corfu addresses behind our
  // own last local write we are. A growing gap means production
  // outpaces the tailer; a frozen `applied` for seconds means the
  // tailer is blocked (likely inside pollView.next()). Leave in
  // place during multi-writer diagnosis; remove once stable.
  auto stat_window_start = std::chrono::steady_clock::now();
  uint64_t stat_entries = 0;
  // Periodically prune the Corfu IStreamView's resolvedQueue/readQueue
  // (TreeSet<Long>). Without this they grow one entry per address the
  // tailer reads, indefinitely — heap dumps from long runs show ~85 MB
  // of TreeMap$Entry attributable to this one stream view. gc() is
  // two-phase (the first call sets the trim mark, the next call prunes
  // below the previously-set mark), so fire it on a steady cadence.
  // kGcTrimInterval chosen so each gc spans many thousands of entries
  // of progress, keeping gc cost amortized and the resident queue size
  // bounded by ~kGcTrimInterval * (applied rate / gc rate).
  uint64_t entries_since_gc = 0;
  constexpr uint64_t kGcTrimInterval = 50000;
  uint64_t applied = 0;  // entries applied by the current pollBatch
  auto sink = [&](int64_t addr, unsigned char const* data, size_t len) {
    if (applyEntry(static_cast<long>(addr), data, len)) ++applied;
  };
  while (running_) {
    applied = 0;
    auto poll_before = std::chrono::steady_clock::now();
    CorfuClient::Poll r = client_->pollBatch(kPollTimeoutMs, kPollBatchSize, sink);
    auto poll_after = std::chrono::steady_clock::now();
    auto poll_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       poll_after - poll_before)
                       .count();
    // Only log unusually-slow polls (>50 ms) so steady-state runs
    // don't drown in output. An idle poll with poll_us ~ 100 ms is
    // just the timeout; log "idle=1" so we can tell at a glance.
    if (poll_us > 50000) {
      std::cerr << "[corfu-tailer] slow pollBatch " << poll_us << "us idle="
                << (r == CorfuClient::Poll::kIdle ? 1 : 0) << "\n";
    }
    if (r == CorfuClient::Poll::kError) {
      continue;
    }
    if (r == CorfuClient::Poll::kTrimmed) {
      // The next address this tailer needs is gone. Skipping it would
      // build a state that no other process has, so stop instead. The
      // thread exits; the destructor's join still returns because
      // running_ is left alone.
      failStop("tailer hit the trim mark");
      break;
    }
    stat_entries += applied;
    entries_since_gc += applied;
    if (entries_since_gc >= kGcTrimInterval) {
      long trim = last_applied_addr_.load(std::memory_order_acquire);
      if (trim >= 0) client_->gc(trim);
      entries_since_gc = 0;
    }
    auto now = std::chrono::steady_clock::now();
    if (now - stat_window_start >= std::chrono::seconds(1)) {
      long applied_now = last_applied_addr_.load(std::memory_order_acquire);
      long written_now = last_written_addr_.load(std::memory_order_acquire);
      // Sample C++-side memory residency. file_buffers_ is the main
      // suspect when process RSS grows without bound — every byte
      // written to every file by every writer lives here until the
      // file is REMOVE'd. Sampling every second is cheap: we only
      // traverse the map of ~hundreds-of-thousands entries at most,
      // all under mtx_.
      size_t n_files = 0, fb_bytes = 0, pend_bytes = 0, cache_bytes = 0;
      size_t dispatch_len = 0;
      {
        std::lock_guard<std::mutex> lk(mtx_);
        n_files = file_buffers_.size();
        for (auto const& kv : file_buffers_) fb_bytes += kv.second.size();
        for (auto const& kv : pending_) {
          for (auto const& batch : kv.second) pend_bytes += batch->payload.size();
        }
        for (auto const& kv : cached_file_) cache_bytes += kv.second.size();
      }
      {
        std::lock_guard<std::mutex> dlk(dispatch_mtx_);
        dispatch_len = dispatch_queue_.size();
      }
      std::cerr << "[corfu-tailer] applied=" << applied_now
                << " written=" << written_now
                << " gap=" << (written_now - applied_now)
                << " rate=" << stat_entries << "/s"
                << " files=" << n_files
                << " fb_MB=" << (fb_bytes >> 20)
                << " pend_MB=" << (pend_bytes >> 20)
                << " cache_MB=" << (cache_bytes >> 20)
                << " disp_q=" << dispatch_len << "\n";
      stat_window_start = now;
      stat_entries = 0;
    }
  }
  detachThread();
}

// Drain remote-append events enqueued by applyEntry and
// invoke the registered listener off the tailer thread. Exits when
// running_ is false and the queue is fully drained — the destructor
// joins the tailer first, so at that point no more events can be
// enqueued and the drain is bounded.
void CorfuDBStorage::dispatchLoop() {
  for (;;) {
    RemoteEvent ev;
    {
      std::unique_lock<std::mutex> lock(dispatch_mtx_);
      dispatch_cv_.wait(lock, [this] {
        return !running_.load() || !dispatch_queue_.empty();
      });
      if (dispatch_queue_.empty()) return;  // !running_ && drained
      ev = std::move(dispatch_queue_.front());
      dispatch_queue_.pop_front();
      dispatch_in_flight_ = true;
    }
    // Invoke the listener outside dispatch_mtx_ so the tailer can
    // continue enqueueing while the listener runs. A throwing
    // listener must not kill the dispatcher — swallow and continue
    // so subsequent events still get delivered.
    if (ev.listener) {
      try {
        ev.listener(ev.file_name, ev.payload.data(), ev.payload.size(),
                    ev.op, ev.addr);
      } catch (std::exception const& e) {
        std::cerr << "[corfu] remote-append listener threw: " << e.what()
                  << "\n";
      } catch (...) {
        std::cerr << "[corfu] remote-append listener threw unknown exception\n";
      }
    }
    // Clear in-flight and notify drain_cv_ only when the queue has
    // gone fully quiescent. A drainer (setRemoteAppendListener with
    // an empty listener) is guaranteed to observe this state before
    // returning.
    {
      std::lock_guard<std::mutex> lock(dispatch_mtx_);
      dispatch_in_flight_ = false;
      if (dispatch_queue_.empty()) drain_cv_.notify_all();
    }
  }
}

// Block until no event is being processed and the queue is empty.
// Called from setRemoteAppendListener when the listener is cleared
// so the caller (typically ~LogHandler) can tear down safely —
// every in-flight event has finished running the old listener and
// no new events will be enqueued (the tailer checks remote_listener_
// under mtx_, which we already cleared).
void CorfuDBStorage::drainDispatchQueue() {
  std::unique_lock<std::mutex> lock(dispatch_mtx_);
  drain_cv_.wait(lock, [this] {
    return dispatch_queue_.empty() && !dispatch_in_flight_;
  });
}

long CorfuDBStorage::globalFenceTarget() {
  // For multi-writer reads: the fence target must include both our own
  // writes (last_written_addr_) AND any remote writer's commits that
  // have already been sequenced. CorfuClient::streamTail() returns the
  // highest address currently in the stream. max() ensures we never
  // regress the target if our own in-flight write hasn't been sequenced
  // yet but a remote write has a higher address.
  long local = last_written_addr_.load(std::memory_order_acquire);
  long global = static_cast<long>(client_->streamTail());
  return std::max(local, global);
}

// Wait until the local tailer has applied everything up to `target`.
//
// BOUNDED, deliberately. The tailer advances only over its OWN stream,
// so a target taken from a global address the stream will never contain
// — another stream's entry, or a hole — is unreachable. An unbounded
// wait here froze every fenced read in the process, permanently and
// silently. A read that gives up returns stale-but-consistent local
// state, which is the same guarantee the default (non-linearizable)
// mode already offers.
void CorfuDBStorage::waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target) {
  constexpr auto kFenceTimeout = std::chrono::seconds(10);
  bool reached = waitForTailerLocked(lock, target, kFenceTimeout);
  if (!reached && !trimmed_out_.load()) {
    std::cerr << "[corfu] fence timed out: target=" << target
              << " applied=" << last_applied_addr_.load(std::memory_order_acquire)
              << " (target is probably not on this stream)\n";
  }
}

bool CorfuDBStorage::waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target,
                                         std::chrono::milliseconds timeout) {
  tailer_cv_.wait_for(lock, timeout, [&] {
    return !running_ || trimmed_out_.load() ||
           last_applied_addr_.load(std::memory_order_acquire) >= target;
  });
  return last_applied_addr_.load(std::memory_order_acquire) >= target;
}

thread_local CorfuDBStorage::FenceToken CorfuDBStorage::fence_token_;
thread_local long CorfuDBStorage::last_append_addr_ = -1;

long CorfuDBStorage::fenceTargetForCaller() {
  if (fence_token_.instance == this) return fence_token_.target;
  return globalFenceTarget();
}

// The one fence of a strict get (see Storage::sync). Sequencer query +
// tailer wait happen here once; subsequent fenced ops on this thread
// reuse the recorded target, so their waitForTailerLocked is satisfied
// immediately — everything sequenced before the fence is already in
// the local buffers, and linearizability doesn't require observing
// anything sequenced after it.
void CorfuDBStorage::sync() {
  long target = globalFenceTarget();
  {
    std::unique_lock<std::mutex> lock(mtx_);
    waitForTailerLocked(lock, target);
  }
  fence_token_ = {this, target};
}

void CorfuDBStorage::clearSync() {
  fence_token_ = {nullptr, -1};
}

void CorfuDBStorage::createDirectory(std::string /*name*/) {
  // No-op: Corfu has no directory concept.
}

bool CorfuDBStorage::sealedBelowLocked(std::string const& fileName, long addr) const {
  auto sit = sealed_at_addr_.find(fileName);
  return sit != sealed_at_addr_.end() && sit->second < addr;
}

std::shared_ptr<CorfuDBStorage::Batch>& CorfuDBStorage::cachedBatchLocked(std::string const& fileName) {
  auto& slot = cached_batch_[fileName];
  if (!slot) slot = std::make_shared<Batch>();
  return slot;
}

std::shared_ptr<CorfuDBStorage::Batch> CorfuDBStorage::takeCachedBatchLocked(std::string const& fileName) {
  auto it = cached_file_.find(fileName);
  if (it == cached_file_.end() || it->second.empty()) return nullptr;
  auto batch = cachedBatchLocked(fileName);
  batch->payload = std::move(it->second);
  cached_file_.erase(it);
  cached_batch_.erase(fileName);
  return batch;
}

// Settle a batch with its final outcome and drop it from pending_.
void CorfuDBStorage::finishBatchLocked(std::string const& fileName,
                                       std::shared_ptr<Batch> const& batch,
                                       Status result) {
  if (batch->done) return;  // a peer REMOVE already settled it
  batch->result = result;
  batch->done = true;
  auto pit = pending_.find(fileName);
  if (pit != pending_.end()) {
    pit->second.remove(batch);
    if (pit->second.empty()) pending_.erase(pit);
  }
}

// FAST PATH (PLAN-trimming.md §0, "fast path"). The slow path below waits
// for the tailer to pass the batch's address because that is the first
// moment it knows whether a SEAL of fileName was sequenced BELOW the
// address. The sequencer can answer that at token time: the token request
// carries key(fileName) in its read set at snapshot S = last_applied_addr_,
// and every SEAL / REMOVE carries the same key in its write set
// (appendKeyed), recorded by the sequencer when their token is issued.
// A granted token at addr therefore proves that no SEAL / REMOVE of
// fileName was tokened in (S, addr]. The tailer applied every entry <= S
// before S was published (applyEntry stores last_applied_addr_ last,
// under mtx_), so sealed_at_addr_ / removed_files_ decide the rest
// exactly. The bytes at addr belong to fileName in every process, and the
// batch is acked before the tailer reaches addr: one token + one write,
// the same cost as before the ack rule. The tailer still applies the
// entry, in address order, as the only writer of file_buffers_; reads
// fence on last_written_addr_, so read-my-writes is unchanged.
//
// A refused token has no address: nothing lands. TX_ABORT_CONFLICT names
// the address T at which the key was last written. Waiting for the tailer
// to reach T then shows either the SEAL / REMOVE (kSealed / kFailure, the
// caller retries on the new tail) or nothing at all — the key's write
// became a hole, its writer retried above T — in which case the append is
// retried with a fresh snapshot. Any other refusal (a snapshot the
// sequencer cannot vouch for: stale epoch, below the tail at sequencer
// bootstrap, below the conflict cache's eviction mark or the trim mark)
// sends the batch to the slow path, which is always correct.
bool CorfuDBStorage::submitBatchFast(std::string const& fileName,
                                     std::shared_ptr<Batch> const& batch) {
  constexpr int kAttempts = 3;
  constexpr auto kConflictWait = std::chrono::seconds(1);
  for (int attempt = 0; attempt < kAttempts; ++attempt) {
    long snapshot;
    {
      std::lock_guard<std::mutex> lk(mtx_);
      if (batch->done) return true;  // a peer REMOVE settled it
      if (sealed_at_addr_.count(fileName)) {
        // The seal is known (own or peer). An append would only land
        // above it and be dropped everywhere: refuse it here.
        finishBatchLocked(fileName, batch, Status::kSealed);
        return true;
      }
      if (removed_files_.count(fileName)) {
        finishBatchLocked(fileName, batch, Status::kFailure);
        return true;
      }
      snapshot = last_applied_addr_.load(std::memory_order_acquire);
    }

    CheckedAppend r = appendChecked(fileName, ::CorfuEntry_Op_APPEND,
                                    batch->payload.data(),
                                    static_cast<int>(batch->payload.size()),
                                    snapshot, /*read_key=*/true, /*write_key=*/false);
    if (r.addr >= 0) {
      publishWrittenAddr(static_cast<long>(r.addr));
      std::lock_guard<std::mutex> lk(mtx_);
      if (batch->done) return true;  // a peer REMOVE settled it meanwhile
      if (removed_files_.count(fileName)) {
        finishBatchLocked(fileName, batch, Status::kFailure);
        return true;
      }
      batch->addr = static_cast<long>(r.addr);
      finishBatchLocked(fileName, batch, Status::kSuccess);
      fast_acks_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }

    if (r.abort != kAbortConflict) {
      switch (r.abort) {
        case kAbortNewSequencer: slow_path_newseq_.fetch_add(1, std::memory_order_relaxed); break;
        case kAbortSeqOverflow: slow_path_overflow_.fetch_add(1, std::memory_order_relaxed); break;
        case kAbortSeqTrim: slow_path_trim_.fetch_add(1, std::memory_order_relaxed); break;
        default: slow_path_other_.fetch_add(1, std::memory_order_relaxed); break;
      }
      return false;
    }

    conflict_aborts_.fetch_add(1, std::memory_order_relaxed);
    std::unique_lock<std::mutex> lock(mtx_);
    bool reached = true;
    if (r.offending >= 0) {
      reached = waitForTailerLocked(lock, static_cast<long>(r.offending), kConflictWait);
    }
    if (batch->done) return true;
    if (sealed_at_addr_.count(fileName)) {
      finishBatchLocked(fileName, batch, Status::kSealed);
      return true;
    }
    if (removed_files_.count(fileName)) {
      finishBatchLocked(fileName, batch, Status::kFailure);
      return true;
    }
    if (!reached) {
      // The tailer did not get to T. The slow path's own append moves
      // the tailer past it, so let that path decide.
      slow_path_stalled_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    // The tailer passed T and fileName is neither sealed nor removed:
    // the key's write at T never landed. Retry with a fresh snapshot.
    spurious_conflicts_.fetch_add(1, std::memory_order_relaxed);
  }
  slow_path_other_.fetch_add(1, std::memory_order_relaxed);
  return false;
}

// Client submit, then wait for the tailer to pass the new address and
// read the outcome under mtx_. mtx_ must NOT be held on entry: the Corfu
// round-trip runs unlocked so it does not block readers, the tailer, or
// writers on other files. The fast path (submitBatchFast) runs first when
// enabled; what follows it is the slow path.
Status CorfuDBStorage::submitBatch(std::string const& fileName,
                                   std::shared_ptr<Batch> const& batch) {
  // The caller holds write_gate_ shared (taken before the batch entered
  // pending_); see the write_gate_ comment in the header.
  if (trimmed_out_.load()) {
    std::lock_guard<std::mutex> lk(mtx_);
    finishBatchLocked(fileName, batch, Status::kFailure);
    return Status::kFailure;
  }
  if (fast_ack_ && submitBatchFast(fileName, batch)) {
    batch_flushed_cv_.notify_all();
    if (batch->result == Status::kSuccess) last_append_addr_ = batch->addr;
    return batch->result;
  }

  // SLOW PATH: a plain token, then wait for the tailer to pass the new
  // address and read the outcome. Taken when the fast path is off, or
  // when the sequencer could not answer for the snapshot (right after a
  // sequencer restart, a trim, or a conflict-cache eviction).
  long addr = appendEntry(fileName, ::CorfuEntry_Op_APPEND,
                          batch->payload.data(),
                          static_cast<int>(batch->payload.size()));

  {
    std::unique_lock<std::mutex> lock(mtx_);
    if (batch->done) {
      // A peer REMOVE settled this batch while the client call was in
      // flight. Do not resurrect it.
    } else if (addr < 0) {
      finishBatchLocked(fileName, batch, Status::kFailure);
    } else {
      // Publish the address first so every fence on this process
      // covers these bytes from here on.
      publishWrittenAddr(addr);

      // ACK RULE. The tailer applies this entry like any peer's, in
      // address order (applyEntryBytes): spliced into file_buffers_, or
      // dropped when a SEAL of fileName was sequenced below addr. So the
      // outcome is known exactly when the tailer has passed addr, and
      // not before — a peer SEAL at S < addr that the tailer has not
      // reached yet is invisible here. The old code spliced and acked
      // right after the JNI return on that partial knowledge; every
      // other process, applying S first, dropped the bytes, and the next
      // compaction of fileName by a peer removed the record for good —
      // 7,407 acked-then-dropped records in a 1M-record 8-writer load,
      // the ~0.6 % read miss. The wait is the tailer's lag, the same
      // wait every fenced read already pays.
      bool const seal_known_before = sealedBelowLocked(fileName, addr);
      auto const wait_start = std::chrono::steady_clock::now();
      waitForTailerLocked(lock, addr);
      auto const wait_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - wait_start)
              .count());
      ack_wait_batches_.fetch_add(1, std::memory_order_relaxed);
      ack_wait_us_total_.fetch_add(wait_us, std::memory_order_relaxed);
      uint64_t prev_max = ack_wait_us_max_.load(std::memory_order_relaxed);
      while (wait_us > prev_max &&
             !ack_wait_us_max_.compare_exchange_weak(prev_max, wait_us, std::memory_order_relaxed)) {
      }
      bool const reached =
          last_applied_addr_.load(std::memory_order_acquire) >= addr;

      if (batch->done) {
        // A peer REMOVE settled this batch during the wait.
      } else if (sealedBelowLocked(fileName, addr)) {
        // Sequenced after the file's SEAL. These bytes belong to no
        // file, and no process will ever read them. Report kSealed so
        // the caller retries on the new tail — a retry cannot duplicate,
        // because this copy is unreadable everywhere. A known seal is
        // decisive whether or not the tailer reached addr.
        if (!seal_known_before) {
          sealed_after_wait_.fetch_add(1, std::memory_order_relaxed);
          std::cerr << "[corfu] submitBatch: SEAL of " << fileName << " at "
                    << sealed_at_addr_[fileName] << " learned while waiting for addr "
                    << addr << " (" << batch->payload.size()
                    << " bytes); retried on the new tail\n";
        }
        finishBatchLocked(fileName, batch, Status::kSealed);
      } else if (trimmed_out_.load() || !reached) {
        // Without the tailer at addr the outcome is unknown, and an ack
        // that may be wrong is worse than a failed put: the caller sees
        // kFailure and does not retry, so nothing is duplicated even if
        // the bytes do become visible later.
        std::cerr << "[corfu] submitBatch: tailer did not reach addr " << addr
                  << " for " << fileName << " (applied "
                  << last_applied_addr_.load(std::memory_order_acquire)
                  << "); append not acked\n";
        finishBatchLocked(fileName, batch, Status::kFailure);
      } else if (removed_files_.count(fileName)) {
        finishBatchLocked(fileName, batch, Status::kFailure);
      } else {
        // The tailer spliced the bytes at addr: readable through
        // file_buffers_ in this process and, in the same order, in every
        // other one.
        batch->addr = addr;
        finishBatchLocked(fileName, batch, Status::kSuccess);
      }
    }
  }
  batch_flushed_cv_.notify_all();
  if (batch->result == Status::kSuccess) last_append_addr_ = batch->addr;
  return batch->result;
}

Status CorfuDBStorage::awaitBatch(std::shared_ptr<Batch> const& batch) {
  std::unique_lock<std::mutex> lock(mtx_);
  batch_flushed_cv_.wait_for(lock, std::chrono::seconds(30),
                             [&] { return batch->done; });
  if (!batch->done) return Status::kFailure;
  if (batch->result == Status::kSuccess) last_append_addr_ = batch->addr;
  return batch->result;
}

// Push every not-yet-sequenced byte of one file onto the shared log and
// wait for the splice. read()/size() call this so they can serve
// file_buffers_ alone — see the read() comment for why that matters.
void CorfuDBStorage::drainForRead(std::string const& fileName) {
  std::shared_lock<std::shared_mutex> gate(write_gate_);  // before any push
  constexpr int kMaxRounds = 64;
  for (int round = 0; round < kMaxRounds; ++round) {
    std::shared_ptr<Batch> to_submit;
    {
      std::unique_lock<std::mutex> lock(mtx_);
      if (removed_files_.count(fileName)) return;
      to_submit = takeCachedBatchLocked(fileName);
      if (to_submit) {
        pending_[fileName].push_back(to_submit);
        last_commited_time_ = std::chrono::system_clock::now();
      } else {
        auto pit = pending_.find(fileName);
        if (pit == pending_.end() || pit->second.empty()) return;
        // Another thread owns the in-flight batches. Wait for them.
        batch_flushed_cv_.wait_for(lock, std::chrono::milliseconds(50));
        continue;
      }
    }
    submitBatch(fileName, to_submit);
  }
}

Status CorfuDBStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  // Synchronous single-payload append: one batch, submitted immediately.
  // submitBatch returns only after the bytes are sequenced AND spliced
  // into file_buffers_, so an append that returns kSuccess is readable.
  std::shared_lock<std::shared_mutex> gate(write_gate_);  // before the push
  auto batch = std::make_shared<Batch>();
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (sealed_files_.count(fileName)) return Status::kSealed;
    if (removed_files_.count(fileName)) return Status::kFailure;
    batch->payload.assign(data, data + length);
    pending_[fileName].push_back(batch);
  }
  return submitBatch(fileName, batch);
}

Status CorfuDBStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  std::lock_guard<std::mutex> lk(mtx_);
  if (sealed_files_.count(fileName)) return Status::kSealed;
  if (removed_files_.count(fileName)) return Status::kFailure;
  auto& buf = cached_file_[fileName];
  buf.insert(buf.end(), data, data + length);
  cachedBatchLocked(fileName);
  return Status::kSuccess;
}

Status CorfuDBStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  // `mine` is the batch that carries THIS call's bytes. Whatever else
  // happens — another thread drains us, a peer seals the file, the JNI
  // submit fails — the status we return is `mine`'s own outcome. The old
  // code returned kSuccess whenever its cached_file_ entry had vanished,
  // which acked bytes that the draining thread could still fail to
  // sequence.
  std::shared_ptr<Batch> mine;
  std::shared_ptr<Batch> to_submit;
  std::shared_lock<std::shared_mutex> gate(write_gate_);  // before the push
  {
    std::unique_lock<std::mutex> lock(mtx_);
    if (sealed_files_.count(fileName)) return Status::kSealed;
    if (removed_files_.count(fileName)) return Status::kFailure;
    auto& buf = cached_file_[fileName];
    buf.insert(buf.end(), data, data + length);
    mine = cachedBatchLocked(fileName);

    auto now = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_commited_time_).count();
    bool should_flush = elapsed > commit_interval_;

    if (!should_flush && sync_mode_) {
      auto remaining_ms = commit_interval_ - elapsed;
      batch_flushed_cv_.wait_for(lock, std::chrono::milliseconds(remaining_ms),
                                 [&] { return mine->done; });
      if (mine->done) {
        if (mine->result == Status::kSuccess) last_append_addr_ = mine->addr;
        return mine->result;
      }
      should_flush = true;
    }

    // Batched mode with the timer unexpired. The bytes stay in
    // cached_file_ and are NOT durable yet, which is what this mode
    // trades away. The shipped configs run commit_interval_ = 0 with
    // sync_mode_ = true, so this path is not on the durable route.
    if (!should_flush) return Status::kSuccess;

    to_submit = takeCachedBatchLocked(fileName);
    if (to_submit) {
      pending_[fileName].push_back(to_submit);
      last_commited_time_ = std::chrono::system_clock::now();
    }
  }
  // mtx_ released — JNI runs without blocking readers or the tailer.

  if (to_submit) submitBatch(fileName, to_submit);
  // takeCachedBatchLocked can hand back a LATER batch than ours if a
  // peer thread drained us during the wait above. Always report the
  // batch holding our own bytes.
  if (to_submit == mine) return mine->result;
  return awaitBatch(mine);
}

Status CorfuDBStorage::flush(std::string const& fileName) {
  std::shared_ptr<Batch> to_submit;
  std::shared_lock<std::shared_mutex> gate(write_gate_);  // before the push
  {
    std::lock_guard<std::mutex> lk(mtx_);
    to_submit = takeCachedBatchLocked(fileName);
    if (!to_submit) return Status::kNotFound;
    pending_[fileName].push_back(to_submit);
    last_commited_time_ = std::chrono::system_clock::now();
  }
  return submitBatch(fileName, to_submit);
}

// Reads serve file_buffers_ ALONE — never pending_ or cached_file_.
//
// Offsets into a file must be append-stable: the bytes at [a, a+len) must
// stay the same bytes forever, because MetadataLogHandler::readMetadataLog
// reads a range and then advances a persistent offset past it. The old
// code spliced file_buffers_ + pending_ + cached_file_, with file_buffers_
// first. The tailer appends REMOTE bytes to file_buffers_ at any moment,
// which shifted every local unsequenced byte to a higher offset — the same
// offset then named different bytes, the protobuf parse failed, and the
// metadata reader was wedged for the life of the process.
//
// file_buffers_ only ever grows by whole sequenced entries, so serving it
// alone makes offsets stable. drainForRead() preserves read-my-writes by
// pushing this process's own buffered bytes onto the log and waiting for
// the splice before we sample. append/appendInBatch/flush likewise return
// only after their bytes are spliced, so anything already acked is here.
//
// Caveat: in batched mode (commit_interval_ > 0) appendInBatch can ack
// bytes that are still in cached_file_. drainForRead flushes those, so a
// read still sees them — it just pays a JNI round-trip to do it.
Status CorfuDBStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  if (trimmed_out_.load()) return Status::kNotFound;
  drainForRead(fileName);
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return Status::kNotFound;
  auto it = file_buffers_.find(fileName);
  if (it == file_buffers_.end()) return Status::kNotFound;

  size_t total = it->second.size();
  size = total;
  data = new unsigned char[total];
  if (total > 0) std::memcpy(data, it->second.data(), total);
  return Status::kSuccess;
}

Status CorfuDBStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  if (trimmed_out_.load()) return Status::kNotFound;
  drainForRead(fileName);
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return Status::kNotFound;
  auto it = file_buffers_.find(fileName);
  if (it == file_buffers_.end()) return Status::kNotFound;
  if (a + length > it->second.size()) return Status::kFailure;

  data = new unsigned char[length];
  if (length > 0) std::memcpy(data, it->second.data() + a, length);
  return Status::kSuccess;
}

size_t CorfuDBStorage::size(std::string fileName) {
  if (trimmed_out_.load()) return 0;
  drainForRead(fileName);
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return 0;
  auto it = file_buffers_.find(fileName);
  return it == file_buffers_.end() ? 0 : it->second.size();
}

void CorfuDBStorage::seal(std::string fileName) {
  // One shared hold of the gate for the pre-seal flush AND the SEAL entry
  // (write_gate_: taken before anything enters pending_).
  std::shared_lock<std::shared_mutex> gate(write_gate_);
  if (trimmed_out_.load()) return;
  // Pre-seal flush: drain every batched byte BEFORE the SEAL marker, and
  // wait for it to sequence, so the seal lands at a HIGHER global address
  // than our own bytes. submitBatch enforces the address rule, so a
  // pre-seal payload that raced past our own SEAL would be dropped —
  // draining first is what stops us from sealing over ourselves.
  //
  // Bytes another thread adds to this file after this point are still
  // accepted by appendInBatch (sealed_files_ is not set yet) and are
  // rejected with kSealed once their address lands above the seal. That
  // is the correct answer: the caller retries on the new tail.
  std::shared_ptr<Batch> to_submit;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    to_submit = takeCachedBatchLocked(fileName);
    if (to_submit) {
      pending_[fileName].push_back(to_submit);
      last_commited_time_ = std::chrono::system_clock::now();
    }
  }
  if (to_submit && submitBatch(fileName, to_submit) != Status::kSuccess) {
    std::cerr << "[corfu] seal: pre-seal flush failed for " << fileName << "\n";
  }

  // Keyed: the sequencer records key(fileName) at this address, which is
  // what lets every writer's fast path (submitBatchFast) see the seal at
  // token time. A SEAL without the key would be invisible to it.
  long addr = appendKeyed(fileName, ::CorfuEntry_Op_SEAL);
  if (addr < 0) return;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    publishWrittenAddr(addr);
    sealed_files_.insert(fileName);
    // Record the seal address right away, before the tailer reaches this
    // SEAL: own appends still in flight are judged against it as soon as
    // the tailer passes their address.
    auto sit = sealed_at_addr_.find(fileName);
    if (sit == sealed_at_addr_.end() || addr < sit->second) {
      sealed_at_addr_[fileName] = addr;
    }
  }
  batch_flushed_cv_.notify_all();
}

bool CorfuDBStorage::isSealed(std::string fileName) {
  if (trimmed_out_.load()) return false;
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  return sealed_files_.count(fileName) > 0;
}

void CorfuDBStorage::remove(std::string fileName) {
  std::shared_lock<std::shared_mutex> gate(write_gate_);
  if (trimmed_out_.load()) return;
  // Keyed for the same reason as the SEAL in seal(): a peer's fast path
  // must see the REMOVE at token time.
  long addr = appendKeyed(fileName, ::CorfuEntry_Op_REMOVE);
  if (addr < 0) return;
  publishWrittenAddr(addr);
  std::lock_guard<std::mutex> lk(mtx_);
  file_buffers_.erase(fileName);
  sealed_files_.erase(fileName);
  removed_files_.insert(fileName);
  sealed_at_addr_.erase(fileName);
  // Settle in-flight batches for the removed file, as the tailer does
  // for a peer REMOVE. Their bytes will never be readable.
  auto pit = pending_.find(fileName);
  if (pit != pending_.end()) {
    for (auto const& batch : pit->second) {
      if (batch->done) continue;
      batch->result = Status::kFailure;
      batch->done = true;
    }
    pending_.erase(pit);
  }
  auto cit = cached_batch_.find(fileName);
  if (cit != cached_batch_.end()) {
    if (!cit->second->done) {
      cit->second->result = Status::kFailure;
      cit->second->done = true;
    }
    cached_batch_.erase(cit);
  }
  cached_file_.erase(fileName);
  batch_flushed_cv_.notify_all();
}

bool CorfuDBStorage::exist(std::string fileName) {
  if (trimmed_out_.load()) return false;
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return false;
  return file_buffers_.count(fileName) > 0;
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
