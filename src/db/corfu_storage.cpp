#ifdef OZONEDB_ENABLE_CORFU
#include "corfu_storage.h"
#include "protobuf/record.pb.h"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>

namespace ozonedb {

namespace {
constexpr char const* kBridgeClass = "site/ycsb/db/corfu/CorfuBridge";
constexpr long kPollTimeoutMs = 100;
// Max entries per pollBatch JNI round-trip. Bigger amortizes JNI and
// Java-side fixed overhead over more entries, smaller bounds the time
// we spend out of the running_ check. 256 is a compromise: with a
// warm Corfu cache (~1 ms/entry) one batch takes ~256 ms worst case;
// with a cold cache (50-400 ms/entry) we'll return partial batches
// via the in-Java short-circuit.
constexpr int kPollBatchSize = 256;

std::vector<std::string> splitJvmOpts(std::string const& opts) {
  std::vector<std::string> out;
  std::istringstream iss(opts);
  std::string tok;
  while (iss >> tok) out.push_back(tok);
  return out;
}
}  // namespace

CorfuDBStorage::CorfuDBStorage(std::string const& endpoint,
                               std::string const& jar_path,
                               std::string const& jvm_opts,
                               std::string const& stream_name,
                               std::string const& db_path)
    : Storage(db_path) {
  {
    std::random_device rd;
    std::mt19937_64 rng(rd());
    client_id_ = rng();
    if (client_id_ == 0) client_id_ = 1;  // reserve 0 as "unset / legacy entry"
  }
  startJvm(jar_path, jvm_opts);
  loadBridge(endpoint, stream_name);
  last_commited_time_ = std::chrono::system_clock::now();
  // Synchronously replay all existing stream entries before returning so
  // reads issued immediately after construction (e.g. by DB::openDB ->
  // rollForwardMetadataLog) see prior state.
  drainInitialEntries();
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

      JNIEnv* env = attachThread();
      if (env) {
        for (auto& kv : drained) {
          long addr = jniAppendEntry(env, kv.first, ::CorfuEntry_Op_APPEND,
                                     kv.second.data(),
                                     static_cast<int>(kv.second.size()));
          if (addr < 0) {
            std::cerr << "[corfu] destructor flush failed for " << kv.first << "\n";
          } else {
            long prev = last_written_addr_.load(std::memory_order_acquire);
            while (addr > prev &&
                   !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
            }
          }
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
  JNIEnv* env = attachThread();
  if (env && bridge_global_ && mid_close_) {
    env->CallVoidMethod(bridge_global_, mid_close_);
    if (env->ExceptionCheck()) env->ExceptionClear();
  }
  if (env) {
    if (bridge_global_) env->DeleteGlobalRef(bridge_global_);
    if (bridge_class_global_) env->DeleteGlobalRef(bridge_class_global_);
  }
  // Intentionally do not destroy the JVM: HotSpot only allows one JVM per
  // process and DestroyJavaVM is unreliable with live threads.
}

void CorfuDBStorage::startJvm(std::string const& jar_path, std::string const& jvm_opts) {
  JavaVM* existing[1];
  jsize nvms = 0;
  if (JNI_GetCreatedJavaVMs(existing, 1, &nvms) == JNI_OK && nvms > 0) {
    jvm_ = existing[0];
    owns_jvm_ = false;
    return;
  }

  std::string classpath_opt = "-Djava.class.path=" + jar_path;
  std::vector<std::string> extra = splitJvmOpts(jvm_opts);

  std::vector<JavaVMOption> options;
  options.push_back({const_cast<char*>(classpath_opt.c_str()), nullptr});
  for (auto& e : extra) {
    options.push_back({const_cast<char*>(e.c_str()), nullptr});
  }

  JavaVMInitArgs args;
  args.version = JNI_VERSION_1_8;
  args.nOptions = static_cast<jint>(options.size());
  args.options = options.data();
  args.ignoreUnrecognized = JNI_FALSE;

  JNIEnv* env = nullptr;
  jint rc = JNI_CreateJavaVM(&jvm_, reinterpret_cast<void**>(&env), &args);
  if (rc != JNI_OK) {
    throw std::runtime_error("Failed to create JVM for CorfuDBStorage (rc=" + std::to_string(rc) + ")");
  }
  owns_jvm_ = true;
}

JNIEnv* CorfuDBStorage::attachThread() {
  if (!jvm_) return nullptr;
  JNIEnv* env = nullptr;
  jint rc = jvm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
  if (rc == JNI_EDETACHED) {
    rc = jvm_->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (rc != JNI_OK) return nullptr;
  } else if (rc != JNI_OK) {
    return nullptr;
  }
  return env;
}

void CorfuDBStorage::detachThread() {
  if (jvm_) jvm_->DetachCurrentThread();
}

void CorfuDBStorage::loadBridge(std::string const& endpoint, std::string const& stream_name) {
  JNIEnv* env = attachThread();
  if (!env) throw std::runtime_error("Failed to attach JVM thread");

  jclass local_class = env->FindClass(kBridgeClass);
  if (!local_class) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    throw std::runtime_error(std::string("CorfuBridge class not found on classpath: ") + kBridgeClass);
  }
  bridge_class_global_ = static_cast<jclass>(env->NewGlobalRef(local_class));
  env->DeleteLocalRef(local_class);

  jmethodID ctor = env->GetMethodID(bridge_class_global_, "<init>", "(Ljava/lang/String;Ljava/lang/String;)V");
  if (!ctor) throw std::runtime_error("CorfuBridge(String,String) not found");

  jstring jendpoint = env->NewStringUTF(endpoint.c_str());
  jstring jstream = env->NewStringUTF(stream_name.c_str());
  jobject local_bridge = env->NewObject(bridge_class_global_, ctor, jendpoint, jstream);
  env->DeleteLocalRef(jendpoint);
  env->DeleteLocalRef(jstream);
  if (!local_bridge || env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    throw std::runtime_error("CorfuBridge constructor failed");
  }
  bridge_global_ = env->NewGlobalRef(local_bridge);
  env->DeleteLocalRef(local_bridge);

  mid_append_ = env->GetMethodID(bridge_class_global_, "append", "([B)J");
  mid_pollNext_ = env->GetMethodID(bridge_class_global_, "pollNext", "(J)[B");
  mid_pollBatch_ = env->GetMethodID(bridge_class_global_, "pollBatch", "(JI)[B");
  mid_tailAddress_ = env->GetMethodID(bridge_class_global_, "tailAddress", "()J");
  mid_gcPollView_ = env->GetMethodID(bridge_class_global_, "gcPollView", "(J)V");
  mid_close_ = env->GetMethodID(bridge_class_global_, "close", "()V");
  if (!mid_append_ || !mid_pollNext_ || !mid_pollBatch_ || !mid_tailAddress_ ||
      !mid_gcPollView_ || !mid_close_) {
    throw std::runtime_error("CorfuBridge method lookup failed");
  }
}

long CorfuDBStorage::jniAppendEntry(JNIEnv* env, std::string const& file_name, int op,
                                    unsigned char const* data, int length) {
  ::CorfuEntry entry;
  entry.set_file_name(file_name);
  entry.set_op(static_cast<::CorfuEntry_Op>(op));
  entry.set_client_id(client_id_);
  if (data != nullptr && length > 0) {
    entry.set_payload(data, length);
  }
  std::string serialized;
  entry.SerializeToString(&serialized);

  jbyteArray jbuf = env->NewByteArray(static_cast<jsize>(serialized.size()));
  env->SetByteArrayRegion(jbuf, 0, static_cast<jsize>(serialized.size()),
                          reinterpret_cast<jbyte const*>(serialized.data()));
  jlong addr = env->CallLongMethod(bridge_global_, mid_append_, jbuf);
  env->DeleteLocalRef(jbuf);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    return -1;
  }
  return static_cast<long>(addr);
}

bool CorfuDBStorage::applyEntryBytes(unsigned char const* data, size_t len) {
  if (len < 8) return false;

  long addr = 0;
  for (int i = 0; i < 8; ++i) {
    addr = (addr << 8) | data[i];
  }

  ::CorfuEntry entry;
  if (!entry.ParseFromArray(data + 8, static_cast<int>(len - 8))) {
    std::cerr << "[corfu] failed to parse entry at addr=" << addr << "\n";
    return false;
  }

  // Stage a remote-append event for the dispatch thread. We do NOT
  // invoke the listener inline here — the listener takes
  // LRUCache::mutex, and inlining that call on the tailer thread
  // creates a deadlock cycle with any foreground op that holds
  // LRUCache::mutex while fencing on this tailer via storage->size().
  // The dispatch thread runs the listener off the tailer's critical
  // path. Local APPENDs are skipped: the writer path already updates
  // any listener-visible index itself.
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
        // Skip locally-originated APPENDs: the writer thread already copied
        // (or will copy) the bytes into file_buffers_ itself as part of the
        // post-JNI reconcile step. Applying them here would double-count
        // the bytes until the writer's own reconcile erases the placeholder.
        if (entry.has_client_id() && entry.client_id() == client_id_) {
          break;
        }
        auto& buf = file_buffers_[fn];
        auto const& payload = entry.payload();
        buf.insert(buf.end(), payload.begin(), payload.end());
        if (remote_listener_) {
          notify = true;
          notify_op = RemoteOp::kAppend;
          notify_file = fn;
          notify_payload.assign(payload.begin(), payload.end());
          listener_snapshot = remote_listener_;
        }
        break;
      }
      case ::CorfuEntry_Op_SEAL:
        sealed_files_.insert(fn);
        break;
      case ::CorfuEntry_Op_REMOVE:
        file_buffers_.erase(fn);
        sealed_files_.erase(fn);
        removed_files_.insert(fn);
        pending_.erase(fn);
        if (remote_listener_) {
          notify = true;
          notify_op = RemoteOp::kRemove;
          notify_file = fn;
          listener_snapshot = remote_listener_;
        }
        break;
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
          std::move(listener_snapshot)});
    }
  }
  tailer_cv_.notify_all();
  if (notify) dispatch_cv_.notify_one();
  return true;
}

bool CorfuDBStorage::applyEntryFromJava(JNIEnv* env, jbyteArray jbuf) {
  jsize len = env->GetArrayLength(jbuf);
  if (len < 8) {
    env->DeleteLocalRef(jbuf);
    return false;
  }
  std::vector<unsigned char> raw(len);
  env->GetByteArrayRegion(jbuf, 0, len, reinterpret_cast<jbyte*>(raw.data()));
  env->DeleteLocalRef(jbuf);
  return applyEntryBytes(raw.data(), raw.size());
}

// Parse the pollBatch wire format (big-endian int32 count followed by
// `count` length-prefixed entries, each in the same addr+payload
// layout as pollNext) and apply each entry. This is the steady-state
// tailer hot path — a single JNI crossing delivers up to
// kPollBatchSize entries, amortizing JNI+Java fixed overhead across
// the batch and letting a healthy Corfu client cache stream entries
// at tens-of-thousands per second.
int CorfuDBStorage::applyBatchFromJava(JNIEnv* env, jbyteArray jbuf) {
  jsize total_len = env->GetArrayLength(jbuf);
  if (total_len < 4) {
    env->DeleteLocalRef(jbuf);
    return 0;
  }
  std::vector<unsigned char> raw(total_len);
  env->GetByteArrayRegion(jbuf, 0, total_len,
                          reinterpret_cast<jbyte*>(raw.data()));
  env->DeleteLocalRef(jbuf);

  size_t pos = 0;
  auto read_be32 = [&]() -> int32_t {
    uint32_t v = (static_cast<uint32_t>(raw[pos]) << 24) |
                 (static_cast<uint32_t>(raw[pos + 1]) << 16) |
                 (static_cast<uint32_t>(raw[pos + 2]) << 8) |
                 static_cast<uint32_t>(raw[pos + 3]);
    pos += 4;
    return static_cast<int32_t>(v);
  };

  int32_t count = read_be32();
  int applied = 0;
  for (int i = 0; i < count; ++i) {
    if (pos + 4 > raw.size()) break;
    int32_t entry_len = read_be32();
    if (entry_len < 0 ||
        pos + static_cast<size_t>(entry_len) > raw.size()) {
      break;  // truncated / malformed — drop the rest of the batch
    }
    if (applyEntryBytes(raw.data() + pos, static_cast<size_t>(entry_len))) {
      ++applied;
    }
    pos += entry_len;
  }
  return applied;
}

void CorfuDBStorage::drainInitialEntries() {
  JNIEnv* env = attachThread();
  if (!env) return;
  // Use timeoutMs=0 so pollNext returns null as soon as the stream cursor
  // reaches the end of pre-existing entries. Keeps cold-start latency low.
  int drained = 0;
  while (true) {
    jbyteArray jbuf = static_cast<jbyteArray>(
        env->CallObjectMethod(bridge_global_, mid_pollNext_, static_cast<jlong>(0)));
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      break;
    }
    if (jbuf == nullptr) break;
    if (applyEntryFromJava(env, jbuf)) ++drained;
  }
  std::cerr << "[corfu] initial replay drained " << drained << " entries\n";
}

void CorfuDBStorage::tailerLoop() {
  JNIEnv* env = attachThread();
  if (!env) {
    std::cerr << "[corfu] tailer failed to attach JVM thread\n";
    return;
  }
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
  while (running_) {
    auto poll_before = std::chrono::steady_clock::now();
    jbyteArray jbuf = static_cast<jbyteArray>(
        env->CallObjectMethod(bridge_global_, mid_pollBatch_,
                              static_cast<jlong>(kPollTimeoutMs),
                              static_cast<jint>(kPollBatchSize)));
    auto poll_after = std::chrono::steady_clock::now();
    auto poll_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       poll_after - poll_before)
                       .count();
    // Only log unusually-slow polls (>50 ms) so steady-state runs
    // don't drown in output. A null jbuf with poll_us ~ 100 ms is
    // just the timeout; log "null=1" so we can tell at a glance.
    if (poll_us > 50000) {
      std::cerr << "[corfu-tailer] slow pollBatch " << poll_us << "us null="
                << (jbuf == nullptr ? 1 : 0) << "\n";
    }
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      continue;
    }
    if (jbuf != nullptr) {
      int applied = applyBatchFromJava(env, jbuf);
      stat_entries += applied;
      entries_since_gc += applied;
    }
    if (entries_since_gc >= kGcTrimInterval) {
      long trim = last_applied_addr_.load(std::memory_order_acquire);
      if (trim >= 0) {
        env->CallVoidMethod(bridge_global_, mid_gcPollView_,
                            static_cast<jlong>(trim));
        if (env->ExceptionCheck()) {
          env->ExceptionDescribe();
          env->ExceptionClear();
        }
      }
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
          for (auto const& p : kv.second) pend_bytes += p.second.size();
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

// Drain remote-append events enqueued by applyEntryFromJava and
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
                    ev.op);
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
  // have already been sequenced. CorfuBridge::tailAddress() returns the
  // highest address currently in the stream. max() ensures we never
  // regress the target if our own in-flight write hasn't been sequenced
  // yet but a remote write has a higher address.
  long local = last_written_addr_.load(std::memory_order_acquire);
  JNIEnv* env = attachThread();
  if (!env) return local;
  jlong global = env->CallLongMethod(bridge_global_, mid_tailAddress_);
  if (env->ExceptionCheck()) {
    env->ExceptionClear();
    return local;
  }
  return std::max(local, static_cast<long>(global));
}

void CorfuDBStorage::waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target) {
  tailer_cv_.wait(lock, [&] {
    return !running_ || last_applied_addr_.load(std::memory_order_acquire) >= target;
  });
}

thread_local CorfuDBStorage::FenceToken CorfuDBStorage::fence_token_;

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

// Helper: drain leading stamped (addr != -1) placeholders from
// pending_[fn] into file_buffers_[fn]. Called with mtx_ held. This
// preserves enqueue order in file_buffers_: a writer whose JNI returned
// out of order will stamp its placeholder but leave reconciliation to
// the earliest-enqueued writer once that writer's JNI returns.
void CorfuDBStorage::reconcilePendingFrontLocked(std::string const& fileName) {
  auto pit = pending_.find(fileName);
  if (pit == pending_.end()) return;
  auto& lst = pit->second;
  while (!lst.empty() && lst.front().first != -1) {
    auto& buf = file_buffers_[fileName];
    auto const& payload = lst.front().second;
    buf.insert(buf.end(), payload.begin(), payload.end());
    lst.pop_front();
  }
  if (lst.empty()) pending_.erase(pit);
}

Status CorfuDBStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  // Stage a placeholder first so local readers see the bytes while the JNI
  // round-trip is in flight; then stamp it with the returned addr and
  // reconcile leading stamped entries into file_buffers_. The tailer skips
  // our entries via client_id so the writer owns the reconcile.
  std::list<std::pair<long, std::vector<unsigned char>>>::iterator placeholder_it;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (sealed_files_.count(fileName)) return Status::kSealed;
    std::vector<unsigned char> payload(data, data + length);
    auto& lst = pending_[fileName];
    lst.push_back({-1, std::move(payload)});
    placeholder_it = std::prev(lst.end());
  }

  JNIEnv* env = attachThread();
  long addr = -1;
  if (env) {
    addr = jniAppendEntry(env, fileName, ::CorfuEntry_Op_APPEND,
                          placeholder_it->second.data(),
                          static_cast<int>(placeholder_it->second.size()));
  }

  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (addr >= 0) {
      placeholder_it->first = addr;
      long prev = last_written_addr_.load(std::memory_order_acquire);
      while (addr > prev &&
             !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
      }
      reconcilePendingFrontLocked(fileName);
    } else {
      auto pit = pending_.find(fileName);
      if (pit != pending_.end()) {
        pit->second.erase(placeholder_it);
        if (pit->second.empty()) pending_.erase(pit);
      }
    }
  }
  if (!env || addr < 0) return Status::kFailure;
  return Status::kSuccess;
}

Status CorfuDBStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  std::lock_guard<std::mutex> lk(mtx_);
  auto& buf = cached_file_[fileName];
  buf.insert(buf.end(), data, data + length);
  return Status::kSuccess;
}

Status CorfuDBStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (sealed_files_.count(fileName)) return Status::kSealed;
    auto& buf = cached_file_[fileName];
    buf.insert(buf.end(), data, data + length);
  }

  // Phase 1 (under mtx_): decide whether to drain, and if so, stage the
  // drained bytes into a pending_ placeholder. mtx_ is then released so
  // the JNI round-trip runs in parallel with readers, the tailer, and
  // writers on other files.
  std::list<std::pair<long, std::vector<unsigned char>>>::iterator placeholder_it;
  {
    std::unique_lock<std::mutex> lock(mtx_);
    auto now = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_commited_time_).count();
    bool should_flush = elapsed > commit_interval_;

    if (!should_flush && sync_mode_) {
      auto remaining_ms = commit_interval_ - elapsed;
      batch_flushed_cv_.wait_for(lock, std::chrono::milliseconds(remaining_ms));
      if (cached_file_.find(fileName) == cached_file_.end()) {
        return Status::kSuccess;
      }
      should_flush = true;
    }

    if (!should_flush) return Status::kSuccess;

    auto it = cached_file_.find(fileName);
    if (it == cached_file_.end() || it->second.empty()) return Status::kSuccess;

    std::vector<unsigned char> payload = std::move(it->second);
    cached_file_.erase(it);
    auto& lst = pending_[fileName];
    lst.push_back({-1, std::move(payload)});
    placeholder_it = std::prev(lst.end());
    last_commited_time_ = std::chrono::system_clock::now();
  }
  // mtx_ released — JNI runs without blocking readers or the tailer.

  // Phase 2: JNI submit. placeholder_it remains valid because pending_ is
  // a std::list (iterator stability across unrelated inserts/erases).
  JNIEnv* env = attachThread();
  long addr = -1;
  if (env) {
    addr = jniAppendEntry(env, fileName, ::CorfuEntry_Op_APPEND,
                          placeholder_it->second.data(),
                          static_cast<int>(placeholder_it->second.size()));
  }

  // Phase 3 (under mtx_): stamp the placeholder with the returned addr,
  // then drain leading stamped placeholders into file_buffers_. This
  // preserves pending_ enqueue order in file_buffers_ — a writer whose
  // JNI returns out of order stamps its placeholder and leaves the copy
  // to whichever writer is currently at the front when its own JNI
  // returns. The tailer never touches this placeholder because entries
  // we wrote carry our client_id and are skipped in applyEntryFromJava.
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (addr >= 0) {
      placeholder_it->first = addr;
      long prev = last_written_addr_.load(std::memory_order_acquire);
      while (addr > prev &&
             !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
      }
      reconcilePendingFrontLocked(fileName);
    } else {
      auto pit = pending_.find(fileName);
      if (pit != pending_.end()) {
        pit->second.erase(placeholder_it);
        if (pit->second.empty()) pending_.erase(pit);
      }
    }
  }
  batch_flushed_cv_.notify_all();
  if (!env || addr < 0) return Status::kFailure;
  return Status::kSuccess;
}

Status CorfuDBStorage::flush(std::string const& fileName) {
  // Three-phase drain/JNI/reconcile identical to appendInBatch so flush()
  // (TableBuilder, sstable finalize, etc.) does not serialize writers or
  // block readers on the JNI round-trip.
  std::list<std::pair<long, std::vector<unsigned char>>>::iterator placeholder_it;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = cached_file_.find(fileName);
    if (it == cached_file_.end()) return Status::kNotFound;
    std::vector<unsigned char> payload = std::move(it->second);
    cached_file_.erase(it);
    auto& lst = pending_[fileName];
    lst.push_back({-1, std::move(payload)});
    placeholder_it = std::prev(lst.end());
  }

  JNIEnv* env = attachThread();
  long addr = -1;
  if (env) {
    addr = jniAppendEntry(env, fileName, ::CorfuEntry_Op_APPEND,
                          placeholder_it->second.data(),
                          static_cast<int>(placeholder_it->second.size()));
  }

  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (addr >= 0) {
      placeholder_it->first = addr;
      long prev = last_written_addr_.load(std::memory_order_acquire);
      while (addr > prev &&
             !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
      }
      reconcilePendingFrontLocked(fileName);
    } else {
      auto pit = pending_.find(fileName);
      if (pit != pending_.end()) {
        pit->second.erase(placeholder_it);
        if (pit->second.empty()) pending_.erase(pit);
      }
    }
  }
  if (!env || addr < 0) return Status::kFailure;
  return Status::kSuccess;
}

// Reads splice file_buffers_ (tailer-applied, cross-process-visible),
// pending_ (committed to the Corfu stream but not yet applied by our
// local tailer), and cached_file_ (local, not-yet-flushed appendInBatch
// bytes) so every drained byte is visible exactly once at every moment.
// Logical layout:
//   [0, applied)                                 from file_buffers_
//   [applied, applied + sum(pending lens))       from pending_ in order
//   [.., .. + cached)                            from cached_file_
// Remote processes still only see file_buffers_ for bytes *this* process
// wrote — cross-process read-after-write still requires a commit_interval
// flush from the writer side.
namespace {
struct Segment {
  unsigned char const* data;
  size_t len;
};
}  // namespace

Status CorfuDBStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return Status::kNotFound;
  auto it = file_buffers_.find(fileName);
  auto pit = pending_.find(fileName);
  auto cit = cached_file_.find(fileName);
  bool has_any = (it != file_buffers_.end()) ||
                 (pit != pending_.end() && !pit->second.empty()) ||
                 (cit != cached_file_.end() && !cit->second.empty());
  if (!has_any) return Status::kNotFound;

  std::vector<Segment> segs;
  size_t total = 0;
  if (it != file_buffers_.end()) {
    segs.push_back({it->second.data(), it->second.size()});
    total += it->second.size();
  }
  if (pit != pending_.end()) {
    for (auto const& p : pit->second) {
      segs.push_back({p.second.data(), p.second.size()});
      total += p.second.size();
    }
  }
  if (cit != cached_file_.end()) {
    segs.push_back({cit->second.data(), cit->second.size()});
    total += cit->second.size();
  }
  size = total;
  data = new unsigned char[total];
  size_t off = 0;
  for (auto const& s : segs) {
    if (s.len == 0) continue;
    std::memcpy(data + off, s.data, s.len);
    off += s.len;
  }
  return Status::kSuccess;
}

Status CorfuDBStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return Status::kNotFound;
  auto it = file_buffers_.find(fileName);
  auto pit = pending_.find(fileName);
  auto cit = cached_file_.find(fileName);
  bool has_any = (it != file_buffers_.end()) ||
                 (pit != pending_.end() && !pit->second.empty()) ||
                 (cit != cached_file_.end() && !cit->second.empty());
  if (!has_any) return Status::kNotFound;

  std::vector<Segment> segs;
  size_t total = 0;
  if (it != file_buffers_.end()) {
    segs.push_back({it->second.data(), it->second.size()});
    total += it->second.size();
  }
  if (pit != pending_.end()) {
    for (auto const& p : pit->second) {
      segs.push_back({p.second.data(), p.second.size()});
      total += p.second.size();
    }
  }
  if (cit != cached_file_.end()) {
    segs.push_back({cit->second.data(), cit->second.size()});
    total += cit->second.size();
  }
  if (a + length > total) return Status::kFailure;

  data = new unsigned char[length];
  size_t seg_start = 0;
  size_t written = 0;
  for (auto const& s : segs) {
    if (written == length) break;
    size_t seg_end = seg_start + s.len;
    if (a + written >= seg_end) {
      seg_start = seg_end;
      continue;
    }
    size_t local_off = (a + written > seg_start) ? (a + written - seg_start) : 0;
    size_t avail = s.len - local_off;
    size_t to_copy = std::min(avail, length - written);
    std::memcpy(data + written, s.data + local_off, to_copy);
    written += to_copy;
    seg_start = seg_end;
  }
  return Status::kSuccess;
}

size_t CorfuDBStorage::size(std::string fileName) {
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return 0;
  size_t total = 0;
  if (auto it = file_buffers_.find(fileName); it != file_buffers_.end())
    total += it->second.size();
  if (auto pit = pending_.find(fileName); pit != pending_.end()) {
    for (auto const& p : pit->second) total += p.second.size();
  }
  if (auto cit = cached_file_.find(fileName); cit != cached_file_.end())
    total += cit->second.size();
  return total;
}

void CorfuDBStorage::seal(std::string fileName) {
  // Pre-seal flush: drain any batched bytes BEFORE the SEAL marker so the
  // seal terminates a consistent byte sequence on the shared stream. Uses
  // the same drain/JNI/reconcile split as appendInBatch so the JNI submit
  // does not hold mtx_.
  std::list<std::pair<long, std::vector<unsigned char>>>::iterator placeholder_it;
  bool has_placeholder = false;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = cached_file_.find(fileName);
    if (it != cached_file_.end() && !it->second.empty()) {
      std::vector<unsigned char> payload = std::move(it->second);
      cached_file_.erase(it);
      auto& lst = pending_[fileName];
      lst.push_back({-1, std::move(payload)});
      placeholder_it = std::prev(lst.end());
      has_placeholder = true;
    }
  }

  JNIEnv* env = attachThread();
  if (has_placeholder) {
    long addr = -1;
    if (env) {
      addr = jniAppendEntry(env, fileName, ::CorfuEntry_Op_APPEND,
                            placeholder_it->second.data(),
                            static_cast<int>(placeholder_it->second.size()));
    }
    {
      std::lock_guard<std::mutex> lk(mtx_);
      if (addr >= 0) {
        placeholder_it->first = addr;
        long prev = last_written_addr_.load(std::memory_order_acquire);
        while (addr > prev &&
               !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
        }
        reconcilePendingFrontLocked(fileName);
      } else {
        std::cerr << "[corfu] seal: pre-seal flush failed for " << fileName << "\n";
        auto pit = pending_.find(fileName);
        if (pit != pending_.end()) {
          pit->second.erase(placeholder_it);
          if (pit->second.empty()) pending_.erase(pit);
        }
      }
    }
  }

  if (!env) return;
  long addr = jniAppendEntry(env, fileName, ::CorfuEntry_Op_SEAL, nullptr, 0);
  if (addr < 0) return;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    long prev = last_written_addr_.load(std::memory_order_acquire);
    while (addr > prev &&
           !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
    }
    sealed_files_.insert(fileName);
  }
}

bool CorfuDBStorage::isSealed(std::string fileName) {
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  return sealed_files_.count(fileName) > 0;
}

void CorfuDBStorage::remove(std::string fileName) {
  JNIEnv* env = attachThread();
  if (!env) return;
  long addr = jniAppendEntry(env, fileName, ::CorfuEntry_Op_REMOVE, nullptr, 0);
  if (addr < 0) return;
  long prev = last_written_addr_.load(std::memory_order_acquire);
  while (addr > prev &&
         !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
  }
  std::lock_guard<std::mutex> lk(mtx_);
  file_buffers_.erase(fileName);
  sealed_files_.erase(fileName);
  removed_files_.insert(fileName);
}

bool CorfuDBStorage::exist(std::string fileName) {
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return false;
  return file_buffers_.count(fileName) > 0;
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
