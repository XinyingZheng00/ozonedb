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
        // An APPEND sequenced ABOVE this file's SEAL arrived after the
        // seal closed the file. It belongs to no file. Every process
        // applies this same rule — and the writer applies it to its own
        // append in submitBatch — so the bytes are unreadable everywhere
        // and the writer's kSealed retry cannot duplicate the record.
        {
          auto sit = sealed_at_addr_.find(fn);
          if (sit != sealed_at_addr_.end() && sit->second < addr) break;
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
  bool reached = tailer_cv_.wait_for(lock, kFenceTimeout, [&] {
    return !running_ || last_applied_addr_.load(std::memory_order_acquire) >= target;
  });
  if (!reached) {
    std::cerr << "[corfu] fence timed out: target=" << target
              << " applied=" << last_applied_addr_.load(std::memory_order_acquire)
              << " (target is probably not on this stream)\n";
  }
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

// Helper: drain the leading stamped (addr != -1) run of pending_[fn]
// into file_buffers_[fn]. Called with mtx_ held. This preserves enqueue
// order in file_buffers_: a writer whose JNI returned out of order will
// stamp its batch but leave the splice to the earliest-enqueued writer
// once that writer's JNI returns.
//
// The splice is the ack point. A batch is marked done+kSuccess here and
// only here, because read() serves file_buffers_ alone — a batch that is
// sequenced but not yet spliced is not yet readable, so acking it early
// would break read-my-writes.
void CorfuDBStorage::reconcilePendingFrontLocked(std::string const& fileName) {
  auto pit = pending_.find(fileName);
  if (pit == pending_.end()) return;
  auto& lst = pit->second;
  while (!lst.empty() && lst.front()->addr != -1) {
    auto batch = lst.front();
    auto& buf = file_buffers_[fileName];
    buf.insert(buf.end(), batch->payload.begin(), batch->payload.end());
    lst.pop_front();
    batch->result = Status::kSuccess;
    batch->done = true;
  }
  if (lst.empty()) pending_.erase(pit);
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

// Settle a batch that will never be spliced. Dropping it from pending_
// also unblocks any stamped batches queued behind it, so reconcile runs
// again before we return.
void CorfuDBStorage::finishBatchLocked(std::string const& fileName,
                                       std::shared_ptr<Batch> const& batch,
                                       Status result) {
  if (batch->done) return;  // a peer REMOVE already settled it
  batch->result = result;
  batch->done = true;
  auto pit = pending_.find(fileName);
  if (pit != pending_.end()) {
    pit->second.remove(batch);
    if (pit->second.empty()) {
      pending_.erase(pit);
    } else {
      reconcilePendingFrontLocked(fileName);
    }
  }
}

// JNI submit, then stamp and reconcile under mtx_. mtx_ must NOT be held
// on entry: the JNI round-trip runs unlocked so it does not block readers,
// the tailer, or writers on other files.
Status CorfuDBStorage::submitBatch(std::string const& fileName,
                                   std::shared_ptr<Batch> const& batch) {
  JNIEnv* env = attachThread();
  long addr = -1;
  if (env) {
    addr = jniAppendEntry(env, fileName, ::CorfuEntry_Op_APPEND,
                          batch->payload.data(),
                          static_cast<int>(batch->payload.size()));
  }

  {
    std::unique_lock<std::mutex> lock(mtx_);
    if (batch->done) {
      // A peer REMOVE settled this batch while the JNI call was in
      // flight. Do not resurrect it.
    } else if (addr < 0) {
      finishBatchLocked(fileName, batch, Status::kFailure);
    } else {
      batch->addr = addr;
      long prev = last_written_addr_.load(std::memory_order_acquire);
      while (addr > prev &&
             !last_written_addr_.compare_exchange_weak(prev, addr, std::memory_order_acq_rel)) {
      }
      auto sit = sealed_at_addr_.find(fileName);
      if (sit != sealed_at_addr_.end() && sit->second < addr) {
        // Sequenced after the file's SEAL. These bytes belong to no
        // file, and no process will ever read them. Report kSealed so
        // the caller retries on the new tail — a retry cannot duplicate,
        // because this copy is unreadable everywhere.
        finishBatchLocked(fileName, batch, Status::kSealed);
      } else if (removed_files_.count(fileName)) {
        finishBatchLocked(fileName, batch, Status::kFailure);
      } else {
        reconcilePendingFrontLocked(fileName);
        // Not spliced yet means an earlier batch on this file is still
        // in flight. Wait for its writer to stamp, so `done` keeps its
        // meaning: the bytes are readable through file_buffers_.
        if (!batch->done) {
          batch_flushed_cv_.wait_for(lock, std::chrono::seconds(30),
                                     [&] { return batch->done; });
          if (!batch->done) {
            std::cerr << "[corfu] submitBatch: splice timed out for " << fileName
                      << " at addr " << addr << "\n";
            finishBatchLocked(fileName, batch, Status::kFailure);
          }
        }
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
  drainForRead(fileName);
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return 0;
  auto it = file_buffers_.find(fileName);
  return it == file_buffers_.end() ? 0 : it->second.size();
}

void CorfuDBStorage::seal(std::string fileName) {
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

  JNIEnv* env = attachThread();
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
    // Record the seal address locally too. Our own tailer skips entries
    // carrying our client_id, so it will never apply this SEAL for us.
    auto sit = sealed_at_addr_.find(fileName);
    if (sit == sealed_at_addr_.end() || addr < sit->second) {
      sealed_at_addr_[fileName] = addr;
    }
  }
  batch_flushed_cv_.notify_all();
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
  long target = fenceTargetForCaller();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return false;
  return file_buffers_.count(fileName) > 0;
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
