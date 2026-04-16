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
  mid_tailAddress_ = env->GetMethodID(bridge_class_global_, "tailAddress", "()J");
  mid_close_ = env->GetMethodID(bridge_class_global_, "close", "()V");
  if (!mid_append_ || !mid_pollNext_ || !mid_tailAddress_ || !mid_close_) {
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

bool CorfuDBStorage::applyEntryFromJava(JNIEnv* env, jbyteArray jbuf) {
  jsize len = env->GetArrayLength(jbuf);
  if (len < 8) {
    env->DeleteLocalRef(jbuf);
    return false;
  }
  std::vector<unsigned char> raw(len);
  env->GetByteArrayRegion(jbuf, 0, len, reinterpret_cast<jbyte*>(raw.data()));
  env->DeleteLocalRef(jbuf);

  long addr = 0;
  for (int i = 0; i < 8; ++i) {
    addr = (addr << 8) | raw[i];
  }

  ::CorfuEntry entry;
  if (!entry.ParseFromArray(raw.data() + 8, static_cast<int>(raw.size() - 8))) {
    std::cerr << "[corfu] failed to parse entry at addr=" << addr << "\n";
    return false;
  }

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
        break;
    }
    last_applied_addr_.store(addr, std::memory_order_release);
  }
  tailer_cv_.notify_all();
  return true;
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
  while (running_) {
    jbyteArray jbuf = static_cast<jbyteArray>(
        env->CallObjectMethod(bridge_global_, mid_pollNext_, static_cast<jlong>(kPollTimeoutMs)));
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      continue;
    }
    if (jbuf == nullptr) continue;  // timeout
    applyEntryFromJava(env, jbuf);
  }
  detachThread();
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
  long target = globalFenceTarget();
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
  long target = globalFenceTarget();
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
  long target = globalFenceTarget();
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
  long target = globalFenceTarget();
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
  long target = globalFenceTarget();
  std::unique_lock<std::mutex> lock(mtx_);
  waitForTailerLocked(lock, target);
  if (removed_files_.count(fileName)) return false;
  return file_buffers_.count(fileName) > 0;
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
