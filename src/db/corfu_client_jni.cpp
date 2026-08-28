// The embedded-JVM CorfuClient: an in-process JVM (or the host JVM when
// the process already is one, as under YCSB) running
// site.ycsb.db.corfu.CorfuBridge. This is the JNI code that used to live
// in corfu_storage.cpp, moved behind the CorfuClient seam with no
// behaviour change (PLAN-native-corfu.md, phase 0).
#ifdef OZONEDB_ENABLE_CORFU
#include "corfu_client.h"
#include <jni.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace ozonedb {

namespace {
constexpr char const* kBridgeClass = "site/ycsb/db/corfu/CorfuBridge";

std::vector<std::string> splitJvmOpts(std::string const& opts) {
  std::vector<std::string> out;
  std::istringstream iss(opts);
  std::string tok;
  while (iss >> tok) out.push_back(tok);
  return out;
}

class JniCorfuClient : public CorfuClient {
 public:
  explicit JniCorfuClient(CorfuClientOptions const& options) {
    startJvm(options.jar_path, options.jvm_opts);
    loadBridge(options.endpoint, options.stream_name);
  }

  ~JniCorfuClient() override {
    close();
    JNIEnv* env = attachThread();
    if (env) {
      if (bridge_global_) env->DeleteGlobalRef(bridge_global_);
      if (bridge_class_global_) env->DeleteGlobalRef(bridge_class_global_);
    }
    bridge_global_ = nullptr;
    bridge_class_global_ = nullptr;
    // Intentionally do not destroy the JVM: HotSpot only allows one JVM
    // per process and DestroyJavaVM is unreliable with live threads.
  }

  int64_t append(std::string_view payload) override {
    JNIEnv* env = attachThread();
    if (!env) return -1;
    jbyteArray jbuf = toByteArray(env, payload);
    jlong addr = env->CallLongMethod(bridge_global_, mid_append_, jbuf);
    env->DeleteLocalRef(jbuf);
    if (clearException(env)) return -1;
    return static_cast<int64_t>(addr);
  }

  CheckedAppend appendChecked(std::string_view payload, int64_t snapshot,
                              std::string_view const* read_key,
                              std::string_view const* write_key) override {
    CheckedAppend out;
    JNIEnv* env = attachThread();
    if (!env) {
      out.abort = kAbortOther;
      return out;
    }
    jbyteArray jbuf = toByteArray(env, payload);
    jbyteArray jread = read_key ? toByteArray(env, *read_key) : nullptr;
    jbyteArray jwrite = write_key ? toByteArray(env, *write_key) : nullptr;
    jobject res = env->CallObjectMethod(bridge_global_, mid_appendChecked_, jbuf,
                                        static_cast<jlong>(snapshot), jread, jwrite);
    env->DeleteLocalRef(jbuf);
    if (jread) env->DeleteLocalRef(jread);
    if (jwrite) env->DeleteLocalRef(jwrite);
    if (clearException(env)) {
      if (res) env->DeleteLocalRef(res);
      out.abort = kAbortOther;
      return out;
    }
    if (!res) {
      out.abort = kAbortOther;
      return out;
    }
    jlong pair[2] = {-1, -1};
    env->GetLongArrayRegion(static_cast<jlongArray>(res), 0, 2, pair);
    env->DeleteLocalRef(res);
    if (env->ExceptionCheck()) {
      env->ExceptionClear();
      out.abort = kAbortOther;
      return out;
    }
    if (pair[0] >= 0) {
      out.addr = static_cast<int64_t>(pair[0]);
    } else {
      out.abort = static_cast<int64_t>(pair[0]);
      out.offending = static_cast<int64_t>(pair[1]);
    }
    return out;
  }

  int64_t globalTail() override { return callLong(mid_globalTail_); }

  int64_t streamTail() override { return callLong(mid_tailAddress_); }

  Poll pollBatch(int timeout_ms, int max_entries, EntrySink const& sink) override {
    JNIEnv* env = attachThread();
    if (!env) return Poll::kError;
    jbyteArray jbuf = static_cast<jbyteArray>(
        env->CallObjectMethod(bridge_global_, mid_pollBatch_,
                              static_cast<jlong>(timeout_ms),
                              static_cast<jint>(max_entries)));
    if (clearException(env)) {
      if (jbuf) env->DeleteLocalRef(jbuf);
      return Poll::kError;
    }
    if (jbuf == nullptr) return Poll::kIdle;
    jsize total_len = env->GetArrayLength(jbuf);
    if (total_len == 0) {
      // The bridge's TRIMMED marker: a zero-length array.
      env->DeleteLocalRef(jbuf);
      return Poll::kTrimmed;
    }
    if (total_len < 4) {
      env->DeleteLocalRef(jbuf);
      return Poll::kError;
    }
    // One copy out of the Java heap. GetPrimitiveArrayCritical would
    // avoid it, but the sink takes CorfuDBStorage::mtx_ and can block on
    // it, which is not allowed inside a critical region.
    std::vector<unsigned char> raw(static_cast<size_t>(total_len));
    env->GetByteArrayRegion(jbuf, 0, total_len, reinterpret_cast<jbyte*>(raw.data()));
    env->DeleteLocalRef(jbuf);
    return deliver(raw, sink);
  }

  bool seek(int64_t addr) override {
    JNIEnv* env = attachThread();
    if (!env) return false;
    env->CallVoidMethod(bridge_global_, mid_seekPollView_, static_cast<jlong>(addr));
    return !clearException(env);
  }

  void gc(int64_t mark) override {
    JNIEnv* env = attachThread();
    if (!env) return;
    env->CallVoidMethod(bridge_global_, mid_gcPollView_, static_cast<jlong>(mark));
    clearException(env);
  }

  int64_t prefixTrim(int64_t addr) override {
    JNIEnv* env = attachThread();
    if (!env) return -1;
    jlong mark = env->CallLongMethod(bridge_global_, mid_prefixTrim_, static_cast<jlong>(addr));
    if (clearException(env)) return -1;
    return static_cast<int64_t>(mark);
  }

  int64_t trimMark() override { return callLong(mid_trimMark_); }

  void detachThread() override {
    if (jvm_) jvm_->DetachCurrentThread();
  }

  void close() override {
    if (closed_) return;
    closed_ = true;
    JNIEnv* env = attachThread();
    if (env && bridge_global_ && mid_close_) {
      env->CallVoidMethod(bridge_global_, mid_close_);
      if (env->ExceptionCheck()) env->ExceptionClear();
    }
  }

 private:
  JavaVM* jvm_ = nullptr;
  bool owns_jvm_ = false;
  bool closed_ = false;
  jobject bridge_global_ = nullptr;
  jclass bridge_class_global_ = nullptr;
  jmethodID mid_append_ = nullptr;
  jmethodID mid_appendChecked_ = nullptr;
  jmethodID mid_globalTail_ = nullptr;
  jmethodID mid_pollBatch_ = nullptr;
  jmethodID mid_tailAddress_ = nullptr;
  jmethodID mid_gcPollView_ = nullptr;
  jmethodID mid_prefixTrim_ = nullptr;
  jmethodID mid_trimMark_ = nullptr;
  jmethodID mid_seekPollView_ = nullptr;
  jmethodID mid_close_ = nullptr;

  void startJvm(std::string const& jar_path, std::string const& jvm_opts) {
    JavaVM* existing[1];
    jsize nvms = 0;
    if (JNI_GetCreatedJavaVMs(existing, 1, &nvms) == JNI_OK && nvms > 0) {
      // The process already is a JVM (YCSB): the bridge jar must be on
      // its classpath; jar_path and jvm_opts are ignored.
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

  JNIEnv* attachThread() {
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

  void loadBridge(std::string const& endpoint, std::string const& stream_name) {
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
    mid_appendChecked_ = env->GetMethodID(bridge_class_global_, "appendChecked", "([BJ[B[B)[J");
    mid_globalTail_ = env->GetMethodID(bridge_class_global_, "globalTail", "()J");
    mid_pollBatch_ = env->GetMethodID(bridge_class_global_, "pollBatch", "(JI)[B");
    mid_tailAddress_ = env->GetMethodID(bridge_class_global_, "tailAddress", "()J");
    mid_gcPollView_ = env->GetMethodID(bridge_class_global_, "gcPollView", "(J)V");
    mid_prefixTrim_ = env->GetMethodID(bridge_class_global_, "prefixTrim", "(J)J");
    mid_trimMark_ = env->GetMethodID(bridge_class_global_, "trimMark", "()J");
    mid_seekPollView_ = env->GetMethodID(bridge_class_global_, "seekPollView", "(J)V");
    mid_close_ = env->GetMethodID(bridge_class_global_, "close", "()V");
    if (!mid_append_ || !mid_appendChecked_ || !mid_globalTail_ ||
        !mid_pollBatch_ || !mid_tailAddress_ || !mid_gcPollView_ || !mid_prefixTrim_ ||
        !mid_trimMark_ || !mid_seekPollView_ || !mid_close_) {
      throw std::runtime_error("CorfuBridge method lookup failed");
    }
  }

  static jbyteArray toByteArray(JNIEnv* env, std::string_view bytes) {
    jbyteArray arr = env->NewByteArray(static_cast<jsize>(bytes.size()));
    env->SetByteArrayRegion(arr, 0, static_cast<jsize>(bytes.size()),
                            reinterpret_cast<jbyte const*>(bytes.data()));
    return arr;
  }

  // True (and the exception cleared and printed) when the last JNI call
  // threw.
  static bool clearException(JNIEnv* env) {
    if (!env->ExceptionCheck()) return false;
    env->ExceptionDescribe();
    env->ExceptionClear();
    return true;
  }

  int64_t callLong(jmethodID mid) {
    JNIEnv* env = attachThread();
    if (!env) return -1;
    jlong v = env->CallLongMethod(bridge_global_, mid);
    if (clearException(env)) return -1;
    return static_cast<int64_t>(v);
  }

  // Parse the pollBatch wire format (big-endian int32 count, then count x
  // {int32 len, int64 addr, payload}) and hand each entry to the sink.
  static Poll deliver(std::vector<unsigned char> const& raw, EntrySink const& sink) {
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
    int delivered = 0;
    for (int i = 0; i < count; ++i) {
      if (pos + 4 > raw.size()) break;
      int32_t entry_len = read_be32();
      if (entry_len < 8 || pos + static_cast<size_t>(entry_len) > raw.size()) {
        break;  // truncated / malformed: drop the rest of the batch
      }
      int64_t addr = 0;
      for (int b = 0; b < 8; ++b) addr = (addr << 8) | raw[pos + b];
      sink(addr, raw.data() + pos + 8, static_cast<size_t>(entry_len) - 8);
      ++delivered;
      pos += static_cast<size_t>(entry_len);
    }
    return delivered > 0 ? Poll::kEntries : Poll::kError;
  }
};
}  // namespace

std::unique_ptr<CorfuClient> makeJniCorfuClient(CorfuClientOptions const& options) {
  return std::make_unique<JniCorfuClient>(options);
}

std::unique_ptr<CorfuClient> makeCorfuClient(CorfuClientOptions const& options) {
  if (options.client == "jni") return makeJniCorfuClient(options);
#ifdef OZONEDB_CORFU_NATIVE
  if (options.client == "native") return makeNativeCorfuClient(options);
#endif
  throw std::runtime_error("corfu_client=" + options.client +
                           " is not available in this build (expected jni"
#ifdef OZONEDB_CORFU_NATIVE
                           " or native"
#endif
                           ")");
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
