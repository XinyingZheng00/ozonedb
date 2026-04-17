#ifndef CORFU_STORAGE_H
#define CORFU_STORAGE_H
#ifdef OZONEDB_ENABLE_CORFU
#include "storage.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <jni.h>

namespace ozonedb {

/**
 * @brief Storage backend backed by a single CorfuDB stream.
 *
 * All ozonedb files (logs, sstables, metadata) are packed into one shared
 * Corfu stream. Each log entry is a CorfuEntry protobuf carrying a filename,
 * an opcode (APPEND / SEAL / REMOVE) and a payload.
 *
 * The C++ process bridges into CorfuDB via an embedded JVM and a thin Java
 * wrapper class (site.ycsb.db.corfu.CorfuBridge) whose fat jar is on the
 * JVM classpath.
 *
 * A background tailer thread continually polls new entries and reconstructs
 * per-file buffers plus a sealed-files set. Reads wait for the tailer to
 * catch up to the writer's last-known address (read-my-writes within a
 * single process).
 */
class CorfuDBStorage : public Storage {
 public:
  CorfuDBStorage(std::string const& endpoint,
                 std::string const& jar_path,
                 std::string const& jvm_opts,
                 std::string const& stream_name,
                 std::string const& db_path);
  ~CorfuDBStorage();

  void createDirectory(std::string name) override;
  Status append(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendInBatch(std::string const& fileName, unsigned char* const& data, int length) override;
  Status flush(std::string const& fileName) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t& size) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override;
  size_t size(std::string fileName) override;
  void seal(std::string fileName) override;
  bool isSealed(std::string fileName) override;
  void remove(std::string fileName) override;
  bool exist(std::string fileName) override;

  void setSyncMode(bool sync) { sync_mode_ = sync; }
  void setRemoteAppendListener(RemoteAppendListener listener) override {
    std::lock_guard<std::mutex> lk(mtx_);
    remote_listener_ = std::move(listener);
  }
  int commit_interval_ = 0;
  bool sync_mode_ = false;

 private:
  // JVM / JNI handles
  JavaVM* jvm_ = nullptr;
  bool owns_jvm_ = false;
  jobject bridge_global_ = nullptr;
  jclass bridge_class_global_ = nullptr;
  jmethodID mid_append_ = nullptr;
  jmethodID mid_pollNext_ = nullptr;
  jmethodID mid_tailAddress_ = nullptr;
  jmethodID mid_close_ = nullptr;

  // Per-file reconstructed state (populated by tailer)
  std::unordered_map<std::string, std::vector<unsigned char>> file_buffers_;
  std::unordered_set<std::string> sealed_files_;
  std::unordered_set<std::string> removed_files_;

  // Pending batched writes (same role as AzureBlobStorage::cached_file)
  std::unordered_map<std::string, std::vector<unsigned char>> cached_file_;

  // Drained bytes that have been handed to JNI but are not yet in
  // file_buffers_. Readers splice file_buffers_ + pending_ + cached_file_
  // so every byte is visible exactly once. The writer owns reconciliation:
  // after JNI returns, it copies the placeholder payload into file_buffers_
  // under mtx_ and erases the placeholder in one atomic section. The tailer
  // does *not* touch pending_ at all — locally-originated APPEND entries
  // are tagged with client_id_ and the tailer skips them, leaving the
  // reconcile job to the writer. std::list is chosen so the iterator the
  // writer saves after push_back remains valid while mtx_ is released for
  // the JNI round-trip.
  std::unordered_map<std::string, std::list<std::pair<long, std::vector<unsigned char>>>> pending_;

  std::mutex mtx_;
  std::condition_variable batch_flushed_cv_;
  std::condition_variable tailer_cv_;
  std::chrono::system_clock::time_point last_commited_time_;

  std::atomic<long> last_applied_addr_{-1};
  std::atomic<long> last_written_addr_{-1};

  // Randomly-generated id that tags every APPEND this process writes to
  // the shared stream. The tailer uses it to distinguish our own entries
  // (which the writer already self-applied into file_buffers_) from peer
  // entries (which the tailer must apply).
  uint64_t client_id_ = 0;

  std::thread tailer_thread_;
  std::atomic<bool> running_{false};

  // Notified for every applied entry that did NOT originate from this
  // process (i.e. remote APPENDs and any REMOVE). Invoked by the tailer
  // outside mtx_ so a listener that acquires its own locks can't
  // deadlock against storage. Installed by LogHandler at startup.
  RemoteAppendListener remote_listener_;

  void startJvm(std::string const& jar_path, std::string const& jvm_opts);
  JNIEnv* attachThread();
  void detachThread();
  void loadBridge(std::string const& endpoint, std::string const& stream_name);
  long jniAppendEntry(JNIEnv* env, std::string const& file_name, int op,
                      unsigned char const* data, int length);
  bool applyEntryFromJava(JNIEnv* env, jbyteArray jbuf);
  void drainInitialEntries();
  void tailerLoop();
  long globalFenceTarget();
  void waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target);
  void reconcilePendingFrontLocked(std::string const& fileName);
};

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
#endif  // CORFU_STORAGE_H
