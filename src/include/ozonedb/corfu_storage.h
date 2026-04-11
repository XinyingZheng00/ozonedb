#ifndef CORFU_STORAGE_H
#define CORFU_STORAGE_H
#ifdef OZONEDB_ENABLE_CORFU
#include "storage.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <jni.h>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
  int commit_interval_ = 10;
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

  std::mutex mtx_;
  std::condition_variable batch_flushed_cv_;
  std::condition_variable tailer_cv_;
  std::chrono::system_clock::time_point last_commited_time_;

  std::atomic<long> last_applied_addr_{-1};
  std::atomic<long> last_written_addr_{-1};

  std::thread tailer_thread_;
  std::atomic<bool> running_{false};

  void startJvm(std::string const& jar_path, std::string const& jvm_opts);
  JNIEnv* attachThread();
  void detachThread();
  void loadBridge(std::string const& endpoint, std::string const& stream_name);
  long jniAppendEntry(JNIEnv* env, std::string const& file_name, int op,
                      unsigned char const* data, int length);
  bool applyEntryFromJava(JNIEnv* env, jbyteArray jbuf);
  void drainInitialEntries();
  void tailerLoop();
  void waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target);
};

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
#endif  // CORFU_STORAGE_H
