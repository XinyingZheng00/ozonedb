#ifndef CORFU_STORAGE_H
#define CORFU_STORAGE_H
#ifdef OZONEDB_ENABLE_CORFU
#include "storage.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
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

  // One-fence-per-get support (Metadata::linearizable_reads). sync()
  // performs the single sequencer query + tailer wait and records a
  // thread-local {instance, target} token; fenceTargetForCaller() then
  // lets read/size/exist/isSealed on the same thread reuse the token
  // instead of re-querying the sequencer — the tailer wait degenerates
  // to a no-op because last_applied_addr_ already covers the target.
  // The token is thread-local, NOT process-wide: a fence taken by one
  // get must not exempt a concurrent get on another thread whose
  // invocation may postdate the sample. The instance field guards
  // against a token from one CorfuDBStorage leaking into another.
  void sync() override;
  void clearSync() override;
  bool hasSyncToken() const override { return fence_token_.instance == this; }

  void setRemoteAppendListener(RemoteAppendListener listener) override {
    bool cleared;
    {
      std::lock_guard<std::mutex> lk(mtx_);
      remote_listener_ = std::move(listener);
      cleared = !remote_listener_;
    }
    // When a listener is cleared (typically LogHandler::~LogHandler),
    // block until the dispatcher has finished running every event
    // that was enqueued under the old listener. Each queued event
    // holds a std::function copy that captured the caller's `this`
    // pointer; running those events after `this` is destroyed would
    // be a use-after-free. Draining here closes that window.
    if (cleared) drainDispatchQueue();
  }
  int commit_interval_ = 0;
  bool sync_mode_ = true;

 private:
  // JVM / JNI handles
  JavaVM* jvm_ = nullptr;
  bool owns_jvm_ = false;
  jobject bridge_global_ = nullptr;
  jclass bridge_class_global_ = nullptr;
  jmethodID mid_append_ = nullptr;
  jmethodID mid_pollNext_ = nullptr;
  jmethodID mid_pollBatch_ = nullptr;
  jmethodID mid_tailAddress_ = nullptr;
  jmethodID mid_gcPollView_ = nullptr;
  jmethodID mid_close_ = nullptr;

  // Per-file reconstructed state (populated by tailer)
  std::unordered_map<std::string, std::vector<unsigned char>> file_buffers_;
  std::unordered_set<std::string> sealed_files_;
  std::unordered_set<std::string> removed_files_;

  // Pending batched writes (same role as AzureBlobStorage::cached_file)
  std::unordered_map<std::string, std::vector<unsigned char>> cached_file_;

  // One drain of a file's cached_file_ bytes, from the moment they leave
  // cached_file_ until the moment they land in file_buffers_ (or fail).
  //
  // Lifetime: co-owned by shared_ptr. Every thread whose bytes went into
  // this batch holds one, and pending_ holds one while the batch is in
  // flight. That is deliberate — a writer must NOT hold a bare iterator
  // into pending_, because the tailer erases pending_ entries when a peer
  // REMOVEs the file. Erasing a shared_ptr from the list is safe under a
  // concurrent writer, an iterator is not.
  //
  // `done` is the single ack point. It is set exactly once, under mtx_,
  // by whichever of these happens first:
  //   - reconcilePendingFrontLocked splices the payload  -> kSuccess
  //   - the JNI submit fails                             -> kFailure
  //   - the file was sealed at a LOWER global address    -> kSealed
  //   - a peer REMOVE dropped the file                   -> kFailure
  // A caller may only ack a write after `done` is true, and must return
  // `result`, never an assumed kSuccess.
  //
  // `done` deliberately means "spliced into file_buffers_", not merely
  // "sequenced". Reads serve file_buffers_ only (see read()), so waiting
  // for the splice is what preserves read-my-writes.
  struct Batch {
    long addr = -1;  // global Corfu address, stamped after JNI returns
    std::vector<unsigned char> payload;
    bool done = false;
    Status result = Status::kFailure;
  };

  // Batches handed to JNI but not yet spliced into file_buffers_, in
  // enqueue order per file. reconcilePendingFrontLocked drains the
  // leading stamped run, so file_buffers_ keeps enqueue order.
  std::unordered_map<std::string, std::list<std::shared_ptr<Batch>>> pending_;

  // The batch that currently owns the bytes sitting in cached_file_[fn].
  // Created when the first byte enters cached_file_[fn] and moved into
  // pending_ on drain. A thread that adds bytes takes a shared_ptr to it,
  // so it can learn the outcome even when a different thread does the
  // drain and the JNI submit.
  std::unordered_map<std::string, std::shared_ptr<Batch>> cached_batch_;

  // Global address of the SEAL entry for a file, once the tailer has seen
  // it. The shared log — not local state — decides whether an APPEND
  // belongs to a file: an APPEND sequenced ABOVE this address arrived
  // after the seal and belongs to no file. Every process applies that
  // same rule, in applyEntryBytes for peer entries and in submitBatch for
  // our own, so a kSealed retry can never duplicate a record.
  std::unordered_map<std::string, long> sealed_at_addr_;

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
  // process (i.e. remote APPENDs and any REMOVE). Installed by
  // LogHandler at startup. Invocation goes through dispatch_thread_
  // below — the tailer never calls the listener synchronously.
  RemoteAppendListener remote_listener_;

  // Queue of remote-append events awaiting listener dispatch. The
  // listener (LogHandler::onRemoteAppend) takes LRUCache::mutex,
  // which is sometimes held by a foreground thread fencing on this
  // tailer via storage->size(). Invoking the listener directly from
  // the tailer would create a cycle: foreground holds LRU mutex,
  // waits for tailer; tailer waits for LRU mutex inside the listener.
  // A dedicated dispatch thread drains this queue and runs the
  // listener off the tailer's critical path so the tailer never
  // blocks on a foreground lock.
  //
  // Queue is unbounded intentionally. A bounded queue would require
  // the tailer to block on a full queue, which reintroduces the
  // inversion. In practice the listener is O(1) per record and
  // keeps pace with the tailer; the queue stays short.
  struct RemoteEvent {
    RemoteOp op;
    std::string file_name;
    std::vector<unsigned char> payload;
    RemoteAppendListener listener;
  };
  std::mutex dispatch_mtx_;
  std::condition_variable dispatch_cv_;
  // Signaled by the dispatcher when it finishes an event and the
  // queue is empty. drainDispatchQueue() (called when the listener
  // is cleared) waits on this so a teardown caller knows all events
  // that captured the old listener have finished.
  std::condition_variable drain_cv_;
  std::deque<RemoteEvent> dispatch_queue_;
  // True while the dispatcher is mid-invocation of a listener — i.e.
  // the event has been popped from dispatch_queue_ but the listener
  // hasn't returned yet. drainDispatchQueue() must block on this in
  // addition to an empty queue.
  bool dispatch_in_flight_ = false;
  std::thread dispatch_thread_;

  void startJvm(std::string const& jar_path, std::string const& jvm_opts);
  JNIEnv* attachThread();
  void detachThread();
  void loadBridge(std::string const& endpoint, std::string const& stream_name);
  long jniAppendEntry(JNIEnv* env, std::string const& file_name, int op,
                      unsigned char const* data, int length);
  bool applyEntryFromJava(JNIEnv* env, jbyteArray jbuf);
  // Core apply logic, takes already-JNI-extracted bytes. Shared by
  // applyEntryFromJava (single-entry path, kept for drainInitialEntries)
  // and applyBatchFromJava (steady-state path driven by pollBatch).
  bool applyEntryBytes(unsigned char const* data, size_t len);
  // Parse a pollBatch-formatted byte array (big-endian count +
  // length-prefixed entries) and apply each via applyEntryBytes.
  // Returns the number of entries successfully applied.
  int applyBatchFromJava(JNIEnv* env, jbyteArray jbuf);
  void drainInitialEntries();
  void tailerLoop();
  void dispatchLoop();
  void drainDispatchQueue();
  long globalFenceTarget();
  void waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target);
  void reconcilePendingFrontLocked(std::string const& fileName);

  // --- batch helpers (see struct Batch) ---
  // All *Locked helpers require mtx_ held by the caller.
  //
  // The batch owning cached_file_[fileName], created on first use.
  std::shared_ptr<Batch>& cachedBatchLocked(std::string const& fileName);
  // Move cached_file_[fileName] into its batch and hand it back for
  // submission. Returns null when the file has no buffered bytes.
  std::shared_ptr<Batch> takeCachedBatchLocked(std::string const& fileName);
  // Settle a batch that will never be spliced, and unblock the queue
  // behind it.
  void finishBatchLocked(std::string const& fileName,
                         std::shared_ptr<Batch> const& batch, Status result);
  // JNI submit + stamp + reconcile. Must be called with mtx_ NOT held:
  // it runs the JNI round-trip unlocked. Sets batch->done before it
  // returns, and returns batch->result.
  Status submitBatch(std::string const& fileName, std::shared_ptr<Batch> const& batch);
  // Block until `batch` is settled by whichever thread owns it, and
  // return its outcome. Takes mtx_ itself, so the caller must not hold it.
  Status awaitBatch(std::shared_ptr<Batch> const& batch);
  // Push every not-yet-sequenced byte of one file onto the shared log and
  // wait for the splice, so read()/size() can serve file_buffers_ alone.
  void drainForRead(std::string const& fileName);

  // The calling thread's sync() token. Static thread_local (member
  // thread_local is not a thing); the instance pointer scopes it to
  // one storage object.
  struct FenceToken {
    CorfuDBStorage const* instance = nullptr;
    long target = -1;
  };
  static thread_local FenceToken fence_token_;
  // Fence target for the current fenced read: the token's target when
  // this thread holds one for this instance, else a fresh (expensive)
  // globalFenceTarget() sample.
  long fenceTargetForCaller();
};

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
#endif  // CORFU_STORAGE_H
