#ifndef CORFU_STORAGE_H
#define CORFU_STORAGE_H
#ifdef OZONEDB_ENABLE_CORFU
#include "checkpoint.h"
#include "corfu_client.h"
#include "storage.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ozonedb {

/**
 * @brief Storage backend backed by a single CorfuDB stream.
 *
 * All ozonedb files (logs, sstables, metadata) are packed into one shared
 * Corfu stream. Each log entry is a CorfuEntry protobuf carrying a filename,
 * an opcode (APPEND / SEAL / REMOVE) and a payload.
 *
 * The process reaches Corfu through a CorfuClient (corfu_client.h): either
 * the embedded JVM + site.ycsb.db.corfu.CorfuBridge (corfu_client = jni) or
 * the C++ client in src/db/corfu/ (corfu_client = native). Everything
 * below the seam -- locks, batches, the ack rule, the tailer -- is the
 * same for both.
 *
 * A background tailer thread continually polls new entries and reconstructs
 * per-file buffers plus a sealed-files set. Reads wait for the tailer to
 * catch up to the writer's last-known address (read-my-writes within a
 * single process).
 */
class CorfuDBStorage : public Storage {
 public:
  // checkpoint_store: where LogTrimmer writes checkpoints (the SSTable
  // object store). When non-null, the constructor loads the newest
  // checkpoint from <checkpoint_dir>/LATEST and replays only the stream
  // entries above it. When null, it replays from address 0 -- and throws
  // if the log was trimmed, because no replay can then be complete.
  // fast_ack: ack a data append as soon as the sequencer granted its
  // token (see submitBatchFast) instead of after the tailer applied it.
  // Off only for A/B measurement; SEAL and REMOVE entries carry their
  // conflict key either way, so a process with the fast path off never
  // blinds a peer that has it on.
  CorfuDBStorage(CorfuClientOptions const& client_options,
                 std::string const& db_path,
                 Storage* checkpoint_store = nullptr,
                 std::string const& checkpoint_dir = "checkpoint",
                 bool fast_ack = true);
  // The pre-seam signature: the JNI client with these jar and JVM
  // options. Kept for the tests and the smoke binaries.
  CorfuDBStorage(std::string const& endpoint,
                 std::string const& jar_path,
                 std::string const& jvm_opts,
                 std::string const& stream_name,
                 std::string const& db_path,
                 Storage* checkpoint_store = nullptr,
                 std::string const& checkpoint_dir = "checkpoint",
                 bool fast_ack = true);
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

  long lastAppendAddressForThread() const override { return last_append_addr_; }

  // --- checkpoints and trimming (PLAN-trimming.md §3) ---
  //
  // The exact live state at one address, for LogTrimmer. Takes write_gate_
  // exclusively, waits until the tailer has passed this process's own last
  // write, and copies file_buffers_ + the sealed/removed sets under mtx_.
  // covered_addr is -1 when no exact state could be taken (tailer stalled,
  // or the storage fail-stopped).
  checkpoint::State takeSnapshot();
  // Corfu prefixTrim(addr) + log-unit compaction. False on a JNI error.
  bool prefixTrim(long addr);
  // First untrimmed address as persisted by the log units; -1 on error.
  long trimMark();
  // Tail of this stream (max of the sequencer's per-stream tail and our
  // own last write). One sequencer round-trip.
  long streamTail();
  // True once the tailer hit a trimmed address (it fell more than one
  // trim cycle behind). Every write then fails and every read misses.
  bool trimmedOut() const { return trimmed_out_.load(); }
  long lastAppliedAddr() const { return last_applied_addr_.load(std::memory_order_acquire); }
  // Peer APPENDs this process dropped because they were sequenced above
  // their file's SEAL (applyEntryBytes). Every process drops the same
  // ones. The writer that produced such an entry acked it if its tailer
  // had not applied the SEAL yet — see submitBatch's ack rule.
  uint64_t droppedAboveSeal() const { return dropped_above_seal_.load(std::memory_order_relaxed); }
  uint64_t droppedAboveSealBytes() const { return dropped_above_seal_bytes_.load(std::memory_order_relaxed); }
  // Ack-path counters (see submitBatchFast). fast: acked on the
  // sequencer's answer; slow: acked after the tailer wait; conflict: the
  // sequencer refused a token because the file's key was written above
  // the snapshot; spurious: a refusal that named an address holding no
  // SEAL/REMOVE of the file (the key's write became a hole).
  uint64_t fastAcks() const { return fast_acks_.load(std::memory_order_relaxed); }
  uint64_t slowAcks() const { return ack_wait_batches_.load(std::memory_order_relaxed); }
  uint64_t conflictAborts() const { return conflict_aborts_.load(std::memory_order_relaxed); }
  uint64_t spuriousConflicts() const { return spurious_conflicts_.load(std::memory_order_relaxed); }
  bool loadedFromCheckpoint() const { return loaded_from_checkpoint_; }
  // Release the calling thread's client resources (the JVM attachment
  // under the JNI client). A thread that drove this storage (LogTrimmer's)
  // must call this before it exits.
  void detachThread();
  // The client name this storage runs on ("jni" or "native").
  std::string const& clientName() const { return client_name_; }

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
  // The Corfu client (corfu_client.h). Owned; every thread calls it
  // directly, the client serializes what its runtime needs.
  std::unique_ptr<CorfuClient> client_;
  std::string client_name_;

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
  //   - the sequencer granted the token with the file's
  //     conflict key in the read set (submitBatchFast)   -> kSuccess
  //   - the tailer applied the entry at its address     -> kSuccess
  //   - the JNI submit fails                             -> kFailure
  //   - the file was sealed at a LOWER global address    -> kSealed
  //   - a peer REMOVE dropped the file                   -> kFailure
  //   - the tailer did not reach the address in time     -> kFailure
  // A caller may only ack a write after `done` is true, and must return
  // `result`, never an assumed kSuccess.
  //
  // `done` means "the bytes at addr belong to fileName in every process":
  // either the sequencer proved it at token time (fast path) or the
  // tailer applied the entry and reported it (slow path). It never means
  // "sequenced, outcome assumed". The tailer stays the only writer of
  // file_buffers_ (own entries included, see applyEntryBytes), which is
  // what keeps every process's copy of a file in the same, address,
  // order; and reads fence on last_written_addr_ (globalFenceTarget), so
  // an acked write is visible to the reads that follow it either way.
  struct Batch {
    long addr = -1;  // global Corfu address, stamped once the outcome is known
    std::vector<unsigned char> payload;
    bool done = false;
    Status result = Status::kFailure;
  };

  // Batches handed to JNI whose outcome is not known yet, per file. Only
  // used to settle in-flight batches when the file is REMOVEd under them.
  std::unordered_map<std::string, std::list<std::shared_ptr<Batch>>> pending_;

  // The batch that currently owns the bytes sitting in cached_file_[fn].
  // Created when the first byte enters cached_file_[fn] and moved into
  // pending_ on drain. A thread that adds bytes takes a shared_ptr to it,
  // so it can learn the outcome even when a different thread does the
  // drain and the JNI submit.
  std::unordered_map<std::string, std::shared_ptr<Batch>> cached_batch_;

  // Global address of the SEAL entry for a file, once the tailer has seen
  // it (or we appended it). The shared log — not local state — decides
  // whether an APPEND belongs to a file: an APPEND sequenced ABOVE this
  // address arrived after the seal and belongs to no file. The tailer
  // applies that rule to every entry, own ones included, in address
  // order (applyEntryBytes); submitBatch reads the result after the
  // tailer passed its address, so a kSealed retry can never duplicate a
  // record and an acked record is never one that peers dropped.
  std::unordered_map<std::string, long> sealed_at_addr_;

  // The write gate. Since the tailer is the only writer of file_buffers_
  // and applies in address order, file_buffers_ under mtx_ is always
  // exactly the state at last_applied_addr_. The gate keeps local submit
  // paths out while takeSnapshot waits for the tailer to pass
  // last_written_addr_ and copies, so the snapshot cannot be starved by
  // a stream of own appends that keep moving that mark.
  //
  // Every path that pushes a batch into pending_ or appends to the log
  // (append, appendInBatch, flush, drainForRead, seal, remove) holds the
  // gate SHARED from its entry until its outcome is known; takeSnapshot
  // holds it EXCLUSIVE. Lock order: write_gate_ before mtx_, never the
  // reverse; nothing holding mtx_ ever waits for the gate. Never take the
  // gate twice on one thread (std::shared_mutex is not recursive):
  // submitBatch and takeCachedBatchLocked assume the caller already holds
  // it.
  std::shared_mutex write_gate_;
  // Set by failStop() when the tailer hit a trimmed address. Sticky.
  std::atomic<bool> trimmed_out_{false};
  bool loaded_from_checkpoint_ = false;
  // Highest X carried by a TRIM entry this process applied (see
  // CorfuEntry.Op.TRIM); -1 when none. Guarded by mtx_. bootstrap
  // compares it with the address its replay started at.
  long trim_marker_addr_ = -1;

  std::mutex mtx_;
  std::condition_variable batch_flushed_cv_;
  std::condition_variable tailer_cv_;
  std::chrono::system_clock::time_point last_commited_time_;

  std::atomic<long> last_applied_addr_{-1};
  std::atomic<long> last_written_addr_{-1};

  // Ack-wait diagnostics (see submitBatch): how long acks waited for the
  // tailer, and how many batches turned out to sit above a SEAL that was
  // learned only during that wait. Each of those was an acked-then-lost
  // record before the wait existed. Printed once at teardown.
  std::atomic<uint64_t> ack_wait_batches_{0};
  std::atomic<uint64_t> ack_wait_us_total_{0};
  std::atomic<uint64_t> ack_wait_us_max_{0};
  std::atomic<uint64_t> sealed_after_wait_{0};
  std::atomic<uint64_t> dropped_above_seal_{0};
  std::atomic<uint64_t> dropped_above_seal_bytes_{0};
  // Fast-path counters (submitBatchFast). slow_path_* count the sequencer
  // answers that sent a batch to the tailer wait, by abort type.
  bool fast_ack_ = true;
  std::atomic<uint64_t> fast_acks_{0};
  std::atomic<uint64_t> conflict_aborts_{0};
  std::atomic<uint64_t> spurious_conflicts_{0};
  std::atomic<uint64_t> slow_path_newseq_{0};
  std::atomic<uint64_t> slow_path_overflow_{0};
  std::atomic<uint64_t> slow_path_trim_{0};
  std::atomic<uint64_t> slow_path_other_{0};
  std::atomic<uint64_t> slow_path_stalled_{0};

  // Randomly-generated id that tags every APPEND this process writes to
  // the shared stream. The tailer applies own and peer entries alike; the
  // id only decides whether the remote-append listener is notified (own
  // records were already indexed by the writer path).
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
    long addr = -1;
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

  // One CorfuEntry, serialized. Shared by both append flavours.
  std::string serializeEntry(std::string const& file_name, int op,
                             unsigned char const* data, int length) const;
  // Plain append: token without conflict keys. The outcome of an APPEND
  // sent this way is known only once the tailer passed its address.
  long appendEntry(std::string const& file_name, int op,
                   unsigned char const* data, int length);
  // Outcome of CorfuClient::appendChecked (see corfu_client.h).
  using CheckedAppend = CorfuClient::CheckedAppend;
  static constexpr long kAbortConflict = CorfuClient::kAbortConflict;
  static constexpr long kAbortNewSequencer = CorfuClient::kAbortNewSequencer;
  static constexpr long kAbortSeqOverflow = CorfuClient::kAbortSeqOverflow;
  static constexpr long kAbortSeqTrim = CorfuClient::kAbortSeqTrim;
  static constexpr long kAbortOther = CorfuClient::kAbortOther;
  // The conflict key of a file: its name. Names are never reused.
  // read_key puts it in the read set at `snapshot`; write_key puts it in
  // the write set. See CorfuClient::appendChecked.
  CheckedAppend appendChecked(std::string const& file_name, int op,
                              unsigned char const* data, int length,
                              long snapshot, bool read_key, bool write_key);
  // Append a payload-less entry (SEAL, REMOVE) with the file's key in the
  // write set, so the sequencer records it. Retries a refused snapshot
  // with the global tail. Returns the address, -1 when the sequencer
  // refused every attempt (the entry was NOT appended).
  long appendKeyed(std::string const& file_name, int op);
  // Raise last_written_addr_ to addr (monotonic).
  void publishWrittenAddr(long addr);
  // Apply one stream entry (the CorfuEntry bytes at global address addr)
  // to the local state. Called from the client's poll sink, on the
  // tailer thread (steady state) or the constructor's thread (replay).
  // The only writer of file_buffers_, in address order.
  bool applyEntry(long addr, unsigned char const* data, size_t len);
  // Replay from the poll view's current position to the tail. kTrimmed
  // when the view hit a trimmed address (the caller restarts from a newer
  // checkpoint); kError on a client failure (the caller must not treat
  // the partial state as complete).
  enum class DrainResult { kOk,
                           kTrimmed,
                           kError };
  DrainResult drainInitialEntries();
  // Constructor-time replay: newest checkpoint (if a store is given), seek
  // to covered_addr + 1, drain, then check the trim mark. Throws when the
  // log is trimmed past what this process could load.
  void bootstrap(Storage* checkpoint_store, std::string const& checkpoint_dir);
  void resetStateLocked();
  void restoreSnapshot(checkpoint::State&& state);
  bool seekPollView(long addr);
  // Append the TRIM entry for X. Returns its address, -1 on failure.
  long appendTrimMarker(long addr);
  // The tailer fell below the trim mark: mark the storage dead, settle
  // every waiting writer with kFailure, wake every waiter.
  void failStop(char const* why);
  void tailerLoop();
  void dispatchLoop();
  void drainDispatchQueue();
  long globalFenceTarget();
  void waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target);
  // Same wait with an explicit bound; true when the tailer reached target.
  bool waitForTailerLocked(std::unique_lock<std::mutex>& lock, long target,
                           std::chrono::milliseconds timeout);
  // True when a SEAL for fileName is known at an address below addr.
  bool sealedBelowLocked(std::string const& fileName, long addr) const;

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
  // Client submit, then wait until the tailer has passed the new address
  // and read the outcome. Must be called with mtx_ NOT held: it runs the
  // Corfu round-trip unlocked. Sets batch->done before it returns, and
  // returns batch->result. The caller holds write_gate_ shared (see
  // write_gate_).
  Status submitBatch(std::string const& fileName, std::shared_ptr<Batch> const& batch);
  // The fast path of submitBatch: keyed append, acked on the sequencer's
  // answer. True when the batch is settled (batch->done); false when the
  // sequencer could not answer and submitBatch must take the slow path.
  // Same locking contract as submitBatch.
  bool submitBatchFast(std::string const& fileName,
                       std::shared_ptr<Batch> const& batch);
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

  // Global address of this thread's last successful append. Set by
  // append/appendInBatch/flush on the way out, read through
  // lastAppendAddressForThread() by LogHandler so it can rank the
  // record it just wrote against records the tailer delivers later.
  static thread_local long last_append_addr_;
  // Fence target for the current fenced read: the token's target when
  // this thread holds one for this instance, else a fresh (expensive)
  // globalFenceTarget() sample.
  long fenceTargetForCaller();
};

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
#endif  // CORFU_STORAGE_H
