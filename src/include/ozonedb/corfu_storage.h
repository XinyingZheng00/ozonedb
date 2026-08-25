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
                 std::string const& db_path,
                 std::string const& log_prefix = "datalog",
                 bool track_versions = false);
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
  Status appendConditional(std::string const& fileName,
                           unsigned char const* data, int length,
                           int64_t expected_version,
                           int64_t& result_version) override;
  Status appendTransaction(std::string const& fileName,
                           unsigned char const* data, int length,
                           std::vector<ReadVersion> const& read_set,
                           int64_t& result_version) override;
  bool versionedLookup(std::string const& key, int64_t& version,
                       std::string& value, bool& has_value,
                       bool& deleted) override;

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
  // Wait for the dispatch thread to deliver every remote-append event
  // enqueued so far (see Storage::syncListeners). Takes dispatch_mtx_
  // only — never mtx_ — so it composes with a held fence token.
  void syncListeners() override;

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

  // Data-log file prefix ("datalog") — the apply loop parses APPEND
  // payloads under this prefix as Records to maintain key_versions_.
  std::string log_prefix_;

  // Version tracking (Metadata::track_versions). Off: the apply loop
  // is byte-for-byte the pre-CAS one — no payload is ever decoded and
  // own APPENDs are skipped outright. On: every data-log APPEND costs a
  // key-only decode (decodeRecordHeaders, run OUTSIDE mtx_) plus an
  // O(records) map update under it.
  bool track_versions_ = false;
  std::atomic<bool> warned_untracked_conditional_{false};

  // Log-ordered per-key version map, guarded by mtx_. addr is the global
  // log address of the key's last *accepted* write — every replica's
  // apply loop computes the identical map because entries are evaluated
  // in address order, own entries included (they are decoded for
  // version tracking even when their byte-apply is skipped). Rebuilt
  // from the stream on every open by drainInitialEntries, so it is
  // complete, not a cache. Memory: one entry per live key
  // (~key + 48 B), never evicted — a version must outlive the record's
  // move into an SSTable, or a CAS with expected_version = -1 could
  // wrongly succeed on a compacted key.
  //
  // Inline values. Every tracked write — blind or conditional — also
  // leaves the record's value in the map (has_value) so
  // DB::getVersioned can serve (value, version) as one atomic pair.
  // The reason is a window in the read path, not the map: a LOGCREATE
  // freezes the superseded tail's size from the rolling writer's
  // (lagging) view, and the scan of a sealed log file stops at that
  // size, so a record appended near a roll can be invisible to scans
  // until compaction rewrites the file in full into an SSTable
  // (Compaction reads storage->read(file), not the frozen size). A
  // blind put re-issues itself when it detects the roll, but the
  // version map already carries the first copy's address — a reader
  // that paired that fresh version with a stale scanned value would
  // let a transaction validate against a write it never saw (a lost
  // update), so the value is pinned here for blind writes too. A CAS
  // cannot re-issue at all (double-apply). Values are released by the
  // apply loop when the file holding them is REMOVEd, i.e. exactly
  // when the SSTable takes over (releaseInlineValuesLocked). The
  // retained set is therefore one copy of the not-yet-compacted log
  // tail (bounded by log_file_size_limit times the compaction lag),
  // not the working set. One exception keeps a value pinned until the
  // key is next written: the target file was already sealed/removed at
  // the entry's log position (the bytes may have missed the
  // compactor's read). Pinning is replica-local memory policy, never
  // an accept/reject input, so it need not be deterministic.
  struct KeyVersion {
    long addr = -1;
    bool has_value = false;
    bool deleted = false;
    std::string value;
    // Log file whose bytes carry the inline value; empty when there is
    // no inline value or it is pinned (see above).
    std::string file;
  };
  std::unordered_map<std::string, KeyVersion> key_versions_;
  // Keys holding an inline value per log file, consumed by
  // releaseInlineValuesLocked on that file's REMOVE. May contain keys
  // whose value has since moved on (re-checked against KeyVersion::file
  // at release), so it is a hint list, not an index.
  std::unordered_map<std::string, std::vector<std::string>> inline_keys_by_file_;

  // Outcome of this process's own conditional appends (single-key CAS
  // and transaction commit records alike), keyed by log address;
  // recorded by the apply loop, consumed (erased) by submitConditional
  // after its waitForTailerLocked returns. Guarded by mtx_. Bounded:
  // one in-flight entry per concurrent caller.
  std::unordered_map<long, bool> cas_outcomes_;

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
  // Monotone event counters, guarded by dispatch_mtx_: events pushed
  // by the tailer and events whose listener call has returned.
  // syncListeners() waits for delivered >= enqueued-as-of-the-call —
  // bounded by the queue length at that instant — rather than for an
  // idle dispatcher, which under sustained peer writes may never come.
  uint64_t dispatch_enqueued_ = 0;
  uint64_t dispatch_delivered_ = 0;
  std::thread dispatch_thread_;

  void startJvm(std::string const& jar_path, std::string const& jvm_opts);
  JNIEnv* attachThread();
  void detachThread();
  void loadBridge(std::string const& endpoint, std::string const& stream_name);
  // Serialize one CorfuEntry and hand it to CorfuBridge.append. A
  // non-null read_set selects the transaction encoding (read_set set,
  // expected_version unset); conditional without a read set is the
  // legacy single-key CAS. See CorfuEntry in record.proto.
  long jniAppendEntry(JNIEnv* env, std::string const& file_name, int op,
                      unsigned char const* data, int length,
                      bool conditional = false, long expected_version = -1,
                      std::vector<ReadVersion> const* read_set = nullptr);
  // Shared body of appendConditional and appendTransaction: sealed
  // check, append, wait for the own apply loop's verdict.
  Status submitConditional(std::string const& fileName,
                           unsigned char const* data, int length,
                           long expected_version,
                           std::vector<ReadVersion> const* read_set,
                           int64_t& result_version);
  // True for files whose APPEND payloads are decoded for version
  // tracking (data-log files: "<log_prefix_>/N").
  bool isVersionTracked(std::string const& file_name) const {
    return file_name.compare(0, log_prefix_.size(), log_prefix_) == 0;
  }
  // What the version map needs from one Record of a data-log payload:
  // the key, whether it is a tombstone, and where the value bytes sit
  // in the payload (copied only if a conditional entry is accepted).
  struct RecordHeader {
    std::string key;
    bool deleted = false;
    bool has_value = false;
    size_t value_off = 0;
    size_t value_len = 0;
  };
  // Key-only decode of a data-log payload (a sequence of varint-length-
  // prefixed Record protos, protobuf::serializeMessage's format) with
  // no Record materialized and no lock held. False on malformed input.
  static bool decodeRecordHeaders(std::string const& payload,
                                  std::vector<RecordHeader>& out);
  // Version effects of one APPEND entry at its log position; all run
  // with mtx_ held from applyEntryBytes. The conditional variant
  // returns acceptance (and records it in cas_outcomes_ for own
  // entries); a decode failure of a non-empty payload rejects,
  // identically on every replica.
  void applyBlindVersionsLocked(::CorfuEntry const& entry,
                                std::vector<RecordHeader> const& recs, long addr);
  bool applyConditionalLocked(::CorfuEntry const& entry, bool decode_ok,
                              std::vector<RecordHeader> const& recs,
                              long addr, bool own);
  // One record of an applied entry: version, tombstone flag, inline
  // value and the file bookkeeping that lets REMOVE release the value.
  void applyRecordVersionLocked(::CorfuEntry const& entry,
                                RecordHeader const& h, long addr);
  // Version of `key` at the current apply position, -1 if unwritten.
  long currentVersionLocked(std::string const& key) const {
    auto it = key_versions_.find(key);
    return it == key_versions_.end() ? -1 : it->second.addr;
  }
  // Drop the inline values whose bytes lived in `file` (REMOVE applied).
  void releaseInlineValuesLocked(std::string const& file);
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
