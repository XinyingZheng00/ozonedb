#ifndef OZONEDB_CORFU_CLIENT_H
#define OZONEDB_CORFU_CLIENT_H
#ifdef OZONEDB_ENABLE_CORFU
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace ozonedb {

// Everything CorfuDBStorage needs to reach one Corfu stream. Filled by
// DB::DB from Metadata (the corfu_* config keys) or by a test directly.
struct CorfuClientOptions {
  std::string endpoint;      // "host:port" of the single Corfu node
  std::string stream_name;   // the OzoneDB stream (corfu_stream_name)
  // "native": the C++ client in src/db/corfu/ (PLAN-native-corfu.md), the
  // default since the phase 5 campaign of 2026-08-27. "jni": the embedded
  // JVM + CorfuBridge.java (corfu_client_jni.cpp), kept for A/B runs.
  std::string client = "native";
  std::string jar_path;      // jni only: the corfu-bridge fat jar
  std::string jvm_opts;      // jni only: extra -X / -D options
  // native only. Defaults mirror the Java runtime as the bridge sets it.
  int read_batch = 1000;             // addresses per ReadLogRequest
  int idle_poll_ms = 5;              // wait between stream-tail queries
  int hole_fill_timeout_ms = 10000;  // EMPTY address -> HOLE write
  int request_timeout_ms = 5000;     // per RPC
};

/**
 * @brief The eleven operations CorfuDBStorage drives a Corfu stream with.
 *
 * One instance = one client id on one stream. Every method may be called
 * from any thread, concurrently: writers append while the tailer polls
 * and LogTrimmer trims. Implementations serialize internally where the
 * underlying runtime needs it.
 *
 * Addresses are global Corfu log addresses. A negative return from a
 * method that yields an address means "not appended" / "unknown": the
 * caller must never treat it as a position in the log.
 *
 * pollBatch delivers entries through the sink in address order, exactly
 * once per address per seek position, and never skips a trimmed address
 * in silence (it reports kTrimmed instead; CorfuDBStorage then restarts
 * from a newer checkpoint, or fail-stops a live tailer).
 */
class CorfuClient {
 public:
  // Abort codes of appendChecked (mirrors CorfuBridge.ABORT_*). All
  // negative, so none can be mistaken for a log address.
  static constexpr int64_t kAbortConflict = -2;
  static constexpr int64_t kAbortNewSequencer = -3;
  static constexpr int64_t kAbortSeqOverflow = -4;
  static constexpr int64_t kAbortSeqTrim = -5;
  static constexpr int64_t kAbortOther = -6;

  // Outcome of appendChecked. addr >= 0 on success; else abort holds one
  // of the kAbort* codes and offending the conflicting address for
  // kAbortConflict (the last address at which the read key was written),
  // -1 otherwise. A refused append consumed no address.
  struct CheckedAppend {
    int64_t addr = -1;
    int64_t abort = 0;
    int64_t offending = -1;
  };

  // Result of one pollBatch call.
  //   kEntries: the sink received >= 1 entry.
  //   kIdle:    nothing new before the timeout; the sink was not called.
  //   kTrimmed: the next address is below the trim mark. Nothing was
  //             delivered by this call; the cursor did not move.
  //   kError:   the client failed (a Java exception, a dead connection).
  //             Entries delivered before the failure stay delivered.
  enum class Poll { kEntries,
                    kIdle,
                    kTrimmed,
                    kError };

  // Receives one entry: its global address and the raw bytes the writer
  // appended (the CorfuEntry protobuf). `data` is valid only for the
  // duration of the call.
  using EntrySink = std::function<void(int64_t addr, unsigned char const* data, size_t len)>;

  virtual ~CorfuClient() = default;

  // Plain append: a token without conflict keys, then the write. Returns
  // the address, -1 when nothing was appended.
  virtual int64_t append(std::string_view payload) = 0;

  // Append whose token request carries conflict keys, so the sequencer
  // answers "was this key written above `snapshot`?" at token time.
  // read_key goes into the read set at `snapshot`, write_key into the
  // write set (recorded at the new address). Either may be null. The
  // keys are raw bytes, not hashed: a Java and a native writer must
  // collide on the same bytes (CorfuBridge.appendChecked builds the
  // TxResolutionInfo directly, so this is what the bridge sends).
  virtual CheckedAppend appendChecked(std::string_view payload, int64_t snapshot,
                                      std::string_view const* read_key,
                                      std::string_view const* write_key) = 0;

  // The global log tail (last issued address), -1 on error. One
  // sequencer round trip.
  virtual int64_t globalTail() = 0;

  // The tail of THIS stream, not the global tail: the highest address
  // the tailer can ever deliver. -6 (Address.NON_EXIST) when the stream
  // has no entry, -1 on error.
  virtual int64_t streamTail() = 0;

  // Deliver up to max_entries entries at and above the cursor. Returns
  // as soon as it has at least one entry and the next is not ready --
  // it never pads a batch. An own append made through this client wakes
  // a waiting poll without the idle tick.
  virtual Poll pollBatch(int timeout_ms, int max_entries, EntrySink const& sink) = 0;

  // Move the cursor so that the next pollBatch delivers the first entry
  // of the stream at or above addr. No RPC. False on error.
  virtual bool seek(int64_t addr) = 0;

  // Hint that every address below mark was applied: the client may drop
  // bookkeeping for them. No-op for a client that keeps none.
  virtual void gc(int64_t mark) = 0;

  // Mark every address <= addr trimmed on the sequencer and the log
  // unit, compact, and return the log unit's trim mark (first untrimmed
  // address). -1 on error.
  virtual int64_t prefixTrim(int64_t addr) = 0;

  // The first untrimmed address as persisted by the log unit (0 or -1 on
  // a log that was never trimmed). -1 on error.
  virtual int64_t trimMark() = 0;

  // Release per-thread resources of the calling thread (the JVM
  // attachment). A thread that used this client and is about to exit
  // must call it. No-op for the native client.
  virtual void detachThread() {}

  // Stop the client. Every later call fails. Idempotent.
  virtual void close() = 0;
};

// The client selected by options.client. Throws std::runtime_error on an
// unknown client name or when the client cannot connect.
std::unique_ptr<CorfuClient> makeCorfuClient(CorfuClientOptions const& options);

// The embedded-JVM client (src/db/corfu_client_jni.cpp).
std::unique_ptr<CorfuClient> makeJniCorfuClient(CorfuClientOptions const& options);

#ifdef OZONEDB_CORFU_NATIVE
// The C++ client (src/db/corfu/native_corfu_client.cpp).
std::unique_ptr<CorfuClient> makeNativeCorfuClient(CorfuClientOptions const& options);
#endif

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
#endif  // OZONEDB_CORFU_CLIENT_H
