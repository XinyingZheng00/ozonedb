package site.ycsb.db.corfu;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.view.stream.IStreamView;

/**
 * Thin JNI-facing wrapper around CorfuRuntime + IStreamView.
 *
 * OzoneDB's C++ CorfuDBStorage loads this class via an embedded JVM and
 * drives Corfu through just five methods: the constructor, {@link #append},
 * {@link #pollNext}, {@link #tailAddress}, and {@link #close}. Keeping the
 * surface area tight minimizes JNI method lookups and simplifies lifetime
 * management on the C++ side.
 *
 * Payload format returned by {@link #pollNext}: 8 bytes of big-endian global
 * log address followed by the raw entry bytes the C++ side appended.
 *
 * Corfu's Java API has drifted across releases; this file targets the
 * 0.3.x line. Adjust imports / method calls if upgrading.
 */
public class CorfuBridge {
  private final CorfuRuntime runtime;
  // Separate stream views for append and poll so Corfu's
  // ThreadSafeStreamView monitor is NOT shared across the two
  // call-sites. Sharing one view meant an in-flight append() (which
  // holds the monitor for the full server round-trip) blocked the
  // tailer's next() call, capping tailer throughput at roughly
  // "one poll per gap between appends." Two views == two monitors
  // == writer and tailer run concurrently. Both views subscribe to
  // the same stream UUID so the poll view sees every entry the
  // append view writes (ordered via the Corfu sequencer).
  private final IStreamView appendView;
  private final IStreamView pollView;
  // The stream both views subscribe to. Needed by tailAddress(): a fence
  // target must be an address on THIS stream, because that is the only
  // thing the tailer advances over.
  private final UUID streamId;
  private volatile boolean closed = false;

  // Poller wake-up channel. When the stream has nothing new the poller
  // used to Thread.sleep(IDLE_POLL_MS); a caller waiting for its OWN
  // entry to be applied (CorfuDBStorage::appendConditional always does
  // -- a compare-and-put is decided by the local tailer) paid up to one
  // full sleep on every call. append() now bumps appendSeq under this
  // monitor and notifies; the poller snapshots appendSeq before each
  // pollView.next() and only parks if nothing local landed in between,
  // so an own append is never missed and never waits. Peer appends
  // still surface at the idle cadence -- a tailer that is already
  // behind them is never parked.
  private static final long IDLE_POLL_MS = 5;
  private final Object pollSignal = new Object();
  private long appendSeq = 0;  // guarded by pollSignal

  public CorfuBridge(String endpoint, String streamName) {
    // maxCacheEntries default is 2500 — too small, the tailer falls
    // into a cache-miss spiral (50–400 ms/entry) as soon as it's more
    // than 2500 behind the writer. 500K caps Corfu's ILogData LRU at
    // ~500 MB (at ~1 KB/LogData) while leaving plenty of headroom for
    // multi-writer bursts; tune via -Xmx in corfu_jvm_opts if needed.
    CorfuRuntimeParameters params = CorfuRuntimeParameters.builder()
        .cacheDisabled(false)
        .maxCacheEntries(500_000L)
        .build();
    this.runtime = CorfuRuntime.fromParameters(params)
        .parseConfigurationString(endpoint)
        .connect();
    this.streamId = CorfuRuntime.getStreamID(streamName);
    this.appendView = runtime.getStreamsView().get(streamId);
    this.pollView = runtime.getStreamsView().get(streamId);
  }

  /**
   * Appends a pre-serialized payload to the Corfu stream.
   *
   * @return the global log address assigned to the entry.
   */
  public long append(byte[] payload) {
    long addr = appendView.append(payload);
    synchronized (pollSignal) {
      appendSeq++;
      pollSignal.notifyAll();
    }
    return addr;
  }

  private long appendSeqSnapshot() {
    synchronized (pollSignal) {
      return appendSeq;
    }
  }

  /**
   * Park until a local append lands or {@link #IDLE_POLL_MS} pass,
   * unless one already landed since {@code seenSeq} was snapshotted.
   *
   * @return {@code false} if interrupted (the caller should give up).
   */
  private boolean awaitAppendOrTick(long seenSeq) {
    synchronized (pollSignal) {
      if (appendSeq != seenSeq) return true;
      try {
        pollSignal.wait(IDLE_POLL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  /**
   * Blocking-ish read of the next entry in the stream.
   *
   * @param timeoutMs maximum time to wait before returning {@code null}
   * @return {@code null} on timeout; otherwise a byte[] whose first 8 bytes
   *         are the entry's global log address (big-endian) and whose
   *         remaining bytes are the raw payload appended by a writer.
   */
  public byte[] pollNext(long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!closed) {
      long seq = appendSeqSnapshot();
      ILogData data = pollView.next();
      if (data != null) {
        long addr = data.getGlobalAddress();
        Object raw = data.getPayload(runtime);
        if (!(raw instanceof byte[])) {
          // A non-byte[] entry means something other than CorfuBridge wrote
          // to this stream; skip it to stay robust against mixed clients.
          continue;
        }
        byte[] payload = (byte[]) raw;
        ByteBuffer buf = ByteBuffer.allocate(8 + payload.length);
        buf.putLong(addr);
        buf.put(payload);
        return buf.array();
      }
      if (System.currentTimeMillis() >= deadline) return null;
      if (!awaitAppendOrTick(seq)) return null;
    }
    return null;
  }

  /**
   * Batched variant of pollNext. Fetches up to {@code maxEntries}
   * entries in a single JNI round-trip, amortizing the cross-language
   * and pollView.next() fixed overhead across many log entries. Under
   * warm-cache steady state this gives the tailer roughly a 10–50x
   * higher entry-apply rate than per-call pollNext.
   *
   * Wire format (big-endian, mirrors Java DataOutputStream):
   *   int32  count
   *   count × (int32 entryLen, entryLen bytes same-as-pollNext-payload)
   *
   * @return {@code null} if no entries were available before
   *         {@code timeoutMs} elapsed; otherwise a length-prefixed
   *         concatenation of 1..maxEntries entries.
   */
  public byte[] pollBatch(long timeoutMs, int maxEntries) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    List<byte[]> batch = new ArrayList<>(Math.min(maxEntries, 256));
    while (!closed && batch.size() < maxEntries) {
      long seq = appendSeqSnapshot();
      ILogData data = pollView.next();
      if (data == null) {
        // No entry ready right now. If we already have some, return
        // them immediately — holding them back to pad a full batch
        // adds latency for no throughput gain.
        if (!batch.isEmpty()) break;
        if (System.currentTimeMillis() >= deadline) return null;
        if (!awaitAppendOrTick(seq)) return null;
        continue;
      }
      long addr = data.getGlobalAddress();
      Object raw = data.getPayload(runtime);
      if (!(raw instanceof byte[])) {
        continue;  // skip foreign entries on a mixed stream
      }
      byte[] payload = (byte[]) raw;
      ByteBuffer entry = ByteBuffer.allocate(8 + payload.length);
      entry.putLong(addr);
      entry.put(payload);
      batch.add(entry.array());
    }
    if (batch.isEmpty()) return null;
    int totalLen = 4;  // count header
    for (byte[] e : batch) totalLen += 4 + e.length;
    ByteBuffer out = ByteBuffer.allocate(totalLen);
    out.putInt(batch.size());
    for (byte[] e : batch) {
      out.putInt(e.length);
      out.put(e);
    }
    return out.array();
  }

  /**
   * @return the tail address of THIS stream (for read-after-write waits).
   *
   * Must be the stream tail, not the global log tail. The C++ tailer waits
   * until it has applied every address up to this value, but it advances
   * only over this stream. A global tail includes addresses belonging to
   * other streams -- and any hole -- which this stream will never deliver,
   * so the wait could never be satisfied and every fenced read in the
   * process blocked forever. {@code query(UUID)} returns the per-stream
   * tail, which the tailer does reach.
   */
  public long tailAddress() {
    return runtime.getSequencerView().query(streamId);
  }

  /**
   * Prune the poll view's internal {@code resolvedQueue}/{@code readQueue}
   * (TreeSet&lt;Long&gt;) of addresses strictly below {@code trimMark}. Without
   * this, those queues grow by one Long per applied entry forever — a
   * stream that has seen ~1M entries accumulates ~85 MB of TreeMap state.
   *
   * Corfu's {@code gc()} is two-phase: the first call records the trim mark,
   * the next call (once the stream pointer has advanced past it) actually
   * clears the queues. So we must call this repeatedly; one-shot doesn't
   * prune. Safe to pass {@code lastAppliedAddress} as the trim mark — the
   * tailer never re-reads already-applied addresses via this view.
   */
  public void gcPollView(long trimMark) {
    pollView.gc(trimMark);
  }

  public void close() {
    closed = true;
    synchronized (pollSignal) {
      pollSignal.notifyAll();
    }
    try {
      runtime.shutdown();
    } catch (Exception ignored) {
    }
  }
}
