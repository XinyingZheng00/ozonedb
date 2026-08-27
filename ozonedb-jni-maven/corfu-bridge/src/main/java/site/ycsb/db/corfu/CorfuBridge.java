package site.ycsb.db.corfu;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.IStreamView;

/**
 * Thin JNI-facing wrapper around CorfuRuntime + IStreamView.
 *
 * OzoneDB's C++ CorfuDBStorage loads this class via an embedded JVM and
 * drives Corfu through a small set of methods: the constructor,
 * {@link #append} / {@link #appendChecked}, {@link #pollBatch},
 * {@link #tailAddress}, the trim helpers, and {@link #close}. Keeping the
 * surface area tight minimizes JNI method lookups and simplifies lifetime
 * management on the C++ side.
 *
 * Entry format inside a {@link #pollBatch} batch: 8 bytes of big-endian
 * global log address followed by the raw entry bytes the C++ side appended.
 *
 * The bridge is one of two implementations of the C++ CorfuClient seam
 * (src/include/ozonedb/corfu_client.h); the other is the native C++ client
 * (PLAN-native-corfu.md). Entries written here must stay readable by that
 * client: raw file-name conflict keys, and payload codec NONE.
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

  // Returned by pollBatch when the poll view's next address is
  // below the log's trim mark (Corfu throws TrimmedException). A null
  // return keeps its meaning (timeout, nothing new); a ZERO-LENGTH array
  // is the marker. The C++ side restarts from a newer checkpoint during
  // bootstrap, and fail-stops a live tailer -- it never skips the trimmed
  // entries, which is why the poll view is NOT opened with
  // StreamOptions.ignoreTrimmed: that option drops them in silence.
  private static final byte[] TRIMMED = new byte[0];

  public CorfuBridge(String endpoint, String streamName) {
    // maxCacheEntries default is 2500 — too small, the tailer falls
    // into a cache-miss spiral (50–400 ms/entry) as soon as it's more
    // than 2500 behind the writer. 500K caps Corfu's ILogData LRU at
    // ~500 MB (at ~1 KB/LogData) while leaving plenty of headroom for
    // multi-writer bursts; tune via -Xmx in corfu_jvm_opts if needed.
    // Read batching for a tailer that is behind (the open-time replay of
    // a loaded stream, or a lagging tailer). Two Corfu parameters gate it
    // and BOTH default to 10: streamBatchSize is how many resolved
    // addresses one read-cache miss asks for (AddressSpaceView.getBatch),
    // and bulkReadSize is how many of those go into one log-unit RPC
    // (AddressSpaceView.fetchAll partitions by it). Raising only the
    // first changed nothing -- the request was split back into RPCs of
    // 10. With both at 10 a 1 M-entry loaded log replayed in 41 s, ~38 us
    // per entry, essentially all of it waiting on ~100 k sequential RPCs;
    // JNI-side batching of the drain made no difference. The address
    // queue is resolved from the sequencer's address map, so it is deep
    // enough for large batches; at 1 KB records a 1000-address RPC is
    // ~1 MB. A tailer that is caught up still fetches only what exists.
    // Override with -Dozonedb.corfu.streamBatchSize / .bulkReadSize.
    int streamBatchSize = Integer.getInteger("ozonedb.corfu.streamBatchSize", 1000);
    int bulkReadSize = Integer.getInteger("ozonedb.corfu.bulkReadSize", 1000);
    // Payload codec NONE. The runtime default is ZSTD
    // (CorfuRuntimeParameters.codecType): every entry was compressed once
    // at append and decompressed by every tailer, on every process, for
    // a payload that is already an OzoneDB record. The native C++ client
    // reads codec NONE only, so both clients must write it. Datasets
    // loaded before this change hold ZSTD entries and must be reloaded.
    CorfuRuntimeParameters params = CorfuRuntimeParameters.builder()
        .cacheDisabled(false)
        .maxCacheEntries(500_000L)
        .streamBatchSize(streamBatchSize)
        .bulkReadSize(bulkReadSize)
        .codecType(Codec.Type.NONE)
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

  // Abort codes returned in {@code appendChecked()[0]}. All negative, so
  // none can be mistaken for a log address. Mirrored by
  // CorfuDBStorage::CheckedAppend on the C++ side.
  public static final long ABORT_CONFLICT = -2;
  public static final long ABORT_NEW_SEQUENCER = -3;
  public static final long ABORT_SEQ_OVERFLOW = -4;
  public static final long ABORT_SEQ_TRIM = -5;
  public static final long ABORT_OTHER = -6;

  /**
   * Append whose token request carries conflict keys, so the sequencer
   * answers "was this file sealed since my snapshot?" at token time
   * (PLAN-trimming.md §0, the fast put path).
   *
   * {@code readKey} goes into the read set at snapshot {@code snapshotAddr}:
   * the sequencer refuses the token ({@link #ABORT_CONFLICT}) when that
   * key was written at an address ABOVE the snapshot
   * ({@code SequencerServer.txnCanCommit}). {@code writeKey} goes into the
   * write set: the sequencer records it at the token's address, at token
   * issue. A refused append gets no address and nothing lands in the log.
   * Either key may be null. The other abort types mean the sequencer
   * cannot answer for this snapshot (stale epoch or a snapshot below the
   * tail at sequencer bootstrap, below the conflict cache's eviction
   * mark, or below the trim mark); the caller then falls back to the
   * tailer wait.
   *
   * @return {@code {address, -1}} on success; {@code {ABORT_*, x}} on an
   *         abort, where x is the offending address (the last address at
   *         which the read key was written) for ABORT_CONFLICT and -1
   *         otherwise.
   */
  public long[] appendChecked(byte[] payload, long snapshotAddr, byte[] readKey, byte[] writeKey) {
    Map<UUID, Set<byte[]>> readSet = readKey == null
        ? Collections.<UUID, Set<byte[]>>emptyMap()
        : Collections.singletonMap(streamId, Collections.singleton(readKey));
    Map<UUID, Set<byte[]>> writeSet = writeKey == null
        ? Collections.<UUID, Set<byte[]>>emptyMap()
        : Collections.singletonMap(streamId, Collections.singleton(writeKey));
    long epoch = runtime.getLayoutView().getLayout().getEpoch();
    TxResolutionInfo info = new TxResolutionInfo(UUID.randomUUID(),
        new Token(epoch, snapshotAddr), readSet, writeSet);
    try {
      long addr = runtime.getStreamsView().append(payload, info, streamId);
      synchronized (pollSignal) {
        appendSeq++;
        pollSignal.notifyAll();
      }
      return new long[] {addr, -1};
    } catch (TransactionAbortedException e) {
      long code;
      if (e.getAbortCause() == null) {
        code = ABORT_OTHER;
      } else {
        switch (e.getAbortCause()) {
          case CONFLICT:
            code = ABORT_CONFLICT;
            break;
          case NEW_SEQUENCER:
            code = ABORT_NEW_SEQUENCER;
            break;
          case SEQUENCER_OVERFLOW:
            code = ABORT_SEQ_OVERFLOW;
            break;
          case SEQUENCER_TRIM:
            code = ABORT_SEQ_TRIM;
            break;
          default:
            code = ABORT_OTHER;
        }
      }
      Long offending = e.getOffendingAddress();
      return new long[] {code, offending == null ? -1 : offending};
    }
  }

  /**
   * @return the global log tail. One sequencer round trip. Used as the
   *         snapshot of a write-key-only append (SEAL, REMOVE) when the
   *         tailer's position was refused: the current tail passes every
   *         sequencer check except a stale epoch, which the runtime
   *         refreshes on its own.
   */
  public long globalTail() {
    return runtime.getSequencerView().query().getSequence();
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
   * Fetches up to {@code maxEntries} entries in a single JNI round-trip,
   * amortizing the cross-language and pollView.next() fixed overhead
   * across many log entries.
   *
   * Wire format (big-endian, mirrors Java DataOutputStream):
   *   int32  count
   *   count × (int32 entryLen, int64 globalAddress, raw entry bytes)
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
      ILogData data;
      try {
        data = pollView.next();
      } catch (TrimmedException e) {
        // Entries already collected were read above the mark at the time;
        // hand them over, and let the next call surface the marker (the
        // view's pointer does not advance, so next() throws again).
        if (!batch.isEmpty()) break;
        return TRIMMED;
      }
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

  /**
   * Mark every address {@code <= addr} trimmed, then ask the log units to
   * reclaim the space.
   *
   * {@code prefixTrim} alone frees nothing on disk: the log unit only
   * persists a new starting address. {@code gc()} sends {@code compact()}
   * to every log unit, which deletes the whole segments
   * ({@code RECORDS_PER_LOG_FILE} entries) below the mark; the last partial
   * segment stays until the mark passes it. Without the call the server
   * compacts on its own schedule (first after 10 min, then every 45 min).
   * The client read cache is pruned as well.
   *
   * @return the trim mark after the call: the FIRST UNTRIMMED address
   *         ({@code addr + 1} once every log unit has applied it).
   */
  public long prefixTrim(long addr) {
    long epoch = runtime.getLayoutView().getLayout().getEpoch();
    runtime.getAddressSpaceView().prefixTrim(new Token(epoch, addr));
    runtime.getAddressSpaceView().gc(addr + 1);
    runtime.getAddressSpaceView().gc();
    return trimMark();
  }

  /**
   * @return the first untrimmed address, as persisted by the log units
   *         (0 or -1 on a log that was never trimmed). Unlike the
   *         sequencer's in-memory mark this value survives a server
   *         restart, which is why bootstrap checks it.
   */
  public long trimMark() {
    return runtime.getAddressSpaceView().getTrimMark().getSequence();
  }

  /**
   * Position the poll view so that the next {@link #pollBatch} returns
   * the first entry of this stream at or above
   * {@code addr}. A joiner that restored checkpoint C calls this with C+1.
   * Seeking at or above the trim mark is what keeps a fresh view from
   * throwing {@code TrimmedException} on its first read.
   */
  public void seekPollView(long addr) {
    pollView.seek(addr);
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
