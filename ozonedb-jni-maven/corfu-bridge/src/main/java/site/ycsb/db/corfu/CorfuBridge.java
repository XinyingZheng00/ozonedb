package site.ycsb.db.corfu;

import java.nio.ByteBuffer;
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
  private final IStreamView streamView;
  private volatile boolean closed = false;

  public CorfuBridge(String endpoint, String streamName) {
    CorfuRuntimeParameters params = CorfuRuntimeParameters.builder().build();
    this.runtime = CorfuRuntime.fromParameters(params)
        .parseConfigurationString(endpoint)
        .connect();
    UUID streamId = CorfuRuntime.getStreamID(streamName);
    this.streamView = runtime.getStreamsView().get(streamId);
  }

  /**
   * Appends a pre-serialized payload to the Corfu stream.
   *
   * @return the global log address assigned to the entry.
   */
  public long append(byte[] payload) {
    return streamView.append(payload);
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
      ILogData data = streamView.next();
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
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  /** @return the last known global log tail (for read-after-write waits). */
  public long tailAddress() {
    return runtime.getSequencerView().query().getSequence();
  }

  public void close() {
    closed = true;
    try {
      runtime.shutdown();
    } catch (Exception ignored) {
    }
  }
}
