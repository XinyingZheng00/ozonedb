package site.ycsb.db;

import jni.OzoneDBJNI;
import site.ycsb.workloads.CoreWorkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Standalone driver for the consistency experiments, mirroring the
 * cr-sqlite bench's probe-staleness / check-lost-updates / check-convergence
 * subcommands. It talks to OzoneDB through the exact same path as the YCSB
 * binding (OzoneDBJNI over libozonedb.so), so what it measures is what the
 * benchmark exercises.
 *
 * <p>One process = one DB instance = one "writer" in ozonedb's multi-writer
 * model. bench/scripts/consistency.py launches several of these with a
 * file-based ready/go barrier and post-processes their CSV/JSON outputs into
 * the same summary.json contract the cr-sqlite repo's plot scripts consume.
 *
 * <p>Modes (first positional argument):
 * <ul>
 *   <li>write-probe: bump an 8-byte big-endian sequence under --key at
 *       --rate writes/s for --duration-s; emit commits.csv (seq,t_commit_ns).
 *       t_commit_ns is System.nanoTime() taken AFTER put() returns, i.e.
 *       after the record is durably ordered in the shared log.
 *   <li>read-probe: poll --key every --read-interval-ms for --duration-s;
 *       emit reads.csv (t_read_ns,seq_seen). Reads that find no value are
 *       skipped, matching the cr-sqlite probe.
 *   <li>counter-worker: --increments read-modify-write increments of --key
 *       (get, +1, put), optionally paced at --rate; emit worker-N.json.
 *   <li>insert-probe: insert brand-new YCSB-keyspace keys at --rate for
 *       --duration-s; after each put() returns, append "i,t_ack_ns" to
 *       --notify-file (flushed per line) and record it in inserts.csv.
 *       Touches --done-file when finished. New keys make "not found" an
 *       unambiguous staleness signal: there is no older value to serve.
 *   <li>visibility-probe: tail --notify-file; each notified key joins a
 *       pending set that is polled every --poll-ms until found (or
 *       --key-timeout-s). The first get per key necessarily starts after
 *       the writer's ack (ack -&gt; notify -&gt; get), so a first-get miss is
 *       a read-latest violation with no clock comparison involved. Emits
 *       visibility.csv with per-key ack/notify/first/found timestamps.
 *   <li>put-long / get-long: seed or read one 8-byte counter value.
 *   <li>hash: md5 over (key,value) for every --sample-every'th key of the
 *       YCSB keyspace (user + FNV hash, the workloads' insertorder=hashed
 *       default); with --wait-stable, repeat until two consecutive passes
 *       agree. Prints one JSON line on stdout.
 * </ul>
 *
 * <p>System.nanoTime() on Linux is CLOCK_MONOTONIC, which shares its origin
 * across processes on one host — commits.csv and reads.csv are directly
 * comparable as long as writer and reader run on the same machine (the
 * orchestrator enforces that; the data path still crosses the network,
 * because consistency is mediated by the remote Corfu log either way).
 */
public final class ConsistencyProbe {

  private ConsistencyProbe() {
  }

  private static Map<String, String> parseFlags(String[] args, int from) {
    Map<String, String> flags = new HashMap<>();
    for (int i = from; i < args.length; i += 2) {
      if (!args[i].startsWith("--") || i + 1 >= args.length) {
        throw new IllegalArgumentException("expected --flag value pairs, got: " + args[i]);
      }
      flags.put(args[i].substring(2), args[i + 1]);
    }
    return flags;
  }

  private static String req(Map<String, String> flags, String name) {
    String v = flags.get(name);
    if (v == null) {
      throw new IllegalArgumentException("missing required flag --" + name);
    }
    return v;
  }

  private static OzoneDBJNI open(Map<String, String> flags) {
    // Watchdog: openDB can block forever in the engine's fenced metadata
    // read when the corfu log holds entries this instance's stream will
    // never cover (e.g. appended by an orphaned probe from an interrupted
    // run). A hung probe left running poisons every later experiment on
    // this corfu server, so halt hard instead of lingering.
    long timeoutS = Long.parseLong(flags.getOrDefault("open-timeout-s", "240"));
    Thread watchdog = new Thread(() -> haltIfStillOpening(timeoutS));
    watchdog.setDaemon(true);
    watchdog.start();
    OzoneDBJNI db = new OzoneDBJNI();
    db.openDB(req(flags, "config"));
    watchdog.interrupt();
    return db;
  }

  private static void haltIfStillOpening(long timeoutS) {
    try {
      Thread.sleep(timeoutS * 1000L);
    } catch (InterruptedException e) {
      return;
    }
    System.err.println("openDB did not return within " + timeoutS
        + "s -- halting (restart corfu_server and retry)");
    Runtime.getRuntime().halt(3);
  }

  private static byte[] encodeLong(long v) {
    return ByteBuffer.allocate(8).putLong(v).array();
  }

  private static long decodeLong(byte[] v) {
    if (v == null || v.length < 8) {
      return -1L;
    }
    return ByteBuffer.wrap(v, 0, 8).getLong();
  }

  /**
   * Touch --ready-file, then poll for --go-file. The orchestrator creates
   * the go file once every participant is ready, so measurement loops in
   * different processes start together instead of skewed by JVM + Corfu
   * connect time (which can differ by seconds).
   */
  private static void barrier(Map<String, String> flags) throws Exception {
    String ready = flags.get("ready-file");
    String go = flags.get("go-file");
    if (ready == null || go == null) {
      return;
    }
    new File(ready).createNewFile();
    long timeoutS = Long.parseLong(flags.getOrDefault("barrier-timeout-s", "300"));
    long deadline = System.currentTimeMillis() + timeoutS * 1000;
    File goFile = new File(go);
    while (!goFile.exists()) {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("barrier timed out waiting for " + go);
      }
      Thread.sleep(10);
    }
  }

  private static void sleepNs(long ns) throws InterruptedException {
    if (ns > 0) {
      Thread.sleep(ns / 1_000_000L, (int) (ns % 1_000_000L));
    }
  }

  private static void writeProbe(Map<String, String> flags) throws Exception {
    String key = flags.getOrDefault("key", "__probe__");
    double rate = Double.parseDouble(req(flags, "rate"));
    long durationS = Long.parseLong(req(flags, "duration-s"));
    String outDir = req(flags, "out-dir");

    OzoneDBJNI db = open(flags);
    List<long[]> commits = new ArrayList<>();
    db.put(key, encodeLong(0));
    commits.add(new long[]{0, System.nanoTime()});
    barrier(flags);

    long periodNs = (long) (1e9 / rate);
    long deadline = System.nanoTime() + durationS * 1_000_000_000L;
    long seq = 0;
    while (System.nanoTime() < deadline) {
      seq++;
      db.put(key, encodeLong(seq));
      commits.add(new long[]{seq, System.nanoTime()});
      sleepNs(periodNs);
    }
    db.closeDB();

    try (PrintWriter w = new PrintWriter(new File(outDir, "commits.csv"), "UTF-8")) {
      w.println("seq,t_commit_ns");
      for (long[] c : commits) {
        w.println(c[0] + "," + c[1]);
      }
    }
    System.out.println("{\"writes\": " + seq + "}");
  }

  private static void readProbe(Map<String, String> flags) throws Exception {
    String key = flags.getOrDefault("key", "__probe__");
    long durationS = Long.parseLong(req(flags, "duration-s"));
    long readIntervalMs = Long.parseLong(flags.getOrDefault("read-interval-ms", "5"));
    String outDir = req(flags, "out-dir");

    OzoneDBJNI db = open(flags);
    barrier(flags);

    // Record BOTH ends of each get: t_read_ns (after it returns; the
    // cr-sqlite-compatible staleness timestamp) and t_start_ns (before it
    // starts). Linearizability is judged against t_start_ns -- a get that
    // overlaps a commit may correctly return the older value, so only a
    // value older than the last commit BEFORE the get began is a violation.
    List<long[]> reads = new ArrayList<>();
    long deadline = System.nanoTime() + durationS * 1_000_000_000L;
    while (System.nanoTime() < deadline) {
      long t0 = System.nanoTime();
      byte[] v = db.get(key);
      long t = System.nanoTime();
      if (v != null && v.length >= 8) {
        reads.add(new long[]{t, decodeLong(v), t0});
      }
      sleepNs(readIntervalMs * 1_000_000L);
    }
    db.closeDB();

    try (PrintWriter w = new PrintWriter(new File(outDir, "reads.csv"), "UTF-8")) {
      w.println("t_read_ns,seq_seen,t_start_ns");
      for (long[] r : reads) {
        w.println(r[0] + "," + r[1] + "," + r[2]);
      }
    }
    System.out.println("{\"reads\": " + reads.size() + "}");
  }

  private static void counterWorker(Map<String, String> flags) throws Exception {
    String key = flags.getOrDefault("key", "__counter__");
    long increments = Long.parseLong(req(flags, "increments"));
    double rate = Double.parseDouble(flags.getOrDefault("rate", "0"));
    int workerIdx = Integer.parseInt(req(flags, "worker"));
    String outDir = req(flags, "out-dir");

    OzoneDBJNI db = open(flags);
    barrier(flags);

    long periodNs = rate > 0 ? (long) (1e9 / rate) : 0;
    long t0 = System.nanoTime();
    long acked = 0;
    long lastWritten = -1;
    for (long i = 0; i < increments; i++) {
      long seen = decodeLong(db.get(key));
      if (seen < 0) {
        seen = 0;
      }
      lastWritten = seen + 1;
      db.put(key, encodeLong(lastWritten));
      acked++;
      sleepNs(periodNs);
    }
    double elapsedS = (System.nanoTime() - t0) / 1e9;
    db.closeDB();

    try (PrintWriter w = new PrintWriter(new File(outDir, "worker-" + workerIdx + ".json"), "UTF-8")) {
      w.println("{\"worker\": " + workerIdx + ", \"acked\": " + acked
          + ", \"last_written\": " + lastWritten
          + ", \"elapsed_s\": " + String.format("%.3f", elapsedS) + "}");
    }
    System.out.println("{\"worker\": " + workerIdx + ", \"acked\": " + acked + "}");
  }

  private static void insertProbe(Map<String, String> flags) throws Exception {
    double rate = Double.parseDouble(flags.getOrDefault("rate", "20"));
    long durationS = Long.parseLong(req(flags, "duration-s"));
    int valueSize = Integer.parseInt(flags.getOrDefault("value-size", "1000"));
    String outDir = req(flags, "out-dir");
    String notifyPath = req(flags, "notify-file");
    String donePath = req(flags, "done-file");

    OzoneDBJNI db = open(flags);
    barrier(flags);

    Random rnd = new Random(42);
    byte[] value = new byte[Math.max(8, valueSize)];
    long periodNs = (long) (1e9 / rate);
    long deadline = System.nanoTime() + durationS * 1_000_000_000L;
    List<long[]> inserts = new ArrayList<>();
    long i = 0;
    // Autoflush PrintWriter: each println pushes the full line (with its
    // newline) through to the file in one flush, so the tailing reader
    // never sees a torn line under normal conditions (it also guards
    // against partial lines itself).
    try (PrintWriter notify = new PrintWriter(new FileWriter(notifyPath), true)) {
      while (System.nanoTime() < deadline) {
        String key = CoreWorkload.buildKeyName(i, 1, false);
        rnd.nextBytes(value);
        ByteBuffer.wrap(value).putLong(i);
        db.put(key, value);
        long tAck = System.nanoTime();
        inserts.add(new long[]{i, tAck});
        notify.println(i + "," + tAck);
        i++;
        sleepNs(periodNs);
      }
    }
    try (PrintWriter w = new PrintWriter(new File(outDir, "inserts.csv"), "UTF-8")) {
      w.println("idx,t_ack_ns");
      for (long[] ins : inserts) {
        w.println(ins[0] + "," + ins[1]);
      }
    }
    new File(donePath).createNewFile();
    db.closeDB();
    System.out.println("{\"inserts\": " + i + "}");
  }

  // Column slots of a visibility-probe per-key record.
  private static final int VIS_IDX = 0;
  private static final int VIS_T_ACK = 1;
  private static final int VIS_T_NOTIFY = 2;
  private static final int VIS_T_FIRST = 3;
  private static final int VIS_FOUND_FIRST = 4;
  private static final int VIS_T_FOUND = 5;
  private static final int VIS_ATTEMPTS = 6;

  private static void visibilityProbe(Map<String, String> flags) throws Exception {
    String outDir = req(flags, "out-dir");
    String notifyPath = req(flags, "notify-file");
    String donePath = req(flags, "done-file");
    long pollMs = Long.parseLong(flags.getOrDefault("poll-ms", "1"));
    long keyTimeoutNs = Long.parseLong(flags.getOrDefault("key-timeout-s", "30"))
        * 1_000_000_000L;
    long runTimeoutS = Long.parseLong(flags.getOrDefault("run-timeout-s", "600"));

    OzoneDBJNI db = open(flags);
    barrier(flags);

    File notifyFile = new File(notifyPath);
    File doneFile = new File(donePath);
    BufferedReader notify = null;
    StringBuilder partial = new StringBuilder();
    // Insertion-ordered so per-tick checks walk keys oldest-first.
    LinkedHashMap<Long, long[]> pending = new LinkedHashMap<>();
    List<long[]> results = new ArrayList<>();
    long runDeadline = System.nanoTime() + runTimeoutS * 1_000_000_000L;

    while (true) {
      if (notify == null && notifyFile.exists()) {
        notify = new BufferedReader(new FileReader(notifyFile));
      }
      // Drain complete notify lines. Char-by-char with a persistent
      // remainder: readLine() would hand us a torn line at EOF and lose
      // its tail when the writer's flush completes.
      if (notify != null) {
        while (notify.ready()) {
          int ch = notify.read();
          if (ch == -1) {
            break;
          }
          if (ch != '\n') {
            partial.append((char) ch);
            continue;
          }
          String line = partial.toString();
          partial.setLength(0);
          int comma = line.indexOf(',');
          if (comma <= 0) {
            continue;
          }
          long idx = Long.parseLong(line.substring(0, comma));
          long tAck = Long.parseLong(line.substring(comma + 1).trim());
          pending.put(idx, new long[]{idx, tAck, System.nanoTime(), -1, 0, -1, 0});
        }
      }

      Iterator<Map.Entry<Long, long[]>> it = pending.entrySet().iterator();
      while (it.hasNext()) {
        long[] r = it.next().getValue();
        long tStart = System.nanoTime();
        byte[] v = db.get(CoreWorkload.buildKeyName(r[VIS_IDX], 1, false));
        r[VIS_ATTEMPTS]++;
        if (r[VIS_T_FIRST] < 0) {
          r[VIS_T_FIRST] = tStart;
          r[VIS_FOUND_FIRST] = v != null ? 1 : 0;
        }
        if (v != null) {
          r[VIS_T_FOUND] = System.nanoTime();
          results.add(r);
          it.remove();
        } else if (tStart - r[VIS_T_NOTIFY] > keyTimeoutNs) {
          results.add(r);
          it.remove();
        }
      }

      boolean drained = notify != null && !notify.ready() && partial.length() == 0;
      if (doneFile.exists() && drained && pending.isEmpty()) {
        break;
      }
      if (System.nanoTime() > runDeadline) {
        System.err.println("visibility-probe run timeout with "
            + pending.size() + " keys still pending");
        results.addAll(pending.values());
        break;
      }
      sleepNs(pollMs * 1_000_000L);
    }
    if (notify != null) {
      notify.close();
    }
    db.closeDB();

    long missesFirst = 0;
    long timeouts = 0;
    for (long[] r : results) {
      if (r[VIS_FOUND_FIRST] == 0) {
        missesFirst++;
      }
      if (r[VIS_T_FOUND] < 0) {
        timeouts++;
      }
    }
    try (PrintWriter w = new PrintWriter(new File(outDir, "visibility.csv"), "UTF-8")) {
      w.println("idx,t_ack_ns,t_notify_ns,t_first_ns,found_first,t_found_ns,attempts");
      for (long[] r : results) {
        w.println(r[VIS_IDX] + "," + r[VIS_T_ACK] + "," + r[VIS_T_NOTIFY] + ","
            + r[VIS_T_FIRST] + "," + r[VIS_FOUND_FIRST] + "," + r[VIS_T_FOUND]
            + "," + r[VIS_ATTEMPTS]);
      }
    }
    System.out.println("{\"checked\": " + results.size()
        + ", \"misses_first\": " + missesFirst
        + ", \"timeouts\": " + timeouts + "}");
  }

  private static void putLong(Map<String, String> flags) {
    OzoneDBJNI db = open(flags);
    db.put(req(flags, "key"), encodeLong(Long.parseLong(req(flags, "value"))));
    db.closeDB();
    System.out.println("{\"put\": \"ok\"}");
  }

  private static void getLong(Map<String, String> flags) throws Exception {
    OzoneDBJNI db = open(flags);
    String key = req(flags, "key");
    long v = decodeLong(db.get(key));
    // A freshly opened instance replays the stream in the background, so an
    // immediate get can catch it mid-replay. --wait-stable polls until two
    // consecutive reads agree on a present value (or the timeout passes).
    if (Boolean.parseBoolean(flags.getOrDefault("wait-stable", "false"))) {
      long pollMs = Long.parseLong(flags.getOrDefault("poll-ms", "500"));
      long timeoutS = Long.parseLong(flags.getOrDefault("timeout-s", "120"));
      long deadline = System.nanoTime() + timeoutS * 1_000_000_000L;
      long prev = Long.MIN_VALUE;
      while ((v < 0 || v != prev) && System.nanoTime() < deadline) {
        prev = v;
        Thread.sleep(pollMs);
        v = decodeLong(db.get(key));
      }
    }
    db.closeDB();
    System.out.println("{\"value\": " + v + "}");
  }

  private static String hashPass(OzoneDBJNI db, long recordCount, long sampleEvery,
                                 long[] presentOut) throws Exception {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    long present = 0;
    for (long i = 0; i < recordCount; i += sampleEvery) {
      String key = CoreWorkload.buildKeyName(i, 1, false);
      byte[] v = db.get(key);
      md5.update(key.getBytes(StandardCharsets.UTF_8));
      if (v != null) {
        present++;
        md5.update(v);
      } else {
        md5.update("MISSING".getBytes(StandardCharsets.UTF_8));
      }
    }
    presentOut[0] = present;
    StringBuilder hex = new StringBuilder();
    for (byte b : md5.digest()) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }

  private static void hash(Map<String, String> flags) throws Exception {
    long recordCount = Long.parseLong(req(flags, "record-count"));
    long sampleEvery = Long.parseLong(flags.getOrDefault("sample-every", "1"));
    boolean waitStable = Boolean.parseBoolean(flags.getOrDefault("wait-stable", "false"));
    long pollMs = Long.parseLong(flags.getOrDefault("poll-ms", "1000"));
    long timeoutS = Long.parseLong(flags.getOrDefault("timeout-s", "900"));
    long checked = (recordCount + sampleEvery - 1) / sampleEvery;

    OzoneDBJNI db = open(flags);
    long t0 = System.nanoTime();
    long[] present = new long[1];
    String digest = hashPass(db, recordCount, sampleEvery, present);
    double stableAfterS = 0;
    if (waitStable) {
      long deadline = System.nanoTime() + timeoutS * 1_000_000_000L;
      while (true) {
        Thread.sleep(pollMs);
        String next = hashPass(db, recordCount, sampleEvery, present);
        if (next.equals(digest)) {
          stableAfterS = (System.nanoTime() - t0) / 1e9;
          break;
        }
        digest = next;
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("hash did not stabilize within " + timeoutS + "s");
        }
      }
    }
    db.closeDB();
    System.out.println("{\"checked\": " + checked + ", \"present\": " + present[0]
        + ", \"md5\": \"" + digest + "\""
        + ", \"stable_after_s\": " + String.format("%.2f", stableAfterS) + "}");
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("usage: ConsistencyProbe <write-probe|read-probe|counter-worker"
          + "|insert-probe|visibility-probe|put-long|get-long|hash>"
          + " --config <shared_config.json> [--flag value ...]");
      System.exit(2);
    }
    Map<String, String> flags = parseFlags(args, 1);
    switch (args[0]) {
    case "write-probe":
      writeProbe(flags);
      break;
    case "read-probe":
      readProbe(flags);
      break;
    case "counter-worker":
      counterWorker(flags);
      break;
    case "insert-probe":
      insertProbe(flags);
      break;
    case "visibility-probe":
      visibilityProbe(flags);
      break;
    case "put-long":
      putLong(flags);
      break;
    case "get-long":
      getLong(flags);
      break;
    case "hash":
      hash(flags);
      break;
    default:
      System.err.println("unknown mode: " + args[0]);
      System.exit(2);
    }
    // The C++ engine's corfu threads attach to this JVM as non-daemon
    // threads, so returning from main does not end the process -- the
    // probe would linger (and its driver block in wait()) forever.
    System.exit(0);
  }
}
