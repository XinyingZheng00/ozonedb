package site.ycsb.db;

import jni.OzoneDBJNI;
import site.ycsb.generator.ZipfianGenerator;
import site.ycsb.workloads.CoreWorkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
 *       With --cas true the RMW is atomic: getVersioned pairs the value
 *       with its log-address version and casPut is accepted only if the
 *       key is still at that version, so lost increments are impossible
 *       and conflicts are retried (and counted).
 *   <li>txn-counter-worker / txn-transfer-worker (txn-mixed-worker) /
 *       txn-audit / txn-skew-worker / seed-accounts: the transaction checks
 *       (PLAN-transactions B1). The same RMW as one transaction; sum-
 *       preserving transfers over --accounts with --keys per transaction
 *       and optional --zipf skew, with a per-attempt latency CSV; a
 *       repeated all-account audit with or without a read-only validation
 *       record; and the write-skew shape on fresh key pairs per round. All
 *       commit through OzoneDBJNI.txnCommit after a sync/getVersioned read
 *       phase; -2 is a read-set conflict, retried and counted as an abort.
 *   <li>insert-probe: insert brand-new YCSB-keyspace keys at --rate for
 *       --duration-s; after each put() returns, append "i,t_ack_ns" to
 *       --notify-file (flushed per line) and record it in inserts.csv.
 *       Touches --done-file when finished. New keys make "not found" an
 *       unambiguous staleness signal: there is no older value to serve.
 *       Cross-node: --notify-connect host:port replaces --notify-file --
 *       acks flow over TCP to the reader (retrying the connect until the
 *       reader listens), and closing the socket is the done signal.
 *   <li>visibility-probe: tail --notify-file; each notified key joins a
 *       pending set that is polled every --poll-ms until found (or
 *       --key-timeout-s). The first get per key necessarily starts after
 *       the writer's ack (ack -&gt; notify -&gt; get), so a first-get miss is
 *       a read-latest violation with no clock comparison involved. Emits
 *       visibility.csv with per-key ack/notify/first/found timestamps.
 *       Cross-node: --notify-listen port accepts the writer's TCP stream;
 *       t_notify (this host's clock) is then the latency reference, since
 *       t_ack lives in the writer's clock domain.
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

  private static long decodeLongAt(byte[] v, int offset) {
    if (v == null || v.length < offset + 8) {
      return -1L;
    }
    return ByteBuffer.wrap(v, offset, 8).getLong();
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

    boolean cas = Boolean.parseBoolean(flags.getOrDefault("cas", "false"));
    // txn: the same RMW as one transaction (read set of one, write set of
    // one) through sync/getVersioned/txnCommit -- what txn-counter-worker
    // selects. Same verdicts as casPut: -2 is a conflict, retried.
    boolean txn = Boolean.parseBoolean(flags.getOrDefault("txn", "false"));
    // blind-zero: a blind put of 0 BEFORE the go barrier, so every worker's
    // first transaction reads a blind-written (value, version) pair from
    // some instance -- the version-value coupling the engine must keep for
    // blind writes too. Before the barrier it cannot race an increment.
    boolean blindZero = Boolean.parseBoolean(flags.getOrDefault("blind-zero", "false"));
    // Conflict backoff: a -2 means another worker's CAS on this key was
    // sequenced between our read and our write, so every worker that
    // lost is about to re-read the same fresh version and collide
    // again. Jittered exponential backoff (base doubling per consecutive
    // loss, capped) spreads the retries; 0 disables it. Counted per
    // increment in maxAttempts so the output shows how bad contention
    // got, not just how often.
    long backoffBaseUs = Long.parseLong(flags.getOrDefault("cas-backoff-us", "200"));
    long backoffCapUs = Long.parseLong(flags.getOrDefault("cas-backoff-cap-us", "8000"));
    Random backoffRng = new Random(0x5eed + 31L * Integer.parseInt(req(flags, "worker")));

    OzoneDBJNI db = open(flags);
    if (blindZero) {
      db.put(key, encodeLong(0));
    }
    barrier(flags);

    long periodNs = rate > 0 ? (long) (1e9 / rate) : 0;
    long t0 = System.nanoTime();
    long acked = 0;
    long conflicts = 0;
    long maxAttempts = 0;
    long lastWritten = -1;
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    for (long i = 0; i < increments; i++) {
      if (txn) {
        long attempts = 0;
        while (true) {
          attempts++;
          db.sync();
          byte[] vv = db.getVersioned(key);
          long version = decodeLong(vv);
          long seen = decodeLongAt(vv, 8);
          if (seen < 0) {
            seen = 0;
          }
          long r = db.txnCommit(new byte[][]{keyBytes}, new long[]{version},
              new byte[][]{keyBytes}, new byte[][]{encodeLong(seen + 1)}, new boolean[]{false});
          db.clearSync();
          if (r >= 0) {
            lastWritten = seen + 1;
            break;
          }
          if (r != -2) {
            throw new IllegalStateException("txnCommit backend failure at increment " + i);
          }
          conflicts++;
          if (backoffBaseUs > 0) {
            long bound = Math.min(backoffCapUs, backoffBaseUs << Math.min(attempts - 1, 20));
            long us = (long) (backoffRng.nextDouble() * bound);
            if (us > 0) {
              sleepNs(us * 1000L);
            }
          }
        }
        if (attempts > maxAttempts) {
          maxAttempts = attempts;
        }
      } else if (cas) {
        // Atomic RMW: retry until this increment's conditional put wins.
        // -2 means another worker advanced the version between our read
        // and our write's log position — re-read and try again. Nothing
        // is ever silently overwritten.
        long attempts = 0;
        while (true) {
          attempts++;
          byte[] vv = db.getVersioned(key);
          long version = decodeLong(vv);
          long seen = decodeLongAt(vv, 8);
          if (seen < 0) {
            seen = 0;
          }
          long r = db.casPut(key, encodeLong(seen + 1), version);
          if (r >= 0) {
            lastWritten = seen + 1;
            break;
          }
          if (r != -2) {
            throw new IllegalStateException("casPut backend failure at increment " + i);
          }
          conflicts++;
          if (backoffBaseUs > 0) {
            long bound = Math.min(backoffCapUs, backoffBaseUs << Math.min(attempts - 1, 20));
            long us = (long) (backoffRng.nextDouble() * bound);
            if (us > 0) {
              sleepNs(us * 1000L);
            }
          }
        }
        if (attempts > maxAttempts) {
          maxAttempts = attempts;
        }
      } else {
        long seen = decodeLong(db.get(key));
        if (seen < 0) {
          seen = 0;
        }
        lastWritten = seen + 1;
        db.put(key, encodeLong(lastWritten));
      }
      acked++;
      sleepNs(periodNs);
    }
    double elapsedS = (System.nanoTime() - t0) / 1e9;
    db.closeDB();

    try (PrintWriter w = new PrintWriter(new File(outDir, "worker-" + workerIdx + ".json"), "UTF-8")) {
      w.println("{\"worker\": " + workerIdx + ", \"acked\": " + acked
          + ", \"last_written\": " + lastWritten
          + ", \"cas\": " + cas + ", \"txn\": " + txn
          + ", \"conflicts\": " + conflicts + ", \"aborts\": " + conflicts
          + ", \"max_attempts\": " + maxAttempts
          + ", \"elapsed_s\": " + String.format("%.3f", elapsedS) + "}");
    }
    System.out.println("{\"worker\": " + workerIdx + ", \"acked\": " + acked
        + ", \"conflicts\": " + conflicts + "}");
  }

  private static void insertProbe(Map<String, String> flags) throws Exception {
    double rate = Double.parseDouble(flags.getOrDefault("rate", "20"));
    long durationS = Long.parseLong(req(flags, "duration-s"));
    int valueSize = Integer.parseInt(flags.getOrDefault("value-size", "1000"));
    // Multi-writer runs give each writer a disjoint idx range (writer w
    // starts at w * 10_000_000) so keys never collide and the reader's
    // pending map needs no writer id -- idx alone identifies the key.
    long keyBase = Long.parseLong(flags.getOrDefault("key-base", "0"));
    // ... and a private inserts file, since all writers share --out-dir.
    String insertsName = flags.getOrDefault("inserts-file", "inserts.csv");
    String outDir = req(flags, "out-dir");
    String notifyPath = flags.get("notify-file");
    String connect = flags.get("notify-connect");
    String donePath = flags.get("done-file");
    if ((notifyPath == null) == (connect == null)) {
      throw new IllegalArgumentException(
          "pass exactly one of --notify-file / --notify-connect");
    }

    OzoneDBJNI db = open(flags);
    // TCP mode: the reader only listens after ITS openDB returned, so a
    // successful connect proves the reader is up; the "go" line it sends
    // once EVERY writer has connected is the start barrier, aligning
    // multi-writer measurement windows despite seconds of JVM + corfu
    // startup skew. No ready/go files needed.
    Socket sock = null;
    PrintWriter notify;
    if (connect != null) {
      sock = connectWithRetry(connect,
          Long.parseLong(flags.getOrDefault("connect-timeout-s", "240")));
      new BufferedReader(new InputStreamReader(
          sock.getInputStream(), StandardCharsets.UTF_8)).readLine();
      notify = new PrintWriter(
          new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8), true);
    } else {
      // Autoflush PrintWriter: each println pushes the full line (with its
      // newline) through in one flush, so the tailing reader never sees a
      // torn line under normal conditions (it also guards against partial
      // lines itself).
      notify = new PrintWriter(new FileWriter(notifyPath), true);
    }
    barrier(flags);

    Random rnd = new Random(42);
    byte[] value = new byte[Math.max(8, valueSize)];
    long periodNs = (long) (1e9 / rate);
    long deadline = System.nanoTime() + durationS * 1_000_000_000L;
    List<long[]> inserts = new ArrayList<>();
    long i = keyBase;
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
    // Closing the channel is the done signal in TCP mode (EOF at the
    // reader); the done-file covers the same-host file channel.
    notify.close();
    if (sock != null) {
      sock.close();
    }
    try (PrintWriter w = new PrintWriter(new File(outDir, insertsName), "UTF-8")) {
      w.println("idx,t_ack_ns");
      for (long[] ins : inserts) {
        w.println(ins[0] + "," + ins[1]);
      }
    }
    if (donePath != null) {
      new File(donePath).createNewFile();
    }
    db.closeDB();
    System.out.println("{\"inserts\": " + (i - keyBase) + "}");
  }

  private static Socket connectWithRetry(String hostPort, long timeoutS) throws Exception {
    int colon = hostPort.lastIndexOf(':');
    String host = hostPort.substring(0, colon);
    int port = Integer.parseInt(hostPort.substring(colon + 1));
    long deadline = System.nanoTime() + timeoutS * 1_000_000_000L;
    while (true) {
      Socket s = new Socket();
      try {
        s.connect(new InetSocketAddress(host, port), 2000);
        s.setTcpNoDelay(true);
        return s;
      } catch (Exception e) {
        s.close();
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("could not connect to " + hostPort, e);
        }
        Thread.sleep(200);
      }
    }
  }

  // Socket-side notify drain: runs on its own thread (one per writer
  // socket) so a blocking readLine never stalls the polling loop.
  // t_notify is stamped HERE, at receipt -- the reader-clock anchor of
  // the ack -> notify -> get happens-before chain. EOF (writer closed
  // the socket) bumps `closed`; the poll loop treats closed == number
  // of writer sockets as "every writer finished".
  private static void drainNotifySocket(BufferedReader in,
                                        ConcurrentLinkedQueue<long[]> queue,
                                        AtomicInteger closed) {
    try {
      while (true) {
        String line = in.readLine();
        if (line == null) {
          break;
        }
        int comma = line.indexOf(',');
        if (comma <= 0) {
          continue;
        }
        long idx = Long.parseLong(line.substring(0, comma));
        long tAck = Long.parseLong(line.substring(comma + 1).trim());
        queue.add(new long[]{idx, tAck, System.nanoTime()});
      }
    } catch (Exception e) {
      System.err.println("notify socket closed: " + e);
    }
    closed.incrementAndGet();
  }

  /**
   * Accept `n` writer notify connections, then release them all at once by
   * sending one "go" line down each socket -- the cross-node start
   * barrier. Writers block on that line after connecting, so measurement
   * windows align even though JVM + corfu startup skews their connect
   * times by seconds. One daemon drainer thread per socket.
   */
  private static List<Socket> acceptNotifyWriters(ServerSocket server, int n,
                                                  ConcurrentLinkedQueue<long[]> arrivals,
                                                  AtomicInteger closed) throws Exception {
    List<Socket> socks = new ArrayList<>();
    for (int k = 0; k < n; k++) {
      Socket s = server.accept();
      s.setTcpNoDelay(true);
      socks.add(s);
    }
    for (Socket s : socks) {
      s.getOutputStream().write("go\n".getBytes(StandardCharsets.UTF_8));
      s.getOutputStream().flush();
    }
    for (Socket s : socks) {
      BufferedReader in = new BufferedReader(
          new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
      Thread drainer = new Thread(() -> drainNotifySocket(in, arrivals, closed));
      drainer.setDaemon(true);
      drainer.start();
    }
    return socks;
  }

  /**
   * Open any newly-appeared notify files and drain complete lines into the
   * pending map. Char-by-char with a persistent per-file remainder:
   * readLine() would hand us a torn line at EOF and lose its tail when the
   * writer's flush completes.
   */
  private static void drainNotifyFiles(File[] notifyFiles, BufferedReader[] notifies,
                                       StringBuilder[] partials,
                                       LinkedHashMap<Long, long[]> pending) throws Exception {
    for (int k = 0; k < notifyFiles.length; k++) {
      if (notifies[k] == null && notifyFiles[k].exists()) {
        notifies[k] = new BufferedReader(new FileReader(notifyFiles[k]));
      }
      if (notifies[k] == null) {
        continue;
      }
      while (notifies[k].ready()) {
        int ch = notifies[k].read();
        if (ch == -1) {
          break;
        }
        if (ch != '\n') {
          partials[k].append((char) ch);
          continue;
        }
        String line = partials[k].toString();
        partials[k].setLength(0);
        int comma = line.indexOf(',');
        if (comma <= 0) {
          continue;
        }
        long idx = Long.parseLong(line.substring(0, comma));
        long tAck = Long.parseLong(line.substring(comma + 1).trim());
        pending.put(idx, new long[]{idx, tAck, System.nanoTime(), -1, 0, -1, 0});
      }
    }
  }

  /**
   * File-channel done test. The orchestrator touches done-file only after
   * every writer has exited, so a notify file that still doesn't exist then
   * belongs to a writer that died before its first ack -- skip it rather
   * than spin until the run timeout.
   */
  private static boolean notifyFilesDrained(File[] notifyFiles, BufferedReader[] notifies,
                                            StringBuilder[] partials) throws Exception {
    for (int k = 0; k < notifyFiles.length; k++) {
      if (!notifyFiles[k].exists()) {
        continue;
      }
      if (notifies[k] == null || notifies[k].ready() || partials[k].length() > 0) {
        return false;
      }
    }
    return true;
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
    String notifyPath = flags.get("notify-file");
    String listen = flags.get("notify-listen");
    String donePath = flags.get("done-file");
    if ((notifyPath == null) == (listen == null)) {
      throw new IllegalArgumentException(
          "pass exactly one of --notify-file / --notify-listen");
    }
    if (notifyPath != null && donePath == null) {
      throw new IllegalArgumentException("--notify-file needs --done-file");
    }
    long pollMs = Long.parseLong(flags.getOrDefault("poll-ms", "1"));
    long keyTimeoutNs = Long.parseLong(flags.getOrDefault("key-timeout-s", "30"))
        * 1_000_000_000L;
    long runTimeoutS = Long.parseLong(flags.getOrDefault("run-timeout-s", "600"));
    boolean tickFence = Boolean.parseBoolean(flags.getOrDefault("tick-fence", "false"));

    OzoneDBJNI db = open(flags);

    // TCP mode: bind only after openDB so a successful writer connect
    // implies this reader is fully up. With N writers the "go" line sent
    // after all N accepts is the start barrier (see acceptNotifyWriters);
    // EOF on every socket is the collective done signal.
    int notifyWriters = Integer.parseInt(flags.getOrDefault("notify-writers", "1"));
    ServerSocket server = null;
    List<Socket> socks = null;
    ConcurrentLinkedQueue<long[]> arrivals = null;
    AtomicInteger socketsClosed = null;
    if (listen != null) {
      server = new ServerSocket(Integer.parseInt(listen));
      server.setSoTimeout(1000 * Integer.parseInt(
          flags.getOrDefault("accept-timeout-s", "300")));
      arrivals = new ConcurrentLinkedQueue<>();
      socketsClosed = new AtomicInteger(0);
      socks = acceptNotifyWriters(server, notifyWriters, arrivals, socketsClosed);
    }
    barrier(flags);

    // --notify-file is a comma-separated list in multi-writer runs: one
    // file per writer, each tailed independently with its own torn-line
    // remainder. Writers use disjoint --key-base idx ranges, so the
    // pending map stays keyed by idx alone.
    File[] notifyFiles = null;
    BufferedReader[] notifies = null;
    StringBuilder[] partials = null;
    if (notifyPath != null) {
      String[] parts = notifyPath.split(",");
      notifyFiles = new File[parts.length];
      notifies = new BufferedReader[parts.length];
      partials = new StringBuilder[parts.length];
      for (int k = 0; k < parts.length; k++) {
        notifyFiles[k] = new File(parts[k]);
        partials[k] = new StringBuilder();
      }
    }
    File doneFile = donePath != null ? new File(donePath) : null;
    // Insertion-ordered so per-tick checks walk keys oldest-first.
    LinkedHashMap<Long, long[]> pending = new LinkedHashMap<>();
    List<long[]> results = new ArrayList<>();
    long runDeadline = System.nanoTime() + runTimeoutS * 1_000_000_000L;

    while (true) {
      if (arrivals != null) {
        while (!arrivals.isEmpty()) {
          long[] a = arrivals.poll();
          if (a == null) {
            break;
          }
          pending.put(a[0], new long[]{a[0], a[1], a[2], -1, 0, -1, 0});
        }
      }
      if (notifyFiles != null) {
        drainNotifyFiles(notifyFiles, notifies, partials, pending);
      }

      // Per-tick batch fence (--tick-fence, strict runs): one fence
      // covers every pending get below, the cr-sqlite /barrier analog.
      // ack -> notify -> fence -> get, so miss counting is unchanged.
      boolean fenced = tickFence && !pending.isEmpty();
      if (fenced) {
        db.sync();
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
      if (fenced) {
        db.clearSync();
      }

      boolean done;
      if (arrivals != null) {
        done = socketsClosed.get() >= notifyWriters && arrivals.isEmpty();
      } else {
        done = doneFile.exists()
            && notifyFilesDrained(notifyFiles, notifies, partials);
      }
      if (done && pending.isEmpty()) {
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
    if (notifies != null) {
      for (BufferedReader r : notifies) {
        if (r != null) {
          r.close();
        }
      }
    }
    if (socks != null) {
      for (Socket s : socks) {
        s.close();
      }
    }
    if (server != null) {
      server.close();
    }
    db.closeDB();
    writeVisibilityResults(outDir, results);
  }

  private static void writeVisibilityResults(String outDir, List<long[]> results)
      throws Exception {
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

  // ------------------------------------------------------------ transactions

  /** {@code --prefix}{@code i} = encodeLong({@code --value}) for i in [0, {@code --count}), blind puts. */
  private static void seedAccounts(Map<String, String> flags) {
    OzoneDBJNI db = open(flags);
    String prefix = flags.getOrDefault("prefix", "acct_");
    int count = Integer.parseInt(req(flags, "count"));
    long value = Long.parseLong(flags.getOrDefault("value", "100"));
    for (int i = 0; i < count; i++) {
      db.put(prefix + i, encodeLong(value));
    }
    db.closeDB();
    System.out.println("{\"seeded\": " + count + ", \"value\": " + value + "}");
  }

  private static long jitteredBackoffUs(Random rng, long attempts, long baseUs, long capUs) {
    if (baseUs <= 0) {
      return 0;
    }
    long bound = Math.min(capUs, baseUs << Math.min(attempts - 1, 20));
    return (long) (rng.nextDouble() * bound);
  }

  /**
   * {@code keys} distinct account ids: uniform when theta is 0, else
   * Zipfian over [0, accounts) with the hot set at the low ids.
   */
  private static int[] pickAccounts(int keys, int accounts, Random rng, ZipfianGenerator zipf) {
    int[] ids = new int[keys];
    for (int j = 0; j < keys; j++) {
      while (true) {
        int id = zipf != null ? (int) zipf.nextValue().longValue() : rng.nextInt(accounts);
        boolean dup = false;
        for (int k = 0; k < j; k++) {
          if (ids[k] == id) {
            dup = true;
            break;
          }
        }
        if (!dup) {
          ids[j] = id;
          break;
        }
      }
    }
    return ids;
  }

  /**
   * txn-transfer-worker / txn-mixed-worker: for --duration-s, each
   * transaction reads --keys distinct accounts (uniform, or Zipfian with
   * --zipf theta over --accounts) and moves a random amount from the first
   * to each of the others, so the sum over all accounts is invariant.
   * Commits through txnCommit; -2 (a read-set version changed) retries
   * with the same jittered backoff as counter-worker. Per attempt one CSV
   * line: fence (sync), reads, and commit (append + verdict wait) in
   * microseconds. Emits worker-N.json with commits/aborts/max_attempts.
   */
  private static void transferWorker(Map<String, String> flags) throws Exception {
    String prefix = flags.getOrDefault("prefix", "acct_");
    int accounts = Integer.parseInt(req(flags, "accounts"));
    int keys = Integer.parseInt(flags.getOrDefault("keys", "2"));
    double theta = Double.parseDouble(flags.getOrDefault("zipf", "0"));
    long durationS = Long.parseLong(req(flags, "duration-s"));
    double rate = Double.parseDouble(flags.getOrDefault("rate", "0"));
    int workerIdx = Integer.parseInt(req(flags, "worker"));
    String outDir = req(flags, "out-dir");
    long backoffBaseUs = Long.parseLong(flags.getOrDefault("cas-backoff-us", "200"));
    long backoffCapUs = Long.parseLong(flags.getOrDefault("cas-backoff-cap-us", "8000"));
    if (keys < 2 || keys > accounts) {
      throw new IllegalArgumentException("--keys must be in [2, --accounts]");
    }
    Random rng = new Random(0x7a7 + 31L * workerIdx);
    ZipfianGenerator zipf = theta > 0 ? new ZipfianGenerator(0, accounts - 1, theta) : null;

    OzoneDBJNI db = open(flags);
    barrier(flags);

    long periodNs = rate > 0 ? (long) (1e9 / rate) : 0;
    long commits = 0;
    long aborts = 0;
    long errors = 0;
    long maxAttempts = 0;
    long t0 = System.nanoTime();
    long deadline = t0 + durationS * 1_000_000_000L;
    try (PrintWriter lat = new PrintWriter(new File(outDir, "txn-latency-" + workerIdx + ".csv"), "UTF-8")) {
      lat.println("t_end_ns,keys,attempt,fence_us,read_us,commit_us,outcome");
      while (System.nanoTime() < deadline) {
        int[] ids = pickAccounts(keys, accounts, rng, zipf);
        long amt = 1 + rng.nextInt(10);
        byte[][] rk = new byte[keys][];
        for (int j = 0; j < keys; j++) {
          rk[j] = (prefix + ids[j]).getBytes(StandardCharsets.UTF_8);
        }
        long attempts = 0;
        while (true) {
          attempts++;
          long tf0 = System.nanoTime();
          db.sync();
          long tf1 = System.nanoTime();
          long[] rv = new long[keys];
          long[] bal = new long[keys];
          for (int j = 0; j < keys; j++) {
            byte[] vv = db.getVersioned(prefix + ids[j]);
            rv[j] = decodeLong(vv);
            bal[j] = decodeLongAt(vv, 8);
            if (bal[j] < 0) {
              db.clearSync();
              throw new IllegalStateException("account " + prefix + ids[j] + " missing: run seed-accounts first");
            }
          }
          long tr1 = System.nanoTime();
          long give = Math.min(amt, Math.max(0, bal[0] / (keys - 1)));
          byte[][] wv = new byte[keys][];
          wv[0] = encodeLong(bal[0] - give * (keys - 1));
          for (int j = 1; j < keys; j++) {
            wv[j] = encodeLong(bal[j] + give);
          }
          long r = db.txnCommit(rk, rv, rk, wv, new boolean[keys]);
          long tc1 = System.nanoTime();
          db.clearSync();
          String outcome = r >= 0 ? "commit" : (r == -2 ? "abort" : "error");
          lat.println(tc1 + "," + keys + "," + attempts + "," + (tf1 - tf0) / 1000 + ","
              + (tr1 - tf1) / 1000 + "," + (tc1 - tr1) / 1000 + "," + outcome);
          if (r >= 0) {
            commits++;
            break;
          }
          if (r != -2) {
            errors++;
            break;
          }
          aborts++;
          long us = jitteredBackoffUs(rng, attempts, backoffBaseUs, backoffCapUs);
          if (us > 0) {
            sleepNs(us * 1000L);
          }
        }
        if (attempts > maxAttempts) {
          maxAttempts = attempts;
        }
        sleepNs(periodNs);
      }
    }
    double elapsedS = (System.nanoTime() - t0) / 1e9;
    db.closeDB();

    String json = "{\"worker\": " + workerIdx + ", \"commits\": " + commits
        + ", \"aborts\": " + aborts + ", \"errors\": " + errors
        + ", \"max_attempts\": " + maxAttempts + ", \"keys\": " + keys
        + ", \"zipf\": " + theta
        + ", \"elapsed_s\": " + String.format("%.3f", elapsedS) + "}";
    try (PrintWriter w = new PrintWriter(new File(outDir, "worker-" + workerIdx + ".json"), "UTF-8")) {
      w.println(json);
    }
    System.out.println(json);
  }

  /**
   * txn-audit: read every account inside one fence and check the sum
   * against --expected-sum, repeatedly until --duration-s (or --rounds)
   * elapses. With --validate true the reads are committed as a read-only
   * validation record: a -2 means a transfer landed between two of the
   * reads and the audit is discarded (an abort), so every counted audit is
   * a serializable snapshot and must sum correctly. With validation off
   * every audit counts, torn sums included -- that difference is what the
   * validation record buys. Emits audit-N.json.
   */
  private static void txnAudit(Map<String, String> flags) throws Exception {
    String prefix = flags.getOrDefault("prefix", "acct_");
    int accounts = Integer.parseInt(req(flags, "accounts"));
    long expected = Long.parseLong(req(flags, "expected-sum"));
    boolean validate = Boolean.parseBoolean(flags.getOrDefault("validate", "true"));
    long durationS = Long.parseLong(flags.getOrDefault("duration-s", "0"));
    long rounds = Long.parseLong(flags.getOrDefault("rounds", "0"));
    int workerIdx = Integer.parseInt(flags.getOrDefault("worker", "0"));
    String outDir = req(flags, "out-dir");
    if (durationS <= 0 && rounds <= 0) {
      rounds = 1;
    }

    OzoneDBJNI db = open(flags);
    barrier(flags);

    byte[][] rk = new byte[accounts][];
    for (int i = 0; i < accounts; i++) {
      rk[i] = (prefix + i).getBytes(StandardCharsets.UTF_8);
    }
    long audits = 0;
    long aborts = 0;
    long violations = 0;
    long firstBadSum = Long.MIN_VALUE;
    long t0 = System.nanoTime();
    long deadline = durationS > 0 ? t0 + durationS * 1_000_000_000L : Long.MAX_VALUE;
    long done = 0;
    while (System.nanoTime() < deadline && (rounds <= 0 || done < rounds)) {
      done++;
      db.sync();
      long[] rv = new long[accounts];
      long sum = 0;
      for (int i = 0; i < accounts; i++) {
        byte[] vv = db.getVersioned(prefix + i);
        rv[i] = decodeLong(vv);
        long bal = decodeLongAt(vv, 8);
        if (bal < 0) {
          db.clearSync();
          throw new IllegalStateException("account " + prefix + i + " missing: run seed-accounts first");
        }
        sum += bal;
      }
      if (validate) {
        long r = db.txnCommit(rk, rv, new byte[0][], new byte[0][], new boolean[0]);
        db.clearSync();
        if (r == -2) {
          aborts++;
          continue;
        }
        if (r < 0 && r != -3) {
          throw new IllegalStateException("txnCommit backend failure in audit " + done);
        }
      } else {
        db.clearSync();
      }
      audits++;
      if (sum != expected) {
        violations++;
        if (firstBadSum == Long.MIN_VALUE) {
          firstBadSum = sum;
        }
      }
    }
    double elapsedS = (System.nanoTime() - t0) / 1e9;
    db.closeDB();

    String json = "{\"worker\": " + workerIdx + ", \"audits\": " + audits
        + ", \"aborts\": " + aborts + ", \"violations\": " + violations
        + ", \"validate\": " + validate + ", \"expected_sum\": " + expected
        + ", \"first_bad_sum\": " + (firstBadSum == Long.MIN_VALUE ? "null" : String.valueOf(firstBadSum))
        + ", \"elapsed_s\": " + String.format("%.3f", elapsedS) + "}";
    try (PrintWriter w = new PrintWriter(new File(outDir, "audit-" + workerIdx + ".json"), "UTF-8")) {
      w.println(json);
    }
    System.out.println(json);
  }

  /**
   * txn-skew-worker: the write-skew shape. Round r uses two fresh keys
   * {@code --prefix}r_a and {@code --prefix}r_b, both unwritten (an absent
   * key reads as "1"). Each of the two workers reads both; if both are
   * still absent, worker 0 writes r_a = 0 and worker 1 writes r_b = 0, and
   * commits. Both commits succeeding is write skew -- impossible here,
   * because both keys are in both read sets, so the second commit in log
   * order sees the first one's write and is rejected. Rounds start on a
   * shared schedule (the go file's mtime + r * --round-ms) so the two
   * transactions overlap. Emits skew-N.csv (round,outcome) and
   * worker-N.json.
   */
  private static void skewWorker(Map<String, String> flags) throws Exception {
    String prefix = flags.getOrDefault("prefix", "skew_");
    long rounds = Long.parseLong(req(flags, "rounds"));
    long roundMs = Long.parseLong(flags.getOrDefault("round-ms", "20"));
    int workerIdx = Integer.parseInt(req(flags, "worker"));
    String outDir = req(flags, "out-dir");
    if (workerIdx != 0 && workerIdx != 1) {
      throw new IllegalArgumentException("--worker must be 0 or 1");
    }

    OzoneDBJNI db = open(flags);
    barrier(flags);
    String goPath = flags.get("go-file");
    long t0 = goPath != null ? new File(goPath).lastModified() : System.currentTimeMillis();

    long commits = 0;
    long aborts = 0;
    long skips = 0;
    long errors = 0;
    try (PrintWriter csv = new PrintWriter(new File(outDir, "skew-" + workerIdx + ".csv"), "UTF-8")) {
      csv.println("round,outcome");
      for (long r = 0; r < rounds; r++) {
        long start = t0 + r * roundMs;
        long wait = start - System.currentTimeMillis();
        if (wait > 0) {
          Thread.sleep(wait);
        }
        String ka = prefix + r + "_a";
        String kb = prefix + r + "_b";
        db.sync();
        byte[] va = db.getVersioned(ka);
        byte[] vb = db.getVersioned(kb);
        boolean aPresent = va != null && va.length > 8;
        boolean bPresent = vb != null && vb.length > 8;
        String outcome;
        if (aPresent || bPresent) {
          db.clearSync();
          skips++;
          outcome = "skip";
        } else {
          byte[][] rk = {ka.getBytes(StandardCharsets.UTF_8), kb.getBytes(StandardCharsets.UTF_8)};
          long[] rv = {decodeLong(va), decodeLong(vb)};
          byte[][] wk = {rk[workerIdx]};
          long res = db.txnCommit(rk, rv, wk, new byte[][]{encodeLong(0)}, new boolean[]{false});
          db.clearSync();
          if (res >= 0) {
            commits++;
            outcome = "commit";
          } else if (res == -2) {
            aborts++;
            outcome = "abort";
          } else {
            errors++;
            outcome = "error";
          }
        }
        csv.println(r + "," + outcome);
      }
    }
    db.closeDB();

    String json = "{\"worker\": " + workerIdx + ", \"rounds\": " + rounds
        + ", \"commits\": " + commits + ", \"aborts\": " + aborts
        + ", \"skips\": " + skips + ", \"errors\": " + errors + "}";
    try (PrintWriter w = new PrintWriter(new File(outDir, "worker-" + workerIdx + ".json"), "UTF-8")) {
      w.println(json);
    }
    System.out.println(json);
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
          + "|txn-counter-worker|txn-transfer-worker|txn-mixed-worker|txn-audit|txn-skew-worker"
          + "|seed-accounts|insert-probe|visibility-probe|put-long|get-long|hash>"
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
    case "txn-counter-worker":
      flags.put("txn", "true");
      counterWorker(flags);
      break;
    case "txn-transfer-worker":
    case "txn-mixed-worker":
      transferWorker(flags);
      break;
    case "txn-audit":
      txnAudit(flags);
      break;
    case "txn-skew-worker":
      skewWorker(flags);
      break;
    case "seed-accounts":
      seedAccounts(flags);
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
