#!/usr/bin/env python3
"""Consistency experiments for OzoneDB, mirroring the cr-sqlite bench.

Runs the same three measurements the cr-sqlite repo's `bench.py` provides
(probe-staleness / check-lost-updates / check-convergence), through OzoneDB's
real client path (ConsistencyProbe -> OzoneDBJNI -> libozonedb.so -> shared
Corfu log). Output files -- commits.csv, reads.csv, summary.json -- follow the
exact same contract, so crsqlite/bench/scripts/plot/plot_consistency.py can
plot both engines side by side.

Run this ON a client node (needs OZONEDB_HOME, a built libozonedb.so, and a
reachable Corfu server). All probe processes run on that one node so
System.nanoTime() is a single shared clock; the measured consistency path
still crosses the network, because every write and every remote-apply goes
through the Corfu log node either way.

Each experiment uses its own fresh Corfu stream (suffixed with a timestamp),
so it neither disturbs nor replays the benchmark stream.

  python3 consistency.py probe-staleness --write-rate 20 --duration 30
  python3 consistency.py check-lost-updates --workers 2 --increments 2000
  python3 consistency.py check-visibility --rate 20 --duration 15
  python3 consistency.py check-convergence --record-count 1000000 --sample 1000
"""

import argparse
import bisect
import json
import os
import subprocess
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "local"))
from load_local_ycsb import corfu_bridge_jar_path
from load_local_ycsb_multiproc import (
    _java_binary,
    _make_corfu_config_per_writer,
    _resolve_ycsb_classpath,
    partition_records,
)
from visibility_analysis import analyze as _analyze_visibility
from ycsb_config import derive as _derive_addresses

OZONEDB_HOME = os.environ.get("OZONEDB_HOME")
PROBE_CLASS = "site.ycsb.db.ConsistencyProbe"
ENGINE = "ozonedb-corfu"
# Config indices far above any benchmark writer count, so the per-writer
# shared_config_w{i}.json files written here never clobber a sweep's.
CFG_IDX_BASE = 900


def _require_home():
    if not OZONEDB_HOME:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")


def _outdir(name):
    d = os.path.join(
        OZONEDB_HOME, "bench", "results", "consistency",
        f"{name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
    )
    os.makedirs(d, exist_ok=True)
    return d


def _fresh_dir(path):
    subprocess.run(["rm", "-rf", path])
    os.makedirs(path, exist_ok=True)
    return path


def _load_cfg(path):
    import yaml

    with open(path) as f:
        return _derive_addresses(yaml.safe_load(f))


def _corfu_settings(cfg, stream, linearizable=False):
    s = dict(cfg["corfu"])
    s["stream_name"] = stream
    if linearizable:
        # Passed through to the per-writer shared_config by
        # _make_corfu_config_per_writer; makes every get fence on the
        # global log tail (see src/db/db.cpp).
        s["linearizable_reads"] = True
        s["trust_background_tail"] = False
    return s


def _kill_stale_probes():
    """Probe JVMs orphaned by an interrupted driver keep their corfu
    connections and can poison the shared log for every later experiment
    (their stream's entries raise the global tail other instances fence
    on). Any ConsistencyProbe already running on this node is such a
    leftover -- experiments never overlap by design -- so reap them."""
    r = subprocess.run(["pgrep", "-f", PROBE_CLASS], capture_output=True, text=True)
    pids = [p for p in r.stdout.split() if p.strip()]
    if pids:
        print(f"[cleanup] killing stale probe processes: {' '.join(pids)}")
        subprocess.run(["kill", "-9", *pids])
        time.sleep(1)


def _make_configs(cfg, stream, count, name, idx_base=CFG_IDX_BASE,
                  linearizable=False):
    """One shared_config JSON + fresh scratch db_path per probe process.
    The Corfu endpoint and stream are shared (that's the point); db_path
    must be private per process, exactly as in the multiproc runners."""
    data_root = cfg["local"]["run"]["ycsb_data_path"]
    settings = _corfu_settings(cfg, stream, linearizable=linearizable)
    paths = []
    for i in range(count):
        db_path = _fresh_dir(os.path.join(data_root, "consistency", f"{name}-w{i}"))
        paths.append(
            _make_corfu_config_per_writer(idx_base + i, db_path, settings, None)
        )
    return paths


CLASSPATH_CACHE = os.path.join(
    os.environ.get("OZONEDB_HOME") or "", "ycsb", ".consistency-classpath"
)


def _classpath(rebuild=False):
    """Persistently cached classpath. The multiproc runners re-run Maven on
    every invocation; for a 15-second probe that Maven pass dominates the
    wall time, so cache the resolved string across invocations and refresh
    only on --rebuild (or when a cached jar has vanished)."""
    if not rebuild and os.path.exists(CLASSPATH_CACHE):
        with open(CLASSPATH_CACHE) as f:
            cached = f.read().strip()
        first_jar = next(
            (p for p in cached.split(os.pathsep) if p.endswith(".jar")), None
        )
        if cached and (first_jar is None or os.path.exists(first_jar)):
            return cached
    ycsb_path = os.path.join(OZONEDB_HOME, "ycsb")
    base = _resolve_ycsb_classpath(ycsb_path, "ozonedb")
    cp = os.pathsep.join([corfu_bridge_jar_path(), base])
    with open(CLASSPATH_CACHE, "w") as f:
        f.write(cp)
    return cp


def _probe_cmd(cp, mode, flags):
    cmd = [_java_binary(), "-cp", cp, PROBE_CLASS, mode]
    for k, v in flags.items():
        cmd += [f"--{k}", str(v)]
    return cmd


def _spawn(cmd, log_path):
    log = open(log_path, "w")
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    return {"proc": proc, "log": log, "log_path": log_path}


def _wait_ready_then_go(out, ready_files, procs, timeout_s=300):
    """File barrier: wait for every probe process to finish its openDB()
    (JVM + Corfu connect + stream replay can take seconds and varies), then
    release them all at once."""
    deadline = time.time() + timeout_s
    while not all(os.path.exists(f) for f in ready_files):
        for p in procs:
            if p["proc"].poll() is not None and p["proc"].returncode != 0:
                raise RuntimeError(
                    f"probe process died before ready (rc={p['proc'].returncode}); "
                    f"see {p['log_path']}"
                )
        if time.time() > deadline:
            raise TimeoutError(
                f"probe processes not ready within {timeout_s}s. If their logs "
                "show the corfu tailer polling but openDB never returning, the "
                "corfu server has entries from other streams in its global log "
                "and the engine's fenced reads block forever on a fresh stream "
                "-- restart corfu_server first (the YCSB sweep restarts it "
                "before every run for the same reason)."
            )
        time.sleep(0.2)
    go = os.path.join(out, "go")
    with open(go, "w"):
        pass
    return time.time()


def _wait_all(procs):
    rcs = []
    for p in procs:
        rcs.append(p["proc"].wait())
        if rcs[-1] != 0:
            print(f"[warning] probe process rc={rcs[-1]}, see {p['log_path']}")
    return rcs


def _reap(procs):
    for p in procs:
        if p["proc"].poll() is None:
            p["proc"].kill()
        p["log"].close()


def _last_json_line(log_path):
    last = None
    with open(log_path, errors="replace") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                last = line
    if last is None:
        raise RuntimeError(f"no JSON output line found in {log_path}")
    return json.loads(last)


def _pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))]


# ================================================================== staleness


def cmd_probe_staleness(cfg, args):
    """One process bumps a sequence key through the shared log at a fixed
    rate; a second process (own DB instance, own Corfu tailer) polls it.
    Staleness on this engine is the log apply lag -- milliseconds -- where
    cr-sqlite's is its anti-entropy interval. Same files, same arithmetic."""
    out = _outdir("staleness")
    tag = os.path.basename(out).split("-", 1)[1]
    stream = args.stream or f"{cfg['corfu']['stream_name']}-probe-{tag}"
    _kill_stale_probes()
    cfgs = _make_configs(cfg, stream, 2, f"staleness-{tag}",
                         linearizable=args.linearizable)
    cp = _classpath(args.rebuild)

    ready_w = os.path.join(out, "ready-writer")
    ready_r = os.path.join(out, "ready-reader")
    go = os.path.join(out, "go")
    common = {"out-dir": out, "go-file": go, "key": "__probe__"}
    writer_cmd = _probe_cmd(cp, "write-probe", {
        **common, "config": cfgs[0], "ready-file": ready_w,
        "rate": args.write_rate, "duration-s": args.duration,
    })
    reader_cmd = _probe_cmd(cp, "read-probe", {
        **common, "config": cfgs[1], "ready-file": ready_r,
        "duration-s": args.duration + args.settle,
        "read-interval-ms": args.read_interval_ms,
    })

    procs = [
        _spawn(writer_cmd, os.path.join(out, "writer.log")),
        _spawn(reader_cmd, os.path.join(out, "reader.log")),
    ]
    try:
        _wait_ready_then_go(out, [ready_w, ready_r], procs)
        _wait_all(procs)
    finally:
        _reap(procs)

    commits = []
    with open(os.path.join(out, "commits.csv")) as f:
        next(f)
        for line in f:
            s, t = line.strip().split(",")
            commits.append((int(s), int(t)))
    reads = []
    with open(os.path.join(out, "reads.csv")) as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            # 3rd column (t_start_ns) added for the linearizability check;
            # tolerate 2-column files from older probes.
            reads.append((int(parts[0]), int(parts[1]),
                          int(parts[2]) if len(parts) > 2 else int(parts[0])))

    commit_times = [t for _, t in commits]
    stale = 0
    version_lags = []
    staleness_ms = []
    violations = 0
    for t_read, seen, t_start in reads:
        latest = bisect.bisect_right(commit_times, t_read) - 1
        lag = max(0, latest - seen)
        version_lags.append(lag)
        if lag > 0:
            stale += 1
            staleness_ms.append((t_read - commit_times[seen + 1]) / 1e6)
        # Real-time (linearizability) check: judged against the get's START.
        # A get overlapping a commit may correctly return the older value;
        # only missing a write committed before the get began violates
        # linearizability. Strict mode should drive this to zero while
        # stale_read_fraction (vs t_read, the cr-sqlite-compatible metric)
        # stays nonzero from reads that overlap their write.
        latest_at_start = bisect.bisect_right(commit_times, t_start) - 1
        if latest_at_start - seen > 0:
            violations += 1

    summary = {
        "engine": ENGINE + ("-linearizable" if args.linearizable else ""),
        "linearizable_reads": args.linearizable,
        "writes": commits[-1][0] if commits else 0,
        "reads": len(reads),
        "stale_reads": stale,
        "stale_read_fraction": (stale / len(reads)) if reads else 0.0,
        "version_lag_p50": _pct(version_lags, 50),
        "version_lag_p95": _pct(version_lags, 95),
        "version_lag_max": max(version_lags) if version_lags else 0,
        "staleness_ms_p50": round(_pct(staleness_ms, 50), 2),
        "staleness_ms_p95": round(_pct(staleness_ms, 95), 2),
        "staleness_ms_max": round(max(staleness_ms), 2) if staleness_ms else 0.0,
        "linearizability_violations": violations,
        "linearizability_violation_fraction":
            (violations / len(reads)) if reads else 0.0,
        # No anti-entropy interval on this engine; 0 keeps the field's type
        # for plot scripts that read it.
        "sync_interval_ms": 0,
        "write_rate_per_s": args.write_rate,
        "duration_s": args.duration,
        "read_interval_ms": args.read_interval_ms,
        "stream": stream,
    }
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    print(
        f"\n=> {stale} of {len(reads)} reads on the second instance were stale "
        f"({summary['stale_read_fraction']:.1%}); staleness here is Corfu apply "
        f"lag, not an anti-entropy interval. Results in {out}"
    )


# ================================================================= visibility


def cmd_check_visibility(cfg, args):
    """Two benchmarks from one run over YCSB-keyspace inserts (PBS-style,
    see bench/PLAN-visibility.md). A writer inserts brand-new keys and acks
    each into a notify file; a reader polls every acked key until it
    appears. B2 (miss rate): did the reader's FIRST get -- which starts
    after the ack by construction, no clocks involved -- find the key?
    B1 (t-visibility): how long after the ack until some get found it?
    New keys make "not found" unambiguous staleness: no older value can
    masquerade as success."""
    out = _outdir("visibility")
    tag = os.path.basename(out).split("-", 1)[1]
    stream = args.stream or f"{cfg['corfu']['stream_name']}-vis-{tag}"
    n = max(1, args.writers)
    _kill_stale_probes()
    # Writers 0..n-1, reader is instance n. Every writer shares the one
    # Corfu stream (that's the point of the scaling experiment); disjoint
    # --key-base ranges keep the keyspaces from colliding.
    cfgs = _make_configs(cfg, stream, n + 1, f"visibility-{tag}",
                         linearizable=args.linearizable)
    cp = _classpath(args.rebuild)

    go = os.path.join(out, "go")
    done = os.path.join(out, "done")
    ready_r = os.path.join(out, "ready-reader")
    ready_ws = [os.path.join(out, f"ready-writer-{w}") for w in range(n)]
    notify_files = [os.path.join(out, f"notify-{w}.csv") for w in range(n)]

    writer_procs = []
    for w in range(n):
        writer_procs.append(_spawn(_probe_cmd(cp, "insert-probe", {
            "config": cfgs[w], "ready-file": ready_ws[w], "go-file": go,
            "out-dir": out, "notify-file": notify_files[w],
            "inserts-file": f"inserts-w{w}.csv",
            "key-base": w * 10_000_000,
            "rate": args.rate, "duration-s": args.duration,
            "value-size": args.value_size,
        }), os.path.join(out, f"writer-{w}.log")))
    reader_proc = _spawn(_probe_cmd(cp, "visibility-probe", {
        "config": cfgs[n], "ready-file": ready_r, "go-file": go,
        "out-dir": out, "notify-file": ",".join(notify_files),
        "done-file": done,
        # Strict runs fence once per tick and share it across the whole
        # pending batch (DB::sync) -- the cr-sqlite /barrier analog.
        # Without it each get fences individually and the reader's ~ms
        # service time queues at high writer counts, inflating the tail
        # with probe-side waiting (misses are unaffected either way).
        "tick-fence": "true" if args.linearizable else "false",
        "poll-ms": args.poll_ms, "key-timeout-s": args.key_timeout_s,
        "run-timeout-s": args.duration + args.key_timeout_s + 120,
    }), os.path.join(out, "reader.log"))
    procs = writer_procs + [reader_proc]
    try:
        _wait_ready_then_go(out, ready_ws + [ready_r], procs)
        # Writers no longer touch the done file themselves: with n of
        # them, "done" means ALL writers have exited, which only the
        # orchestrator can observe. The reader then drains every notify
        # file to EOF before finishing.
        _wait_all(writer_procs)
        with open(done, "w"):
            pass
        _wait_all([reader_proc])
    finally:
        _reap(procs)

    _analyze_visibility(out, ENGINE, args.linearizable, cross_node=False, params={
        "rate_per_s": args.rate,
        "writers": n,
        "aggregate_rate_per_s": n * args.rate,
        "tick_fence": bool(args.linearizable),
        "duration_s": args.duration,
        "value_size": args.value_size,
        "poll_ms": args.poll_ms,
        "stream": stream,
    })


def cmd_visibility_reader(cfg, args):
    """Reader half of the cross-node visibility experiment: listen for the
    writer's TCP ack notifications and poll every notified key. Started on
    clients[1] by run_visibility_cross_node.sh; the writer's connect only
    succeeds once this probe is listening (i.e. after its openDB), so the
    socket handshake is the start barrier. Leaves visibility.csv in
    --out-dir for the laptop to pull -- home dirs are NOT shared between
    nodes."""
    out = os.path.join(OZONEDB_HOME, args.out_dir)
    os.makedirs(out, exist_ok=True)
    _kill_stale_probes()
    cfgs = _make_configs(cfg, args.stream, 1,
                         f"visx-reader-{os.path.basename(out)}",
                         idx_base=990, linearizable=args.linearizable)
    cp = _classpath(args.rebuild)
    cmd = _probe_cmd(cp, "visibility-probe", {
        "config": cfgs[0], "out-dir": out, "notify-listen": args.port,
        "notify-writers": args.writers,
        "tick-fence": "true" if args.linearizable else "false",
        "poll-ms": args.poll_ms, "key-timeout-s": args.key_timeout_s,
        "run-timeout-s": args.run_timeout_s,
    })
    proc = _spawn(cmd, os.path.join(out, "reader.log"))
    try:
        rc = proc["proc"].wait()
    finally:
        _reap([proc])
    if rc != 0:
        raise RuntimeError(f"visibility-probe rc={rc}, see {proc['log_path']}")
    print(f"reader done, visibility.csv in {out}")


def cmd_visibility_writer(cfg, args):
    """Writer half of the cross-node visibility experiment: insert new keys
    and ack each over TCP to the reader at --connect (reader's LAN
    address). Blocks until the insert phase ends; closing the socket is the
    done signal the reader drains against.

    Deliberately does NOT call _kill_stale_probes(): N writer invocations
    run concurrently on this node in the multi-writer flow, and each
    later-starting one would SIGKILL its already-running siblings (the
    kill matches every ConsistencyProbe JVM). The laptop driver performs
    ONE stale-probe kill over ssh before launching the writer fleet."""
    out = os.path.join(OZONEDB_HOME, args.out_dir)
    os.makedirs(out, exist_ok=True)
    # Per-worker config index / db_path / logs: N writer invocations share
    # this node and this out-dir, so everything they create must be keyed
    # by the worker index (930+w stays clear of the reader's 990).
    cfgs = _make_configs(cfg, args.stream, 1,
                         f"visx-writer{args.worker}-{os.path.basename(out)}",
                         idx_base=930 + args.worker,
                         linearizable=args.linearizable)
    cp = _classpath(args.rebuild)
    cmd = _probe_cmd(cp, "insert-probe", {
        "config": cfgs[0], "out-dir": out, "notify-connect": args.connect,
        "key-base": args.worker * 10_000_000,
        "inserts-file": f"inserts-w{args.worker}.csv",
        "rate": args.rate, "duration-s": args.duration,
        "value-size": args.value_size,
    })
    proc = _spawn(cmd, os.path.join(out, f"writer-{args.worker}.log"))
    try:
        rc = proc["proc"].wait()
    finally:
        _reap([proc])
    if rc != 0:
        raise RuntimeError(f"insert-probe rc={rc}, see {proc['log_path']}")
    print(f"writer done, inserts.csv in {out}")


# =============================================================== lost updates


def cmd_check_lost_updates(cfg, args):
    """K read-modify-write increments of one counter split across N writer
    processes on the shared log. OzoneDB's KV surface is get/put (no CAS or
    transactions), so the RMW race window is one log-append + apply latency
    -- milliseconds -- where cr-sqlite's is its sync interval, and unlike
    cr-sqlite no acknowledged put is ever discarded (appends are totally
    ordered; put returns only after the record is sequenced). --rate paces
    each worker to sweep the conflict window."""
    out = _outdir("lost-updates")
    tag = os.path.basename(out).split("-", 2)[2]
    stream = args.stream or f"{cfg['corfu']['stream_name']}-lostupd-{tag}"
    n = args.workers
    _kill_stale_probes()
    # workers + seed + final reader each get their own instance
    cfgs = _make_configs(cfg, stream, n + 2, f"lost-updates-{tag}",
                         linearizable=args.linearizable)
    cp = _classpath(args.rebuild)

    seed_cmd = _probe_cmd(cp, "put-long", {
        "config": cfgs[n], "key": "__counter__", "value": 0,
    })
    with open(os.path.join(out, "seed.log"), "w") as log:
        subprocess.run(seed_cmd, stdout=log, stderr=subprocess.STDOUT, check=True)

    per_worker = partition_records(args.increments, n)
    go = os.path.join(out, "go")
    ready_files = []
    procs = []
    for i, (_, cnt) in enumerate(per_worker):
        ready = os.path.join(out, f"ready-{i}")
        ready_files.append(ready)
        cmd = _probe_cmd(cp, "counter-worker", {
            "config": cfgs[i], "key": "__counter__", "increments": cnt,
            "rate": args.rate, "worker": i, "out-dir": out,
            "ready-file": ready, "go-file": go,
        })
        procs.append(_spawn(cmd, os.path.join(out, f"worker-{i}.log")))

    try:
        t_go = _wait_ready_then_go(out, ready_files, procs)
        _wait_all(procs)
        elapsed = time.time() - t_go
    finally:
        _reap(procs)

    workers = []
    for i in range(n):
        with open(os.path.join(out, f"worker-{i}.json")) as f:
            workers.append(json.load(f))
    acked = sum(w["acked"] for w in workers)

    time.sleep(args.settle)
    final_cmd = _probe_cmd(cp, "get-long", {
        "config": cfgs[n + 1], "key": "__counter__",
        "wait-stable": "true", "poll-ms": 500, "timeout-s": 120,
    })
    final_log = os.path.join(out, "final.log")
    with open(final_log, "w") as log:
        subprocess.run(final_cmd, stdout=log, stderr=subprocess.STDOUT, check=True)
    f_val = _last_json_line(final_log)["value"]

    k = args.increments
    summary = {
        "engine": ENGINE + ("-linearizable" if args.linearizable else ""),
        "linearizable_reads": args.linearizable,
        "mode": "ozonedb writers (shared Corfu log)",
        "replicas": n,
        "workers": n,
        "increments_issued": k,
        "increments_acked": acked,
        "final_counter": f_val,
        "final_per_replica": [f_val],
        "last_written_per_worker": [w["last_written"] for w in workers],
        "lost_updates": k - f_val,
        "lost_fraction": (k - f_val) / k,
        "elapsed_s": round(elapsed, 2),
        "convergence_s": round(args.settle, 2),
        "sync_interval_ms": 0,
        "rate_per_worker_per_s": args.rate,
        "stream": stream,
    }
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    verdict = (
        "no updates lost"
        if k == f_val
        else f"{k - f_val} of {k} increments lost ({(k - f_val) / k:.1%}) -- "
        "get+put RMW races within the ~ms apply window; no acknowledged put "
        "was discarded by the log itself"
    )
    print(f"\n=> final counter {f_val} after {k} increments: {verdict}. Results in {out}")


# ================================================================ convergence


def cmd_check_convergence(cfg, args):
    """Open N fresh instances of the same stream (default: the benchmark
    stream) and md5 a sample of the YCSB keyspace on each until stable.
    Equal digests = every instance materialized the same totally-ordered
    state; stable_after_s includes the fresh instance's replay time."""
    out = _outdir("convergence")
    tag = os.path.basename(out).split("-", 1)[1]
    stream = args.stream or cfg["corfu"]["stream_name"]
    record_cnt = args.record_count or int(cfg["local"]["run"]["record_cnt"])
    sample_every = max(1, record_cnt // args.sample)
    _kill_stale_probes()
    cfgs = _make_configs(cfg, stream, args.instances, f"convergence-{tag}",
                         linearizable=args.linearizable)
    cp = _classpath(args.rebuild)

    procs = []
    logs = []
    for i in range(args.instances):
        cmd = _probe_cmd(cp, "hash", {
            "config": cfgs[i], "record-count": record_cnt,
            "sample-every": sample_every, "wait-stable": "true",
            "poll-ms": args.poll_ms, "timeout-s": args.timeout_s,
        })
        log_path = os.path.join(out, f"hash-{i}.log")
        logs.append(log_path)
        procs.append(_spawn(cmd, log_path))
    _wait_all(procs)
    _reap(procs)

    instances = [_last_json_line(lp) for lp in logs]
    for i, inst in enumerate(instances):
        print(
            f"instance {i}: checked={inst['checked']} present={inst['present']} "
            f"md5={inst['md5']} stable_after_s={inst['stable_after_s']}"
        )
    equal = len({inst["md5"] for inst in instances}) == 1
    summary = {
        "engine": ENGINE + ("-linearizable" if args.linearizable else ""),
        "linearizable_reads": args.linearizable,
        "stream": stream,
        "record_count": record_cnt,
        "sample_every": sample_every,
        "instances": instances,
        "hashes_equal": equal,
    }
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(
        f"\n=> {'CONVERGED: all' if equal else 'DIVERGED: not all'} "
        f"{args.instances} instances materialize identical state. Results in {out}"
    )
    if not equal:
        sys.exit(1)


# ======================================================================= main


def main():
    _require_home()
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--linearizable", action="store_true",
                   help="set linearizable_reads=true in the shared_config "
                        "(strict cross-writer reads)")
    p.add_argument("--rebuild", action="store_true",
                   help="re-run the Maven classpath resolution instead of using the cache")
    p.add_argument(
        "--config",
        default=os.path.join(OZONEDB_HOME, "bench/scripts/config/ycsb.yaml"),
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe-staleness", help="stale reads across two instances")
    sp.add_argument("--write-rate", type=float, default=20.0, help="probe writes/s")
    sp.add_argument("--duration", type=int, default=30, help="write phase seconds")
    sp.add_argument("--read-interval-ms", type=int, default=5)
    sp.add_argument("--settle", type=int, default=5,
                    help="extra reader seconds after the writer stops")
    sp.add_argument("--stream", default=None, help="override the probe stream name")

    sp = sub.add_parser("check-lost-updates",
                        help="concurrent RMW increments across writer processes")
    sp.add_argument("--workers", type=int, default=2)
    sp.add_argument("--increments", type=int, default=2000, help="total across workers")
    sp.add_argument("--rate", type=float, default=0.0,
                    help="increments/s per worker (0 = as fast as possible)")
    sp.add_argument("--settle", type=float, default=3.0,
                    help="seconds to wait before the final read")
    sp.add_argument("--stream", default=None)

    sp = sub.add_parser("check-visibility",
                        help="insert visibility: retry-until-found latency "
                             "+ first-read miss rate (PBS-style)")
    sp.add_argument("--rate", type=float, default=20.0,
                    help="inserts/s PER WRITER (aggregate = writers x rate)")
    sp.add_argument("--writers", type=int, default=1,
                    help="concurrent writer processes, all on the one shared "
                         "stream, disjoint key ranges (scaling experiments)")
    sp.add_argument("--duration", type=int, default=15, help="insert phase seconds")
    sp.add_argument("--value-size", type=int, default=1000, help="value bytes")
    sp.add_argument("--poll-ms", type=int, default=1,
                    help="reader poll interval (bounds B1 resolution)")
    sp.add_argument("--key-timeout-s", type=int, default=30,
                    help="give up on a key after this long (>> sync interval)")
    sp.add_argument("--stream", default=None)

    sp = sub.add_parser("visibility-reader",
                        help="cross-node reader role (driven by run_visibility_cross_node.sh)")
    sp.add_argument("--out-dir", required=True,
                    help="run dir relative to OZONEDB_HOME")
    sp.add_argument("--stream", required=True)
    sp.add_argument("--port", type=int, default=7911)
    sp.add_argument("--writers", type=int, default=1,
                    help="writer connections to accept before the go barrier")
    sp.add_argument("--poll-ms", type=int, default=1)
    sp.add_argument("--key-timeout-s", type=int, default=30)
    sp.add_argument("--run-timeout-s", type=int, default=600)

    sp = sub.add_parser("visibility-writer",
                        help="cross-node writer role (driven by run_visibility_cross_node.sh)")
    sp.add_argument("--out-dir", required=True)
    sp.add_argument("--stream", required=True)
    sp.add_argument("--connect", required=True, help="reader LAN host:port")
    sp.add_argument("--worker", type=int, default=0,
                    help="writer index: disjoint key range, config slot, logs")
    sp.add_argument("--rate", type=float, default=20.0)
    sp.add_argument("--duration", type=int, default=15)
    sp.add_argument("--value-size", type=int, default=1000)

    sp = sub.add_parser("check-convergence",
                        help="hash a key sample on N fresh instances of one stream")
    sp.add_argument("--record-count", type=int, default=None,
                    help="YCSB recordcount of the stream (default: local.run.record_cnt)")
    sp.add_argument("--sample", type=int, default=1000, help="approx keys to check")
    sp.add_argument("--instances", type=int, default=2)
    sp.add_argument("--poll-ms", type=int, default=2000)
    sp.add_argument("--timeout-s", type=int, default=900)
    sp.add_argument("--stream", default=None,
                    help="stream to check (default: the benchmark stream)")

    args = p.parse_args()
    cfg = _load_cfg(args.config)
    if args.cmd == "probe-staleness":
        cmd_probe_staleness(cfg, args)
    elif args.cmd == "check-lost-updates":
        cmd_check_lost_updates(cfg, args)
    elif args.cmd == "check-visibility":
        cmd_check_visibility(cfg, args)
    elif args.cmd == "visibility-reader":
        cmd_visibility_reader(cfg, args)
    elif args.cmd == "visibility-writer":
        cmd_visibility_writer(cfg, args)
    elif args.cmd == "check-convergence":
        cmd_check_convergence(cfg, args)


if __name__ == "__main__":
    main()
