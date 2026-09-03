#!/usr/bin/env python3
"""Phase 1 — Multi-process writer-scaling sweep.

Drives a (system × workload × n_processes × repeat) matrix where each cell
spawns N independent OS processes, each running YCSB single-threaded against
its OWN DB directory/file pre-loaded with the full 1M × 1KB Zipfian dataset.

Differences vs the multi-thread orchestrator (run_fig2_sweep.py):
  * "threadcount" per process is fixed at 1; concurrency comes from N processes.
  * Each process gets an independent DB copy (cached_DB → work_path_proc{i}).
  * OzoneDB gets a per-process shared_config JSON to avoid the
    config-file-collision noted in run_fig2_sweep.py:211-228.
  * Aggregate per-cell throughput = sum of per-process throughputs.
  * One drop_caches per cell (before all N workers spawn), not per-process.
  * Per-process logs at logs/{system}_workload{w}_p{N}_r{R}_proc{i}.log.

Invocation:
    python3 run_phase1_multiproc_sweep.py \
        --systems ozonedb,trunkcpp,bcw2 \
        --workloads a,b,c,d,f \
        --processes 2,4,8,16 \
        --repeats 3 \
        --duration 120 \
        --results-dir /users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase1-multi-process

Reads cached DBs from:
    ozonedb:  /tank/ycsb_data/cached_data-ozonedb-1KB-1000000  (dir)
    trunkcpp: /tank/ycsb_data/trunkcpp_ycsb/cached_trunkcpp_ycsb.db  (file + -wal/-shm)
    bcw2:     /tank/ycsb_data/bcw2_ycsb/cached_bcw_ycsb.db  (file + -wal/-wal2/-shm)

Per-process work paths land under:
    /tank/ycsb_data/multi-proc-run/{system}_p{N}_proc{i}{suffix}

Tear-down: every per-cell work path is wiped after each repeat. Cached_* is preserved.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# -----------------------------------------------------------------------------
# Static config
# -----------------------------------------------------------------------------

# Paths are env-overridable so the sweep can run from a relocated checkout.
# Defaults target the ozonedb-revision-backup layout.
YCSB_CPP_DIR = Path(os.environ.get(
    "YCSB_CPP_DIR", "/users/Xinying/ozonedb-revision-backup/YCSB-cpp"))
YCSB_CPP_BIN = YCSB_CPP_DIR / "ycsb"

OZONEDB_HOME = Path(os.environ.get(
    "OZONEDB_HOME", "/users/Xinying/ozonedb-revision-backup/ozonedb"))
OZONEDB_YCSB = OZONEDB_HOME / "ycsb"
OZONEDB_BIN = OZONEDB_YCSB / "bin" / "ycsb"

DATA_ROOT = Path(os.environ.get("YCSB_DATA_ROOT", "/tank/ycsb_data"))
WORK_ROOT = DATA_ROOT / "multi-proc-run"  # parent for all per-process working DBs

DEFAULT_RECORD_COUNT = 1_000_000
DEFAULT_WORKLOADS = ["a", "b", "c", "d", "f"]
DEFAULT_PROCESSES = [2, 4, 8, 16]
DEFAULT_RUN_DURATION = 120
DEFAULT_REPEATS = 3
DEFAULT_VARIANCE_PCT = 10.0
PER_RUN_TIMEOUT_S = 60 * 30
TIME_BIN = "/usr/bin/time"

CONFIG_TMP_ROOT = Path("/tmp/phase1_ozonedb_configs")

# Populated by warmup_ozonedb_classpath() before any worker spawns. Workers
# then bypass bin/ycsb (which would race on parallel `mvn package` calls) and
# invoke java directly.
OZONEDB_JAVA_CMD_PREFIX: Optional[list[str]] = None


# -----------------------------------------------------------------------------
# System descriptors — only the three Phase-1 systems
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class SystemSpec:
    name: str
    cached_path: Path
    aux_suffixes: tuple = ()
    cached_is_dir: bool = False
    work_suffix: str = ""  # appended to per-process base name; ".db" for sqlite-style

    def per_proc_work_path(self, n_procs: int, proc_id: int) -> Path:
        base = WORK_ROOT / f"{self.name}_p{n_procs}_proc{proc_id}{self.work_suffix}"
        return base

    def restore_into(self, work_path: Path) -> None:
        """Copy cached DB into a fresh work_path. Wipes any prior state at that path."""
        self._wipe(work_path)
        if self.cached_is_dir:
            subprocess.run(
                ["cp", "-r", "--reflink=auto", str(self.cached_path), str(work_path)],
                check=True,
            )
        else:
            subprocess.run(
                ["cp", "--reflink=auto", str(self.cached_path), str(work_path)],
                check=True,
            )
            for suf in self.aux_suffixes:
                src = self.cached_path.with_name(self.cached_path.name + suf)
                if src.exists():
                    subprocess.run(
                        ["cp", "--reflink=auto", str(src),
                         str(work_path.with_name(work_path.name + suf))],
                        check=True,
                    )

    def _wipe(self, work_path: Path) -> None:
        if work_path.exists():
            if work_path.is_dir():
                shutil.rmtree(work_path, ignore_errors=True)
            else:
                work_path.unlink(missing_ok=True)
        for suf in self.aux_suffixes:
            p = work_path.with_name(work_path.name + suf)
            if p.exists():
                p.unlink(missing_ok=True)

    def cleanup(self, work_path: Path) -> None:
        self._wipe(work_path)


def system_specs() -> dict[str, SystemSpec]:
    return {
        "ozonedb": SystemSpec(
            name="ozonedb",
            cached_path=DATA_ROOT / "cached_data-ozonedb-1KB-1000000",
            cached_is_dir=True,
            work_suffix="",
        ),
        "trunkcpp": SystemSpec(
            name="trunkcpp",
            cached_path=DATA_ROOT / "trunkcpp_ycsb" / "cached_trunkcpp_ycsb.db",
            aux_suffixes=("-wal", "-wal2", "-shm"),
            work_suffix=".db",
        ),
        "bcw2": SystemSpec(
            name="bcw2",
            cached_path=DATA_ROOT / "bcw2_ycsb" / "cached_bcw_ycsb.db",
            aux_suffixes=("-wal", "-wal2", "-shm"),
            work_suffix=".db",
        ),
    }


# -----------------------------------------------------------------------------
# Workload-file management
# -----------------------------------------------------------------------------


def workload_file(workload: str) -> Path:
    if workload == "a":
        return YCSB_CPP_DIR / "workloads" / "workloada_hctree"
    return YCSB_CPP_DIR / "workloads" / f"workload{workload}"


def ensure_ozonedb_workload(workload: str, record_count: int) -> Path:
    """Pre-generate (idempotent) the OzoneDB workload file. Returns path."""
    wl_dir = OZONEDB_YCSB / "workloads" / "generated_workloads"
    wl_dir.mkdir(parents=True, exist_ok=True)
    wl_path = wl_dir / f"workload{workload}_1KB_{record_count}_{record_count}"
    if not wl_path.exists():
        subprocess.run(
            ["python3",
             str(OZONEDB_HOME / "bench" / "scripts" / "generate_workload.py"),
             "--workload_name", workload,
             "--key_size", "1KB",
             "--operation_cnt", str(record_count),
             "--record_cnt", str(record_count)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    return wl_path


def pre_generate_all_workloads(workloads: list[str], record_count: int) -> None:
    """Idempotently pre-generate every OzoneDB workload before any worker spawns,
    eliminating the TOCTOU race on parallel exists()/generate calls."""
    for w in workloads:
        ensure_ozonedb_workload(w, record_count)


def warmup_ozonedb_classpath(record_count: int) -> list[str]:
    """Run the bin/ycsb wrapper ONCE to (a) build the YCSB jars via Maven and
    (b) print its resolved `java -cp ... site.ycsb.Client -db ...` command line.

    Subsequent OzoneDB workers bypass the wrapper and invoke java directly with
    that prefix, which avoids the parallel-mvn race observed at N=16 where
    multiple wrappers stomp each other in ~/.m2.

    Returns the list-form java command prefix
    (e.g. ['java','-cp', '<cp>', 'site.ycsb.Client','-db','site.ycsb.db.OzoneDBClient']).
    """
    print("[warmup] resolving OzoneDB classpath via single bin/ycsb run "
          "(this builds jars; ~30-45 s on cold ~/.m2)...", flush=True)

    # Pick a workload that's known to work for the warmup run.
    ensure_ozonedb_workload("c", record_count)
    wl_path = OZONEDB_YCSB / "workloads" / "generated_workloads" / (
        f"workloadc_1KB_{record_count}_{record_count}"
    )

    spec = system_specs()["ozonedb"]
    warm_work = WORK_ROOT / "ozonedb_warmup"
    if warm_work.exists():
        shutil.rmtree(warm_work, ignore_errors=True)
    spec.restore_into(warm_work)

    CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    cfg_path = CONFIG_TMP_ROOT / "shared_config_warmup.json"
    with open(OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json") as f:
        cfg = json.load(f)
    cfg["db_path"] = str(warm_work) + "/"
    with cfg_path.open("w") as f:
        json.dump(cfg, f, indent=4)

    env = os.environ.copy()
    env["OZONEDB_HOME"] = str(OZONEDB_HOME)

    cmd = [str(OZONEDB_BIN), "run", "ozonedb",
           "-threads", "1", "-s",
           "-P", str(wl_path),
           "-p", "status.interval=1",
           "-p", "maxexecutiontime=2",
           "-p", "operationcount=0",
           "-p", f"shared_config={cfg_path}"]

    proc = subprocess.run(cmd, env=env, cwd=str(OZONEDB_YCSB),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(
            f"warmup OzoneDB run failed (rc={proc.returncode}):\n{proc.stderr[-4000:]}"
        )

    # The wrapper prints the resolved `java -cp <cp> site.ycsb.Client -db ...`
    # command to stderr. Find it.
    java_line = None
    for line in proc.stderr.splitlines():
        if " site.ycsb.Client " in line and "-cp" in line:
            java_line = line
            break
    if java_line is None:
        raise RuntimeError(
            "could not find printed java command in wrapper stderr; "
            "first 200 lines:\n" + "\n".join(proc.stderr.splitlines()[:200])
        )

    parts = shlex.split(java_line)
    # Truncate at the "-db <classname>" pair: that's the prefix every worker
    # shares. The remaining flags (-threads, -P, -p ..., -t) are appended
    # per-worker.
    if "site.ycsb.Client" not in parts:
        raise RuntimeError(f"unexpected java line: {java_line}")
    main_idx = parts.index("site.ycsb.Client")
    if main_idx + 2 >= len(parts) or parts[main_idx + 1] != "-db":
        raise RuntimeError(f"unexpected java line shape: {java_line}")
    prefix = parts[: main_idx + 3]  # ['java', ..., 'site.ycsb.Client', '-db', '<classname>']

    # Cleanup warmup state
    spec.cleanup(warm_work)
    cfg_path.unlink(missing_ok=True)

    print(f"[warmup] resolved prefix: {prefix[0]} ... -cp <{len(prefix[2])} chars> "
          f"site.ycsb.Client -db {prefix[-1]}", flush=True)
    return prefix


# -----------------------------------------------------------------------------
# Per-process command builder (threadcount=1 per process)
# -----------------------------------------------------------------------------


def build_proc_cmd(system: SystemSpec, workload: str, n_procs: int, repeat: int,
                   proc_id: int, work_path: Path, duration: int, record_count: int
                   ) -> tuple[list[str], dict[str, str], Optional[Path]]:
    env = os.environ.copy()
    cwd: Optional[Path] = None

    if system.name in ("trunkcpp", "bcw2"):
        prop = {"trunkcpp": "sqlite-trunk.properties",
                "bcw2": "sqlite-bcw.properties"}[system.name]
        cmd = [
            str(YCSB_CPP_BIN),
            "-db", "sqlite",
            "-run",
            "-P", str(workload_file(workload)),
            "-P", str(YCSB_CPP_DIR / "sqlite" / prop),
            "-p", f"sqlite.dbpath={work_path}",
            "-p", "threadcount=1",
            "-p", f"recordcount={record_count}",
            "-p", f"maxexecutiontime={duration}",
        ]
        return cmd, env, cwd

    if system.name == "ozonedb":
        if OZONEDB_JAVA_CMD_PREFIX is None:
            raise RuntimeError(
                "OzoneDB classpath not resolved; warmup_ozonedb_classpath() "
                "must run before any worker spawn."
            )
        env["OZONEDB_HOME"] = str(OZONEDB_HOME)
        wl_path = ensure_ozonedb_workload(workload, record_count)

        # Per-process shared_config — avoids collisions when N processes spawn
        # in parallel.
        CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        cfg_in = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json"
        with open(cfg_in) as f:
            cfg = json.load(f)
        cfg["db_path"] = str(work_path) + "/"
        cfg_out = CONFIG_TMP_ROOT / (
            f"shared_config_workload{workload}_p{n_procs}_r{repeat}_proc{proc_id}.json"
        )
        with cfg_out.open("w") as f:
            json.dump(cfg, f, indent=4)

        # Bypass bin/ycsb (which would race on parallel `mvn package`).
        # The prefix already includes ['java', ..., '-cp', '<cp>',
        # 'site.ycsb.Client', '-db', 'site.ycsb.db.OzoneDBClient'].
        # Trailing '-t' tells site.ycsb.Client to do a transaction (run) phase.
        cmd = list(OZONEDB_JAVA_CMD_PREFIX) + [
            "-threads", "1",
            "-s",
            "-P", str(wl_path),
            "-p", "status.interval=1",
            "-p", f"maxexecutiontime={duration}",
            "-p", "operationcount=0",
            "-p", f"shared_config={cfg_out}",
            "-t",
        ]
        cwd = OZONEDB_YCSB
        return cmd, env, cwd

    raise ValueError(f"unsupported system in Phase 1: {system.name}")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def drop_caches() -> None:
    subprocess.run(["sync"], check=False)
    subprocess.run(
        ["sudo", "-n", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
        check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def kill_stragglers() -> None:
    self_pid = os.getpid()
    for pat in (r"^.*YCSB-cpp/ycsb\b", r"^.*site\.ycsb\.Client"):
        try:
            out = subprocess.check_output(["pgrep", "-f", pat], text=True).strip()
        except subprocess.CalledProcessError:
            continue
        for pid_str in out.splitlines():
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            if pid == self_pid:
                continue
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    time.sleep(1)
    # SIGKILL anything still alive
    for pat in (r"^.*YCSB-cpp/ycsb\b", r"^.*site\.ycsb\.Client"):
        try:
            out = subprocess.check_output(["pgrep", "-f", pat], text=True).strip()
        except subprocess.CalledProcessError:
            continue
        for pid_str in out.splitlines():
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            if pid == self_pid:
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


_NUM = r"[\d.]+(?:[eE][+-]?\d+)?"
_THROUGHPUT_PATTERNS = [
    re.compile(rf"Run throughput\(ops/sec\):\s*({_NUM})"),
    re.compile(rf"^\[OVERALL\],\s+Throughput\(ops/sec\),\s+({_NUM})", re.MULTILINE),
]
_RSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")


def parse_throughput(text: str) -> Optional[float]:
    for pat in _THROUGHPUT_PATTERNS:
        m = pat.search(text)
        if m:
            return float(m.group(1))
    return None


def parse_max_rss_kb(text: str) -> Optional[int]:
    m = _RSS_RE.search(text)
    return int(m.group(1)) if m else None


# Latency parsing — YCSB-cpp emits e.g. "Run latency p99 (ns): 12345"
# and YCSB-Java emits "[READ], 99thPercentileLatency(us), 1234".
_LAT_PATTERNS = {
    "p50": [
        re.compile(r"Run latency p50 \(ns\):\s*(\d+)"),
        re.compile(r"^\[(?:UPDATE|INSERT|READ|READ-MODIFY-WRITE|SCAN)\],\s+50thPercentileLatency\(us\),\s+(\d+)", re.MULTILINE),
    ],
    "p99": [
        re.compile(r"Run latency p99 \(ns\):\s*(\d+)"),
        re.compile(r"^\[(?:UPDATE|INSERT|READ|READ-MODIFY-WRITE|SCAN)\],\s+99thPercentileLatency\(us\),\s+(\d+)", re.MULTILINE),
    ],
    "p999": [
        re.compile(r"Run latency p999 \(ns\):\s*(\d+)"),
        re.compile(r"^\[(?:UPDATE|INSERT|READ|READ-MODIFY-WRITE|SCAN)\],\s+99\.9thPercentileLatency\(us\),\s+(\d+)", re.MULTILINE),
    ],
}


def parse_latencies(text: str) -> dict[str, Optional[int]]:
    """Best-effort latency parse. YCSB-cpp values are in ns; YCSB-Java in us.
    Caller can normalize later. Returns dict with 'p50','p99','p999' (ns OR us)."""
    out: dict[str, Optional[int]] = {}
    for k, pats in _LAT_PATTERNS.items():
        out[k] = None
        for p in pats:
            m = p.search(text)
            if m:
                out[k] = int(m.group(1))
                break
    return out


# -----------------------------------------------------------------------------
# One repeat: spawn N parallel workers
# -----------------------------------------------------------------------------


@dataclass
class WorkerOutcome:
    proc_id: int
    log_path: Path
    throughput: Optional[float]
    max_rss_kb: Optional[int]
    p50: Optional[int]
    p99: Optional[int]
    p999: Optional[int]
    returncode: int


@dataclass
class CellRepeat:
    ok: bool
    aggregate_throughput: Optional[float]
    per_proc_throughputs: list[Optional[float]]
    per_proc_logs: list[Path]
    sum_max_rss_kb: int
    p50_worst: Optional[int]
    p99_worst: Optional[int]
    p999_worst: Optional[int]
    elapsed_s: float
    error: str = ""


def run_one_repeat(spec: SystemSpec, workload: str, n_procs: int, repeat: int,
                   duration: int, record_count: int, results_dir: Path) -> CellRepeat:
    log_dir = results_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    work_paths: list[Path] = [spec.per_proc_work_path(n_procs, i) for i in range(n_procs)]

    # Pre-cell setup
    try:
        kill_stragglers()
        for wp in work_paths:
            spec.restore_into(wp)
        drop_caches()
    except Exception as e:
        return CellRepeat(ok=False, aggregate_throughput=None,
                          per_proc_throughputs=[], per_proc_logs=[],
                          sum_max_rss_kb=0,
                          p50_worst=None, p99_worst=None, p999_worst=None,
                          elapsed_s=0.0, error=f"setup failed: {e}")

    # Spawn N workers back-to-back
    workers: list[dict] = []
    log_paths: list[Path] = []
    t0 = time.time()
    try:
        for i, wp in enumerate(work_paths):
            cmd, env, cwd = build_proc_cmd(spec, workload, n_procs, repeat, i, wp,
                                           duration, record_count)
            timed = [TIME_BIN, "-v"] + cmd
            log_path = log_dir / (
                f"{spec.name}_workload{workload}_p{n_procs}_r{repeat}_proc{i}.log"
            )
            log_paths.append(log_path)
            log_f = log_path.open("w")
            log_f.write(f"# CMD: {shlex.join(timed)}\n")
            log_f.write(f"# work_path={wp}\n")
            log_f.flush()
            proc = subprocess.Popen(
                timed, stdout=log_f, stderr=subprocess.STDOUT,
                env=env, cwd=str(cwd) if cwd else None,
            )
            workers.append({"proc_id": i, "proc": proc, "log_f": log_f, "log_path": log_path})

        # Wait for all with a global cell deadline
        cell_deadline = t0 + duration + 600  # 10-min slack for setup/teardown inside YCSB
        for w in workers:
            remaining = cell_deadline - time.time()
            try:
                w["proc"].wait(timeout=max(remaining, 1))
            except subprocess.TimeoutExpired:
                w["proc"].kill()
                w["proc"].wait()
                w["log_f"].write("\n# CELL TIMEOUT — process killed\n")
            w["log_f"].close()
    finally:
        # Always clean up — even on early bail
        elapsed = time.time() - t0
        for wp in work_paths:
            spec.cleanup(wp)
        # Best-effort kill anything still lingering
        kill_stragglers()

    # Parse outcomes
    outcomes: list[WorkerOutcome] = []
    for w in workers:
        try:
            text = w["log_path"].read_text()
        except OSError:
            text = ""
        lat = parse_latencies(text)
        outcomes.append(WorkerOutcome(
            proc_id=w["proc_id"],
            log_path=w["log_path"],
            throughput=parse_throughput(text),
            max_rss_kb=parse_max_rss_kb(text),
            p50=lat["p50"], p99=lat["p99"], p999=lat["p999"],
            returncode=w["proc"].returncode,
        ))

    per_tputs = [o.throughput for o in outcomes]
    if all(t is not None for t in per_tputs) and outcomes and all(o.returncode == 0 for o in outcomes):
        agg = sum(t for t in per_tputs if t is not None)
        ok = True
        err = ""
    else:
        agg = None
        ok = False
        err = ("partial/total fail: " +
               ", ".join(f"proc{o.proc_id} rc={o.returncode} tput={o.throughput}"
                         for o in outcomes))

    sum_rss = sum(o.max_rss_kb for o in outcomes if o.max_rss_kb is not None)
    p50_worst = max((o.p50 for o in outcomes if o.p50 is not None), default=None)
    p99_worst = max((o.p99 for o in outcomes if o.p99 is not None), default=None)
    p999_worst = max((o.p999 for o in outcomes if o.p999 is not None), default=None)

    return CellRepeat(
        ok=ok,
        aggregate_throughput=agg,
        per_proc_throughputs=per_tputs,
        per_proc_logs=log_paths,
        sum_max_rss_kb=sum_rss,
        p50_worst=p50_worst, p99_worst=p99_worst, p999_worst=p999_worst,
        elapsed_s=elapsed,
        error=err,
    )


# -----------------------------------------------------------------------------
# Cell sweep
# -----------------------------------------------------------------------------


@dataclass
class CellSummary:
    system: str
    workload: str
    n_processes: int
    aggregate_throughputs: list[float] = field(default_factory=list)
    per_proc_throughputs_per_repeat: list[list[Optional[float]]] = field(default_factory=list)
    sum_rss_kb: list[int] = field(default_factory=list)
    p50_worst: list[int] = field(default_factory=list)
    p99_worst: list[int] = field(default_factory=list)
    p999_worst: list[int] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def relative_stddev_pct(self) -> Optional[float]:
        if len(self.aggregate_throughputs) < 2:
            return None
        m = statistics.mean(self.aggregate_throughputs)
        if m == 0:
            return None
        return 100.0 * statistics.stdev(self.aggregate_throughputs) / m


def append_skipped(skipped_md: Path, cell: CellSummary, why: str,
                   log_paths: list[Path]) -> None:
    line = (
        f"- `{cell.system}` workload {cell.workload}  N={cell.n_processes}: **{why}**  \n"
        f"  Logs: " + ", ".join(f"`{p.name}`" for p in log_paths) + "\n"
    )
    with skipped_md.open("a") as f:
        f.write(line)


def run_cell(spec: SystemSpec, workload: str, n_procs: int, repeats: int,
             duration: int, record_count: int, skipped_md: Path,
             variance_pct: float, results_dir: Path) -> CellSummary:
    summary = CellSummary(system=spec.name, workload=workload, n_processes=n_procs)
    all_log_paths: list[Path] = []
    permanent_fail = False

    print(f"\n=== {spec.name} workload-{workload}  N={n_procs} ===", flush=True)

    for repeat in range(1, repeats + 1):
        result = run_one_repeat(spec, workload, n_procs, repeat, duration,
                                record_count, results_dir)
        all_log_paths.extend(result.per_proc_logs)

        if not result.ok:
            print(f"  repeat {repeat}: FAIL ({result.error}). retrying once.", flush=True)
            retry = run_one_repeat(spec, workload, n_procs, repeat, duration,
                                   record_count, results_dir)
            all_log_paths.extend(retry.per_proc_logs)
            if not retry.ok:
                permanent_fail = True
                summary.failures.append(retry.error or result.error)
                # Rename the retry's per-proc logs to FAILED
                for lp in retry.per_proc_logs:
                    failed = lp.with_name(lp.name.replace(".log", "_FAILED.log"))
                    try:
                        lp.rename(failed)
                    except OSError:
                        pass
                break
            result = retry

        summary.aggregate_throughputs.append(result.aggregate_throughput)
        summary.per_proc_throughputs_per_repeat.append(result.per_proc_throughputs)
        summary.sum_rss_kb.append(result.sum_max_rss_kb)
        if result.p50_worst is not None:
            summary.p50_worst.append(result.p50_worst)
        if result.p99_worst is not None:
            summary.p99_worst.append(result.p99_worst)
        if result.p999_worst is not None:
            summary.p999_worst.append(result.p999_worst)

        per_proc_str = ",".join(f"{t:.0f}" if t else "?" for t in result.per_proc_throughputs)
        print(
            f"  repeat {repeat}: agg={result.aggregate_throughput:.0f} ops/s  "
            f"per-proc=[{per_proc_str}]  sum_rss={result.sum_max_rss_kb} kB  "
            f"({result.elapsed_s:.0f}s)",
            flush=True,
        )

    if permanent_fail:
        append_skipped(
            skipped_md, summary,
            f"FAILED after retry: {summary.failures[-1] if summary.failures else 'unknown'}",
            all_log_paths,
        )
    elif (var := summary.relative_stddev_pct()) is not None and var > variance_pct:
        print(f"  variance {var:.1f}% > {variance_pct}%, re-running once.", flush=True)
        rerun = run_one_repeat(spec, workload, n_procs, repeats + 1, duration,
                               record_count, results_dir)
        all_log_paths.extend(rerun.per_proc_logs)
        if rerun.ok:
            summary.aggregate_throughputs.append(rerun.aggregate_throughput)
            summary.per_proc_throughputs_per_repeat.append(rerun.per_proc_throughputs)
            summary.sum_rss_kb.append(rerun.sum_max_rss_kb)
            if rerun.p50_worst is not None:
                summary.p50_worst.append(rerun.p50_worst)
            if rerun.p99_worst is not None:
                summary.p99_worst.append(rerun.p99_worst)
            if rerun.p999_worst is not None:
                summary.p999_worst.append(rerun.p999_worst)

    return summary


def write_results_csv(path: Path, summaries: list[CellSummary]) -> None:
    with path.open("w") as f:
        f.write(
            "system,workload,n_processes,n_repeats,"
            "agg_throughput_mean,agg_throughput_stddev,rel_stddev_pct,"
            "sum_max_rss_kb_mean,p50_worst_mean,p99_worst_mean,p999_worst_mean,"
            "per_proc_throughputs,failures\n"
        )
        for s in summaries:
            n = len(s.aggregate_throughputs)
            tput_mean = statistics.mean(s.aggregate_throughputs) if n else ""
            tput_sd = statistics.stdev(s.aggregate_throughputs) if n >= 2 else ""
            rel = s.relative_stddev_pct() or ""
            rss = statistics.mean(s.sum_rss_kb) if s.sum_rss_kb else ""
            p50 = statistics.mean(s.p50_worst) if s.p50_worst else ""
            p99 = statistics.mean(s.p99_worst) if s.p99_worst else ""
            p999 = statistics.mean(s.p999_worst) if s.p999_worst else ""
            # Encode per-proc throughputs as a |-separated list of comma lists
            ppt = "|".join(
                ",".join(f"{x:.2f}" if x is not None else "nan" for x in row)
                for row in s.per_proc_throughputs_per_repeat
            )
            fails = ";".join(s.failures) if s.failures else ""
            f.write(f"{s.system},{s.workload},{s.n_processes},{n},{tput_mean},"
                    f"{tput_sd},{rel},{rss},{p50},{p99},{p999},{ppt},{fails}\n")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--systems", default="ozonedb,trunkcpp,bcw2",
                   help="Comma-separated; only ozonedb/trunkcpp/bcw2 supported in Phase 1")
    p.add_argument("--workloads", default=",".join(DEFAULT_WORKLOADS))
    p.add_argument("--processes", default=",".join(map(str, DEFAULT_PROCESSES)),
                   help="Comma-separated process counts to sweep over")
    p.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    p.add_argument("--duration", type=int, default=DEFAULT_RUN_DURATION)
    p.add_argument("--record-count", type=int, default=DEFAULT_RECORD_COUNT)
    p.add_argument("--results-dir", type=Path, required=True,
                   help="Phase output dir (must contain or accept logs/, results.csv, ...)")
    p.add_argument("--variance-pct", type=float, default=DEFAULT_VARIANCE_PCT)
    args = p.parse_args()

    systems_arg = [s.strip() for s in args.systems.split(",") if s.strip()]
    specs_all = system_specs()
    unknown = [s for s in systems_arg if s not in specs_all]
    if unknown:
        print(f"Unsupported system(s) for Phase 1: {unknown}", file=sys.stderr)
        return 2

    specs = [specs_all[s] for s in systems_arg]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    proc_counts = [int(x) for x in args.processes.split(",") if x.strip()]
    repeats = args.repeats
    duration = args.duration
    record_count = args.record_count
    results_dir = args.results_dir.resolve()

    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "logs").mkdir(exist_ok=True)
    skipped_md = results_dir / "skipped_runs.md"
    if not skipped_md.exists():
        skipped_md.write_text("# Skipped / failed cells\n\n")
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)

    print(f"Phase 1 sweep: systems={[s.name for s in specs]}  workloads={workloads}  "
          f"N={proc_counts}  repeats={repeats}  duration={duration}s  "
          f"recordcount={record_count}  results_dir={results_dir}", flush=True)

    pre_generate_all_workloads(workloads, record_count)

    # If ozonedb is in the sweep, resolve its java classpath ONCE up front
    # so workers can bypass bin/ycsb (which would race in parallel `mvn package`).
    global OZONEDB_JAVA_CMD_PREFIX
    if any(s.name == "ozonedb" for s in specs):
        OZONEDB_JAVA_CMD_PREFIX = warmup_ozonedb_classpath(record_count)

    summaries: list[CellSummary] = []
    t_start = time.time()
    for spec in specs:
        for workload in workloads:
            for n_procs in proc_counts:
                summary = run_cell(spec, workload, n_procs, repeats, duration,
                                   record_count, skipped_md, args.variance_pct,
                                   results_dir)
                summaries.append(summary)
                # Write CSV after every cell so progress is durable
                write_results_csv(results_dir / "results.csv", summaries)
    elapsed_total = time.time() - t_start
    print(f"\nTotal sweep wallclock: {elapsed_total / 60:.1f} min", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
