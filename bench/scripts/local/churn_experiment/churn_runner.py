#!/usr/bin/env python3
"""Churn-tolerance experiment v4 — fixed-backlog completion-time.

The setup creates a known, uniform compaction backlog by loading 200k
records with the compaction watcher DISABLED (compaction_policy=1, which
makes db.cpp:107-110 skip startCompactionWatcher). After load, the DB has
~250 log files in datalog/ and zero entries in any sstable level. metadata
log records LOGCREATE but no COMPACT. task.log is empty.

The same loaded DB is snapshot once and copied for every trial so the
starting state is byte-identical across (N, R) combinations.

Each trial then:
  1. cp the snapshot to a per-trial DB dir
  2. spawn N writers with compaction enabled (compaction_policy=0)
     and OZONEDB_CHURN_ABORT_RATE=R, running YCSB workload-c (read-only,
     no new compactions added)
  3. poll task.log every 1 s; declare quiescent when:
        * every task has at least one COMPLETE record
        * AND n_tasks_total has not changed for QUIESCENCE_GRACE_S seconds
        * AND no log-level files remain that would trigger more work
  4. SIGTERM writers, single-process integrity check (sequential
     read of all 200k keys)
  5. tear down

Two `vary` modes:
  --vary writers   varies N at fixed R (default 0%)
  --vary abort     varies R at fixed N (default 4)
  --vary both      runs both back-to-back
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Optional

sys.path.insert(0, str(Path("/users/Xinying/ozonedb/bench/results/local/"
                            "multi-process-and-extended/scripts")))
import run_phase1_multiproc_sweep as p1  # type: ignore

sys.path.insert(0, str(Path(__file__).parent))
import task_log_parser  # type: ignore


EXPERIMENT_DIR = Path(__file__).parent
RESULTS_ROOT = EXPERIMENT_DIR / "results"

OZONEDB_HOME = Path("/users/Xinying/ozonedb")
OZONEDB_YCSB = OZONEDB_HOME / "ycsb"
OZONEDB_BIN = OZONEDB_YCSB / "bin" / "ycsb"

DB_ROOT = Path("/tank/test/churn_v4")
SNAPSHOT_DIR = DB_ROOT / "snapshot_loaded_no_compaction"
CONFIG_TMP_ROOT = Path("/tmp/churn_v4_configs")

DEFAULT_RECORD_COUNT = 50_000        # smaller than v3 — keeps trial wallclock tractable
DEFAULT_KEY_SIZE = "1KB"
DEFAULT_COMPLETION_TIMEOUT_S = 600        # 10 min cap per completion phase
DEFAULT_POLL_INTERVAL_S = 1.0
QUIESCENCE_GRACE_S = 8               # n_tasks_total must be flat this long
LOAD_TIMEOUT_S = 600
VERIFY_TIMEOUT_S = 600

CHURN_BASE_CONFIG = EXPERIMENT_DIR / "churn_config.json"


# Vary configs (each row of the CSV says which parameter the trial varies)
DEFAULT_WRITERS_VALUES = [1, 2, 4, 8]   # for `--vary writers`
DEFAULT_ABORT_VALUES = [0.0, 0.10, 0.20, 0.30, 0.50]
DEFAULT_FIXED_R = 0.0   # held constant when varying writers
DEFAULT_FIXED_N = 4     # held constant when varying abort rate


def now_ns() -> int:
    return time.monotonic_ns()


def wipe(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)


def make_shared_config(db_path: Path, dest: Path,
                       compaction_policy: int = 0) -> Path:
    cfg = json.loads(CHURN_BASE_CONFIG.read_text())
    cfg["db_path"] = str(db_path) + "/"
    cfg["compaction_policy"] = compaction_policy
    dest.write_text(json.dumps(cfg, indent=4))
    return dest


def ensure_workload(name: str, key_size: str, record_count: int,
                    operation_count: int) -> Path:
    wl_dir = OZONEDB_YCSB / "workloads" / "generated_workloads"
    wl_dir.mkdir(parents=True, exist_ok=True)
    wl_path = wl_dir / f"workload{name}_{key_size}_{operation_count}_{record_count}"
    if not wl_path.exists():
        subprocess.run(
            ["python3",
             str(OZONEDB_HOME / "bench" / "scripts" / "generate_workload.py"),
             "--workload_name", name,
             "--key_size", key_size,
             "--operation_cnt", str(operation_count),
             "--record_cnt", str(record_count)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    return wl_path


def kill_stragglers() -> None:
    p1.kill_stragglers()


# -----------------------------------------------------------------------------
# Snapshot setup (one-time): load 200k records with compaction disabled
# -----------------------------------------------------------------------------


def build_snapshot(record_count: int, key_size: str,
                   force_rebuild: bool = False) -> Path:
    """Run YCSB load with compaction_policy=1 (no watcher started). Resulting
    DB dir is snapshotted at SNAPSHOT_DIR for later per-trial cp."""
    if SNAPSHOT_DIR.exists() and not force_rebuild:
        marker = SNAPSHOT_DIR / ".snapshot_meta.json"
        if marker.exists():
            try:
                meta = json.loads(marker.read_text())
                if meta.get("record_count") == record_count and meta.get("key_size") == key_size:
                    print(f"[snapshot] reusing existing {SNAPSHOT_DIR}", flush=True)
                    return SNAPSHOT_DIR
            except Exception:
                pass

    wipe(SNAPSHOT_DIR)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    cfg_path = CONFIG_TMP_ROOT / "snapshot_load.json"
    make_shared_config(SNAPSHOT_DIR, cfg_path, compaction_policy=1)

    wl = ensure_workload("a", key_size, record_count, record_count)
    env = os.environ.copy()
    env["OZONEDB_HOME"] = str(OZONEDB_HOME)
    env.pop("OZONEDB_CHURN_ABORT_RATE", None)
    cmd = [str(OZONEDB_BIN), "load", "ozonedb",
           "-threads", "1", "-s",
           "-P", str(wl),
           "-p", "status.interval=10",
           "-p", f"shared_config={cfg_path}"]
    log_path = SNAPSHOT_DIR / "load.log"
    print(f"[snapshot] loading {record_count} records into {SNAPSHOT_DIR} "
          f"(compaction disabled)...", flush=True)
    with log_path.open("w") as lf:
        lf.write(f"# CMD: {shlex.join(cmd)}\n")
        lf.flush()
        proc = subprocess.run(cmd, env=env, cwd=str(OZONEDB_YCSB),
                              stdout=lf, stderr=subprocess.STDOUT,
                              timeout=LOAD_TIMEOUT_S)
    if proc.returncode != 0:
        raise RuntimeError(f"snapshot load failed (rc={proc.returncode})")

    text = log_path.read_text()
    m = re.search(r"^\[OVERALL\],\s+Throughput\(ops/sec\),\s+([\d.eE+-]+)",
                  text, re.MULTILINE)
    tput = float(m.group(1)) if m else None

    # Inventory final state
    datalog_files = sorted((SNAPSHOT_DIR / "datalog").glob("*")) if (SNAPSHOT_DIR / "datalog").exists() else []
    sstable_dirs = [(SNAPSHOT_DIR / d).name for d in os.listdir(SNAPSHOT_DIR)
                    if d.startswith("sstable")]
    task_log = SNAPSHOT_DIR / "task.log"
    n_log_files = len(datalog_files)
    task_log_size = task_log.stat().st_size if task_log.exists() else 0
    print(f"[snapshot] done: load_throughput={tput:.0f} ops/s, "
          f"n_log_files={n_log_files}, "
          f"sstable_dirs={sstable_dirs}, task_log_bytes={task_log_size}",
          flush=True)

    (SNAPSHOT_DIR / ".snapshot_meta.json").write_text(json.dumps({
        "record_count": record_count, "key_size": key_size,
        "load_throughput_ops": tput, "n_log_files": n_log_files,
        "sstable_dirs": sstable_dirs, "task_log_bytes": task_log_size,
    }, indent=2))
    return SNAPSHOT_DIR


# -----------------------------------------------------------------------------
# Completion phase
# -----------------------------------------------------------------------------


def quiescent_state(task_log_path: Path) -> tuple[int, int]:
    """Returns (n_total, n_pending) — pending = task without any COMPLETE."""
    if not task_log_path.exists():
        return 0, 0
    records = task_log_parser.parse_task_log(task_log_path)
    if not records:
        return 0, 0
    grouped = task_log_parser.group_by_task(records)
    n_total = len(grouped)
    n_completed = sum(1 for k, recs in grouped.items()
                      if any(r["status"] == "COMPLETE" for r in recs))
    return n_total, n_total - n_completed


def build_writer_cmd(workload_path: Path, cfg_path: Path,
                           completion_timeout_s: int) -> list[str]:
    if p1.OZONEDB_JAVA_CMD_PREFIX is None:
        raise RuntimeError("OzoneDB classpath not warmed up")
    return list(p1.OZONEDB_JAVA_CMD_PREFIX) + [
        "-threads", "1",
        "-s",
        "-P", str(workload_path),
        "-p", "status.interval=30",
        "-p", "requestdistribution=uniform",
        "-p", "operationcount=1000000000",
        "-p", f"maxexecutiontime={completion_timeout_s + 60}",
        "-p", f"shared_config={cfg_path}",
        "-t",
    ]


def run_completion(db_path: Path, cfg_path: Path, record_count: int,
              key_size: str, n_writers: int, abort_rate: float,
              completion_timeout_s: int, poll_interval_s: float,
              quiescence_grace_s: int, trial_dir: Path
              ) -> tuple[bool, float, int, int]:
    """Spawn n_writers writers (workload-c) with the given abort_rate.
    Poll task.log until: every task has a COMPLETE record AND n_total has
    been stable for quiescence_grace_s seconds. Returns
    (completed, elapsed_s, n_total, n_pending)."""
    wl = ensure_workload("c", key_size, record_count, record_count)
    cmd = build_writer_cmd(wl, cfg_path, completion_timeout_s)

    env = os.environ.copy()
    env["OZONEDB_HOME"] = str(OZONEDB_HOME)
    env["OZONEDB_CHURN_ABORT_RATE"] = str(abort_rate)
    # PICK_TRACE: forward to the child if set in the parent (for diagnostics).
    if "OZONEDB_PICK_TRACE" in os.environ:
        env["OZONEDB_PICK_TRACE"] = os.environ["OZONEDB_PICK_TRACE"]

    procs: list[subprocess.Popen] = []
    log_files: list[IO] = []
    log_paths: list[Path] = []
    t0 = now_ns()
    for i in range(n_writers):
        log_path = trial_dir / f"writer_{i}.log"
        log_paths.append(log_path)
        lf = log_path.open("w")
        lf.write(f"# CMD: {shlex.join(cmd)}\n"
                 f"# env OZONEDB_CHURN_ABORT_RATE={abort_rate}\n")
        lf.flush()
        log_files.append(lf)
        proc = subprocess.Popen(cmd, env=env, cwd=str(OZONEDB_YCSB),
                                stdout=lf, stderr=subprocess.STDOUT, bufsize=0)
        procs.append(proc)

    completed = False
    n_total = 0
    n_pending = -1
    task_log_path = db_path / "task.log"
    deadline_ns = t0 + completion_timeout_s * 1_000_000_000

    last_n_total = -1
    last_change_ns = now_ns()
    last_progress_log = 0.0
    while now_ns() < deadline_ns:
        if all(p.poll() is not None for p in procs):
            print(f"    run: ALL writers exited; bailing", flush=True)
            break
        n_total, n_pending = quiescent_state(task_log_path)
        elapsed = (now_ns() - t0) / 1e9
        if n_total != last_n_total:
            last_n_total = n_total
            last_change_ns = now_ns()
        time_since_change = (now_ns() - last_change_ns) / 1e9

        if elapsed - last_progress_log > 5.0:
            print(f"    completion @ {elapsed:6.1f}s: n_total={n_total} pending={n_pending} "
                  f"stable_for={time_since_change:.1f}s", flush=True)
            last_progress_log = elapsed

        # Quiescent: pending=0 AND n_total stable for grace seconds
        if n_pending == 0 and n_total > 0 and time_since_change >= quiescence_grace_s:
            completed = True
            break
        time.sleep(poll_interval_s)
    elapsed_s = (now_ns() - t0) / 1e9

    for proc in procs:
        if proc.poll() is None:
            try:
                proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
    for proc in procs:
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
    for lf in log_files:
        with contextlib.suppress(Exception):
            lf.flush(); lf.close()

    n_total, n_pending = quiescent_state(task_log_path)
    return completed, elapsed_s, n_total, n_pending


def run_verify(db_path: Path, record_count: int, key_size: str,
               cfg_path: Path, log_path: Path
               ) -> tuple[Optional[bool], Optional[int], Optional[int]]:
    wl = ensure_workload("c", key_size, record_count, record_count)
    env = os.environ.copy()
    env["OZONEDB_HOME"] = str(OZONEDB_HOME)
    env.pop("OZONEDB_CHURN_ABORT_RATE", None)
    cmd = [str(OZONEDB_BIN), "run", "ozonedb",
           "-threads", "1", "-s",
           "-P", str(wl),
           "-p", "status.interval=10",
           "-p", "requestdistribution=sequential",
           "-p", f"operationcount={record_count}",
           "-p", "maxexecutiontime=0",
           "-p", f"shared_config={cfg_path}"]
    with log_path.open("w") as lf:
        lf.write(f"# CMD: {shlex.join(cmd)}\n")
        lf.flush()
        try:
            proc = subprocess.run(cmd, env=env, cwd=str(OZONEDB_YCSB),
                                  stdout=lf, stderr=subprocess.STDOUT,
                                  timeout=VERIFY_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            return None, None, None
    text = log_path.read_text()
    ok_m = re.search(r"^\[READ\],\s+Return=OK,\s+(\d+)", text, re.MULTILINE)
    nf_m = re.search(r"^\[READ\],\s+Return=NOT_FOUND,\s+(\d+)", text, re.MULTILINE)
    err_m = re.search(r"^\[READ\],\s+Return=ERROR,\s+(\d+)", text, re.MULTILINE)
    ok_n = int(ok_m.group(1)) if ok_m else 0
    nf_n = int(nf_m.group(1)) if nf_m else 0
    err_n = int(err_m.group(1)) if err_m else 0
    if proc.returncode != 0 and ok_n + nf_n + err_n == 0:
        return None, None, None
    return ok_n == record_count, ok_n, record_count


# -----------------------------------------------------------------------------
# Trial structure
# -----------------------------------------------------------------------------


@dataclass
class TrialResult:
    trial_id: str
    vary: str                # "writers" or "abort"
    n_writers: int
    abort_rate: float
    record_count: int
    setup_ok: bool = False
    completion_time_s: Optional[float] = None
    completed: Optional[bool] = None
    n_tasks_total: Optional[int] = None
    n_pending_after_completion: Optional[int] = None
    n_tasks_completed: Optional[int] = None
    n_tasks_with_reassignment: Optional[int] = None
    max_generation_observed: Optional[int] = None
    generation_distribution: dict = field(default_factory=dict)
    integrity_pass: Optional[bool] = None
    integrity_keys_ok: Optional[int] = None
    integrity_keys_total: Optional[int] = None
    notes: list[str] = field(default_factory=list)


def execute_trial(trial_id: str, vary: str, n_writers: int,
                  abort_rate: float, record_count: int, key_size: str,
                  completion_timeout_s: int, results_dir: Path,
                  quiescence_grace_s: int = QUIESCENCE_GRACE_S) -> TrialResult:
    trial_dir = results_dir / "per_trial" / trial_id
    trial_dir.mkdir(parents=True, exist_ok=True)

    res = TrialResult(
        trial_id=trial_id, vary=vary,
        n_writers=n_writers, abort_rate=abort_rate,
        record_count=record_count,
    )

    db_path = DB_ROOT / f"db_{trial_id}"
    wipe(db_path)

    # cp snapshot -> db_path
    print(f"  [{trial_id}] copy snapshot...", flush=True)
    subprocess.run(["cp", "-r", "--reflink=auto", str(SNAPSHOT_DIR), str(db_path)],
                   check=True)
    # Remove any stale .snapshot_meta.json from the working copy
    (db_path / ".snapshot_meta.json").unlink(missing_ok=True)
    (db_path / "load.log").unlink(missing_ok=True)
    res.setup_ok = True

    cfg_path = CONFIG_TMP_ROOT / f"shared_config_{trial_id}.json"
    make_shared_config(db_path, cfg_path, compaction_policy=0)  # enable compaction

    print(f"  [{trial_id}] completion phase ({n_writers}w @ abort={abort_rate})...",
          flush=True)
    completed, completion_elapsed, n_total, n_pending = run_completion(
        db_path, cfg_path, record_count, key_size,
        n_writers, abort_rate, completion_timeout_s,
        DEFAULT_POLL_INTERVAL_S, quiescence_grace_s, trial_dir,
    )
    res.completed = completed
    res.completion_time_s = completion_elapsed
    res.n_pending_after_completion = n_pending
    print(f"    completed={completed} in {completion_elapsed:.1f}s "
          f"n_total={n_total} pending={n_pending}", flush=True)
    if not completed:
        res.notes.append(f"completion timed out / writers crashed; "
                         f"{n_pending} pending of {n_total} total")

    # Parse final task.log for richer stats
    task_log_path = db_path / "task.log"
    if task_log_path.exists():
        records = task_log_parser.parse_task_log(task_log_path)
        grouped = task_log_parser.group_by_task(records)
        summaries = [task_log_parser.task_summary(grouped[k]) for k in grouped]
        (trial_dir / "task_log_timeline.json").write_text(
            json.dumps({"records": records, "tasks": summaries}, indent=2)
        )
        res.n_tasks_total = len(grouped)
        res.n_tasks_completed = sum(1 for s in summaries if s["ever_completed"])
        res.n_tasks_with_reassignment = sum(1 for s in summaries
                                             if s["max_generation"] > 0)
        max_gens = [s["max_generation"] for s in summaries]
        res.max_generation_observed = max(max_gens) if max_gens else 0
        res.generation_distribution = dict(sorted(Counter(max_gens).items()))

    # Integrity check
    print(f"  [{trial_id}] integrity check...", flush=True)
    try:
        ipass, ok, total = run_verify(db_path, record_count, key_size,
                                      cfg_path, trial_dir / "verify.log")
        res.integrity_pass = ipass
        res.integrity_keys_ok = ok
        res.integrity_keys_total = total
    except Exception as e:
        res.notes.append(f"verify raised: {e}")

    wipe(db_path)
    cfg_path.unlink(missing_ok=True)
    print(f"  [{trial_id}] DONE  completion={res.completion_time_s:.1f}s "
          f"completed={res.completed} integrity={res.integrity_pass} "
          f"max_gen={res.max_generation_observed} "
          f"tasks={res.n_tasks_total}", flush=True)
    return res


# -----------------------------------------------------------------------------
# CSV
# -----------------------------------------------------------------------------


_CSV_FIELDS = [
    "trial_id", "vary", "n_writers", "abort_rate", "record_count",
    "setup_ok",
    "completion_time_s", "completed", "n_pending_after_completion",
    "n_tasks_total", "n_tasks_completed", "n_tasks_with_reassignment",
    "max_generation_observed", "generation_distribution",
    "integrity_pass", "integrity_keys_ok", "integrity_keys_total",
    "notes",
]


def append_csv_row(csv_path: Path, res: TrialResult) -> None:
    write_header = not csv_path.exists()
    with csv_path.open("a") as f:
        if write_header:
            f.write(",".join(_CSV_FIELDS) + "\n")
        row = []
        for fn in _CSV_FIELDS:
            v = getattr(res, fn)
            if isinstance(v, list):
                v = ";".join(str(x).replace(",", "_") for x in v)
            elif isinstance(v, dict):
                v = ";".join(f"{k}:{val}" for k, val in v.items())
            elif isinstance(v, bool) or v is None:
                v = "" if v is None else str(v).lower()
            else:
                v = str(v).replace(",", "_")
            row.append(v)
        f.write(",".join(row) + "\n")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--record-count", type=int, default=DEFAULT_RECORD_COUNT)
    p.add_argument("--key-size", type=str, default=DEFAULT_KEY_SIZE)
    p.add_argument("--completion-timeout", type=int, default=DEFAULT_COMPLETION_TIMEOUT_S)
    p.add_argument("--results-dir", type=Path, default=None)
    p.add_argument("--vary", choices=("writers", "abort", "both"),
                   default="both", help="Which parameter to vary")
    p.add_argument("--writers", type=str,
                   default=",".join(str(x) for x in DEFAULT_WRITERS_VALUES),
                   help="Comma-separated N values when --vary=writers")
    p.add_argument("--abort-rates", type=str,
                   default=",".join(str(x) for x in DEFAULT_ABORT_VALUES),
                   help="Comma-separated R values when --vary=abort")
    p.add_argument("--fixed-r", type=float, default=DEFAULT_FIXED_R,
                   help="R held constant when --vary=writers")
    p.add_argument("--fixed-n", type=int, default=DEFAULT_FIXED_N,
                   help="N held constant when --vary=abort")
    p.add_argument("--rebuild-snapshot", action="store_true",
                   help="Force re-running the priming load")
    p.add_argument("--repeats", type=int, default=1,
                   help="Number of repetitions per (vary, N, R) trial")
    p.add_argument("--quiescence-grace", type=int, default=QUIESCENCE_GRACE_S,
                   help="Seconds n_total must be flat before declaring completion")
    args = p.parse_args()

    timestamp = time.strftime("%Y%m%dT%H%M%S")
    results_dir = args.results_dir or RESULTS_ROOT / f"v4_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "per_trial").mkdir(exist_ok=True)
    DB_ROOT.mkdir(parents=True, exist_ok=True)
    CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)

    cfg_dump = {
        "record_count": args.record_count, "key_size": args.key_size,
        "completion_timeout_s": args.completion_timeout, "vary": args.vary,
        "writers_values": args.writers, "abort_values": args.abort_rates,
        "fixed_r": args.fixed_r, "fixed_n": args.fixed_n,
        "repeats": args.repeats, "quiescence_grace_s": args.quiescence_grace,
    }
    (results_dir / "run_config.json").write_text(json.dumps(cfg_dump, indent=2))
    print(f"Churn v4 results -> {results_dir}", flush=True)
    print(f"  config: {cfg_dump}", flush=True)

    kill_stragglers()
    for w in ("a", "c"):
        ensure_workload(w, args.key_size, args.record_count, args.record_count)
    print("Warming up OzoneDB classpath...", flush=True)
    p1.OZONEDB_JAVA_CMD_PREFIX = p1.warmup_ozonedb_classpath(args.record_count)

    # One-time canonical load with compaction disabled
    build_snapshot(args.record_count, args.key_size,
                   force_rebuild=args.rebuild_snapshot)

    csv_path = results_dir / "trials.csv"

    trials: list[tuple[str, str, int, float]] = []
    if args.vary in ("writers", "both"):
        ns = [int(x) for x in args.writers.split(",") if x.strip()]
        for n in ns:
            trial_id = f"varyW_n{n}_r{int(args.fixed_r * 100):d}pct"
            trials.append((trial_id, "writers", n, args.fixed_r))
    if args.vary in ("abort", "both"):
        rs = [float(x) for x in args.abort_rates.split(",") if x.strip()]
        for r in rs:
            trial_id = f"varyR_n{args.fixed_n}_r{int(r * 100):d}pct"
            trials.append((trial_id, "abort", args.fixed_n, r))

    # Expand each (trial_id, vary, n, r) base into args.repeats individual trial
    # ids (suffixed with _rep<i>). Each rep gets its own DB dir + per_trial dir.
    expanded: list[tuple[str, str, int, float]] = []
    for trial_id, vary, n, r in trials:
        if args.repeats <= 1:
            expanded.append((trial_id, vary, n, r))
        else:
            for i in range(1, args.repeats + 1):
                expanded.append((f"{trial_id}_rep{i}", vary, n, r))
    trials = expanded

    print(f"\n[plan] running {len(trials)} trials (repeats={args.repeats}, grace={args.quiescence_grace}s):", flush=True)
    for trial_id, vary, n, r in trials:
        print(f"  {trial_id}  vary={vary} N={n} R={r}", flush=True)

    for idx, (trial_id, vary, n, r) in enumerate(trials, 1):
        print(f"\n[{idx}/{len(trials)}] trial={trial_id}", flush=True)
        res = execute_trial(
            trial_id=trial_id, vary=vary,
            n_writers=n, abort_rate=r,
            record_count=args.record_count, key_size=args.key_size,
            completion_timeout_s=args.completion_timeout,
            results_dir=results_dir,
            quiescence_grace_s=args.quiescence_grace,
        )
        append_csv_row(csv_path, res)

    print(f"\nResults: {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
