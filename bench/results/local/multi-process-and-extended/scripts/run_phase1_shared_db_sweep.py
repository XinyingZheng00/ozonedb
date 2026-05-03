#!/usr/bin/env python3
"""Phase 1 — SHARED-DB multi-process writer-scaling sweep.

N independent OS processes all open the SAME on-disk database simultaneously
and exercise it concurrently. This is the actual test of the systems'
multi-writer coordination protocols (vs the independent-DB variant in
run_phase1_multiproc_sweep.py, which is shared-nothing).

Design (per-cell):
  1. Restore cached_DB into ONE work_path (single copy, not N).
  2. drop_caches.
  3. Spawn N YCSB workers (each threadcount=1) all pointing at the same DB.
     - OzoneDB: each worker uses an identical shared_config pointing at the
       single work_path. Coordination is via OzoneDB's append-based task log;
       each process auto-generates a fingerprint internally — no per-process
       config differences needed.
     - SQLite (trunkcpp/bcw2): each worker uses sqlite.dbpath=<single path>
       with locking_mode=NORMAL (overrides the EXCLUSIVE setting in the
       baseline properties files; EXCLUSIVE is incompatible with cross-process
       sharing because the first connection grabs the file lock and starves
       all others). All other knobs unchanged.
  4. Wait for all workers; sum per-process throughputs as headline aggregate.
  5. Tear down the single work_path.

Run protocol (per user request 2026-04-29):
  * 1 repeat per cell
  * 90 s duration

Logs: phase1-multi-process/logs/{system}_workload{w}_p{N}_proc{i}.log
Aggregate CSV: phase1-multi-process/results.csv
Failures: phase1-multi-process/skipped_runs.md

Reuses warmup_ozonedb_classpath() from run_phase1_multiproc_sweep.py to
bypass the parallel-mvn race.

Usage:
  python3 run_phase1_shared_db_sweep.py \\
      --systems ozonedb,trunkcpp,bcw2 \\
      --workloads a,b,c,d,f \\
      --processes 2,4,8,16 \\
      --repeats 1 \\
      --duration 90 \\
      --results-dir /users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase1-multi-process
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

# Reuse Phase-1 helpers (system specs, classpath warmup, parsers, etc.).
sys.path.insert(0, str(Path(__file__).parent))
import run_phase1_multiproc_sweep as p1  # type: ignore

# Single shared work paths per system (one DB per cell, not N).
SHARED_WORK = {
    "ozonedb":  p1.WORK_ROOT / "shared_ozonedb",          # dir
    "trunkcpp": p1.WORK_ROOT / "shared_trunkcpp_ycsb.db", # file + -wal/-shm
    "bcw2":     p1.WORK_ROOT / "shared_bcw_ycsb.db",      # file + -wal/-wal2/-shm
}

DEFAULT_RECORD_COUNT = 1_000_000
DEFAULT_WORKLOADS = ["a", "b", "c", "d", "f"]
DEFAULT_PROCESSES = [2, 4, 8, 16]
DEFAULT_DURATION = 90
DEFAULT_REPEATS = 1


def restore_shared_for(spec_name: str) -> Path:
    """Copy cached_DB to SHARED_WORK[spec_name] (single copy). Returns path."""
    spec = p1.system_specs()[spec_name]
    work = SHARED_WORK[spec_name]
    spec.restore_into(work)
    return work


def cleanup_shared_for(spec_name: str) -> None:
    spec = p1.system_specs()[spec_name]
    spec.cleanup(SHARED_WORK[spec_name])


def build_shared_proc_cmd(spec_name: str, workload: str, n_procs: int,
                          repeat: int, proc_id: int, work_path: Path,
                          duration: int, record_count: int
                          ) -> tuple[list[str], dict[str, str], Optional[Path]]:
    env = os.environ.copy()
    cwd: Optional[Path] = None

    if spec_name in ("trunkcpp", "bcw2"):
        prop = {"trunkcpp": "sqlite-trunk.properties",
                "bcw2": "sqlite-bcw.properties"}[spec_name]
        cmd = [
            str(p1.YCSB_CPP_BIN),
            "-db", "sqlite",
            "-run",
            "-P", str(p1.workload_file(workload)),
            "-P", str(p1.YCSB_CPP_DIR / "sqlite" / prop),
            "-p", f"sqlite.dbpath={work_path}",
            "-p", "threadcount=1",
            "-p", f"recordcount={record_count}",
            "-p", f"maxexecutiontime={duration}",
            # Two overrides for cross-process shared-DB compatibility:
            #
            # (1) sqlite.locking_mode=NORMAL — EXCLUSIVE (in baseline properties)
            #     would have the first connection grab the file lock and starve
            #     all others.
            # (2) sqlite.vfs=unix — YCSB-cpp's SqliteDB::OpenDB() defaults to
            #     "unix-excl" VFS which holds an OS-level exclusive lock on the
            #     DB file for the lifetime of the connection (works for
            #     multi-thread within one process; breaks multi-process). The
            #     default "unix" VFS uses POSIX advisory locks and supports
            #     concurrent multi-process access. Configurable via the
            #     `sqlite.vfs` property added to the binding for this sweep.
            "-p", "sqlite.locking_mode=NORMAL",
            "-p", "sqlite.vfs=unix",
        ]
        return cmd, env, cwd

    if spec_name == "ozonedb":
        if p1.OZONEDB_JAVA_CMD_PREFIX is None:
            raise RuntimeError("OzoneDB classpath not resolved (run warmup first)")
        env["OZONEDB_HOME"] = str(p1.OZONEDB_HOME)
        wl_path = p1.ensure_ozonedb_workload(workload, record_count)

        # All N processes use a config pointing at the SAME db_path. Per-process
        # config files have identical content (we keep them per-process for log
        # naming clarity, not because OzoneDB needs different configs).
        p1.CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        cfg_in = p1.OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json"
        with open(cfg_in) as f:
            cfg = json.load(f)
        cfg["db_path"] = str(work_path) + "/"
        # Ensure mode=1 (MultipleProcesses); the base config already has this
        # but we set explicitly to be safe.
        cfg["mode"] = 1
        cfg_out = p1.CONFIG_TMP_ROOT / (
            f"shared_config_SHARED_workload{workload}_p{n_procs}_r{repeat}_proc{proc_id}.json"
        )
        with cfg_out.open("w") as f:
            json.dump(cfg, f, indent=4)

        cmd = list(p1.OZONEDB_JAVA_CMD_PREFIX) + [
            "-threads", "1",
            "-s",
            "-P", str(wl_path),
            "-p", "status.interval=1",
            "-p", f"maxexecutiontime={duration}",
            "-p", "operationcount=0",
            "-p", f"shared_config={cfg_out}",
            "-t",
        ]
        cwd = p1.OZONEDB_YCSB
        return cmd, env, cwd

    raise ValueError(f"unsupported system in Phase 1: {spec_name}")


@dataclass
class CellRepeat:
    ok: bool
    aggregate_throughput: Optional[float]
    per_proc_throughputs: list[Optional[float]]
    per_proc_logs: list[Path]
    sum_max_rss_kb: int
    p99_worst: Optional[int]
    elapsed_s: float
    error: str = ""


def run_one_repeat(spec_name: str, workload: str, n_procs: int, repeat: int,
                   duration: int, record_count: int, results_dir: Path
                   ) -> CellRepeat:
    log_dir = results_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Setup: restore single shared DB, drop caches.
    try:
        p1.kill_stragglers()
        work_path = restore_shared_for(spec_name)
        p1.drop_caches()
    except Exception as e:
        return CellRepeat(False, None, [], [], 0, None, 0.0, f"setup failed: {e}")

    workers: list[dict] = []
    log_paths: list[Path] = []
    t0 = time.time()
    try:
        for i in range(n_procs):
            cmd, env, cwd = build_shared_proc_cmd(
                spec_name, workload, n_procs, repeat, i, work_path,
                duration, record_count,
            )
            timed = [p1.TIME_BIN, "-v"] + cmd
            log_path = log_dir / (
                f"{spec_name}_workload{workload}_p{n_procs}_r{repeat}_proc{i}.log"
            )
            log_paths.append(log_path)
            log_f = log_path.open("w")
            log_f.write(f"# CMD: {shlex.join(timed)}\n")
            log_f.write(f"# shared_work_path={work_path}  proc_id={i}/{n_procs}\n")
            log_f.flush()
            proc = subprocess.Popen(timed, stdout=log_f, stderr=subprocess.STDOUT,
                                    env=env, cwd=str(cwd) if cwd else None)
            workers.append({"proc_id": i, "proc": proc, "log_f": log_f, "log_path": log_path})

        cell_deadline = t0 + duration + 600
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
        elapsed = time.time() - t0
        cleanup_shared_for(spec_name)
        p1.kill_stragglers()

    per_tputs: list[Optional[float]] = []
    rsses: list[int] = []
    p99s: list[int] = []
    rcs: list[int] = []
    for w in workers:
        try:
            text = w["log_path"].read_text()
        except OSError:
            text = ""
        per_tputs.append(p1.parse_throughput(text))
        rss = p1.parse_max_rss_kb(text)
        if rss is not None:
            rsses.append(rss)
        lat = p1.parse_latencies(text)
        if lat["p99"] is not None:
            p99s.append(lat["p99"])
        rcs.append(w["proc"].returncode)

    if all(t is not None for t in per_tputs) and all(rc == 0 for rc in rcs):
        agg: Optional[float] = sum(t for t in per_tputs if t is not None)
        ok = True
        err = ""
    else:
        agg = None
        ok = False
        err = ", ".join(f"proc{i} rc={rcs[i]} tput={per_tputs[i]}"
                        for i in range(len(per_tputs)))

    return CellRepeat(
        ok=ok,
        aggregate_throughput=agg,
        per_proc_throughputs=per_tputs,
        per_proc_logs=log_paths,
        sum_max_rss_kb=sum(rsses),
        p99_worst=max(p99s) if p99s else None,
        elapsed_s=elapsed,
        error=err,
    )


@dataclass
class CellSummary:
    system: str
    workload: str
    n_processes: int
    aggregate_throughputs: list[float] = field(default_factory=list)
    per_proc_throughputs_per_repeat: list[list[Optional[float]]] = field(default_factory=list)
    sum_rss_kb: list[int] = field(default_factory=list)
    p99_worst: list[int] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


def append_skipped(skipped_md: Path, cell: CellSummary, why: str,
                   log_paths: list[Path]) -> None:
    line = (
        f"- `{cell.system}` workload {cell.workload}  N={cell.n_processes}: **{why}**  \n"
        f"  Logs: " + ", ".join(f"`{p.name}`" for p in log_paths) + "\n"
    )
    with skipped_md.open("a") as f:
        f.write(line)


def run_cell(spec_name: str, workload: str, n_procs: int, repeats: int,
             duration: int, record_count: int, skipped_md: Path,
             results_dir: Path) -> CellSummary:
    summary = CellSummary(system=spec_name, workload=workload, n_processes=n_procs)
    print(f"\n=== {spec_name} workload-{workload}  N={n_procs} (SHARED DB) ===", flush=True)
    all_logs: list[Path] = []

    for repeat in range(1, repeats + 1):
        result = run_one_repeat(spec_name, workload, n_procs, repeat, duration,
                                record_count, results_dir)
        all_logs.extend(result.per_proc_logs)
        if not result.ok:
            print(f"  repeat {repeat}: FAIL ({result.error}). retrying once.", flush=True)
            retry = run_one_repeat(spec_name, workload, n_procs, repeat,
                                   duration, record_count, results_dir)
            all_logs.extend(retry.per_proc_logs)
            if not retry.ok:
                summary.failures.append(retry.error or result.error)
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
        if result.p99_worst is not None:
            summary.p99_worst.append(result.p99_worst)
        per_proc_str = ",".join(f"{t:.0f}" if t else "?"
                                for t in result.per_proc_throughputs)
        print(
            f"  repeat {repeat}: agg={result.aggregate_throughput:.0f} ops/s  "
            f"per-proc=[{per_proc_str}]  rss={result.sum_max_rss_kb} kB  "
            f"({result.elapsed_s:.0f}s)",
            flush=True,
        )
    if summary.failures and not summary.aggregate_throughputs:
        append_skipped(skipped_md, summary,
                       f"FAILED after retry: {summary.failures[-1]}",
                       all_logs)
    return summary


def write_results_csv(path: Path, summaries: list[CellSummary]) -> None:
    with path.open("w") as f:
        f.write("system,workload,n_processes,n_repeats,"
                "agg_throughput_mean,agg_throughput_stddev,rel_stddev_pct,"
                "sum_max_rss_kb_mean,p99_worst_mean,"
                "per_proc_throughputs,failures\n")
        for s in summaries:
            n = len(s.aggregate_throughputs)
            mean = statistics.mean(s.aggregate_throughputs) if n else ""
            sd = statistics.stdev(s.aggregate_throughputs) if n >= 2 else ""
            rel = (100 * statistics.stdev(s.aggregate_throughputs) / mean) \
                if (n >= 2 and mean) else ""
            rss = statistics.mean(s.sum_rss_kb) if s.sum_rss_kb else ""
            p99 = statistics.mean(s.p99_worst) if s.p99_worst else ""
            ppt = "|".join(",".join(f"{x:.2f}" if x else "nan" for x in row)
                           for row in s.per_proc_throughputs_per_repeat)
            fails = ";".join(s.failures) if s.failures else ""
            f.write(f"{s.system},{s.workload},{s.n_processes},{n},{mean},"
                    f"{sd},{rel},{rss},{p99},{ppt},{fails}\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--systems", default=",".join(SHARED_WORK.keys()))
    p.add_argument("--workloads", default=",".join(DEFAULT_WORKLOADS))
    p.add_argument("--processes", default=",".join(map(str, DEFAULT_PROCESSES)))
    p.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    p.add_argument("--duration", type=int, default=DEFAULT_DURATION)
    p.add_argument("--record-count", type=int, default=DEFAULT_RECORD_COUNT)
    p.add_argument("--results-dir", type=Path, required=True)
    args = p.parse_args()

    systems_arg = [s.strip() for s in args.systems.split(",") if s.strip()]
    bad = [s for s in systems_arg if s not in SHARED_WORK]
    if bad:
        print(f"unsupported system(s): {bad}", file=sys.stderr)
        return 2
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    proc_counts = [int(x) for x in args.processes.split(",") if x.strip()]
    proc_counts.sort()
    repeats = args.repeats
    duration = args.duration
    record_count = args.record_count

    results_dir = args.results_dir.resolve()
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "logs").mkdir(exist_ok=True)
    skipped_md = results_dir / "skipped_runs.md"
    if not skipped_md.exists():
        skipped_md.write_text("# Phase 1 (shared-DB) skipped/failed cells\n\n")
    p1.WORK_ROOT.mkdir(parents=True, exist_ok=True)
    p1.CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)

    print(f"Phase 1 SHARED-DB sweep: systems={systems_arg}  workloads={workloads}  "
          f"N={proc_counts}  repeats={repeats}  duration={duration}s  "
          f"recordcount={record_count}  results_dir={results_dir}", flush=True)

    p1.pre_generate_all_workloads(workloads, record_count)

    if "ozonedb" in systems_arg:
        p1.OZONEDB_JAVA_CMD_PREFIX = p1.warmup_ozonedb_classpath(record_count)

    summaries: list[CellSummary] = []
    t_start = time.time()
    for spec_name in systems_arg:
        for workload in workloads:
            for n_procs in proc_counts:
                summary = run_cell(spec_name, workload, n_procs, repeats,
                                   duration, record_count, skipped_md,
                                   results_dir)
                summaries.append(summary)
                write_results_csv(results_dir / "results.csv", summaries)
    print(f"\nTotal sweep wallclock: {(time.time() - t_start) / 60:.1f} min", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
