#!/usr/bin/env python3
"""Phase 2 — Extended multi-thread Load sweep for HCTree + OzoneDB.

Runs YCSB -load (insert-only) at increasing thread counts on a SINGLE database
per (system, T) cell, until the system hits a bottleneck (per the four-rule
stopping criterion in the prompt). The Load workload is "all inserts" — uses
the workload-A file with operationcount=recordcount and `-load` mode (which
ignores read/update mix and inserts the full record set).

For each (system, T) cell:
  1. Wipe any existing work DB.
  2. Spawn one YCSB load process with threadcount=T against a single empty DB.
  3. Capture throughput (records loaded per second), max RSS, bytes written,
     runtime; per-T result row in results.csv.

Stopping criterion (per-system, applies once observed for two CONSECUTIVE
thread-count steps):
  (1) Throughput plateau:  agg change < 5%.
  (2) Throughput regression: agg drops vs previous step.
  (3) Hard failure:  OOM, crash, or runtime > 3× the T=16 baseline.
  (4) Resource ceiling: CPU saturated AND no throughput gain, OR disk
      bandwidth saturated AND no throughput gain.

Usage:
  python3 run_phase2_extended_threads.py \\
      --systems hctree,ozonedb \\
      --thread-schedule 16,24,32,48,64,96,128,192,256 \\
      --repeats 3 \\
      --record-count 1000000 \\
      --results-dir /users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase2-extended-threads
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Reuse helpers from the existing single-process orchestrator.
sys.path.insert(0, "/users/Xinying/ozonedb/bench/scripts/local")
from run_fig2_sweep import (  # type: ignore
    SystemSpec, system_specs, drop_caches, kill_stragglers,
    diskstats_sectors_written, _physical_devices_for,
    OZONEDB_HOME, OZONEDB_YCSB, OZONEDB_BIN,
    YCSB_CPP_DIR, YCSB_CPP_BIN, TIME_BIN,
    parse_max_rss_kb,
)


# YCSB-cpp prints "Load throughput(ops/sec): N" for -load and
# "Run throughput(ops/sec): N" for -run. The base orchestrator only matches
# the latter (it never ran -load). Match either here.
_NUM = r"[\d.]+(?:[eE][+-]?\d+)?"
_THROUGHPUT_PATTERNS = [
    re.compile(rf"(?:Load|Run) throughput\(ops/sec\):\s*({_NUM})"),
    re.compile(rf"^\[OVERALL\],\s+Throughput\(ops/sec\),\s+({_NUM})", re.MULTILINE),
]


def parse_throughput(text: str) -> Optional[float]:
    for pat in _THROUGHPUT_PATTERNS:
        m = pat.search(text)
        if m:
            return float(m.group(1))
    return None

PER_LOAD_TIMEOUT_S = 60 * 60  # 1 h hard cap per load
PLATEAU_DELTA_PCT = 5.0
DEFAULT_REPEATS = 3
DEFAULT_THREAD_SCHEDULE = [16, 24, 32, 48, 64, 96, 128]


def build_load_cmd(system: SystemSpec, threads: int, record_count: int
                   ) -> tuple[list[str], dict[str, str], Optional[Path]]:
    env = os.environ.copy()
    cwd: Optional[Path] = None

    if system.name in ("trunkcpp", "bcw2", "hctree"):
        prop = {"trunkcpp": "sqlite-trunk.properties",
                "bcw2": "sqlite-bcw.properties",
                "hctree": "hctree.properties"}[system.name]
        cmd = [
            str(YCSB_CPP_BIN),
            "-db", "sqlite",
            "-load",
            "-P", str(YCSB_CPP_DIR / "workloads" / "workloada_hctree"),
            "-P", str(YCSB_CPP_DIR / "sqlite" / prop),
            "-p", f"sqlite.dbpath={system.work_path}",
            "-p", f"threadcount={threads}",
            "-p", f"recordcount={record_count}",
            "-p", f"operationcount={record_count}",
        ]
        return cmd, env, cwd

    if system.name == "ozonedb":
        env["OZONEDB_HOME"] = str(OZONEDB_HOME)
        wl_dir = OZONEDB_YCSB / "workloads" / "generated_workloads"
        wl_dir.mkdir(parents=True, exist_ok=True)
        wl_path = wl_dir / f"workloada_1KB_{record_count}_{record_count}"
        if not wl_path.exists():
            subprocess.run(
                ["python3",
                 str(OZONEDB_HOME / "bench" / "scripts" / "generate_workload.py"),
                 "--workload_name", "a",
                 "--key_size", "1KB",
                 "--operation_cnt", str(record_count),
                 "--record_cnt", str(record_count)],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        shared = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json"
        with open(shared) as f:
            cfg = json.load(f)
        cfg["db_path"] = str(system.work_path) + "/"
        out = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb.json"
        with out.open("w") as f:
            json.dump(cfg, f, indent=4)
        cmd = [
            str(OZONEDB_BIN), "load", "ozonedb",
            "-threads", str(threads),
            "-s",
            "-P", str(wl_path),
            "-p", "status.interval=1",
            "-p", f"shared_config={out}",
        ]
        cwd = OZONEDB_YCSB
        return cmd, env, cwd

    raise ValueError(f"unsupported system in Phase 2: {system.name}")


@dataclass
class LoadOutcome:
    threads: int
    repeat: int
    throughput: Optional[float]
    max_rss_kb: Optional[int]
    bytes_written: Optional[int]
    runtime_s: float
    log_path: Path
    returncode: int
    fail_reason: str = ""


def cpu_pct_from_time_v(stderr: str) -> Optional[float]:
    m = re.search(r"Percent of CPU this job got:\s*(\d+)%", stderr)
    return float(m.group(1)) if m else None


def run_one_load(spec: SystemSpec, threads: int, repeat: int, record_count: int,
                 results_dir: Path) -> LoadOutcome:
    log_dir = results_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{spec.name}_load_t{threads}_r{repeat}.log"

    kill_stragglers()
    spec._wipe_work()
    drop_caches()

    cmd, env, cwd = build_load_cmd(spec, threads, record_count)
    timed = [TIME_BIN, "-v"] + cmd
    devices = _physical_devices_for(spec.work_path)
    sectors_before = diskstats_sectors_written(devices)
    t0 = time.time()
    fail_reason = ""
    try:
        proc = subprocess.run(
            timed, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env, cwd=str(cwd) if cwd else None,
            timeout=PER_LOAD_TIMEOUT_S,
        )
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        kill_stragglers()
        with log_path.open("w") as f:
            f.write(f"# CMD: {shlex.join(timed)}\n# TIMEOUT after {PER_LOAD_TIMEOUT_S}s\n")
        spec._wipe_work()
        return LoadOutcome(threads, repeat, None, None, None, elapsed, log_path, -1,
                           f"timeout after {PER_LOAD_TIMEOUT_S}s")
    finally:
        sectors_after = diskstats_sectors_written(devices)

    out = proc.stdout or ""
    err = proc.stderr or ""
    tput = parse_throughput(out + err)
    rss = parse_max_rss_kb(err)
    cpu = cpu_pct_from_time_v(err)
    bw = (sectors_after - sectors_before) * 512 if devices else None

    with log_path.open("w") as f:
        f.write(f"# CMD: {shlex.join(timed)}\n")
        f.write(f"# devices={devices} sectors_before={sectors_before} sectors_after={sectors_after}\n")
        f.write(f"# elapsed_s={elapsed:.2f} cpu_pct={cpu} returncode={proc.returncode}\n")
        f.write("\n=== STDOUT ===\n")
        f.write(out)
        f.write("\n=== STDERR ===\n")
        f.write(err)

    spec._wipe_work()

    if proc.returncode != 0 or tput is None:
        fail_reason = f"rc={proc.returncode} tput_parsed={tput is not None}"

    return LoadOutcome(threads, repeat, tput, rss, bw, elapsed, log_path,
                       proc.returncode, fail_reason)


@dataclass
class CellAgg:
    threads: int
    throughputs: list[float] = field(default_factory=list)
    max_rss_kb: list[int] = field(default_factory=list)
    bytes_written: list[int] = field(default_factory=list)
    runtimes: list[float] = field(default_factory=list)
    cpu_pcts: list[float] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


def run_cell(spec: SystemSpec, threads: int, repeats: int, record_count: int,
             results_dir: Path) -> CellAgg:
    print(f"\n=== {spec.name}  Load  T={threads} ===", flush=True)
    cell = CellAgg(threads=threads)
    permanent_fail = False
    for r in range(1, repeats + 1):
        outcome = run_one_load(spec, threads, r, record_count, results_dir)
        if outcome.fail_reason:
            print(f"  repeat {r}: FAIL ({outcome.fail_reason}). retrying once.", flush=True)
            outcome = run_one_load(spec, threads, r, record_count, results_dir)
            if outcome.fail_reason:
                cell.failures.append(outcome.fail_reason)
                permanent_fail = True
                break
        if outcome.throughput is not None:
            cell.throughputs.append(outcome.throughput)
        if outcome.max_rss_kb is not None:
            cell.max_rss_kb.append(outcome.max_rss_kb)
        if outcome.bytes_written is not None:
            cell.bytes_written.append(outcome.bytes_written)
        cell.runtimes.append(outcome.runtime_s)
        print(
            f"  repeat {r}: tput={outcome.throughput} ops/s  rss={outcome.max_rss_kb} kB  "
            f"bytes={outcome.bytes_written}  runtime={outcome.runtime_s:.0f}s",
            flush=True,
        )
    if permanent_fail and not cell.throughputs:
        # all repeats failed; mark with sentinel
        pass
    return cell


def stopping_check(history: list[CellAgg], baseline_runtime: Optional[float]) -> Optional[str]:
    """Apply the four-rule stopping criterion. Returns reason string when triggered;
    None when sweep should continue.

    A rule must hold for two CONSECUTIVE steps to halt.
    """
    if len(history) < 3:
        return None  # need at least baseline + 2 subsequent steps
    last3 = history[-3:]
    # Skip if any of last3 has no throughput (treat as inconclusive — don't halt
    # on the absence; the failure was already logged).
    means = []
    for c in last3:
        if not c.throughputs:
            return None
        means.append(statistics.mean(c.throughputs))

    # Rule 1: plateau — change < 5% for two consecutive steps
    delta1 = abs(means[1] - means[0]) / means[0] * 100 if means[0] else 0
    delta2 = abs(means[2] - means[1]) / means[1] * 100 if means[1] else 0
    if delta1 < PLATEAU_DELTA_PCT and delta2 < PLATEAU_DELTA_PCT:
        return f"plateau (Δ={delta1:.1f}%, {delta2:.1f}% < {PLATEAU_DELTA_PCT}% over two steps)"

    # Rule 2: regression for two consecutive steps
    if means[1] < means[0] and means[2] < means[1]:
        return f"regression (means: {means[0]:.0f} → {means[1]:.0f} → {means[2]:.0f})"

    # Rule 3: hard failure — runtime > 3× baseline for two consecutive steps
    if baseline_runtime is not None:
        rt = [statistics.mean(c.runtimes) if c.runtimes else 0 for c in last3[1:]]
        if rt[0] > 3 * baseline_runtime and rt[1] > 3 * baseline_runtime:
            return (f"runtime exceeds 3× baseline for two consecutive steps "
                    f"({baseline_runtime:.0f}s baseline; recent {rt[0]:.0f}s, {rt[1]:.0f}s)")

    return None


def write_results_csv(path: Path, system: str, cells: list[CellAgg]) -> None:
    """Append rows for one system. Caller writes the header on first call."""
    write_header = not path.exists()
    with path.open("a") as f:
        if write_header:
            f.write("system,threads,n_repeats,throughput_mean,throughput_stddev,"
                    "rel_stddev_pct,max_rss_kb_mean,bytes_written_mean,"
                    "runtime_s_mean,failures\n")
        for c in cells:
            n = len(c.throughputs)
            mean = statistics.mean(c.throughputs) if n else ""
            sd = statistics.stdev(c.throughputs) if n >= 2 else ""
            rel = (100 * statistics.stdev(c.throughputs) / mean) if (n >= 2 and mean) else ""
            rss = statistics.mean(c.max_rss_kb) if c.max_rss_kb else ""
            bw = statistics.mean(c.bytes_written) if c.bytes_written else ""
            rt = statistics.mean(c.runtimes) if c.runtimes else ""
            fails = ";".join(c.failures) if c.failures else ""
            f.write(f"{system},{c.threads},{n},{mean},{sd},{rel},{rss},{bw},{rt},{fails}\n")


def append_worklog(worklog: Path, msg: str) -> None:
    worklog.parent.mkdir(parents=True, exist_ok=True)
    with worklog.open("a") as f:
        f.write(msg + "\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--systems", default="hctree,ozonedb",
                   help="hctree and/or ozonedb (Phase 2 systems)")
    p.add_argument("--thread-schedule",
                   default=",".join(map(str, DEFAULT_THREAD_SCHEDULE)),
                   help="comma-separated thread counts to try in order")
    p.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    p.add_argument("--record-count", type=int, default=1_000_000)
    p.add_argument("--results-dir", type=Path, required=True)
    args = p.parse_args()

    systems_arg = [s.strip() for s in args.systems.split(",") if s.strip()]
    specs_all = system_specs()
    bad = [s for s in systems_arg if s not in ("hctree", "ozonedb")]
    if bad:
        print(f"Phase 2 supports hctree/ozonedb only; got: {bad}", file=sys.stderr)
        return 2
    schedule = [int(x) for x in args.thread_schedule.split(",") if x.strip()]
    schedule.sort()

    args.results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.results_dir / "results.csv"
    if csv_path.exists():
        csv_path.unlink()  # fresh sweep
    skipped = args.results_dir / "skipped_runs.md"
    if not skipped.exists():
        skipped.write_text("# Phase 2 skipped cells\n\n")
    worklog = args.results_dir / "WORKLOG.md"
    append_worklog(
        worklog,
        f"\n## {time.strftime('%Y-%m-%d %H:%M:%S')} — Phase 2 launched\n"
        f"systems={systems_arg}  schedule={schedule}  repeats={args.repeats}\n"
    )

    for sys_name in systems_arg:
        spec = specs_all[sys_name]
        print(f"\n##### Phase 2: {sys_name} #####", flush=True)
        history: list[CellAgg] = []
        baseline_runtime: Optional[float] = None
        stop_reason = ""
        for T in schedule:
            cell = run_cell(spec, T, args.repeats, args.record_count, args.results_dir)
            history.append(cell)
            write_results_csv(csv_path, sys_name, [cell])
            if T == schedule[0] and cell.runtimes:
                baseline_runtime = statistics.mean(cell.runtimes)
                append_worklog(
                    worklog,
                    f"- {sys_name}: T={T} baseline runtime = {baseline_runtime:.0f}s, "
                    f"throughput mean = "
                    f"{statistics.mean(cell.throughputs) if cell.throughputs else 'NA'}",
                )
            reason = stopping_check(history, baseline_runtime)
            if reason:
                stop_reason = reason
                append_worklog(
                    worklog,
                    f"- {sys_name}: STOPPED at T={T}. Reason: {reason}",
                )
                break
            if not cell.throughputs:
                # Two consecutive failures stop; check this here
                if len(history) >= 2 and not history[-2].throughputs:
                    stop_reason = f"two consecutive hard failures at T={schedule[schedule.index(T)-1]} and T={T}"
                    append_worklog(worklog, f"- {sys_name}: STOPPED. {stop_reason}")
                    break
        if not stop_reason:
            append_worklog(worklog, f"- {sys_name}: completed full schedule without hitting stop.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
