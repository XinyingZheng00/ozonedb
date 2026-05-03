#!/usr/bin/env python3
"""Phase 3 — Extended multi-process SHARED-DB Load sweep for OzoneDB.

N independent OS processes per cell all -load into the SAME empty DB (matches
Phase 1's shared-DB design). Disjoint key ranges via YCSB
`insertstart`/`insertcount` so total inserts = recordcount (1M), each process
inserts recordcount/N records into [i*1M/N, (i+1)*1M/N).

Each process is single-threaded; concurrency comes entirely from N processes.
Aggregate throughput = sum of per-process throughputs.

Reuses helpers from run_phase1_multiproc_sweep.py:
  * warmup_ozonedb_classpath() to bypass parallel-mvn races.
  * SystemSpec / system_specs() / drop_caches / kill_stragglers /
    parse_throughput / parse_max_rss_kb / ensure_ozonedb_workload.

Usage:
  python3 run_phase3_extended_processes.py \\
      --process-schedule 16,24,32,48,64,96,128 \\
      --repeats 3 \\
      --record-count 1000000 \\
      --results-dir /users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase3-extended-processes
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

# Reuse helpers from Phase 1.
sys.path.insert(0, str(Path(__file__).parent))
import run_phase1_multiproc_sweep as p1  # type: ignore


# YCSB-cpp / YCSB-Java emit "Load throughput(ops/sec): N" during -load and
# "Run throughput(ops/sec): N" during -t. Phase 1's parser only handles the
# Run pattern. Override locally for Phase 3 to match Load too.
_NUM = r"[\d.]+(?:[eE][+-]?\d+)?"
_LOAD_THROUGHPUT_PATTERNS = [
    re.compile(rf"(?:Load|Run) throughput\(ops/sec\):\s*({_NUM})"),
    re.compile(rf"^\[OVERALL\],\s+Throughput\(ops/sec\),\s+({_NUM})", re.MULTILINE),
]


def parse_load_throughput(text: str) -> Optional[float]:
    for pat in _LOAD_THROUGHPUT_PATTERNS:
        m = pat.search(text)
        if m:
            return float(m.group(1))
    return None

PER_LOAD_TIMEOUT_S = 60 * 60  # per-cell wall-clock cap (covers all N parallel loads)
PLATEAU_DELTA_PCT = 5.0
DEFAULT_REPEATS = 3
DEFAULT_PROCESS_SCHEDULE = [16, 24, 32, 48, 64, 96, 128]


def build_load_proc_cmd(n_procs: int, repeat: int, proc_id: int,
                        work_path: Path, record_count: int
                        ) -> tuple[list[str], dict[str, str], Optional[Path]]:
    """OzoneDB-only Phase 3 load command. Single thread per process.

    Each process inserts a disjoint key range:
      proc_id i loads [i * recordcount/N, (i+1) * recordcount/N)
    so that total inserts across N processes = recordcount, no key collisions.
    `work_path` is the SHARED DB path — every process opens the same one.
    """
    if p1.OZONEDB_JAVA_CMD_PREFIX is None:
        raise RuntimeError("OzoneDB classpath not resolved (run warmup first)")
    env = os.environ.copy()
    env["OZONEDB_HOME"] = str(p1.OZONEDB_HOME)

    wl_path = p1.ensure_ozonedb_workload("a", record_count)

    p1.CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    cfg_in = p1.OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json"
    with open(cfg_in) as f:
        cfg = json.load(f)
    cfg["db_path"] = str(work_path) + "/"
    cfg["mode"] = 1  # MultipleProcesses
    cfg_out = p1.CONFIG_TMP_ROOT / (
        f"shared_config_LOAD_p{n_procs}_r{repeat}_proc{proc_id}.json"
    )
    with cfg_out.open("w") as f:
        json.dump(cfg, f, indent=4)

    per_proc_records = record_count // n_procs
    insert_start = proc_id * per_proc_records
    # Last proc absorbs any remainder so total = record_count.
    if proc_id == n_procs - 1:
        per_proc_records = record_count - insert_start

    # Bypass bin/ycsb. The trailing "-load" tells site.ycsb.Client to do load
    # phase. insertstart/insertcount give each proc a disjoint key range so
    # there are no insert collisions on the shared DB.
    cmd = list(p1.OZONEDB_JAVA_CMD_PREFIX) + [
        "-threads", "1",
        "-s",
        "-P", str(wl_path),
        "-p", "status.interval=1",
        "-p", f"recordcount={record_count}",
        "-p", f"insertstart={insert_start}",
        "-p", f"insertcount={per_proc_records}",
        "-p", f"shared_config={cfg_out}",
        "-load",
    ]
    cwd = p1.OZONEDB_YCSB
    return cmd, env, cwd


@dataclass
class LoadCellRepeat:
    ok: bool
    aggregate_throughput: Optional[float]
    per_proc_throughputs: list[Optional[float]]
    per_proc_logs: list[Path]
    sum_max_rss_kb: int
    runtime_s: float
    error: str = ""


def run_one_load_cell(n_procs: int, repeat: int, record_count: int,
                      results_dir: Path) -> LoadCellRepeat:
    log_dir = results_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # SHARED empty work path for all N processes — Phase 3 mirrors Phase 1's
    # shared-DB design.
    shared_work = p1.WORK_ROOT / f"ozonedb_load_shared_p{n_procs}"
    work_paths = [shared_work] * n_procs

    p1.kill_stragglers()
    if shared_work.exists():
        shutil.rmtree(shared_work, ignore_errors=True)
    p1.drop_caches()

    workers: list[dict] = []
    log_paths: list[Path] = []
    t0 = time.time()
    try:
        for i, wp in enumerate(work_paths):
            cmd, env, cwd = build_load_proc_cmd(n_procs, repeat, i, wp, record_count)
            timed = [p1.TIME_BIN, "-v"] + cmd
            log_path = log_dir / f"ozonedb_load_p{n_procs}_r{repeat}_proc{i}.log"
            log_paths.append(log_path)
            log_f = log_path.open("w")
            log_f.write(f"# CMD: {shlex.join(timed)}\n")
            log_f.write(f"# work_path={wp}\n")
            log_f.flush()
            proc = subprocess.Popen(timed, stdout=log_f, stderr=subprocess.STDOUT,
                                    env=env, cwd=str(cwd) if cwd else None)
            workers.append({"proc_id": i, "proc": proc, "log_f": log_f, "log_path": log_path})

        cell_deadline = t0 + PER_LOAD_TIMEOUT_S
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
        if shared_work.exists():
            shutil.rmtree(shared_work, ignore_errors=True)
        p1.kill_stragglers()

    per_tputs: list[Optional[float]] = []
    rsses: list[int] = []
    rcs: list[int] = []
    for w in workers:
        try:
            text = w["log_path"].read_text()
        except OSError:
            text = ""
        per_tputs.append(parse_load_throughput(text))
        rss = p1.parse_max_rss_kb(text)
        if rss is not None:
            rsses.append(rss)
        rcs.append(w["proc"].returncode)

    if all(t is not None for t in per_tputs) and all(rc == 0 for rc in rcs):
        agg = sum(t for t in per_tputs if t is not None)
        ok = True
        err = ""
    else:
        agg = None
        ok = False
        err = ", ".join(f"proc{i} rc={rcs[i]} tput={per_tputs[i]}" for i in range(len(per_tputs)))

    return LoadCellRepeat(
        ok=ok, aggregate_throughput=agg, per_proc_throughputs=per_tputs,
        per_proc_logs=log_paths, sum_max_rss_kb=sum(rsses), runtime_s=elapsed,
        error=err,
    )


@dataclass
class CellAgg:
    n_processes: int
    aggregate_throughputs: list[float] = field(default_factory=list)
    per_proc_throughputs_per_repeat: list[list[Optional[float]]] = field(default_factory=list)
    sum_rss_kb: list[int] = field(default_factory=list)
    runtimes: list[float] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


def run_cell(n_procs: int, repeats: int, record_count: int,
             results_dir: Path) -> CellAgg:
    print(f"\n=== ozonedb LOAD  N={n_procs} ===", flush=True)
    cell = CellAgg(n_processes=n_procs)
    for r in range(1, repeats + 1):
        result = run_one_load_cell(n_procs, r, record_count, results_dir)
        if not result.ok:
            print(f"  repeat {r}: FAIL ({result.error}). retrying once.", flush=True)
            result = run_one_load_cell(n_procs, r, record_count, results_dir)
            if not result.ok:
                cell.failures.append(result.error)
                break
        cell.aggregate_throughputs.append(result.aggregate_throughput)
        cell.per_proc_throughputs_per_repeat.append(result.per_proc_throughputs)
        cell.sum_rss_kb.append(result.sum_max_rss_kb)
        cell.runtimes.append(result.runtime_s)
        per_proc_str = ",".join(f"{t:.0f}" if t else "?" for t in result.per_proc_throughputs)
        print(
            f"  repeat {r}: agg={result.aggregate_throughput:.0f} ops/s  "
            f"per-proc=[{per_proc_str}]  sum_rss={result.sum_max_rss_kb} kB  "
            f"runtime={result.runtime_s:.0f}s",
            flush=True,
        )
    return cell


def stopping_check(history: list[CellAgg], baseline_runtime: Optional[float]) -> Optional[str]:
    """Same four-rule criterion as Phase 2 plus FD/RAM ceiling notes."""
    if len(history) < 3:
        return None
    last3 = history[-3:]
    means = []
    for c in last3:
        if not c.aggregate_throughputs:
            return None
        means.append(statistics.mean(c.aggregate_throughputs))

    delta1 = abs(means[1] - means[0]) / means[0] * 100 if means[0] else 0
    delta2 = abs(means[2] - means[1]) / means[1] * 100 if means[1] else 0
    if delta1 < PLATEAU_DELTA_PCT and delta2 < PLATEAU_DELTA_PCT:
        return f"plateau (Δ={delta1:.1f}%, {delta2:.1f}%)"

    if means[1] < means[0] and means[2] < means[1]:
        return f"regression ({means[0]:.0f} → {means[1]:.0f} → {means[2]:.0f})"

    if baseline_runtime:
        rt = [statistics.mean(c.runtimes) if c.runtimes else 0 for c in last3[1:]]
        if rt[0] > 3 * baseline_runtime and rt[1] > 3 * baseline_runtime:
            return (f"runtime > 3× baseline for two consecutive steps "
                    f"({baseline_runtime:.0f}s baseline; {rt[0]:.0f}s, {rt[1]:.0f}s)")
    return None


def write_csv(path: Path, cells: list[CellAgg]) -> None:
    write_header = not path.exists()
    with path.open("a") as f:
        if write_header:
            f.write("system,n_processes,n_repeats,agg_throughput_mean,"
                    "agg_throughput_stddev,rel_stddev_pct,sum_max_rss_kb_mean,"
                    "runtime_s_mean,per_proc_throughputs,failures\n")
        for c in cells:
            n = len(c.aggregate_throughputs)
            mean = statistics.mean(c.aggregate_throughputs) if n else ""
            sd = statistics.stdev(c.aggregate_throughputs) if n >= 2 else ""
            rel = (100 * statistics.stdev(c.aggregate_throughputs) / mean) if (n >= 2 and mean) else ""
            rss = statistics.mean(c.sum_rss_kb) if c.sum_rss_kb else ""
            rt = statistics.mean(c.runtimes) if c.runtimes else ""
            ppt = "|".join(",".join(f"{x:.2f}" if x else "nan" for x in row)
                           for row in c.per_proc_throughputs_per_repeat)
            fails = ";".join(c.failures) if c.failures else ""
            f.write(f"ozonedb,{c.n_processes},{n},{mean},{sd},{rel},{rss},{rt},{ppt},{fails}\n")


def append_worklog(worklog: Path, msg: str) -> None:
    worklog.parent.mkdir(parents=True, exist_ok=True)
    with worklog.open("a") as f:
        f.write(msg + "\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--process-schedule",
                   default=",".join(map(str, DEFAULT_PROCESS_SCHEDULE)))
    p.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    p.add_argument("--record-count", type=int, default=1_000_000)
    p.add_argument("--results-dir", type=Path, required=True)
    args = p.parse_args()

    schedule = sorted(int(x) for x in args.process_schedule.split(",") if x.strip())
    args.results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.results_dir / "results.csv"
    if csv_path.exists():
        csv_path.unlink()
    skipped = args.results_dir / "skipped_runs.md"
    if not skipped.exists():
        skipped.write_text("# Phase 3 skipped cells\n\n")
    worklog = args.results_dir / "WORKLOG.md"
    append_worklog(worklog,
                   f"\n## {time.strftime('%Y-%m-%d %H:%M:%S')} — Phase 3 launched\n"
                   f"schedule={schedule}  repeats={args.repeats}\n")

    # Pre-warm ozonedb classpath
    p1.WORK_ROOT.mkdir(parents=True, exist_ok=True)
    p1.CONFIG_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    p1.OZONEDB_JAVA_CMD_PREFIX = p1.warmup_ozonedb_classpath(args.record_count)

    history: list[CellAgg] = []
    baseline_runtime: Optional[float] = None
    for N in schedule:
        cell = run_cell(N, args.repeats, args.record_count, args.results_dir)
        history.append(cell)
        write_csv(csv_path, [cell])
        if N == schedule[0] and cell.runtimes:
            baseline_runtime = statistics.mean(cell.runtimes)
            append_worklog(
                worklog,
                f"- baseline N={N}: runtime mean = {baseline_runtime:.0f}s  "
                f"throughput mean = "
                f"{statistics.mean(cell.aggregate_throughputs) if cell.aggregate_throughputs else 'NA'}",
            )
        reason = stopping_check(history, baseline_runtime)
        if reason:
            append_worklog(worklog, f"- STOPPED at N={N}: {reason}")
            break
        if not cell.aggregate_throughputs:
            if len(history) >= 2 and not history[-2].aggregate_throughputs:
                append_worklog(
                    worklog,
                    f"- STOPPED at N={N}: two consecutive hard failures",
                )
                break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
