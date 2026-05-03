#!/usr/bin/env python3
"""Load-phase orchestrator for the Fig 1 / Fig 2 sweep.

For each of the five systems, drops the working DB, runs a single-threaded
YCSB load (1 M records × 1 KB), captures throughput / RSS / bytes-written,
and renames the loaded DB to its `cached_*` location so subsequent run-phase
sweeps start from this freshly-loaded state.

Output:
    {results-dir}/{system}_load_t1_r1.log     per-cell log
    {results-dir}/load_results.csv            machine-readable summary
    {results-dir}/skipped_runs.md             appended on permanent failure

Default results-dir is the fig1 single-writer dir; load throughput belongs
in the same place as single-writer run throughput so the plot can stack
them on one axis.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Re-use orchestrator helpers / specs.
sys.path.insert(0, str(Path(__file__).parent))
from run_fig2_sweep import (  # type: ignore
    SystemSpec, system_specs, drop_caches, kill_stragglers,
    diskstats_sectors_written, _physical_devices_for,
    OZONEDB_HOME, OZONEDB_YCSB, OZONEDB_BIN, ROCKSDB_PREFIX,
    YCSB_CPP_DIR, YCSB_CPP_BIN, TIME_BIN,
    parse_throughput, parse_max_rss_kb,
    PER_RUN_TIMEOUT_S,
)


def build_load_cmd(system: SystemSpec, record_count: int) -> tuple[list[str], dict, Optional[Path]]:
    """Build a -load (insert-phase) YCSB command for the given system.

    Mirrors build_run_cmd in run_fig2_sweep.py but with `-load` instead of `-run`,
    threadcount=1, operationcount=record_count, and no maxexecutiontime cap.
    """
    env = os.environ.copy()
    cwd: Optional[Path] = None

    if system.name in ("trunkcpp", "bcw2", "hctree"):
        prop = {
            "trunkcpp": "sqlite-trunk.properties",
            "bcw2": "sqlite-bcw.properties",
            "hctree": "hctree.properties",
        }[system.name]
        cmd = [
            str(YCSB_CPP_BIN),
            "-db", "sqlite",
            "-load",
            "-P", str(YCSB_CPP_DIR / "workloads" / "workloada_hctree"),
            "-P", str(YCSB_CPP_DIR / "sqlite" / prop),
            "-p", f"sqlite.dbpath={system.work_path}",
            "-p", "threadcount=1",
            "-p", f"recordcount={record_count}",
            "-p", f"operationcount={record_count}",
        ]
        return cmd, env, cwd

    if system.name == "rocksdb":
        ld = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"{ROCKSDB_PREFIX}/lib" + (f":{ld}" if ld else "")
        cmd = [
            str(YCSB_CPP_BIN),
            "-db", "rocksdb",
            "-load",
            "-P", str(YCSB_CPP_DIR / "workloads" / "workloada"),
            "-P", str(YCSB_CPP_DIR / "rocksdb" / "rocksdb.properties"),
            "-p", f"rocksdb.dbname={system.work_path}",
            "-p", "rocksdb.destroy=true",
            "-p", "rocksdb.per_op_db=false",   # load is single-thread; never per-op.
            "-p", "threadcount=1",
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
            subprocess.run([
                "python3",
                str(OZONEDB_HOME / "bench" / "scripts" / "generate_workload.py"),
                "--workload_name", "a",
                "--key_size", "1KB",
                "--operation_cnt", str(record_count),
                "--record_cnt", str(record_count),
            ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Patch shared_config to point at the working dir for OzoneDB.
        shared = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json"
        with open(shared) as f:
            cfg = json.load(f)
        cfg["db_path"] = str(system.work_path) + "/"
        out = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb.json"
        with open(out, "w") as f:
            json.dump(cfg, f, indent=4)

        cmd = [
            str(OZONEDB_BIN), "load", "ozonedb",
            "-threads", "1",
            "-s",
            "-P", str(wl_path),
            "-p", "status.interval=1",
            "-p", f"shared_config={out}",
        ]
        cwd = OZONEDB_YCSB
        return cmd, env, cwd

    raise ValueError(f"unknown system {system.name}")


def _aux_paths(spec: SystemSpec) -> list[Path]:
    """Aux files for a SQLite/HCTree DB (wal, wal2, shm, pagemap, log-N)."""
    return [spec.work_path.with_name(spec.work_path.name + suf)
            for suf in spec.aux_suffixes]


def commit_to_cached(spec: SystemSpec) -> None:
    """Move/copy the freshly-loaded DB to the cached_* location used by the run sweep."""
    if spec.cached_path.exists():
        if spec.cached_path.is_dir():
            shutil.rmtree(spec.cached_path, ignore_errors=True)
        else:
            spec.cached_path.unlink(missing_ok=True)
    # Aux files for the cached SQLite DB.
    for suf in spec.aux_suffixes:
        p = spec.cached_path.with_name(spec.cached_path.name + suf)
        if p.exists():
            p.unlink(missing_ok=True)

    # Move loaded files into cached_* location.
    spec.cached_path.parent.mkdir(parents=True, exist_ok=True)
    if spec.work_path.is_dir():
        shutil.move(str(spec.work_path), str(spec.cached_path))
    else:
        shutil.move(str(spec.work_path), str(spec.cached_path))
        for suf in spec.aux_suffixes:
            src = spec.work_path.with_name(spec.work_path.name + suf)
            if src.exists():
                shutil.move(str(src),
                            str(spec.cached_path.with_name(spec.cached_path.name + suf)))


@dataclass
class LoadResult:
    system: str
    ok: bool
    throughput: Optional[float] = None
    max_rss_kb: Optional[int] = None
    bytes_written: Optional[int] = None
    duration_s: float = 0.0
    log_path: Optional[Path] = None
    error: str = ""


def run_load(spec: SystemSpec, record_count: int, results_dir: Path) -> LoadResult:
    log_path = results_dir / f"{spec.name}_load_t1_r1.log"

    # Wipe any prior state for this system.
    kill_stragglers()
    if spec.work_path.exists():
        if spec.work_path.is_dir():
            shutil.rmtree(spec.work_path, ignore_errors=True)
        else:
            spec.work_path.unlink(missing_ok=True)
            for p in _aux_paths(spec):
                p.unlink(missing_ok=True)
    drop_caches()

    cmd, env, cwd = build_load_cmd(spec, record_count)
    timed_cmd = [TIME_BIN, "-v"] + cmd

    devices = _physical_devices_for(spec.work_path.parent)
    sectors_before = diskstats_sectors_written(devices)
    t0 = time.time()
    try:
        proc = subprocess.run(
            timed_cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=env, cwd=str(cwd) if cwd else None,
            timeout=PER_RUN_TIMEOUT_S,
        )
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        with log_path.open("w") as f:
            f.write(f"# CMD: {shlex.join(timed_cmd)}\n# TIMEOUT after {PER_RUN_TIMEOUT_S}s\n")
        return LoadResult(system=spec.name, ok=False, duration_s=elapsed,
                          log_path=log_path, error=f"timeout after {PER_RUN_TIMEOUT_S}s")
    sectors_after = diskstats_sectors_written(devices)

    out = proc.stdout or ""
    err = proc.stderr or ""
    # YCSB-cpp emits "Load throughput(ops/sec): N" at the end; YCSB-Java uses
    # the same "[OVERALL], Throughput(ops/sec), N" format the run-phase parser
    # already handles.  Re-use parse_throughput (handles both) but also fall
    # back to "Load throughput".
    throughput = parse_throughput(out + err)
    if throughput is None:
        m = re.search(r"Load throughput\(ops/sec\):\s*([\d.]+(?:[eE][+-]?\d+)?)", out + err)
        if m:
            throughput = float(m.group(1))
    max_rss = parse_max_rss_kb(err)

    with log_path.open("w") as f:
        f.write(f"# CMD: {shlex.join(timed_cmd)}\n")
        f.write(f"# devices={devices} sectors_before={sectors_before} sectors_after={sectors_after}\n")
        f.write(f"# elapsed_s={elapsed:.2f} returncode={proc.returncode}\n")
        f.write("\n=== STDOUT ===\n")
        f.write(out)
        f.write("\n=== STDERR ===\n")
        f.write(err)

    if proc.returncode != 0 or throughput is None:
        return LoadResult(system=spec.name, ok=False, throughput=throughput,
                          max_rss_kb=max_rss, log_path=log_path,
                          duration_s=elapsed,
                          error=f"returncode={proc.returncode}; throughput_parsed={throughput is not None}")

    # Move loaded DB to its cached_* location so subsequent run sweeps use it.
    try:
        commit_to_cached(spec)
    except Exception as e:
        return LoadResult(system=spec.name, ok=False, throughput=throughput,
                          max_rss_kb=max_rss, log_path=log_path,
                          duration_s=elapsed,
                          error=f"caching failed: {e}")

    return LoadResult(
        system=spec.name, ok=True,
        throughput=throughput, max_rss_kb=max_rss,
        bytes_written=(sectors_after - sectors_before) * 512 if devices else None,
        duration_s=elapsed, log_path=log_path,
    )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--systems", default="ozonedb,trunkcpp,bcw2,hctree,rocksdb")
    p.add_argument("--record-count", type=int, default=1_000_000)
    p.add_argument("--results-dir", type=Path,
                   default=Path("/users/Xinying/ozonedb/bench/results/local/fig1-single-writer-local"))
    args = p.parse_args()

    systems = [s.strip() for s in args.systems.split(",") if s.strip()]
    available = system_specs()
    unknown = [s for s in systems if s not in available]
    if unknown:
        print(f"unknown system(s): {unknown}", file=sys.stderr)
        return 2

    args.results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.results_dir / "load_results.csv"
    with csv_path.open("w") as f:
        f.write("system,record_count,throughput,max_rss_kb,bytes_written,duration_s,error\n")

    for sys_name in systems:
        spec = available[sys_name]
        print(f"\n=== load: {sys_name} ===", flush=True)
        result = run_load(spec, args.record_count, args.results_dir)
        if result.ok:
            print(f"  OK  throughput={result.throughput:,.0f} ops/s  rss={result.max_rss_kb} kB  "
                  f"bytes={result.bytes_written}  ({result.duration_s:.0f}s)", flush=True)
        else:
            print(f"  FAIL {result.error}  ({result.duration_s:.0f}s)", flush=True)

        with csv_path.open("a") as f:
            f.write(f"{result.system},{args.record_count},"
                    f"{result.throughput or ''},"
                    f"{result.max_rss_kb or ''},"
                    f"{result.bytes_written or ''},"
                    f"{result.duration_s:.2f},"
                    f"{result.error}\n")

    print(f"\nload summary: {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
