#!/usr/bin/env python3
"""Fig 2 multi-writer scaling sweep orchestrator.

Drives a (system x workload x threadcount x repeat) matrix for the
five-system multi-writer experiment.  For every cell:

    1. Ensure no stragglers (sqlite3 / ycsb processes from a previous attempt).
    2. Restore the cached DB into a fresh working location.
    3. sync && drop OS page cache.
    4. Sample /proc/diskstats, then exec the YCSB binary under /usr/bin/time
       -v to capture max RSS.
    5. Sample /proc/diskstats again, parse run-throughput, append to per-cell
       log.
    6. On non-zero exit OR missing throughput line: retry once after fresh
       state.  If still failing, mark FAILED and append to skipped_runs.md.
    7. After all repeats of a cell, compute throughput stddev and re-queue
       the cell up to once if variance > 10%%.

Logs land in $RESULTS_DIR/{system}_{workload}_{threads}_{repeat}.log
Skips land in $RESULTS_DIR/skipped_runs.md
Summary lands in $RESULTS_DIR/results.csv
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

# --------------------------------------------------------------------------
#  Static configuration
# --------------------------------------------------------------------------

YCSB_CPP_DIR = Path("/users/Xinying/YCSB-cpp")
YCSB_CPP_BIN = YCSB_CPP_DIR / "ycsb"
ROCKSDB_PREFIX = Path("/users/Xinying/rocksdb-9.6.1")

OZONEDB_HOME = Path("/users/Xinying/ozonedb")
OZONEDB_YCSB = OZONEDB_HOME / "ycsb"
OZONEDB_BIN = OZONEDB_YCSB / "bin" / "ycsb"

RESULTS_DIR = Path("/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local")
DATA_ROOT = Path("/tank/ycsb_data")

DEFAULT_RECORD_COUNT = 1_000_000
DEFAULT_WORKLOADS = ["a", "b", "c", "d", "f"]
DEFAULT_THREADS = [2, 4, 8, 16]
DEFAULT_RUN_DURATION = 120
DEFAULT_REPEATS = 3
DEFAULT_VARIANCE_PCT = 10.0   # re-run cell if relative stddev > 10%
PER_RUN_TIMEOUT_S = 60 * 30   # absolute wall-clock cap per repeat
TIME_BIN = "/usr/bin/time"

# --------------------------------------------------------------------------
#  System descriptors
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SystemSpec:
    """How to invoke YCSB for one storage system in the sweep."""

    name: str
    cached_path: Path
    work_path: Path
    aux_suffixes: tuple = ()

    def restore_cached(self) -> None:
        """Copy cached DB into the working location before each repeat."""
        self._wipe_work()
        if self.cached_path.is_dir():
            # cp --reflink=auto where supported (btrfs/xfs); plain copy otherwise.
            run_quiet(["cp", "-r", "--reflink=auto", str(self.cached_path), str(self.work_path)])
        else:
            run_quiet(["cp", "--reflink=auto", str(self.cached_path), str(self.work_path)])
            for suf in self.aux_suffixes:
                src = self.cached_path.with_name(self.cached_path.name + suf)
                if src.exists():
                    run_quiet(["cp", "--reflink=auto", str(src),
                               str(self.work_path.with_name(self.work_path.name + suf))])

    def _wipe_work(self) -> None:
        if self.work_path.exists():
            if self.work_path.is_dir():
                shutil.rmtree(self.work_path, ignore_errors=True)
            else:
                self.work_path.unlink(missing_ok=True)
        for suf in self.aux_suffixes:
            p = self.work_path.with_name(self.work_path.name + suf)
            if p.exists():
                p.unlink(missing_ok=True)

    def post_run_cleanup(self) -> None:
        """Remove the working DB after each repeat; cached_* is preserved."""
        self._wipe_work()


def system_specs() -> dict[str, SystemSpec]:
    return {
        "ozonedb": SystemSpec(
            name="ozonedb",
            cached_path=DATA_ROOT / "cached_data-ozonedb-1KB-1000000",
            work_path=DATA_ROOT / "ozonedb_run",
        ),
        "rocksdb": SystemSpec(
            name="rocksdb",
            cached_path=DATA_ROOT / "cached_rocksdb_cpp_ycsb",
            work_path=DATA_ROOT / "rocksdb_cpp_ycsb",
        ),
        "trunkcpp": SystemSpec(
            name="trunkcpp",
            cached_path=DATA_ROOT / "trunkcpp_ycsb" / "cached_trunkcpp_ycsb.db",
            work_path=DATA_ROOT / "trunkcpp_ycsb" / "trunkcpp_ycsb.db",
            aux_suffixes=("-wal", "-wal2", "-shm"),
        ),
        "bcw2": SystemSpec(
            name="bcw2",
            cached_path=DATA_ROOT / "bcw2_ycsb" / "cached_bcw_ycsb.db",
            work_path=DATA_ROOT / "bcw2_ycsb" / "bcw_ycsb.db",
            aux_suffixes=("-wal", "-wal2", "-shm"),
        ),
        "hctree": SystemSpec(
            name="hctree",
            cached_path=DATA_ROOT / "hctree_ycsb" / "cached_hctree_ycsb.db",
            work_path=DATA_ROOT / "hctree_ycsb" / "hctree_ycsb.db",
            aux_suffixes=("-wal", "-shm", "-pagemap", "-log-0", "-log-1"),
        ),
    }


# --------------------------------------------------------------------------
#  Command builders
# --------------------------------------------------------------------------


def workload_file(workload: str) -> Path:
    """Map a YCSB workload letter to its file path on disk."""
    if workload == "a":
        return YCSB_CPP_DIR / "workloads" / "workloada_hctree"
    return YCSB_CPP_DIR / "workloads" / f"workload{workload}"


def build_run_cmd(system: SystemSpec, workload: str, threads: int,
                  duration: int, record_count: int) -> tuple[list[str], dict[str, str], Optional[Path]]:
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
            "-run",
            "-P", str(workload_file(workload)),
            "-P", str(YCSB_CPP_DIR / "sqlite" / prop),
            "-p", f"sqlite.dbpath={system.work_path}",
            "-p", f"threadcount={threads}",
            "-p", f"recordcount={record_count}",
            "-p", f"maxexecutiontime={duration}",
        ]
        return cmd, env, cwd

    if system.name == "rocksdb":
        # Add rpath for RocksDB shared library.
        ld = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"{ROCKSDB_PREFIX}/lib" + (f":{ld}" if ld else "")
        cmd = [
            str(YCSB_CPP_BIN),
            "-db", "rocksdb",
            "-run",
            "-P", str(workload_file(workload)),
            "-P", str(YCSB_CPP_DIR / "rocksdb" / "rocksdb.properties"),
            "-p", f"rocksdb.dbname={system.work_path}",
            "-p", f"threadcount={threads}",
            "-p", f"recordcount={record_count}",
            "-p", f"maxexecutiontime={duration}",
        ]
        return cmd, env, cwd

    if system.name == "ozonedb":
        # Generate a workload file on the fly (same convention used by
        # /users/Xinying/ozonedb/bench/scripts/local/run_local_ycsb.py).
        env["OZONEDB_HOME"] = str(OZONEDB_HOME)
        wl_dir = OZONEDB_YCSB / "workloads" / "generated_workloads"
        wl_dir.mkdir(parents=True, exist_ok=True)
        wl_path = wl_dir / f"workload{workload}_1KB_{record_count}_{record_count}"
        if not wl_path.exists():
            run_quiet([
                "python3",
                str(OZONEDB_HOME / "bench" / "scripts" / "generate_workload.py"),
                "--workload_name", workload,
                "--key_size", "1KB",
                "--operation_cnt", str(record_count),
                "--record_cnt", str(record_count),
            ])

        # Patch shared_config_rocksdb.json to point at the working dir.
        shared = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json"
        with open(shared) as f:
            cfg = json.load(f)
        cfg["db_path"] = str(system.work_path) + "/"
        out = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb.json"
        with open(out, "w") as f:
            json.dump(cfg, f, indent=4)

        cmd = [
            str(OZONEDB_BIN), "run", "ozonedb",
            "-threads", str(threads),
            "-s",
            "-P", str(wl_path),
            "-p", "status.interval=1",
            "-p", f"maxexecutiontime={duration}",
            "-p", "operationcount=0",
            "-p", f"shared_config={out}",
        ]
        # The bin/ycsb python wrapper invokes Maven from cwd; it must be run
        # from the YCSB root that contains pom.xml, otherwise classpath
        # resolution fails with "Could not find the selected project ...".
        cwd = OZONEDB_YCSB
        return cmd, env, cwd

    raise ValueError(f"unknown system {system.name}")


# --------------------------------------------------------------------------
#  Helpers
# --------------------------------------------------------------------------


def run_quiet(cmd: list[str]) -> None:
    subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def drop_caches() -> None:
    subprocess.run(["sync"], check=False)
    subprocess.run(
        ["sudo", "-n", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
        check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def kill_stragglers() -> None:
    # Best-effort: nuke any leftover YCSB worker processes from a previous
    # attempt.  Use specific patterns and exclude this orchestrator's own pid
    # (the script lives under /users/Xinying/ozonedb/, so a naive `-f ozonedb`
    # would match and kill ourselves).
    self_pid = os.getpid()
    for pat in (r"^.*YCSB-cpp/ycsb\b", r"^.*site\.ycsb\.Client"):
        # pgrep -f to identify, then kill all but our own pid.
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


_DISKSTATS_RE = re.compile(r"\s*\d+\s+\d+\s+(\S+)\s+(\d+\s+){13,17}")


def _physical_devices_for(path: Path) -> list[str]:
    """Return the kernel block-device name(s) backing the given path.

    Handles both plain block devices (e.g. /dev/sda3 -> ['sda3']) and ZFS
    pools (-> the list of physical vdevs from `zpool status <pool>`).  Returns
    [] if the device cannot be determined.
    """
    if not path.exists():
        path = path.parent  # df needs an existing path; parent dir is fine.
    try:
        out = subprocess.check_output(["df", "--output=source,fstype", str(path)],
                                      text=True).splitlines()
    except Exception:
        return []
    if len(out) < 2:
        return []
    src, _, fstype = out[1].strip().partition(" ")
    fstype = fstype.strip()
    src = src.strip()
    name = Path(src).name

    # Direct block device.
    if Path("/proc/diskstats").exists():
        with open("/proc/diskstats") as f:
            diskstats_names = {line.split()[2] for line in f if len(line.split()) > 3}
        if name in diskstats_names:
            return [name]

    # ZFS pool — resolve to its physical vdevs.
    if fstype == "zfs":
        try:
            zout = subprocess.check_output(["zpool", "status", name], text=True)
        except Exception:
            return []
        devs = []
        for line in zout.splitlines():
            tok = line.strip().split()
            # Lines listing vdevs look like "  sda6  ONLINE  0  0  0".
            if len(tok) >= 5 and tok[1] in ("ONLINE", "DEGRADED") and tok[0] not in (name,):
                devs.append(tok[0])
        return devs
    return []


def diskstats_sectors_written(devices: list[str]) -> int:
    """Sum 'sectors written' across the given devices."""
    if not devices:
        return 0
    targets = set(devices)
    total = 0
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                parts = line.split()
                # Format: major minor name reads(0..3) writes(4..7) ...
                # field 9 (0-indexed) is sectors_written.
                if len(parts) >= 14 and parts[2] in targets:
                    total += int(parts[9])
    except Exception:
        pass
    return total


# RocksDB throughput line:           "Run throughput(ops/sec): 41815"
# OzoneDB / YCSB-Java summary line:  "[OVERALL], Throughput(ops/sec), 41815.32"
# Throughputs ≥ 1M ops/sec render in scientific notation (e.g. "1.11796e+06").
_NUM = r"[\d.]+(?:[eE][+-]?\d+)?"
_THROUGHPUT_PATTERNS = [
    re.compile(rf"Run throughput\(ops/sec\):\s*({_NUM})"),
    re.compile(rf"^\[OVERALL\],\s+Throughput\(ops/sec\),\s+({_NUM})", re.MULTILINE),
]


def parse_throughput(stdout: str) -> Optional[float]:
    for pat in _THROUGHPUT_PATTERNS:
        m = pat.search(stdout)
        if m:
            return float(m.group(1))
    return None


# /usr/bin/time -v:  "Maximum resident set size (kbytes): 12345"
_RSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")


def parse_max_rss_kb(stderr: str) -> Optional[int]:
    m = _RSS_RE.search(stderr)
    return int(m.group(1)) if m else None


# --------------------------------------------------------------------------
#  One repeat
# --------------------------------------------------------------------------


@dataclass
class RepeatResult:
    ok: bool
    throughput: Optional[float] = None
    max_rss_kb: Optional[int] = None
    sectors_written: Optional[int] = None
    duration_s: float = 0.0
    log_path: Optional[Path] = None
    error: str = ""


def run_one_repeat(system: SystemSpec, workload: str, threads: int, repeat: int,
                   duration: int, record_count: int, results_dir: Path) -> RepeatResult:
    log_path = results_dir / f"{system.name}_workload{workload}_t{threads}_r{repeat}.log"

    try:
        kill_stragglers()
        system.restore_cached()
        drop_caches()
    except Exception as e:
        return RepeatResult(ok=False, log_path=log_path, error=f"setup failed: {e}")

    cmd, env, cwd = build_run_cmd(system, workload, threads, duration, record_count)
    timed_cmd = [TIME_BIN, "-v"] + cmd

    devices = _physical_devices_for(system.work_path)
    sectors_before = diskstats_sectors_written(devices)
    t0 = time.time()
    try:
        proc = subprocess.run(
            timed_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            cwd=str(cwd) if cwd else None,
            timeout=PER_RUN_TIMEOUT_S,
        )
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        kill_stragglers()
        with log_path.open("w") as f:
            f.write(f"# CMD: {shlex.join(timed_cmd)}\n# TIMEOUT after {PER_RUN_TIMEOUT_S}s\n")
        return RepeatResult(ok=False, duration_s=elapsed, log_path=log_path,
                            error=f"timeout after {PER_RUN_TIMEOUT_S}s")
    finally:
        sectors_after = diskstats_sectors_written(devices)
        system.post_run_cleanup()

    out = proc.stdout or ""
    err = proc.stderr or ""
    throughput = parse_throughput(out + err)
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
        return RepeatResult(
            ok=False, throughput=throughput, max_rss_kb=max_rss,
            sectors_written=(sectors_after - sectors_before) if devices else None,
            duration_s=elapsed, log_path=log_path,
            error=f"returncode={proc.returncode}; throughput_parsed={throughput is not None}",
        )

    return RepeatResult(
        ok=True, throughput=throughput, max_rss_kb=max_rss,
        sectors_written=(sectors_after - sectors_before) if devices else None,
        duration_s=elapsed, log_path=log_path,
    )


# --------------------------------------------------------------------------
#  Sweep
# --------------------------------------------------------------------------


@dataclass
class CellSummary:
    system: str
    workload: str
    threads: int
    throughputs: list[float] = field(default_factory=list)
    max_rss_kb: list[int] = field(default_factory=list)
    bytes_written: list[int] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def relative_stddev_pct(self) -> Optional[float]:
        if len(self.throughputs) < 2:
            return None
        mean = statistics.mean(self.throughputs)
        if mean == 0:
            return None
        return 100.0 * statistics.stdev(self.throughputs) / mean


def append_skipped(skipped_md: Path, cell: CellSummary, why: str, log_paths: list[Path]) -> None:
    line = (
        f"- `{cell.system}` workload {cell.workload}  T={cell.threads}: **{why}**  \n"
        f"  Logs: " + ", ".join(f"`{p.name}`" for p in log_paths) + "\n"
    )
    with skipped_md.open("a") as f:
        f.write(line)


def run_cell(spec: SystemSpec, workload: str, threads: int, repeats: int,
             duration: int, record_count: int, skipped_md: Path,
             variance_pct: float, results_dir: Path) -> CellSummary:
    summary = CellSummary(system=spec.name, workload=workload, threads=threads)
    log_paths: list[Path] = []
    permanent_fail = False

    for repeat in range(1, repeats + 1):
        result = run_one_repeat(spec, workload, threads, repeat, duration, record_count, results_dir)
        log_paths.append(result.log_path)

        if not result.ok:
            print(f"  repeat {repeat}: FAIL ({result.error}). retrying once.", flush=True)
            retry = run_one_repeat(spec, workload, threads, repeat, duration, record_count, results_dir)
            log_paths.append(retry.log_path)
            if not retry.ok:
                permanent_fail = True
                summary.failures.append(retry.error or result.error)
                renamed = retry.log_path.with_name(
                    f"{spec.name}_workload{workload}_t{threads}_FAILED.log"
                )
                try:
                    retry.log_path.rename(renamed)
                    log_paths[-1] = renamed
                except OSError:
                    pass
                break
            result = retry

        summary.throughputs.append(result.throughput)
        if result.max_rss_kb is not None:
            summary.max_rss_kb.append(result.max_rss_kb)
        if result.sectors_written is not None:
            # 1 sector = 512 bytes
            summary.bytes_written.append(result.sectors_written * 512)
        print(
            f"  repeat {repeat}: {result.throughput:.0f} ops/s  "
            f"rss={result.max_rss_kb} kB  bytes={result.sectors_written * 512 if result.sectors_written else '?'}  "
            f"({result.duration_s:.0f}s)",
            flush=True,
        )

    if permanent_fail:
        append_skipped(skipped_md, summary,
                       f"FAILED after retry: {summary.failures[-1] if summary.failures else 'unknown'}",
                       log_paths)
    elif (var := summary.relative_stddev_pct()) is not None and var > variance_pct:
        # Re-run cell once if variance is high.
        print(f"  variance {var:.1f}% > {variance_pct}%, re-running once.", flush=True)
        rerun_result = run_one_repeat(spec, workload, threads, repeats + 1, duration, record_count, results_dir)
        log_paths.append(rerun_result.log_path)
        if rerun_result.ok and rerun_result.throughput is not None:
            summary.throughputs.append(rerun_result.throughput)
            if rerun_result.max_rss_kb is not None:
                summary.max_rss_kb.append(rerun_result.max_rss_kb)
            if rerun_result.sectors_written is not None:
                summary.bytes_written.append(rerun_result.sectors_written * 512)

    return summary


def write_results_csv(path: Path, summaries: list[CellSummary]) -> None:
    with path.open("w") as f:
        f.write("system,workload,threads,n_repeats,throughput_mean,throughput_stddev,"
                "rel_stddev_pct,max_rss_kb_mean,bytes_written_mean,failures\n")
        for s in summaries:
            n = len(s.throughputs)
            tput_mean = statistics.mean(s.throughputs) if n else ""
            tput_sd = statistics.stdev(s.throughputs) if n >= 2 else ""
            rel = s.relative_stddev_pct() or ""
            rss = statistics.mean(s.max_rss_kb) if s.max_rss_kb else ""
            bw = statistics.mean(s.bytes_written) if s.bytes_written else ""
            fails = ";".join(s.failures) if s.failures else ""
            f.write(f"{s.system},{s.workload},{s.threads},{n},{tput_mean},"
                    f"{tput_sd},{rel},{rss},{bw},{fails}\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--systems", default="ozonedb,trunkcpp,bcw2,hctree,rocksdb",
                   help="Comma-separated system names (default: all five)")
    p.add_argument("--workloads", default=",".join(DEFAULT_WORKLOADS))
    p.add_argument("--threads", default=",".join(map(str, DEFAULT_THREADS)))
    p.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    p.add_argument("--duration", type=int, default=DEFAULT_RUN_DURATION)
    p.add_argument("--record-count", type=int, default=DEFAULT_RECORD_COUNT)
    p.add_argument("--results-dir", type=Path, default=RESULTS_DIR)
    p.add_argument("--variance-pct", type=float, default=DEFAULT_VARIANCE_PCT)
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)

    systems = [s.strip() for s in args.systems.split(",") if s.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    threads_list = [int(t) for t in args.threads.split(",") if t.strip()]

    available = system_specs()
    unknown = [s for s in systems if s not in available]
    if unknown:
        print(f"unknown system(s): {unknown}", file=sys.stderr)
        return 2

    skipped_md = args.results_dir / "skipped_runs.md"
    if not skipped_md.exists():
        with skipped_md.open("w") as f:
            f.write(f"# Skipped / failed cells — sweep started {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n\n")

    summaries: list[CellSummary] = []
    total_cells = len(systems) * len(workloads) * len(threads_list)
    cell_idx = 0
    for sys_name in systems:
        spec = available[sys_name]
        for workload in workloads:
            for threads in threads_list:
                cell_idx += 1
                print(f"\n[{cell_idx}/{total_cells}] {sys_name} workload{workload} T={threads}",
                      flush=True)
                summary = run_cell(spec, workload, threads, args.repeats,
                                   args.duration, args.record_count, skipped_md,
                                   args.variance_pct, args.results_dir)
                summaries.append(summary)
                # Write CSV after every cell so a partial sweep is still useful.
                write_results_csv(args.results_dir / "results.csv", summaries)

    print("\n=== sweep done ===")
    print(f"Results CSV: {args.results_dir / 'results.csv'}")
    print(f"Skip log:    {skipped_md}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
