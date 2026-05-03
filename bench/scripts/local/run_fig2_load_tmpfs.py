#!/usr/bin/env python3
"""Load-phase orchestrator pointed at tmpfs (/dev/shm) instead of /tank.

The tank version of this benchmark is sustained-sync-write-bandwidth-limited
on the SATA SSD; running the same load on tmpfs (where fsync is a no-op)
isolates the engine-level bottleneck from the storage bottleneck.

Output:  /dev/shm/ycsb_tmpfs_load/load_results.csv
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

YCSB_CPP = Path("/users/Xinying/YCSB-cpp")
YCSB_CPP_BIN = YCSB_CPP / "ycsb"
ROCKSDB_PREFIX = Path("/users/Xinying/rocksdb-9.6.1")
OZONEDB_HOME = Path("/users/Xinying/ozonedb")
OZONEDB_YCSB = OZONEDB_HOME / "ycsb"
OZONEDB_BIN = OZONEDB_YCSB / "bin" / "ycsb"

TMPFS_ROOT = Path("/dev/shm/ycsb_tmpfs_load")
RESULTS_DIR = Path("/users/Xinying/ozonedb/bench/results/local/fig1-single-writer-local")
RECORD_COUNT = 1_000_000
TIME_BIN = "/usr/bin/time"

THROUGHPUT_RE = re.compile(
    r"(?:Load throughput\(ops/sec\):\s*|^\[OVERALL\],\s+Throughput\(ops/sec\),\s+)"
    r"([\d.]+(?:[eE][+-]?\d+)?)",
    re.MULTILINE,
)


def parse_throughput(text: str) -> float | None:
    m = THROUGHPUT_RE.search(text)
    return float(m.group(1)) if m else None


def diskstats_sectors(device: str) -> int:
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                p = line.split()
                if len(p) >= 14 and p[2] == device:
                    return int(p[9])
    except Exception:
        pass
    return 0


def run_one(name: str, work_path: Path) -> dict:
    """Run a single-threaded load for `name` against the tmpfs work_path."""
    print(f"\n=== load (tmpfs): {name} ===", flush=True)
    if work_path.exists():
        if work_path.is_dir():
            shutil.rmtree(work_path, ignore_errors=True)
        else:
            work_path.unlink(missing_ok=True)
    work_path.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    cwd = None

    if name in ("trunkcpp", "bcw2", "hctree"):
        prop_file = {
            "trunkcpp": "sqlite-trunk.properties",
            "bcw2":     "sqlite-bcw.properties",
            "hctree":   "hctree.properties",
        }[name]
        cmd = [
            str(YCSB_CPP_BIN), "-db", "sqlite", "-load",
            "-P", str(YCSB_CPP / "workloads" / "workloada_hctree"),
            "-P", str(YCSB_CPP / "sqlite" / prop_file),
            "-p", f"sqlite.dbpath={work_path}",
            "-p", "threadcount=1",
            "-p", f"recordcount={RECORD_COUNT}",
            "-p", f"operationcount={RECORD_COUNT}",
        ]
    elif name == "rocksdb":
        env["LD_LIBRARY_PATH"] = f"{ROCKSDB_PREFIX}/lib:" + env.get("LD_LIBRARY_PATH", "")
        cmd = [
            str(YCSB_CPP_BIN), "-db", "rocksdb", "-load",
            "-P", str(YCSB_CPP / "workloads" / "workloada"),
            "-P", str(YCSB_CPP / "rocksdb" / "rocksdb.properties"),
            "-p", f"rocksdb.dbname={work_path}",
            "-p", "rocksdb.destroy=true",
            "-p", "rocksdb.per_op_db=false",
            "-p", "threadcount=1",
            "-p", f"recordcount={RECORD_COUNT}",
            "-p", f"operationcount={RECORD_COUNT}",
        ]
    elif name == "ozonedb":
        env["OZONEDB_HOME"] = str(OZONEDB_HOME)
        wl_dir = OZONEDB_YCSB / "workloads" / "generated_workloads"
        wl_path = wl_dir / f"workloada_1KB_{RECORD_COUNT}_{RECORD_COUNT}"
        if not wl_path.exists():
            subprocess.run([
                "python3", str(OZONEDB_HOME / "bench" / "scripts" / "generate_workload.py"),
                "--workload_name", "a", "--key_size", "1KB",
                "--operation_cnt", str(RECORD_COUNT), "--record_cnt", str(RECORD_COUNT),
            ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Patch shared_config to point at tmpfs.
        shared_base = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb_base.json"
        with open(shared_base) as f:
            cfg = json.load(f)
        cfg["db_path"] = str(work_path) + "/"
        out = OZONEDB_HOME / "src" / "config" / "local" / "shared_config_rocksdb.json"
        with open(out, "w") as f:
            json.dump(cfg, f, indent=4)
        cmd = [
            str(OZONEDB_BIN), "load", "ozonedb",
            "-threads", "1", "-s",
            "-P", str(wl_path),
            "-p", "status.interval=1",
            "-p", f"shared_config={out}",
        ]
        cwd = OZONEDB_YCSB
    else:
        raise ValueError(name)

    timed = [TIME_BIN, "-v"] + cmd
    sec_before = diskstats_sectors("sda6")
    t0 = time.time()
    proc = subprocess.run(timed, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, env=env, cwd=str(cwd) if cwd else None,
                          timeout=60 * 30)
    elapsed = time.time() - t0
    sec_after = diskstats_sectors("sda6")

    out = (proc.stdout or "") + (proc.stderr or "")
    throughput = parse_throughput(out)
    rss_m = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", proc.stderr or "")
    rss = int(rss_m.group(1)) if rss_m else None

    log_path = RESULTS_DIR / f"{name}_load_tmpfs_t1_r1.log"
    with log_path.open("w") as f:
        f.write(f"# CMD: {shlex.join(timed)}\n")
        f.write(f"# tmpfs work_path={work_path}  rc={proc.returncode}  elapsed_s={elapsed:.2f}\n")
        f.write(f"# sda6 sectors_before={sec_before} sectors_after={sec_after} (should be ≈0 delta on tmpfs)\n")
        f.write("\n=== STDOUT ===\n"); f.write(proc.stdout or "")
        f.write("\n=== STDERR ===\n"); f.write(proc.stderr or "")

    # Cleanup tmpfs DB.
    if work_path.exists():
        if work_path.is_dir():
            shutil.rmtree(work_path, ignore_errors=True)
        else:
            work_path.unlink(missing_ok=True)
            for suf in ("-wal", "-wal2", "-shm", "-pagemap", "-log-0", "-log-1"):
                p = work_path.with_name(work_path.name + suf)
                if p.exists():
                    p.unlink(missing_ok=True)

    sda6_bytes = (sec_after - sec_before) * 512
    if throughput is not None:
        print(f"  OK  throughput={throughput:,.0f} ops/s  rss={rss} kB  "
              f"sda6_bytes={sda6_bytes}  ({elapsed:.0f}s)", flush=True)
    else:
        print(f"  FAIL  rc={proc.returncode}  ({elapsed:.0f}s)", flush=True)
    return {
        "system": name, "throughput": throughput, "max_rss_kb": rss,
        "sda6_bytes_during_load": sda6_bytes, "duration_s": elapsed,
        "returncode": proc.returncode,
    }


def main() -> int:
    TMPFS_ROOT.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    plan = [
        ("ozonedb",  TMPFS_ROOT / "ozonedb"),
        ("rocksdb",  TMPFS_ROOT / "rocksdb"),
        ("trunkcpp", TMPFS_ROOT / "trunkcpp.db"),
        ("bcw2",     TMPFS_ROOT / "bcw2.db"),
        ("hctree",   TMPFS_ROOT / "hctree.db"),
    ]

    csv = RESULTS_DIR / "load_tmpfs_results.csv"
    with csv.open("w") as f:
        f.write("system,throughput,max_rss_kb,sda6_bytes_during_load,duration_s,returncode\n")
    for name, work in plan:
        r = run_one(name, work)
        with csv.open("a") as f:
            f.write(f"{r['system']},{r['throughput'] or ''},{r['max_rss_kb'] or ''},"
                    f"{r['sda6_bytes_during_load']},{r['duration_s']:.2f},{r['returncode']}\n")
    print(f"\ntmpfs-load summary: {csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
