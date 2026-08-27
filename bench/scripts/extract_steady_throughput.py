#!/usr/bin/env python3
"""Steady-state throughput from a strict-mode sweep result directory.

Why not the aggregate files: YCSB's Throughput(ops/sec) is
total_ops / run_time, and the OzoneDB linearizable path spends the first
~45-90s of every run replaying the shared log (the tailer catch-up) before
the first fenced read returns. That dead time drags the whole-run average
down and is not steady-state throughput. Cassandra has no such warmup.

So this reads the per-writer YCSB result files, which carry one status line
per second (status.interval=1):

    ... 120 sec: 69510 operations; 1124 current ops/sec; est completion ...

and averages `current ops/sec` over the LAST --window seconds of each
writer's run -- past any catch-up for OzoneDB, and steady for Cassandra.
Per-writer steady rates are summed into the cell's throughput. This is a
rate, so the two systems' different cell durations do not matter.

Per-writer files are named (multi-node runner):
  {ks}-{opcnt}-{rc}-workload{wl}-{label}_w{idx}of{total}_t{thread}_trial{trial}.result

Usage:
  extract_steady_throughput.py <results_dir> [--window 60] [--tsv out.tsv]
"""
import argparse
import os
import re
import sys
from collections import defaultdict

STATUS_RE = re.compile(r"(\d+) sec: \d+ operations; ([\d.]+) current ops/sec")
FAILED_RE = re.compile(r"\[(READ|UPDATE|INSERT|READ-MODIFY-WRITE)-FAILED\], Count=(\d+)")
PERWRITER_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<label>.+?)_w(?P<widx>\d+)of(?P<total>\d+)_t(?P<thread>\d+)"
    r"_trial(?P<trial>\d+)\.result$"
)


def writer_steady(path, window):
    """Mean current-ops/sec over the last `window` seconds, and total FAILED ops."""
    samples = []  # (elapsed_sec, current_ops_sec)
    failed = 0
    with open(path, errors="replace") as f:
        for line in f:
            m = STATUS_RE.search(line)
            if m:
                samples.append((int(m.group(1)), float(m.group(2))))
            fm = FAILED_RE.search(line)
            if fm:
                failed += int(fm.group(2))
    if not samples:
        return None, failed, 0
    last_t = samples[-1][0]
    cutoff = last_t - window
    win = [ops for (t, ops) in samples if t >= cutoff]
    if not win:
        win = [samples[-1][1]]
    return sum(win) / len(win), failed, last_t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results_dir")
    ap.add_argument("--window", type=int, default=60, help="trailing seconds to average (default 60)")
    ap.add_argument("--tsv", help="also write a TSV here")
    args = ap.parse_args()

    if not os.path.isdir(args.results_dir):
        sys.exit(f"no such dir: {args.results_dir}")

    # cell key -> {writers: {widx: rate}, failed: n, total: N, minlast: sec}
    cells = defaultdict(lambda: {"writers": {}, "failed": 0, "total": None, "minlast": 1 << 30})
    for fn in sorted(os.listdir(args.results_dir)):
        m = PERWRITER_RE.match(fn)
        if not m:
            continue
        rate, failed, last_t = writer_steady(os.path.join(args.results_dir, fn), args.window)
        if rate is None:
            continue
        key = (m.group("label"), m.group("wl"), int(m.group("total")), m.group("trial"))
        c = cells[key]
        c["writers"][int(m.group("widx"))] = rate
        c["failed"] += failed
        c["total"] = int(m.group("total"))
        c["minlast"] = min(c["minlast"], last_t)

    rows = []
    for (label, wl, total, trial), c in cells.items():
        present = len(c["writers"])
        cell_tps = sum(c["writers"].values())
        rows.append((label, wl, total, present, cell_tps, c["failed"], c["minlast"]))
    rows.sort(key=lambda r: (r[0], r[1], r[2]))

    hdr = f"{'label':32} {'wl':3} {'writers':7} {'have':4} {'steady_ops_s':13} {'failed':7} {'run_s':5}"
    print(hdr)
    print("-" * len(hdr))
    for label, wl, total, present, tps, failed, last in rows:
        flag = "" if present == total else f"  <-- MISSING {total - present}/{total}"
        print(f"{label:32} {wl:3} {total:7} {present:4} {tps:13.1f} {failed:7} {last:5}{flag}")

    if args.tsv:
        with open(args.tsv, "w") as f:
            f.write("label\tworkload\twriters\thave\tsteady_ops_per_sec\tfailed\trun_seconds\n")
            for label, wl, total, present, tps, failed, last in rows:
                f.write(f"{label}\t{wl}\t{total}\t{present}\t{tps:.1f}\t{failed}\t{last}\n")
        print(f"\nwrote {args.tsv}")


if __name__ == "__main__":
    main()
