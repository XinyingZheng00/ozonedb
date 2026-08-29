#!/usr/bin/env python3
"""Per-cell summary of one or more result tags (PLAN-cost-2 checkpoints).
  summarize.py TAGDIR [TAGDIR ...]   -- runs the extractor, prints one line per cell
"""
import csv
import os
import subprocess
import sys

home = os.environ.get("OZONEDB_HOME", "/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2")
tsv = os.path.join(home, "bench/results/local/cost2-chains/summary.tsv")
dirs = sys.argv[1:]
subprocess.run([sys.executable, os.path.join(home, "bench/scripts/extract_cost_coefficients.py"),
                *dirs, "--window", "60", "--tsv", tsv], check=True, stdout=subprocess.DEVNULL)
rows = list(csv.DictReader(open(tsv), delimiter="\t"))

def f(v, fmt="{:.3f}"):
    try:
        return fmt.format(float(v))
    except (TypeError, ValueError):
        return "-"

cols = ["tag", "label", "wl", "w", "have", "ops/s", "failed", "h", "h_st", "fill", "get/op", "get/op_st", "cpu_ms", "rss_MB",
        "disk_ratio", "d_fill", "disk_h", "d_amp", "fill/op", "replay_ms"]
print("\t".join(cols))
for r in sorted(rows, key=lambda r: (r["tag"], r["workload"], r["label"], int(r["writers"] or 0))):
    print("\t".join([
        r["tag"][-12:], r["label"].replace("ozonedb-corfu-", "oz-"), r["workload"], r["writers"], r["have"],
        f(r["steady_ops_s"], "{:.0f}"), r["failed"], f(r["h"]), f(r["h_steady"]), f(r.get("cache_fill"), "{:.2f}"),
        f(r["get_per_op"], "{:.4f}"), f(r["get_per_op_steady"], "{:.4f}"),
        f(float(r["client_cpu_s_per_op"]) * 1000 if r["client_cpu_s_per_op"] else None, "{:.3f}"),
        f(float(r["max_rss_kb"]) / 1024 if r.get("max_rss_kb") else None, "{:.0f}"),
        f(r.get("disk_ratio")),
        f(float(r["disk_bytes"]) / float(r["disk_capacity"]) if r.get("disk_bytes") and r.get("disk_capacity") and float(r["disk_capacity"]) > 0 else None, "{:.2f}"),
        f(r.get("disk_h")), f(r.get("disk_amp"), "{:.1f}"),
        f(r.get("disk_fill_gets_per_op"), "{:.4f}"), f(r.get("replay_ms"), "{:.0f}"),
    ]))
