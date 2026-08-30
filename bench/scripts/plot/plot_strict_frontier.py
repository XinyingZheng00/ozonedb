#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
# ]
# ///
"""Throughput against writer count at the strict point of the consistency frontier.

Reads the TSV that extract_steady_throughput.py writes, NOT the aggregate
result files. The aggregate files carry YCSB's Throughput(ops/sec), which is
total_ops divided by the orchestrator wall time and therefore includes SSH,
JVM start and the OzoneDB tailer catch-up. The TSV carries the steady-state
rate over a trailing window, which is the number this campaign reports.

The TSV has no trial column: the extractor keys a cell by trial but does not
write the trial out, so three trials give three rows that share label, workload
and writers. This script groups on those three columns. The marker is the mean
and the error bar is the full range over the trials of that cell.

Usage:
    uv run plot_strict_frontier.py <tsv> [--output out.png] [--title TITLE]
"""

import argparse
import csv
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Panel order. d sits last because it is the only workload that inserts keys.
WORKLOAD_ORDER = ["a", "b", "c", "f", "d"]
WORKLOAD_TITLE = {
    "a": "a: 50 % read, 50 % update",
    "b": "b: 95 % read, 5 % update",
    "c": "c: 100 % read",
    "f": "f: read-modify-write",
    "d": "d: 95 % read-latest, 5 % insert",
}

# A label is matched by the token it starts with, so the long OzoneDB label
# does not have to be repeated here.
SERIES = [
    ("cassandra-serial", "Cassandra serial", "#d1495b", "o"),
    ("ozonedb-corfu-linearizable", "OzoneKV linearizable", "#00798c", "s"),
]


def load(path):
    """tsv -> {(series_key, workload, writers): [rate, ...]}"""
    grouped = defaultdict(list)
    labels_seen = set()
    with open(path, newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            label = row["label"]
            labels_seen.add(label)
            key = None
            for prefix, _, _, _ in SERIES:
                if label.startswith(prefix):
                    key = prefix
                    break
            if key is None:
                continue
            grouped[(key, row["workload"], int(row["writers"]))].append(
                float(row["steady_ops_per_sec"])
            )
    return grouped, labels_seen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tsv")
    ap.add_argument("--output", default="bench/strict-frontier-100g.png")
    ap.add_argument("--title", default="Strict consistency, 100 GB dataset")
    args = ap.parse_args()

    grouped, labels_seen = load(args.tsv)
    if not grouped:
        sys.exit(f"no known series in {args.tsv}; labels were: {sorted(labels_seen)}")

    workloads = [w for w in WORKLOAD_ORDER if any(k[1] == w for k in grouped)]
    fig, axes = plt.subplots(1, len(workloads), figsize=(4.0 * len(workloads), 4.2), sharex=True)
    if len(workloads) == 1:
        axes = [axes]

    for ax, wl in zip(axes, workloads):
        for prefix, nice, colour, marker in SERIES:
            pts = sorted((k[2], v) for k, v in grouped.items() if k[0] == prefix and k[1] == wl)
            if not pts:
                continue
            xs = [w for w, _ in pts]
            means = [sum(v) / len(v) for _, v in pts]
            lo = [m - min(v) for m, (_, v) in zip(means, pts)]
            hi = [max(v) - m for m, (_, v) in zip(means, pts)]
            ax.errorbar(
                xs, means, yerr=[lo, hi], label=nice, color=colour, marker=marker,
                capsize=3, linewidth=1.8, markersize=5,
            )
        ax.set_xscale("log", base=2)
        ax.set_xticks([2, 4, 8, 16, 32])
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_yscale("log")
        ax.set_title(WORKLOAD_TITLE.get(wl, wl), fontsize=10)
        ax.set_xlabel("writer processes")
        ax.grid(True, which="both", alpha=0.25, linewidth=0.5)

    axes[0].set_ylabel("steady-state throughput (ops/s)")
    axes[0].legend(fontsize=9, loc="upper left")
    fig.suptitle(args.title, fontsize=12)
    fig.tight_layout()
    fig.savefig(args.output, dpi=160, bbox_inches="tight")
    print(f"wrote {args.output}")

    # The ratio table is what the write-up quotes, so print it here too.
    print("\nCassandra / OzoneKV, mean over trials")
    print("workload  writers      cass     ozone   ratio")
    for wl in workloads:
        for w in sorted({k[2] for k in grouped if k[1] == wl}):
            c = grouped.get(("cassandra-serial", wl, w))
            o = grouped.get(("ozonedb-corfu-linearizable", wl, w))
            if not c or not o:
                continue
            cm, om = sum(c) / len(c), sum(o) / len(o)
            print(f"{wl:>8}  {w:>7}  {cm:>8.0f}  {om:>8.0f}  {cm / om:>6.2f}")


if __name__ == "__main__":
    main()
