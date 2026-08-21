#!/usr/bin/env python3
"""Multi-node YCSB throughput face-off: cr-sqlite (async), cr-sqlite-syncread
(fenced reads), ozonedb-corfu (default reads), ozonedb-corfu-linearizable.

Reads the multinode aggregate + per-writer .result files produced by
`bench.py multinode` (crsqlite) and run_multinode_ycsb_with_corfu.sh
(ozonedb) from any number of result dirs, groups by (workload, engine), and
draws two panels: SumWriterThroughput and op-weighted READ latency.

Engine pairing for the paper (PLAN-syncread.md): cr-sqlite <-> ozonedb
default (both eventually-visible reads); cr-sqlite-syncread <-> ozonedb
--linearizable (both freshness-guaranteed reads).

Usage:
    python3 plot_throughput.py <result-dirs...> [--out NAME]

Example (crsqlite repo root, ozonedb checkout alongside):
    python3 bench/scripts/plot/plot_throughput.py \
        bench/results/local/p3-run-* bench/results/local/p3sr-run-* \
        ../ozonedb/bench/results/local/20260820-131808
"""

import argparse
import os
import re
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# Ordered: the async pair first, then the freshness-guaranteed pair.
ENGINES = [
    "crsqlite",
    "ozonedb-corfu",
    "crsqlite-syncread",
    "ozonedb-corfu-linearizable",
]
ENGINE_LABELS = {
    "crsqlite": "cr-sqlite (async reads)",
    "crsqlite-syncread": "cr-sqlite (fenced reads)",
    "ozonedb-corfu": "ozonedb (default reads)",
    "ozonedb-corfu-linearizable": "ozonedb (linearizable reads)",
}
ENGINE_COLORS = {
    "crsqlite": "#DD8452",
    "crsqlite-syncread": "#C44E52",
    "ozonedb-corfu": "#8CB4E1",
    "ozonedb-corfu-linearizable": "#4C72B0",
}

AGG_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<db>.+?)_agg_multinode_w(?P<total>\d+)_t\d+_trial(?P<trial>\d+)\.result$"
)
PERWRITER_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<db>.+?)_w(?P<widx>\d+)of(?P<total>\d+)_t\d+_trial(?P<trial>\d+)\.result$"
)


def parse_kv(path):
    """{(tag, key): value} from a YCSB-style '[TAG], Key, Value' file."""
    out = {}
    with open(path) as f:
        for line in f:
            parts = [p.strip() for p in line.strip().split(",")]
            if len(parts) >= 3 and parts[0].startswith("["):
                try:
                    out[(parts[0], parts[1])] = float(parts[2])
                except ValueError:
                    pass
    return out


def collect(dirs):
    """{(wl, engine): {"tput": x, "read_ms": y, "update_ms": z, ...}}"""
    data = defaultdict(dict)
    lat_acc = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))  # (wl,db)->op->[ops,us*ops]
    for d in dirs:
        for fn in sorted(os.listdir(d)):
            path = os.path.join(d, fn)
            if not os.path.isfile(path):
                continue
            m = AGG_RE.match(fn)
            if m:
                kv = parse_kv(path)
                t = kv.get(("[AGGREGATE]", "SumWriterThroughput(ops/sec)"))
                if t is not None:
                    data[(m.group("wl"), m.group("db"))]["tput"] = t
                continue
            m = PERWRITER_RE.match(fn)
            if m:
                kv = parse_kv(path)
                for op in ("READ", "UPDATE", "INSERT"):
                    ops = kv.get((f"[{op}]", "Operations"))
                    avg = kv.get((f"[{op}]", "AverageLatency(us)"))
                    if ops and avg:
                        acc = lat_acc[(m.group("wl"), m.group("db"))][op]
                        acc[0] += ops
                        acc[1] += ops * avg
    for key, per_op in lat_acc.items():
        for op, (ops, us) in per_op.items():
            if ops:
                data[key][op.lower() + "_ms"] = (us / ops) / 1000.0
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--out", default="throughput_faceoff")
    args = ap.parse_args()

    data = collect(args.dirs)
    if not data:
        sys.exit("no multinode aggregate files found in the given dirs")
    workloads = sorted({wl for wl, _ in data})
    engines = [e for e in ENGINES if any((wl, e) in data for wl in workloads)]
    print(f"workloads={workloads} engines={engines}")
    for wl in workloads:
        for e in engines:
            if (wl, e) in data:
                print(f"  workload {wl:>2} {e:<28} {data[(wl, e)]}")

    fig, (ax_t, ax_l) = plt.subplots(1, 2, figsize=(11, 4.2))
    x = np.arange(len(workloads))
    width = 0.8 / len(engines)

    for ax, field, title, ylabel in (
        (ax_t, "tput", "Throughput (2 writers, 1 per node, 300 s)",
         "Sum writer throughput (ops/s)"),
        (ax_l, "read_ms", "Read latency", "Avg READ latency (ms)"),
    ):
        for i, e in enumerate(engines):
            xs, ys = [], []
            for j, wl in enumerate(workloads):
                v = data.get((wl, e), {}).get(field)
                if v is not None:
                    xs.append(x[j] + (i - (len(engines) - 1) / 2) * width)
                    ys.append(v)
            bars = ax.bar(xs, ys, width * 0.92, color=ENGINE_COLORS[e],
                          label=ENGINE_LABELS[e])
            for b, v in zip(bars, ys):
                txt = f"{v:,.0f}" if field == "tput" else (
                    f"{v * 1000:.0f}µs" if v < 0.1 else f"{v:.2g}ms")
                ax.annotate(txt, (b.get_x() + b.get_width() / 2, v),
                            ha="center", va="bottom", fontsize=7, rotation=0)
        ax.set_yscale("log")
        ax.set_xticks(x)
        ax.set_xticklabels([f"workload {w}" for w in workloads])
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=10)
        ax.grid(axis="y", alpha=0.3)
        ax.margins(y=0.18)
    ax_t.legend(fontsize=8, loc="upper left")

    fig.suptitle(
        "YCSB multi-node: matched read guarantees "
        "(async ↔ default, fenced ↔ linearizable)",
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("png", "pdf"):
        p = os.path.join(out_dir, f"{args.out}.{ext}")
        fig.savefig(p, dpi=150)
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
