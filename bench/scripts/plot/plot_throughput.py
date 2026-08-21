#!/usr/bin/env python3
"""Multi-node YCSB figures: cr-sqlite (async), cr-sqlite-syncread (fenced
reads), ozonedb-corfu (default reads), ozonedb-corfu-linearizable.

Reads multinode aggregate + per-writer .result files produced by
`bench.py multinode` (crsqlite) and run_multinode_ycsb_with_corfu.sh
(ozonedb) from any number of result dirs. When a cross-node aggregate is
missing (a laptop-side orchestrator hang killed the fetch but the writers
finished), SumWriterThroughput is rebuilt as the sum of per-writer
[OVERALL] Throughput -- identical arithmetic to the aggregate writer.

ozonedb linearizable runs carry the same db token as default runs, so tag
their result dirs with --relabel DIR=ozonedb-corfu-linearizable.

Modes:
  faceoff (default): grouped bars, workloads x engines, at --writers N.
  scaling:           SumWriterThroughput vs total writers, per workload,
                     plus fenced-read barrier p50/p99 vs writer count.

Engine pairing (PLAN-syncread.md): cr-sqlite <-> ozonedb default (both
eventually-visible reads); cr-sqlite-syncread <-> ozonedb --linearizable
(both freshness-guaranteed reads).

Usage:
    python3 plot_throughput.py <result-dirs...> [--mode faceoff|scaling]
        [--writers N] [--relabel DIR=ENGINE ...] [--out NAME]
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
ENGINE_MARKERS = {
    "crsqlite": "o",
    "crsqlite-syncread": "s",
    "ozonedb-corfu": "^",
    "ozonedb-corfu-linearizable": "D",
}

AGG_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<db>.+?)_agg_multinode_w(?P<total>\d+)_t\d+_trial(?P<trial>\d+)\.result$"
)
PERWRITER_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<db>.+?)_w(?P<widx>\d+)of(?P<total>\d+)_t\d+_trial(?P<trial>\d+)\.result$"
)
# Per-host aggregates carry the [SYNCREAD] barrier stats.
HOSTAGG_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<db>.+?)_agg_w\d+of(?P<total>\d+)_off\d+_t\d+_trial(?P<trial>\d+)\.result$"
)


def parse_kv(path):
    """{(tag, key): value} from a YCSB-style '[TAG], Key, Value' file.
    Also returns the [SYNCREAD] WriterNBarrierP50/P99 values as a list."""
    out = {}
    p50s, p99s = [], []
    with open(path) as f:
        for line in f:
            parts = [p.strip() for p in line.strip().split(",")]
            if len(parts) >= 3 and parts[0].startswith("["):
                try:
                    v = float(parts[2])
                except ValueError:
                    continue
                out[(parts[0], parts[1])] = v
                if parts[0] == "[SYNCREAD]":
                    if "BarrierP50" in parts[1]:
                        p50s.append(v)
                    elif "BarrierP99" in parts[1]:
                        p99s.append(v)
    return out, p50s, p99s


def collect(dirs, relabel):
    """{(wl, engine, total): {"tput", "read_ms", "update_ms",
    "barrier_p50", "barrier_p99"}}"""
    agg_tput = {}
    writer_tput = defaultdict(dict)   # key -> widx -> ops/s
    lat_acc = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
    barrier = defaultdict(lambda: ([], []))

    def engine_of(d, db):
        return relabel.get(os.path.abspath(d), db)

    for d in dirs:
        if not os.path.isdir(d):
            print(f"[collect] skipping missing dir {d}")
            continue
        for fn in sorted(os.listdir(d)):
            path = os.path.join(d, fn)
            if not os.path.isfile(path):
                continue
            m = AGG_RE.match(fn)
            if m:
                kv, _, _ = parse_kv(path)
                t = kv.get(("[AGGREGATE]", "SumWriterThroughput(ops/sec)"))
                if t is not None:
                    key = (m.group("wl"), engine_of(d, m.group("db")),
                           int(m.group("total")))
                    agg_tput[key] = t
                continue
            m = HOSTAGG_RE.match(fn)
            if m:
                _, p50s, p99s = parse_kv(path)
                key = (m.group("wl"), engine_of(d, m.group("db")),
                       int(m.group("total")))
                barrier[key][0].extend(p50s)
                barrier[key][1].extend(p99s)
                continue
            m = PERWRITER_RE.match(fn)
            if m:
                kv, _, _ = parse_kv(path)
                key = (m.group("wl"), engine_of(d, m.group("db")),
                       int(m.group("total")))
                t = kv.get(("[OVERALL]", "Throughput(ops/sec)"))
                if t is not None:
                    writer_tput[key][int(m.group("widx"))] = t
                for op in ("READ", "UPDATE", "INSERT"):
                    ops = kv.get((f"[{op}]", "Operations"))
                    avg = kv.get((f"[{op}]", "AverageLatency(us)"))
                    if ops and avg:
                        acc = lat_acc[key][op]
                        acc[0] += ops
                        acc[1] += ops * avg

    data = defaultdict(dict)
    for key, per_w in writer_tput.items():
        total = key[2]
        if len(per_w) != total:
            print(f"[collect] WARNING {key}: {len(per_w)}/{total} per-writer "
                  "files -- computed sum UNDER-REPORTS")
        data[key]["tput"] = sum(per_w.values())
        data[key]["tput_source"] = "per-writer-sum"
    for key, t in agg_tput.items():
        data[key]["tput"] = t
        data[key]["tput_source"] = "aggregate"
    for key, per_op in lat_acc.items():
        for op, (ops, us) in per_op.items():
            if ops:
                data[key][op.lower() + "_ms"] = (us / ops) / 1000.0
    for key, (p50s, p99s) in barrier.items():
        if p50s:
            data[key]["barrier_p50"] = sum(p50s) / len(p50s)
        if p99s:
            data[key]["barrier_p99"] = max(p99s)
    return data


def plot_faceoff(data, writers, out):
    workloads = sorted({k[0] for k in data if k[2] == writers})
    engines = [e for e in ENGINES
               if any((wl, e, writers) in data for wl in workloads)]
    fig, (ax_t, ax_l) = plt.subplots(1, 2, figsize=(11.5, 4.2))
    x = np.arange(len(workloads))
    width = 0.8 / max(1, len(engines))
    for ax, field, title, ylabel in (
        (ax_t, "tput", f"Throughput ({writers} writers, 300 s)",
         "Sum writer throughput (ops/s)"),
        (ax_l, "read_ms", "Read latency", "Avg READ latency (ms)"),
    ):
        for i, e in enumerate(engines):
            xs, ys = [], []
            for j, wl in enumerate(workloads):
                v = data.get((wl, e, writers), {}).get(field)
                if v is not None:
                    xs.append(x[j] + (i - (len(engines) - 1) / 2) * width)
                    ys.append(v)
            bars = ax.bar(xs, ys, width * 0.92, color=ENGINE_COLORS[e],
                          label=ENGINE_LABELS[e])
            for b, v in zip(bars, ys):
                txt = f"{v:,.0f}" if field == "tput" else (
                    f"{v * 1000:.0f}µs" if v < 0.1 else f"{v:.2g}ms")
                ax.annotate(txt, (b.get_x() + b.get_width() / 2, v),
                            ha="center", va="bottom", fontsize=6.5)
        ax.set_yscale("log")
        ax.set_xticks(x)
        ax.set_xticklabels([f"workload {w}" for w in workloads])
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=10)
        ax.grid(axis="y", alpha=0.3)
        ax.margins(y=0.2)
    ax_t.legend(fontsize=7.5, loc="upper left")
    fig.suptitle(
        "YCSB multi-node: matched read guarantees "
        "(async ↔ default, fenced ↔ linearizable)", fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    return fig


def plot_scaling(data, out):
    workloads = sorted({k[0] for k in data})
    totals = sorted({k[2] for k in data})
    engines = [e for e in ENGINES if any(k[1] == e for k in data)]
    ncols = len(workloads) + 1
    fig, axes = plt.subplots(1, ncols, figsize=(3.5 * ncols, 3.6))
    for ax, wl in zip(axes, workloads):
        for e in engines:
            xs = [t for t in totals if (wl, e, t) in data
                  and "tput" in data[(wl, e, t)]]
            ys = [data[(wl, e, t)]["tput"] for t in xs]
            if not xs:
                continue
            ax.plot(xs, ys, marker=ENGINE_MARKERS[e], color=ENGINE_COLORS[e],
                    label=ENGINE_LABELS[e], linewidth=1.6, markersize=5)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xticks(totals)
        ax.set_xticklabels([str(t) for t in totals])
        ax.set_xlabel("total writers")
        ax.set_title(f"workload {wl}", fontsize=10)
        ax.grid(alpha=0.3)
    axes[0].set_ylabel("Sum writer throughput (ops/s)")
    axes[0].legend(fontsize=6.5, loc="best")

    # Barrier-cost panel (B1): fenced-read round cost vs writer count.
    axb = axes[-1]
    for wl, style in zip(workloads, ("-", "--", ":")):
        e = "crsqlite-syncread"
        xs = [t for t in totals if (wl, e, t) in data
              and "barrier_p50" in data[(wl, e, t)]]
        if not xs:
            continue
        axb.plot(xs, [data[(wl, e, t)]["barrier_p50"] for t in xs],
                 marker="s", linestyle=style, color=ENGINE_COLORS[e],
                 label=f"p50, workload {wl}", linewidth=1.4, markersize=4)
        axb.plot(xs, [data[(wl, e, t)].get("barrier_p99") for t in xs],
                 marker="s", linestyle=style, color="#8B3A3E", alpha=0.55,
                 label=f"p99, workload {wl}", linewidth=1.0, markersize=3)
    axb.set_xscale("log", base=2)
    axb.set_xticks(totals)
    axb.set_xticklabels([str(t) for t in totals])
    axb.set_xlabel("total writers")
    axb.set_ylabel("barrier round (ms)")
    axb.set_title("fenced-read barrier cost", fontsize=10)
    axb.grid(alpha=0.3)
    axb.legend(fontsize=5.5, loc="best")

    fig.suptitle("YCSB multi-node scaling (2 client nodes, 1 KB, 1M keys, 300 s)",
                 fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    return fig


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--mode", choices=("faceoff", "scaling"), default="faceoff")
    ap.add_argument("--writers", type=int, default=2,
                    help="faceoff mode: total writer count to plot")
    ap.add_argument("--relabel", action="append", default=[],
                    metavar="DIR=ENGINE",
                    help="override the engine for every file in DIR "
                         "(e.g. a linearizable ozonedb result dir)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    relabel = {}
    for r in args.relabel:
        d, _, e = r.partition("=")
        if not e:
            sys.exit(f"bad --relabel {r!r}, expected DIR=ENGINE")
        relabel[os.path.abspath(d)] = e

    data = collect(args.dirs, relabel)
    if not data:
        sys.exit("no result files found in the given dirs")
    for key in sorted(data):
        print(f"  {key}: {data[key]}")

    if args.mode == "faceoff":
        fig = plot_faceoff(data, args.writers, args.out)
        out = args.out or "throughput_faceoff"
    else:
        fig = plot_scaling(data, args.out)
        out = args.out or "throughput_scaling"

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("png", "pdf"):
        p = os.path.join(out_dir, f"{out}.{ext}")
        fig.savefig(p, dpi=150)
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
