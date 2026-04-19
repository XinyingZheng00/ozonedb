#!/usr/bin/env python3
"""
Figure 2. Multi-writer thread scaling

Plots aggregate throughput (ops/s) and workload-weighted average latency (ms)
vs. number of YCSB client threads on one node.

Expects raw YCSB result files named like:
  <key>-dur<sec>s-<record>-workload<letter>-<system>_t<threads>.result
Example:
  1KB-dur300s-1000000-workloada-ozonedb_t8.result

Systems (slug before _t): ozonedb, hctree, rocksdb, or rocksdb-sharing-dbinstance
  (the last two are merged into one RocksDB series).
Display: OzoneDB, hctree, RocksDB (shared DB instance)
"""

from __future__ import annotations

import argparse
import math
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

plt.rcParams.update(
    {
        "font.size": 14,
        "axes.labelsize": 14,
        "axes.titlesize": 14,
        "xtick.labelsize": 12,
        "ytick.labelsize": 12,
        "legend.fontsize": 12,
        "figure.titlesize": 15,
    }
)

THREAD_ORDER = [1, 2, 4, 8, 16, 32]

SYSTEMS = ("ozonedb", "hctree", "rocksdb")
ROCKSDB_SLUGS = frozenset({"rocksdb", "rocksdb-sharing-dbinstance"})

SYSTEM_LABEL = {
    "ozonedb": "OzoneDB",
    "hctree": "hctree",
    "rocksdb": "RocksDB (shared DB instance)",
}

COLORS = {
    "ozonedb": "#1f77b4",
    "hctree": "#9467bd",
    "rocksdb": "#ff7f0e",
}

MARKERS = {"ozonedb": "o", "hctree": "s", "rocksdb": "^"}


def parse_ycsb_result(path: Path) -> tuple[float, float]:
    """
    Return (throughput_mean_ops_s, weighted_avg_latency_ms) from a YCSB text result.
    If multiple [OVERALL] blocks (e.g. repeated rounds), averages throughput and latency across rounds.
    """
    text = path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    throughput_pattern = re.compile(r"^\[OVERALL\], Throughput\(ops/sec\),\s*([0-9.]+)")
    op_count_pattern = re.compile(r"^\[([A-Z\-]+)\], Operations,\s*(\d+)")
    op_avg_pattern = re.compile(r"^\[([A-Z\-]+)\], AverageLatency\(us\),\s*([0-9.]+)")

    throughput_values: list[float] = []
    latency_values_ms: list[float] = []
    current_op_count_by_name: dict[str, int] = {}
    current_weighted_latency_sum_us = 0.0
    current_total_ops = 0

    for line in lines:
        m = throughput_pattern.match(line)
        if m:
            if throughput_values and current_total_ops > 0:
                latency_values_ms.append((current_weighted_latency_sum_us / current_total_ops) / 1000.0)
            throughput_values.append(float(m.group(1)))
            current_op_count_by_name = {}
            current_weighted_latency_sum_us = 0.0
            current_total_ops = 0
            continue

        m = op_count_pattern.match(line)
        if m:
            op = m.group(1)
            current_op_count_by_name[op] = int(m.group(2))
            continue

        m = op_avg_pattern.match(line)
        if m:
            op = m.group(1)
            if op == "CLEANUP":
                continue
            op_count = current_op_count_by_name.get(op)
            if op_count is None:
                continue
            avg_latency_us = float(m.group(2))
            current_weighted_latency_sum_us += op_count * avg_latency_us
            current_total_ops += op_count

    if throughput_values and current_total_ops > 0:
        latency_values_ms.append((current_weighted_latency_sum_us / current_total_ops) / 1000.0)

    thr = float(np.mean(throughput_values)) if throughput_values else math.nan
    lat = float(np.mean(latency_values_ms)) if latency_values_ms else math.nan
    return thr, lat


def collect_series(
    results_dir: Path,
    workload_letter: str,
) -> dict[str, dict[int, tuple[float, float]]]:
    """
    Map system -> thread_count -> (throughput, latency_ms).
    """
    out: dict[str, dict[int, tuple[float, float]]] = {s: {} for s in SYSTEMS}
    wl = workload_letter.lower()
    suffix_re = re.compile(rf"-workload{re.escape(wl)}-([^_]+)_t(\d+)\.result$")

    for path in results_dir.iterdir():
        if not path.is_file() or path.suffix != ".result":
            continue
        m = suffix_re.search(path.name)
        if not m:
            continue
        slug = m.group(1)
        threads = int(m.group(2))
        if slug in ROCKSDB_SLUGS:
            system = "rocksdb"
        elif slug in SYSTEMS:
            system = slug
        else:
            continue
        thr, lat = parse_ycsb_result(path)
        out[system][threads] = (thr, lat)
    return out


def plot_figure(
    series: dict[str, dict[int, tuple[float, float]]],
    workload_letter: str,
    output_path: Path,
) -> None:
    fig, (ax_thr, ax_lat) = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
    fig.suptitle(
        "Figure 3. Single-node thread scaling\n"
        "Throughput and average latency vs. YCSB client threads (single node)",
        fontsize=14,
    )

    x = np.arange(len(THREAD_ORDER))
    xtick_labels = [str(t) for t in THREAD_ORDER]

    for system in SYSTEMS:
        ys_thr: list[float] = []
        ys_lat: list[float] = []
        for t in THREAD_ORDER:
            pair = series[system].get(t)
            if pair is None or math.isnan(pair[0]):
                ys_thr.append(math.nan)
                ys_lat.append(math.nan)
            else:
                ys_thr.append(pair[0])
                ys_lat.append(pair[1])

        ax_thr.plot(
            x,
            ys_thr,
            marker=MARKERS[system],
            color=COLORS[system],
            linewidth=2,
            label=SYSTEM_LABEL[system],
        )
        ax_lat.plot(
            x,
            ys_lat,
            marker=MARKERS[system],
            color=COLORS[system],
            linewidth=2,
            label=SYSTEM_LABEL[system],
        )

    for ax in (ax_thr, ax_lat):
        ax.set_xticks(x)
        ax.set_xticklabels(xtick_labels)
        ax.set_xlabel("Threads (YCSB client)")
        ax.grid(axis="y", linestyle="--", alpha=0.45)

    ax_thr.set_ylabel("Aggregate throughput (ops/s)")
    ax_thr.set_title(f"Throughput (YCSB workload {workload_letter})")

    ax_lat.set_ylabel("Average latency (ms)")
    ax_lat.set_title("Workload-weighted avg latency")

    handles, labels = ax_thr.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False, bbox_to_anchor=(0.5, -0.02))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Wrote {output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "results" / "local" / "fig2-multi-writer-local",
        help="Directory containing *_workload<l>-<system>_t<N>.result files.",
    )
    parser.add_argument(
        "--workload",
        type=str,
        default="a",
        help="Workload letter in filenames (e.g. a for workloada).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[2]
        / "results"
        / "local"
        / "fig2-multi-writer-local"
        / "figure3_thread_scaling.pdf",
        help="Output PDF path.",
    )
    args = parser.parse_args()

    wl = args.workload.strip().lower()
    if len(wl) != 1 or not wl.isalpha():
        raise SystemExit(f"Invalid --workload {args.workload!r}; use a single letter like a")

    if not args.results_dir.is_dir():
        raise SystemExit(f"Results directory not found: {args.results_dir}")

    series = collect_series(args.results_dir, wl)
    for system in SYSTEMS:
        n = len(series[system])
        if n == 0:
            print(f"Warning: no result files for system {system!r} under {args.results_dir}", flush=True)
        else:
            print(f"{system}: {n} thread points -> {sorted(series[system].keys())}")

    plot_figure(series, wl, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
