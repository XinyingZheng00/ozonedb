#!/usr/bin/env python3
"""Three Phase-1 paper-style plots.

  (a) fig2_multi_process_bars.pdf   — multi-process bar chart, format mirrors
                                       fig2-multi-writer-local/fig2_multi_writer.pdf
                                       (2x3 grid, grouped bars per system per N)
  (b) multi_thread_lines.pdf        — multi-thread scaling line chart
                                       (2x3 grid, x = T, lines per system)
  (c) multi_process_lines.pdf       — multi-process scaling line chart
                                       (2x3 grid, x = N, lines per system)

Three systems: ozonedb, trunkcpp, bcw2 (matches the user-requested subset).
Five workloads: a, b, c, d, f (one panel each + 1 legend panel).

Reads:
  --thread-csv  fig2-multi-writer-local/results.csv          (multi-thread)
  --proc-csv    phase1-multi-process/results.csv             (multi-process, shared DB)
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter


def _sci_fmt(x: float, pos: int) -> str:
    """Format y-tick as e.g. 2x10^5; collapses the long-zero counts."""
    if x == 0:
        return "0"
    exp = int(np.floor(np.log10(abs(x))))
    mantissa = x / (10 ** exp)
    if abs(mantissa - round(mantissa)) < 1e-9:
        return rf"${int(round(mantissa))}{{\times}}10^{{{exp}}}$"
    return rf"${mantissa:.1f}{{\times}}10^{{{exp}}}$"

# Phase 1 (multi-process) has 3 systems; multi-thread has all 5.
SYSTEM_ORDER_PROC = ["ozonedb", "trunkcpp", "bcw2"]
SYSTEM_ORDER_THREAD = ["ozonedb", "rocksdb", "trunkcpp", "bcw2", "hctree"]

SYSTEM_LABELS = {
    "ozonedb":  "OzoneKV",
    "rocksdb":  "RocksDB",
    "trunkcpp": "SQLite (trunk, plain WAL)",
    "bcw2":     "SQLite (BEGIN CONCURRENT + WAL2)",
    "hctree":   "SQLite (HCTree)",
}

# Wrapped variants for the in-panel legend so the long bcw2 label
# stays within its cell width.
SYSTEM_LABELS_LEGEND = {
    "ozonedb":  "OzoneKV    ",
    "rocksdb":  "RocksDB",
    "trunkcpp": "SQLite (trunk, plain WAL)",
    "bcw2":     "SQLite (BEGIN CONCURRENT\n+ WAL2)",
    "hctree":   "SQLite (HCTree)",
}

# Match fig2_multi_writer.pdf colors so the figures read as a set.
SYSTEM_COLORS = {
    "ozonedb":  "#1f77b4",  # blue
    "rocksdb":  "#d62728",  # red
    "trunkcpp": "#2ca02c",  # green
    "bcw2":     "#ff7f0e",  # orange
    "hctree":   "#9467bd",  # purple
}

SYSTEM_MARKERS = {"ozonedb": "o", "rocksdb": "s", "trunkcpp": "^",
                  "bcw2": "v", "hctree": "D"}

WORKLOAD_LABELS = {
    "a": "Workload A",
    "b": "Workload B",
    "c": "Workload C",
    "d": "Workload D",
    "f": "Workload F",
}


def load_csv(path: Path, x_col: str) -> dict:
    """rows[system][workload][N] = (mean, stddev)."""
    rows: dict = defaultdict(lambda: defaultdict(dict))
    with path.open() as f:
        for row in csv.DictReader(f):
            sys_ = row.get("system")
            wl = row.get("workload")
            try:
                n = int(row[x_col])
            except (KeyError, ValueError):
                continue
            tput_key = ("agg_throughput_mean" if "agg_throughput_mean" in row
                        else "throughput_mean")
            sd_key = ("agg_throughput_stddev" if "agg_throughput_stddev" in row
                      else "throughput_stddev")
            try:
                m = float(row[tput_key])
            except (KeyError, ValueError):
                continue
            try:
                sd = float(row[sd_key]) if row.get(sd_key) else 0.0
            except ValueError:
                sd = 0.0
            rows[sys_][wl][n] = (m, sd)
    return rows


def plot_bars(rows: dict, x_counts: list[int], title: str, x_label: str,
              out_path: Path, system_order: list[str],
              log_y: bool = True) -> None:
    """Multi-writer bar chart in fig2 format. 1x5 grid, grouped bars per system per N."""
    workloads = ["a", "b", "c", "d", "f"]
    n_sys = len(system_order)
    bar_width = 0.8 / n_sys
    x_idx = np.arange(len(x_counts))

    fig, axes = plt.subplots(1, 5, figsize=(28, 6.5), sharey=True)

    for i, wl in enumerate(workloads):
        ax = axes[i]
        for j, sys_ in enumerate(system_order):
            per_n = rows.get(sys_, {}).get(wl, {})
            means = [per_n.get(n, (np.nan, 0))[0] for n in x_counts]
            sds = [per_n.get(n, (np.nan, 0))[1] for n in x_counts]
            offset = (j - (n_sys - 1) / 2) * bar_width
            display = [max(m, 0.1) if (m is not None and not np.isnan(m)) else np.nan
                       for m in means]
            ax.bar(x_idx + offset, display, bar_width,
                   yerr=sds, capsize=3,
                   color=SYSTEM_COLORS[sys_],
                   label=SYSTEM_LABELS[sys_],
                   edgecolor="black", linewidth=0.4)
        ax.set_xticks(x_idx)
        ax.set_xticklabels([str(n) for n in x_counts], fontsize=26)
        ax.tick_params(axis="y", labelsize=24)
        if log_y:
            ax.set_yscale("log")
        else:
            ax.yaxis.set_major_formatter(FuncFormatter(_sci_fmt))
        ax.set_title(WORKLOAD_LABELS.get(wl, f"Workload {wl}"), fontsize=28)
        ax.set_xlabel("Writers", fontsize=26)
        if i == 0:
            ax.set_ylabel("Throughput (ops/sec)", fontsize=26)
        ax.grid(True, axis="y", alpha=0.3)

    handles, _ = axes[0].get_legend_handles_labels()
    labels = [SYSTEM_LABELS[s] for s in system_order]
    fig.legend(handles, labels, loc="lower center",
               bbox_to_anchor=(0.5, 0.02), ncol=len(system_order),
               fontsize=24, frameon=False,
               handletextpad=1.0, columnspacing=2.5)

    fig.tight_layout()
    fig.subplots_adjust(bottom=0.28, wspace=0.12)
    fig.savefig(out_path)
    plt.close(fig)


def plot_lines(rows: dict, x_counts: list[int], title: str, x_label: str,
               out_path: Path, system_order: list[str],
               log_y: bool = True) -> None:
    """Scaling line chart. 1x5 grid, x = N, one line per system."""
    workloads = ["a", "b", "c", "d", "f"]
    fig, axes = plt.subplots(1, 5, figsize=(28, 6.5), sharey=True)

    for i, wl in enumerate(workloads):
        ax = axes[i]
        for sys_ in system_order:
            per_n = rows.get(sys_, {}).get(wl, {})
            xs = [n for n in x_counts if n in per_n]
            ys = [per_n[n][0] for n in xs]
            sds = [per_n[n][1] for n in xs]
            if not xs:
                continue
            ax.errorbar(xs, ys, yerr=sds, marker=SYSTEM_MARKERS[sys_],
                        markersize=14, linewidth=3.2, capsize=5,
                        color=SYSTEM_COLORS[sys_],
                        label=SYSTEM_LABELS[sys_])
        ax.set_xscale("log", base=2)
        if log_y:
            ax.set_yscale("log")
        else:
            ax.yaxis.set_major_formatter(FuncFormatter(_sci_fmt))
        ax.set_xticks(x_counts)
        ax.set_xticklabels([str(n) for n in x_counts], fontsize=24)
        ax.tick_params(axis="y", labelsize=24)
        ax.set_xlabel("Writers", fontsize=26)
        if i == 0:
            ax.set_ylabel("Throughput (ops/sec)", fontsize=26)
        ax.set_title(WORKLOAD_LABELS.get(wl, f"Workload {wl}"), fontsize=28)
        ax.grid(True, which="both", alpha=0.3)

    handles, _ = axes[0].get_legend_handles_labels()
    labels = [SYSTEM_LABELS[s] for s in system_order]
    fig.legend(handles, labels, loc="lower center",
               bbox_to_anchor=(0.5, 0.02), ncol=len(system_order),
               fontsize=22, frameon=False,
               handletextpad=1.0, columnspacing=2.0)

    fig.tight_layout()
    fig.subplots_adjust(bottom=0.28, wspace=0.12)
    fig.savefig(out_path)
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--thread-csv", type=Path,
        default=Path("/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local/results.csv"),
    )
    p.add_argument(
        "--proc-csv", type=Path,
        default=Path("/users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase1-multi-process/results.csv"),
    )
    p.add_argument("--out-dir", type=Path,
                   default=Path("/users/Xinying/ozonedb/bench/results/local/multi-process-and-extended"))
    p.add_argument("--linear-y", action="store_true")
    args = p.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    thread_rows = load_csv(args.thread_csv, "threads")
    proc_rows = load_csv(args.proc_csv, "n_processes")

    log_y = not args.linear_y
    counts = [2, 4, 8, 16]

    # (a) Multi-process bar chart in fig2 format. Phase 1 has 3 systems.
    # Linear y for the multi-process plots (per user request).
    plot_bars(proc_rows, counts,
              title="Multi-process scaling",
              x_label="N", log_y=False,
              system_order=SYSTEM_ORDER_PROC,
              out_path=args.out_dir / "fig2_multi_process_bars.pdf")
    print(f"wrote {args.out_dir / 'fig2_multi_process_bars.pdf'}")

    # (b) Multi-thread scaling line chart. All 5 systems. Log y so the
    # multi-thread WAL collapse cells (~10 ops/s) and the HCTree-C peaks
    # (~2M ops/s) coexist on the same axis.
    plot_lines(thread_rows, counts,
               title="Multi-thread scaling",
               x_label="threads", log_y=log_y,
               system_order=SYSTEM_ORDER_THREAD,
               out_path=args.out_dir / "multi_thread_lines.pdf")
    print(f"wrote {args.out_dir / 'multi_thread_lines.pdf'}")

    # (c) Multi-process scaling line chart. Phase 1 has 3 systems.
    # Linear y per user request.
    plot_lines(proc_rows, counts,
               title="Multi-process scaling",
               x_label="processes", log_y=False,
               system_order=SYSTEM_ORDER_PROC,
               out_path=args.out_dir / "multi_process_lines.pdf")
    print(f"wrote {args.out_dir / 'multi_process_lines.pdf'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
