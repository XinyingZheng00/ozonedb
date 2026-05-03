#!/usr/bin/env python3
"""Per-workload N-vs-aggregated-throughput line plot, paper-style 1x5 row.

Five series (system + concurrency mode):
  - SQLite (trunk, plain WAL) - thread       (trunkcpp, fig2 multi-thread CSV)
  - SQLite (trunk, plain WAL) - process      (trunkcpp, phase1 multi-process CSV)
  - SQLite (BEGIN CONCURRENT + WAL2) - thread    (bcw2, fig2 multi-thread CSV)
  - SQLite (BEGIN CONCURRENT + WAL2) - process   (bcw2, phase1 multi-process CSV)
  - SQLite (HCTree) - thread                     (hctree, fig2 multi-thread CSV)

One panel per workload (a, b, c, d, f) in a single row; legend below the figure.
Same fonts/layout as plot_phase1_paper.py so the figure reads as a set.
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


THREAD_CSV = Path(
    "/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local/results.csv"
)
PROC_CSV = Path(
    "/users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/"
    "phase1-multi-process/results.csv"
)

# (label, csv, system, x-col, throughput-col, color, marker, linestyle)
SERIES = [
    ("SQLite (trunk, plain WAL) - thread",
     THREAD_CSV, "trunkcpp", "threads", "throughput_mean",
     "#2ca02c", "^", "-"),
    ("SQLite (trunk, plain WAL) - process",
     PROC_CSV, "trunkcpp", "n_processes", "agg_throughput_mean",
     "#2ca02c", "s", "--"),
    ("SQLite (BEGIN CONCURRENT + WAL2) - thread",
     THREAD_CSV, "bcw2", "threads", "throughput_mean",
     "#ff7f0e", "v", "-"),
    ("SQLite (BEGIN CONCURRENT + WAL2) - process",
     PROC_CSV, "bcw2", "n_processes", "agg_throughput_mean",
     "#ff7f0e", "D", "--"),
    ("SQLite (HCTree) - thread",
     THREAD_CSV, "hctree", "threads", "throughput_mean",
     "#9467bd", "o", "-"),
]

WORKLOADS = ["a", "b", "c", "d", "f"]


def _sci_fmt(x: float, pos: int) -> str:
    if x == 0:
        return "0"
    exp = int(np.floor(np.log10(abs(x))))
    mantissa = x / (10 ** exp)
    if abs(mantissa - round(mantissa)) < 1e-9:
        return rf"${int(round(mantissa))}{{\times}}10^{{{exp}}}$"
    return rf"${mantissa:.1f}{{\times}}10^{{{exp}}}$"


def _load(path: Path, system: str, x_col: str, tput_col: str) -> dict:
    """Return rows[workload][N] = (mean, stddev)."""
    out: dict = defaultdict(dict)
    sd_col = tput_col.replace("_mean", "_stddev")
    with path.open() as f:
        for row in csv.DictReader(f):
            if row.get("system") != system:
                continue
            wl = row.get("workload")
            try:
                n = int(row[x_col])
                m = float(row[tput_col])
            except (KeyError, ValueError):
                continue
            try:
                sd = float(row[sd_col]) if row.get(sd_col) else 0.0
            except ValueError:
                sd = 0.0
            out[wl][n] = (m, sd)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path,
                   default=Path("/users/Xinying/ozonedb/bench/results/local/"
                                "multi-process-and-extended/"
                                "thread_vs_process_per_workload.pdf"))
    p.add_argument("--linear-y", action="store_true")
    args = p.parse_args()

    cache: dict = {}
    for _label, csv_path, sys_, x_col, tput_col, *_ in SERIES:
        key = (csv_path, sys_, x_col, tput_col)
        if key not in cache:
            cache[key] = _load(csv_path, sys_, x_col, tput_col)

    log_y = not args.linear_y
    fig, axes = plt.subplots(1, 5, figsize=(28, 6.5), sharey=True)

    for i, wl in enumerate(WORKLOADS):
        ax = axes[i]
        for label, csv_path, sys_, x_col, tput_col, color, marker, ls in SERIES:
            per_n = cache[(csv_path, sys_, x_col, tput_col)].get(wl, {})
            if not per_n:
                continue
            xs = sorted(per_n.keys())
            ys = [per_n[n][0] for n in xs]
            sds = [per_n[n][1] for n in xs]
            ax.errorbar(xs, ys, yerr=sds, marker=marker, markersize=14,
                        linewidth=3.0, capsize=5, color=color, linestyle=ls,
                        label=label)
        ax.set_xscale("log", base=2)
        if log_y:
            ax.set_yscale("log")
        else:
            ax.yaxis.set_major_formatter(FuncFormatter(_sci_fmt))
        ax.set_xticks([2, 4, 8, 16])
        ax.set_xticklabels(["2", "4", "8", "16"], fontsize=24)
        ax.tick_params(axis="y", labelsize=24)
        ax.set_xlabel("N (writers)", fontsize=26)
        if i == 0:
            ax.set_ylabel("Aggregated throughput (ops/sec)", fontsize=26)
        ax.set_title(f"Workload {wl.upper()}", fontsize=28)
        ax.grid(True, which="both", alpha=0.3)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center",
               bbox_to_anchor=(0.5, 0.02), ncol=3,
               fontsize=20, frameon=False,
               handletextpad=1.0, columnspacing=2.0,
               labelspacing=0.7)

    fig.tight_layout()
    fig.subplots_adjust(bottom=0.32, wspace=0.12)
    fig.savefig(args.out)
    fig.savefig(args.out.with_suffix(".png"), dpi=150)
    print(f"wrote {args.out} + .png")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
