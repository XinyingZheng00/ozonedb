#!/usr/bin/env python3
"""Generate Fig 2 multi-writer scaling plots from results.csv.

Output:
  - One PDF / PNG per workload (5 total) with throughput vs. thread count,
    one line per system.  Missing cells are gaps (no interpolation).
  - A summary throughput table per workload as a markdown file.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

try:
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.ticker import FuncFormatter
except ImportError as e:
    print("matplotlib not available; install with `pip install matplotlib`", file=sys.stderr)
    raise


def _sci_fmt(x: float, pos: int) -> str:
    """Format y-tick as e.g. 2x10^5; collapses the long-zero counts."""
    if x == 0:
        return "0"
    exp = int(np.floor(np.log10(abs(x))))
    mantissa = x / (10 ** exp)
    if abs(mantissa - round(mantissa)) < 1e-9:
        return rf"${int(round(mantissa))}{{\times}}10^{{{exp}}}$"
    return rf"${mantissa:.1f}{{\times}}10^{{{exp}}}$"

SYSTEM_ORDER = ["ozonedb", "rocksdb", "trunkcpp", "bcw2", "hctree"]
SYSTEM_LABELS = {
    "ozonedb": "OzoneKV",
    "rocksdb": "RocksDB",
    "trunkcpp": "SQLite (trunk, plain WAL)",
    "bcw2": "SQLite (BEGIN CONCURRENT + WAL2)",
    "hctree": "SQLite (HCTree)",
}
SYSTEM_MARKERS = {
    "ozonedb": "o", "rocksdb": "s", "trunkcpp": "^", "bcw2": "v", "hctree": "D",
}

WORKLOAD_LABELS = {
    "a": "Workload A (50% read, 50% update)",
    "b": "Workload B (95% read, 5% update)",
    "c": "Workload C (100% read)",
    "d": "Workload D (95% read, 5% insert, latest dist.)",
    "f": "Workload F (50% read, 50% read-modify-write)",
}

# Compact two-line x-axis labels for the single-writer plot
# (workload letter + operation ratio, multi-line with newline).
WORKLOAD_X_LABELS = {
    "load":       "Load",
    "load_tmpfs": "Load tmpfs",
    "a": "A",
    "b": "B",
    "c": "C",
    "d": "D",
    "f": "F",
}


def load_csv(path: Path) -> dict:
    """rows[system][workload][threads] = (mean, stddev) tuple."""
    rows = defaultdict(lambda: defaultdict(dict))
    with path.open() as f:
        for row in csv.DictReader(f):
            try:
                mean = float(row["throughput_mean"])
            except (ValueError, KeyError):
                continue
            try:
                sd = float(row["throughput_stddev"]) if row.get("throughput_stddev") else 0.0
            except ValueError:
                sd = 0.0
            rows[row["system"]][row["workload"]][int(row["threads"])] = (mean, sd)
    return rows


# Distinct colors so all 5 systems are visually separable.
_COLORS = {
    "ozonedb":  "#1f77b4",  # blue
    "rocksdb":  "#d62728",  # red
    "trunkcpp": "#2ca02c",  # green
    "bcw2":     "#ff7f0e",  # orange
    "hctree":   "#9467bd",  # purple
}


def _load_load_csv(path: Path) -> dict[str, float]:
    """Return {system: throughput} for the bulk-load phase."""
    out: dict[str, float] = {}
    if not path.exists():
        return out
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t = float(row["throughput"])
            except (ValueError, KeyError):
                continue
            out[row["system"]] = t
    return out


def plot_single_writer(rows: dict, out_path: Path, log_y: bool,
                       load_throughput: Optional[dict[str, float]] = None,
                       load_tmpfs_throughput: Optional[dict[str, float]] = None) -> None:
    """One plot: x-axis = (Load, Load tmpfs,) Workload A/B/C/D/F; T=1 only.

    Adds a "Load" group when ``load_throughput`` is provided, and a "Load tmpfs"
    group when ``load_tmpfs_throughput`` is provided.
    """
    import numpy as np

    workloads: list[str] = []
    if load_throughput:
        workloads.append("load")
    if load_tmpfs_throughput:
        workloads.append("load_tmpfs")
    workloads.extend(["a", "b", "c", "d", "f"])

    n_sys = len(SYSTEM_ORDER)
    bar_width = 0.8 / n_sys
    x_idx = np.arange(len(workloads))

    fig, ax = plt.subplots(figsize=(18, 9))
    for j, sysname in enumerate(SYSTEM_ORDER):
        means: list[float] = []
        sds: list[float] = []
        for wl in workloads:
            if wl == "load":
                m = (load_throughput or {}).get(sysname, float("nan"))
                s = 0.0
            elif wl == "load_tmpfs":
                m = (load_tmpfs_throughput or {}).get(sysname, float("nan"))
                s = 0.0
            else:
                per_t = rows.get(sysname, {}).get(wl, {})
                m, s = per_t.get(1, (float("nan"), 0))
                if m is None:
                    m = float("nan")
            means.append(m)
            sds.append(s if s else 0)
        offset = (j - (n_sys - 1) / 2) * bar_width
        display = [max(m, 0.1) if not np.isnan(m) else np.nan for m in means]
        ax.bar(
            x_idx + offset, display, bar_width,
            yerr=sds, capsize=2,
            color=_COLORS.get(sysname, "gray"),
            label=SYSTEM_LABELS.get(sysname, sysname),
            edgecolor="black", linewidth=0.4,
        )

    ax.set_xticks(x_idx)
    ax.set_xticklabels([WORKLOAD_X_LABELS.get(wl, wl.upper()) for wl in workloads],
                       fontsize=26)
    ax.tick_params(axis="y", labelsize=30)
    ax.set_ylabel("Throughput (ops/sec)", fontsize=26)
    if log_y:
        ax.set_yscale("log")
    ax.set_title("Single-writer performance", fontsize=26)
    ax.grid(True, axis="y", alpha=0.3)
    # Add log-scale headroom above the tallest bar so the in-plot legend
    # doesn't overlap any bars (the HCTree workload-C bar is the tallest).
    ymin, ymax = ax.get_ylim()
    ax.set_ylim(ymin, ymax * 10)
    ax.legend(loc="upper left", fontsize=25, title="System",
              title_fontsize=22, frameon=True, framealpha=0.95, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path)
    fig.savefig(out_path.with_suffix(".png"), dpi=150)
    plt.close(fig)


def plot_multi_writer(rows: dict, out_path: Path, log_y: bool,
                      thread_counts: list[int]) -> None:
    """1x5 grid, one panel per workload, grouped bars per system, T>1 only."""
    workloads = ["a", "b", "c", "d", "f"]
    n_sys = len(SYSTEM_ORDER)
    bar_width = 0.8 / n_sys
    x_idx = np.arange(len(thread_counts))

    fig, axes = plt.subplots(1, 5, figsize=(28, 6.5), sharey=True)

    for i, wl in enumerate(workloads):
        ax = axes[i]
        for j, sysname in enumerate(SYSTEM_ORDER):
            per_t = rows.get(sysname, {}).get(wl, {})
            means = [per_t.get(t, (np.nan, 0))[0] for t in thread_counts]
            sds   = [per_t.get(t, (np.nan, 0))[1] for t in thread_counts]
            offset = (j - (n_sys - 1) / 2) * bar_width
            display_means = [max(m, 0.1) if (m is not None and not np.isnan(m)) else np.nan
                             for m in means]
            ax.bar(
                x_idx + offset, display_means, bar_width,
                yerr=sds, capsize=3,
                color=_COLORS.get(sysname, "gray"),
                label=SYSTEM_LABELS.get(sysname, sysname),
                edgecolor="black", linewidth=0.4,
            )
        ax.set_xticks(x_idx)
        ax.set_xticklabels([str(t) for t in thread_counts], fontsize=26)
        ax.tick_params(axis="y", labelsize=24)
        if log_y:
            ax.set_yscale("log")
        else:
            ax.yaxis.set_major_formatter(FuncFormatter(_sci_fmt))
        ax.set_title(f"Workload {wl.upper()}", fontsize=28)
        ax.set_xlabel("Writers", fontsize=26)
        if i == 0:
            ax.set_ylabel("Throughput (ops/sec)", fontsize=26)
        ax.grid(True, axis="y", alpha=0.3)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center",
               bbox_to_anchor=(0.5, 0.02), ncol=n_sys,
               fontsize=22, frameon=False,
               handletextpad=1.0, columnspacing=2.0)

    fig.tight_layout()
    fig.subplots_adjust(bottom=0.28, wspace=0.12)
    fig.savefig(out_path)
    fig.savefig(out_path.with_suffix(".png"), dpi=150)
    plt.close(fig)


def write_table(rows: dict, workload: str, out_path: Path) -> None:
    threads = sorted({t for sys_data in rows.values()
                      for w_data in [sys_data.get(workload, {})]
                      for t in w_data.keys()})
    if not threads:
        return
    lines = [f"### Workload {workload.upper()}",
             f"_{WORKLOAD_LABELS.get(workload, '')}_  ",
             "Throughput (ops/sec; mean ± stddev across repeats); — = missing.",
             "",
             "| System | " + " | ".join(f"T={t}" for t in threads) + " |",
             "|---|" + "|".join("---" for _ in threads) + "|"]
    for sys_name in SYSTEM_ORDER:
        cells: list[str] = []
        per_t = rows.get(sys_name, {}).get(workload, {})
        for t in threads:
            if t not in per_t:
                cells.append("—")
                continue
            m, sd = per_t[t]
            if sd > 0:
                cells.append(f"{m:,.0f} ± {sd:,.0f}")
            else:
                cells.append(f"{m:,.0f}")
        lines.append(f"| {SYSTEM_LABELS.get(sys_name, sys_name)} | " + " | ".join(cells) + " |")
    lines.append("")
    out_path.write_text("\n".join(lines))


def _merge_rows(*sources: dict) -> dict:
    """Merge multiple {sys: {wl: {t: (mean,sd)}}} dicts into one."""
    out: dict = {}
    for src in sources:
        for sysname, by_wl in src.items():
            dst_wl = out.setdefault(sysname, {})
            for wl, by_t in by_wl.items():
                dst_t = dst_wl.setdefault(wl, {})
                dst_t.update(by_t)
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--fig1-dir", type=Path,
                   default=Path("/users/Xinying/ozonedb/bench/results/local/fig1-single-writer-local"),
                   help="Single-writer (T=1) results directory; reads results.csv, "
                        "writes fig1_single_writer.{pdf,png}")
    p.add_argument("--fig2-dir", type=Path,
                   default=Path("/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local"),
                   help="Multi-writer (T>1) results directory; reads results.csv, "
                        "writes fig2_multi_writer.{pdf,png} + per-workload tables")
    p.add_argument("--linear-y", action="store_true",
                   help="Use linear y axis (default: log, since values span 10^1..10^6).")
    p.add_argument("--multi-threads", default="2,4,8,16",
                   help="Thread counts shown in the multi-writer plot.")
    args = p.parse_args()

    args.fig1_dir.mkdir(parents=True, exist_ok=True)
    args.fig2_dir.mkdir(parents=True, exist_ok=True)

    fig1_csv = args.fig1_dir / "results.csv"
    fig2_csv = args.fig2_dir / "results.csv"
    if not fig1_csv.exists() and not fig2_csv.exists():
        print(f"no results.csv in either {args.fig1_dir} or {args.fig2_dir}",
              file=sys.stderr)
        return 1

    fig1_rows = load_csv(fig1_csv) if fig1_csv.exists() else {}
    fig2_rows = load_csv(fig2_csv) if fig2_csv.exists() else {}

    # Single-writer plot reads from fig1 dir, writes back into fig1 dir.
    # If a load_results.csv exists alongside, prepend a "Load" bar group.
    if fig1_rows:
        sw_pdf = args.fig1_dir / "fig1_single_writer.pdf"
        load_csv_path = args.fig1_dir / "load_results.csv"
        load_tmpfs_csv_path = args.fig1_dir / "load_tmpfs_results.csv"
        load_tput = _load_load_csv(load_csv_path) if load_csv_path.exists() else None
        load_tmpfs_tput = _load_load_csv(load_tmpfs_csv_path) if load_tmpfs_csv_path.exists() else None
        plot_single_writer(fig1_rows, sw_pdf, log_y=not args.linear_y,
                           load_throughput=load_tput,
                           load_tmpfs_throughput=load_tmpfs_tput)
        extras = []
        if load_tput:        extras.append(load_csv_path.name)
        if load_tmpfs_tput:  extras.append(load_tmpfs_csv_path.name)
        suffix = f"  (Load columns from {', '.join(extras)})" if extras else ""
        print(f"  wrote {sw_pdf}{suffix}")
    else:
        print(f"  fig1 single-writer plot skipped (no rows in {fig1_csv})")

    # Multi-writer plot reads from fig2 dir, writes back into fig2 dir.
    if fig2_rows:
        mw_pdf = args.fig2_dir / "fig2_multi_writer.pdf"
        multi_threads = [int(t) for t in args.multi_threads.split(",")]
        plot_multi_writer(fig2_rows, mw_pdf,
                          log_y=not args.linear_y, thread_counts=multi_threads)
        print(f"  wrote {mw_pdf}")

        # Per-workload markdown tables include both T=1 (from fig1) and T>1
        # rows, so the report's tabular view stays comprehensive.
        merged = _merge_rows(fig1_rows, fig2_rows)
        for workload in ["a", "b", "c", "d", "f"]:
            tpath = args.fig2_dir / f"fig2_workload{workload}_table.md"
            write_table(merged, workload, tpath)
            print(f"  wrote {tpath}")
    else:
        print(f"  fig2 multi-writer plot skipped (no rows in {fig2_csv})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
