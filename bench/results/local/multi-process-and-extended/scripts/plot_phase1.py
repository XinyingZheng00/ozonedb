#!/usr/bin/env python3
"""Plot Phase 1 multi-process scaling results.

Reads results.csv (written by run_phase1_multiproc_sweep.py) and produces:
  - phase1_multi_process.png / .pdf  — log-x scaling plot, one line per system,
    one panel per workload, x=N processes, y=aggregate throughput (ops/s).

Optional: --thread-csv <path> overlays the corresponding multi-thread results
from fig2-multi-writer-local/results.csv for direct comparison (multi-process
vs multi-thread on the same axes), one panel per workload.

Usage:
    python3 plot_phase1.py \\
        --csv /users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase1-multi-process/results.csv \\
        --out-dir /users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase1-multi-process/plots \\
        [--thread-csv /users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local/results.csv]
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

SYSTEM_ORDER = ["ozonedb", "trunkcpp", "bcw2"]
SYSTEM_COLOR = {
    "ozonedb": "#1f77b4",
    "trunkcpp": "#d62728",
    "bcw2": "#2ca02c",
    "hctree": "#9467bd",
    "rocksdb": "#ff7f0e",
}
WORKLOADS = ["a", "b", "c", "d", "f"]


def load_csv(path: Path, n_col: str) -> dict:
    """Returns {system: {workload: [(N, mean_tput, stddev_tput), ...]}}."""
    out: dict = {}
    with path.open() as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            sys_ = row["system"]
            wl = row["workload"]
            try:
                n = int(row[n_col])
            except (KeyError, ValueError):
                continue
            tput = row.get("agg_throughput_mean") or row.get("throughput_mean") or ""
            sd = row.get("agg_throughput_stddev") or row.get("throughput_stddev") or ""
            if not tput:
                continue
            out.setdefault(sys_, {}).setdefault(wl, []).append(
                (n, float(tput), float(sd) if sd else 0.0)
            )
    for s in out:
        for w in out[s]:
            out[s][w].sort()
    return out


def plot_grid(proc_data: dict, thread_data: dict | None, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, len(WORKLOADS), figsize=(4 * len(WORKLOADS), 4),
                             sharey=False)
    for ax, wl in zip(axes, WORKLOADS):
        for sys_ in SYSTEM_ORDER:
            color = SYSTEM_COLOR.get(sys_, "gray")
            if sys_ in proc_data and wl in proc_data[sys_]:
                xs = [r[0] for r in proc_data[sys_][wl]]
                ys = [r[1] for r in proc_data[sys_][wl]]
                yerr = [r[2] for r in proc_data[sys_][wl]]
                ax.errorbar(xs, ys, yerr=yerr, marker="o",
                            label=f"{sys_} (proc)", color=color, linestyle="-")
            if thread_data and sys_ in thread_data and wl in thread_data[sys_]:
                xs = [r[0] for r in thread_data[sys_][wl]]
                ys = [r[1] for r in thread_data[sys_][wl]]
                yerr = [r[2] for r in thread_data[sys_][wl]]
                ax.errorbar(xs, ys, yerr=yerr, marker="x",
                            label=f"{sys_} (thread)", color=color, linestyle="--",
                            alpha=0.6)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("N (processes / threads)")
        ax.set_title(f"workload {wl}")
        ax.grid(True, which="both", alpha=0.3)
    axes[0].set_ylabel("aggregate throughput (ops/s)")
    # Single legend for the whole figure
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=min(len(labels), 6),
               bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("Phase 1 — multi-process writer scaling"
                 + (" (with multi-thread overlay)" if thread_data else ""))
    fig.tight_layout(rect=[0, 0.04, 1, 0.96])
    fig.savefig(out_dir / "phase1_multi_process.png", dpi=140, bbox_inches="tight")
    fig.savefig(out_dir / "phase1_multi_process.pdf", bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--csv", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--thread-csv", type=Path, default=None,
                   help="Overlay multi-thread results from fig2-multi-writer-local/results.csv")
    args = p.parse_args()

    proc_data = load_csv(args.csv, n_col="n_processes")
    thread_data = load_csv(args.thread_csv, n_col="threads") if args.thread_csv else None
    plot_grid(proc_data, thread_data, args.out_dir)
    print(f"wrote {args.out_dir}/phase1_multi_process.png + .pdf", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
