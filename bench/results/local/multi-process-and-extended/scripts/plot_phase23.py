#!/usr/bin/env python3
"""Plot extended-scaling sweeps (Phase 2 threads, Phase 3 processes).

Produces a single plot with x = N (log scale), y = aggregate throughput
(log scale), one line per system. Marks the stopping point on each line
when the sweep recorded a stop reason in WORKLOG.md.

Usage:
  python3 plot_phase23.py --csv <results.csv> --x-col threads \\
      --out-dir <plots-dir> --title 'Phase 2: Load thread scaling' \\
      [--worklog WORKLOG.md]

  python3 plot_phase23.py --csv <results.csv> --x-col n_processes \\
      --out-dir <plots-dir> --title 'Phase 3: Load process scaling' \\
      [--worklog WORKLOG.md]
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

SYSTEM_COLOR = {
    "ozonedb": "#1f77b4",
    "trunkcpp": "#d62728",
    "bcw2": "#2ca02c",
    "hctree": "#9467bd",
    "rocksdb": "#ff7f0e",
}


def load_csv(path: Path, x_col: str) -> dict:
    """Returns {system: [(N, mean_tput, stddev), ...]} sorted by N."""
    out: dict = {}
    with path.open() as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            sys_ = row["system"]
            try:
                n = int(row[x_col])
            except (KeyError, ValueError):
                continue
            tput_key = ("agg_throughput_mean" if "agg_throughput_mean" in row
                        else "throughput_mean")
            sd_key = ("agg_throughput_stddev" if "agg_throughput_stddev" in row
                      else "throughput_stddev")
            tput = row.get(tput_key, "")
            sd = row.get(sd_key, "")
            if not tput:
                continue
            out.setdefault(sys_, []).append(
                (n, float(tput), float(sd) if sd else 0.0)
            )
    for s in out:
        out[s].sort()
    return out


_STOP_RE = re.compile(r"-\s+(\S+):\s+STOPPED at (?:T|N)=(\d+)")


def parse_stop_points(worklog: Path) -> dict:
    """Parse WORKLOG.md lines like: '- ozonedb: STOPPED at T=64. Reason: ...'
    Returns {system: stopping_N}."""
    if not worklog or not worklog.exists():
        return {}
    out: dict = {}
    for line in worklog.read_text().splitlines():
        m = _STOP_RE.search(line)
        if m:
            out[m.group(1)] = int(m.group(2))
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--csv", required=True, type=Path)
    p.add_argument("--x-col", required=True, choices=["threads", "n_processes"])
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--title", default="")
    p.add_argument("--worklog", type=Path, default=None)
    args = p.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    data = load_csv(args.csv, args.x_col)
    stops = parse_stop_points(args.worklog) if args.worklog else {}

    fig, ax = plt.subplots(figsize=(8, 5))
    for sys_, points in data.items():
        xs = [r[0] for r in points]
        ys = [r[1] for r in points]
        yerr = [r[2] for r in points]
        color = SYSTEM_COLOR.get(sys_, "gray")
        ax.errorbar(xs, ys, yerr=yerr, marker="o", color=color, label=sys_)
        if sys_ in stops:
            stop_N = stops[sys_]
            stop_idx = next((i for i, x in enumerate(xs) if x == stop_N), None)
            if stop_idx is not None:
                ax.scatter([xs[stop_idx]], [ys[stop_idx]], marker="x", s=200,
                           color=color, zorder=5,
                           label=f"{sys_} stop @ {args.x_col}={stop_N}")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel(args.x_col)
    ax.set_ylabel("aggregate throughput (ops/s)")
    if args.title:
        ax.set_title(args.title)
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()

    out_base = args.out_dir / ("phase2_threads" if args.x_col == "threads"
                               else "phase3_processes")
    fig.savefig(f"{out_base}.png", dpi=140, bbox_inches="tight")
    fig.savefig(f"{out_base}.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_base}.png + .pdf", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
