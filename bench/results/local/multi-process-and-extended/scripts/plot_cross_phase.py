#!/usr/bin/env python3
"""Cross-phase OzoneDB Load comparison: thread scaling (Phase 2) vs process
scaling (Phase 3) on a single plot, x = N (log scale), y = aggregate
throughput (log scale). Headline: do they saturate at the same point?
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load_xy(csv_path: Path, x_col: str, sys_name: str) -> tuple[list[int], list[float]]:
    xs: list[int] = []
    ys: list[float] = []
    with csv_path.open() as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            if row.get("system") != sys_name:
                continue
            try:
                n = int(row[x_col])
            except (KeyError, ValueError):
                continue
            tput_key = ("agg_throughput_mean" if "agg_throughput_mean" in row
                        else "throughput_mean")
            tput = row.get(tput_key, "")
            if not tput:
                continue
            xs.append(n)
            ys.append(float(tput))
    pairs = sorted(zip(xs, ys))
    return [p[0] for p in pairs], [p[1] for p in pairs]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--phase2-csv", required=True, type=Path)
    p.add_argument("--phase3-csv", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    args = p.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    t_x, t_y = load_xy(args.phase2_csv, "threads", "ozonedb")
    p_x, p_y = load_xy(args.phase3_csv, "n_processes", "ozonedb")

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(t_x, t_y, marker="o", label="Phase 2: threads (single process)",
            color="#1f77b4")
    ax.plot(p_x, p_y, marker="s", label="Phase 3: processes (shared DB)",
            color="#d62728")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("N (writer threads or processes)")
    ax.set_ylabel("aggregate Load throughput (ops/s)")
    ax.set_title("OzoneDB Load — thread vs process scaling")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    out = args.out_dir / "ozonedb_thread_vs_process_load"
    fig.savefig(f"{out}.png", dpi=140, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}.png + .pdf", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
