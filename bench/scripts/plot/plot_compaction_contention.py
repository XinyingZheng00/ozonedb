#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
# ]
# ///
"""Plot compaction commit latency vs writer count from the contention sweep.

Reads the summary CSV produced by run_corfu_compaction_contention.py
(one row per (writer_count, trial)) and emits a PDF with mean / p50 / p95 /
p99 series, error bars across trials.

Usage:
    uv run plot_compaction_contention.py \\
        --summary $OZONEDB_HOME/bench/results/local/<run_tag>/compaction_contention_summary.csv \\
        --out $OZONEDB_HOME/bench/scripts/plot/out/compaction_contention.pdf
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def load(summary_path: Path):
    """Return {metric: {writer_count: [trial values]}}."""
    data: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    with summary_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                w = int(row["writer_count"])
            except (KeyError, ValueError):
                continue
            for metric in ("mean_ms", "p50_ms", "p95_ms", "p99_ms"):
                try:
                    data[metric][w].append(float(row[metric]))
                except (KeyError, ValueError):
                    continue
    return data


def plot(data, out_path: Path) -> None:
    series = [
        ("mean_ms", "mean", "o", "#4C72B0"),
        ("p50_ms", "p50", "s", "#55A868"),
        ("p95_ms", "p95", "^", "#DD8452"),
        ("p99_ms", "p99", "x", "#C44E52"),
    ]

    fig, ax = plt.subplots(figsize=(8, 5))
    all_writers: set[int] = set()
    for metric, label, marker, color in series:
        per_w = data.get(metric, {})
        all_writers.update(per_w.keys())
        if not per_w:
            continue
        ws = sorted(per_w.keys())
        means = [sum(per_w[w]) / len(per_w[w]) for w in ws]
        # Error bars: half the trial spread. With 3 trials this approximates
        # ±range/2 around the mean — coarse but sufficient for a visual cue.
        errs = [
            (max(per_w[w]) - min(per_w[w])) / 2 if len(per_w[w]) > 1 else 0.0
            for w in ws
        ]
        ax.errorbar(ws, means, yerr=errs, marker=marker, color=color,
                    label=label, capsize=4, linewidth=1.5)

    if not all_writers:
        raise SystemExit("no plottable rows in summary CSV")

    sorted_ws = sorted(all_writers)
    ax.set_xscale("log", base=2)
    ax.set_xticks(sorted_ws)
    ax.set_xticklabels([str(w) for w in sorted_ws])
    ax.set_xlabel("Writer count (concurrent processes)")
    ax.set_ylabel("Compaction commit latency (ms)")
    ax.set_title("End-to-end compaction commit latency under multi-writer contention\n"
                 "(needed → all peers committed)")
    ax.legend(loc="best")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path)
    print(f"wrote {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True,
                        help="Path to compaction_contention_summary.csv")
    parser.add_argument("--out", type=Path, required=True,
                        help="Output PDF path")
    args = parser.parse_args()
    if not args.summary.exists():
        raise SystemExit(f"summary CSV not found: {args.summary}")
    plot(load(args.summary), args.out)


if __name__ == "__main__":
    main()
