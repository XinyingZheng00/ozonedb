#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
# ]
# ///
"""Plot throughput and latency vs. number of writers from aggregated YCSB result files.

Usage:
    uv run plot_writers_scaling.py <prefix> [--results-dir DIR] [--output-dir DIR]

Example:
    uv run plot_writers_scaling.py 1KB-100000-insert-ozonedb-corfu
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

AGG_RE = re.compile(r"_agg_w(\d+)_t\d+\.result$")
LATENCY_OPS = ("READ", "UPDATE")


@dataclass
class AggResult:
    writers: int
    throughput: float
    p99_by_op: dict[str, float] = field(default_factory=dict)


def parse_agg_file(path: Path) -> tuple[float, dict[str, float]] | None:
    """Return (throughput ops/sec, {op: 99th-percentile-latency us}) from an agg result file."""
    throughput = None
    p99: dict[str, float] = {}
    current_section = None

    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 3:
                continue
            tag, key, value = parts[0], parts[1], parts[2]

            if tag == "[AGGREGATE]" and key == "Throughput(ops/sec)":
                throughput = float(value)
                continue

            if tag == "[CLEANUP]":
                continue

            m = re.fullmatch(r"\[([A-Z]+)\]", tag)
            if not m:
                continue
            current_section = m.group(1)

            if key == "MaxOver99thPercentileLatency(us)":
                p99[current_section] = float(value)

    if throughput is None:
        return None
    return throughput, p99


def collect_points(results_dir: Path, prefix: str) -> list[AggResult]:
    points: list[AggResult] = []
    for path in results_dir.iterdir():
        name = path.name
        if not name.startswith(prefix):
            continue
        m = AGG_RE.search(name)
        if not m:
            continue
        writers = int(m.group(1))
        parsed = parse_agg_file(path)
        if parsed is None:
            print(f"warning: could not parse {name}", file=sys.stderr)
            continue
        throughput, p99 = parsed
        points.append(AggResult(writers=writers, throughput=throughput, p99_by_op=p99))
    points.sort(key=lambda p: p.writers)
    return points


def plot(points: list[AggResult], prefix: str, output_dir: Path) -> None:
    labels = [str(p.writers) for p in points]
    throughput = [p.throughput for p in points]

    present_ops = [op for op in LATENCY_OPS if any(op in p.p99_by_op for p in points)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    ax1.bar(labels, throughput, color="#4C72B0")
    ax1.set_xlabel("Number of writers")
    ax1.set_ylabel("Throughput (ops/sec)")
    ax1.set_title("Throughput")
    ax1.grid(axis="y", linestyle="--", alpha=0.5)

    if present_ops:
        x = np.arange(len(labels))
        width = 0.8 / len(present_ops)
        colors = {"READ": "#DD8452", "UPDATE": "#55A868"}
        for i, op in enumerate(present_ops):
            values = [p.p99_by_op.get(op, 0.0) for p in points]
            offset = (i - (len(present_ops) - 1) / 2) * width
            ax2.bar(x + offset, values, width, label=op, color=colors.get(op))
        ax2.set_xticks(x)
        ax2.set_xticklabels(labels)
        ax2.legend()
    else:
        ax2.text(0.5, 0.5, "no READ/UPDATE latency data", ha="center", va="center",
                 transform=ax2.transAxes)

    ax2.set_xlabel("Number of writers")
    ax2.set_ylabel("99th percentile latency (us)")
    ax2.set_title("Latency (p99)")
    ax2.grid(axis="y", linestyle="--", alpha=0.5)

    fig.suptitle(prefix)
    fig.tight_layout()

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{prefix}_writers_scaling.png"
    fig.savefig(out_path, dpi=150)
    print(f"wrote {out_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prefix", help="Result filename prefix to match")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "results" / "local",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "out",
    )
    args = parser.parse_args()

    if not args.results_dir.is_dir():
        print(f"error: results dir not found: {args.results_dir}", file=sys.stderr)
        return 1

    points = collect_points(args.results_dir, args.prefix)
    if not points:
        print(f"error: no agg result files matched prefix {args.prefix!r}", file=sys.stderr)
        return 1

    print(f"found {len(points)} data points:")
    for p in points:
        lat_str = "  ".join(f"{op}_p99={p.p99_by_op[op]:.0f}us"
                            for op in LATENCY_OPS if op in p.p99_by_op)
        print(f"  writers={p.writers:>3}  throughput={p.throughput:>10.2f} ops/s  {lat_str}")

    plot(points, args.prefix, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
