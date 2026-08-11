#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
# ]
# ///
"""Plot throughput vs. number of writers for ozonedb-corfu and slatedb, one subplot per workload.

Usage:
    uv run plot_ozonedb_vs_slatedb.py <prefix> [--results-dir DIR] [--output-dir DIR]

Example:
    uv run plot_ozonedb_vs_slatedb.py 1KB-999999999-1000000-
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

ENGINES = ("ozonedb-corfu-durable", "slatedb-durable")
ENGINE_COLORS = {"ozonedb-corfu-durable": "#4C72B0", "slatedb-durable": "#DD8452"}
ENGINE_LABELS = {"ozonedb-corfu-durable": "ozonedb-corfu", "slatedb-durable": "slatedb"}

FILE_RE = re.compile(
    r"^(?P<rest>.+?)-(?P<engine>ozonedb-corfu-durable|slatedb-durable)_agg_w(?P<writers>\d+)_t\d+\.result$"
)
WORKLOAD_RE = re.compile(r"workload([a-z])$")


def parse_throughput(path: Path, engine: str) -> float | None:
    """For ozonedb use SumWriterThroughput(ops/sec); for slatedb use Throughput(ops/sec)."""
    if engine == "ozonedb-corfu-durable":
        target = ("[AGGREGATE]", "SumWriterThroughput(ops/sec)")
    else:
        target = ("[OVERALL]", "Throughput(ops/sec)")

    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 3:
                continue
            tag, key, value = parts[0], parts[1], parts[2]
            if (tag, key) == target:
                try:
                    return float(value)
                except ValueError:
                    return None
    return None


def collect(results_dir: Path, prefix: str) -> dict[str, dict[str, dict[int, float]]]:
    """Return {workload: {engine: {writers: throughput}}}."""
    data: dict[str, dict[str, dict[int, float]]] = defaultdict(
        lambda: defaultdict(dict)
    )

    for path in results_dir.iterdir():
        name = path.name
        if not name.startswith(prefix):
            continue
        m = FILE_RE.match(name)
        if not m:
            continue
        rest = m.group("rest")
        engine = m.group("engine")
        writers = int(m.group("writers"))
        if writers == 1:
            continue
        wl_match = WORKLOAD_RE.search(rest)
        if not wl_match:
            continue
        workload = wl_match.group(1)

        throughput = parse_throughput(path, engine)
        if throughput is None:
            print(f"warning: could not parse throughput from {name}", file=sys.stderr)
            continue
        data[workload][engine][writers] = throughput

    return data


def plot(
    data: dict[str, dict[str, dict[int, float]]],
    prefix: str,
    output_dir: Path,
) -> None:
    workloads = sorted(data.keys())
    n = len(workloads)
    cols = min(n, 3)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 4.5 * rows), squeeze=False)

    for idx, workload in enumerate(workloads):
        ax = axes[idx // cols][idx % cols]
        engine_data = data[workload]

        all_writers = sorted(
            {w for e in ENGINES for w in engine_data.get(e, {}).keys()}
        )
        if not all_writers:
            ax.text(
                0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes
            )
            ax.set_title(f"workload{workload}")
            continue

        x = np.arange(len(all_writers))
        width = 0.8 / len(ENGINES)

        for i, engine in enumerate(ENGINES):
            values = [engine_data.get(engine, {}).get(w, 0.0) for w in all_writers]
            offset = (i - (len(ENGINES) - 1) / 2) * width
            ax.bar(
                x + offset,
                values,
                width,
                label=ENGINE_LABELS[engine],
                color=ENGINE_COLORS[engine],
            )

        ax.set_xticks(x)
        ax.set_xticklabels([str(w) for w in all_writers])
        ax.set_xlabel("Number of writers")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_yscale("log")
        ax.set_title(f"workload{workload}")
        ax.grid(axis="y", linestyle="--", alpha=0.5)
        ax.legend()

    for idx in range(n, rows * cols):
        axes[idx // cols][idx % cols].axis("off")

    fig.suptitle("YCSB Throughput: ozonedb-corfu vs slatedb")
    fig.tight_layout()

    output_dir.mkdir(parents=True, exist_ok=True)
    safe_prefix = prefix.rstrip("-") or "results"
    out_path = output_dir / "ozonedb_vs_slatedb.pdf"
    fig.savefig(out_path, dpi=450)
    print(f"wrote {out_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prefix", help="Result filename prefix to match")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "results" / "final",
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

    data = collect(args.results_dir, args.prefix)
    if not data:
        print(
            f"error: no matching result files for prefix {args.prefix!r}",
            file=sys.stderr,
        )
        return 1

    for workload in sorted(data):
        print(f"workload{workload}:")
        for engine in ENGINES:
            pts = data[workload].get(engine, {})
            if not pts:
                continue
            pts_str = "  ".join(f"w{w}={tp:.2f}" for w, tp in sorted(pts.items()))
            print(f"  {engine}: {pts_str}")

    plot(data, args.prefix, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
