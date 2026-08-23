#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
# ]
# ///
"""Plot throughput vs. number of writers for ozonedb-corfu, slatedb and
cr-sqlite, one subplot per workload.

Three aggregate filename styles are ingested, so the renamed final/ files
and the current multinode campaign both work:

    <rest>-<engine>_agg_w<N>_t<T>.result                    (original)
    <rest>-<engine>-agg-w<N>-t<T>.result                    (renamed final/)
    <rest>-<engine>_agg_multinode_w<N>_t<T>_trial<X>.result (multinode runs)

When a (workload, engine, writers) cell has no aggregate at all, the sum of
per-writer <rest>-<engine>_w<i>of<N>_... [OVERALL] Throughput is used
instead -- the same arithmetic the aggregate writer applies (a laptop-side
fetch hang can lose the aggregate while the writers finished). Multiple
trials of a cell are averaged. Engines with no data still appear in the
legend with empty bars.

Usage:
    uv run plot_ozonedb_vs_slatedb.py [prefix] [--results-dir DIR ...] [--output-dir DIR]

Example:
    uv run plot_ozonedb_vs_slatedb.py                       # all of bench/results/final, recursively
    uv run plot_ozonedb_vs_slatedb.py 1KB-999999999-1000000-
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

ENGINES = ("ozonedb-corfu-durable", "slatedb-durable", "crsqlite")
ENGINE_COLORS = {
    "ozonedb-corfu-durable": "#4C72B0",
    "slatedb-durable": "#DD8452",
    "crsqlite": "#55A868",
}
ENGINE_LABELS = {
    "ozonedb-corfu-durable": "ozonedb-corfu",
    "slatedb-durable": "slatedb",
    "crsqlite": "cr-sqlite",
}

# Longest token first so e.g. "slatedb" can never claim a "slatedb-durable"
# file; a "crsqlite-syncread" file matches no ENGINES token at all.
_ENG = "|".join(sorted((re.escape(e) for e in ENGINES), key=len, reverse=True))
AGG_RES = (
    re.compile(rf"^(?P<rest>.+?)-(?P<engine>{_ENG})_agg_multinode_w(?P<writers>\d+)_t\d+_trial\d+\.result$"),
    re.compile(rf"^(?P<rest>.+?)-(?P<engine>{_ENG})_agg_w(?P<writers>\d+)_t\d+\.result$"),
    re.compile(rf"^(?P<rest>.+?)-(?P<engine>{_ENG})-agg-w(?P<writers>\d+)-t\d+\.result$"),
)
# Never matches the per-host "_agg_w2of4_off0_" aggregates: those have
# "_agg_" between the engine token and "w<i>of<N>".
PERWRITER_RE = re.compile(
    rf"^(?P<rest>.+?)-(?P<engine>{_ENG})_w(?P<widx>\d+)of(?P<writers>\d+)_t\d+(?:_trial(?P<trial>\d+))?\.result$"
)
WORKLOAD_RE = re.compile(r"workload([a-z])$")

# ozonedb/crsqlite aggregates carry SumWriterThroughput; slatedb summaries
# and per-writer YCSB files only have the [OVERALL] line.
THROUGHPUT_KEYS = (
    ("[AGGREGATE]", "SumWriterThroughput(ops/sec)"),
    ("[OVERALL]", "Throughput(ops/sec)"),
)


def parse_throughput(path: Path) -> float | None:
    found: dict[tuple[str, str], float] = {}
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 3:
                continue
            key = (parts[0], parts[1])
            if key in THROUGHPUT_KEYS:
                try:
                    found[key] = float(parts[2])
                except ValueError:
                    pass
    for key in THROUGHPUT_KEYS:
        if key in found:
            return found[key]
    return None


def workload_of(rest: str) -> str | None:
    m = WORKLOAD_RE.search(rest)
    return m.group(1) if m else None


def collect(results_dirs: list[Path], prefix: str) -> dict[str, dict[str, dict[int, float]]]:
    """Return {workload: {engine: {writers: mean throughput across trials}}}."""
    agg_cells: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    # (workload, engine, writers, trial) -> {writer index: ops/s}
    per_writer: dict[tuple[str, str, int, int], dict[int, float]] = defaultdict(dict)

    for results_dir in results_dirs:
        for path in sorted(results_dir.rglob("*.result")):
            name = path.name
            if prefix and not name.startswith(prefix):
                continue

            m = next((m for rx in AGG_RES if (m := rx.match(name))), None)
            if m:
                workload = workload_of(m.group("rest"))
                writers = int(m.group("writers"))
                if workload is None or writers == 1:
                    continue
                throughput = parse_throughput(path)
                if throughput is None:
                    print(f"warning: could not parse throughput from {name}", file=sys.stderr)
                    continue
                agg_cells[(workload, m.group("engine"), writers)].append(throughput)
                continue

            m = PERWRITER_RE.match(name)
            if m:
                workload = workload_of(m.group("rest"))
                writers = int(m.group("writers"))
                if workload is None or writers == 1:
                    continue
                throughput = parse_throughput(path)
                if throughput is None:
                    continue
                trial = int(m.group("trial") or 1)
                key = (workload, m.group("engine"), writers, trial)
                per_writer[key][int(m.group("widx"))] = throughput

    data: dict[str, dict[str, dict[int, float]]] = defaultdict(lambda: defaultdict(dict))

    # Per-writer sums fill only cells with no aggregate at all.
    fallback: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    for (workload, engine, writers, _trial), by_idx in per_writer.items():
        cell = (workload, engine, writers)
        if cell in agg_cells:
            continue
        if len(by_idx) != writers:
            print(
                f"warning: {cell}: {len(by_idx)}/{writers} per-writer files -- "
                "computed sum UNDER-REPORTS",
                file=sys.stderr,
            )
        fallback[cell].append(sum(by_idx.values()))
    for (workload, engine, writers), sums in fallback.items():
        print(f"note: {(workload, engine, writers)}: no aggregate, using per-writer sum")
        data[workload][engine][writers] = sum(sums) / len(sums)

    for (workload, engine, writers), vals in agg_cells.items():
        data[workload][engine][writers] = sum(vals) / len(vals)

    return data


def plot(
    data: dict[str, dict[str, dict[int, float]]],
    output_dir: Path,
) -> None:
    workloads = sorted(data.keys())
    n = len(workloads)
    cols = min(n, 3)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 4.5 * rows), squeeze=False)

    all_writers = sorted(
        {w for wl in workloads for e in ENGINES for w in data[wl].get(e, {})}
    )

    for idx, workload in enumerate(workloads):
        ax = axes[idx // cols][idx % cols]
        engine_data = data[workload]

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

    fig.suptitle("YCSB Throughput: ozonedb-corfu vs slatedb vs cr-sqlite")
    fig.tight_layout()

    output_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        out_path = output_dir / f"ozonedb_vs_slatedb.{ext}"
        fig.savefig(out_path, dpi=200)
        print(f"wrote {out_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "prefix",
        nargs="?",
        default="",
        help="Optional result filename prefix to match (default: everything)",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        action="append",
        dest="results_dirs",
        default=None,
        help="Result dir, scanned recursively; may be given multiple times "
        "(default: bench/results/final)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "out",
    )
    args = parser.parse_args()

    results_dirs = args.results_dirs or [
        Path(__file__).resolve().parents[2] / "results" / "final"
    ]
    for d in results_dirs:
        if not d.is_dir():
            print(f"error: results dir not found: {d}", file=sys.stderr)
            return 1

    data = collect(results_dirs, args.prefix)
    if not data:
        print("error: no matching result files", file=sys.stderr)
        return 1

    for workload in sorted(data):
        print(f"workload{workload}:")
        for engine in ENGINES:
            pts = data[workload].get(engine, {})
            if not pts:
                continue
            pts_str = "  ".join(f"w{w}={tp:.2f}" for w, tp in sorted(pts.items()))
            print(f"  {engine}: {pts_str}")

    plot(data, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
