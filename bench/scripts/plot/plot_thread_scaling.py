#!/usr/bin/env python3
"""
Generate the multi-panel YCSB throughput plot from a directory of *.result files.

Assumptions based on the sample filename and the target PDF:
  - Each result file contains one workload / one system / one writer-count setting.
  - Each result file contains 3 experimental runs.
  - Each run ends with a line like:
        [OVERALL], Throughput(ops/sec), 7200.348559775002
  - The filename contains:
        * workload name such as workloada / workloadb / ...
        * system name such as ozonedb or slatedb
        * writer count in a token like _t2, -t4, _threads8, _writers16, etc.

Example:
  python plot_ycsb_throughput.py \
      --input-dir /path/to/results \
      --output /path/to/ozonedb_vs_slatedb.png
"""

from __future__ import annotations

import argparse
import math
import re
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np


THROUGHPUT_RE = re.compile(r"\[OVERALL\],\s*Throughput\(ops/sec\),\s*([0-9]+(?:\.[0-9]+)?)")
HCTREE_THROUGHPUT_RE = re.compile(r"^Run throughput\(ops/sec\):\s*([0-9]+(?:\.[0-9]+)?)", re.MULTILINE)
WORKLOAD_RE = re.compile(r"(workload[a-z])", re.IGNORECASE)

# Tries a few common patterns for writer/thread count in filenames.
WRITER_PATTERNS = [
    re.compile(r"(?:^|[_\-.])t(\d+)(?:[_\-.]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[_\-.])threads?(\d+)(?:[_\-.]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[_\-.])writers?(\d+)(?:[_\-.]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[_\-.])(\d+)writers?(?:[_\-.]|$)", re.IGNORECASE),
]

# Adjust this mapping if your actual filenames use different system tags.
SYSTEM_ALIASES = {
    "ozonedb": "ozonedb",
    "hctree": "hctree",
    "bcw2": "bcw2",
    "rocksdb": "rocksdb",
}

DEFAULT_WORKLOAD_ORDER = ["workloada", "workloadb", "workloadc", "workloadd", "workloadf"]
DEFAULT_SYSTEM_ORDER = ["ozonedb", "hctree", "bcw2", "rocksdb"]


def extract_writer_count(stem: str) -> int:
    for pattern in WRITER_PATTERNS:
        match = pattern.search(stem)
        if match:
            return int(match.group(1))
    raise ValueError(f"Could not infer writer/thread count from filename: {stem}")


def extract_workload(stem: str) -> str:
    match = WORKLOAD_RE.search(stem)
    if not match:
        raise ValueError(f"Could not infer workload from filename: {stem}")
    return match.group(1).lower()


def extract_system(stem: str) -> str:
    lower = stem.lower()
    for alias, canonical in SYSTEM_ALIASES.items():
        if alias in lower:
            return canonical
    raise ValueError(
        "Could not infer system from filename: "
        f"{stem}. Update SYSTEM_ALIASES in the script if needed."
    )


def parse_ozonekv_result(result_file: Path) -> List[float]:
    text = result_file.read_text(encoding="utf-8", errors="replace")
    values = [float(x) for x in THROUGHPUT_RE.findall(text)]
    if not values:
        raise ValueError(f"No throughput lines found in file: {result_file}")
    return values


def parse_hctree_result(result_file: Path) -> List[float]:
    text = result_file.read_text(encoding="utf-8", errors="replace")
    values = [float(x) for x in HCTREE_THROUGHPUT_RE.findall(text)]
    if not values:
        raise ValueError(f"No 'Run throughput(ops/sec)' lines found in file: {result_file}")
    return values


def parse_bcw2_result(result_file: Path) -> List[float]:
    text = result_file.read_text(encoding="utf-8", errors="replace")
    values = [float(x) for x in HCTREE_THROUGHPUT_RE.findall(text)]
    if not values:
        raise ValueError(f"No 'Run throughput(ops/sec)' lines found in file: {result_file}")
    return values


def parse_rocksdb_result(result_file: Path) -> List[float]:
    text = result_file.read_text(encoding="utf-8", errors="replace")
    values = [float(x) for x in HCTREE_THROUGHPUT_RE.findall(text)]
    if not values:
        raise ValueError(f"No 'Run throughput(ops/sec)' lines found in file: {result_file}")
    return values


def summarize_result_file(result_file: Path) -> Tuple[str, str, int, List[float], float, float]:
    stem = result_file.stem
    workload = extract_workload(stem)
    system = extract_system(stem)
    writers = extract_writer_count(stem)
    if system == "hctree":
        runs = parse_hctree_result(result_file)
    elif system == "bcw2":
        runs = parse_bcw2_result(result_file)
    elif system == "rocksdb":
        runs = parse_rocksdb_result(result_file)
    else:
        runs = parse_ozonekv_result(result_file)

    avg = mean(runs)
    std = stdev(runs) if len(runs) > 1 else 0.0
    return workload, system, writers, runs, avg, std


def collect_results(input_dir: Path) -> Dict[str, Dict[str, Dict[int, Dict[str, object]]]]:
    data: Dict[str, Dict[str, Dict[int, Dict[str, object]]]] = defaultdict(lambda: defaultdict(dict))

    result_files = sorted(input_dir.glob("*.result"))
    if not result_files:
        raise FileNotFoundError(f"No .result files found in {input_dir}")

    for result_file in result_files:
        workload, system, writers, runs, avg, std = summarize_result_file(result_file)
        data[workload][system][writers] = {
            "runs": runs,
            "avg": avg,
            "std": std,
            "file": str(result_file),
        }

    return data


def infer_workload_order(data: Dict[str, Dict[str, Dict[int, Dict[str, object]]]]) -> List[str]:
    workloads = set(data.keys())
    ordered = [w for w in DEFAULT_WORKLOAD_ORDER if w in workloads]
    remaining = sorted(workloads - set(ordered))
    return ordered + remaining


def infer_system_order(data: Dict[str, Dict[str, Dict[int, Dict[str, object]]]]) -> List[str]:
    systems = set()
    for workload_data in data.values():
        systems.update(workload_data.keys())
    ordered = [s for s in DEFAULT_SYSTEM_ORDER if s in systems]
    remaining = sorted(systems - set(ordered))
    return ordered + remaining


def infer_writer_order(
    data: Dict[str, Dict[str, Dict[int, Dict[str, object]]]],
    requested_writers: List[int] | None,
) -> List[int]:
    if requested_writers:
        return requested_writers

    writers = set()
    for workload_data in data.values():
        for system_data in workload_data.values():
            writers.update(system_data.keys())
    return sorted(writers)


def make_plot(
    data: Dict[str, Dict[str, Dict[int, Dict[str, object]]]],
    output_path: Path,
    workloads: List[str],
    systems: List[str],
    writers: List[int],
    title: str,
    dpi: int,
) -> None:
    n_panels = len(workloads)
    ncols = 3
    nrows = math.ceil(n_panels / ncols)

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(16, 8.5), squeeze=False)
    axes_flat = axes.ravel()

    x = np.arange(len(writers), dtype=float)
    width = 0.8 / max(len(systems), 1)

    for idx, workload in enumerate(workloads):
        ax = axes_flat[idx]
        workload_data = data.get(workload, {})

        for sys_idx, system in enumerate(systems):
            system_data = workload_data.get(system, {})
            y = []
            yerr = []
            for writer in writers:
                point = system_data.get(writer)
                if point is None:
                    y.append(np.nan)
                    yerr.append(0.0)
                else:
                    y.append(float(point["avg"]))
                    yerr.append(float(point["std"]))

            offset = (sys_idx - (len(systems) - 1) / 2.0) * width
            ax.bar(
                x + offset,
                y,
                width=width,
                yerr=yerr,
                capsize=4,
                label=system,
                alpha=0.9,
            )

        ax.set_title(workload)
        ax.set_xticks(x)
        ax.set_xticklabels([str(w) for w in writers])
        ax.set_xlabel("Number of writers")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_yscale("log")
        ax.grid(True, axis="y", linestyle="--", alpha=0.35)

    # Collect handles/labels from the first populated subplot for the shared legend.
    shared_handles, shared_labels = [], []
    for ax in axes_flat:
        h, l = ax.get_legend_handles_labels()
        if h:
            shared_handles, shared_labels = h, l
            break

    # Hide empty subplots, then place the shared legend inside the last empty one.
    empty_axes = list(axes_flat[n_panels:])
    for ax in empty_axes:
        ax.axis("off")
    if shared_handles and empty_axes:
        empty_axes[-1].legend(
            shared_handles,
            shared_labels,
            loc="center",
            fontsize="large",
            frameon=True,
        )

    fig.suptitle(title)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    print(f"\nSaved plot to: {output_path}")


def print_summary(
    data: Dict[str, Dict[str, Dict[int, Dict[str, object]]]],
    workloads: List[str],
    systems: List[str],
    writers: List[int],
) -> None:
    print("Parsed throughput summary (mean ± std over runs):")
    for workload in workloads:
        print(f"\n[{workload}]")
        for system in systems:
            for writer in writers:
                point = data.get(workload, {}).get(system, {}).get(writer)
                if point is None:
                    continue
                runs_str = ", ".join(f"{v:.2f}" for v in point["runs"])
                print(
                    f"  {system:14s} writers={writer:<3d} "
                    f"avg={point['avg']:.2f} std={point['std']:.2f} runs=[{runs_str}]"
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot YCSB throughput from .result files.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing *.result files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("fig2_thread_scaling.pdf"),
        help="Output image path. Example: plot.png or plot.pdf",
    )
    parser.add_argument(
        "--writers",
        type=int,
        nargs="*",
        default=None,
        help="Optional explicit writer counts to plot, e.g. --writers 2 4 8 16 32",
    )
    parser.add_argument(
        "--title",
        type=str,
        default="YCSB Throughput: multi-threaded writer scaling",
        help="Figure title.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=200,
        help="Output DPI for raster images.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = collect_results(args.input_dir)
    workloads = infer_workload_order(data)
    systems = infer_system_order(data)
    writers = infer_writer_order(data, args.writers)

    print_summary(data, workloads, systems, writers)
    make_plot(
        data=data,
        output_path=args.input_dir / args.output,
        workloads=workloads,
        systems=systems,
        writers=writers,
        title=args.title,
        dpi=args.dpi,
    )


if __name__ == "__main__":
    main()
