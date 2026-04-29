import argparse
import math
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

plt.rcParams.update(
    {
        "font.size": 20,
        "axes.titlesize": 18,
        "axes.labelsize": 18,
        "xtick.labelsize": 18,
        "ytick.labelsize": 18,
        "legend.fontsize": 20,
        "figure.titlesize": 20,
    }
)


WORKLOAD_ORDER = ["load", "a", "b", "c", "d", "f"]
SYSTEM_ORDER = ["ozonedb", "rocksdb", "sqlite"]
SYSTEM_DISPLAY = {
    "ozonedb": "OzoneDB",
    "rocksdb": "RocksDB",
    "sqlite": "SQLite",
}


def build_workload_labels():
    workloads_dir = Path(__file__).resolve().parents[3] / "ycsb" / "workloads"
    labels = []
    for w in WORKLOAD_ORDER:
        if w == "load":
            labels.append("insert\n(I100)")
            continue
        path = workloads_dir / f"workload{w}"
        mix_text = "mix N/A"
        if path.exists():
            props = {}
            for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                props[k.strip()] = v.strip()
            read = float(props.get("readproportion", 0) or 0)
            insert = float(props.get("insertproportion", 0) or 0)
            update = float(props.get("updateproportion", 0) or 0)
            rmw = float(props.get("readmodifywriteproportion", 0) or 0)
            parts = []
            read_pct = int(round(read * 100))
            update_pct = int(round(update * 100))
            insert_pct = int(round(insert * 100))
            rmw_pct = int(round(rmw * 100))
            if read_pct > 0:
                parts.append(f"R{read_pct}")
            if update_pct > 0:
                parts.append(f"U{update_pct}")
            if insert_pct > 0:
                parts.append(f"I{insert_pct}")
            if rmw_pct > 0:
                parts.append(f"RMW{rmw_pct}")
            mix_text = ",".join(parts) if parts else "0"
        labels.append(f"ycsb {w}\n({mix_text})")
    return labels


def parse_result_file(file_path: Path):
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    throughput_values = []
    latency_values_ms = []
    current_op_count_by_name = {}
    current_weighted_latency_sum_us = 0.0
    current_total_ops = 0

    throughput_pattern = re.compile(r"^\[OVERALL\], Throughput\(ops/sec\),\s*([0-9.]+)")
    op_count_pattern = re.compile(r"^\[([A-Z\-]+)\], Operations,\s*(\d+)")
    op_avg_pattern = re.compile(r"^\[([A-Z\-]+)\], AverageLatency\(us\),\s*([0-9.]+)")

    for line in lines:
        m = throughput_pattern.match(line)
        if m:
            if throughput_values and current_total_ops > 0:
                latency_values_ms.append((current_weighted_latency_sum_us / current_total_ops) / 1000.0)
            throughput_values.append(float(m.group(1)))
            current_op_count_by_name = {}
            current_weighted_latency_sum_us = 0.0
            current_total_ops = 0
            continue

        m = op_count_pattern.match(line)
        if m:
            op = m.group(1)
            current_op_count_by_name[op] = int(m.group(2))
            continue

        m = op_avg_pattern.match(line)
        if m:
            op = m.group(1)
            if op == "CLEANUP":
                continue
            op_count = current_op_count_by_name.get(op)
            if op_count is None:
                continue
            avg_latency_us = float(m.group(2))
            current_weighted_latency_sum_us += op_count * avg_latency_us
            current_total_ops += op_count

    if throughput_values and current_total_ops > 0:
        latency_values_ms.append((current_weighted_latency_sum_us / current_total_ops) / 1000.0)

    throughput_mean = float(np.mean(throughput_values)) if throughput_values else math.nan
    throughput_std = float(np.std(throughput_values, ddof=0)) if throughput_values else math.nan
    latency_mean = float(np.mean(latency_values_ms)) if latency_values_ms else math.nan
    latency_std = float(np.std(latency_values_ms, ddof=0)) if latency_values_ms else math.nan

    return throughput_mean, throughput_std, latency_mean, latency_std


def collect_metrics(results_dir: Path, insert_results_dir: Path):
    data = {
        system: {
            w: {
                "throughput_mean": math.nan,
                "throughput_std": math.nan,
                "latency_mean_ms": math.nan,
                "latency_std_ms": math.nan,
            }
            for w in WORKLOAD_ORDER
        }
        for system in SYSTEM_ORDER
    }
    file_pattern = re.compile(r".*workload([abcdf])-([a-z0-9_]+)_t\d+\.result$")
    insert_pattern = re.compile(r".*-insert-([a-z0-9_]+)_t\d+\.result$")

    for result_file in results_dir.glob("*.result"):
        m = file_pattern.match(result_file.name)
        if not m:
            continue
        workload = m.group(1)
        system = m.group(2)
        if workload not in WORKLOAD_ORDER or system not in SYSTEM_ORDER:
            continue

        throughput_mean, throughput_std, latency_mean_ms, latency_std_ms = parse_result_file(result_file)
        data[system][workload]["throughput_mean"] = throughput_mean
        data[system][workload]["throughput_std"] = throughput_std
        data[system][workload]["latency_mean_ms"] = latency_mean_ms
        data[system][workload]["latency_std_ms"] = latency_std_ms

    for result_file in insert_results_dir.glob("*-insert-*.result"):
        m = insert_pattern.match(result_file.name)
        if not m:
            continue
        system = m.group(1)
        if system not in SYSTEM_ORDER:
            continue
        throughput_mean, throughput_std, latency_mean_ms, latency_std_ms = parse_result_file(result_file)
        data[system]["load"]["throughput_mean"] = throughput_mean
        data[system]["load"]["throughput_std"] = throughput_std
        data[system]["load"]["latency_mean_ms"] = latency_mean_ms
        data[system]["load"]["latency_std_ms"] = latency_std_ms

    return data


def plot_figure(data, output_path: Path):
    workload_labels = build_workload_labels()
    colors = {
        "ozonedb": "#1f77b4",
        "rocksdb": "#ff7f0e",
        "sqlite": "#2ca02c",
    }
    x = np.arange(len(WORKLOAD_ORDER))
    width = 0.18

    fig, axes = plt.subplots(1, 2, figsize=(16, 8), constrained_layout=True)
    fig.suptitle("Figure 1. Single-writer throughput and latency", fontsize=20, fontweight="bold")

    for idx, system in enumerate(SYSTEM_ORDER):
        offset = (idx - 1.5) * width
        throughput_values = [data[system][w]["throughput_mean"] for w in WORKLOAD_ORDER]
        throughput_errors = [data[system][w]["throughput_std"] for w in WORKLOAD_ORDER]
        latency_values = [data[system][w]["latency_mean_ms"] for w in WORKLOAD_ORDER]
        latency_errors = [data[system][w]["latency_std_ms"] for w in WORKLOAD_ORDER]

        axes[0].bar(
            x + offset,
            throughput_values,
            width=width,
            yerr=throughput_errors,
            capsize=3,
            label=SYSTEM_DISPLAY[system],
            color=colors[system],
            alpha=0.9,
        )
        axes[1].bar(
            x + offset,
            latency_values,
            width=width,
            yerr=latency_errors,
            capsize=3,
            label=SYSTEM_DISPLAY[system],
            color=colors[system],
            alpha=0.9,
        )

    axes[0].set_ylabel("Throughput (ops/s)")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(workload_labels, fontsize=14)
    axes[0].grid(axis="y", linestyle="--", alpha=0.4)

    axes[1].set_ylabel("Avg Latency (ms)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(workload_labels, fontsize=14)
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=len(SYSTEM_ORDER),
        frameon=False,
        bbox_to_anchor=(0.5, -0.08),

    )

    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved figure to: {output_path}")

    missing = []
    for system in SYSTEM_ORDER:
        for workload in WORKLOAD_ORDER:
            if math.isnan(data[system][workload]["throughput_mean"]):
                missing.append(f"{SYSTEM_DISPLAY[system]} / ycsb {workload}")
    if missing:
        banner = "!" * 80
        print(banner, file=sys.stderr)
        print(
            f"WARNING: {len(missing)} (system, workload) cell(s) have no [OVERALL] data.",
            file=sys.stderr,
        )
        print("These cells will appear empty in the plot:", file=sys.stderr)
        for entry in missing:
            print(f"  - {entry}", file=sys.stderr)
        print(
            "Inspect the corresponding .result files for crashes or empty runs.",
            file=sys.stderr,
        )
        print(banner, file=sys.stderr)


# ---------------------------------------------------------------------------
# Independent path: HCTree / BCW2 / RocksDB single-writer results produced by
#   YCSB-cpp/run_benchmark_hctree.sh
#   YCSB-cpp/run_benchmark_bcw2.sh
#   YCSB-cpp/run_benchmark_rocksdb.sh
# Result filenames: 1KB-dur<S>s-<RC>-workload<X>-(hctree|bcw2|rocksdb)_t<N>.result
# ---------------------------------------------------------------------------

HBR_WORKLOAD_ORDER = ["a", "b", "c", "d", "f"]
HBR_SYSTEM_ORDER = ["hctree", "bcw2", "rocksdb"]
HBR_SYSTEM_DISPLAY = {
    "hctree": "HCTree",
    "bcw2": "BCW2",
    "rocksdb": "RocksDB",
}
HBR_SYSTEM_COLORS = {
    "hctree": "#1f77b4",
    "bcw2": "#9467bd",
    "rocksdb": "#ff7f0e",
}


def build_workload_labels_hbr():
    workloads_dir = Path(__file__).resolve().parents[3] / "ycsb" / "workloads"
    labels = []
    for w in HBR_WORKLOAD_ORDER:
        path = workloads_dir / f"workload{w}"
        mix_text = "mix N/A"
        if path.exists():
            props = {}
            for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                props[k.strip()] = v.strip()
            read = float(props.get("readproportion", 0) or 0)
            insert = float(props.get("insertproportion", 0) or 0)
            update = float(props.get("updateproportion", 0) or 0)
            rmw = float(props.get("readmodifywriteproportion", 0) or 0)
            parts = []
            if int(round(read * 100)) > 0:
                parts.append(f"R{int(round(read * 100))}")
            if int(round(update * 100)) > 0:
                parts.append(f"U{int(round(update * 100))}")
            if int(round(insert * 100)) > 0:
                parts.append(f"I{int(round(insert * 100))}")
            if int(round(rmw * 100)) > 0:
                parts.append(f"RMW{int(round(rmw * 100))}")
            mix_text = ",".join(parts) if parts else "0"
        labels.append(f"ycsb {w}\n({mix_text})")
    return labels


def parse_result_file_ycsbcpp(file_path: Path, thread_count: int = 1):
    """Parse a YCSB-cpp native-format result file.

    YCSB-cpp emits per-run lines:
      Run runtime(sec): <float>
      Run operations(ops): <int>
      Run throughput(ops/sec): <float>

    Avg op latency (ms) is derived as thread_count * 1000 / throughput,
    i.e. the per-op wall latency assuming threads are saturated.
    """
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    throughput_values = []
    latency_values_ms = []

    thr_re = re.compile(r"^Run throughput\(ops/sec\):\s*([0-9.]+)\s*$")
    for line in text.splitlines():
        m = thr_re.match(line)
        if m:
            thr = float(m.group(1))
            throughput_values.append(thr)
            if thr > 0:
                latency_values_ms.append(thread_count * 1000.0 / thr)

    thr_mean = float(np.mean(throughput_values)) if throughput_values else math.nan
    thr_std = float(np.std(throughput_values, ddof=0)) if throughput_values else math.nan
    lat_mean = float(np.mean(latency_values_ms)) if latency_values_ms else math.nan
    lat_std = float(np.std(latency_values_ms, ddof=0)) if latency_values_ms else math.nan
    return thr_mean, thr_std, lat_mean, lat_std


def collect_metrics_hbr(results_dir: Path, thread_count: int = 1):
    """Parse HCTree/BCW2/RocksDB result files for a fixed thread count.

    Returns nested dict: data[system][workload] -> metric dict.
    """
    data = {
        system: {
            w: {
                "throughput_mean": math.nan,
                "throughput_std": math.nan,
                "latency_mean_ms": math.nan,
                "latency_std_ms": math.nan,
            }
            for w in HBR_WORKLOAD_ORDER
        }
        for system in HBR_SYSTEM_ORDER
    }
    pattern = re.compile(
        r".*workload([abcdf])-(hctree|bcw2|rocksdb)_t(\d+)\.result$"
    )
    for result_file in results_dir.glob("*.result"):
        m = pattern.match(result_file.name)
        if not m:
            continue
        workload, system, t = m.group(1), m.group(2), int(m.group(3))
        if t != thread_count:
            continue
        if workload not in HBR_WORKLOAD_ORDER or system not in HBR_SYSTEM_ORDER:
            continue
        thr_mean, thr_std, lat_mean, lat_std = parse_result_file_ycsbcpp(result_file, thread_count=thread_count)
        data[system][workload]["throughput_mean"] = thr_mean
        data[system][workload]["throughput_std"] = thr_std
        data[system][workload]["latency_mean_ms"] = lat_mean
        data[system][workload]["latency_std_ms"] = lat_std
    return data


def plot_figure_hbr(data, output_path: Path, title: str = "Single-writer: HCTree vs BCW2 vs RocksDB"):
    workload_labels = build_workload_labels_hbr()
    x = np.arange(len(HBR_WORKLOAD_ORDER))
    width = 0.25

    fig, axes = plt.subplots(1, 2, figsize=(16, 8), constrained_layout=True)
    fig.suptitle(title, fontsize=20, fontweight="bold")

    for idx, system in enumerate(HBR_SYSTEM_ORDER):
        offset = (idx - 1) * width
        thr_values = [data[system][w]["throughput_mean"] for w in HBR_WORKLOAD_ORDER]
        thr_errors = [data[system][w]["throughput_std"] for w in HBR_WORKLOAD_ORDER]
        lat_values = [data[system][w]["latency_mean_ms"] for w in HBR_WORKLOAD_ORDER]
        lat_errors = [data[system][w]["latency_std_ms"] for w in HBR_WORKLOAD_ORDER]

        axes[0].bar(
            x + offset, thr_values, width=width, yerr=thr_errors, capsize=3,
            label=HBR_SYSTEM_DISPLAY[system], color=HBR_SYSTEM_COLORS[system], alpha=0.9,
        )
        axes[1].bar(
            x + offset, lat_values, width=width, yerr=lat_errors, capsize=3,
            label=HBR_SYSTEM_DISPLAY[system], color=HBR_SYSTEM_COLORS[system], alpha=0.9,
        )

    axes[0].set_ylabel("Throughput (ops/s)")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(workload_labels, fontsize=14)
    axes[0].grid(axis="y", linestyle="--", alpha=0.4)

    axes[1].set_ylabel("Avg Latency (ms)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(workload_labels, fontsize=14)
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles, labels, loc="lower center", ncol=len(HBR_SYSTEM_ORDER),
        frameon=False, bbox_to_anchor=(0.5, -0.08),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved figure to: {output_path}")

    missing = []
    for system in HBR_SYSTEM_ORDER:
        for workload in HBR_WORKLOAD_ORDER:
            if math.isnan(data[system][workload]["throughput_mean"]):
                missing.append(f"{HBR_SYSTEM_DISPLAY[system]} / ycsb {workload}")
    if missing:
        banner = "!" * 80
        print(banner, file=sys.stderr)
        print(
            f"WARNING: {len(missing)} (system, workload) cell(s) have no [OVERALL] data.",
            file=sys.stderr,
        )
        for entry in missing:
            print(f"  - {entry}", file=sys.stderr)
        print(banner, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Plot single-writer YCSB results. Two modes: 'fig1' (ozonedb/rocksdb/sqlite) and 'hbr' (hctree/bcw2/rocksdb)."
    )
    parser.add_argument(
        "--mode",
        choices=["fig1", "hbr"],
        default="fig1",
        help="fig1: original OzoneDB/RocksDB/SQLite plot. hbr: HCTree/BCW2/RocksDB plot from run_benchmark_*.sh.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=None,
        help="Directory containing *.result files. Defaults vary by --mode.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output image path. Defaults vary by --mode.",
    )
    parser.add_argument(
        "--insert-results-dir",
        type=Path,
        default=Path("../../results/local/fig1-single-writer-local"),
        help="(fig1 only) Directory containing *-insert-*.result files.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="(hbr only) Thread count to plot (single-writer = 1).",
    )
    args = parser.parse_args()

    if args.mode == "fig1":
        results_dir = args.results_dir or Path("../../results/local/fig1-single-writer-local")
        output = args.output or Path("../../results/local/fig1-single-writer-local/figure1_single_writer.pdf")
        if not results_dir.exists():
            raise FileNotFoundError(f"Results directory does not exist: {results_dir}")
        if not args.insert_results_dir.exists():
            raise FileNotFoundError(f"Insert results directory does not exist: {args.insert_results_dir}")
        data = collect_metrics(results_dir, args.insert_results_dir)
        output.parent.mkdir(parents=True, exist_ok=True)
        plot_figure(data, output)
    else:
        results_dir = args.results_dir or Path("../../results/local/fig2-multi-writer-local")
        output = args.output or Path("../../results/local/fig2-multi-writer-local/single_writer_hctree_bcw2_rocksdb.pdf")
        if not results_dir.exists():
            raise FileNotFoundError(f"Results directory does not exist: {results_dir}")
        data = collect_metrics_hbr(results_dir, thread_count=args.threads)
        plot_figure_hbr(data, output)


if __name__ == "__main__":
    main()
