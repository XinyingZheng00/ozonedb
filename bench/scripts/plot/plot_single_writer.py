import argparse
import math
import re
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


WORKLOAD_ORDER = ["load", "a", "b", "c", "f"]
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
            mix_text = " ".join(parts) if parts else "0"
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
    axes[0].set_xticklabels(workload_labels)
    axes[0].grid(axis="y", linestyle="--", alpha=0.4)

    axes[1].set_ylabel("Avg Latency (ms)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(workload_labels)
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=4,
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
        print("Missing data points:")
        for entry in missing:
            print(f"  - {entry}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot Figure 1 (single-writer throughput and latency) from YCSB result files."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("../../results/local/fig1-single-writer-local"),
        help="Directory containing *.result files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("../../results/local/fig1-single-writer-local/figure1_single_writer.pdf"),
        help="Output image path.",
    )
    parser.add_argument(
        "--insert-results-dir",
        type=Path,
        default=Path("../../results/local"),
        help="Directory containing *-insert-*.result files.",
    )
    args = parser.parse_args()

    if not args.results_dir.exists():
        raise FileNotFoundError(f"Results directory does not exist: {args.results_dir}")

    if not args.insert_results_dir.exists():
        raise FileNotFoundError(f"Insert results directory does not exist: {args.insert_results_dir}")

    data = collect_metrics(args.results_dir, args.insert_results_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    plot_figure(data, args.output)


if __name__ == "__main__":
    main()
