# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "matplotlib",
#     "numpy",
# ]
# ///

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


WORKLOAD_ORDER = ["a", "b", "c", "d", "f"]
SYSTEM_ORDER = ["ozonedb", "slatedb"]
SYSTEM_DISPLAY = {
    "ozonedb": "OzoneDB",
    "slatedb": "SlateDB",
}


def build_workload_labels():
    workloads_dir = Path(__file__).resolve().parents[3] / "ycsb" / "workloads"
    labels = []
    for w in WORKLOAD_ORDER:
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

    throughput = math.nan
    op_counts = {}
    op_latencies_us = {}

    throughput_pat = re.compile(
        r"^\[(?:OVERALL|AGGREGATE)\], Throughput\(ops/sec\),\s*([0-9.]+)"
    )
    op_count_pat = re.compile(r"^\[([A-Z\-]+)\], Operations,\s*(\d+)")
    op_latency_pat = re.compile(
        r"^\[([A-Z\-]+)\], (?:Weighted)?AverageLatency\(us\),\s*([0-9.]+)"
    )

    for line in text.splitlines():
        m = throughput_pat.match(line)
        if m:
            throughput = float(m.group(1))
            continue
        m = op_count_pat.match(line)
        if m:
            op_counts[m.group(1)] = int(m.group(2))
            continue
        m = op_latency_pat.match(line)
        if m:
            op = m.group(1)
            if op != "CLEANUP":
                op_latencies_us[op] = float(m.group(2))

    weighted_sum_us = 0.0
    total_ops = 0
    for op, count in op_counts.items():
        if op == "CLEANUP" or op not in op_latencies_us:
            continue
        weighted_sum_us += count * op_latencies_us[op]
        total_ops += count

    latency_ms = (weighted_sum_us / total_ops / 1000.0) if total_ops > 0 else math.nan
    return throughput, latency_ms


def collect_metrics(results_dir: Path):
    data = {
        system: {
            w: {"throughput": math.nan, "latency_ms": math.nan}
            for w in WORKLOAD_ORDER
        }
        for system in SYSTEM_ORDER
    }

    ozonedb_pat = re.compile(r".*workload([abcdf])-ozonedb-corfu_agg_w\d+_t\d+\.result$")
    slatedb_pat = re.compile(r"ycsb_workload([abcdf])_run_w\d+.*\.txt$")

    for f in results_dir.iterdir():
        if not f.is_file():
            continue
        m = ozonedb_pat.match(f.name)
        if m:
            w = m.group(1)
            if w in WORKLOAD_ORDER:
                tp, lat = parse_result_file(f)
                data["ozonedb"][w]["throughput"] = tp
                data["ozonedb"][w]["latency_ms"] = lat
            continue
        m = slatedb_pat.match(f.name)
        if m:
            w = m.group(1)
            if w in WORKLOAD_ORDER:
                tp, lat = parse_result_file(f)
                data["slatedb"][w]["throughput"] = tp
                data["slatedb"][w]["latency_ms"] = lat

    return data


def plot_figure(data, output_path: Path):
    workload_labels = build_workload_labels()
    colors = {
        "ozonedb": "#1f77b4",
        "slatedb": "#ff7f0e",
    }
    x = np.arange(len(WORKLOAD_ORDER))
    width = 0.25

    fig, axes = plt.subplots(1, 2, figsize=(16, 8), constrained_layout=True)
    fig.suptitle(
        "Figure 1. Single-writer throughput and latency",
        fontsize=20,
        fontweight="bold",
    )

    for idx, system in enumerate(SYSTEM_ORDER):
        offset = (idx - 0.5) * width
        tp_vals = [data[system][w]["throughput"] for w in WORKLOAD_ORDER]
        lat_vals = [data[system][w]["latency_ms"] for w in WORKLOAD_ORDER]

        axes[0].bar(
            x + offset, tp_vals, width=width,
            label=SYSTEM_DISPLAY[system], color=colors[system], alpha=0.9,
        )
        axes[1].bar(
            x + offset, lat_vals, width=width,
            label=SYSTEM_DISPLAY[system], color=colors[system], alpha=0.9,
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
        handles, labels,
        loc="lower center", ncol=len(SYSTEM_ORDER),
        frameon=False, bbox_to_anchor=(0.5, -0.08),
    )

    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved figure to: {output_path}")

    missing = []
    for system in SYSTEM_ORDER:
        for workload in WORKLOAD_ORDER:
            if math.isnan(data[system][workload]["throughput"]):
                missing.append(f"{SYSTEM_DISPLAY[system]} / ycsb {workload}")
    if missing:
        print("Missing data points:")
        for entry in missing:
            print(f"  - {entry}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot single-writer throughput and latency from YCSB result files."
    )
    parser.add_argument(
        "--results-dir", type=Path,
        default=Path(__file__).resolve().parents[2] / "results" / "final",
        help="Directory containing result files.",
    )
    parser.add_argument(
        "--output", type=Path,
        default=None,
        help="Output image path (default: <results-dir>/figure1_single_writer.pdf).",
    )
    args = parser.parse_args()

    if not args.results_dir.exists():
        raise FileNotFoundError(f"Results directory does not exist: {args.results_dir}")

    output = args.output or (args.results_dir / "figure1_single_writer.pdf")
    output.parent.mkdir(parents=True, exist_ok=True)

    data = collect_metrics(args.results_dir)
    plot_figure(data, output)


if __name__ == "__main__":
    main()
