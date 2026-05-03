#!/usr/bin/env python3
"""Generate v4 plots from a results dir.

Splits the CSV by `vary` column into the writers-vary and abort-vary
plots:
  results/v4_<ts>/plots/completion_time_vs_writers.pdf   (vary=writers)
  results/v4_<ts>/plots/completion_time_vs_abort.pdf     (vary=abort)
  results/v4_<ts>/plots/max_generation.pdf          (combined)
  results/v4_<ts>/plots/generation_dist.pdf         (combined)

The 8 s quiescence-grace floor (orchestrator waits for n_total to be
flat for 8 s before declaring completed) is annotated on the plots and
also reported as "actual completion time ≈ completion_time_s − 8 s".
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


QUIESCENCE_GRACE_S = 8.0


def to_float(s: str) -> float | None:
    if s == "" or s.lower() == "none":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_trials(csv_path: Path) -> list[dict]:
    with csv_path.open() as f:
        return list(csv.DictReader(f))


def parse_gen_dist(s: str) -> dict[int, int]:
    out = {}
    if not s:
        return out
    for tok in s.split(";"):
        if ":" not in tok:
            continue
        k, v = tok.split(":")
        try:
            out[int(k)] = int(v)
        except ValueError:
            continue
    return out


# -----------------------------------------------------------------------------
# Plot 1: completion time vs # writers (vary=writers)
# -----------------------------------------------------------------------------


def plot_completion_vs_writers(rows: list[dict], out: Path) -> None:
    rows = [r for r in rows if r["vary"] == "writers"]
    rows.sort(key=lambda r: int(r["n_writers"]))
    if not rows:
        return
    xs = [int(r["n_writers"]) for r in rows]
    ys_total = [to_float(r["completion_time_s"]) or 0 for r in rows]
    ys_actual = [max(0.001, y - QUIESCENCE_GRACE_S) for y in ys_total]
    # Throughput: workload data (1 KB/record) divided by actual completion time.
    # Records-per-second × 1KB / 1MB → MB/s of user data fully processed
    # through the entire compaction cascade.
    ys_speed = [
        (int(r["record_count"]) / (1024.0)) / t
        for r, t in zip(rows, ys_actual)
    ]

    fig, axes = plt.subplots(1, 2, figsize=(7, 3))
    ax_t, ax_s = axes

    ax_t.plot(xs, ys_actual, "s-", color="#ff7f0e", label="actual completion time")
    ax_t.set_xscale("log", base=2)
    ax_t.set_xticks(xs)
    ax_t.set_xticklabels([str(x) for x in xs])
    ax_t.set_xlabel("Number of Writers")
    ax_t.set_ylabel("Completion time (s)")
    # ax_t.set_title("Completion time vs. # writers")
    ax_t.grid(True, linestyle=":", alpha=0.5)

    ax_s.plot(xs, ys_speed, "o-", color="#1f77b4", label="compaction speed")
    ax_s.set_xscale("log", base=2)
    ax_s.set_xticks(xs)
    ax_s.set_xticklabels([str(x) for x in xs])
    ax_s.set_xlabel("Number of Writers")
    ax_s.set_ylabel("Compaction speed (MB/s)")
    # ax_s.set_title("Compaction speed vs. # writers")
    ax_s.grid(True, linestyle=":", alpha=0.5)

    fig.suptitle("Distributed Compaction Efficiency (Without Churn)", fontsize=12)
    fig.tight_layout(rect=[0, 0.05, 1, 1.03])
    # Panel labels below each subplot, in figure coordinates.
    for ax, label in ((ax_t, "(a)"), (ax_s, "(b)")):
        bbox = ax.get_position()
        fig.text((bbox.x0 + bbox.x1) / 2, 0.04, label,
                 ha="center", va="bottom", fontsize=11)
    fig.savefig(out)
    plt.close(fig)


# -----------------------------------------------------------------------------
# Plot 2: completion time vs abort rate (vary=abort)
# -----------------------------------------------------------------------------


def plot_completion_vs_abort(rows: list[dict], out: Path) -> None:
    rows = [r for r in rows if r["vary"] == "abort"]
    if not rows:
        return

    from collections import defaultdict
    groups: dict[float, list[dict]] = defaultdict(list)
    for r in rows:
        groups[float(r["abort_rate"])].append(r)
    rates = sorted(groups.keys())

    xs = [100 * r for r in rates]
    means_total: list[float] = []
    lows_total: list[float] = []
    highs_total: list[float] = []
    means_actual: list[float] = []
    max_gens: list[int] = []
    for rate in rates:
        vals = [to_float(r["completion_time_s"]) or 0 for r in groups[rate]]
        m = sum(vals) / len(vals)
        means_total.append(m)
        lows_total.append(m - min(vals))
        highs_total.append(max(vals) - m)
        means_actual.append(max(0.0, m - QUIESCENCE_GRACE_S))
        max_gens.append(max(int(float(r["max_generation_observed"]))
                            for r in groups[rate]))

    fig, ax = plt.subplots(figsize=(6, 3.8))
    ax.errorbar(xs, means_total, yerr=[lows_total, highs_total],
                fmt="o-", color="#1f77b4", capsize=3,
                label="total wallclock (mean, min–max)")
    ax.plot(xs, means_actual, "s--", color="#ff7f0e",
            label=f"actual completion time (− {QUIESCENCE_GRACE_S:.0f} s grace)")
    ax.axhline(QUIESCENCE_GRACE_S, color="#888", linestyle=":", linewidth=1,
               label=f"{QUIESCENCE_GRACE_S:.0f} s quiescence grace floor")
    ax.set_xlabel("Abort rate R (%)")
    ax.set_ylabel("Completion time (s)")
    ax.set_title("Completion time vs. abort rate (N=4 writers)")
    ax.grid(True, linestyle=":", alpha=0.5)
    ax.legend(loc="upper left", fontsize=8)

    for x, yt, mg in zip(xs, means_total, max_gens):
        ax.annotate(f"max_gen={mg}", (x, yt),
                    textcoords="offset points", xytext=(6, 6),
                    fontsize=8, color="#444")

    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


# -----------------------------------------------------------------------------
# Plot 3: max generation observed across both varies
# -----------------------------------------------------------------------------


def plot_max_generation(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.5))
    # Left: vary writers
    wrows = [r for r in rows if r["vary"] == "writers"]
    wrows.sort(key=lambda r: int(r["n_writers"]))
    if wrows:
        xs = [int(r["n_writers"]) for r in wrows]
        mg = [int(float(r["max_generation_observed"])) for r in wrows]
        axes[0].bar([str(x) for x in xs], mg, color="#1f77b4")
        axes[0].set_xlabel("# writers (N)")
        axes[0].set_ylabel("max_generation observed")
        axes[0].set_title("Writers (vary N) (R=0%)")
        axes[0].grid(True, axis="y", linestyle=":", alpha=0.5)

    # Right: vary abort
    arows = [r for r in rows if r["vary"] == "abort"]
    arows.sort(key=lambda r: float(r["abort_rate"]))
    if arows:
        xs = [100 * float(r["abort_rate"]) for r in arows]
        mg = [int(float(r["max_generation_observed"])) for r in arows]
        axes[1].bar([f"{x:.0f}%" for x in xs], mg, color="#ff7f0e")
        axes[1].set_xlabel("Abort rate R (%)")
        axes[1].set_ylabel("max_generation observed")
        axes[1].set_title("Abort (vary R) (N=4)")
        axes[1].grid(True, axis="y", linestyle=":", alpha=0.5)

    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


# -----------------------------------------------------------------------------
# Plot 4: generation distribution per trial
# -----------------------------------------------------------------------------


def plot_generation_distribution(rows: list[dict], out: Path) -> None:
    """Stacked horizontal bars: one row per abort rate (mean across reps),
    segments colored by final generation. Shows what fraction of tasks
    completed cleanly (gen 0) vs. needed retries."""
    if not rows:
        return
    abort_rows = [r for r in rows if r["vary"] == "abort"]
    if not abort_rows:
        return

    # Group by abort rate, aggregate generation distributions per R
    from collections import defaultdict
    groups: dict[float, list[dict]] = defaultdict(list)
    for r in abort_rows:
        groups[float(r["abort_rate"])].append(r)

    rates = sorted(groups.keys())
    all_gens: set[int] = set()
    per_rate_mean: dict[float, dict[int, float]] = {}
    for rate in rates:
        rs = groups[rate]
        summed: dict[int, int] = {}
        for r in rs:
            for g, c in parse_gen_dist(r["generation_distribution"]).items():
                summed[g] = summed.get(g, 0) + c
        n = len(rs)
        per_rate_mean[rate] = {g: c / n for g, c in summed.items()}
        all_gens.update(summed.keys())
    gens = sorted(all_gens)

    fig, ax = plt.subplots(figsize=(4, 0.4 + 0.55 * len(rates)))
    ys = list(range(len(rates)))
    # Normalize each row so all bars span 0..1 (proportional view).
    totals = [sum(per_rate_mean[rate].values()) or 1.0 for rate in rates]
    lefts = [0.0] * len(rates)
    cmap = plt.get_cmap("viridis")
    for i, g in enumerate(gens):
        widths = [per_rate_mean[rate].get(g, 0.0) / tot
                  for rate, tot in zip(rates, totals)]
        ax.barh(ys, widths, left=lefts,
                label=f"v{g}",
                color=cmap(i / max(1, len(gens) - 1)),
                edgecolor="white", linewidth=0.5)
        lefts = [l + w for l, w in zip(lefts, widths)]
    ax.set_yticks(ys)
    ax.set_yticklabels([f"R={int(100 * r)}%" for r in rates])
    ax.invert_yaxis()
    ax.set_xlim(0, 1)
    ax.set_xlabel("Fraction of tasks")
    ax.set_title("Version Number Distribution by Abort Rate")
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5),
              fontsize=8, title="Version \nNumber", frameon=False)
    ax.grid(True, axis="x", linestyle=":", alpha=0.5)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("trials_csv", type=Path)
    args = p.parse_args()
    if not args.trials_csv.exists():
        print(f"missing: {args.trials_csv}", file=sys.stderr)
        return 1
    results_dir = args.trials_csv.parent
    plots_dir = results_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    rows = load_trials(args.trials_csv)
    plot_completion_vs_writers(rows, plots_dir / "completion_time_vs_writers.pdf")
    plot_completion_vs_abort(rows, plots_dir / "completion_time_vs_abort.pdf")
    plot_max_generation(rows, plots_dir / "max_generation.pdf")
    plot_generation_distribution(rows, plots_dir / "generation_dist.pdf")
    print(f"plots -> {plots_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
