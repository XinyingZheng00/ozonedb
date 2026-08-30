#!/usr/bin/env python3
"""The paper's cost figure, drawn from the same model as plot_cost_model.py.

  paper_cost_figure.py results.tsv prices.json --space space.json
                       [--tier-variant ch64k-adm] [--cassandra-mode serial]
                       [--cpu-workload a] [--h-workload c] [--read-fraction 0.25]
                       --out bench/fig_cost

Writes <out>.{pdf,png} (double-column: panel (a) the projection, panel (b) the
bill at 10 TB) and <out>_col.{pdf,png} (single column: panel (a) alone).

Panel (a) is plot_cost_model.py's figure cut to what the text argues from:
three series (OzoneKV with the high RAM cache, Cassandra on EBS, Cassandra on
local NVMe as the gray context line; --tier adds OzoneKV with an SSD tier per
writer), the x-axis from the smallest measured dataset, every line solid across the
measured decade and dotted beyond it, filled markers on the measured sizes
and none beyond, one "cheaper from" marker per OzoneKV line,
and the 10 TB and 100 TB value labels. --read-fraction overrides
prices.json's projection.read_fraction (the offered mix; writes are blind
puts, so this is the insert workload `ai` of PLAN-cost-2, not workload a).
Panel (b) is one decade of the same model split into its cost lines.
Everything is computed here from Coefficients and Model; nothing is typed in.
"""

import argparse
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FixedLocator, NullFormatter, NullLocator  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plot_cost_model as pcm  # noqa: E402

GB = pcm.GB
TB = 1000.0 * GB

# Series hues (validated categorical slots, light surface): OzoneKV blue,
# the tier aqua, Cassandra EBS orange; Cassandra NVMe is context, so gray.
BLUE, AQUA, ORANGE, GRAY = "#2a78d6", "#1baf7a", "#eb6834", "#8a8a82"
INK, INK2 = "#1f1f1e", "#5f5f5a"
# Panel (b): one ramp per system, darkest = largest line.
BLUES = ["#1c5cab", "#3987e5", "#6da7ec", "#9ec5f4"]
ORANGES = ["#c94f22", "#f0906a"]


def fmt_usd(v):
    return f"${v:,.0f}"


def fmt_d(d_bytes, sig=None):
    gb = d_bytes / GB
    v, unit = (gb / 1000, "TB") if gb >= 1000 else (gb, "GB")
    if sig:
        v = float(f"{v:.{sig}g}")
    return f"{v:g} {unit}"


def build(args):
    rows = pcm.load_rows(args.coefficients_tsv)
    with open(args.prices_json) as f:
        prices = json.load(f)
    if args.read_fraction is not None:
        prices["projection"]["read_fraction"] = args.read_fraction
    space = {}
    if args.space:
        with open(args.space) as f:
            space = {k: v for k, v in json.load(f).items() if not k.startswith("_")}
    coef = pcm.Coefficients(
        rows,
        space,
        args.h_workload,
        read_fraction=prices["projection"].get("read_fraction", 0.5),
        tier_variant_filter=args.tier_variant,
        cpu_workload=args.cpu_workload,
        cassandra_mode=args.cassandra_mode,
    )
    coef.report()
    return coef, pcm.Model(coef, prices), prices


def cheaper_from(ds, ozone, cass):
    """The smallest D from which OzoneKV stays cheaper than the cheaper
    Cassandra layout over the rest of the grid. plot_cost_model.find_crossover
    returns the first dip instead; between 9 and 13 TB the lines sit within
    1 % of each other and dip in and out with the node steps, so the first dip
    depends on the grid and the last one is the number the text can quote."""
    last_dearer = None
    for i, (o, c) in enumerate(zip(ozone, cass)):
        if o >= c:
            last_dearer = i
    if last_dearer is None:
        return ds[0]
    return ds[last_dearer + 1] if last_dearer + 1 < len(ds) else None


def series(coef, model, prices, tier=False):
    pr = prices["projection"]
    # The x-axis starts at the decade of the smallest measured dataset
    # (10.24 GB -> 10 GB); everything to its right is the model.
    d_min_gb = pr["d_min_gb"]
    if coef.measured_d:
        d_min_gb = 10 ** int(round(pcm.math.log10(min(coef.measured_d) / GB)))
    ds = pcm.grid(d_min_gb, pr["d_max_gb"], per_decade=48)
    # The SSD tier is opt-in (--tier): off, disk_gb is 0 and every tier
    # series, marker, label and bar is skipped.
    disk_gb = float(pr.get("disk_gb_per_client", 0)) if tier else 0.0
    hi, lo = pr["cache_gb_high"], pr["cache_gb_low"]
    s = {
        "ds": ds,
        "cass_nvme": [model.cassandra(d, "nvme")["total"] for d in ds],
        "cass_ebs": [model.cassandra(d, "ebs")["total"] for d in ds],
        "oz_hi": [model.ozonedb(d, hi)["total"] for d in ds],
        "oz_disk": [model.ozonedb(d, hi, disk_gb=disk_gb)["total"] for d in ds],
        "disk_gb": disk_gb,
        "hi": hi,
        "lo": lo,
    }
    cass_min = [min(a, b) for a, b in zip(s["cass_nvme"], s["cass_ebs"])]
    s["x_oz"] = cheaper_from(ds, s["oz_hi"], cass_min)
    s["x_disk"] = cheaper_from(ds, s["oz_disk"], cass_min) if disk_gb else None
    # The tier holds the dataset while its own hit rate is still that of a
    # full tier: the last grid point at or above 0.95.
    full = (
        [d for d in ds if model.ozonedb(d, hi, disk_gb=disk_gb)["h_tier"] >= 0.95]
        if disk_gb
        else []
    )
    s["tier_full_to"] = max(full) if full else None
    return s


def panel_a(ax, coef, model, prices, s, single):
    ds = s["ds"]
    x = [d / GB for d in ds]
    fs = 6.5 if single else 7
    ax.set_xscale("log")
    ax.set_yscale("log")
    ymin, ymax = 1000.0, 250000.0
    ax.set_ylim(ymin, ymax)
    ax.set_xlim(x[0] / 1.12, x[-1] * (5.0 if single else 3.2))

    # Solid across the measured decade, dotted where only the model speaks.
    # The cut sits on the last grid point at or below the largest measured
    # size; both segments share that point, so each line stays continuous.
    bx = max(coef.measured_d) / GB
    cut = max(i for i, v in enumerate(x) if v <= bx)

    def line(y, **kw):
        label = kw.pop("label", None)
        ax.plot(x[: cut + 1], y[: cut + 1], ls="-", label=label, **kw)
        ax.plot(x[cut:], y[cut:], ls=(0, (1, 1.6)), **kw)

    line(s["cass_nvme"], color=GRAY, lw=1.4, label="Cassandra, NVMe i4i")
    line(s["cass_ebs"], color=ORANGE, lw=1.8, label="Cassandra, EBS gp3")
    line(s["oz_hi"], color=BLUE, lw=1.8, label="OzoneKV")
    if s["disk_gb"]:
        line(
            s["oz_disk"],
            color=AQUA,
            lw=1.8,
            label=f"OzoneKV + {s['disk_gb'] / 1000:g} TB SSD tier per writer",
        )

    # Measured sizes: filled markers on every line, none beyond.
    for d in coef.measured_d:
        for key, col in (
            ("oz_hi", BLUE),
            ("oz_disk", AQUA),
            ("cass_ebs", ORANGE),
            ("cass_nvme", GRAY),
        ):
            if key == "oz_disk" and not s["disk_gb"]:
                continue
            y = (
                model.ozonedb(
                    d, s["hi"], disk_gb=s["disk_gb"] if key == "oz_disk" else 0
                )["total"]
                if key.startswith("oz")
                else model.cassandra(d, "ebs" if key == "cass_ebs" else "nvme")["total"]
            )
            ax.plot(
                [d / GB], [y], "o", color=col, ms=3.8, mec="white", mew=0.6, zorder=5
            )
    ax.plot([], [], "o", color=INK2, ms=3.8, mec="white", mew=0.6, label="measured")
    ax.plot([], [], ls=(0, (1, 1.6)), color=INK2, lw=1.4, label="projected")

    # One exact-cost label per line, at its right end (100 TB). The x-axis
    # runs a little past the last decade so the labels sit in clear space.
    ends = [("cass_nvme", "Cassandra NVMe"), ("cass_ebs", "Cassandra EBS"), ("oz_hi", "OzoneKV")]
    if s["disk_gb"]:
        ends.append(("oz_disk", "OzoneKV + tier"))
    for key, _ in ends:
        ax.annotate(
            fmt_usd(s[key][-1]),
            (x[-1], s[key][-1]),
            xytext=(4, 0),
            textcoords="offset points",
            ha="left",
            va="center",
            fontsize=fs,
            color=INK,
        )

    # Marker labels along the top.
    ytop = ymax / 1.25
    # One "cheaper from" marker per OzoneKV line: the size from which the line
    # stays below the cheaper Cassandra layout. The earlier one is labelled to
    # its left, the later one to its right, so the labels never meet.
    marks = [(s["x_disk"], AQUA, "tier"), (s["x_oz"], BLUE, f"{s['hi']} GB cache")]
    marks = sorted((d, c, n) for d, c, n in marks if d)
    if len(marks) == 1 or (
        len(marks) == 2 and abs(marks[0][0] - marks[1][0]) < 1e-6 * marks[0][0]
    ):
        marks = [(marks[0][0], INK2, "OzoneKV")]
    for i, (d, col, name) in enumerate(marks):
        ax.axvline(d / GB, color=col, lw=0.8, ls=(0, (1.5, 1.5)), alpha=0.9)
        left = i == 0 and len(marks) > 1
        ax.text(
            d / GB * (0.93 if left else 1.07),
            ytop,
            f"{name} cheaper\nfrom {fmt_d(d, 2)}",
            fontsize=fs - 0.5,
            color=col,
            va="top",
            ha="right" if left else "left",
        )

    ax.set_xlabel("dataset size", fontsize=fs + 0.5)
    ax.set_ylabel("USD per month", fontsize=fs + 0.5)
    ticks = [d for d in (10, 100, 1000, 10000, 100000) if x[0] <= d <= x[-1]]
    ax.xaxis.set_major_locator(FixedLocator(ticks))
    ax.set_xticklabels([fmt_d(t * GB) for t in ticks], fontsize=fs)
    ax.xaxis.set_minor_locator(NullLocator())
    yticks = [1000, 2000, 5000, 10000, 20000, 50000, 100000]
    yticks = [t for t in yticks if ymin <= t <= ymax]
    ax.yaxis.set_major_locator(FixedLocator(yticks))
    ax.set_yticklabels([f"${t // 1000}k" for t in yticks], fontsize=fs)
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.grid(True, which="major", color=INK2, alpha=0.12, lw=0.6)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(INK2)
        ax.spines[side].set_linewidth(0.6)
    ax.tick_params(length=2, color=INK2, labelsize=fs)
    pr = prices["projection"]
    # A white backing: the "cheaper from" marker can fall under the legend.
    leg = ax.legend(
        fontsize=fs - 0.5,
        loc="upper left",
        bbox_to_anchor=(0.0, 0.80),
        frameon=True,
        facecolor="white",
        edgecolor="none",
        framealpha=0.92,
        borderpad=0.3,
        handlelength=2.0,
        borderaxespad=0.0,
        labelspacing=0.35,
    )
    # The offered load, mix and RF belong to the caption, not the legend.


def panel_b(ax, model, prices, s, d_bytes):
    pr = prices["projection"]
    fs = 7
    hi = s["hi"]
    oz = model.ozonedb(d_bytes, hi)
    ozd = model.ozonedb(d_bytes, hi, disk_gb=s["disk_gb"])
    ce = model.cassandra(d_bytes, "ebs")
    # (label, [(segment name, value, color), ...]) top to bottom.
    oz_lines = lambda o, tier: [
        ("S3 GETs", o["get_cost"], BLUES[0]),
        ("log tier", o["log_tier"], BLUES[1]),
        ("clients", o["client_cost"], BLUES[2]),
        ("S3 storage, PUTs", o["bulk"] + o["put_cost"] + o["ckpt_cost"], BLUES[3]),
    ] + ([("SSD tier", o["disk_cost"], AQUA)] if tier else [])
    bars = [
        (f"OzoneKV, {hi} GB cache", oz_lines(oz, False)),
    ]
    if s["disk_gb"]:
        bars.append((f"OzoneKV + {s['disk_gb'] / 1000:g} TB tier", oz_lines(ozd, True)))
    bars += [
        (
            "Cassandra SERIAL, EBS",
            [
                ("Cassandra nodes", ce["node_cost"], ORANGES[0]),
                ("Cassandra clients", ce["client_cost"], ORANGES[1]),
            ],
        ),
    ]
    seen = {}
    ys = list(range(len(bars)))[::-1]
    total_max = max(sum(v for _, v, _ in segs) for _, segs in bars)
    for y, (label, segs) in zip(ys, bars):
        left = 0.0
        for name, v, col in segs:
            ax.barh(
                y,
                v,
                left=left,
                height=0.42 if len(bars) <= 2 else 0.62,
                color=col,
                lw=0,
                edgecolor="none",
                label=None if name in seen else name,
            )
            seen[name] = True
            if v >= 0.09 * total_max:
                ax.text(
                    left + v / 2,
                    y,
                    fmt_usd(v),
                    ha="center",
                    va="center",
                    fontsize=fs - 1,
                    color="white",
                )
            left += v + 0.004 * total_max  # a hairline surface gap between segments
    ax.set_yticks(ys)
    ax.set_ylim(-0.85, len(bars) - 0.15)
    ax.set_yticklabels(
        [f"{label}\n{fmt_usd(sum(v for _, v, _ in segs))}" for label, segs in bars],
        fontsize=fs,
    )
    ax.set_xlim(0, total_max * 1.3)
    step = 5000 if total_max > 8000 else 2000 if total_max > 3500 else 1000
    ticks = list(range(0, int(total_max * 1.3) + 1, step))
    ax.xaxis.set_major_locator(FixedLocator(ticks))
    ax.set_xticklabels([f"${t // 1000}k" if t else "$0" for t in ticks], fontsize=fs)
    ax.set_xlabel(f"USD per month at {fmt_d(d_bytes)}", fontsize=fs + 0.5)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(INK2)
    ax.spines["bottom"].set_linewidth(0.6)
    ax.tick_params(axis="y", length=0)
    ax.tick_params(axis="x", length=2, color=INK2)
    ax.grid(True, axis="x", color=INK2, alpha=0.12, lw=0.6)
    ax.set_axisbelow(True)
    ax.legend(
        fontsize=fs - 1,
        loc="upper right",
        frameon=False,
        ncol=1,
        handlelength=0.9,
        handleheight=0.9,
        handletextpad=0.5,
        borderaxespad=0.0,
        labelspacing=0.3,
    )


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("coefficients_tsv")
    ap.add_argument("prices_json")
    ap.add_argument("--space")
    ap.add_argument("--h-workload", default="c")
    ap.add_argument("--cpu-workload", default=None)
    ap.add_argument("--tier-variant", default="")
    ap.add_argument("--cassandra-mode", default="serial", choices=("serial", "quorum"))
    ap.add_argument(
        "--tier",
        action="store_true",
        help="draw the SSD-tier line (prices.json projection.disk_gb_per_client per writer); off by default",
    )
    ap.add_argument(
        "--read-fraction",
        type=float,
        default=None,
        help="reads as a fraction of the offered ops (default: prices.json projection.read_fraction)",
    )
    ap.add_argument(
        "--breakdown-at",
        default="10 TB",
        help="the decade panel (b) splits (default 10 TB)",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="output stem; writes <stem>.{pdf,png} and <stem>_col.{pdf,png}",
    )
    args = ap.parse_args()

    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans"],
            "font.size": 7,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "axes.titlesize": 7,
            "figure.dpi": 600,
            "savefig.dpi": 600,
        }
    )
    coef, model, prices = build(args)
    s = series(coef, model, prices, tier=args.tier)
    m = pcm.re.match(r"^\s*([\d.]+)\s*(GB|TB)\s*$", args.breakdown_at)
    if not m:
        raise SystemExit(f"--breakdown-at: cannot parse {args.breakdown_at!r}")
    d_b = float(m.group(1)) * (TB if m.group(2) == "TB" else GB)

    pr = prices["projection"]
    print(
        f"panel (a): {pr['ops_per_s']:,} ops/s, {pr['read_fraction']:.0%} reads, RF={pr['rf']}, "
        f"against cassandra-{args.cassandra_mode}; measured at "
        + ", ".join(fmt_d(d) for d in coef.measured_d)
    )
    print(
        f"  tier holds the dataset to {fmt_d(s['tier_full_to']) if s['tier_full_to'] else 'n/a'}; "
        f"cheaper from: {s['hi']} GB cache {fmt_d(s['x_oz']) if s['x_oz'] else 'never'}, "
        f"tier {fmt_d(s['x_disk']) if s['x_disk'] else 'never'}"
    )

    # Double column: (a) + (b).
    fig, (ax_a, ax_b) = plt.subplots(
        1,
        2,
        figsize=(7.0, 2.05),
        gridspec_kw={"width_ratios": [1.55, 1], "wspace": 0.55},
    )
    panel_a(ax_a, coef, model, prices, s, single=False)
    panel_b(ax_b, model, prices, s, d_b)
    for ax, tag in ((ax_a, "(a)"), (ax_b, "(b)")):
        ax.text(
            -0.02,
            1.02,
            tag,
            transform=ax.transAxes,
            fontsize=8,
            fontweight="bold",
            va="bottom",
            ha="right",
        )
    fig.subplots_adjust(left=0.082, right=0.99, top=0.92, bottom=0.21)
    for ext in ("pdf", "png"):
        fig.savefig(f"{args.out}.{ext}")
        print(f"wrote {args.out}.{ext}")
    plt.close(fig)

    # Single column: (a) alone.
    fig, ax = plt.subplots(figsize=(3.5, 1.6))
    panel_a(ax, coef, model, prices, s, single=True)
    fig.subplots_adjust(left=0.158, right=0.985, top=0.962, bottom=0.25)
    for ext in ("pdf", "png"):
        fig.savefig(f"{args.out}_col.{ext}")
        print(f"wrote {args.out}_col.{ext}")


if __name__ == "__main__":
    main()
