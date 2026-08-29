#!/usr/bin/env python3
"""Two projection tables from plot_cost_model.py --table, one figure.

  overlay_projections.py base-projection.tsv zipf-projection.tsv --out fig.png
                         [--base-name "YCSB zipfian"] [--other-name "key-space zipfian"]

The Cassandra lines are drawn once, from the first table (they do not depend
on the OzoneDB request distribution). The OzoneDB lines are drawn twice: solid
for the first table (YCSB's scrambled zipfian, the main corpus) and dashed
for the second (the zipfian over the key space; bench/PLAN-cost-2.md, Task 0b
and Task 7b). The tables hold one row per decade, so the lines join the
decades; the full-grid curve of either corpus is in that corpus's own
cost_model.pdf.
"""
import argparse
import csv
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

D_RE = re.compile(r"^\s*([\d.]+)\s*(GB|TB)\s*$")


def read_table(path):
    """{column: [values]} with D parsed back to GB (fmt_gb writes '10 GB' / '1 TB')."""
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    if not rows:
        raise SystemExit(f"{path}: no rows")
    out = {k: [] for k in rows[0]}
    for r in rows:
        m = D_RE.match(r["D"])
        if not m:
            raise SystemExit(f"{path}: cannot parse D={r['D']!r}")
        gb = float(m.group(1)) * (1000.0 if m.group(2) == "TB" else 1.0)
        for k, v in r.items():
            if k == "D":
                out[k].append(gb)
                continue
            try:
                out[k].append(float(v))
            except ValueError:  # text columns such as cass_bound
                out[k].append(v)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("base_tsv")
    ap.add_argument("other_tsv")
    ap.add_argument("--out", required=True, help="figure path; a .pdf twin is written beside a .png")
    ap.add_argument("--base-name", default="YCSB zipfian")
    ap.add_argument("--other-name", default="key-space zipfian")
    ap.add_argument("--title", default="")
    ap.add_argument("--no-other-tier", action="store_true",
                    help="do not draw the second table's tier line (its corpus has no tier cells, so the line is the model's fallback)")
    args = ap.parse_args()

    base, other = read_table(args.base_tsv), read_table(args.other_tsv)
    fig, ax = plt.subplots(figsize=(6.0, 3.8))
    x = base["D"]
    ax.plot(x, base["cass_nvme"], color="#b03a2e", lw=1.8, label="Cassandra RF=3, NVMe (i4i)")
    ax.plot(x, base["cass_ebs"], color="#e59866", lw=1.8, ls="-.", label="Cassandra RF=3, EBS gp3")
    for tab, name, ls in ((base, args.base_name, "-"), (other, args.other_name, "--")):
        ax.plot(tab["D"], tab["ozone_hi_cache"], color="#1f618d", lw=1.6, ls=ls,
                label=f"OzoneDB, {name} (high .. low RAM cache)")
        ax.plot(tab["D"], tab["ozone_lo_cache"], color="#1f618d", lw=1.0, ls=ls)
        ax.fill_between(tab["D"], tab["ozone_hi_cache"], tab["ozone_lo_cache"], color="#1f618d",
                        alpha=0.25 if ls == "-" else 0.10)
        if tab is other and args.no_other_tier:
            continue
        if "ozone_disk_cache" in tab and any(v > 0 for v in tab["disk_gb"]):
            ax.plot(tab["D"], tab["ozone_disk_cache"], color="#117864", lw=1.6, ls=ls,
                    label=f"OzoneDB + disk tier, {name}")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("dataset size (GB)")
    ax.set_ylabel("USD per month")
    if args.title:
        ax.set_title(args.title, fontsize=9)
    ax.grid(True, which="both", alpha=0.25)
    ax.legend(fontsize=7, loc="upper left")
    fig.tight_layout()
    out = args.out
    stem = out[:-4] if out.lower().endswith((".png", ".pdf")) else out
    for ext in ("pdf", "png"):
        fig.savefig(f"{stem}.{ext}", dpi=200)
        print(f"wrote {stem}.{ext}")


if __name__ == "__main__":
    main()
