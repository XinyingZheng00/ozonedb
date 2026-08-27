#!/usr/bin/env python3
"""Projected monthly cost against dataset size (bench/PLAN-cost.md P0.7).

  plot_cost_model.py coefficients.tsv prices.json [--space space.json]
                     [--out-dir DIR] [--table out.tsv] [--h-workload c]

coefficients.tsv is what extract_cost_coefficients.py writes. prices.json
holds the list prices, the instance roles and the projection parameters.
space.json holds the phase 1 space measurements (sC, sO, L_gb, ...); see
space.example.json. Anything the TSV or space.json does not provide falls
back to the pre-measurement estimate and is printed as ASSUMED, so the
figure never silently mixes measured and guessed inputs.

The model (bench/PLAN-cost.md, "The model"), per month:

  Cassandra(D) = N(D) * p_node + clients_C * p_client_C
    N(D) = max(min_nodes, ceil(RF * sC * D / (disk_node * fill)),
                          ceil(ops / (ops_node_C * util)))
  OzoneDB(D)   = p_seq + log_units * (p_logunit + L * p_disk)
               + sO * D * p_s3
               + (R * (1 - h(c / D)) * g + W * get_per_write) * p_get * S
               + W * put_per_write * p_put * S
               + k * (S / T_trim) * p_put
               + clients_O * p_client_O
    clients_X  = max(min_clients, ceil(ops * cpu_s_per_op_X / (vcpu * util)))

S is seconds per month; R and W are reads/s and writes/s of the offered
load; h is interpolated in log(c/D) between the measured cache-sweep points
and held flat beyond them (pessimistic below the smallest measured ratio).
The "today" line is the same without trimming: the log tier keeps
L0_kb_per_put bytes per record ever written (at least the load) on every log
unit, and pays no checkpoint PUTs.
"""
import argparse
import csv
import json
import math
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
GB = 1024.0 ** 3

DEFAULT_SPACE = {"sC": 1.05, "sO": 1.10, "L_gb": 0.30, "L0_kb_per_put": 1.1,
                 "trim_interval_s": 30, "wa": 1.2}
# Pre-measurement estimates for the per-op coefficients, used only when the
# TSV has no row that provides them.
DEFAULT_COEF = {"g": 1.0, "put_per_write": 1.0 / 60000, "get_per_write": 0.015, "k": 12.0,
                "cpu_s_per_op_O": 0.0012, "cpu_s_per_op_C": 0.0003,
                "ops_node_C": 20000.0}


def fnum(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def load_rows(path):
    with open(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def median(xs):
    xs = sorted(xs)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


class Coefficients:
    """Everything the model needs, with a source tag per value."""

    def __init__(self, rows, space, h_workload):
        self.src = {}
        ozone = [r for r in rows if r["label"].startswith("ozonedb-corfu")
                 and "linearizable" not in r["label"] and r["workload"] != "load"]
        cass = [r for r in rows if r["label"].startswith("cassandra")]

        # h(ratio): the cache sweep, one workload.
        pts = {}
        for r in ozone:
            if r["workload"] != h_workload:
                continue
            h, ratio = fnum(r["h"]), fnum(r["cache_ratio"])
            if h is None or ratio is None or ratio <= 0:
                continue
            pts.setdefault(ratio, []).append(h)
        self.h_points = sorted((ratio, sum(v) / len(v)) for ratio, v in pts.items())
        if self.h_points:
            self.src["h"] = f"measured, workload {h_workload}, {len(self.h_points)} ratios"
        else:
            # Pessimistic straight line: every miss is a miss until the cache
            # holds the whole dataset.
            self.h_points = [(1e-5, 0.0), (1.0, 1.0)]
            self.src["h"] = "ASSUMED (no cache-sweep rows)"

        def pick(name, values, default):
            values = [v for v in values if v is not None]
            if values:
                self.src[name] = f"measured ({len(values)} cells)"
                return median(values)
            self.src[name] = "ASSUMED"
            return default

        # g from the read-only workload: under a write workload the GET
        # counter also holds compaction input reads, which are the separate
        # per-write term below.
        self.g = pick("g", [fnum(r["get_per_miss"]) for r in ozone
                            if r["workload"] == h_workload
                            and fnum(r["cache_misses"]) and fnum(r["cache_misses"]) > 100], DEFAULT_COEF["g"])
        # Compaction GETs per write: the load is write-only, so its GETs are
        # all compaction input reads (about 0.015 per put on the 1 GB load).
        loads = [r for r in rows if r["label"].startswith("ozonedb-corfu") and r["workload"] == "load"]
        gpw = []
        for r in loads:
            g_, h_, w_ = fnum(r["s3_get"]), fnum(r["s3_head"]) or 0.0, fnum(r["writes"])
            if g_ is not None and w_ and w_ > 0:
                gpw.append((g_ + h_) / w_)
        self.get_per_write = pick("get_per_write", gpw, DEFAULT_COEF["get_per_write"])
        # Compaction PUTs per write, checkpoint objects excluded.
        ppw = []
        for r in ozone:
            put, writes, ck = fnum(r["s3_put"]), fnum(r["writes"]), fnum(r["ckpt_objects"]) or 0.0
            lst = fnum(r["s3_list"]) or 0.0
            if put is not None and writes and writes > 0:
                ppw.append(max(0.0, put + lst - ck) / writes)
        self.put_per_write = pick("put_per_write", ppw, DEFAULT_COEF["put_per_write"])
        ks = []
        for r in ozone:
            n, objs = fnum(r["ckpt_count"]), fnum(r["ckpt_objects"])
            if n and objs:
                ks.append(objs / n)
        self.k = pick("k", ks, DEFAULT_COEF["k"])
        self.cpu_O = pick("cpu_s_per_op_O", [fnum(r["client_cpu_s_per_op"]) for r in ozone],
                          DEFAULT_COEF["cpu_s_per_op_O"])
        self.cpu_C = pick("cpu_s_per_op_C", [fnum(r["client_cpu_s_per_op"]) for r in cass
                                             if "serial" not in r["label"]],
                          DEFAULT_COEF["cpu_s_per_op_C"])
        # One Cassandra box: steady ops/s divided by the busy fraction of the
        # box (pidstat), the largest over the scaling cells.
        caps = []
        for r in cass:
            ops, busy = fnum(r["steady_ops_s"]), fnum(r["server_busy_frac"])
            if ops and busy and busy > 0.02:
                caps.append(ops / busy)
        self.ops_node_C = pick("ops_node_C", [max(caps)] if caps else [], DEFAULT_COEF["ops_node_C"])

        self.space = dict(DEFAULT_SPACE)
        for key in DEFAULT_SPACE:
            if key in space:
                self.space[key] = float(space[key])
                self.src[key] = "measured (space.json)"
            else:
                self.src[key] = "ASSUMED"
        self.measured_d = sorted({fnum(r["dataset_bytes"]) for r in rows if fnum(r["dataset_bytes"])})

    def h(self, ratio):
        pts = self.h_points
        if ratio <= pts[0][0]:
            return pts[0][1]
        if ratio >= pts[-1][0]:
            return pts[-1][1]
        x = math.log10(ratio)
        for (r0, h0), (r1, h1) in zip(pts, pts[1:]):
            if r0 <= ratio <= r1:
                x0, x1 = math.log10(r0), math.log10(r1)
                return h0 + (h1 - h0) * (x - x0) / (x1 - x0)
        return pts[-1][1]

    def report(self):
        print("coefficients:")
        for name, val in (("g", self.g), ("get_per_write", self.get_per_write),
                          ("put_per_write", self.put_per_write), ("k", self.k),
                          ("cpu_s_per_op_O", self.cpu_O), ("cpu_s_per_op_C", self.cpu_C),
                          ("ops_node_C", self.ops_node_C)):
            print(f"  {name:16} {val:<14.6g} {self.src[name]}")
        for key in DEFAULT_SPACE:
            print(f"  {key:16} {self.space[key]:<14.6g} {self.src[key]}")
        print(f"  {'h':16} {self.src['h']}")
        for ratio, h in self.h_points:
            print(f"      c/D={ratio:<10.3g} h={h:.4f}")


class Model:
    def __init__(self, coef, prices):
        self.c = coef
        self.p = prices
        pr = prices["projection"]
        self.ops = float(pr["ops_per_s"])
        self.R = self.ops * float(pr["read_fraction"])
        self.W = self.ops - self.R
        self.S = float(prices.get("seconds_per_month", 2.63e6))
        self.util = float(pr.get("target_util", 0.7))

    def inst(self, role):
        return self.p["instances"][self.p["roles"][role]]

    def clients(self, cpu_s_per_op, role):
        inst = self.inst(role)
        n = math.ceil(self.ops * cpu_s_per_op / (inst["vcpu"] * self.util))
        n = max(int(self.p["projection"].get("min_clients", 1)), n)
        return n, n * inst["usd_month"]

    def cassandra(self, d_bytes, layout):
        pr, st = self.p["projection"], self.p["storage"]
        d_gb = d_bytes / GB
        if layout == "nvme":
            node = self.inst("cassandra_node_nvme")
            disk_gb = node["nvme_gb"]
            disk_cost = 0.0
        else:
            node = self.inst("cassandra_node_ebs")
            disk_gb = float(self.p["roles"]["cassandra_ebs_gb_per_node"])
            disk_cost = disk_gb * st["gp3_usd_gb_month"]
        need_gb = pr["rf"] * self.c.space["sC"] * d_gb
        n_storage = math.ceil(need_gb / (disk_gb * pr["disk_fill"]))
        n_ops = math.ceil(self.ops / (self.c.ops_node_C * self.util))
        n = max(int(pr["min_cassandra_nodes"]), n_storage, n_ops)
        nc, client_cost = self.clients(self.c.cpu_C, "cassandra_client")
        total = n * (node["usd_month"] + disk_cost) + client_cost
        return {"total": total, "nodes": n, "clients": nc,
                "bound": "storage" if n == n_storage and n_storage >= n_ops else
                ("ops" if n == n_ops else "floor")}

    def ozonedb(self, d_bytes, cache_gb, trimming=True):
        pr, st, sp = self.p["projection"], self.p["storage"], self.c.space
        d_gb = d_bytes / GB
        seq = self.inst("sequencer")["usd_month"]
        logunit = self.inst("logunit")["usd_month"]
        if trimming:
            log_gb = sp["L_gb"]
        else:
            # Untrimmed: every record ever written stays in the log. The load
            # alone is D / 1 KB records at L0_kb_per_put each.
            log_gb = max(sp["L_gb"], d_bytes / 1024.0 * sp["L0_kb_per_put"] * 1024.0 / GB)
        log_tier = seq + pr["log_units"] * (logunit + log_gb * st["gp3_usd_gb_month"])
        bulk = sp["sO"] * d_gb * st["s3_standard_usd_gb_month"]
        h = self.c.h(cache_gb * GB / d_bytes)
        gets = (self.R * (1.0 - h) * self.c.g + self.W * self.c.get_per_write) * self.S
        puts = self.W * self.c.put_per_write * self.S
        ckpt = self.c.k * (self.S / sp["trim_interval_s"]) if trimming else 0.0
        req = (gets * st["s3_get_usd_per_million"] + (puts + ckpt) * st["s3_put_usd_per_million"]) / 1e6
        nc, client_cost = self.clients(self.c.cpu_O, "ozonedb_client")
        total = log_tier + bulk + req + client_cost
        return {"total": total, "log_tier": log_tier, "bulk": bulk, "requests": req,
                "gets": gets, "puts": puts + ckpt, "h": h, "clients": nc, "log_gb": log_gb}


def grid(d_min_gb, d_max_gb, per_decade=12):
    lo, hi = math.log10(d_min_gb), math.log10(d_max_gb)
    n = int((hi - lo) * per_decade) + 1
    return [10 ** (lo + i * (hi - lo) / (n - 1)) * GB for i in range(n)]


def fmt_gb(d_bytes):
    """Axis labels only: the grid is 10^n GB, and 1000 GB reads as 1 TB."""
    gb = d_bytes / GB
    return f"{gb / 1000:.3g} TB" if gb >= 1000 else f"{gb:.3g} GB"


def find_crossover(ds, ozone, cass):
    """First D where OzoneDB turns cheaper after being dearer. None when it
    never turns; the caller then says which side the whole range is on."""
    dearer_seen = False
    for d, o, c in zip(ds, ozone, cass):
        if o >= c:
            dearer_seen = True
        elif dearer_seen:
            return d
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("coefficients_tsv")
    ap.add_argument("prices_json")
    ap.add_argument("--space", help="space.json with sC, sO, L_gb, L0_kb_per_put, trim_interval_s")
    ap.add_argument("--h-workload", default="c", help="workload whose cache sweep gives h (default c)")
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--table", help="write the projection table as TSV")
    args = ap.parse_args()

    rows = load_rows(args.coefficients_tsv)
    with open(args.prices_json) as f:
        prices = json.load(f)
    space = {}
    if args.space:
        with open(args.space) as f:
            space = {k: v for k, v in json.load(f).items() if not k.startswith("_")}
    coef = Coefficients(rows, space, args.h_workload)
    coef.report()
    model = Model(coef, prices)
    pr = prices["projection"]
    print(f"projection: {model.ops:.0f} ops/s, read fraction {pr['read_fraction']}, "
          f"RF={pr['rf']}, {pr['log_units']} log units, prices as of {prices.get('as_of', '?')}")

    ds = grid(pr["d_min_gb"], pr["d_max_gb"])
    series = {
        "cass_nvme": [model.cassandra(d, "nvme")["total"] for d in ds],
        "cass_ebs": [model.cassandra(d, "ebs")["total"] for d in ds],
        "ozone_hi_cache": [model.ozonedb(d, pr["cache_gb_high"])["total"] for d in ds],
        "ozone_lo_cache": [model.ozonedb(d, pr["cache_gb_low"])["total"] for d in ds],
        "ozone_today": [model.ozonedb(d, pr["cache_gb_high"], trimming=False)["total"] for d in ds],
    }
    cass_min = [min(a, b) for a, b in zip(series["cass_nvme"], series["cass_ebs"])]
    crossover = find_crossover(ds, series["ozone_lo_cache"], cass_min)
    if crossover is None:
        side = "cheaper" if series["ozone_lo_cache"][0] < cass_min[0] else "dearer"
        crossover_note = f"OzoneDB ({pr['cache_gb_low']} GB cache) is {side} than the cheaper Cassandra layout over the whole range"
    else:
        crossover_note = f"crossover (OzoneDB with a {pr['cache_gb_low']} GB cache turns cheaper than the cheaper Cassandra layout): {fmt_gb(crossover)}"

    # Table.
    hdr = f"{'D':>10} {'cass_nvme':>11} {'cass_ebs':>11} {'ozone_16GB':>11} {'ozone_4GB':>11} {'today':>11} {'h16':>6} {'nodes':>5}"
    print(hdr)
    lines = []
    for d, cn, ce, oh, ol, ot in zip(ds, series["cass_nvme"], series["cass_ebs"],
                                     series["ozone_hi_cache"], series["ozone_lo_cache"],
                                     series["ozone_today"]):
        gb = d / GB
        if abs(math.log10(gb) - round(math.log10(gb))) > 1e-6:
            continue
        oz = model.ozonedb(d, pr["cache_gb_high"])
        cs = model.cassandra(d, "nvme")
        print(f"{fmt_gb(d):>10} {cn:>11,.0f} {ce:>11,.0f} {oh:>11,.0f} {ol:>11,.0f} {ot:>11,.0f} {oz['h']:>6.3f} {cs['nodes']:>5}")
        lines.append((fmt_gb(d), cn, ce, oh, ol, ot, oz["h"], cs["nodes"], cs["bound"]))
    print(crossover_note)
    if args.table:
        with open(args.table, "w") as f:
            f.write("D\tcass_nvme\tcass_ebs\tozone_hi_cache\tozone_lo_cache\tozone_today\th_hi_cache\tcass_nodes\tcass_bound\n")
            for ln in lines:
                f.write("\t".join(str(x) for x in ln) + "\n")
        print(f"wrote {args.table}")

    # Figure.
    os.makedirs(args.out_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(6.0, 3.8))
    x = [d / GB for d in ds]
    ax.plot(x, series["cass_nvme"], color="#b03a2e", lw=1.8, label="Cassandra RF=3, NVMe (i4i)")
    ax.plot(x, series["cass_ebs"], color="#e59866", lw=1.8, ls="-.", label="Cassandra RF=3, EBS gp3")
    ax.fill_between(x, series["ozone_hi_cache"], series["ozone_lo_cache"], color="#1f618d", alpha=0.25,
                    label=f"OzoneDB + trimming ({pr['cache_gb_high']} GB .. {pr['cache_gb_low']} GB cache per client)")
    ax.plot(x, series["ozone_hi_cache"], color="#1f618d", lw=1.6)
    ax.plot(x, series["ozone_lo_cache"], color="#1f618d", lw=1.0)
    ax.plot(x, series["ozone_today"], color="#1f618d", lw=1.2, ls="--", label="OzoneDB without trimming")
    for d in coef.measured_d:
        ax.plot([d / GB], [model.ozonedb(d, pr["cache_gb_high"])["total"]], "o", color="#1f618d", ms=5)
        ax.plot([d / GB], [model.cassandra(d, "nvme")["total"]], "o", color="#b03a2e", ms=5)
    if coef.measured_d:
        ax.plot([], [], "o", color="gray", ms=5, label="measured cells")
    ax.set_xscale("log")
    ax.set_yscale("log")
    if crossover:
        ax.axvline(crossover / GB, color="gray", lw=0.8, ls=":")
        ax.text(crossover / GB, 0.03, f" crossover {fmt_gb(crossover)}", fontsize=7,
                color="gray", va="bottom", transform=ax.get_xaxis_transform())
    ax.set_xlabel("dataset size (GB)")
    ax.set_ylabel("USD per month")
    ax.set_title(f"{model.ops:,.0f} ops/s, {pr['read_fraction']:.0%} reads, prices {prices.get('as_of', '?')}", fontsize=9)
    ax.grid(True, which="both", alpha=0.25)
    ax.legend(fontsize=7, loc="upper left")
    assumed = [k for k, v in coef.src.items() if v.startswith("ASSUMED")]
    if assumed:
        ax.text(0.99, 0.02, "assumed: " + ", ".join(assumed), transform=ax.transAxes,
                fontsize=6, color="#922b21", ha="right", va="bottom")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        out = os.path.join(args.out_dir, f"cost_model.{ext}")
        fig.savefig(out, dpi=200)
        print(f"wrote {out}")


if __name__ == "__main__":
    main()
