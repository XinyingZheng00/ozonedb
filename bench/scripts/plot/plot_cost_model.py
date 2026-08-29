#!/usr/bin/env python3
"""Projected monthly cost against dataset size (bench/PLAN-cost.md P0.7).

  plot_cost_model.py coefficients.tsv prices.json [--space space.json]
                     [--out-dir DIR] [--table out.tsv] [--h-workload c]
                     [--cpu-workload a] [--tier-variant ch64k-adm]

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
               + clients_O * disk_gb * p_disk
    clients_X  = max(min_clients, ceil(ops * cpu_s_per_op_X / (vcpu * util)))

With a disk-cache tier of `disk_gb` per client (bench/PLAN-disk-cache.md), the
tier answers what the RAM cache missed, so the two hit rates compose:

  h = 1 - (1 - h(c / D)) * (1 - disk_h(disk_gb / D))

disk_h is the tier's own measured hit rate. The client CPU per op is the full
tier's, but only when the tier holds the dataset: a partial tier measured more
CPU than no tier at all, so below a ratio of 1 the baseline is the floor. An
extra fill_get_per_op(disk_gb / D) GETs per read pay for the misses the tier
fills -- also ratio-aware, because a partial tier refills far more often than a
full one -- and the tier's gp3 volumes are the last term.

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
import re
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


def tier_variant(label):
    """`ozonedb-corfu-lru8m-dc512m-ch64k-adm-kp` -> `ch64k-adm`: the tokens after
    `-dc<size>`, without `-kp` (the page-cache A/B is not a variant). `` for a
    round-1 label and for a label without a tier."""
    m = re.search(r"-dc\d+[gmkb](.*)$", label)
    if not m:
        return ""
    return "-".join(t for t in m.group(1).strip("-").split("-") if t and t != "kp")


class Coefficients:
    """Everything the model needs, with a source tag per value."""

    def __init__(self, rows, space, h_workload, read_fraction=0.5, tier_variant_filter="",
                 cpu_workload=None):
        self.src = {}
        self.read_fraction = float(read_fraction)
        ozone = [r for r in rows if r["label"].startswith("ozonedb-corfu")
                 and "linearizable" not in r["label"] and r["workload"] != "load"]
        # A disk-cache tier changes what the S3 counters mean: a RAM block-cache
        # miss is served from the client's local copy of the SSTable, not from
        # the object store. So the tier cells feed the tier coefficients at the
        # end of this method; of the S3-path coefficients they may only feed the
        # ones the tier cannot move -- the cumulative RAM hit rate (the writer's
        # own [lru_cache] counters), the compaction PUTs (write-through happens
        # after the backing PUT) and the checkpoint objects. Anything derived
        # from the S3 request rate -- h_steady, g, the client CPU per op -- is
        # taken from the S3-path cells only.
        # Only one disk-cache variant may feed the tier coefficients: round-1
        # file-mode rows and round-2 chunk/admission rows measure different
        # engines, so mixing their ratios would interpolate across two curves
        # (bench/PLAN-disk-cache-2.md, Task 6).
        tier = [r for r in ozone if fnum(r.get("disk_capacity"))
                and tier_variant(r["label"]) == tier_variant_filter]
        if not tier and any(fnum(r.get("disk_capacity")) for r in ozone):
            print(f"no disk-cache rows for --tier-variant '{tier_variant_filter}'")
        ozone_s3 = [r for r in ozone if not fnum(r.get("disk_capacity"))]
        cass = [r for r in rows if r["label"].startswith("cassandra")]

        # h(ratio): the cache sweep, one workload. h_steady (the last window
        # of the run, from the MinIO request-rate series) when the cell has
        # it, else the cumulative counter, which includes the cold start. A
        # tier cell only ever contributes its cumulative counter: h_steady is
        # inferred from the S3 request rate, and behind a tier that rate is
        # the tier's residual, not the RAM cache's.
        pts = {}
        n_steady = n_cum = n_unfilled = 0
        for r in ozone:
            if r["workload"] != h_workload:
                continue
            # A writer's cache fills at that writer's own miss rate (about
            # 1.9 MB/s at 10 GB), so a large cache can close a cell far from
            # full and its hit rate is that of a smaller cache. cache_fill is
            # the LRU's bytes at close over its capacity (PLAN-cost-2 Task 5);
            # a cell below 0.9 is not a point on the curve. Older TSVs have
            # no column and keep every cell.
            fill = fnum(r.get("cache_fill"))
            if fill is not None and fill < 0.9 and not fnum(r.get("disk_capacity")):
                n_unfilled += 1
                continue
            h, ratio = fnum(r.get("h_steady")), fnum(r["cache_ratio"])
            if fnum(r.get("disk_capacity")):
                h = None
            steady = h is not None
            if not steady:
                h = fnum(r["h"])
            if h is None or ratio is None or ratio <= 0:
                continue
            if steady:
                n_steady += 1
            else:
                n_cum += 1
            pts.setdefault(ratio, []).append((1 if steady else 0, fnum(r.get("run_s")) or 0.0, h))
        # One point per ratio: a steady-state cell over a cumulative one, then
        # the longest cell. A 120 s cell's last window is still warming a
        # 512 MB cache; the 600 s sweep is the converged value.
        self.h_points = sorted((ratio, max(v)[2]) for ratio, v in pts.items())
        if self.h_points:
            self.src["h"] = (f"measured, workload {h_workload}, {len(self.h_points)} ratios "
                             f"from {n_steady + n_cum} cells ({n_steady} steady-state, {n_cum} cumulative"
                             + (f", {n_unfilled} unfilled cells skipped" if n_unfilled else "") + ")")
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
        self.g = pick("g", [fnum(r["get_per_miss"]) for r in ozone_s3
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
        # Client CPU per op from the workload whose read mix matches the
        # projection (a at 50 % reads, c above 95 %): workload a costs an
        # OzoneDB client 2-4x the CPU of workload c (compaction, log tailing),
        # so a median over both would answer neither question. --cpu-workload
        # overrides the rule for a corpus whose workloads carry another name
        # (the key-space zipfian cells are `az` and `cz`; PLAN-cost-2.md
        # Task 0b), so that corpus is read with --h-workload cz --cpu-workload az.
        cpu_wl = cpu_workload or ("c" if self.read_fraction >= 0.95 else "a")
        self.cpu_workload = cpu_wl
        self.cpu_O = pick("cpu_s_per_op_O", [fnum(r["client_cpu_s_per_op"]) for r in ozone_s3
                                             if r["workload"] == cpu_wl],
                          DEFAULT_COEF["cpu_s_per_op_O"])
        self.cpu_C = pick("cpu_s_per_op_C", [fnum(r["client_cpu_s_per_op"]) for r in cass
                                             if "serial" not in r["label"] and r["workload"] == cpu_wl],
                          DEFAULT_COEF["cpu_s_per_op_C"])
        # One Cassandra box: steady ops/s divided by the busy fraction of the
        # box (pidstat), the largest over the scaling cells.
        caps = []
        for r in cass:
            ops, busy = fnum(r["steady_ops_s"]), fnum(r["server_busy_frac"])
            if ops and busy and busy > 0.02:
                caps.append(ops / busy)
        self.ops_node_C = pick("ops_node_C", [max(caps)] if caps else [], DEFAULT_COEF["ops_node_C"])

        # Disk-cache tier (bench/PLAN-disk-cache.md, campaign disk-20260829).
        # disk_h(ratio) is the *tier's own* hit rate against the tier budget
        # over the dataset, so the model can compose it with whatever RAM
        # cache the projection buys; h_disk(ratio) is the combined h_total of
        # the cells as measured, kept for callers that want the raw curve. Both
        # come from the same workload as h. The -kp cell is the page-cache A/B
        # of the largest budget, not another ratio, so it is excluded.
        dpts = {}
        for r in tier:
            if r["workload"] != h_workload or r["label"].endswith("-kp"):
                continue
            ratio = fnum(r.get("disk_ratio"))
            if not ratio or ratio <= 0:
                continue
            # One point per ratio, the cell with the smallest RAM cache, so
            # the tier is what was varied -- the same rule as full_tier() and
            # fill_points. Without it a second trial of a cell would put two
            # points at one ratio and _interp would pick whichever sorted
            # first.
            # On a tie (a rerun of the same cell), the fuller tier: a longer
            # cell that reached more of its budget (PLAN-cost-2 Task 6).
            cap = fnum(r.get("cache_capacity")) or 0.0
            held = fnum(r.get("disk_bytes")) or 0.0
            if ratio not in dpts or (cap, -held) < (dpts[ratio][0], -dpts[ratio][3]):
                dpts[ratio] = (cap, fnum(r.get("h_total")), fnum(r.get("disk_h")), held)
        self.h_disk_points = sorted((ratio, ht) for ratio, (_, ht, _, _) in dpts.items() if ht is not None)
        self.disk_h_points = sorted((ratio, dh) for ratio, (_, _, dh, _) in dpts.items() if dh is not None)

        def full_tier(workload):
            """The largest tier budget measured on `workload`. On a tie, the
            cell with the smallest RAM cache, so the tier is what was varied."""
            cands = [r for r in tier if r["workload"] == workload
                     and not r["label"].endswith("-kp") and fnum(r.get("disk_ratio"))]
            if not cands:
                return None
            best = max(fnum(r["disk_ratio"]) for r in cands)
            return min((r for r in cands if fnum(r["disk_ratio"]) == best),
                       key=lambda r: (fnum(r.get("cache_capacity")) or 0.0, -(fnum(r.get("disk_bytes")) or 0.0)))

        # The CPU per op and the fill rate must both come from the same workload
        # as the baseline cpu_O, or the tier line would compare two workloads.
        # Both are ratio-dependent: a partial tier costs more CPU than no tier
        # and refills about 80x as often as a full one, because it evicts what
        # it has just fetched.
        full_cpu = full_tier(cpu_wl)
        fpts = {}
        for r in tier:
            if r["workload"] != cpu_wl or r["label"].endswith("-kp"):
                continue
            ratio, f = fnum(r.get("disk_ratio")), fnum(r.get("disk_fill_gets_per_op"))
            if not ratio or ratio <= 0 or f is None:
                continue
            # One point per ratio: the cell with the smallest RAM cache, so the
            # tier is what was varied. Same rule as full_tier().
            cap = fnum(r.get("cache_capacity")) or 0.0
            held = fnum(r.get("disk_bytes")) or 0.0
            if ratio not in fpts or (cap, -held) < (fpts[ratio][0], -fpts[ratio][2]):
                fpts[ratio] = (cap, f, held)
        self.fill_points = sorted((ratio, f) for ratio, (_, f, _) in fpts.items())
        if self.disk_h_points and full_cpu is not None and self.fill_points:
            variant = f", variant '{tier_variant_filter}'"
            self.src["h_disk"] = (f"measured, workload {h_workload}, "
                                  f"{len(self.disk_h_points)} tier ratios" + variant)
            self.cpu_O_disk = fnum(full_cpu.get("client_cpu_s_per_op")) or self.cpu_O
            self.src["cpu_s_per_op_O_disk"] = f"measured ({full_cpu['label']}, workload {cpu_wl})" + variant
            self.src["fill_get_per_op"] = (f"measured, workload {cpu_wl}, "
                                           f"{len(self.fill_points)} tier ratios" + variant)
        else:
            # No tier cells: the tier hits nothing, fills nothing and costs the
            # baseline CPU, so the disk_gb line is priced but changes nothing else.
            self.h_disk_points = list(self.h_points)
            self.disk_h_points = [(1.0, 0.0)]
            self.fill_points = [(1.0, 0.0)]
            self.cpu_O_disk = self.cpu_O
            for name in ("h_disk", "cpu_s_per_op_O_disk", "fill_get_per_op"):
                self.src[name] = "ASSUMED (no disk-cache rows)"

        self.space = dict(DEFAULT_SPACE)
        for key in DEFAULT_SPACE:
            if key in space:
                self.space[key] = float(space[key])
                self.src[key] = "measured (space.json)"
            else:
                self.src[key] = "ASSUMED"
        self.measured_d = sorted({fnum(r["dataset_bytes"]) for r in rows if fnum(r["dataset_bytes"])})

    @staticmethod
    def _interp(pts, ratio):
        """Hit rate at `ratio`, linear in log10(ratio) between the measured
        points and held flat outside them."""
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

    def h(self, ratio):
        return self._interp(self.h_points, ratio)

    def h_disk(self, ratio):
        """Combined hit rate of the RAM cache and the disk-cache tier exactly as
        the cells measured it (an 8 MB RAM cache behind the tier). The model
        composes h() with disk_h() instead, so that the RAM cache the projection
        buys is the one it is charged for."""
        return self._interp(self.h_disk_points, ratio)

    def disk_h(self, ratio):
        """The tier's own hit rate, over the reads the RAM cache missed, at a
        tier budget of `ratio` times the dataset. Clamped at the largest
        measured ratio: a tier bigger than the dataset is a full tier."""
        return self._interp(self.disk_h_points, ratio)

    def fill_get_per_op(self, ratio):
        """S3 GETs per op spent refilling the tier, at a tier budget of
        `ratio` times the dataset. Per *op*, not per read: the extractor's
        disk_fill_gets_per_op is fill_gets / (reads + writes + scans), and
        both fills and evictions are driven by the write mix as much as by
        the reads. A partial tier evicts what it has just fetched and fetches
        it again: 0.0175 GETs per op at a 0.52 ratio against 0.00022 at a
        full tier, so this cannot be a constant."""
        return self._interp(self.fill_points, ratio)

    def report(self):
        print("coefficients:")
        for name, val in (("g", self.g), ("get_per_write", self.get_per_write),
                          ("put_per_write", self.put_per_write), ("k", self.k),
                          ("cpu_s_per_op_O", self.cpu_O), ("cpu_s_per_op_C", self.cpu_C),
                          ("ops_node_C", self.ops_node_C),
                          ("cpu_s_per_op_O_disk", self.cpu_O_disk)):
            print(f"  {name:19} {val:<14.6g} {self.src[name]}")
        for key in DEFAULT_SPACE:
            print(f"  {key:19} {self.space[key]:<14.6g} {self.src[key]}")
        print(f"  {'h':19} {self.src['h']}")
        for ratio, h in self.h_points:
            print(f"      c/D={ratio:<10.3g} h={h:.4f}")
        print(f"  {'disk_h':19} {self.src['h_disk']}")
        for (ratio, dh), (_, ht) in zip(self.disk_h_points, self.h_disk_points):
            print(f"      disk/D={ratio:<7.3g} disk_h={dh:.4f}  (h_total as measured {ht:.4f})")
        print(f"  {'fill_get_per_op':19} {self.src['fill_get_per_op']}")
        for ratio, f in self.fill_points:
            print(f"      disk/D={ratio:<7.3g} fill_gets_per_op={f:.5f}")


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
                "node_cost": n * (node["usd_month"] + disk_cost), "client_cost": client_cost,
                "bound": "storage" if n == n_storage and n_storage >= n_ops else
                ("ops" if n == n_ops else "floor")}

    def ozonedb(self, d_bytes, cache_gb, trimming=True, disk_gb=0):
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
        ratio_disk = disk_gb * GB / d_bytes
        h_ram = self.c.h(cache_gb * GB / d_bytes)
        if disk_gb > 0:
            # The tier answers what the RAM cache missed, so the two hit rates
            # compose: the projection is charged for the RAM cache it buys, not
            # for the 8 MB the tier cells ran behind.
            h_tier = self.c.disk_h(ratio_disk)
            h = 1.0 - (1.0 - h_ram) * (1.0 - h_tier)
            # A partial tier measured *more* client CPU than no tier at all --
            # it pays for the fills it keeps evicting -- so only a tier that
            # holds the dataset may be charged less than the baseline.
            cpu = self.c.cpu_O_disk if ratio_disk >= 1.0 else max(self.c.cpu_O, self.c.cpu_O_disk)
            # Per op, not per read: fill_get_per_op is measured as
            # fill_gets / (reads + writes + scans), so charging it on the
            # read rate alone would bill half the refills at a 50 % mix.
            fill = self.ops * self.c.fill_get_per_op(ratio_disk) * self.S
        else:
            h_tier = 0.0
            h = h_ram
            cpu = self.c.cpu_O
            fill = 0.0
        gets = (self.R * (1.0 - h) * self.c.g + self.W * self.c.get_per_write) * self.S + fill
        puts = self.W * self.c.put_per_write * self.S
        ckpt = self.c.k * (self.S / sp["trim_interval_s"]) if trimming else 0.0
        req = (gets * st["s3_get_usd_per_million"] + (puts + ckpt) * st["s3_put_usd_per_million"]) / 1e6
        nc, client_cost = self.clients(cpu, "ozonedb_client")
        disk_cost = nc * disk_gb * st["gp3_usd_gb_month"]
        total = log_tier + bulk + req + client_cost + disk_cost
        return {"total": total, "log_tier": log_tier, "bulk": bulk, "requests": req,
                "gets": gets, "puts": puts + ckpt, "h": h, "clients": nc, "log_gb": log_gb,
                "get_cost": gets * st["s3_get_usd_per_million"] / 1e6,
                "put_cost": puts * st["s3_put_usd_per_million"] / 1e6,
                "ckpt_cost": ckpt * st["s3_put_usd_per_million"] / 1e6,
                "client_cost": client_cost, "disk_cost": disk_cost, "disk_gb": disk_gb,
                "fill_gets": fill, "h_ram": h_ram, "h_tier": h_tier}


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
    ap.add_argument("--cpu-workload", default=None,
                    help="workload whose cells give the client CPU per op and the tier fill rate "
                         "(default: c when projection.read_fraction >= 0.95, else a); "
                         "the key-space zipfian corpus uses --h-workload cz --cpu-workload az")
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--table", help="write the projection table as TSV")
    ap.add_argument("--tier-variant", default="",
                    help="which disk-cache variant's rows feed the tier coefficients: the label tokens after -dc<size>, "
                         "e.g. ch64k-adm (default: the round-1 file-mode rows, whose labels carry no variant tokens)")
    args = ap.parse_args()

    rows = load_rows(args.coefficients_tsv)
    with open(args.prices_json) as f:
        prices = json.load(f)
    space = {}
    if args.space:
        with open(args.space) as f:
            space = {k: v for k, v in json.load(f).items() if not k.startswith("_")}
    coef = Coefficients(rows, space, args.h_workload,
                        read_fraction=prices["projection"].get("read_fraction", 0.5),
                        tier_variant_filter=args.tier_variant,
                        cpu_workload=args.cpu_workload)
    coef.report()
    model = Model(coef, prices)
    pr = prices["projection"]
    print(f"projection: {model.ops:.0f} ops/s, read fraction {pr['read_fraction']}, "
          f"RF={pr['rf']}, {pr['log_units']} log units, prices as of {prices.get('as_of', '?')}")

    ds = grid(pr["d_min_gb"], pr["d_max_gb"])
    disk_gb = float(pr.get("disk_gb_per_client", 0))
    series = {
        "cass_nvme": [model.cassandra(d, "nvme")["total"] for d in ds],
        "cass_ebs": [model.cassandra(d, "ebs")["total"] for d in ds],
        "ozone_hi_cache": [model.ozonedb(d, pr["cache_gb_high"])["total"] for d in ds],
        "ozone_lo_cache": [model.ozonedb(d, pr["cache_gb_low"])["total"] for d in ds],
        "ozone_today": [model.ozonedb(d, pr["cache_gb_high"], trimming=False)["total"] for d in ds],
        "ozone_disk": [model.ozonedb(d, pr["cache_gb_high"], disk_gb=disk_gb)["total"] for d in ds],
    }
    cass_min = [min(a, b) for a, b in zip(series["cass_nvme"], series["cass_ebs"])]
    crossover = find_crossover(ds, series["ozone_lo_cache"], cass_min)
    if crossover is None:
        side = "cheaper" if series["ozone_lo_cache"][0] < cass_min[0] else "dearer"
        crossover_note = f"OzoneDB ({pr['cache_gb_low']} GB cache) is {side} than the cheaper Cassandra layout over the whole range"
    else:
        crossover_note = f"crossover (OzoneDB with a {pr['cache_gb_low']} GB cache turns cheaper than the cheaper Cassandra layout): {fmt_gb(crossover)}"
    crossover_disk = find_crossover(ds, series["ozone_disk"], cass_min) if disk_gb else None
    if disk_gb and crossover_disk is None:
        side = "cheaper" if series["ozone_disk"][0] < cass_min[0] else "dearer"
        disk_note = (f"OzoneDB (+ {disk_gb:.0f} GB tier per client) is {side} than the cheaper "
                     f"Cassandra layout over the whole range")
    elif disk_gb:
        disk_note = (f"crossover with a {disk_gb:.0f} GB tier per client: {fmt_gb(crossover_disk)}")
    else:
        disk_note = "no disk-cache tier in this projection (projection.disk_gb_per_client is 0)"

    # Table.
    hdr = (f"{'D':>10} {'cass_nvme':>11} {'cass_ebs':>11} {'ozone_16GB':>11} {'ozone_4GB':>11} "
           f"{'today':>11} {'oz_disk':>11} {'h16':>6} {'h_disk':>6} {'nodes':>5}")
    print(hdr)
    lines = []
    for d, cn, ce, oh, ol, ot, od in zip(ds, series["cass_nvme"], series["cass_ebs"],
                                         series["ozone_hi_cache"], series["ozone_lo_cache"],
                                         series["ozone_today"], series["ozone_disk"]):
        gb = d / GB
        if abs(math.log10(gb) - round(math.log10(gb))) > 1e-6:
            continue
        oz = model.ozonedb(d, pr["cache_gb_high"])
        ozl = model.ozonedb(d, pr["cache_gb_low"])
        ozt = model.ozonedb(d, pr["cache_gb_high"], trimming=False)
        ozd = model.ozonedb(d, pr["cache_gb_high"], disk_gb=disk_gb)
        cs = model.cassandra(d, "nvme")
        cse = model.cassandra(d, "ebs")
        print(f"{fmt_gb(d):>10} {cn:>11,.0f} {ce:>11,.0f} {oh:>11,.0f} {ol:>11,.0f} {ot:>11,.0f} "
              f"{od:>11,.0f} {oz['h']:>6.3f} {ozd['h']:>6.3f} {cs['nodes']:>5}")
        # Totals first, then the cost lines of bench/PLAN-cost.md's projection
        # table (all USD per month, rounded to the dollar).
        r1 = lambda v: round(v)
        lines.append((fmt_gb(d), r1(cn), r1(ce), r1(oh), r1(ol), r1(ot), round(oz["h"], 4), cs["nodes"], cs["bound"],
                      r1(cs["node_cost"]), cse["nodes"], r1(cse["node_cost"]), cs["clients"], r1(cs["client_cost"]),
                      r1(oz["log_tier"]), r1(oz["bulk"]), r1(oz["get_cost"]), round(ozl["h"], 4), r1(ozl["get_cost"]),
                      r1(oz["put_cost"]), r1(oz["ckpt_cost"]), oz["clients"], r1(oz["client_cost"]),
                      r1(ozt["log_tier"]), round(ozt["log_gb"], 1),
                      r1(od), round(ozd["h"], 4), round(ozd["h_tier"], 4), round(disk_gb),
                      r1(ozd["get_cost"]), ozd["clients"], r1(ozd["client_cost"]), r1(ozd["disk_cost"])))
    print(crossover_note)
    print(disk_note)
    if args.table:
        cols = ["D", "cass_nvme", "cass_ebs", "ozone_hi_cache", "ozone_lo_cache", "ozone_today", "h_hi_cache",
                "cass_nodes", "cass_bound",
                "cass_nvme_nodes_usd", "cass_ebs_nodes", "cass_ebs_nodes_usd", "cass_clients", "cass_clients_usd",
                "ozone_log_tier_usd", "ozone_s3_storage_usd", "ozone_gets_hi_usd", "h_lo_cache", "ozone_gets_lo_usd",
                "ozone_puts_compaction_usd", "ozone_puts_checkpoint_usd", "ozone_clients", "ozone_clients_usd",
                "ozone_today_log_tier_usd", "ozone_today_log_gb",
                "ozone_disk_cache", "h_disk_cache", "disk_h_tier", "disk_gb", "ozone_disk_gets_usd",
                "ozone_disk_clients", "ozone_disk_clients_usd", "ozone_disk_gp3_usd"]
        with open(args.table, "w") as f:
            f.write("\t".join(cols) + "\n")
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
    if disk_gb:
        ax.plot(x, series["ozone_disk"], color="#117864", lw=1.6, ls=":",
                label=f"OzoneDB + trimming ({pr['cache_gb_high']} GB RAM + {disk_gb / 1000:.0f} TB gp3 per client)")
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
