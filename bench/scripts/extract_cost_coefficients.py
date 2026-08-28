#!/usr/bin/env python3
"""Per-cell cost coefficients from a results directory (bench/PLAN-cost.md P0.6).

One TSV row per cell (label, workload, writers, trial), built from:

  per-writer YCSB files      {ks}-{opcnt}-{rc}-workload{wl}-{label}_w{i}of{N}_t{t}_trial{T}.result
  per-writer load files      {ks}-{rc}-insert-{label}_w{i}of{N}_t{t}.result   (workload "load")
  server samples             _server/<host>/{label}_workload{wl}_w{N}_trial{T}.{before,after,pidstat}.txt
                             (written by bench/scripts/server_sampler.sh via run_multinode_ycsb.py)

From the writer files: YCSB operation counts, the steady ops/s over the last
--window seconds (extract_steady_throughput.writer_steady), the block-cache
counters (`[lru_cache] sstable hits=… misses=…`, printed at DB close), the
open-time replay line (`[corfu] initial replay drained …`), the checkpoint
restore line (`[corfu] restoring checkpoint C=… files=…`), the trimmer lines
(`[trimmer] checkpoint C=… files=… live_MB=…`), the ack counters, and GNU
time's `User time` / `System time` / `Maximum resident set size`.

From the server samples: MinIO request counters by API (before/after delta),
bytes in and out, `du -sk` deltas, `mc du` of the bucket, and pidstat CPU
seconds per server process class (corfu, minio, cassandra).

Derived: h (hit rate), cache_ratio (cache bytes / dataset bytes), GETs per op,
GETs per miss (g), PUTs per op, join GETs (restore files + manifest + LATEST),
checkpoint objects (files + 2 per checkpoint), client CPU-seconds per op,
server CPU-seconds per op, and the fraction of the server box that was busy.

Usage:
  extract_cost_coefficients.py RESULTS_DIR [RESULTS_DIR ...] [--window 60]
                               [--record-bytes 1024] [--tsv out.tsv]
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extract_steady_throughput import writer_steady  # noqa: E402

RUN_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<label>.+?)_w(?P<widx>\d+)of(?P<total>\d+)_t(?P<thread>\d+)"
    r"_trial(?P<trial>\d+)\.result$"
)
LOAD_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<rc>\d+)-insert-(?P<label>.+?)_w(?P<widx>\d+)of(?P<total>\d+)"
    r"_t(?P<thread>\d+)\.result$"
)
# `_rc<N>` is the record count of a load cell. The two load wrappers write it
# because both dataset sizes of a campaign load into the same results root
# and the same sample cell name: the 10 GB load of cost-20260827 overwrote
# the 1 GB load's server sample. A sample with `_rc` matches only rows of
# that record count; a sample without it (run cells, whose tag directory
# already separates dataset sizes) matches any.
SAMPLE_RE = re.compile(
    r"^(?P<label>.+?)_workload(?P<wl>[^_]+)_w(?P<total>\d+)(?:_rc(?P<rc>\d+))?_trial(?P<trial>\d+)"
    r"\.(?P<kind>before|after|pidstat|minio)\.txt$"
)
SERIES_RE = re.compile(r"^(\d+) (\w+)(\{[^}]*\})? ([-+0-9.eE]+)$")

YCSB_RE = re.compile(r"^\[([A-Z\-]+)\],\s*([^,]+?),\s*(.+?)\s*$")
CACHE_RE = re.compile(
    r"\[lru_cache\] sstable hits=(\d+) misses=(\d+) hit_rate=([\d.]+)% capacity=(\d+)"
)
# Second stats line (bench/PLAN-compaction-cache.md part C): hits and misses
# per SSTable level as "<level>:<count>" pairs, blocks of files no longer in
# the View, retired Table objects, and the warm-worker counters.
LEVELS_RE = re.compile(
    r"\[lru_cache\] levels hits=(?P<hits>[\d:,]+) misses=(?P<misses>[\d:,]+)"
    r" sstable_files=(?P<sstable_files>\d+) dead_files=(?P<dead_files>\d+)"
    r" dead_blocks=(?P<dead_blocks>\d+) dead_bytes=(?P<dead_bytes>\d+)"
    r" retired=(?P<retired>\d+) warm files=(?P<warm_files>\d+) blocks=(?P<warm_blocks>\d+)"
    r" bytes=(?P<warm_bytes>\d+) skipped_disabled=(?P<skipped_disabled>\d+)"
    r" skipped_level=(?P<skipped_level>\d+) skipped_budget=(?P<skipped_budget>\d+)"
    r" skipped_affinity=(?P<skipped_affinity>\d+) skipped_gone=(?P<skipped_gone>\d+)"
    r" skipped_built=(?P<skipped_built>\d+) dropped=(?P<dropped>\d+) warm_hits=(?P<warm_hits>\d+)"
)
# Disk-cache tier stats line (bench/PLAN-disk-cache.md), printed after the
# [lru_cache] lines when disk_cache_dir is set. All twenty fields are parsed;
# only a subset become TSV columns (build_row).
DISK_RE = re.compile(
    r"\[disk_cache\] hits=(?P<hits>\d+) misses=(?P<misses>\d+) hit_bytes=(?P<hit_bytes>\d+)"
    r" miss_bytes=(?P<miss_bytes>\d+) passthrough=(?P<passthrough>\d+) fills=(?P<fills>\d+)"
    r" fill_bytes=(?P<fill_bytes>\d+) fill_gets=(?P<fill_gets>\d+)"
    r" fill_skipped_budget=(?P<fill_skipped_budget>\d+) fill_skipped_present=(?P<fill_skipped_present>\d+)"
    r" fill_gone=(?P<fill_gone>\d+) fill_failed=(?P<fill_failed>\d+) fill_dropped=(?P<fill_dropped>\d+)"
    r" writethrough_files=(?P<writethrough_files>\d+) evictions=(?P<evictions>\d+)"
    r" evicted_bytes=(?P<evicted_bytes>\d+) invalidated=(?P<invalidated>\d+) files=(?P<files>\d+)"
    r" bytes=(?P<bytes>\d+) capacity=(?P<capacity>\d+)"
)
LEVEL_COLUMNS = ("l1", "l2", "l3", "l4plus")


def level_pairs(s):
    """'0:5,1:194,2:12' -> {0: 5, 1: 194, 2: 12}."""
    out = {}
    for pair in s.split(","):
        if ":" in pair:
            lvl, n = pair.split(":", 1)
            out[int(lvl)] = int(n)
    return out


def level_buckets(counts):
    """{level: n} -> {l1, l2, l3, l4plus}; slot 0 (non-SSTable names) is dropped."""
    b = {c: 0 for c in LEVEL_COLUMNS}
    for lvl, n in counts.items():
        if lvl == 1:
            b["l1"] += n
        elif lvl == 2:
            b["l2"] += n
        elif lvl == 3:
            b["l3"] += n
        elif lvl >= 4:
            b["l4plus"] += n
    return b
REPLAY_RE = re.compile(
    r"\[corfu\] initial replay drained (\d+) entries in (\d+) batches, (\d+) ms.*?"
    r"stream_MB=(\d+) live_files=(\d+) live_MB=(\d+)"
)
RESTORE_RE = re.compile(r"\[corfu\] restoring checkpoint C=(\d+) files=(\d+) live_MB=(\d+)")
TRIMMER_RE = re.compile(
    r"\[trimmer\] checkpoint C=(\d+) prev=(-?\d+) files=(\d+) live_MB=(\d+).*?upload_ms=(\d+)"
)
ACK_RE = re.compile(r"\[corfu\] ack: fast=(\d+) slow=(\d+)")
TIME_USER_RE = re.compile(r"User time \(seconds\): ([\d.]+)")
TIME_SYS_RE = re.compile(r"System time \(seconds\): ([\d.]+)")
TIME_RSS_RE = re.compile(r"Maximum resident set size \(kbytes\): (\d+)")

METRIC_RE = re.compile(r"^metric (\w+)(\{[^}]*\})? ([-+0-9.eE]+)$")
LABEL_RE = re.compile(r'(\w+)="([^"]*)"')

# S3 bills by request class. GET-class: GET, HEAD (and SELECT). PUT-class:
# PUT, COPY, POST, LIST. DELETE is free. MinIO's api labels are lowercase.
GET_APIS = {"getobject"}
HEAD_APIS = {"headobject"}
PUT_APIS = {"putobject", "putobjectpart", "newmultipartupload",
            "completemultipartupload", "copyobject", "postpolicybucket"}
LIST_APIS = {"listobjects", "listobjectsv1", "listobjectsv2", "listobjectversions",
             "listbuckets", "listmultipartuploads", "listobjectparts"}
DELETE_APIS = {"deleteobject", "deleteobjects", "deletemultipleobjects"}

DU_CLASSES = (("corfu", "/mnt/corfu/run_batch"), ("corfu_load", "/mnt/corfu/load"),
              ("minio", "/tank/minio"), ("cassandra", "/tank/cassandra/data"))


def fnum(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------- writers --

def parse_writer(path, window):
    """Everything one per-writer .result file says."""
    w = {
        "ops": {}, "failed": 0, "runtime_ms": None,
        "cache_hits": None, "cache_misses": None, "cache_capacity": None,
        "levels": None, "disk": None,
        "replay": None, "restore": None, "ckpts": [], "ack_fast": None, "ack_slow": None,
        "user_s": None, "sys_s": None, "rss_kb": None,
    }
    with open(path, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            m = YCSB_RE.match(line.strip())
            if m:
                sec, key, val = m.group(1), m.group(2).strip(), m.group(3).strip()
                if sec == "OVERALL" and key == "RunTime(ms)":
                    w["runtime_ms"] = fnum(val)
                elif key == "Operations":
                    n = fnum(val) or 0.0
                    if sec.endswith("-FAILED"):
                        w["failed"] += n
                    elif sec not in ("OVERALL",):
                        w["ops"][sec] = n
                continue
            m = CACHE_RE.search(line)
            if m:
                w["cache_hits"] = int(m.group(1))
                w["cache_misses"] = int(m.group(2))
                w["cache_capacity"] = int(m.group(4))
                continue
            m = LEVELS_RE.search(line)
            if m:
                d = {k: int(v) for k, v in m.groupdict().items() if k not in ("hits", "misses")}
                d["hits"] = level_buckets(level_pairs(m.group("hits")))
                d["misses"] = level_buckets(level_pairs(m.group("misses")))
                w["levels"] = d
                continue
            m = DISK_RE.search(line)
            if m:
                w["disk"] = {k: int(v) for k, v in m.groupdict().items()}
                continue
            m = REPLAY_RE.search(line)
            if m:
                w["replay"] = {
                    "entries": int(m.group(1)), "ms": int(m.group(3)),
                    "stream_mb": int(m.group(4)), "live_files": int(m.group(5)),
                    "live_mb": int(m.group(6)),
                }
                continue
            m = RESTORE_RE.search(line)
            if m:
                w["restore"] = {"addr": int(m.group(1)), "files": int(m.group(2)),
                                "live_mb": int(m.group(3))}
                continue
            m = TRIMMER_RE.search(line)
            if m:
                w["ckpts"].append({"files": int(m.group(3)), "live_mb": int(m.group(4)),
                                   "upload_ms": int(m.group(5))})
                continue
            m = ACK_RE.search(line)
            if m:
                w["ack_fast"], w["ack_slow"] = int(m.group(1)), int(m.group(2))
                continue
            m = TIME_USER_RE.search(line)
            if m:
                w["user_s"] = float(m.group(1))
                continue
            m = TIME_SYS_RE.search(line)
            if m:
                w["sys_s"] = float(m.group(1))
                continue
            m = TIME_RSS_RE.search(line)
            if m:
                w["rss_kb"] = int(m.group(1))
                continue
    rate, _failed, last_t = writer_steady(path, window)
    w["steady"] = rate
    w["last_t"] = last_t
    return w


# ----------------------------------------------------------------- samples --

def parse_snapshot(path):
    """One server_sampler.sh snapshot -> dict."""
    s = {"time": None, "nproc": None, "api": defaultdict(float), "bytes_in": 0.0,
         "bytes_out": 0.0, "du": {}, "mcdu": {}, "pids": {}, "errors": []}
    with open(path, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("# server_sampler"):
                mt = re.search(r"time=(\d+)", line)
                mn = re.search(r"nproc=(\d+)", line)
                s["time"] = int(mt.group(1)) if mt else None
                s["nproc"] = int(mn.group(1)) if mn else None
                continue
            m = METRIC_RE.match(line)
            if m:
                name, labels, val = m.group(1), m.group(2) or "", float(m.group(3))
                lab = dict(LABEL_RE.findall(labels))
                if name == "minio_s3_requests_total":
                    s["api"][lab.get("api", "?").lower()] += val
                elif name == "minio_s3_traffic_received_bytes":
                    s["bytes_in"] += val
                elif name == "minio_s3_traffic_sent_bytes":
                    s["bytes_out"] += val
                continue
            if line.startswith("metric_error "):
                s["errors"].append(line)
                continue
            if line.startswith("du "):
                parts = line.split()
                if len(parts) == 3:
                    s["du"][parts[1]] = int(parts[2])
                continue
            if line.startswith("mcdu "):
                try:
                    j = json.loads(line[5:])
                    s["mcdu"][j.get("prefix") or j.get("key") or "?"] = float(j.get("size", 0))
                except ValueError:
                    pass
                continue
            if line.startswith("pids "):
                parts = line.split()
                if len(parts) >= 3:
                    s["pids"][parts[1]] = [int(p) for p in parts[2].split(",") if p]
                continue
    return s


def parse_pidstat(path, pid_class, default_interval=10.0):
    """CPU-seconds per process class from `pidstat -h -u -r -p … N` output.
    Every row is one process over one interval; %CPU x interval / 100 is its
    CPU-seconds. The interval is the gap between consecutive samples of the
    same pid (default_interval before the second sample)."""
    cpu_s = defaultdict(float)
    rss_kb = defaultdict(float)
    last_t = {}
    cols = None
    t0, t1 = None, None
    samples = 0
    with open(path, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                cols = line[1:].split()
                continue
            if cols is None:
                continue
            parts = line.split()
            if len(parts) < len(cols) - 1:
                continue
            # `pidstat -h -H` prints epoch seconds in the Time column. Without
            # -H (older sysstat) it prints a clock time, possibly with AM/PM as
            # an extra token; then every row counts as one default interval.
            if len(parts) == len(cols) + 1 and parts[1] in ("AM", "PM"):
                parts = [parts[0] + parts[1]] + parts[2:]
            row = dict(zip(cols, parts))
            try:
                pid = int(row.get("PID", "-1"))
                pct = float(row.get("%CPU", "0"))
                rss = float(row.get("RSS", "0"))
            except ValueError:
                continue
            try:
                t = float(row.get("Time", "nan"))
                if t != t:
                    raise ValueError
            except ValueError:
                t = None
            samples += 1
            if t is not None:
                t0 = t if t0 is None else min(t0, t)
                t1 = t if t1 is None else max(t1, t)
                interval = (t - last_t[pid]) if pid in last_t and t > last_t[pid] else default_interval
                last_t[pid] = t
            else:
                interval = default_interval
            cls = pid_class.get(pid, "other")
            cpu_s[cls] += pct * interval / 100.0
            rss_kb[cls] = max(rss_kb[cls], rss)
    if t0 is not None and t1 is not None:
        elapsed = t1 - t0 + default_interval
    elif samples:
        elapsed = samples / max(1, len({p for p in last_t} or {0})) * default_interval
    else:
        elapsed = None
    return cpu_s, rss_kb, elapsed


def parse_series(path):
    """DIR/CELL.minio.txt -> sorted [(t, {"get","put","in","out"})], cumulative."""
    by_t = {}
    with open(path, errors="replace") as f:
        for line in f:
            m = SERIES_RE.match(line.strip())
            if not m:
                continue
            t, name, labels, val = int(m.group(1)), m.group(2), m.group(3) or "", float(m.group(4))
            d = by_t.setdefault(t, {"get": 0.0, "put": 0.0, "in": 0.0, "out": 0.0})
            if name == "minio_s3_requests_total":
                api = dict(LABEL_RE.findall(labels)).get("api", "").lower()
                if api in GET_APIS or api in HEAD_APIS:
                    d["get"] += val
                elif api in PUT_APIS or api in LIST_APIS:
                    d["put"] += val
            elif name == "minio_s3_traffic_received_bytes":
                d["in"] += val
            elif name == "minio_s3_traffic_sent_bytes":
                d["out"] += val
    return sorted(by_t.items())


def steady_rates(series, window, end_at=None):
    """Request rates over the last `window` seconds of the run.

    `end_at` is the writers' last activity in sampler epoch seconds; the
    window ends at the last sample at or before it, so the idle tail between
    the writers' exit and the sampler's stop is not averaged in. Without it
    the window ends at the last sample whose counters moved, which is only
    the same thing while the object store is busy for as long as the writers
    are: with a disk-cache tier that holds the dataset the counters stop at
    the end of the fill burst, and that fallback would report the fill as
    the steady state."""
    if len(series) < 2:
        return None
    end = None
    if end_at is not None:
        for t, s in series:
            if t <= end_at:
                end = (t, s)
        if end is not None and end[0] <= series[0][0]:
            end = None  # no window left; fall back to the counters
    if end is None:
        for (t0, a), (t1, b) in zip(series, series[1:]):
            if b["get"] > a["get"] or b["put"] > a["put"]:
                end = (t1, b)
    if end is None:
        return None
    t_end, s_end = end
    start = None
    for t, s in series:
        if t >= t_end - window:
            start = (t, s)
            break
    if start is None or start[0] >= t_end:
        return None
    dt = float(t_end - start[0])
    return {k: (s_end[k] - start[1][k]) / dt for k in ("get", "put", "in", "out")} | {"seconds": dt}


def load_samples(results_dir):
    """{(label, wl, total, trial): [per-host dict]} from _server/*/."""
    root = os.path.join(results_dir, "_server")
    out = defaultdict(list)
    if not os.path.isdir(root):
        return out
    for host in sorted(os.listdir(root)):
        hdir = os.path.join(root, host)
        if not os.path.isdir(hdir):
            continue
        cells = defaultdict(dict)
        for fn in os.listdir(hdir):
            m = SAMPLE_RE.match(fn)
            if m:
                rc = int(m.group("rc")) if m.group("rc") else None
                cells[(m.group("label"), m.group("wl"), int(m.group("total")),
                       m.group("trial"), rc)][m.group("kind")] = os.path.join(hdir, fn)
        for key, files in cells.items():
            entry = {"host": host, "before": None, "after": None, "cpu_s": {},
                     "rss_kb": {}, "elapsed": None, "series": []}
            if "before" in files:
                entry["before"] = parse_snapshot(files["before"])
            if "after" in files:
                entry["after"] = parse_snapshot(files["after"])
            if "minio" in files:
                entry["series"] = parse_series(files["minio"])
            if "pidstat" in files:
                pid_class = {}
                src = entry["before"] or entry["after"]
                if src:
                    for cls, pids in src["pids"].items():
                        for p in pids:
                            pid_class[p] = cls
                entry["cpu_s"], entry["rss_kb"], entry["elapsed"] = parse_pidstat(
                    files["pidstat"], pid_class)
            out[key].append(entry)
    return out


def samples_for(samples, label, wl, total, trial, rc=None):
    """Exact cell first (record-count-tagged, then untagged); else a sample
    whose workload token lists this workload (one orchestrator call that
    ran several workloads). A record-count-tagged sample never matches a
    row of another record count."""
    for key in ((label, wl, total, trial, rc), (label, wl, total, trial, None)):
        exact = samples.get(key)
        if exact:
            return exact, False
    for (l, w, t, tr, r), entries in samples.items():
        if l == label and t == total and tr == trial and wl in w.split("+") \
                and r in (None, rc):
            return entries, True
    return [], False


# -------------------------------------------------------------------- cells --

def api_sum(snap, apis):
    return sum(v for k, v in snap["api"].items() if k in apis)


def build_row(key, writers, samples, shared, record_bytes, window=60, tag=""):
    label, wl, total, trial, rc = key
    have = len(writers)
    ws = list(writers.values())
    reads = sum(w["ops"].get("READ", 0) for w in ws)
    writes = sum(w["ops"].get(k, 0) for w in ws for k in ("UPDATE", "INSERT", "READ-MODIFY-WRITE"))
    scans = sum(w["ops"].get("SCAN", 0) for w in ws)
    ops = reads + writes + scans
    failed = sum(w["failed"] for w in ws)
    steady = [w["steady"] for w in ws if w["steady"] is not None]
    hits = [w["cache_hits"] for w in ws if w["cache_hits"] is not None]
    misses = [w["cache_misses"] for w in ws if w["cache_misses"] is not None]
    caps = [w["cache_capacity"] for w in ws if w["cache_capacity"] is not None]
    users = [w["user_s"] for w in ws if w["user_s"] is not None]
    syss = [w["sys_s"] for w in ws if w["sys_s"] is not None]
    rss = [w["rss_kb"] for w in ws if w["rss_kb"] is not None]
    replays = [w["replay"] for w in ws if w["replay"]]
    restores = [w["restore"] for w in ws if w["restore"]]
    ckpts = [c for w in ws for c in w["ckpts"]]
    writer_last_t = max((w["last_t"] for w in ws if w["last_t"]), default=None)
    notes = []
    if have != total:
        notes.append(f"missing_writers={total - have}")

    row = {
        "tag": tag,
        "label": label, "workload": wl, "writers": total, "have": have, "trial": trial,
        "record_cnt": rc, "dataset_bytes": rc * record_bytes if rc else "",
        "ops": int(ops), "reads": int(reads), "writes": int(writes), "failed": int(failed),
        "run_s": min((w["last_t"] for w in ws if w["last_t"]), default=""),
        "steady_ops_s": round(sum(steady), 1) if steady else "",
        "cache_hits": sum(hits) if hits else "",
        "cache_misses": sum(misses) if misses else "",
        "h": round(sum(hits) / (sum(hits) + sum(misses)), 5) if hits and (sum(hits) + sum(misses)) else "",
        "cache_capacity": caps[0] if caps else "",
        "cache_ratio": round(caps[0] / (rc * record_bytes), 7) if caps and rc else "",
    }
    # Second cache line: sums over the writers that printed it. Absent on
    # result files from builds before part C, so every column stays "".
    levels = [w["levels"] for w in ws if w["levels"]]
    if levels:
        for c in LEVEL_COLUMNS:
            row[f"hits_{c}"] = sum(lv["hits"][c] for lv in levels)
            row[f"misses_{c}"] = sum(lv["misses"][c] for lv in levels)
        for src, dst in (("dead_files", "cache_dead_files"), ("dead_blocks", "cache_dead_blocks"),
                         ("dead_bytes", "cache_dead_bytes"), ("retired", "cache_retired_tables"),
                         ("warm_files", "warm_files"), ("warm_blocks", "warm_blocks"),
                         ("warm_bytes", "warm_bytes"), ("warm_hits", "warm_hits"),
                         ("skipped_disabled", "warm_skipped_disabled"),
                         ("skipped_level", "warm_skipped_level"),
                         ("skipped_budget", "warm_skipped_budget"),
                         ("skipped_affinity", "warm_skipped_affinity"),
                         ("skipped_gone", "warm_skipped_gone"),
                         ("skipped_built", "warm_skipped_built"),
                         ("dropped", "warm_dropped")):
            row[dst] = sum(lv[src] for lv in levels)
        wb = row["warm_blocks"]
        row["warm_hit_frac"] = round(row["warm_hits"] / wb, 4) if wb else ""
    # Disk-cache tier: sums over the writers that printed the line. Absent on
    # cells run without disk_cache_dir, so every disk_* column stays "".
    disks = [w["disk"] for w in ws if w["disk"]]
    if disks:
        disk_sums = {k: sum(d[k] for d in disks) for k in disks[0]}
        disk_denom = disk_sums["hits"] + disk_sums["misses"]
        disk_h = round(disk_sums["hits"] / disk_denom, 5) if disk_denom else ""
        h_val = row["h"]
        h_total = round(1 - (1 - h_val) * (1 - disk_h), 5) if h_val != "" and disk_h != "" else ""
        row.update({
            "disk_hits": disk_sums["hits"],
            "disk_misses": disk_sums["misses"],
            "disk_h": disk_h,
            "disk_hit_bytes": disk_sums["hit_bytes"],
            "disk_miss_bytes": disk_sums["miss_bytes"],
            "disk_fills": disk_sums["fills"],
            "disk_fill_bytes": disk_sums["fill_bytes"],
            "disk_fill_gets": disk_sums["fill_gets"],
            "disk_fill_gets_per_op": round(disk_sums["fill_gets"] / ops, 5) if ops else "",
            "disk_evictions": disk_sums["evictions"],
            "disk_invalidated": disk_sums["invalidated"],
            "disk_files": disk_sums["files"],
            "disk_bytes": disk_sums["bytes"],
            "disk_capacity": disk_sums["capacity"],
            "disk_ratio": round(disks[0]["capacity"] / (rc * record_bytes), 7) if rc else "",
            "h_total": h_total,
        })
    # Server samples: request deltas, bytes, du, bucket, CPU.
    agg = {"get": 0.0, "head": 0.0, "put": 0.0, "list": 0.0, "delete": 0.0, "other": 0.0,
           "bytes_in": 0.0, "bytes_out": 0.0}
    du_after, du_delta = {}, {}
    bucket_bytes, ckpt_bytes = None, None
    cpu = defaultdict(float)
    nproc, elapsed = None, None
    have_counters = False
    rates = None
    for e in samples:
        b, a = e["before"], e["after"]
        if e["series"] and rates is None:
            # The writers report elapsed seconds; the sampler reports epoch
            # seconds and its `before` snapshot is taken at cell start, so
            # start + the longest writer's last status line is the end of
            # the run on the sampler's clock.
            start_t = (e["before"] or {}).get("time") or e["series"][0][0]
            end_at = start_t + writer_last_t if writer_last_t else None
            rates = steady_rates(e["series"], window, end_at)
        if b and a and not (b["errors"] and a["errors"]):
            have_counters = True
            for cls, apis in (("get", GET_APIS), ("head", HEAD_APIS), ("put", PUT_APIS),
                              ("list", LIST_APIS), ("delete", DELETE_APIS)):
                agg[cls] += api_sum(a, apis) - api_sum(b, apis)
            known = GET_APIS | HEAD_APIS | PUT_APIS | LIST_APIS | DELETE_APIS
            agg["other"] += sum(v for k, v in a["api"].items() if k not in known) - \
                sum(v for k, v in b["api"].items() if k not in known)
            agg["bytes_in"] += a["bytes_in"] - b["bytes_in"]
            agg["bytes_out"] += a["bytes_out"] - b["bytes_out"]
        if a:
            for cls, path in DU_CLASSES:
                if path in a["du"]:
                    du_after[cls] = a["du"][path]
                    if b and path in b["du"]:
                        du_delta[cls] = a["du"][path] - b["du"][path]
            if a["mcdu"]:
                tops = {k: v for k, v in a["mcdu"].items() if k.count("/") <= 1}
                bucket_bytes = max(a["mcdu"].values()) if not tops else max(tops.values())
                ck = [v for k, v in a["mcdu"].items() if "checkpoint" in k]
                if ck:
                    ckpt_bytes = max(ck)
            nproc = a["nproc"] or nproc
        for cls, v in e["cpu_s"].items():
            cpu[cls] += v
        if e["elapsed"]:
            elapsed = max(elapsed or 0, e["elapsed"])
    if samples and not have_counters:
        notes.append("no_minio_counters")
    if not samples:
        notes.append("no_server_sample")
    if shared:
        notes.append("server_sample_shared_across_workloads")

    get_class = agg["get"] + agg["head"]
    put_class = agg["put"] + agg["list"]
    # Steady state: the request rate over the last window of activity against
    # the steady ops/s of the same window. h_steady assumes every miss costs
    # g GETs (g from the cumulative counters of this cell) and charges all
    # GETs to reads, so it is exact for a read-only workload and slightly
    # pessimistic under writes (compaction input reads count as misses).
    steady_sum = sum(steady) if steady else 0.0
    g_cum = get_class / sum(misses) if (have_counters and misses and sum(misses)) else None
    read_frac = reads / ops if ops else 0.0
    get_per_op_steady = (rates["get"] / steady_sum) if (rates and steady_sum) else None
    h_steady = None
    if get_per_op_steady is not None and g_cum and read_frac > 0:
        h_steady = max(0.0, min(1.0, 1.0 - get_per_op_steady / (g_cum * read_frac)))
    row.update({
        "s3_get_rate_steady": round(rates["get"], 2) if rates else "",
        "s3_put_rate_steady": round(rates["put"], 3) if rates else "",
        "s3_bytes_out_rate_steady": round(rates["out"]) if rates else "",
        "steady_window_s": round(rates["seconds"]) if rates else "",
        "get_per_op_steady": round(get_per_op_steady, 5) if get_per_op_steady is not None else "",
        "h_steady": round(h_steady, 5) if h_steady is not None else "",
        "s3_get": int(agg["get"]) if have_counters else "",
        "s3_head": int(agg["head"]) if have_counters else "",
        "s3_put": int(agg["put"]) if have_counters else "",
        "s3_list": int(agg["list"]) if have_counters else "",
        "s3_delete": int(agg["delete"]) if have_counters else "",
        "s3_other": int(agg["other"]) if have_counters else "",
        "s3_bytes_in": int(agg["bytes_in"]) if have_counters else "",
        "s3_bytes_out": int(agg["bytes_out"]) if have_counters else "",
        "get_per_op": round(get_class / ops, 5) if have_counters and ops else "",
        "get_per_miss": round(get_class / sum(misses), 4) if have_counters and misses and sum(misses) else "",
        "put_per_op": round(put_class / ops, 6) if have_counters and ops else "",
        "put_per_write": round(put_class / writes, 6) if have_counters and writes else "",
        "du_corfu_kb": du_after.get("corfu", ""),
        "du_corfu_delta_kb": du_delta.get("corfu", ""),
        "du_minio_kb": du_after.get("minio", ""),
        "du_minio_delta_kb": du_delta.get("minio", ""),
        "du_cassandra_kb": du_after.get("cassandra", ""),
        "du_cassandra_delta_kb": du_delta.get("cassandra", ""),
        "bucket_bytes": int(bucket_bytes) if bucket_bytes is not None else "",
        "checkpoint_bytes": int(ckpt_bytes) if ckpt_bytes is not None else "",
        "client_user_s": round(sum(users), 2) if users else "",
        "client_sys_s": round(sum(syss), 2) if syss else "",
        "client_cpu_s_per_op": round((sum(users) + sum(syss)) / ops, 7) if users and ops else "",
        "client_rss_max_kb": max(rss) if rss else "",
        "replay_entries": round(sum(r["entries"] for r in replays) / len(replays)) if replays else "",
        "replay_ms": round(sum(r["ms"] for r in replays) / len(replays)) if replays else "",
        "replay_stream_mb": round(sum(r["stream_mb"] for r in replays) / len(replays), 1) if replays else "",
        "replay_live_files": round(sum(r["live_files"] for r in replays) / len(replays), 1) if replays else "",
        "replay_live_mb": round(sum(r["live_mb"] for r in replays) / len(replays), 1) if replays else "",
        "restore_files": round(sum(r["files"] for r in restores) / len(restores), 1) if restores else "",
        "restore_live_mb": round(sum(r["live_mb"] for r in restores) / len(restores), 1) if restores else "",
        # One GET per file object, plus the manifest and LATEST (checkpoint.cpp).
        "join_gets": round(sum(r["files"] for r in restores) / len(restores) + 2, 1) if restores else "",
        "ckpt_count": len(ckpts),
        "ckpt_files_mean": round(sum(c["files"] for c in ckpts) / len(ckpts), 1) if ckpts else "",
        # One PUT per file object, plus the manifest and LATEST, per checkpoint.
        "ckpt_objects": sum(c["files"] for c in ckpts) + 2 * len(ckpts) if ckpts else "",
        "ckpt_live_mb_mean": round(sum(c["live_mb"] for c in ckpts) / len(ckpts), 1) if ckpts else "",
        "ckpt_upload_ms_mean": round(sum(c["upload_ms"] for c in ckpts) / len(ckpts)) if ckpts else "",
        "ack_fast": sum(w["ack_fast"] for w in ws if w["ack_fast"] is not None),
        "ack_slow": sum(w["ack_slow"] for w in ws if w["ack_slow"] is not None),
        "server_cpu_corfu_s": round(cpu.get("corfu", 0.0), 1) if cpu else "",
        "server_cpu_minio_s": round(cpu.get("minio", 0.0), 1) if cpu else "",
        "server_cpu_cassandra_s": round(cpu.get("cassandra", 0.0), 1) if cpu else "",
        "server_cpu_s_per_op": round(sum(cpu.values()) / ops, 7) if cpu and ops else "",
        "server_nproc": nproc or "",
        "server_elapsed_s": round(elapsed) if elapsed else "",
        # Busy fraction over the writers' run window, not the sampler's
        # elapsed time: a cell whose orchestrator overran (ssh hang) has a
        # long idle tail in the sample that would dilute the fraction.
        "server_busy_frac": round(sum(cpu.values()) / ((row.get("run_s") or elapsed) * nproc), 4) if cpu and (row.get("run_s") or elapsed) and nproc else "",
        "notes": ";".join(notes),
    })
    return row


COLUMNS = [
    "tag", "label", "workload", "writers", "have", "trial", "record_cnt", "dataset_bytes",
    "ops", "reads", "writes", "failed", "run_s", "steady_ops_s",
    "cache_hits", "cache_misses", "h", "cache_capacity", "cache_ratio",
    "disk_hits", "disk_misses", "disk_h", "disk_hit_bytes", "disk_miss_bytes",
    "disk_fills", "disk_fill_bytes", "disk_fill_gets", "disk_fill_gets_per_op",
    "disk_evictions", "disk_invalidated", "disk_files", "disk_bytes", "disk_capacity",
    "disk_ratio", "h_total",
    "hits_l1", "hits_l2", "hits_l3", "hits_l4plus",
    "misses_l1", "misses_l2", "misses_l3", "misses_l4plus",
    "cache_dead_files", "cache_dead_blocks", "cache_dead_bytes", "cache_retired_tables",
    "warm_files", "warm_blocks", "warm_bytes", "warm_hits", "warm_hit_frac",
    "warm_skipped_disabled", "warm_skipped_level", "warm_skipped_budget",
    "warm_skipped_affinity", "warm_skipped_gone", "warm_skipped_built", "warm_dropped",
    "s3_get", "s3_head", "s3_put", "s3_list", "s3_delete", "s3_other",
    "s3_bytes_in", "s3_bytes_out", "get_per_op", "get_per_miss", "put_per_op", "put_per_write",
    "s3_get_rate_steady", "s3_put_rate_steady", "s3_bytes_out_rate_steady", "steady_window_s",
    "get_per_op_steady", "h_steady",
    "du_corfu_kb", "du_corfu_delta_kb", "du_minio_kb", "du_minio_delta_kb",
    "du_cassandra_kb", "du_cassandra_delta_kb", "bucket_bytes", "checkpoint_bytes",
    "client_user_s", "client_sys_s", "client_cpu_s_per_op", "client_rss_max_kb",
    "replay_entries", "replay_ms", "replay_stream_mb", "replay_live_files", "replay_live_mb",
    "restore_files", "restore_live_mb", "join_gets",
    "ckpt_count", "ckpt_files_mean", "ckpt_objects", "ckpt_live_mb_mean", "ckpt_upload_ms_mean",
    "ack_fast", "ack_slow",
    "server_cpu_corfu_s", "server_cpu_minio_s", "server_cpu_cassandra_s", "server_cpu_s_per_op",
    "server_nproc", "server_elapsed_s", "server_busy_frac", "notes",
]


def collect(results_dir, window, record_bytes):
    cells = defaultdict(dict)  # (label, wl, total, trial, rc) -> {widx: writer}
    for fn in sorted(os.listdir(results_dir)):
        full = os.path.join(results_dir, fn)
        if not os.path.isfile(full):
            continue
        m = RUN_RE.match(fn)
        if m:
            key = (m.group("label"), m.group("wl"), int(m.group("total")),
                   m.group("trial"), int(m.group("rc")))
            cells[key][int(m.group("widx"))] = parse_writer(full, window)
            continue
        m = LOAD_RE.match(fn)
        if m:
            key = (m.group("label"), "load", int(m.group("total")), "0", int(m.group("rc")))
            cells[key][int(m.group("widx"))] = parse_writer(full, window)
    samples = load_samples(results_dir)
    tag = os.path.basename(os.path.normpath(results_dir))
    rows = []
    for key, writers in cells.items():
        label, wl, total, trial, rc = key
        entries, shared = samples_for(samples, label, wl, total, trial, rc)
        rows.append(build_row(key, writers, entries, shared, record_bytes, window, tag))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results_dirs", nargs="+")
    ap.add_argument("--window", type=int, default=60, help="trailing seconds for steady ops/s (default 60)")
    ap.add_argument("--record-bytes", type=int, default=1024,
                    help="logical bytes per record for cache_ratio and dataset_bytes (default 1024)")
    ap.add_argument("--tsv", help="write the rows here")
    args = ap.parse_args()

    rows = []
    for d in args.results_dirs:
        if not os.path.isdir(d):
            sys.exit(f"no such dir: {d}")
        rows.extend(collect(d, args.window, args.record_bytes))
    rows.sort(key=lambda r: (r["label"], r["workload"], r["writers"], r["trial"], r["tag"]))

    hdr = (f"{'tag':22} {'label':34} {'wl':4} {'w':3} {'have':4} {'steady':9} {'h':8} {'h_stdy':8} {'ratio':10} "
           f"{'get/op':8} {'get/op_s':8} {'g':7} {'put/op':9} {'cpu_s/op':10} {'srv_s/op':10} {'ckpt':5} {'join':5} notes")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{r['tag'][:22]:22} {r['label']:34} {r['workload']:4} {r['writers']:3} {r['have']:4} "
              f"{str(r['steady_ops_s']):9} {str(r['h']):8} {str(r['h_steady']):8} {str(r['cache_ratio']):10} "
              f"{str(r['get_per_op']):8} {str(r['get_per_op_steady']):8} {str(r['get_per_miss']):7} {str(r['put_per_op']):9} "
              f"{str(r['client_cpu_s_per_op']):10} {str(r['server_cpu_s_per_op']):10} "
              f"{str(r['ckpt_count']):5} {str(r['join_gets']):5} {r['notes']}")

    if args.tsv:
        with open(args.tsv, "w") as f:
            f.write("\t".join(COLUMNS) + "\n")
            for r in rows:
                f.write("\t".join(str(r.get(c, "")) for c in COLUMNS) + "\n")
        print(f"\nwrote {args.tsv} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
