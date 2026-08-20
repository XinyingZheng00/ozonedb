#!/usr/bin/env python3
"""Reduce a check-visibility run's visibility.csv to summary.json.

Stdlib-only and OZONEDB_HOME-independent so it runs anywhere the results
land: consistency.py imports it for same-host runs, and the cross-node
wrapper (run_visibility_cross_node.sh) invokes it on the laptop after
pulling the writer's and reader's output dirs together.

Cross-node runs pass --cross-node: writer and reader clocks are separate
CLOCK_MONOTONIC domains with arbitrary origins, so t_ack (writer clock) is
meaningless in the reader's frame and every latency is referenced to
t_notify -- the reader-side receipt of the ack notification -- instead.
That inflates visibility by one one-way network hop (~50 us on the LAN),
noise at the ms/50 ms scales being measured. The MISS COUNT needs no clock
at all: the first get provably starts after the ack (ack -> notify -> get
through the socket), so a first-get miss is a read-latest violation on any
clock.
"""

import argparse
import json
import os

# PBS-curve age buckets (ms): P[stale at age a] = P[visibility latency > a],
# the complementary CDF of the retry-until-found latency, so one run yields
# the whole curve.
PBS_AGES_MS = [0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500]


def _pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))]


def analyze(out, engine, linearizable, cross_node=False, params=None):
    """Read <out>/visibility.csv, write <out>/summary.json, print a verdict.

    engine: base engine name; '-linearizable' and '-xnode' suffixes are
    appended here from the flags. params: extra summary fields (rate_per_s,
    duration_s, value_size, poll_ms, stream, ...).
    """
    rows = []
    with open(os.path.join(out, "visibility.csv")) as f:
        next(f)
        for line in f:
            rows.append([int(x) for x in line.strip().split(",")])

    # Columns: idx, t_ack, t_notify, t_first, found_first, t_found, attempts
    ref = 2 if cross_node else 1
    checked = len(rows)
    misses = sum(1 for r in rows if r[4] == 0)
    timeouts = sum(1 for r in rows if r[5] < 0)
    vis_ms = [(r[5] - r[ref]) / 1e6 for r in rows if r[5] >= 0]
    first_age_ms = [(r[3] - r[ref]) / 1e6 for r in rows if r[3] >= 0]
    # Timeouts count as "still stale at every age" so they push the curve
    # up instead of silently vanishing from it.
    pbs = [
        {
            "age_ms_le": a,
            "p_stale": ((sum(1 for v in vis_ms if v > a) + timeouts) / checked)
            if checked else 0.0,
        }
        for a in PBS_AGES_MS
    ]

    if linearizable:
        engine += "-linearizable"
    if cross_node:
        engine += "-xnode"
    summary = {
        "engine": engine,
        "linearizable_reads": linearizable,
        "cross_node": cross_node,
        "channel": "tcp" if cross_node else "file",
        "inserts": checked,
        "checked": checked,
        "misses_first_read": misses,
        "miss_first_read_fraction": (misses / checked) if checked else 0.0,
        "timeouts": timeouts,
        "visibility_ms_p50": round(_pct(vis_ms, 50), 3),
        "visibility_ms_p90": round(_pct(vis_ms, 90), 3),
        "visibility_ms_p95": round(_pct(vis_ms, 95), 3),
        "visibility_ms_p99": round(_pct(vis_ms, 99), 3),
        "visibility_ms_max": round(max(vis_ms), 3) if vis_ms else 0.0,
        "first_check_age_ms_p50": round(_pct(first_age_ms, 50), 3),
        "first_check_age_ms_p99": round(_pct(first_age_ms, 99), 3),
        "pbs": pbs,
        "sync_interval_ms": 0,
    }
    if not cross_node:
        notify_lag_ms = [(r[2] - r[1]) / 1e6 for r in rows]
        summary["notify_lag_ms_p50"] = round(_pct(notify_lag_ms, 50), 4)
    summary.update(params or {})

    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    if linearizable and misses == 0 and timeouts == 0:
        verdict = ("0 first-read misses -- every key was visible on the first "
                   "get issued after its ack (read-latest holds)")
    elif linearizable:
        verdict = (f"{misses} first-read misses / {timeouts} timeouts UNDER "
                   "linearizable_reads -- should be impossible; check logs")
    else:
        verdict = (f"{misses} of {checked} first reads missed "
                   f"({summary['miss_first_read_fraction']:.1%}); "
                   f"visibility p50 {summary['visibility_ms_p50']} ms")
    print(f"\n=> {verdict}. Results in {out}")
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--dir", required=True, help="run dir holding visibility.csv")
    p.add_argument("--engine", default="ozonedb-corfu")
    p.add_argument("--linearizable", action="store_true")
    p.add_argument("--cross-node", action="store_true")
    p.add_argument("--rate", type=float, default=None)
    p.add_argument("--duration", type=int, default=None)
    p.add_argument("--value-size", type=int, default=None)
    p.add_argument("--poll-ms", type=float, default=None)
    p.add_argument("--stream", default=None)
    args = p.parse_args()
    params = {
        k: v
        for k, v in [
            ("rate_per_s", args.rate),
            ("duration_s", args.duration),
            ("value_size", args.value_size),
            ("poll_ms", args.poll_ms),
            ("stream", args.stream),
        ]
        if v is not None
    }
    analyze(args.dir, args.engine, args.linearizable,
            cross_node=args.cross_node, params=params)


if __name__ == "__main__":
    main()
