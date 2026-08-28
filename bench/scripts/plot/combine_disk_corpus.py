#!/usr/bin/env python3
"""Assemble the coefficient corpus the disk-cache projection is built from.

`plot_cost_model.py` takes one coefficient TSV, but the disk-cache projection
needs coefficients from four campaigns, and their column sets differ (the
`disk_*` columns only exist from `disk-20260829` on). So the sources are merged
as dicts over the union of their headers rather than concatenated as lines, and
duplicate cells (same tag/label/workload/writers/trial/record_cnt) are dropped,
first source winning.

    disk2-<date>             the round-2 tier cells (chunk mode / TinyLFU
                             admission), from --disk2 PATH; skipped when the
                             file is absent (bench/PLAN-disk-cache-2.md)
  + disk-20260829            the round-1 tier cells (the stale 10 GB load and
                             the cassandra rows dropped)
  + cost-20260829-rr         the no-tier controls the tier is read against
  + cost-20260828-4k         the 4 KiB workload-c cache sweep, i.e. h(c / D)
  + cost-20260827            the cassandra rows

Usage (from the repo root):

    python3 bench/scripts/plot/combine_disk_corpus.py /tmp/corpus.tsv
    python3 bench/scripts/plot/plot_cost_model.py /tmp/corpus.tsv \\
        bench/scripts/plot/prices.json --space bench/scripts/plot/space.json \\
        --table bench/results-disk-20260829-projection.tsv

Both tier sources are kept in one corpus: `plot_cost_model.py --tier-variant`
selects which of them feeds the tier coefficients, so a round-1 and a round-2
projection are two runs over the same file.

`--no-disk` drops the tier rows, which exercises the model's "no disk-cache
rows" fallback (every tier coefficient ASSUMED). It is not a copy of the
baseline line: a few medians (`put_per_write`, `k`) run over all OzoneDB cells
and move by about a dollar when the tier rows go. The baseline check is the
`ozone_hi_cache` column of the full-corpus projection against
`bench/results-cost-20260829-rr-projection.tsv`.
"""
import csv
import os
import sys

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
DEFAULT_DISK2 = "bench/results-disk2-20260829.tsv"
SOURCES = [
    ("bench/results-disk-20260829.tsv",
     lambda r, nd: not r["label"].startswith("cassandra") and r["record_cnt"] != "10000000"
     and not (nd and r.get("disk_capacity"))),
    ("bench/results-cost-20260829-rr.tsv",
     lambda r, nd: r["label"].startswith("ozonedb") and r["record_cnt"] != "10000000"),
    ("bench/results-cost-20260828-4k.tsv",
     lambda r, nd: r["label"].startswith("ozonedb") and r["workload"] == "c"
     and (r["cache_misses"] or "0").isdigit() and int(r["cache_misses"] or 0) >= 500),
    ("bench/results-cost-20260827.tsv",
     lambda r, nd: r["label"].startswith("cassandra")),
]
KEY = ("tag", "label", "workload", "writers", "trial", "record_cnt")


def main():
    argv = sys.argv[1:]
    disk2 = DEFAULT_DISK2
    if "--disk2" in argv:
        i = argv.index("--disk2")
        disk2 = argv[i + 1]
        del argv[i:i + 2]
    args = [a for a in argv if not a.startswith("-")]
    no_disk = "--no-disk" in argv
    out_path = args[0] if args else "-"
    sources = list(SOURCES)
    # First source wins on a duplicate key, and the round-2 tags never collide
    # with the round-1 ones, so the order only decides the column order.
    if os.path.exists(os.path.join(ROOT, disk2)):
        sources.insert(0, (disk2,
                           lambda r, nd: not r["label"].startswith("cassandra") and r["record_cnt"] != "10000000"
                           and not (nd and r.get("disk_capacity"))))
    else:
        print(f"  {disk2}: absent, skipped", file=sys.stderr)
    cols, out, seen = [], [], set()
    for rel, keep in sources:
        with open(os.path.join(ROOT, rel)) as f:
            rd = csv.DictReader(f, delimiter="\t")
            rows, fields = list(rd), rd.fieldnames
        for c in fields:
            if c not in cols:
                cols.append(c)
        n = 0
        for r in rows:
            key = tuple(r.get(k, "") for k in KEY)
            if not keep(r, no_disk) or key in seen:
                continue
            seen.add(key)
            out.append(r)
            n += 1
        print(f"  {rel}: {n} of {len(rows)} rows", file=sys.stderr)
    f = sys.stdout if out_path == "-" else open(out_path, "w", newline="")
    try:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in out:
            w.writerow({c: r.get(c, "") for c in cols})
    finally:
        if f is not sys.stdout:
            f.close()
    print(f"wrote {out_path} ({len(out)} rows, {len(cols)} columns)", file=sys.stderr)


if __name__ == "__main__":
    main()
