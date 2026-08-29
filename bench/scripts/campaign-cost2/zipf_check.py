#!/usr/bin/env python3
"""PLAN-cost-2 Task 0b functional check: the first cz cell must hit far above the
scrambled-zipfian cell at the same cache (0.14 at 100 MiB / 10 GB). Exit 1 stops
the chain."""
import csv
import os
import subprocess
import sys

results_dir = sys.argv[1]
home = os.environ["OZONEDB_HOME"]
tsv = os.path.join(home, "bench/results/local/cost2-chains/zipf_check.tsv")
subprocess.run([sys.executable, os.path.join(home, "bench/scripts/extract_cost_coefficients.py"),
                results_dir, "--window", "60", "--tsv", tsv], check=True)
rows = [r for r in csv.DictReader(open(tsv), delimiter="\t") if r["workload"] == "cz"]
if not rows:
    print("[zipf-check] no cz rows"); sys.exit(1)
ok = True
for r in rows:
    # `h` is the writer's cumulative RAM hit rate; h_total exists for tier cells only.
    h = float(r["h"]) if r["h"] else -1.0
    failed = float(r["failed"] or 0)
    print(f"[zipf-check] {r['label']} writers={r['writers']} h={h:.3f} h_steady={r['h_steady']} "
          f"failed={failed:.0f} steady_ops_s={r['steady_ops_s']}")
    ok = ok and h >= 0.4 and failed == 0
print("[zipf-check] " + ("PASS" if ok else "FAIL: h below 0.4 or failed ops; the zipfian_keyspace branch did not take"))
sys.exit(0 if ok else 1)
