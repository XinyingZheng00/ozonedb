# Results: strict-consistency throughput frontier at 100 GB

**Date:** 2026-08-30 to 2026-08-31. **Plan:** `bench/PLAN-strict-100g.md`.
**Cluster:** CloudLab `amd197` (Corfu, MinIO and Cassandra, never at the same time)
plus 8 client nodes. **Dataset:** 100,000,000 x 1 KB records, about 103 GB.
**Replication:** factor 1 on both systems. **Trials:** 3. **Cells:** 150.
**Cell duration:** 300 s on both systems. **Metric:** steady-state ops/sec, the
mean of `current ops/sec` over the last 120 s of each writer, summed across
writers (`bench/scripts/extract_steady_throughput.py --window 120`).
**Run tag:** `strict100-20260830`.

**Engine labels**, exactly as the result files carry them:

- `cassandra-serial` — SERIAL reads, QUORUM writes with `IF [NOT] EXISTS` Paxos.
- `ozonedb-corfu-linearizable-native-lru16g-dc50g-ch64k-adm` — strict reads, the
  native C++ Corfu client, a 16 GiB block cache and a 50 GiB disk-cache tier in
  chunk mode with 64 KiB entries and TinyLFU admission.

**Failures: 0 failed operations in all 150 cells**, on both systems, at every
writer count. The plan named Paxos timeouts at 32 writers as a risk. That risk
did not appear.

---

## Read this before the numbers: two cache asymmetries

The two systems do not meet the same cache conditions. Both asymmetries favour
Cassandra, so every OzoneKV result below is a lower bound.

**1. OzoneKV runs with cold caches in every cell.** Each cell is a fresh YCSB
process, so the block cache starts at zero bytes, and
`load_local_ycsb_multiproc.py:543` deletes each writer's tier directory before
the cell. Chunk mode also starts cold by design. The measured end state of a
300 s cell, workload c at 8 writers, writer 0, is stable across all three trials:

| Cache | Budget | Filled at the end of a cell | Fraction | Evictions |
|---|--:|--:|--:|--:|
| LRU block cache | 17.18 GB | 651 MB | **3.8 %** | 0 |
| Disk-cache tier | 53.69 GB | 11.1 GB | **20.7 %** | 0 |

The tier took 170,080 chunk fills and reached a 3.6 % hit rate. The block cache
reached 38.2 %. Zero evictions in either tier proves that the budget never
bound the run. The fill rate bound it. A 32 GiB tier and a 50 GiB tier give the
same 300 s number.

**2. Cassandra runs with a warm page cache.** `restore-load` relinks the same
inodes before every cell, so pages cached in one cell stay valid in the next.
The server holds 125 GB of RAM, and `free -g` reported 85 GB in `buff/cache`
against a 103 GB dataset. Cassandra therefore serves most of the corpus from
memory.

**Read the table below as a warm server against a cold-joining client.** That is
the elastic regime this sweep set out to measure: an OzoneKV writer joins by
opening the log and serves reads at once. It is not a warm-cache OzoneKV number.
The warm point remains a follow-up.

---

## Steady-state throughput (ops/sec, mean of 3 trials)

| Workload | Writers | Cassandra serial | OzoneKV linearizable | Ratio |
|---|--:|--:|--:|--:|
| a (50 r / 50 u) | 2 | 3,145 | 505 | 6.23 |
| a | 4 | 6,097 | 1,693 | 3.60 |
| a | 8 | 10,663 | 2,854 | 3.74 |
| a | 16 | 17,108 | 5,118 | 3.34 |
| a | 32 | 24,798 | 7,660 | 3.24 |
| b (95 r / 5 u) | 2 | 3,547 | 1,035 | 3.43 |
| b | 4 | 7,254 | 2,790 | 2.60 |
| b | 8 | 12,851 | 4,572 | 2.81 |
| b | 16 | 21,317 | 7,458 | 2.86 |
| b | 32 | 32,651 | 11,813 | 2.76 |
| c (100 r) | 2 | 3,770 | 1,268 | 2.97 |
| c | 4 | 7,574 | 3,486 | 2.17 |
| c | 8 | 13,588 | 6,703 | 2.03 |
| c | 16 | 22,936 | 12,808 | 1.79 |
| c | 32 | 35,515 | 21,091 | **1.68** |
| f (read-modify-write) | 2 | 2,446 | 782 | 3.13 |
| f | 4 | 4,913 | 1,314 | 3.74 |
| f | 8 | 8,310 | 2,095 | 3.97 |
| f | 16 | 13,344 | 3,568 | 3.74 |
| f | 32 | 19,527 | 6,649 | 2.94 |
| d (95 r-latest / 5 ins) | 2 | 4,809 | 2,355 | 2.04 |
| d | 4 | 10,036 | 4,150 | 2.42 |
| d | 8 | 19,497 | 6,424 | 3.03 |
| d | 16 | 35,550 | 9,945 | 3.57 |
| d | 32 | 54,120 | 15,555 | 3.48 |

Figure: `bench/strict-frontier-100g.png`, five panels, one per workload.

---

## Findings

1. **Cassandra serial is faster at every one of the 25 points.** One 32-core
   server runs SERIAL reads and Paxos writes for 32 concurrent clients and never
   saturates. This repeats the 1 GB result of `bench/RESULTS-strict-frontier.md`
   at a dataset 100 times larger.

2. **The read-only gap closes as writers grow, and it is the one curve that
   converges.** On workload c the ratio falls from 2.97 at 2 writers to 1.68 at
   32. Growth from 4 to 32 writers is 6.05x for OzoneKV against 4.69x for
   Cassandra. OzoneKV reads scale across 8 client machines. Cassandra reads
   contend for one server. The lines head toward a crossover above 32 writers.
   This sweep did not reach it.

3. **Insert-heavy work diverges instead.** On workload d the ratio *widens* from
   2.04 to 3.48 as writers grow, and Cassandra grows 5.39x from 4 to 32 writers
   against OzoneKV's 3.75x. Every OzoneKV write takes a token from one Corfu
   sequencer, so write throughput is globally serialized. Workload d inserts new
   keys and is the most write-bound of the five.

4. **Read-modify-write is the worst case for OzoneKV, and it is not a scaling
   problem.** Workload f holds a ratio near 3.7 to 4.0 across the middle of the
   range. An `f` operation is a read and a write, so it pays the strict read
   fence and the sequencer token together.

5. **The strict read path is not the bottleneck at 100 GB. The object store
   is.** Workload c at 8 writers reached 6,703 ops/s against the 6,330 that
   `bench/RESULTS-cost.md` measured for the *default* read mode with a 100 MiB
   cache and no tier. The fence costs little next to a MinIO GET.

6. **The tier helps, but a 300 s cell cannot pay for it.** Workload c at 8
   writers landed at 6,703, between the 6,330 of a no-tier cell and the 10,088
   of a warm 50 GiB tier. The tier reached 20.7 % of its budget by the end of a
   cell. The warm value needs a longer cell.

---

## Reproducibility

Three trials give a tight result. The spread is the full range over the three
trials, divided by their mean.

| Statistic | Value |
|---|--:|
| Median spread, all 50 cells | 3.3 % |
| Median spread, excluding the 2-writer point | 2.2 % |
| Cells above the plan's 15 % threshold | 6 of 50 |

**All six wide cells are 2-writer cells.** Above 2 writers the two systems repeat
to within a few percent. OzoneKV at 32 writers on workload d gave 15,574,
15,574 and 15,516 ops/s in the three trials.

The six wide cells move in opposite directions, and both directions have the
same cause. OzoneKV rises across trials (workload c: 956, 1,252, 1,597) because
the MinIO page cache on the server warms. Cassandra falls (workload c: 4,166,
3,653, 3,492) because the OzoneKV half of each trial evicts Cassandra's pages
from that same page cache. At 2 writers a cell uses only two client hosts, so
this server-side state dominates. **Treat the 2-writer column as indicative.**

---

## Comparison against the 1 GB run

`bench/RESULTS-strict-frontier.md` ran the same matrix shape on 1 GB, 1 trial,
on a different cluster. Ratios at 4 writers:

| Workload | Ratio at 1 GB | Ratio at 100 GB | Change |
|---|--:|--:|---|
| a | 3.97 | 3.60 | slightly better for OzoneKV |
| b | 2.30 | 2.60 | worse |
| c | 1.49 | 2.17 | worse |
| d | 1.86 | 2.42 | worse |
| f | 3.40 | 3.74 | worse |

The read-only case lost the most. At 1 GB a small client cache holds a useful
share of the dataset. At 100 GB it holds 0.6 %, so nearly every read reaches the
object store. This is the same `h ~ c / D` relation that
`bench/RESULTS-cost.md` measured.

---

## Method notes and caveats

- **Steady state, not whole-run average.** YCSB's `Throughput(ops/sec)` divides
  by the orchestrator wall time, which includes SSH, JVM start and the OzoneKV
  tailer catch-up. This report never uses it. `plot_strict_frontier.py` reads
  the extractor TSV for the same reason.
- **The extractor dropped a dead tail during this campaign** (commit
  `99ed79ae`). YCSB prints one status line per second while it closes the
  client, so every run ends with three or four samples that report 0
  operations, and those were averaged into the window. The fix raises both
  systems by about 3 % at a 120 s window. Numbers in `RESULTS-cost.md` and
  `RESULTS-strict-frontier.md` predate it.
- **Cassandra snapshots became hard links during this campaign** (commit
  `ba412fae`). A `cp -a` of the 103 GB data directory does not fit in the 98 GB
  free. Cassandra never rewrites an SSTable in place, so `cp -al` is safe, and
  `nodetool snapshot` does the same. This is what leaves the page cache warm.
- **Cells are identical in length across trials.** Every YCSB run reported
  308 s of orchestrator wall time in trial 1 and in trial 3. The campaign took
  24 h rather than the planned 20 h 30 min, because per-cell restart overhead
  grew from about 4 min to about 9 min. `nodetool drain` must flush a
  write-heavy cell before the next restore. This changed the schedule and not
  the measurement.
- **Not measured here:** per-core-normalized throughput, elasticity as a
  measured join time, writer-failure behaviour, staleness of the relaxed modes,
  and any replicated configuration.

---

## Reproduce

```bash
# Both 100 GB corpora already exist on amd197 and need no reload.
# One trial per invocation. Cassandra runs first, then OzoneDB.
nohup bash bench/scripts/campaign-strict100/chain_trial.sh 1 \
  > bench/results/local/strict100-chains/trial1.log 2>&1 < /dev/null & disown
# Repeat for trials 2 and 3. Each chain ends with CHAIN-DONE.

python3 bench/scripts/extract_steady_throughput.py \
  bench/results/local/strict100-20260830 --window 120 \
  --tsv bench/results-strict100-20260830.tsv
uv run bench/scripts/plot/plot_strict_frontier.py \
  bench/results-strict100-20260830.tsv --output bench/strict-frontier-100g.png
```

## Next

1. **The warm-cache point.** Two long cells at 8 writers let the tier fill and
   give the steady value that the 50 GiB budget describes. It costs about
   1 h 45 min and needs no new corpus. See `bench/PLAN-strict-100g.md`,
   "Follow-ups".
2. **Push past 32 writers on workload c.** The ratio fell to 1.68 and still
   trends down. The crossover is the strongest claim available to OzoneKV, and
   it sits just outside this sweep.
3. **The relaxed points of the frontier**, Cassandra `--consistency quorum` and
   OzoneKV without `--linearizable`, to complete a two-point-per-system figure.
