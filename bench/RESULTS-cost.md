# Results: cost model with measured coefficients (campaign `cost-20260827`)

**Date:** 2026-08-27. **Plan:** `bench/PLAN-cost.md`. **Cluster:** CloudLab amd127
(Corfu + MinIO + Cassandra, 32 cores, 125 GB RAM, one 63 GB root disk) + 8 clients
(amd160, 126, 138, 135, 166, 146, 159, 133; 32 cores each). **Datasets:** 1,000,000 and
10,000,000 x 1 KB records. **Replication:** factor 1 on both systems on the cluster, RF=3
in the projection. **Trimming:** on (`--log-trim`, one checkpoint every 30 s) unless a cell
says `notrim`. **Metric:** steady-state ops/s, mean of the last 60 s per writer, summed.
**Failures:** 0 failed operations in every cell. **Cells:** 37 rows, all `rc=0`.

Artifacts, all under `bench/`:

- `results-cost-20260827.tsv` — one row per cell, 64 columns (`extract_cost_coefficients.py`).
- `results-cost-20260827-loads-1gb.tsv` — the two 1 GB load rows with their server sample
  (see "Method notes": the 10 GB load overwrote those sample files later).
- `results-cost-20260827-projection.tsv` — the projection, totals and per-line breakdown.
- `results-cost-20260827.{png,pdf}` — the figure. `scripts/plot/space.json` — the space
  coefficients. `scripts/plot/prices.json` — list prices, read 2026-08-26.

## Findings

1. **The block cache has no zipfian locality, so `h` tracks the capacity fraction.**
   At a cache of 52 % of the SSTable bytes the steady-state hit rate is 0.645, at 6.6 %
   it is 0.23, at 0.8 % it is 0.10, and at 0.1 % and below it is 0.01 or less. The cause is
   the 64 KB SSTable block (`src/db/sstable/table_builder.cpp:16`) under YCSB's hashed
   keys: the hot keys are spread over every block of the table. The plan's assumption
   `h >= 0.99` does not hold for this engine. The 10 GB cells confirm the shape: at the
   same cache-to-data ratio, `h` differs by at most 0.06 from the 1 GB point.
2. **The S3 GET line dominates OzoneDB at every scale.** At 10,000 ops/s with 50 % reads
   and a 16 GB client cache, GETs cost $1,954 per month at 1 GB (h = 0.645) and $5,045 at
   10 TB (h = 0.03). Storage, compaction PUTs and checkpoint PUTs together stay under
   $300 up to 10 TB. A row cache, or blocks of 4 KB, is the engine change that moves the
   curve. Without one, the "cheap object store" argument rests on GET pricing, not on
   storage pricing.
3. **OzoneDB clients need 35x the CPU per operation of Cassandra clients.** Workload a:
   2.16 ms per op (median, 8 writers) against 0.061 ms. Every OzoneDB process tails the
   whole shared log and runs compaction, so the per-op client CPU grows with the
   aggregate write rate: 0.85 ms at 2 writers, 1.09 ms at 4, 1.58 ms at 8. At 10,000 ops/s
   that is 8 `m6i.xlarge` clients ($1,120) against one `c6i.large` ($62).
4. **Crossover at 17.8 TB.** With a 4 GB cache per client, OzoneDB with trimming becomes
   cheaper than the cheaper Cassandra layout (RF=3 on EBS gp3, 8 TB per node) at 17.8 TB.
   Against Cassandra on NVMe (`i4i.2xlarge`) the crossover is 5.6 TB. Without trimming
   ("today") the crossovers move to 8.3 TB (NVMe) and 46 TB (EBS). Below 1 TB,
   Cassandra costs $1,565 per month on three nodes, and OzoneDB costs $3,700 to $6,600.
   The pre-measurement estimate (break-even at 1 to 4 TB, 4 to 6x cheaper at 10 TB)
   assumed `h >= 0.99` and a client CPU near Cassandra's. Neither held.
5. **The log tier is flat in D and trimming works as designed.** The trimmed Corfu log is
   187 MiB at 1 GB and 171 MiB at 10 GB. A checkpoint is 5 to 6 objects (22 objects over 4
   checkpoints at 1 GB, 222 over 36 at 10 GB), so checkpoint PUTs cost $2 per month at
   30 s intervals. A joining process restores 3 to 5 files (28 to 70 MB) with 5 to 7 GETs
   and replays 36k to 51k entries in 1.4 to 1.9 s. Without trimming, the log tier reaches
   $3,254 at 10 TB (11 TB of log) and $27,014 at 100 TB, which is the dashed "today" line.
6. **Consistency costs little on the OzoneDB side and a lot on the Cassandra side.**
   Linearizable OzoneDB reads run at the default rate on workload c (11,008 against
   10,830 ops/s) and 7 % lower on workload a. Cassandra serial (SERIAL reads + LWT writes)
   runs at 31 % (a) and 36 % (c) of quorum, with 2.7x the server CPU per op.

## Coefficient table

Symbols are those of `bench/PLAN-cost.md`. "Dataset bytes" is records x 1,024.

| Symbol | 1 GB | 10 GB | Source |
|---|--:|--:|---|
| `sC` Cassandra live bytes / dataset bytes, after `nodetool compact` | 1.066 (1,091,949,066 B) | 1.066 (10,918,624,402 B) | phase 1, phase 5 |
| `sC` peak before compact | 1.066 (1 SSTable after the drain) | 1.066 (7 SSTables, 10,920,018,751 B) | phase 1, phase 5 |
| `sO` SSTable objects / dataset bytes (checkpoints excluded) | 1.172 (1,145 MiB, 10 objects) | 1.168 (11.96 GB, 64 objects incl. checkpoints) | phase 1, phase 5 |
| Checkpoint bytes kept in the bucket | 44 MiB (2 kept) | 104 MB | phase 1, phase 5 |
| `L` Corfu log directory, trimming on | 187 MiB | 171 MiB | phase 1, phase 5 |
| `L0` slope, trimming off | 1.1 KB per put | (not repeated) | `PLAN-trimming.md` |
| `wa` MinIO bytes in / dataset bytes, during the load | 2.34 | 3.41 | phase 1, phase 5 |
| GETs per write (compaction reads during the load) | 0.015 | 0.032 | phase 1, phase 5 |
| PUTs per write (compaction + checkpoint objects) | 4.7e-5 | 5.1e-5 | phase 1, phase 5 |
| `k` objects per checkpoint | 5.5 (22 / 4) | 6.2 (222 / 36) | phase 1, phase 5 |
| Checkpoint live bytes, upload time | 35 MB, 287 ms | 55 MB, 454 ms | phase 1, phase 5 |
| `j` GETs per join, replay | 5 GETs, 35,641 entries in 1.4 s, 28 MB restored | 7 GETs, 50,758 entries in 1.8 s, 70 MB restored | phases 2, 5 |
| `g` GETs per cache miss, workload c | 1.00 (600 s), 0.95 (120 s) | 1.0 | phase 2 |
| `h` steady state, workload c, see the next table | 0.645 .. 0.002 | 0.069 (0.1 %), 0.0 (0.013 %) | phases 2, 5 |
| `cpuO` client CPU per op, default / linearizable | a: 1.58 / 1.39 ms; c: 0.34 / 0.39 ms | a: 2.0-2.3 ms; c: 1.1 ms | phases 2, 5 |
| `cpuC` client CPU per op, quorum / serial | a: 0.062 / 0.091 ms; c: 0.059 / 0.076 ms | a: 0.061 ms; c: 0.060 ms | phases 3, 5 |
| Server CPU per op, OzoneDB (Corfu + MinIO) | a: 1.2 ms; c: 0.56 ms (512 MB) .. 1.3 ms (128 KB) | a: 1.3-1.5 ms; c: 1.0-1.1 ms | phases 2, 5 |
| Server CPU per op, Cassandra quorum / serial | 0.12 / 0.33 ms (a), 0.12 / 0.26 ms (c) | 0.12 ms | phases 3, 5 |
| `ops_node_C` one Cassandra box, ops/s at 100 % busy | 275,000 (45,389 ops/s at 16.6 % busy) | | phase 4 |
| Load rate, 8 writers | OzoneDB 9,417 puts/s (0.86 ms client CPU per put); Cassandra 36,707 (0.12 ms) | OzoneDB 9,627 (0.70 ms); Cassandra 42,826 (0.063 ms) | phases 1, 5 |

`g` reads 0.95 in the 120 s cells because reads of keys that still sit in the log tail
miss the record cache without an S3 GET. The product `g x (1 - h)` equals the measured
GETs per op, which is what the model uses.

## Cache sweep: `h(c / D)` at 1 GB, workload c, 8 writers, 600 s cells

| Cache per client | c / D | `h` (steady, last 60 s) | GETs per op | ops/s | Client CPU per op | Server CPU per op |
|--:|--:|--:|--:|--:|--:|--:|
| 512 MB | 52.4 % | 0.645 | 0.355 | 10,575 | 0.34 ms | 0.56 ms |
| 64 MB | 6.55 % | 0.232 | 0.762 | 6,126 | 0.55 ms | 1.03 ms |
| 8 MB | 0.82 % | 0.101 | 0.892 | 5,286 | 0.61 ms | 1.20 ms |
| 1 MB | 0.10 % | 0.012 | 0.980 | 5,059 | 0.62 ms | 1.31 ms |
| 128 KB | 0.013 % | 0.002 | 0.991 | 4,982 | 0.63 ms | 1.35 ms |

The 120 s cells of the same sizes give 0.597, 0.198, 0.062, 0.0, 0.0: the last minute of
a 120 s run is still filling the larger caches. The 10 GB cells at 0.10 % and 0.013 %
give 0.069 and 0.0.

## Throughput and CPU, 8 writers, 120 s cells

| Cell | Workload | ops/s | GETs per op | PUTs per op | Client CPU per op | Server CPU per op | Server busy |
|---|---|--:|--:|--:|--:|--:|--:|
| OzoneDB 512 MB | a | 2,960 | 0.38 | 4.7e-5 | 1.58 ms | 1.21 ms | 11 % |
| OzoneDB 512 MB, trimming off | a | 3,026 | 0.36 | 3.3e-5 | 1.76 ms | 1.35 ms | 11 % |
| OzoneDB 512 MB, linearizable | a | 2,939 | 0.40 | 4.1e-5 | 1.39 ms | 1.11 ms | 9 % |
| OzoneDB 64 MB .. 128 KB | a | 2,539 .. 2,579 | 0.54 .. 0.65 | 7.3e-5 .. 9.1e-5 | 2.4 .. 3.0 ms | 2.1 .. 2.6 ms | 11 % |
| OzoneDB 512 MB | c | 10,830 | 0.38 | 4e-6 | 0.39 ms | 0.56 ms | 17 % |
| OzoneDB 512 MB, trimming off | c | 10,902 | 0.38 | 4e-6 | 0.39 ms | 0.55 ms | 17 % |
| OzoneDB 512 MB, linearizable | c | 11,008 | 0.38 | 4e-6 | 0.39 ms | 0.55 ms | 17 % |
| Cassandra quorum | a | 43,183 | | | 0.062 ms | 0.12 ms | 17 % |
| Cassandra quorum | c | 45,389 | | | 0.059 ms | 0.12 ms | 17 % |
| Cassandra serial | a | 13,289 | | | 0.091 ms | 0.33 ms | 14 % |
| Cassandra serial | c | 16,540 | | | 0.076 ms | 0.26 ms | 13 % |
| OzoneDB 10 MB, 10 GB | a | 2,301 | 0.70 | 6.3e-5 | 2.30 ms | 1.50 ms | 9 % |
| OzoneDB 10 MB, 10 GB | c | 4,619 | 0.89 | 1.2e-5 | 1.10 ms | 0.99 ms | 13 % |
| OzoneDB 1.25 MB, 10 GB | a | 2,299 | 0.70 | 5.5e-5 | 2.03 ms | 1.32 ms | 9 % |
| OzoneDB 1.25 MB, 10 GB | c | 4,331 | 0.98 | 1.2e-5 | 1.14 ms | 1.10 ms | 14 % |
| Cassandra quorum, 10 GB | a | 43,669 | | | 0.061 ms | 0.12 ms | 16 % |
| Cassandra quorum, 10 GB | c | 44,213 | | | 0.060 ms | 0.11 ms | 16 % |

The trimmer adds 1.4e-5 PUTs per op on workload a (4.7e-5 against 3.3e-5) and changes
throughput by 2 % or less.

## Server scaling, workload a, 512 MB cache

| Clients | OzoneDB ops/s | per writer | OzoneDB client CPU per op | OzoneDB server busy | Cassandra quorum ops/s | Cassandra server busy |
|--:|--:|--:|--:|--:|--:|--:|
| 2 | 1,370 | 685 | 0.85 ms | 3.4 % | 10,480 | 4.4 % |
| 4 | 2,003 | 501 | 1.09 ms | 5.4 % | 22,097 | 8.2 % |
| 8 | 2,960 | 370 | 1.58 ms | 11.1 % | 43,183 | 17.0 % |

Cassandra scales linearly and is client-bound. OzoneDB scales sublinearly while its server
stays below 12 % busy: the limit is the client path, not the server.

## Projection: 10,000 ops/s, 50 % reads, RF=3, list prices of 2026-08-26 (USD per month)

| Cost line | 1 GB | 1 TB | 10 TB |
|---|--:|--:|--:|
| Cassandra nodes, NVMe `i4i.2xlarge` x N | 1,503 (N=3) | 1,503 (N=3) | 12,525 (N=25) |
| Cassandra nodes, EBS `m6i.xlarge` + 8 TB gp3 x N | 2,340 (N=3) | 2,340 (N=3) | 4,680 (N=6) |
| Cassandra clients (`c6i.large`) | 62 (1) | 62 (1) | 62 (1) |
| **Cassandra total, NVMe / EBS** | **1,565 / 2,402** | **1,565 / 2,402** | **12,587 / 4,742** |
| OzoneDB log tier (sequencer `c6i.large` + 3 log units `r6i.xlarge`, `L`) | 614 | 614 | 614 |
| S3 storage (`sO x D` + checkpoints) | 0 | 27 | 270 |
| S3 GETs at `h(16 GB / D)` | 1,954 (h 0.645) | 4,481 (h 0.143) | 5,045 (h 0.031) |
| S3 GETs at `h(4 GB / D)` | 1,954 (h 0.645) | 4,848 (h 0.071) | 5,165 (h 0.008) |
| S3 PUTs, compaction | 2 | 2 | 2 |
| S3 PUTs, checkpoints at `T_trim` = 30 s | 2 | 2 | 2 |
| OzoneDB clients (`m6i.xlarge`) | 1,120 (8) | 1,120 (8) | 1,120 (8) |
| **OzoneDB total, 16 GB cache** | **3,693** | **6,247** | **7,053** |
| **OzoneDB total, 4 GB cache** | **3,693** | **6,613** | **7,173** |
| OzoneDB "today" (no trimming: log tier holds `L0` x total writes) | 3,691 | 6,509 | 9,691 |

The model holds `h` flat beyond the largest measured ratio (52 %), so the 16 GB and
4 GB lines coincide below 30 GB. That is conservative: a cache larger than the table
would hit near 1.0. The `ozone_today` line assumes the same 10,000 ops/s ran for one
month with no trimming. `results-cost-20260827-projection.tsv` holds every line for
1 GB to 100 TB.

## Figure

`results-cost-20260827.png`: log-log, dataset size 1 GB to 100 TB against USD per month.
Cassandra RF=3 on NVMe and on EBS, OzoneDB with trimming as a band (16 GB to 4 GB cache
per client), OzoneDB without trimming dashed, measured cells at 1 GB and 10 GB, the
crossover at 17.8 TB. Stated assumptions:

1. List prices, us-east-1, on-demand, 730 h per month, read 2026-08-26.
2. The Cassandra node count is the larger of the 3-node floor, the storage bound at 70 %
   fill, and the ops bound from `ops_node_C`. Storage binds above 1 TB.
3. The hot set is scale-free: `h` depends on `c / D` only. Checked at 1 GB and 10 GB
   (difference at most 0.06 at equal ratio).
4. The log tier is flat in D with trimming. Checked at 1 GB (187 MiB) and 10 GB (171 MiB).

## Method notes and caveats

- **Steady-state `h`.** The sampler polls the MinIO request counters every 10 s. `h_steady`
  is `1 - GETs per op` over the last 60 s of activity, from the GET rate and the writers'
  status lines. The cumulative cache counters include the cold start and the record cache,
  so they are reported (`h`) but not used.
- **The 1 GB load samples were overwritten.** Both loads wrote their server sample under
  the same cell name in the same results root. The 10 GB load replaced the 1 GB files. The
  1 GB rows in `results-cost-20260827.tsv` therefore carry `no_server_sample`. The values
  above come from `results-cost-20260827-loads-1gb.tsv`, the extraction made from the
  original files before the 10 GB load ran. The load wrappers now put `_rc<records>` in
  the cell name, and the extractor matches on it.
- **`get_per_write` in the model is the 10 GB value (0.032).** Compaction reads per write
  grow with the depth of the tree (0.015 at 1 GB). The projection range is above 10 GB.
- **Client CPU is the workload a median at 8 writers.** It is a function of the writer
  count (see server scaling), so the client line is an estimate for the 8-writer point,
  not a constant.
- **`ozone_today` uses the trimmed `k` and `L0` from `PLAN-trimming.md`** (1.1 KB of log
  per put); it was not re-measured here.
- **The h sweep is one trial per point.** Two 512 MB cells (120 s, trimming on and off)
  agree within 1 % on `h` and throughput, so trial noise is small against the effects
  reported.
- **Sequencing on one disk.** The server holds both systems on a 63 GB root disk, so
  phase 5 loaded OzoneDB, ran its cells, freed its 10 GB state, then loaded Cassandra.
  Bucket and log directory were snapshotted and restored together for every cell.
- **No cpufreq interface** on the EPYC 7302P nodes: fairness rule 3 of the plan does not
  apply. SMT was on.
- **Three orchestration bugs found and fixed during the run**, all in the bench scripts:
  a `pkill` pattern that killed the ssh shell, `cassandra_ctl.sh compact` parsing the
  Cassandra 5 `compactionstats` table format, and idle ssh sessions dropped after 600 s
  (`ServerAliveInterval` added). None affected a reported number.

## Reproduce

```bash
export TAG=cost-20260827
bash bench/scripts/local/load_corfu_dataset.sh --writers 8 --log-trim            # phase 1
bash bench/scripts/local/load_multinode_cassandra.sh --writers 8
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 536870912 \
    --workloads "a c" --writers-list 1 --trial 1 --duration 120 --run-tag $TAG       # phase 2, per size
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 536870912 \
    --workloads c --writers-list 1 --trial 1 --duration 600 --run-tag $TAG-long      # h sweep, per size
bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency quorum \
    --workloads "a c" --writers-list 1 --trial 1 --duration 120 --run-tag $TAG       # phase 3
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/$TAG bench/results/local/$TAG-notrim \
    bench/results/local/$TAG-long bench/results/local/$TAG-10g bench/results/local \
    --tsv bench/results-cost-$TAG.tsv
python3 bench/scripts/plot/plot_cost_model.py bench/results-cost-$TAG.tsv bench/scripts/plot/prices.json \
    --space bench/scripts/plot/space.json --table bench/results-cost-$TAG-projection.tsv
```
