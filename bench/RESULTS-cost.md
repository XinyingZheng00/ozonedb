# Results: cost model with measured coefficients (campaign `cost-20260827`)

**Date:** 2026-08-27. **Plan:** `bench/PLAN-cost.md`. **Cluster:** CloudLab amd127
(Corfu + MinIO + Cassandra, 32 cores, 125 GB RAM, one 63 GB root disk) + 8 clients
(amd160, 126, 138, 135, 166, 146, 159, 133; 32 cores each). **Datasets:** 1,000,000 and
10,000,000 x 1 KB records. **Replication:** factor 1 on both systems on the cluster, RF=3
in the projection. **Trimming:** on (`--log-trim`, one checkpoint every 30 s) unless a cell
says `notrim`. **Metric:** steady-state ops/s, mean of the last 60 s per writer, summed.
**Failures:** 0 failed operations in every cell. **Cells:** 37 rows, all `rc=0`.
**Correction (2026-08-27):** every OzoneDB workload-a cell lost 1 to 4 of its 8 writers
to a native crash that the runner did not report; the workload-a sums are survivor sums.
Found in the 4 KiB re-run, fixed in `96b9265d`. See "A crash in every workload-a cell".

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
   storage pricing. The 4 KiB re-run below (`cost-20260828-4k`) measures that change:
   `h` 1.05x to 19x higher, but compaction GETs 13.6x higher until compaction reads
   ranges instead of blocks.
3. **OzoneDB clients need 35x the CPU per operation of Cassandra clients.** Workload a:
   2.16 ms per op (median, 8 writers, survivor writers of the crash below) against
   0.061 ms; 1.37 ms on the 4 KiB build with the crash fix (600 s, 8/8 writers), 1.16 ms
   with compaction range reads (`2dc2ed24`, 600 s cells only). Every OzoneDB process tails the
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
throughput by 2 % or less. **OzoneDB workload-a rows are sums over the writers that
survived the crash described in the 4 KiB section:** 7/8 at 512 MB, 5/8 at 64 MB, 8 MB
and 1 MB, 4/8 at 128 KB, 6/8 with trimming off, 8/8 linearizable, 7/8 and 8/8 at 10 GB.
Workload c and Cassandra rows are complete (8/8).

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

## Re-run with 4 KiB blocks (campaign `cost-20260828-4k`, engine commit `dd0f195e`)

**Date:** 2026-08-27. **Change:** `BLOCK_SIZE` 65536 to 4096 in
`src/db/sstable/table_builder.cpp:16`, the SlateDB and LevelDB default. Everything else is
the same as `cost-20260827`: same cluster, same 8 clients, `build.sh` on every client, a
fresh 1 GB load with trimming, the 600 s workload-c sweep at the five cache sizes, then
a/c cells at 512 MB and 8 MB (120 s and 600 s). The 600 s workload-a cells ran on the
build with the crash fix `96b9265d` (see "A crash in every workload-a cell"). The
Cassandra rows are those of `cost-20260827`. All cells `rc=0`; the two 600 s workload-a
cells count 1 and 6 failed reads in 1.6 M operations (a key in a file removed under the
reader, the documented transient miss of the non-linearizable path).

Artifacts, under `bench/`: `results-cost-20260828-4k.tsv` (15 rows),
`results-cost-20260828-4k-projection.tsv` (compaction GETs as measured),
`results-cost-20260828-4k-projection-rangeread.tsv` (compaction GETs set to zero, see
below), `results-cost-20260828-4k.png`.

### Cache sweep, workload c, 8 writers, 600 s cells: 64 KiB against 4 KiB

| Cache per client | c / D | `h` 64 KiB | `h` 4 KiB | Gain | Simulated gain | ops/s 64 KiB | ops/s 4 KiB | Client CPU per op 64 / 4 | Server CPU per op 64 / 4 |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 512 MB | 52.4 % | 0.645 | 0.675 | 1.05x | 1.12x | 10,575 | 14,350 | 0.34 / 0.25 ms | 0.56 / 0.42 ms |
| 64 MB | 6.55 % | 0.232 | 0.341 | 1.47x | 1.47x | 6,126 | 8,086 | 0.55 / 0.37 ms | 1.03 / 0.74 ms |
| 8 MB | 0.82 % | 0.101 | 0.215 | 2.13x | 1.88x | 5,286 | 7,057 | 0.61 / 0.42 ms | 1.20 / 0.87 ms |
| 1 MB | 0.10 % | 0.012 | 0.141 | 11.3x | 3.3x | 5,059 | 6,419 | 0.62 / 0.46 ms | 1.31 / 0.96 ms |
| 128 KB | 0.013 % | 0.002 | 0.032 | 19x | 9x | 4,982 | 5,858 | 0.63 / 0.50 ms | 1.35 / 1.06 ms |

"Simulated gain" is the ratio of LRU hit rates for 4-record and 64-record blocks in a
zipfian (0.99) simulation over 1M hashed keys, at the same cache ratio. The middle of the
curve matches the simulation. The small caches gain more than simulated because the
measured 64 KiB curve was below the simulation there (a 1 MB cache holds 16 blocks of
64 KiB and 256 blocks of 4 KiB). Read throughput rose 18 to 36 % at every size, and the
client CPU per read fell 21 to 27 %: a miss now parses 4 records instead of 64. Peak RSS
per client rose 10 %. `g` stays 0.993 GETs per miss.

### Load, compaction and the 4 KiB penalty

| | 64 KiB | 4 KiB |
|---|--:|--:|
| Load, 8 writers, 1M x 1 KB | 9,417 puts/s, 120 s | 8,054 puts/s, 147 s |
| GETs per put (compaction input reads) | 0.015 | **0.205** |
| PUTs per put | 4.7e-5 | 4.4e-5 |
| Client CPU per put | 0.86 ms | 0.91 ms |
| Server CPU per put | 0.32 ms | 0.56 ms |
| SSTables after the load (bucket) | 203 + 942 MiB | 204 + 953 MiB |
| Trimmed log, checkpoints, objects | 187 MiB, 4, 22 | 188 MiB, 4, 20 |

Compaction reads every input file through `Table::getAll`
(`src/db/sstable/table_reader.cpp`), which calls `readBlock` once per block, and
`readBlock` (`src/db/sstable/block_handler.cpp:88`) issues one `storage->read` per call.
Sixteen times more blocks means 13.6x more GETs per put. The index grew 16x in entries
(about 1 % of the bucket) and the table-open cost did not change (5 GETs per join).

### Workload a and 120 s cells

| Cell | Workload | Duration | ops/s 64 KiB | ops/s 4 KiB | GETs per op 64 / 4 | Client CPU per op 64 / 4 | Server CPU per op 64 / 4 |
|---|---|--:|--:|--:|--:|--:|--:|
| 512 MB | a | 120 s | 2,960 | 2,605 | 0.38 / 0.76 | 1.58 / 1.53 ms | 1.21 / 1.36 ms |
| 8 MB | a | 120 s | 2,543 | 2,528 | 0.64 / 0.80 | 2.47 / 1.54 ms | 2.14 / 1.40 ms |
| 512 MB | c | 120 s | 10,830 | 10,218 | 0.38 / 0.50 | 0.39 / 0.41 ms | 0.56 / 0.62 ms |
| 8 MB | c | 120 s | 5,398 | 7,003 | 0.89 / 0.76 | 0.71 / 0.52 ms | 1.17 / 0.85 ms |
| 512 MB | a | 600 s, fix `96b9265d` | none | 2,708 (8/8) | - / 0.65 | - / 1.17 ms | - / 1.26 ms |
| 8 MB | a | 600 s, fix `96b9265d` | none | 2,566 (8/8) | - / 0.79 | - / 1.21 ms | - / 1.37 ms |

Two caveats apply to 120 s cells with 4 KiB blocks. First, a 4 KiB miss brings 4 records
into the cache, a 64 KiB miss brings 64, so a 512 MB cache fills 16x slower: the 512 MB
120 s cells are still warm-up (`h` 0.475 against 0.675 at 600 s). Second, under workload a
the last-60 s GET rate includes the compaction reads, so "GETs per op" on workload a is
reads plus compaction, not `g x (1 - h)`. The client CPU per op on workload a, the number
the projection uses, is 1.37 ms, the median of the four 4 KiB workload-a cells (2.16 ms in
the 64 KiB cells, whose survivors ran with fewer peers). Per writer, the 600 s cells give
339 and 321 ops/s at 8/8 writers; the 64 KiB 120 s survivors averaged 329 (512 MB, 7/8)
and 291 (8 MB, 5/8) over their runs, so the block size does not change workload-a
throughput by more than the crash noise. Peak RSS per client reached 3.9 GB at 512 MB
(2.8 GB in the 64 KiB 120 s cell): retired Tables of removed files are kept 30 s.

### Projection with the 4 KiB curve: 10,000 ops/s, 50 % reads, RF=3 (USD per month)

| | 1 GB | 1 TB | 10 TB | Crossover vs Cassandra on EBS |
|---|--:|--:|--:|--:|
| Cassandra, NVMe / EBS | 1,565 / 2,402 | 1,565 / 2,402 | 12,587 / 4,742 | |
| OzoneDB 64 KiB, 16 GB cache (`cost-20260827`) | 3,693 | 6,247 | 7,053 | 17.8 TB |
| OzoneDB 4 KiB, compaction GETs as measured (0.205 per put) | 4,092 | 6,308 | 7,070 | 17.8 TB |
| OzoneDB 4 KiB, compaction GETs at the 64 KiB value (0.032) | 3,182 | 5,399 | 6,160 | 14.7 TB |
| OzoneDB 4 KiB, compaction range reads (0 per put) | 3,014 | 5,231 | 5,993 | 14.7 TB |

Lines at 10 TB, 16 GB cache: `h` 0.031 to 0.157; S3 GETs $5,045 (64 KiB) to $5,482
(4 KiB, measured) to $4,404 (4 KiB, range reads); clients 8 to 5 ($1,120 to $700) from the
lower client CPU per op. Log tier ($614), storage ($270) and PUTs ($5) do not change. The
4 KiB block curve is worth about $1,060 per month at 10 TB ($640 in GETs, $420 in
clients), and it is realised only when compaction stops reading one block per GET: as
measured, compaction GETs ($1,078 at 10 TB) cancel the read saving.

### A crash in every workload-a cell, found and fixed

The first 600 s workload-a cell on the 4 KiB build finished with 1 of 8 writers. The
other seven died at 130 s with `SIGSEGV` at address 0 in
`ozonedb::LRUCache::readDataBlocks` (JVM `hs_err_pid*.log` on every client), right after
an L2 compaction commit, several hosts in the same second. `/usr/bin/time` and the
multiproc runner return 0 after a JVM abort, so the orchestrator saw `rc=0` on 8/8 hosts.
The crash reports on the clients date back to the 64 KiB campaign: every OzoneDB
workload-a cell of `cost-20260827` (08:10 to 08:52 UTC), the 8-host scaling cell (18:46)
and the 10 GB 10 MB cell lost writers, all with the same frame. Workload c never
compacts and never crashed; the linearizable cell did not crash either.

| Cell, workload a | 64 KiB writers finished | 4 KiB writers finished |
|---|--:|--:|
| 512 MB, 120 s | 7/8 | 8/8 |
| 64 MB / 8 MB / 1 MB / 128 KB, 120 s | 5/8, 5/8, 5/8, 4/8 | - / 8/8 / - / - |
| 512 MB, trimming off / linearizable, 120 s | 6/8, 8/8 | - |
| 512 MB, 2 / 4 / 8 hosts (scaling), 120 s | 2/2, 4/4, 7/8 | - |
| 10 GB, 10 MB / 1.25 MB, 120 s | 7/8, 8/8 | - |
| 512 MB / 8 MB, 600 s, before the fix | - | 1/8, 1/8 |
| 512 MB / 8 MB, 600 s, with the fix | - | 8/8, 8/8 |

Cause (`src/db/cache.cpp`): `invalidateLogFile`, run when the tailer applies a peer
compaction's REMOVE, deleted the file's `Table*` and erased its cache entry while a reader
could be inside `Table::get` on that object; `getSSTable` hands out raw pointers and the
reader holds no lock across the block read. The reader's entry lookup in `readDataBlocks`
then failed, `table` became null, and `blockReader` dereferenced it. Fix (`96b9265d`):
removed Tables are retired for 30 s instead of deleted, `readDataBlocks` falls back to the
caller's Table and treats a missing Table or a failed block read as a failed fetch
through the existing cleanup path, and `LRUCache::get` uses lookups only. Correction
(2026-08-28): `invalidateLogFile` runs only for log-prefix names (`LogHandler::onRemoteAppend`
rejects other files), so it cannot have deleted an SSTable's `Table*`; the null `table`
came from a cache entry without a `Table*`, and the `caller_table` fallback with the null
guards is the part of the fix that matters. The retired-Table deque is harmless. With the fix the
600 s cells finish 8/8, the fallback path logs 8 and 27 removed-file misses per cell, and
YCSB counts 1 and 6 failed reads.

What it changes in the numbers above: the workload-a sums of `cost-20260827` are survivor
sums, and the survivors ran with fewer peers (per-writer throughput rises as writers drop:
609 / 447 / 329 ops/s at 2 / 4 / 8 hosts). The client CPU per op of finding 3 (2.16 ms) is
a survivor median. The 4 KiB 120 s cells and the 600 s cells with the fix are complete.
The workload-c sweep, the loads, the Cassandra cells and every coefficient except the
workload-a client CPU are unaffected. The 64 KiB workload-a cells were not re-run with the
fix: that needs a 64 KiB rebuild and a fresh load (about 40 minutes), and the block size is
a compile-time constant; a config key for it would let both sizes run from one build.

### Conclusions of the re-run

1. **4 KiB blocks are a net win on the read path.** `h` gains 1.5 to 2x at 1 to 7 % cache
   ratio and 11 to 19x below 0.1 %, read throughput gains 18 to 36 %, client CPU per read
   falls a quarter. The zipfian simulation predicted the shape.
2. **Compaction must read ranges, not blocks.** `Table::getAll` reads the whole file, so it
   must fetch the data section in one range read (or in a few MiB-sized reads) and slice
   blocks from memory, as SlateDB does with `blocks_to_fetch: 256` on its compaction
   iterator. Compaction already holds the full table in memory, so the buffer adds no new
   peak. Expected: GETs per put near zero, load puts/s back to the 64 KiB rate, projection
   $5,993 at 10 TB, crossover 14.7 TB. **Done and measured** (`2dc2ed24`, section
   "Compaction range reads"): 0.00015 GETs per put, 9,634 puts/s, $5,992 at 10 TB.
3. **`h` still tracks the capacity fraction.** With 4 KiB blocks the model reaches 0.157
   at a 16 GB cache on 10 TB. Cache bytes (a local-disk tier) and key-range read affinity
   remain the levers at scale; the block size alone does not reach `h >= 0.99`.
4. **Runner exit codes hide native crashes.** A JVM abort leaves `rc=0` at every layer.
   The extractor must count writers with an `[OVERALL]` block per cell and flag cells
   with fewer than the configured number (not yet done).

## Compaction range reads (campaign `cost-20260829-rr`, engine commits `2dc2ed24` + `2380ddf0`)

The change planned in `PLAN-compaction-range-read.md`: `Table::getAll`, which compaction
uses to read an SSTable input, now reads the data section in block-aligned ranged reads of
at most `compaction_read_bytes` (default 64 MiB, one read per input) and slices the blocks
in memory, instead of one `storage->read` per 4 KiB block. It also validates the index and
returns `Status`; compaction skips an input that a peer removed and abandons the task when
an input that still exists cannot be read. The point-read path is unchanged. Same cluster
and settings as the 4 KiB re-run: 4 KiB blocks, trimming on, a fresh 1 GB load, then 600 s
cells with 8 writers at 512 MB and 8 MB (workload a) and one 600 s workload-c control at
512 MB. All cells `rc=0`, 8/8 writers finished, 0 crashes. Rows in
`results-cost-20260829-rr.tsv`; the projection in `results-cost-20260829-rr-projection.tsv`
takes `h` from the 4 KiB sweep (the read path did not change; the control cell checks it).

### Load and compaction

| | 64 KiB | 4 KiB | 4 KiB + range reads |
|---|--:|--:|--:|
| Load, 8 writers, 1M x 1 KB | 9,417 puts/s | 8,054 puts/s | **9,634 puts/s** |
| S3 GETs during the load | 15,003 | 204,858 | **100** |
| GETs per put (`get_per_write`) | 0.015 | 0.205 | **0.00015** |
| Bytes read back from MinIO | 988 MB | 998 MB | 1,000 MB |
| PUTs per put | 4.7e-5 | 4.4e-5 | 4.7e-5 |
| Client CPU per put | 0.86 ms | 0.91 ms | **0.83 ms** |
| Server CPU per put (MinIO + Corfu) | 0.32 ms | 0.56 ms | **0.30 ms** |
| Server busy fraction | 8.3 % | 15.7 % | 8.5 % |
| SSTables after the load (bucket) | 203 + 942 MiB | 204 + 953 MiB | 206 + 955 MiB |
| Trimmed log, checkpoints, objects | 187 MiB, 4, 22 | 188 MiB, 4, 20 | 154 MiB, 4, 23 |

Compaction still reads the bucket about once (1.0 GB out of MinIO), in 100 requests
instead of 204,858: about 6 per input file (`size`, footer, index, meta-index, two filter
blocks, one data read). The load rate, the client CPU and the server CPU are back at or
below the 64 KiB values. The 4 KiB penalty was the request count, not the bytes.

### Workload a, 600 s, 8 writers: 4 KiB with the crash fix against 4 KiB + range reads

| Cell | ops/s | GETs per op, last 60 s | GETs per miss | Client CPU per op | Server CPU per op | NOT_FOUND reads |
|---|--:|--:|--:|--:|--:|--:|
| 512 MB | 2,708 / **2,674** | 0.654 / **0.532** | 1.14 / **0.99** | 1.17 / **1.14 ms** | 1.26 / **1.16 ms** | 1 / 7 |
| 8 MB | 2,566 / **2,608** | 0.787 / **0.674** | 1.12 / **0.99** | 1.21 / **1.18 ms** | 1.37 / **1.28 ms** | 6 / 5 |

Each cell issues 117k to 133k fewer GETs (about 800k puts x 0.205, minus the compactions
that fall outside the run). GETs per miss is back to 0.99: the extra 0.14 per miss in the
4 KiB cells was compaction. Throughput is unchanged within 2 %, and the client CPU per op
that the projection uses is 1.16 ms (median of the two cells; 1.19 ms in the two 4 KiB
600 s cells, 1.37 ms when the 120 s warm-up cells are included). No compaction skipped an
input or abandoned a task in any cell (20 compactions per cell, `inputs_skipped=0`).

### Workload c control, 600 s, 512 MB

The read path did not change, and the control says so: 4 KiB against 4 KiB + range reads
at 512 MB, 600 s, 8 writers: 14,350 against 14,331 ops/s, `h` (last 60 s) 0.675 against
0.679, 0.322 against 0.319 GETs per op, 0.993 GETs per miss both, client CPU per read
0.247 against 0.245 ms, server CPU per read 0.423 against 0.425 ms, 0 failed operations
both. The projection therefore takes the `h` curve from the 4 KiB sweep, with this cell
as its 52 % point.

### Reads under a write-heavy mix miss the block cache

With compaction GETs gone, the remaining 0.53 GETs per op at 512 MB (52 % cache ratio) are
read misses: 1.06 per read, where workload c at the same cache gets 0.32. The block-cache
counters agree: `h` = 0.179 under workload a against 0.650 under workload c at 512 MB, and
0.010 against 0.231 at 8 MB. The cost model takes `h` from the workload-c sweep, so it
understates the GET line of a 50 % write mix at high cache ratios; at the projection's
0.16 % ratio (16 GB on 10 TB) both `h` values are small and the gap is at most 17 % of the
GET line. The mechanism, from `src/db/cache.cpp` (corrected 2026-08-28; an earlier version
of this paragraph blamed the log tail): `current_size`, `capacity` and `lru_list` hold only
SSTable blocks. Log records live in a per-file map outside the byte budget and leave only
when their log file is compacted (`invalidateLogFile`), so they cannot evict blocks; they
do add to RSS. What empties the block cache under writes is compaction itself. Every
compaction output is cold in the seven processes that did not build it (the builder
publishes its blocks write-through, `TableBuilder::flush`); the hot keys of a zipfian
write mix are also the most often updated, so their newest versions sit in the log and in
the level that is rewritten most often; and a deleted SSTable's blocks are never dropped
(`invalidateLogFile` runs only for log-prefix names), so they stay in the budget until
they reach the LRU tail, and its `Table*` is never freed. With 4 KiB blocks a file warms
16x more slowly than with 64 KiB, and under workload a the L1 files are rewritten faster
than that. The measurement that separates these: block-cache misses per level, and bytes
in the budget that belong to files no longer in the View, both printable at DB close.

### Projection: 10,000 ops/s, 50 % reads, RF=3 (USD per month)

| | 1 GB | 1 TB | 10 TB | Crossover vs Cassandra on EBS |
|---|--:|--:|--:|--:|
| Cassandra, NVMe / EBS | 1,565 / 2,402 | 1,565 / 2,402 | 12,587 / 4,742 | |
| OzoneDB 64 KiB (`cost-20260827`) | 3,693 | 6,247 | 7,053 | 17.8 TB |
| OzoneDB 4 KiB, compaction GETs as measured (0.205 per put) | 4,092 | 6,308 | 7,070 | 17.8 TB |
| OzoneDB 4 KiB, compaction range reads, predicted (0 per put) | 3,014 | 5,231 | 5,993 | 14.7 TB |
| **OzoneDB 4 KiB + range reads, measured (0.00015 per put)** | **2,997** | **5,230** | **5,992** | **14.7 TB** |

The measured line lands on the predicted one to the dollar. At 10 TB with a 16 GB cache:
`h` 0.157; S3 GETs $4,405 (from $5,482 with compaction GETs as measured, $5,045 at
64 KiB), of which compaction is now about $1 (from $1,078); clients 5 ($700, from 8 and
$1,120 at 64 KiB); log tier $614, storage $270, PUTs $4. Against the 64 KiB engine of
`cost-20260827` the two changes together save $1,061 per month at 10 TB (15 %) and move
the crossover from 17.8 TB to 14.7 TB. Figure: `results-cost-20260829-rr.png`.

### Conclusions

1. **The 4 KiB block curve is now realised.** Compaction GETs per put fell 1,375x (0.205
   to 0.00015), the load rate, client CPU and server CPU are back at the 64 KiB values, and
   the read-side gain of 4 KiB blocks (`h` 1.05x to 19x) is no longer cancelled. The 10 TB
   projection is $5,992 per month, crossover 14.7 TB.
2. **Request count, not bytes, was the penalty.** MinIO served the same 1.0 GB to
   compaction either way; 204,858 requests cost 0.25 ms of server CPU per put and 0.08 ms
   of client CPU per put more than 100 requests.
3. **`FileStorage` reported short reads as success.** `FileStorage::read(name, buf, offset,
   length)` tested the stream pointer, not the stream. Fixed in `2380ddf0`; found by the
   new truncated-file test. The S3 backend was not affected.
4. **Under a write-heavy mix the block cache is nearly cold.** `h` 0.18 against 0.65 at a
   52 % cache ratio. The projection's `h` comes from the read-only sweep. The cause is
   compaction, not the log tail (the LRU budget holds blocks only): outputs are cold in
   every process but their builder, and deleted files' blocks are never dropped. The next
   levers, in order: drop a deleted SSTable's blocks and `Table*` on its REMOVE; warm a
   compaction output in the peers with one range read per output (the read path that
   compaction now uses); count misses per level to size the effect first.

## Compaction-aware block cache (campaigns `cost-20260828-cache` and `-cache2`, engine commits `9ddaf573` to `f987cd54`)

The change planned in `PLAN-compaction-cache.md`, in three parts. C: block-cache hits and
misses per SSTable level, dead blocks (files no longer in the View), warm counters, printed
as a second `[lru_cache] levels …` line at DB close. A: when a COMPACT is applied to a
process's view, the inputs' cached blocks leave the byte budget and their `Table*` is
retired (queued under `view_mutex`, drained after it). B: one worker thread per process
reads each accepted output in ranged chunks (`Table::warm`, one GET per 64 MiB) and
publishes its blocks the way the builder's write-through does. B is off by default
(`cache_warm_enabled`); the policy has a level rule (`cache_warm_max_level`, default 1), a
budget rule (`cache_warm_max_fraction` of the cache, default 0.25) and an affinity rule
(`cache_warm_min_input_blocks`, default 1). Same cluster and settings as the range-read
campaign: 4 KiB blocks, trimming on, a fresh 1 GB load, 600 s cells with 8 writers. Rows in
`results-cost-20260828-cache.tsv` (`-cache-long` = the first build, `-cache2-long` = the
build with the affinity fix `4facdec7`).

### Load

The load with the warm on: 9,380 puts/s (range reads: 9,634), 99 GETs for 1M puts
(`get_per_write` 0.00015, unchanged), client CPU 0.83 ms per put (0.83), server CPU 0.32 ms
(0.30), 8/8 writers, no file warmed. The affinity rule refused every output, as planned for
a pure load. The load rows print non-zero `dead_bytes`; that was a scan against the
open-time View (the raw pointer is refreshed only by `DB::get`), fixed in `74099ed4` to use a
fresh snapshot. Run cells refresh it per get and were not affected.

### Workload a, 600 s, 8 writers, 512 MB (52 % cache ratio)

| Cell | ops/s | `h` | L1 hits / misses | L2 hits / misses | GETs per op (run / last 60 s) | Warmed files / blocks / read fraction | Client CPU per op | Server CPU per op | RSS peak |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Range reads (`cost-20260829-rr`) | 2,674 | 0.179 | | | 0.532 / | | 1.14 ms | 1.16 ms | |
| A + C, warm off | 2,724 | 0.183 | 64,574 / 226,140 | 135,248 / 666,435 | 0.543 / 0.518 | 0 | 1.18 ms | 1.14 ms | 3.68 GB |
| Warm on, first build (defaults) | 2,706 | 0.183 | 64,347 / 225,803 | 135,781 / 665,837 | 0.543 / 0.521 | 0 | 1.17 ms | 1.16 ms | 3.77 GB |
| Warm on, first build, level 2, 50 % | 2,783 | 0.269 | 66,352 / 231,842 | 233,404 / 583,473 | 0.486 / 0.465 | 42 / 880k / 0.14 | 1.18 ms | 1.10 ms | 3.77 GB |
| Warm on, affinity fix (defaults) | 2,857 | 0.348 | 273,200 / 33,701 | 126,561 / 715,517 | 0.434 / 0.409 | 105 / 1.01M / 0.20 | 1.13 ms | 1.03 ms | 4.00 GB |
| **Warm on, affinity fix, level 2, 50 %** | **2,912** | **0.403** | 275,436 / 37,386 | 194,835 / 660,020 | **0.397 / 0.373** | 154 / 2.02M / 0.15 | 1.14 ms | 1.01 ms | 4.18 GB |

Every cell finished 8/8 with 0 to 7 NOT_FOUND reads (the documented transient miss), GETs
per miss 0.99, `dead_blocks` 0 to 2 (a reader that published a block of a file removed
under it) and every retired `Table*` freed. `h` here is per block lookup; a workload-a read
probes about 1.3 levels, so the extractor's `h_steady` (one lookup per read) understates it
and is not quoted.

Three results, in the order they were found.

1. **The drop alone (A + C) changes nothing measurable**: 2,724 against 2,674 ops/s, `h`
   0.183 against 0.179. Dead blocks were a hygiene problem, not a hit-rate problem.
2. **The first warm-on build warmed nothing.** Its counters said why: `skipped_affinity`
   248 and `skipped_level` 104 (minus 23 per process for the load's COMPACT records
   replayed at open). Every L1 output comes from a log-to-L1 compaction, whose inputs are
   log files and never had an SSTable block cached, so the affinity rule refused all of
   them; the level rule refused the L2 outputs. The fix (`4facdec7`): for a compaction with
   log inputs the affinity signal is the number of lookups on the destination level since
   the previous such event (0 in a load, hundreds per second under workload a).
3. **Warming the L1 outputs lifts the L1 hit rate from 0.22 to 0.89** (misses 226k to
   34k), `h` from 0.18 to 0.35, GETs per op from 0.54 to 0.43 (-20 %), throughput +5 %,
   client CPU per op -4 %, server CPU per op -10 %. Adding the L2 outputs (level rule 2,
   budget rule 50 %) takes `h` to 0.40 and GETs per op to 0.40 (-27 %), throughput +7 %.
   The cost: 154 files read once (about 1,100 extra requests, 9 GB more read from MinIO
   over 600 s, free in-region on S3), RSS +0.5 GB (the 64 MiB chunk plus the parsed blocks
   in flight), and only 15 to 20 % of the warmed blocks are read before they are evicted
   or dropped again.

### Why L2 stays cold

Three quarters of the misses are on L2 in every cell. Under hashed keys one L1 file spans
the whole key space, so every L1-to-L2 compaction rewrites the L2 files it overlaps: 13
such compactions per process per 600 s, 48 L2 outputs of about 100 MB, about 4.8 GB of L2
rewritten per 600 s against a level that holds 1 GB. Part A drops the cached half of L2
at each rewrite, and part B can only put it back by reading the whole output into a cache
that holds half of the level: 85 % of what it publishes is never read. A warm that
publishes only the blocks whose key range overlaps the dropped inputs' cached blocks
("restore what the LRU had") would keep the read to one GET and cut the published bytes;
that is the next step in `PLAN-compaction-cache.md`. The structural cause, L1 files that
overlap all of L2, is the compaction shape, not the cache.

### Workload a at 8 MB and the workload-c control

At 8 MB (0.8 % ratio) the budget rule refused every L1 output (`skipped_budget` 240,
`skipped_level` 104): 2,591 ops/s, `h` 0.010, 0.657 GETs per op, 0 failed, `dead_blocks`
0, against the range-read cell's 2,608 / 0.010 / 0.674. The control (workload c, 512 MB,
warm on): 14,011 ops/s, `h` 0.649, 0.347 GETs per op, 0 failed, no file warmed (no
compaction), against the range-read control's 14,331 / 0.679 / 0.319. The read path did
not change; the 2 % throughput gap is inside the run-to-run spread of the three controls
(14,350 / 14,331 / 14,011).

### Two bugs found on the way

The bucket restore between cells (`mc mirror --overwrite --remove`) kept a cell's
`checkpoint/LATEST` (same 8 bytes, newer mtime) while removing the checkpoint it named,
so every writer of the next cell died at open ("a LATEST that cannot be read"); three
cells were lost before the restore force-copied every `LATEST` (`1127b7cc`). The loader
wrapper named its server sample after the plain label, so the `--cache-warm` load
overwrote the range-read load's sample (`41fd65a0`; the sample files were renamed by hand).

### Projection

Unchanged. The model's `h` comes from the workload-c sweep, and at the projection's 0.16 %
ratio (16 GB on 10 TB) both `h` values are small. What changed is the write-heavy `h` at
52 %: 0.40 with the warm on against 0.65 read-only, so the model overstates the GET line of
a 50 % write mix at high cache ratios by less than before (0.18 against 0.65 in the
range-read campaign). The engine default keeps the warm off; `cache_warm_enabled=true`
with `cache_warm_max_level=2` and `cache_warm_max_fraction=0.5` is the measured setting
for a write mix at a large cache ratio.

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
