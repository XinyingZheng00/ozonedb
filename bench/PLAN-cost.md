# Plan: cost model with measured coefficients, OzoneDB (Corfu + S3) vs Cassandra (branch `worktree-plan-cost`)

**Status (2026-08-27):** phases 0 to 5 are DONE. Campaign `cost-20260827` ran on the
cluster (amd127 + 8 clients): 37 cells, 0 failed operations. Results, coefficient and
projection tables, the figure and the findings are in `bench/RESULTS-cost.md`
(`results-cost-20260827*.tsv`, `results-cost-20260827.png`). Headline: `h` tracks the
cache-to-data ratio (0.645 at 52 %, 0.01 at 0.1 %) because 64 KB blocks under hashed
keys have no zipfian locality, so the S3 GET line dominates; with 35x the client CPU
per op of Cassandra, OzoneDB with trimming turns cheaper than Cassandra RF=3 only above
17.8 TB (EBS) or 5.6 TB (NVMe). Phase 6 (real S3) was not run. The branch is `visibility`
at `35f5cb90` with `worktree-cassandra-bench` (`5518b2b4`) merged in, so one tree holds
the trimmed engine and the Cassandra baseline. The Cassandra branch alone is 16 commits
behind `visibility` and must not be used for this experiment.
**Re-run 2026-08-27 with 4 KiB blocks** (engine commit `dd0f195e`, campaign
`cost-20260828-4k`, section "Re-run with 4 KiB blocks" of `RESULTS-cost.md`): `h` rose
to 0.675 / 0.341 / 0.215 / 0.141 / 0.032 at the five cache ratios (1.05x to 19x), read
throughput +18 to 36 %, client CPU per read -25 %; but compaction GETs per put rose from
0.015 to 0.205 because `Table::getAll` reads one block per GET. The projection is
unchanged at 10 TB ($7,070) until compaction reads ranges ($5,993, crossover 14.7 TB).
That engine change is the next step. The re-run also found that every workload-a cell
of `cost-20260827` had lost 1 to 4 of 8 writers to a native crash (`readDataBlocks` on a
file removed under the reader; runner still `rc=0`); fixed in `96b9265d`, the 600 s
workload-a cells now finish 8/8. The 64 KiB workload-a sums are survivor sums.

## Goal

One figure: projected monthly cost against dataset size, one line per system, with the
crossover where OzoneDB becomes cheaper. Every input to the figure is one of two things:

- a public list price, or
- a coefficient measured on the CloudLab cluster, which is free.

There is no budget for a cloud run at the crossover size. The model is linear in a small set
of per-operation and per-byte coefficients. None of them depends on the dataset size, except
the cache hit rate, and that one depends only on the ratio of cache to dataset. So the whole
model is measurable at 1 GB and 10 GB, and the extrapolation to 10 TB is arithmetic that a
reviewer can check.

## What the trimmed engine changes in the model

The 16 commits from `visibility` change three cost terms. The plan measures all three.

| Change | Effect on cost | Coefficient |
|---|---|---|
| Checkpoint + `prefixTrim` (`PLAN-trimming.md`) | The log tier holds one trim interval of entries, not the full history. Measured on the 1M load: Corfu disk 293 MB with trimming, 1.1 GB without. Live state 51-61 MB. The log tier is flat in D. | `L` (live log bytes) |
| Checkpoint uploads | Every `log_trim_interval_ms` the trimmer writes one object per live file plus a manifest and `LATEST`. At 30 s that is about 2,900 checkpoints per month. This is a new S3 PUT term that the untrimmed engine did not have. | `k` (objects per checkpoint), `b_k` (bytes per checkpoint) |
| Join from the checkpoint | A new process downloads the checkpoint (one GET per live file, about 61 MB) instead of replaying the stream. Replay is 1.6 s, not 16 s. This is a per-join request cost, and it is also the elasticity number. | `j` (GETs per join), `b_j` (bytes per join) |
| Sequencer-keyed fast ack | Put throughput is back to about 1,090 puts/s per writer (8,395/s aggregate at 8 writers). This is the write rate that sizes the stateful tier. | `ops_node` |

Trimming trades log-tier disk for S3 PUT requests. The interval is the knob. The plan
measures the trade at the default interval and reports the PUT term next to the disk saving.

## The model

```
Cassandra(D) = N(D) * p_node                            # stateful tier
             + n_clients * cpuC * p_cpu                 # client compute

  N(D) = max(3, ceil(RF * sC * D / disk_node), ceil(W / ops_node_C))

OzoneDB(D)   = p_seq + 3 * (p_logunit + L * p_disk)      # log tier, flat in D
             + sO * D * p_s3                             # bulk bytes
             + R * (1 - h(c/D)) * g * p_get * 2.63e6     # read misses -> GETs
             + W * wa / obj * p_put * 2.63e6             # compaction PUTs
             + k * (2.63e6 / T_trim) * p_put             # checkpoint PUTs
             + n_clients * cpuO * p_cpu                  # client compute
```

`2.63e6` is seconds per month. `T_trim` is `log_trim_interval_ms / 1000`.

| Symbol | Meaning | Depends on D | Measured in |
|---|---|---|---|
| `sC` | Cassandra bytes on disk per logical byte, after compaction, plus the transient peak during compaction | no | phase 1 |
| `sO` | bucket bytes per logical byte, after one compaction cycle | no | phase 1 |
| `L` | live log bytes with trimming on (Corfu disk after the load) | no | phase 1 |
| `L0` | Corfu disk without trimming, the "today" line | yes, linear in writes | phase 1 (control from `PLAN-trimming.md`) |
| `h(c/D)` | SSTable block cache hit rate as a function of cache-to-dataset ratio | only through the ratio | phase 2 |
| `g` | S3 GETs per cache miss | no | phase 2 |
| `wa` | bytes written to S3 per logical byte written (write amplification) | weakly | phase 1 (load) |
| `k`, `b_k` | objects and bytes per checkpoint | no | phase 2 (trimmer on vs off) |
| `j`, `b_j` | GETs and bytes per process join | no | phase 2 (replay line) |
| `cpuC`, `cpuO` | client CPU-seconds per operation, and peak RSS | no | phases 2-4 |
| `ops_node_C`, `cpu_per_writer` | server ops/s and server CPU per writer | no | phase 4 |

`RF` is 1 in every measured cell (parity rule) and 3 in the projection, for both systems.
The projection multiplies Cassandra bytes by 3 and gives OzoneDB three Corfu log units
(chain replication). State this next to the figure.

## Unit prices

Approximate on-demand list prices, us-east-1, 730 hours per month. Verify each one against
the current price list before the paper goes out, and record the date.

| Item | Symbol | Price per month |
|---|---|---|
| c6i.large (2 vCPU, 4 GB) — sequencer, thin client | `p_seq`, `p_cpu` (Cassandra client) | $62 |
| m6i.xlarge (4 vCPU, 16 GB) — OzoneDB client | `p_cpu` (OzoneDB client) | $140 |
| r6i.xlarge (4 vCPU, 32 GB) — Corfu log unit | `p_logunit` | $184 |
| i4i.2xlarge (8 vCPU, 64 GB, 1.9 TB NVMe) — Cassandra node | `p_node` | $501 |
| i4i.4xlarge (16 vCPU, 128 GB, 3.75 TB NVMe) — Cassandra node at 10 TB | `p_node` | $1,002 |
| gp3 EBS | `p_disk` | $0.08 per GB |
| S3 Standard storage | `p_s3` | $0.023 per GB |
| S3 GET | `p_get` | $0.40 per million |
| S3 PUT | `p_put` | $5.00 per million |

## Fixed parameters

| Knob | Value | Where |
|---|---|---|
| Server | `amd127` (`10.10.1.1`): Corfu, MinIO, and Cassandra, never two at once | `nodes.log`, `nodes.store`, `nodes.cassandra` |
| Clients | 8 hosts, `amd160` … `amd133`, one writer process with one thread each | `nodes.clients`, `--writers-list 1` |
| Replication | none: Corfu `-s`, Cassandra RF=1 | parity rule |
| Datasets | D1 = 1,000,000 × 1 KB (1 GB), D10 = 10,000,000 × 1 KB (10 GB) | `local.load.record_cnt` |
| Workloads | `a` (50/50) and `c` (100 % read). These bound the write and read terms. | `--workloads "a c"` |
| Read mode | OzoneDB default reads. The linearizable fence adds Corfu RPCs, not S3 requests, so it changes only `cpuO`. Two extra cells measure that. | `--linearizable` on those two cells only |
| Trimming | on (`--log-trim`) in every OzoneDB cell except the two controls in phase 2 | flag, never a `ycsb.yaml` edit |
| Duration | 120 s per cell; report the last 60 s (`extract_steady_throughput.py`) | `--duration 120` |
| Trial | 1. Coefficients are ratios and are stable; report the spread across the two workloads. | `--trial 1` |
| Run tag | one tag for both systems, `cost-YYYYMMDD` | `--run-tag` |

## Fairness rules

1. Same server box, same clients, same dataset, same duration, same per-process concurrency,
   RF=1 on both systems.
2. Never run Corfu and Cassandra at the same time. Stop one before the other starts.
3. Check `scaling_governor` on every client before the first cell. A reboot resets it, and
   two hosts of the last cluster ran every cell at half speed until it was pinned.
4. Client CPU is a cost line for both systems. Measure it the same way on both.
5. Snapshot the MinIO counters before and after every cell. They are cumulative since the
   process started, and a cell without a matching "before" snapshot has no request count.

## Phase 0. Instrumentation (code, before any cell)

Implemented 2026-08-27 on `worktree-plan-cost`. Not yet exercised on the cluster. The
smoke cell at the top of phase 1 is where each item produces its first real output.
Push with `sync.yml` first. Then re-run `setup.sh --role minio` on the store node (the
metrics environment), and `setup.sh --role corfu-server` and `--role cassandra` for
`sysstat`. Nothing in phase 0 touches C++, so no rebuild. Before the push, `touch` the
sources if the branch changed: rsync keeps mtimes and a stale `.so` survives a branch
switch.

- [x] **P0.1 `--lru-cache-bytes N`.** Forwarded like `--log-trim` through
      `load_local_ycsb_multiproc.py`, `run_local_ycsb_multiproc.py`,
      `run_multinode_ycsb.py` and `run_multinode_ycsb_with_corfu.sh`.
      `lru_cache_corfu_settings()` is the one override. `_make_corfu_config_per_writer`
      and `_make_local_config_per_writer` write `lru_cache_bytes`. `result_label()`
      appends `-lru64m` (`lru_label_token()`: exact powers of 1024 are short, anything
      else is bytes). Unset means the base config and no token, so old result names do not
      change. Also added: `--record_cnt` (`--record-cnt` on the wrappers) on the same chain
      and on both load wrappers, for phase 5.
- [x] **P0.2 MinIO metrics + sampler.** `setup.sh --role minio` writes
      `MINIO_PROMETHEUS_AUTH_TYPE=public` into the unit. `sysstat` and `curl` are in the
      `corfu-server`, `minio` and `cassandra` roles. `bench/scripts/server_sampler.sh`
      (`start DIR CELL`, `stop DIR CELL`, `snapshot`) records the filtered
      `/minio/v2/metrics/{cluster,bucket}` counters, `du -sk` of `/mnt/corfu/run_batch`,
      `/mnt/corfu/load`, `/tank/minio` and `/tank/cassandra/data`, `mc du --json` of the
      bucket, and the server pids. It runs `pidstat -h -u -r` between start and stop.
      It is driven from `run_multinode_ycsb.py`, not from the shell wrappers. The
      orchestrator is the one place that knows the cell's label, writer count and trial,
      so the sample files carry the same cell name as the result files:
      `<label>_workload<wl>_w<N>_trial<T>.{before,after,pidstat}.txt` under
      `<results>/_server/<host>/`. It samples `nodes.log` and `nodes.store` for
      `ozonedb-corfu`, and `nodes.cassandra` for `cassandra`. `--no_server_sample` turns it
      off. Both load wrappers sample the same way around the load, into
      `bench/results/local/_server/` with cell `..._workloadload_w8_trial0`.
- [x] **P0.3 Client CPU.** `spawn_parallel` prefixes every writer with `/usr/bin/time -v`
      when GNU time is present (`time` is in the client role). The block lands at the end
      of each `.result` file. Without GNU time the writer runs bare and the CPU columns
      stay empty.
- [x] **P0.4 Cache counters.** No code change. The extractor parses
      `[lru_cache] sstable hits=… misses=… capacity=…`. **Confirm on the smoke cell** that
      the line is in a finished `.result` file. Only SSTable blocks count. Data-log reads
      come from `file_buffers_` in memory on the Corfu backend and never reach S3, so this
      counter is the one the model needs.
- [x] **P0.5 Cassandra space.** `cassandra_ctl.sh space [KS]` prints the `tablestats`
      space lines (server up) and `du_kb` of `data`, `data/data`, `data/commitlog` and
      `data.load`. `cassandra_ctl.sh compact [KS]` runs `nodetool compact` and waits until
      `compactionstats` shows no pending task.
- [x] **P0.6 Extractor.** `bench/scripts/extract_cost_coefficients.py DIR [DIR ...]
      [--window 60] [--record-bytes 1024] [--tsv out.tsv]`. One row per (label, workload,
      writers, trial), 64 columns: YCSB ops, steady ops/s, cache hits, misses, `h`,
      `cache_ratio`, MinIO GET/HEAD/PUT/LIST/DELETE deltas and bytes, `du` deltas, bucket
      and `checkpoint/` bytes, client user and sys CPU and RSS, the replay and restore
      lines, `join_gets` (files + manifest + LATEST), trimmer count and `ckpt_objects`
      (files + 2 per checkpoint), ack counters, server CPU-seconds per process class,
      `server_busy_frac`. Derived: `get_per_op`, `get_per_miss` (`g`), `put_per_op`,
      `put_per_write`, `client_cpu_s_per_op`, `server_cpu_s_per_op`. Load files at the
      results root are the `load` workload. Verified on a synthetic results directory
      that uses the exact log-line formats of the engine.
- [x] **P0.7 Plotter.** `bench/scripts/plot/plot_cost_model.py coefficients.tsv
      bench/scripts/plot/prices.json --space space.json [--table out.tsv]`.
      `prices.json` holds the prices (with `as_of`), the instance roles and the projection
      parameters: 10,000 ops/s, 50 % reads, RF=3, 3 log units, 4 GB and 16 GB caches.
      `space.json` (copy `space.example.json`) holds the phase 1 numbers that no cell row
      carries: `sC`, `sO`, `L_gb`, `L0_kb_per_put`, `trim_interval_s`. Every coefficient is
      printed with its source. Anything not measured is marked ASSUMED on the figure.
      Client and server node counts follow from CPU per op at 70 % utilisation, so the
      client term scales with load, not with D. Writes `out/cost_model.{pdf,png}`.

## Phase 1. Space (once per dataset)

Gives `sC`, `sO`, `L`, `L0`, `wa`, and the checkpoint sizes.

1. Check that the cluster is free (the shared-cluster memory). Check the governor.
2. OzoneDB load with trimming:
   ```bash
   bash bench/scripts/local/load_corfu_dataset.sh --writers 8 --log-trim
   ```
   Snapshot the MinIO counters before and after with `server_sampler.sh`. Record:
   - `du -s /mnt/corfu/load` → `L` (with trimming).
   - `mc du ozonedb-local/ozonedb-sstables` → bucket bytes; split `checkpoint/` from the
     SSTable prefix with `mc du` on each. The SSTable part over 1 GB is `sO`.
   - MinIO `traffic_received_bytes` delta over 1 GB is `wa` including checkpoints; the
     `checkpoint/` bytes times the checkpoint count gives the checkpoint share. Subtract it.
   - the replay line of one fresh writer after the load (`stream_MB`, `live_MB`,
     `live_files`) → `L`, `k`, and the first estimate of `b_j`.
   - `[trimmer] checkpoint C=…` lines: count, and objects and bytes per checkpoint → `k`, `b_k`.
3. `L0`: the control without trimming is already measured (1.1 GB stream, 1,022,163 entries
   for 1M puts, `PLAN-trimming.md` baseline). Rerun it only at 10 GB if phase 5 needs the
   "today" line at that size. It is `1.1 KB` per put, and the model uses that slope.
4. Cassandra load:
   ```bash
   bash bench/scripts/local/load_multinode_cassandra.sh --writers 8
   ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh space'
   ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh compact && /tank/cassandra/cassandra_ctl.sh space'
   ```
   Record both `space` outputs. Before `compact` is the transient peak (headroom); after is
   `sC` at RF=1. YCSB field bytes are random, so LZ4 gains about nothing, but measure it.
5. Stop Cassandra. Both datasets stay snapshotted (`/mnt/corfu/load` plus the bucket for
   OzoneDB, `/tank/cassandra/data.load` for Cassandra). The Corfu snapshot and the bucket
   are one unit: a restore must copy both, or `bootstrap` throws.

## Phase 2. Request coefficients (OzoneDB, 12 cells)

Gives `h(c/D)`, `g`, PUTs per op, `k`, `j`, and `cpuO`.

The trick: the 10 TB target with a 16 GB cache per client is `c/D = 0.16 %`, and with a
4 GB cache it is `0.04 %`. Reach those ratios on the 1 GB dataset by shrinking the cache.

| `--lru-cache-bytes` | `c/D` at 1 GB | Stands for |
|---|---|---|
| 536870912 (512 MB, the default) | 50 % | hot set fits |
| 67108864 (64 MB) | 6 % | |
| 8388608 (8 MB) | 0.8 % | 10 TB with a 100 GB cache |
| 1048576 (1 MB) | 0.1 % | 10 TB with 16 GB |
| 131072 (128 KB) | 0.012 % | 10 TB with 1 GB, the pessimistic edge |

```bash
TAG=cost-$(date +%Y%m%d)
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -sd,)
for c in 536870912 67108864 8388608 1048576 131072; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes $c \
    --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" \
    --trial 1 --duration 120 --run-tag $TAG || { echo "FAILED at lru=$c"; break; }
done
# controls: trimmer off, default cache, to isolate the checkpoint PUTs. The label does
# not carry the trim state, so a separate tag keeps these from overwriting the trimmed
# 512 MB cell; the extractor takes both dirs at once.
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --lru-cache-bytes 536870912 \
  --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-notrim
```

Per cell, the extractor gives `h`, GETs per op, `g = GETs / misses`, PUTs per op, and
CPU per op. The PUT difference between the trimmed cells and the two controls, divided by
the checkpoint count from the `[trimmer]` lines, is `k` measured a second way. The replay
line of every writer at cell start gives `j` and `b_j` (GETs and bytes to restore the
checkpoint) — the same number on every host, so it is eight samples per cell.

The cache is per process. Eight writers hold eight caches, and `c` in the model is the
cache of one process. Say so in the paper.

Two more cells give `cpuO` under strict reads:

```bash
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --linearizable \
  --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG
```

## Phase 3. Cassandra cells (4 cells)

Gives `cpuC` and the Cassandra points of the frontier at this cluster.

```bash
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh restore-load'   # the wrapper does this too
for mode in quorum serial; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency $mode \
    --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG
done
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh stop'
```

`quorum` equals `one` at RF=1 and is the default point. `serial` is the strict point.
`/usr/bin/time -v` gives `cpuC` for both.

## Phase 4. Server scaling (6 cells)

Gives `ops_node` and `cpu_per_writer` for both systems, which set the throughput-bound
node count. Workload `a` only, writers 2, 4, 8 (hosts 2, 4, 8, one writer each).

```bash
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -sd,; }
for n in 2 4 8; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency quorum \
    --workloads a --writers-list 1 --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG
done
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh stop'
for n in 2 4 8; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim \
    --workloads a --writers-list 1 --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG
done
```

`pidstat` on `CorfuServer`, `minio`, and `CassandraDaemon` gives server CPU-seconds per
operation. The slope against writer count is `cpu_per_writer`. Server ops/s at 8 writers,
divided by the fraction of the box that `pidstat` shows busy, is `ops_node` for one box.
The 8-writer OzoneDB cell here is the same configuration as the 512 MB cell of phase 2,
so use that one and run only 2 and 4.

## Phase 5. Scale check at 10 GB (load + 4 cells)

Checks the one assumption that the extrapolation rests on: `h` depends on `c/D` only.

1. Pass `--record-cnt 10000000` to both load wrappers and to every run cell of this
   phase (no `ycsb.yaml` edit). Load both systems as in phase 1. Expect
   about 20 min for OzoneDB (8,395 puts/s aggregate on the fast path) and a similar time
   for Cassandra. Record every phase 1 quantity again. `sO`, `sC`, `k`, `L` must match
   the 1 GB values within 10 %. `L` in particular must not grow: it is the trimming claim.
2. Two cache ratios that the 1 GB sweep also covered:

   | `--lru-cache-bytes` | `c/D` at 10 GB | 1 GB twin |
   |---|---|---|
   | 10485760 (10 MB) | 0.1 % | 1 MB cell |
   | 1310720 (1.25 MB) | 0.012 % | 128 KB cell |

   Run workloads `a` and `c` at each. If `h` at equal ratio agrees within a few points, the
   curve is scale-free at these sizes and the 10 TB read-off is justified. If it does not,
   report both curves and extrapolate from the 10 GB one, and say why.
3. One fresh-writer join at 10 GB: the replay line gives `j`, `b_j`, and the time to first
   op. This is the elasticity number for the paper, and it must be about the 1 GB value,
   because the checkpoint holds live state, not history.

## Phase 6. Real S3 check (optional, under $5)

MinIO counts requests. A reviewer will ask whether the real service counts the same. Point
one cell at a real bucket:

- `s3.endpoint` = empty (real AWS), `s3_use_path_style = false`, real credentials in the
  per-writer config, bucket in `us-east-1`.
- One workload `c` cell, 8 writers, 120 s, `--lru-cache-bytes 1048576`.
- Read the request count from the bucket's CloudWatch request metrics (enable them on the
  bucket first) or from the next bill.

Compare GETs per op against the MinIO cell with the same flags. Throughput will differ,
because the latency is milliseconds instead of a LAN round trip. The coefficient is what
must agree. At 8 writers the cell makes under 200,000 requests, which is under $0.10, plus
the storage of the 1 GB dataset for a day.

## Extraction

```bash
# the tagged run cells, the trimmer-off controls, and the results root (load files + their server samples)
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/$TAG bench/results/local/$TAG-notrim \
    bench/results/local --tsv bench/results-cost-$TAG.tsv
cp bench/scripts/plot/space.example.json bench/scripts/plot/space.json   # fill in the phase 1 numbers
python3 bench/scripts/plot/plot_cost_model.py bench/results-cost-$TAG.tsv bench/scripts/plot/prices.json \
    --space bench/scripts/plot/space.json --table bench/results-cost-$TAG-projection.tsv
```

Commit the TSV, `prices.json` (with the date the prices were read), and the figure under
`bench/`, the way `results-strict-frontier.tsv` is kept. The raw cells stay in
`bench/results/local/` (gitignored).

## The coefficient table (fill in)

| Symbol | 1 GB | 10 GB | Source cell |
|---|---|---|---|
| `sC` after compact / peak | | | phase 1 |
| `sO` | | | phase 1 |
| `L` (trim on) | | | phase 1 |
| `L0` slope (bytes per put) | 1.1 KB | | `PLAN-trimming.md` |
| `wa` (SSTable bytes only) | | | phase 1 |
| `k`, `b_k` | | | phases 1-2 |
| `j`, `b_j`, time to first op | | | phases 2, 5 |
| `g` | | | phase 2 |
| `h` at 50 / 6 / 0.8 / 0.1 / 0.012 % | | (0.1, 0.012 only) | phases 2, 5 |
| `cpuO` default / linearizable, a and c | | | phase 2 |
| `cpuC` quorum / serial, a and c | | | phase 3 |
| `ops_node`, `cpu_per_writer`, both | | | phase 4 |

## The projection table (fill in)

| Cost line | 1 GB | 1 TB | 10 TB |
|---|---|---|---|
| Cassandra stateful tier, RF=3 | | | |
| Cassandra clients | | | |
| **Cassandra total** | | | |
| OzoneDB log tier (3 log units + sequencer, `L`) | | | |
| S3 storage (`sO * D` + 2 checkpoints) | | | |
| S3 GETs at `h(16 GB / D)` | | | |
| S3 GETs at `h(4 GB / D)` | | | |
| S3 PUTs, compaction | | | |
| S3 PUTs, checkpoints at `T_trim` = 30 s | | | |
| OzoneDB clients | | | |
| **OzoneDB total, 16 GB cache** | | | |
| **OzoneDB total, 4 GB cache** | | | |
| OzoneDB "today" (no trim: log tier = `L0` slope × total writes) | | | |

The pre-measurement estimate from list prices alone (2026-08-26 discussion): OzoneDB about
70 % more expensive at 1 GB (the clients), break-even between 1 and 4 TB, 4-6× cheaper at
10 TB, and every one of those numbers needs `h ≥ 0.99`. The measurement replaces the guessed
coefficients behind that estimate.

## The figure

Log-log axes. X: dataset size, 1 GB to 100 TB. Y: dollars per month. Lines:

- Cassandra RF=3 on NVMe (`i4i`) and on EBS (`m6i` + gp3). Two lines, because the cheaper
  layout moves the crossover.
- OzoneDB with trimming, as a band: lower edge at `h(16 GB / D)`, upper edge at `h(4 GB / D)`.
- OzoneDB "today" without trimming, dashed.
- Markers at 1 GB and 10 GB: the measured cells, priced.
- A vertical mark at the crossover.

Next to the figure, four stated assumptions: list prices and their date, the Cassandra
node count is storage-bound above the 3-node floor, the hot set is scale-free (checked at
1 GB and 10 GB), and the log tier is flat in D (checked at 1 GB and 10 GB).

## Time budget

| Step | Estimate |
|---|---|
| Phase 0 scripts | one day |
| Phase 1, both loads at 1 GB (or restore) + compact | 30 min |
| Phase 2, 14 cells × (120 s + ~40 s restart) | 40 min |
| Phase 3, 4 cells × (120 s + ~90 s restore) | 15 min |
| Phase 4, 4 new cells | 15 min |
| Phase 5, two 10 GB loads + 5 cells | 1.5 h |
| Phase 6 (optional) | 30 min |
| Extraction and figure | 30 min |

About one working day on the cluster after phase 0.

## What to watch

- **MinIO counters are cumulative.** A cell with no "before" snapshot has no request
  count. `server_sampler.sh` must run around every cell, both wrappers.
- **The `[lru_cache]` line prints at destruction.** A writer killed by `--cell_timeout`
  loses it. A missing line means the cell has no `h`; rerun it.
- **Misses are not GETs.** Block and table loads are single-flighted, and a table load is
  one GET for many blocks. `g` is the measured ratio, and MinIO is the source of truth.
- **Checkpoint PUTs inflate PUTs per op on short cells.** Separate them with the
  `[trimmer]` counts or the two controls. Report the checkpoint term on its own line.
- **`wa` comes from the load, not from a 120 s cell.** A cell may or may not contain a
  compaction. The load contains the full cycle.
- **Workload `a` at 8 writers writes about 200 MB per cell**, six data-log files at 32 MB.
  Compactions do run inside a cell, so the PUT counter is non-zero even without the
  trimmer. That is expected.
- **`nodetool compact` on 10 GB takes minutes.** Wait for it before `space`.
- **The trimmer is global writer 0**, the first writer on the first host. If that host is
  dropped from `--client-hosts`, another host becomes writer 0. The trimmer moves with it.
- **A restore must copy the Corfu dir and the bucket together.** `bootstrap` throws on a
  checkpoint that is ahead of the log, and fail-stops a tailer below the trim mark.

## Follow-ups (not in this plan)

- A second log backend, so the substrate claim in the paper is a measurement and not a
  design note. NATS JetStream is the cheapest to add (dense sequence, tail query, C client).
- The trim interval as a knob: `T_trim` at 10 s, 30 s, 120 s, to plot the disk-vs-PUT trade.
- Cassandra RF=3 on three boxes, to measure `sC` at RF=3 instead of multiplying.
