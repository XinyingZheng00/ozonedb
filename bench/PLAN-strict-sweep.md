# Plan: strict-mode throughput sweep, OzoneDB vs Cassandra (branch `worktree-cassandra-bench`)

**Status (2026-08-25):** planned, not run. The scripts exist on this branch
(commit `ce39b485`) and passed dry runs only. The Cassandra YCSB binding has
not been compiled yet; step 3 does that before any cell runs.

## Goal

One throughput-vs-writers curve per (system, workload) at the **strict** point
of the consistency frontier:

- OzoneDB on Corfu with `--linearizable` (label `ozonedb-corfu-linearizable`).
- Cassandra with `--consistency serial` (label `cassandra-serial`: SERIAL
  reads, `IF [NOT] EXISTS` writes, one Paxos round per write).

Matrix: total writers **2, 4, 8, 16, 32** x workloads **a b c d f** x 1 trial
x 2 systems = **50 cells**. Every cell restarts its server from the load
snapshot, so cells never see each other's writes.

## Fixed parameters

| Knob | Value | Where |
|---|---|---|
| Server | one node, `amd132` (`10.10.1.0`), for both systems | `nodes.log`, `nodes.cassandra` |
| Replication | none: Corfu `-s`, Cassandra RF=1 | parity rule, see below |
| Dataset | 1,000,000 records x 1 KB (10 fields x 100 B) | `local.run.record_cnt`, workload template |
| Threads per writer process | 1 | `local.run.threads: [1]` |
| Duration per cell | 120 s (`--duration 120`) | overrides `local.run.max_exec_time` |
| Operation count | unbounded (`999999999`), duration-capped | `local.run.operation_cnt` |
| Trial | 1 (`--trial 1`) | |
| Run tag | one tag for both systems, `strict-YYYYMMDD` | labels keep the files apart |

A "writer" is one YCSB process with one thread on one client host. Total
outstanding requests therefore equal the writer count, for both systems.

Sweep points, spread across hosts before doubling up on a host:

| total writers | client hosts | `--writers-list` (per host) |
|---|---|---|
| 2 | first 2 of `cloudlab.hosts` | 1 |
| 4 | first 4 | 1 |
| 8 | all 8 | 1 |
| 16 | all 8 | 2 |
| 32 | all 8 | 4 |

## Fairness rules (do not bend)

1. Both systems un-replicated, same server box, same clients, same dataset,
   same duration, same per-process concurrency.
2. Never run Corfu and Cassandra at the same time. They share `amd132`, and
   Corfu's JVM is started with a 120 GB heap. Stop one before the other
   starts (steps 7 and 9).
3. The consistency mode is a flag on the wrapper, never a `ycsb.yaml` edit.
4. Read `SumWriterThroughput(ops/sec)` from the aggregate files, not
   `Throughput(ops/sec)`. The latter divides by the orchestrator wall time,
   which includes ssh, Maven classpath resolution, and JVM start.

## Prerequisites

- [ ] The cluster is free. Check for another session's `run_multinode_ycsb.py`,
      `ansible-playbook`, YCSB JVMs on the clients, and a running
      `CorfuServer`, before step 1 (see the shared-cluster memory).
- [ ] `/mnt/corfu/load` on `amd132` and the MinIO bucket hold the 1M x 1 KB
      OzoneDB dataset from the last `load_corfu_dataset.sh` (the O1-O3
      campaign). Do not reload unless it is missing.
- [ ] `~/.m2` on every client can reach Maven Central (the binding pulls
      `cassandra-driver-core:3.11.5` on first build).

## Procedure

Run every command from the laptop, from the repo root, with the main checkout
on this branch (`git checkout worktree-cassandra-bench`, or merge it into
`visibility`). `sync.yml` pushes the checked-out tree.

### 1. Push the tree

```bash
ansible-playbook -i bench/ansible/inventory.py bench/ansible/sync.yml       # 8 clients
# amd132 is not in cloudlab.hosts: push the bench scripts by hand.
scp -r bench/scripts oliverr3@amd132.utah.cloudlab.us:ozonedb/bench/
```

### 2. Install Cassandra on `amd132` (once)

```bash
ssh oliverr3@amd132.utah.cloudlab.us 'bash ozonedb/bench/scripts/setup.sh --role cassandra'
```

Expect: JDK 17 installed, tarball unpacked under `/tank/cassandra`,
`cassandra.yaml` bound to `10.10.1.0`, `/tank/cassandra/env.sh` and
`/tank/cassandra/cassandra_ctl.sh` written. The server is not started.

### 3. Compile the binding on every client (before any timed cell)

```bash
ansible -i bench/ansible/inventory.py all -m shell \
  -a "bash -lc 'cd \$OZONEDB_HOME/ycsb && mvn -q -pl site.ycsb:cassandra-binding -am package -DskipTests'"
```

This is the first real compile of `ycsb/cassandra`. If it fails, fix the
binding before anything else. Without this step the first cell on each host
would compile inside the run and start late.

### 4. Load the Cassandra dataset (once)

```bash
bash bench/scripts/local/load_multinode_cassandra.sh --writers 8
```

Wipes `/tank/cassandra/data`, creates keyspace `ycsb` (RF=1, 10 fields),
loads 1M records from the first client with 8 processes, then snapshots to
`/tank/cassandra/data.load` and leaves the server running. Check the loader
result files in `bench/results/local/` on that client: `1KB-1000000-insert-cassandra-quorum_agg_w8_t1.result`
must show `TotalOperations, 1000000`.

### 5. Smoke test (one short cell per system)

```bash
TAG=strict-smoke
bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency serial \
  --workloads a --writers-list 1 --client-hosts "$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | head -n 2 | paste -sd,)" \
  --trial 1 --duration 30 --run-tag $TAG
ssh oliverr3@amd132.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh stop'

bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable \
  --workloads a --writers-list 1 --client-hosts "$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | head -n 2 | paste -sd,)" \
  --trial 1 --duration 30 --run-tag $TAG
```

Pass criteria, in `bench/results/local/strict-smoke/`: one
`*_agg_multinode_w2_t1_trial1.result` per system, two per-writer files each,
no `Missing writer indices` warning in the orchestrator output, and the
per-writer files show zero `*-FAILED` operations. Also `grep lwt bench/results/local/cassandra_client.log`
on a client must show `lwt=true` and a not-applied count.

### 6. Cassandra sweep (25 cells, about 90 min)

```bash
TAG=strict-$(date +%Y%m%d); DUR=120
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -sd,)
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -sd,; }
for point in 2:1 4:1 8:1 8:2 8:4; do          # hosts:writers_per_host -> 2 4 8 16 32 writers
  n=${point%%:*}; w=${point##*:}
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency serial \
    --workloads "a b c d f" --writers-list "$w" --client-hosts "$(hosts_n $n)" \
    --trial 1 --duration $DUR --run-tag $TAG || { echo "FAILED at $point"; break; }
done
```

Each invocation: `restore-load` -> start -> wait -> 5 workloads -> stop; it
leaves the server running at the end, and the next invocation restores again.

### 7. Stop Cassandra

```bash
ssh oliverr3@amd132.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh stop'
```

### 8. OzoneDB sweep (25 cells, about 70 min)

Same loop, same `$TAG`, the corfu wrapper:

```bash
for point in 2:1 4:1 8:1 8:2 8:4; do
  n=${point%%:*}; w=${point##*:}
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable \
    --workloads "a b c d f" --writers-list "$w" --client-hosts "$(hosts_n $n)" \
    --trial 1 --duration $DUR --run-tag $TAG || { echo "FAILED at $point"; break; }
done
```

### 9. Leave the cluster as you found it

The corfu wrapper leaves Corfu running (the usual end state). Stop it if the
next user is the Cassandra sweep.

### 10. Verify and extract

```bash
ls bench/results/local/$TAG/*_agg_multinode_*_trial1.result | wc -l      # expect 50
grep -L "SumWriterThroughput" bench/results/local/$TAG/*_agg_multinode_*  # expect nothing
for f in bench/results/local/$TAG/*_agg_multinode_*_trial1.result; do
  b=$(basename "$f" .result)
  wl=$(echo "$b" | sed -E 's/.*-workload([a-z]+)-.*/\1/')
  label=$(echo "$b" | sed -E 's/.*-workload[a-z]+-(.*)_agg_multinode_.*/\1/')
  n=$(echo "$b" | sed -E 's/.*_agg_multinode_w([0-9]+)_.*/\1/')
  tp=$(grep SumWriterThroughput "$f" | awk -F', ' '{print $3}')
  echo "$label $wl $n $tp"
done | sort -k1,1 -k2,2 -k3,3n > bench/results/local/$TAG/summary.tsv
```

Also check, per cell, the orchestrator log for `Missing writer indices`
(under-reported aggregate) and the per-writer files for `*-FAILED`
operations: an LWT that times out under contention counts as a failure, not
as throughput.

Plot: `bench/scripts/plot/plot_writers_scaling.py` matches
`_agg_w{N}_t{T}.result`; the multinode aggregates are named
`_agg_multinode_w{N}_t{T}_trial{K}.result`. Widen `AGG_RE` to
`_agg(?:_multinode)?_w(\d+)_t\d+(?:_trial\d+)?\.result$` and run it once per
prefix, or feed `summary.tsv` to the campaign plotter. One figure: x = total
writers (log2), y = ops/s, one line per (system, workload), 10 lines, or five
small panels, one per workload, two lines each.

## Time budget

| Step | Estimate |
|---|---|
| 1-4 (sync, install, compile, load) | 30-45 min, once |
| 5 smoke | 5 min |
| 6 Cassandra sweep | 25 cells x (120 s + ~90 s restore/start) ~ 90 min |
| 8 OzoneDB sweep | 25 cells x (120 s + ~40 s restart) ~ 70 min |
| 10 verify/extract | 10 min |

About 3.5 hours end to end. With `DUR=300` (the yaml default) add 2.5 hours.

## What to watch

- **Binding compile (step 3).** Not yet compiled anywhere. The three driver
  calls added over upstream are `getQueryString()`, `setSerialConsistencyLevel()`,
  `wasApplied()`.
- **`cqlsh` on `amd132`.** Cassandra 5.0's `cqlsh` needs a Python 3 that its
  bundled driver supports. `cassandra_ctl.sh wait` and `schema` both use it.
  If it fails, `pip install cqlsh` is the fallback and the ctl script's `cql()`
  helper is the one place to point at it.
- **LWT contention at 16-32 writers.** Zipfian keys plus Paxos per write can
  produce `WriteTimeoutException`s on hot keys. They show up as
  `[UPDATE-FAILED]` / `[INSERT-FAILED]` counts in the per-writer files and as
  the not-applied count in `cassandra_client.log`. Report them; do not fold
  them into throughput.
- **Workload d** inserts new keys during the run; `restore-load` before every
  cell keeps the dataset at 1M for the next one.
- **Corfu tail catch-up plateaus** at 32 writers are background catch-up, not
  deadlock (see the concurrent-writer-stalls memory); a 120 s cell averages
  over them, which is part of why one trial is acceptable here.

## Follow-ups (not in this sweep)

- The relaxed points of the frontier: the same loops with Cassandra
  `--consistency quorum` (== `one` at RF=1) and OzoneDB without
  `--linearizable`, same tag. That completes the two-point-per-system
  frontier figure.
- A thread sweep (`local.run.threads`) at fixed 8 writers, to show the
  server ceiling rather than the per-request latency.
- 3+3 replicated configuration, only with replication on both sides.
