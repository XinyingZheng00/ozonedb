# Plan: strict-mode throughput sweep at 100 GB, OzoneKV against Cassandra

**Status (2026-08-30):** planned, not run. This plan replaces
`bench/PLAN-strict-sweep.md` at a 100 GB dataset with three trials. The 1 GB
run of that plan is written up in `bench/RESULTS-strict-frontier.md`.

## Goal

One throughput-against-writers curve for each (system, workload) pair at the
strict point of the consistency frontier, on a 100 GB dataset:

- OzoneDB on Corfu with `--linearizable`.
- Cassandra with `--consistency serial`. Reads are SERIAL. Every write is an
  `IF [NOT] EXISTS` Paxos round.

Matrix: **5 writer points x 5 workloads x 3 trials x 2 systems = 150 cells.**

| Axis | Values |
|---|---|
| Total writers | 2, 4, 8, 16, 32 |
| Workloads | a, b, c, f, d (in this order) |
| Trials | 1, 2, 3 |
| Systems | `ozonedb-corfu-linearizable`, `cassandra-serial` |

The 1 GB run answered the question at 2 to 14 writers on a 1 GB corpus.
Cassandra won every point. At 100 GB the Cassandra box is disk-bound and its
advantage falls. Campaign `cost2-20260828` measured both systems at 8 writers
on this exact corpus. Those numbers are the expected values below.

## Expected values (8 writers, 100 GB, from `bench/RESULTS-cost.md`)

| System | Workload a | Workload c |
|---|--:|--:|
| OzoneDB native, 100 MiB cache, default reads | 2,756 ops/s | 6,330 ops/s |
| Cassandra serial | 8,277 ops/s | 11,612 ops/s |

At 10 GB the linearizable read mode cost nothing against the default mode
(3,024 against 2,968 on workload a). The fence is cheap next to an S3 GET.
Expect the same at 100 GB for a small writer count. Expect a loss at 32
writers, where the Corfu sequencer serializes every append.

---

## Decisions taken for you

The request named the matrix only. These knobs were set from the measured
history of this cluster. Change them before Task 1 if you disagree.

1. **No reload.** The 100 GB corpora already exist on the cluster (Task 1
   checks them). A reload costs 2 h 15 min for OzoneDB alone.
2. **Cell duration: 300 s for OzoneDB, 180 s for Cassandra.** The reported
   number is a rate, so unequal durations are comparable. OzoneDB needs the
   longer cell because its per-writer block cache must fill first.
3. **Per-writer LRU cache: 100 MiB.** This is the ratio measured at 100 GB in
   `cost2-20260828`, so the new curve joins the existing table.
4. **No disk-cache tier on the main curves.** Task 9 adds an optional parity
   control with a tier.
5. **Log trimming on (`--log-trim`).** Without it a writer replays the whole
   100 GB log before its first fenced read. With it the join costs 612 ms to
   730 ms.
6. **Native Corfu client** (the default). The JNI client costs 1.7x the client
   CPU per operation.
7. **Trial-major order.** Each trial runs both systems end to end. If the
   CloudLab lease ends early, the result is a whole number of trials.
8. **Cassandra snapshots become hard links** (Task 2). A full copy of the
   103 GB data directory does not fit in the 98 GB of free space.

---

## Fixed parameters

| Knob | Value | Where |
|---|---|---|
| Server | `amd197` (`10.10.1.1`), Corfu, MinIO and Cassandra | `nodes.log`, `nodes.store`, `nodes.cassandra` |
| Clients | 8 nodes, `amd189`, `188`, `182`, `183`, `192`, `198`, `181`, `200` | `nodes.clients` |
| Replication | none: Corfu `-s`, Cassandra RF=1 | parity rule 1 |
| Dataset | 100,000,000 records x 1 KB (10 fields x 100 B) | `--record-cnt 100000000` |
| Threads per writer process | 1 | `local.run.threads: [1]` |
| Operation count | unbounded, duration-capped | `local.run.operation_cnt` |
| OzoneDB cache | 100 MiB per writer | `--lru-cache-bytes 104857600` |
| Run tags | `strict100-20260830-oz`, `strict100-20260830-cass` | `--run-tag` |

A writer is one YCSB process with one thread on one client host. The number of
outstanding requests equals the writer count, for both systems.

Writer points spread across hosts before a host takes a second writer:

| Total writers | Client hosts | `--writers-list` (per host) |
|--:|--:|--:|
| 2 | first 2 | 1 |
| 4 | first 4 | 1 |
| 8 | all 8 | 1 |
| 16 | all 8 | 2 |
| 32 | all 8 | 4 |

Each client has 32 cores and 125 GB of RAM. One writer process uses about 0.57
cores. Four writers use 2.3 of 32 cores. The clients are not the limit at any
point of this sweep.

---

## Fairness rules (do not bend)

1. Both systems are un-replicated, on the same server box, with the same
   clients, the same dataset, the same per-process concurrency.
2. Never run Corfu and Cassandra at the same time. They share `amd197`. Corfu
   starts with a 120 GB heap. Stop one before the other starts.
3. The consistency mode is a flag on the wrapper, never a `ycsb.yaml` edit.
4. Read the steady-state rate from `extract_steady_throughput.py`, not
   `Throughput(ops/sec)` from the aggregate file. The aggregate divides by the
   orchestrator wall time, which includes SSH and JVM start.
5. Report failed operations separately. A Paxos write timeout is a failure, not
   throughput.

---

## Cluster state, checked 2026-08-30

| Path on `amd197` | Size | Meaning |
|---|--:|---|
| `/mnt/corfu/load` | 52 MB | trimmed 100 GB Corfu log snapshot |
| `/mnt/corfu/load-bucket` | 114 GB | matching SSTable bucket snapshot |
| `/mnt/corfu/load-10g` | 438 MB | 10 GB log snapshot, not needed here |
| `/mnt/corfu/load-bucket-10g` | 12 GB | 10 GB bucket snapshot, not needed here |
| `/tank/cassandra/data` | 103 GB | live Cassandra data, 100 GB corpus |
| `/tank/cassandra/data.load` | **absent** | the snapshot is missing |
| `/tank/ssd` | 440 GB, 98 GB free | holds all of the above and MinIO |

Only MinIO runs. No Corfu server and no Cassandra daemon are up.

Two consequences drive Task 1 and Task 2:

- The Cassandra load snapshot is gone. `restore-load` fails without it.
- A `cp -a` snapshot needs 103 GB. Only 98 GB is free. Hard links fix this.

---

## Risks

| Risk | Effect | Handling |
|---|---|---|
| CloudLab lease ends mid-campaign | partial matrix | trial-major order (Task 7, Task 8) |
| `/tank/ssd` fills during compaction | Cassandra stops | Task 1 frees 12.4 GB, Task 10 checks free space each trial |
| Another session uses the cluster | both runs corrupt | Task 0 guard, and the guard repeats in every chain |
| Paxos timeouts at 32 writers | throughput looks high | `extract_steady_throughput.py` counts `*-FAILED` operations |
| Corfu sequencer saturates at 32 writers | flat OzoneDB curve | this is a result, not a fault. Record it |
| A writer plateaus for seconds | looks like a hang | this is background catch-up, not a deadlock |

---

## Task 0: check that the cluster is free

- [ ] **Step 1: Look for another session's drivers on this laptop**

```bash
pgrep -af "run_multinode_ycsb|ansible-playbook|chain_" || echo "laptop clear"
```

Expected: `laptop clear`, or only this job's own processes.

- [ ] **Step 2: Look for servers and clients in use**

```bash
SRV=oliverr3@amd197.utah.cloudlab.us
ssh -o BatchMode=yes $SRV 'pgrep -af "[C]assandraDaemon|[C]orfuServer" || echo "server clear"'
python3 bench/scripts/ycsb_config.py --list clients --field ssh | while read -r h; do
  echo -n "$h: "; ssh -o BatchMode=yes -o ConnectTimeout=8 "oliverr3@$h" 'pgrep -af "[y]csb|[j]ava -cp" | head -1 || echo clear'
done
```

Expected: `server clear`, and `clear` on every client.

If any process belongs to another session, stop. Do not kill it.

---

## Task 1: free disk space and check the corpora

**Files:** none. This task runs on the server only.

- [ ] **Step 1: Delete the 10 GB snapshots**

They belong to campaign `cost2-20260828` and this sweep does not use them.

```bash
SRV=oliverr3@amd197.utah.cloudlab.us
ssh -o BatchMode=yes $SRV 'rm -rf /mnt/corfu/load-10g /mnt/corfu/load-bucket-10g; df -h /tank/ssd | tail -1'
```

Expected: about 110 GB free on `/tank/ssd`.

- [ ] **Step 2: Check the OzoneDB corpus**

```bash
ssh -o BatchMode=yes $SRV 'du -sh /mnt/corfu/load /mnt/corfu/load-bucket; mc ls --recursive ozonedb-local/ozonedb-sstables 2>/dev/null | wc -l'
```

Expected: `/mnt/corfu/load` about 52 MB, `/mnt/corfu/load-bucket` about 114 GB.
The bucket snapshot must hold about 288 objects.

If `load-bucket` is absent, the OzoneDB half of this plan cannot run. Reload
with `bash bench/scripts/local/load_corfu_dataset.sh --writers 16 --record-cnt 100000000 --log-trim`.
That load takes 2 h 15 min.

- [ ] **Step 3: Check the Cassandra corpus row count**

```bash
ssh -o BatchMode=yes $SRV '/tank/cassandra/cassandra_ctl.sh start && /tank/cassandra/cassandra_ctl.sh wait'
ssh -o BatchMode=yes $SRV '/tank/cassandra/env.sh 2>/dev/null; nodetool tablestats ycsb.usertable | grep -iE "partitions|space used"'
```

Expected: an estimated partition count near 100,000,000, and a live space near
103 GB. A count below 99,000,000 means the corpus is wrong. If it is wrong,
reload with `bash bench/scripts/local/load_multinode_cassandra.sh --writers 16 --record-cnt 100000000`.

- [ ] **Step 4: Stop the server again**

```bash
ssh -o BatchMode=yes $SRV '/tank/cassandra/cassandra_ctl.sh stop'
```

---

## Task 2: make the Cassandra snapshot cheap

**Files:**
- Modify: `bench/scripts/cassandra_ctl.sh:197-213`

**Why:** `do_save_load` and `do_restore_load` both use `cp -a`. At 103 GB that
copy does not fit in free space, and it costs minutes on every cell. Cassandra
never rewrites an SSTable in place, so hard links are safe. `nodetool snapshot`
uses hard links for the same reason. The commit log, the saved caches and the
hints directory **are** rewritten in place, so those keep a real copy.

**Interfaces:**
- Consumes: `$DATA_DIR` = `$INSTALL_DIR/data`, `$LOAD_DIR` = `$INSTALL_DIR/data.load`.
- Produces: `save-load` and `restore-load` with the same names and the same
  arguments. `run_multinode_ycsb_with_cassandra.sh` calls them unchanged.

- [ ] **Step 1: Replace both function bodies**

Replace lines 197 to 213 of `bench/scripts/cassandra_ctl.sh` with:

```bash
# The snapshot uses hard links. Cassandra never rewrites an SSTable in place:
# a compaction writes new files and unlinks old ones, so the snapshot's link
# keeps the old inode alive and the live tree is free to move on. This is what
# `nodetool snapshot` does. The commit log, the saved caches and the hints ARE
# rewritten in place, so those three get a real copy.
LINKED_EXCLUDE="commitlog saved_caches hints"

relink_volatile() {
  local src="$1" dst="$2" d
  for d in $LINKED_EXCLUDE; do
    [[ -d "$src/$d" ]] || continue
    rm -rf "${dst:?}/$d"
    cp -a "$src/$d" "$dst/$d"
  done
}

do_save_load() {
  do_stop
  [[ -d "$DATA_DIR" ]] || die "save-load: $DATA_DIR does not exist -- nothing was loaded"
  log "snapshot $DATA_DIR -> $LOAD_DIR (hard links)"
  rm -rf "$LOAD_DIR"
  cp -al "$DATA_DIR" "$LOAD_DIR"
  relink_volatile "$DATA_DIR" "$LOAD_DIR"
  # du counts a shared inode once per tree, so this prints the logical size,
  # not the bytes the snapshot added. Read `df` for the bytes.
  du -sh "$LOAD_DIR" | sed 's/^/[cassandra_ctl]   /'
  df -h "$INSTALL_DIR" | tail -1 | sed 's/^/[cassandra_ctl]   /'
}

do_restore_load() {
  do_stop
  [[ -d "$LOAD_DIR" ]] || die "restore-load: $LOAD_DIR missing -- run load_multinode_cassandra.sh first (or pass --no-restore to the sweep)"
  log "restore $LOAD_DIR -> $DATA_DIR (hard links)"
  rm -rf "$DATA_DIR"
  cp -al "$LOAD_DIR" "$DATA_DIR"
  relink_volatile "$LOAD_DIR" "$DATA_DIR"
}
```

- [ ] **Step 2: Check the shell syntax**

```bash
bash -n bench/scripts/cassandra_ctl.sh && echo "syntax ok"
```

Expected: `syntax ok`.

- [ ] **Step 3: Commit**

```bash
git add bench/scripts/cassandra_ctl.sh
git commit -m "cassandra_ctl: snapshot the load with hard links, real-copy the commit log"
```

---

## Task 3: take the Cassandra load snapshot

**Files:** none. This task runs on the server.

- [ ] **Step 1: Push the changed control script**

`run_multinode_ycsb_with_cassandra.sh` pushes `cassandra_ctl.sh` to every
server at start (`push_ctl`). Push it by hand for this task.

```bash
SRV=oliverr3@amd197.utah.cloudlab.us
scp bench/scripts/cassandra_ctl.sh $SRV:/tank/cassandra/cassandra_ctl.sh
```

- [ ] **Step 2: Take the snapshot**

```bash
ssh -o BatchMode=yes $SRV '/tank/cassandra/cassandra_ctl.sh save-load'
```

Expected: `data.load` reports about 103 GB, and `df` still reports about 110 GB
free. If free space fell by 100 GB, the copy did not use hard links. Stop and
fix Task 2.

- [ ] **Step 3: Check that the links are shared**

```bash
ssh -o BatchMode=yes $SRV 'f=$(find /tank/cassandra/data/data -name "*-Data.db" | head -1); g="/tank/cassandra/data.load/${f#/tank/cassandra/data/}"; stat -c "%i %h %n" "$f" "$g"'
```

Expected: two lines with the **same inode number** and a link count of 2.

- [ ] **Step 4: Check that a restore works**

```bash
ssh -o BatchMode=yes $SRV '/tank/cassandra/cassandra_ctl.sh restore-load && /tank/cassandra/cassandra_ctl.sh start && /tank/cassandra/cassandra_ctl.sh wait'
ssh -o BatchMode=yes $SRV 'nodetool tablestats ycsb.usertable | grep -iE "partitions|space used"'
ssh -o BatchMode=yes $SRV '/tank/cassandra/cassandra_ctl.sh stop'
```

Expected: the server starts, and the partition count matches Task 1 Step 3.

---

## Task 4: push the tree to every node

- [ ] **Step 1: Sync the clients**

```bash
export OZONEDB_HOME=$PWD
ansible-playbook -i bench/ansible/inventory.py bench/ansible/sync.yml
```

Expected: 8 hosts changed, 0 failed. `sync.yml` excludes `.git/` and `vcpkg/`,
so it cannot bootstrap a fresh node.

- [ ] **Step 2: Check that no client needs a rebuild**

This sweep changes no C++ code, so the built library on each client stays
valid. Check that one is present.

```bash
python3 bench/scripts/ycsb_config.py --list clients --field ssh | head -1 | while read -r h; do
  ssh -o BatchMode=yes "oliverr3@$h" 'ls -la $OZONEDB_HOME/build/libozonedb.so $OZONEDB_HOME/build/libOzoneDB.so'
done
```

Expected: both files exist.

If the library is missing, run `bash bench/scripts/build.sh` on every client
through Ansible before Task 5.

---

## Task 5: smoke test, one short cell per system

**Why:** the smoke test proves the result labels, the restore path and the
extractor before 15 hours of cells run.

- [ ] **Step 1: Set the shared variables**

```bash
export OZONEDB_HOME=$PWD
TAG=strict100-smoke
SRV=oliverr3@amd197.utah.cloudlab.us
RC=100000000
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
H2=$(echo "$HOSTS" | tr ',' '\n' | head -n 2 | paste -s -d, -)
```

- [ ] **Step 2: One Cassandra cell**

```bash
bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency serial \
  --record-cnt $RC --workloads c --writers-list 1 --client-hosts "$H2" \
  --trial 1 --duration 60 --run-tag $TAG
ssh -o BatchMode=yes $SRV '/tank/cassandra/cassandra_ctl.sh stop'
```

- [ ] **Step 3: One OzoneDB cell**

```bash
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable --log-trim \
  --record-cnt $RC --lru-cache-bytes 104857600 --workloads c --writers-list 1 \
  --client-hosts "$H2" --trial 1 --duration 120 --run-tag $TAG
ssh -o BatchMode=yes $SRV "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true"
```

- [ ] **Step 4: Check the output**

```bash
ls bench/results/local/$TAG/
grep -c "" bench/results/local/$TAG/*_agg_multinode_w2_t1_trial1.result
grep -h "FAILED" bench/results/local/$TAG/*_w*of*_t1_trial1.result || echo "no failed operations"
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG --window 30
```

Pass criteria, all four:

1. Two `*_agg_multinode_w2_t1_trial1.result` files, one per system.
2. Four per-writer files, two per system.
3. `no failed operations`.
4. The extractor prints two rows with a non-zero rate.

- [ ] **Step 5: Record the exact engine labels**

```bash
ls bench/results/local/$TAG/ | sed -E 's/.*-workload[a-z]+-(.*)_agg_multinode_.*/\1/' | sort -u
```

Write the two labels into `bench/RESULTS-strict-100g.md` later. Do not guess
them. The OzoneDB label carries both the `-native` token and the
`-linearizable` token, and the exact order comes from this command.

---

## Task 6: write the trial chain script

**Files:**
- Create: `bench/scripts/campaign-strict100/chain_trial.sh`
- Create: `bench/scripts/campaign-strict100/README.md`

**Why:** one trial takes about 5.2 hours. The chain runs detached, so the
harness cannot kill it when a turn ends.

**Interfaces:**
- Consumes: `run_multinode_ycsb_with_cassandra.sh`, `run_multinode_ycsb_with_corfu.sh`.
- Produces: results under `bench/results/local/strict100-20260830-cass` and
  `bench/results/local/strict100-20260830-oz`, on every client and on this laptop.

- [ ] **Step 1: Write the chain**

```bash
mkdir -p bench/scripts/campaign-strict100
cat > bench/scripts/campaign-strict100/chain_trial.sh <<'CHAIN'
#!/bin/bash
# PLAN-strict-100g: one trial of the strict frontier at 100 GB.
# Usage: chain_trial.sh <trial number>. Runs detached on the laptop.
# Cassandra runs first, then OzoneDB. They share the server and must never
# run at the same time.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
T=${1:?trial number (1, 2 or 3)}

export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-strict-100g
cd "$OZONEDB_HOME"

TAG=strict100-20260830
SRV=oliverr3@amd197.utah.cloudlab.us
RC=100000000
CACHE=104857600            # 100 MiB per writer
WORKLOADS="a b c f d"      # d last: it is the only workload that inserts keys
POINTS="2:1 4:1 8:1 8:2 8:4"   # hosts:writers_per_host -> 2 4 8 16 32 writers
CASS=bench/scripts/local/run_multinode_ycsb_with_cassandra.sh
OZ=bench/scripts/local/run_multinode_ycsb_with_corfu.sh

HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -s -d, -; }
step() { echo "[chain $(date '+%F %T')] $*"; }

step "trial $T: guard -- nothing else may hold the server"
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh stop >/dev/null 2>&1 || true; pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 2; if pgrep -af "[C]assandraDaemon|[C]orfuServer"; then echo "server busy"; exit 1; fi; echo "server clear"; df -h /tank/ssd | tail -1'

step "trial $T: Cassandra serial, 25 cells, 180 s each"
for point in $POINTS; do
  n=${point%%:*}; w=${point##*:}
  step "  cassandra point ${n}x${w}"
  bash $CASS --consistency serial --record-cnt $RC \
    --workloads "$WORKLOADS" --writers-list "$w" --client-hosts "$(hosts_n "$n")" \
    --trial "$T" --duration 180 --run-tag $TAG-cass
done
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh stop; df -h /tank/ssd | tail -1'

step "trial $T: OzoneDB linearizable, 25 cells, 300 s each"
for point in $POINTS; do
  n=${point%%:*}; w=${point##*:}
  step "  ozonedb point ${n}x${w}"
  bash $OZ --linearizable --log-trim --record-cnt $RC --lru-cache-bytes $CACHE \
    --workloads "$WORKLOADS" --writers-list "$w" --client-hosts "$(hosts_n "$n")" \
    --trial "$T" --duration 300 --run-tag $TAG-oz
done
ssh -o BatchMode=yes "$SRV" "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true; df -h /tank/ssd | tail -1"

step "CHAIN-DONE trial $T"
CHAIN
chmod +x bench/scripts/campaign-strict100/chain_trial.sh
bash -n bench/scripts/campaign-strict100/chain_trial.sh && echo "syntax ok"
```

Expected: `syntax ok`.

- [ ] **Step 2: Write the README**

```bash
cat > bench/scripts/campaign-strict100/README.md <<'DOC'
# Campaign strict100-20260830 (bench/PLAN-strict-100g.md)

One trial per invocation. Cassandra first, then OzoneDB, because they share
the server box. Edit OZONEDB_HOME at the top before a rerun.

Launch detached from the laptop (macOS has no setsid):

    nohup bash bench/scripts/campaign-strict100/chain_trial.sh 1 \
      > bench/results/local/strict100-chains/trial1.log 2>&1 < /dev/null & disown

Watch it: tail -f bench/results/local/strict100-chains/trial1.log
Done when the log holds CHAIN-DONE. Failed when it holds CHAIN-FAILED.
DOC
mkdir -p bench/results/local/strict100-chains
```

- [ ] **Step 3: Check the plan the chain will run, without running it**

Add `--dry-run` by hand to one point and read the printed iteration plan.

```bash
export OZONEDB_HOME=$PWD
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable --log-trim \
  --record-cnt 100000000 --lru-cache-bytes 104857600 --workloads "a b c f d" \
  --writers-list 4 --client-hosts "$HOSTS" --trial 1 --duration 300 \
  --run-tag strict100-dry --dry-run
```

Expected: 5 dry-run lines, one per workload, each with `writers_per_host=4`,
and a header line with `read_mode=linearizable log_trim=1 record_cnt=100000000`.

- [ ] **Step 4: Commit**

```bash
git add bench/scripts/campaign-strict100 bench/PLAN-strict-100g.md
git commit -m "bench: strict frontier at 100 GB -- plan and trial chain"
```

---

## Task 7: run trial 1

- [ ] **Step 1: Launch the chain detached**

```bash
nohup bash bench/scripts/campaign-strict100/chain_trial.sh 1 \
  > bench/results/local/strict100-chains/trial1.log 2>&1 < /dev/null & disown
```

- [ ] **Step 2: Watch the first two cells**

```bash
tail -f bench/results/local/strict100-chains/trial1.log
```

Expected within 15 minutes: a `server clear` line, a `cassandra point 2x1`
line, and a `[bench] trial=1 workload=a writers_per_host=1 rc=0` line.

If the first cell fails, stop the chain and read the failure before a rerun.

- [ ] **Step 3: Check the trial when the log holds `CHAIN-DONE`**

```bash
TAG=strict100-20260830
ls bench/results/local/$TAG-cass/*_agg_multinode_*_trial1.result | wc -l   # expect 25
ls bench/results/local/$TAG-oz/*_agg_multinode_*_trial1.result   | wc -l   # expect 25
grep -L "SumWriterThroughput" bench/results/local/$TAG-*/*_agg_multinode_*_trial1.result
grep -l "Missing writer indices" bench/results/local/strict100-chains/trial1.log
```

Expected: 25 and 25, no file without `SumWriterThroughput`, and no
`Missing writer indices` line.

- [ ] **Step 4: Read the trial-1 numbers before 10 more hours run**

```bash
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG-cass --window 60
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG-oz  --window 120
```

Compare the 8-writer rows against the expected values at the top of this plan.
If workload a on OzoneDB is far from 2,756 ops/s, or Cassandra serial is far
from 8,277 ops/s, stop. Find the cause before trial 2.

---

## Task 8: run trials 2 and 3

- [ ] **Step 1: Run trial 2**

```bash
nohup bash bench/scripts/campaign-strict100/chain_trial.sh 2 \
  > bench/results/local/strict100-chains/trial2.log 2>&1 < /dev/null & disown
```

Wait for `CHAIN-DONE`. Repeat Task 7 Step 3 with `trial2`.

- [ ] **Step 2: Run trial 3**

```bash
nohup bash bench/scripts/campaign-strict100/chain_trial.sh 3 \
  > bench/results/local/strict100-chains/trial3.log 2>&1 < /dev/null & disown
```

Wait for `CHAIN-DONE`. Repeat Task 7 Step 3 with `trial3`.

- [ ] **Step 3: Check the full matrix**

```bash
TAG=strict100-20260830
ls bench/results/local/$TAG-cass/*_agg_multinode_*.result | wc -l    # expect 75
ls bench/results/local/$TAG-oz/*_agg_multinode_*.result   | wc -l    # expect 75
```

Expected: 75 and 75, which is 150 cells.

---

## Task 9 (optional): client-storage parity control

**Why:** the main curves give OzoneDB 100 MiB of client RAM per writer, and
give Cassandra a 31 GB heap plus the page cache of a 125 GB server. A reader
can call that unfair. This control gives each OzoneDB writer local storage
instead, which is the deployment the cost model recommends.

Run it only after Task 8. It adds 5 cells and about 1 hour.

- [ ] **Step 1: Run the parity cells at 8 writers**

```bash
export OZONEDB_HOME=$PWD
TAG=strict100-20260830
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable --log-trim \
  --record-cnt 100000000 --lru-cache-bytes 838860800 \
  --disk-cache-bytes 53687091200 --disk-cache-dir /tank/cache \
  --workloads "a b c f d" --writers-list 1 --client-hosts "$HOSTS" \
  --trial 1 --duration 2700 --run-tag $TAG-oz-tier
```

The 2700 s duration is the value campaign `cost2-20260828` needed for a 50 GiB
tier to fill at about 30 MB/s per writer.

- [ ] **Step 2: Extract**

```bash
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG-oz-tier --window 300
```

Expected: workload c near 10,088 ops/s, the 50 GiB value measured at 100 GB.

---

## Task 10: extract, check and write up

**Files:**
- Create: `bench/results-strict100-20260830.tsv`
- Create: `bench/RESULTS-strict-100g.md`

- [ ] **Step 1: Build one TSV**

```bash
TAG=strict100-20260830
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG-cass \
  --window 60  --tsv bench/results-strict100-20260830-cass.tsv
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG-oz \
  --window 120 --tsv bench/results-strict100-20260830-oz.tsv
head -1 bench/results-strict100-20260830-cass.tsv > bench/results-strict100-20260830.tsv
tail -q -n +2 bench/results-strict100-20260830-cass.tsv bench/results-strict100-20260830-oz.tsv \
  >> bench/results-strict100-20260830.tsv
wc -l bench/results-strict100-20260830.tsv
```

Expected: 151 lines, which is one header and 150 cells.

The TSV columns are `label`, `workload`, `writers`, `have`, `steady_ops_per_sec`,
`failed`, `run_seconds`. There is **no trial column**: the extractor keys a cell
by trial but does not write the trial out. Three trials therefore give three
rows with the same first three columns. Step 3 groups on those three columns and
recovers the three values. Check that the `have` column equals the `writers`
column on every row. A smaller `have` means a lost writer file, and that row
under-reports the cell.

- [ ] **Step 2: Count failed operations**

```bash
grep -h "FAILED" bench/results/local/$TAG-*/*_w*of*_t1_trial*.result | sort | uniq -c | sort -rn | head -20
```

A Paxos timeout at 16 or 32 writers is a real result. Record the count per
cell in the write-up. Never fold a failed operation into throughput.

- [ ] **Step 3: Check the trial spread**

Three trials exist to show the noise. Compute the spread per cell.

```bash
python3 - <<'PY'
import csv, collections, statistics
rows = list(csv.DictReader(open("bench/results-strict100-20260830.tsv"), delimiter="\t"))
key = lambda r: (r["label"], r["workload"], r["writers"])
g = collections.defaultdict(list)
for r in rows:
    g[key(r)].append(float(r["steady_ops_per_sec"]))
worst = sorted(((max(v) - min(v)) / statistics.mean(v), k, v) for k, v in g.items() if len(v) > 1)
for spread, k, v in worst[-10:]:
    print(f"{spread:6.1%}  {k}  {[round(x) for x in v]}")
PY
```

A spread above 15 % on any cell needs a note in the write-up.

- [ ] **Step 4: Plot**

`bench/scripts/plot/plot_writers_scaling.py` matches `_agg_w{N}_t{T}.result`.
The multi-node files are named `_agg_multinode_w{N}_t{T}_trial{K}.result`.
Feed the TSV to the campaign plotter instead, or widen `AGG_RE` to:

```python
AGG_RE = re.compile(r"_agg(?:_multinode)?_w(\d+)_t\d+(?:_trial\d+)?\.result$")
```

One figure, five panels, one per workload. Each panel: x is the total writer
count on a log-2 axis, y is steady ops/s, two lines, and an error bar over the
three trials.

- [ ] **Step 5: Write `bench/RESULTS-strict-100g.md`**

Follow the shape of `bench/RESULTS-strict-frontier.md`. It must hold:

1. The date, the cluster, the dataset, the replication factor, the read modes.
2. The exact engine labels from Task 5 Step 5.
3. The steady-state table: workload, writers, both systems, and the ratio.
4. The failed-operation count per cell, or a line that says zero everywhere.
5. The trial spread from Step 3.
6. A comparison against the 1 GB run in `bench/RESULTS-strict-frontier.md`.
7. The reproduce block: the chain command and the two extractor commands.

- [ ] **Step 6: Commit**

```bash
git add bench/results-strict100-*.tsv bench/RESULTS-strict-100g.md bench/*.png bench/*.pdf
git commit -m "bench: strict frontier at 100 GB -- 150 cells, 3 trials, write-up"
```

---

## Time budget

| Step | Estimate |
|---|---|
| Task 0 to Task 4: guard, disk, snapshot, sync | 45 min |
| Task 5: smoke | 15 min |
| Task 6: chain scripts and dry run | 20 min |
| Task 7: trial 1 | 5 h 15 min |
| Task 8: trials 2 and 3 | 10 h 30 min |
| Task 9: optional parity control | 1 h |
| Task 10: extract, plot, write up | 1 h |

**Total: about 18 hours,** or 17 hours without Task 9.

Per-cell arithmetic behind the trial estimate:

- Cassandra: 25 cells x (180 s run + 60 s restore and start + 40 s launch) = 1 h 55 min.
- OzoneDB: 25 cells x (300 s run + 90 s bucket mirror + 40 s start + 40 s launch) = 3 h 20 min.

## Levers if 18 hours is too long

Apply these in order. Each one keeps the matrix shape.

1. **Drop to 2 trials.** Saves 5 h 15 min. The spread from two trials is weaker
   but still reportable.
2. **Cut the OzoneDB cell to 240 s and the window to 90 s.** Saves 1 h 15 min.
   The 100 MiB cache fills in about 55 s, so 240 s still holds a steady window.
3. **Drop workload b.** Workload b sits between a and c and adds little. Saves
   3 h.

Do not cut the writer points. The shape of the curve is the result.

## Follow-ups (not in this sweep)

- The relaxed points of the frontier: Cassandra `--consistency quorum` and
  OzoneDB without `--linearizable`, same matrix. That completes a
  two-point-per-system frontier figure.
- A thread sweep at a fixed writer count, to find the server ceiling rather
  than the per-request latency.
- A 3+3 replicated configuration, with replication on both sides.
- Elasticity: the time for a new writer to join and reach steady state. The
  trimmed join is 612 ms to 730 ms at 100 GB, and Cassandra has no equivalent.
