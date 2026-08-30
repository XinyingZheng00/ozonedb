# Plan: strict-mode throughput sweep at 100 GB, OzoneKV against Cassandra

**Status (2026-08-30):** planned, not run. This plan replaces
`bench/PLAN-strict-sweep.md` at a 100 GB dataset with two trials. The 1 GB
run of that plan is written up in `bench/RESULTS-strict-frontier.md`.

## Goal

One throughput-against-writers curve for each (system, workload) pair at the
strict point of the consistency frontier, on a 100 GB dataset:

- OzoneDB on Corfu with `--linearizable`, a 16 GiB per-writer block cache and a
  50 GiB per-writer disk-cache tier.
- Cassandra with `--consistency serial`. Reads are SERIAL. Every write is an
  `IF [NOT] EXISTS` Paxos round.

Matrix: **5 writer points x 5 workloads x 2 trials x 2 systems = 100 cells.**

| Axis | Values |
|---|---|
| Total writers | 2, 4, 8, 16, 32 |
| Workloads | a, b, c, f, d (in this order) |
| Trials | 1, 2 |
| Systems | `ozonedb-corfu-linearizable`, `cassandra-serial` |

Both systems get **300 s cells**. The reported number is a steady-state rate
over the last 120 s of each writer.

---

## Read this first: what a 300 s cell measures

Every cell starts both OzoneDB caches **empty**, and they cannot fill in 300 s.
This is not a defect of the plan. It is a property of the configuration, and the
write-up must state it.

**Why the caches start empty.** Each cell is a fresh YCSB process, so the
in-process LRU block cache starts at zero bytes.
`load_local_ycsb_multiproc.py:543` calls `shutil.rmtree` on each writer's tier
directory before the cell, so the disk tier starts at zero bytes too. Chunk mode
also starts cold by design: a leftover sparse file does not record which chunks
hold data.

**How far they get in 300 s.** Campaign `cost2-20260828` measured the fill rates
at 100 GB, per writer.

| Cache | Fill rate | Reached in 300 s | Fraction of budget | Time to fill |
|---|--:|--:|--:|--:|
| LRU block cache, 16 GiB budget | 1.9 MB/s | about 570 MB | 3 % | 2 h 30 min |
| Disk tier, 50 GiB budget | 30 MB/s | about 9 GB | 17 % | 30 min |

So the sweep measures OzoneDB **in its cache-fill phase**, not at the steady
state that the 16 GiB and 50 GiB budgets describe. Three consequences:

1. The budgets are not the binding constraint at 300 s. The fill rate is. A
   32 GiB tier and a 50 GiB tier give the same 300 s number.
2. The numbers will land between the no-tier and the warm-tier values of
   `bench/RESULTS-cost.md`, closer to the no-tier end.
3. Cassandra is measured the same way. It restarts from the restored data
   directory each cell, so its page cache is also cold.

**This is a real and reportable regime.** It is the elastic case that OzoneDB
claims: a writer joins by opening the log and starts serving at once. Report it
under that name, and say plainly that the numbers are not warm-cache numbers.

The warm point is **out of scope for this sweep** by decision. It costs 1 h
45 min for two long cells at 8 writers. The Follow-ups section holds the exact
command for a later run.

---

## Expected values (8 writers, 100 GB, from `bench/RESULTS-cost.md`)

| Configuration | Workload a | Workload c |
|---|--:|--:|
| Cassandra serial | 8,277 ops/s | 11,612 ops/s |
| OzoneDB, 100 MiB cache, no tier, default reads | 2,756 ops/s | 6,330 ops/s |
| OzoneDB, 800 MiB cache, no tier | — | 6,872 ops/s |
| OzoneDB, 800 MiB cache, **warm** 50 GiB tier | 2,800 ops/s | 10,088 ops/s |

Expect the 300 s cells of this sweep to sit between the second row and the
fourth, nearer the second. The fourth row is a warm-cache value and no cell of
this sweep reaches it.

At 10 GB the linearizable read mode cost nothing against the default mode
(3,024 against 2,968 on workload a), because the fence is cheap next to an S3
GET. Expect a loss at 32 writers, where the Corfu sequencer serializes every
append.

---

## Decisions taken for you

The request named the matrix, the cell duration, the trial count, the tier and
the cache size. These remaining knobs come from the measured history of this
cluster.

1. **No reload.** The 100 GB corpora already exist on the cluster (Task 1
   checks them). A reload costs 2 h 15 min for OzoneDB alone.
2. **Tier size: 50 GiB per writer, chunk mode, TinyLFU admission** (the
   defaults). 50 GiB is the size measured at 100 GB in `cost2-20260828`, so a
   later warm run joins the existing table. At 32 writers this is 4 writers x
   50 GiB = 200 GiB per host, inside the 428 GB free on `/tank/cache`.
3. **LRU cache: 16 GiB per writer**, as requested. At 32 writers this is
   4 x 16 GiB = 64 GiB of a client's 125 GB. The cache fills on demand, so the
   300 s cells never reach that.
4. **Steady window: the last 120 s of a 300 s cell.** Both systems use the same
   duration, so one extractor call with one window covers the campaign.
5. **Log trimming on (`--log-trim`).** Without it a writer replays the whole
   100 GB log before its first fenced read. With it the join costs 612 ms to
   730 ms.
6. **Native Corfu client** (the default). The JNI client costs 1.7x the client
   CPU per operation.
7. **One run tag.**
8. **Trial-major order.** Each trial runs both systems end to end. If the
   CloudLab lease ends early, the result is a whole number of trials.
9. **Cassandra snapshots become hard links** (Task 2). A full copy of the
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
| Cell duration, both systems | 300 s | `--duration 300` |
| Steady window | last 120 s | `--window 120` |
| Trials | 1 and 2 | `--trial N`, one per chain run |
| OzoneDB LRU cache | 16 GiB per writer | `--lru-cache-bytes 17179869184` |
| OzoneDB disk tier | 50 GiB per writer on `/tank/cache` | `--disk-cache-bytes 53687091200 --disk-cache-dir /tank/cache` |
| Tier mode | chunk, 64 KiB entries, TinyLFU admission | the defaults, written into every label |
| Run tag | `strict100-20260830` | `--run-tag` |

A writer is one YCSB process with one thread on one client host. The number of
outstanding requests equals the writer count, for both systems.

Writer points spread across hosts before a host takes a second writer:

| Total writers | Client hosts | `--writers-list` (per host) | Tier per host | LRU per host |
|--:|--:|--:|--:|--:|
| 2 | first 2 | 1 | 50 GiB | 16 GiB |
| 4 | first 4 | 1 | 50 GiB | 16 GiB |
| 8 | all 8 | 1 | 50 GiB | 16 GiB |
| 16 | all 8 | 2 | 100 GiB | 32 GiB |
| 32 | all 8 | 4 | 200 GiB | 64 GiB |

Each client has 32 cores, 125 GB of RAM and 428 GB free on `/tank/cache`. One
writer process uses about 0.57 cores. Four writers use 2.3 of 32 cores. The
clients are not the limit at any point of this sweep.

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
6. Report the cache-fill state with every OzoneDB number. See the section
   "Read this first".

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

On client `amd189`: 32 cores, 125 GB of RAM, `/tank/cache` 440 GB with 428 GB
free. Only MinIO runs on the server. No Corfu server and no Cassandra daemon
are up.

Two facts drive Task 1 and Task 2:

- The Cassandra load snapshot is gone. `restore-load` fails without it.
- A `cp -a` snapshot needs 103 GB. Only 98 GB is free. Hard links fix this.

---

## Risks

| Risk | Effect | Handling |
|---|---|---|
| Caches never fill in 300 s | OzoneDB looks slow | stated up front, and the write-up must name the regime |
| Two trials give a weak spread | noise looks like signal | Task 9 Step 3 prints the spread per cell. Flag any cell above 15 % |
| CloudLab lease ends mid-campaign | partial matrix | trial-major order (Task 7, Task 8) |
| `/tank/ssd` fills during compaction | Cassandra stops | Task 1 frees 12.4 GB, the chain prints `df` each trial |
| `/tank/cache` fills at 32 writers | tier writes fail | 200 GiB of 428 GB free, Task 1 Step 5 checks it |
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

**Files:** none. This task runs on the cluster only.

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
ssh -o BatchMode=yes $SRV 'nodetool tablestats ycsb.usertable | grep -iE "partitions|space used"'
```

Expected: an estimated partition count near 100,000,000, and a live space near
103 GB. A count below 99,000,000 means the corpus is wrong. If it is wrong,
reload with `bash bench/scripts/local/load_multinode_cassandra.sh --writers 16 --record-cnt 100000000`.

- [ ] **Step 4: Stop the server again**

```bash
ssh -o BatchMode=yes $SRV '/tank/cassandra/cassandra_ctl.sh stop'
```

- [ ] **Step 5: Check tier space and RAM on every client**

The 32-writer point puts 4 writers on each host. Each writer takes up to 50 GiB
of `/tank/cache` and up to 16 GiB of RAM.

```bash
python3 bench/scripts/ycsb_config.py --list clients --field ssh | while read -r h; do
  echo -n "$h: "; ssh -o BatchMode=yes -o ConnectTimeout=8 "oliverr3@$h" \
    'df -h --output=avail /tank/cache | tail -1 | tr -d "\n"; echo -n " cache, "; free -g | awk "/^Mem:/ {print \$7\" GB RAM available\"}"'
done
```

Expected on every host: at least 210 GB free on `/tank/cache`, and at least
70 GB of available RAM. If a host reports less, clear stale tier directories
with `rm -rf /tank/cache/w*` on that host.

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

**Why:** the smoke test proves the result labels, the restore path, the tier
path and the extractor before 12 hours of cells run.

- [ ] **Step 1: Set the shared variables**

```bash
export OZONEDB_HOME=$PWD
TAG=strict100-smoke
SRV=oliverr3@amd197.utah.cloudlab.us
RC=100000000
CACHE=17179869184     # 16 GiB
TIER=53687091200      # 50 GiB
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
  --record-cnt $RC --lru-cache-bytes $CACHE \
  --disk-cache-bytes $TIER --disk-cache-dir /tank/cache \
  --workloads c --writers-list 1 --client-hosts "$H2" \
  --trial 1 --duration 120 --run-tag $TAG
ssh -o BatchMode=yes $SRV "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true"
```

- [ ] **Step 4: Check the output**

```bash
ls bench/results/local/$TAG/
grep -h "FAILED" bench/results/local/$TAG/*_w*of*_t1_trial1.result || echo "no failed operations"
grep -h "disk_cache" bench/results/local/$TAG/*_w*of*_t1_trial1.result
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG --window 30
```

Pass criteria, all five:

1. Two `*_agg_multinode_w2_t1_trial1.result` files, one per system.
2. Four per-writer files, two per system.
3. `no failed operations`.
4. A `[disk_cache] hits=... misses=... fills=...` line in each OzoneDB writer
   file, with a non-zero `fills` count. A zero count means the tier is off.
5. The extractor prints two rows with a non-zero rate.

- [ ] **Step 5: Record the exact engine labels**

```bash
ls bench/results/local/$TAG/ | sed -E 's/.*-workload[a-z]+-(.*)_agg_multinode_.*/\1/' | sort -u
```

Write the two labels into `bench/RESULTS-strict-100g.md` later. Do not guess
them. The OzoneDB label carries the `-native` token, the `-linearizable` token
and a tier token of the form `-dc50g-ch64k-adm`. The exact order comes from
this command.

- [ ] **Step 6: Check that the tier directory was wiped and rebuilt**

```bash
ssh -o BatchMode=yes "oliverr3@$(echo "$H2" | cut -d, -f1)" 'du -sh /tank/cache/w* 2>/dev/null; df -h /tank/cache | tail -1'
```

Expected: one `w0` directory holding a few GB. The runner wipes it before every
cell, so it never grows across the campaign.

---

## Task 6: write the trial chain script

**Files:**
- Create: `bench/scripts/campaign-strict100/chain_trial.sh`
- Create: `bench/scripts/campaign-strict100/README.md`

**Why:** one trial takes about 6 hours. The chain runs detached, so the
harness cannot kill it when a turn ends.

**Interfaces:**
- Consumes: `run_multinode_ycsb_with_cassandra.sh`, `run_multinode_ycsb_with_corfu.sh`.
- Produces: results under `bench/results/local/strict100-20260830`, on every
  client and on this laptop.

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
T=${1:?trial number (1 or 2)}

export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-strict-100g
cd "$OZONEDB_HOME"

TAG=strict100-20260830
SRV=oliverr3@amd197.utah.cloudlab.us
RC=100000000
DUR=300
CACHE=17179869184          # 16 GiB LRU block cache per writer
TIER=53687091200           # 50 GiB disk-cache tier per writer
WORKLOADS="a b c f d"      # d last: it is the only workload that inserts keys
POINTS="2:1 4:1 8:1 8:2 8:4"   # hosts:writers_per_host -> 2 4 8 16 32 writers
CASS=bench/scripts/local/run_multinode_ycsb_with_cassandra.sh
OZ=bench/scripts/local/run_multinode_ycsb_with_corfu.sh

HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -s -d, -; }
step() { echo "[chain $(date '+%F %T')] $*"; }

step "trial $T: guard -- nothing else may hold the server"
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh stop >/dev/null 2>&1 || true; pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 2; if pgrep -af "[C]assandraDaemon|[C]orfuServer"; then echo "server busy"; exit 1; fi; echo "server clear"; df -h /tank/ssd | tail -1'

step "trial $T: Cassandra serial, 25 cells, ${DUR} s each"
for point in $POINTS; do
  n=${point%%:*}; w=${point##*:}
  step "  cassandra point ${n}x${w}"
  bash $CASS --consistency serial --record-cnt $RC \
    --workloads "$WORKLOADS" --writers-list "$w" --client-hosts "$(hosts_n "$n")" \
    --trial "$T" --duration $DUR --run-tag $TAG
done
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh stop; df -h /tank/ssd | tail -1'

step "trial $T: OzoneDB linearizable, 16 GiB cache + 50 GiB tier, 25 cells, ${DUR} s each"
for point in $POINTS; do
  n=${point%%:*}; w=${point##*:}
  step "  ozonedb point ${n}x${w}"
  bash $OZ --linearizable --log-trim --record-cnt $RC \
    --lru-cache-bytes $CACHE --disk-cache-bytes $TIER --disk-cache-dir /tank/cache \
    --workloads "$WORKLOADS" --writers-list "$w" --client-hosts "$(hosts_n "$n")" \
    --trial "$T" --duration $DUR --run-tag $TAG
done
ssh -o BatchMode=yes "$SRV" "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true; df -h /tank/ssd | tail -1"

step "trial $T: client tier space after the run"
for h in $(echo "$HOSTS" | tr ',' ' '); do
  echo -n "$h: "
  ssh -o BatchMode=yes -o ConnectTimeout=8 "oliverr3@$h" 'df -h --output=avail /tank/cache | tail -1'
done

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

One trial per invocation, two trials in all. Cassandra first, then OzoneDB,
because they share the server box. Edit OZONEDB_HOME at the top before a rerun.

Every cell is 300 s and starts both OzoneDB caches empty. The 16 GiB LRU
reaches about 3 % and the 50 GiB tier about 17 % by the end of a cell. That is
deliberate. Read the "Read this first" section of the plan before you read the
numbers.

Launch detached from the laptop (macOS has no setsid):

    nohup bash bench/scripts/campaign-strict100/chain_trial.sh 1 \
      > bench/results/local/strict100-chains/trial1.log 2>&1 < /dev/null & disown

Watch it: tail -f bench/results/local/strict100-chains/trial1.log
Done when the log holds CHAIN-DONE. Failed when it holds CHAIN-FAILED.
DOC
mkdir -p bench/results/local/strict100-chains
```

- [ ] **Step 3: Check the plan the chain will run, without running it**

```bash
export OZONEDB_HOME=$PWD
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable --log-trim \
  --record-cnt 100000000 --lru-cache-bytes 17179869184 \
  --disk-cache-bytes 53687091200 --disk-cache-dir /tank/cache \
  --workloads "a b c f d" --writers-list 4 --client-hosts "$HOSTS" --trial 1 \
  --duration 300 --run-tag strict100-dry --dry-run
```

Expected: 5 dry-run lines, one per workload, each with `writers_per_host=4`,
and a header line with `read_mode=linearizable log_trim=1
lru_cache_bytes=17179869184 disk_cache_bytes=53687091200 disk_cache_mode=chunk
disk_cache_admission=frequency record_cnt=100000000 duration=300`.

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
ls bench/results/local/$TAG/*_agg_multinode_*_trial1.result | wc -l    # expect 50
grep -L "SumWriterThroughput" bench/results/local/$TAG/*_agg_multinode_*_trial1.result
grep -c "Missing writer indices" bench/results/local/strict100-chains/trial1.log
```

Expected: 50, which is 25 cells per system. No file without
`SumWriterThroughput`. A `Missing writer indices` count of 0.

- [ ] **Step 4: Read the trial-1 numbers before trial 2 runs**

```bash
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG --window 120
```

Compare the 8-writer rows against the expected values at the top of this plan.
Cassandra serial must land near 8,277 ops/s on workload a. OzoneDB must land
between the no-tier row and the warm-tier row, because the caches are 3 % and
17 % full. If either system is far outside that, stop. Find the cause before
trial 2. With only two trials there is no third run to outvote a bad one.

- [ ] **Step 5: Record the tier hit rate of trial 1**

```bash
grep -h "\[disk_cache\]" bench/results/local/$TAG/*_w*of*_t1_trial1.result | head -5
```

Expected: a modest `hits` count against `misses`, and a `fills` count near
137,000, which is 9 GB divided by the 64 KiB entry size. Record the ratio in
the write-up. It is the direct evidence for the cache-fill statement.

---

## Task 8: run trial 2

- [ ] **Step 1: Run trial 2**

```bash
nohup bash bench/scripts/campaign-strict100/chain_trial.sh 2 \
  > bench/results/local/strict100-chains/trial2.log 2>&1 < /dev/null & disown
```

Wait for `CHAIN-DONE`. Repeat Task 7 Step 3 with `trial2`.

- [ ] **Step 2: Check the full matrix**

```bash
TAG=strict100-20260830
ls bench/results/local/$TAG/*_agg_multinode_*.result | wc -l    # expect 100
```

Expected: 100 cells.

---

## Task 9: extract, check and write up

**Files:**
- Create: `bench/results-strict100-20260830.tsv`
- Create: `bench/RESULTS-strict-100g.md`

- [ ] **Step 1: Build the TSV**

```bash
TAG=strict100-20260830
python3 bench/scripts/extract_steady_throughput.py bench/results/local/$TAG \
  --window 120 --tsv bench/results-strict100-20260830.tsv
wc -l bench/results-strict100-20260830.tsv
```

Expected: 101 lines, which is one header and 100 cells.

The TSV columns are `label`, `workload`, `writers`, `have`, `steady_ops_per_sec`,
`failed`, `run_seconds`. There is **no trial column**: the extractor keys a cell
by trial but does not write the trial out. Two trials therefore give two rows
with the same first three columns. Step 3 groups on those three columns and
recovers the two values. Check that the `have` column equals the `writers`
column on every row. A smaller `have` means a lost writer file, and that row
under-reports the cell.

- [ ] **Step 2: Count failed operations**

```bash
grep -h "FAILED" bench/results/local/$TAG/*_w*of*_t1_trial*.result | sort | uniq -c | sort -rn | head -20
```

A Paxos timeout at 16 or 32 writers is a real result. Record the count per
cell in the write-up. Never fold a failed operation into throughput.

- [ ] **Step 3: Check the trial spread**

Two trials exist to show the noise. Compute the spread per cell.

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

A spread above 15 % on any cell needs a note in the write-up. Two trials give a
range, not a standard deviation. Plot the range, and call it a range.

- [ ] **Step 4: Plot**

`bench/scripts/plot/plot_writers_scaling.py` matches `_agg_w{N}_t{T}.result`.
The multi-node files are named `_agg_multinode_w{N}_t{T}_trial{K}.result`.
Feed the TSV to the campaign plotter instead, or widen `AGG_RE` to:

```python
AGG_RE = re.compile(r"_agg(?:_multinode)?_w(\d+)_t\d+(?:_trial\d+)?\.result$")
```

One figure, five panels, one per workload. Each panel: x is the total writer
count on a log-2 axis, y is steady ops/s, two lines, and a range bar over the
two trials.

- [ ] **Step 5: Write `bench/RESULTS-strict-100g.md`**

Follow the shape of `bench/RESULTS-strict-frontier.md`. It must hold:

1. The date, the cluster, the dataset, the replication factor, the read modes.
2. The exact engine labels from Task 5 Step 5.
3. **The cache-fill statement, near the top.** State that every 300 s cell
   starts both OzoneDB caches empty, and give the two fractions from Task 7
   Step 5. A reader who takes these numbers as steady-state OzoneDB throughput
   will be wrong.
4. The steady-state table: workload, writers, both systems, and the ratio.
5. The failed-operation count per cell, or a line that says zero everywhere.
6. The trial range from Step 3, named as a range over two trials.
7. A comparison against the 1 GB run in `bench/RESULTS-strict-frontier.md`.
8. The reproduce block: the chain command and the extractor command.

- [ ] **Step 6: Commit**

```bash
git add bench/results-strict100-*.tsv bench/RESULTS-strict-100g.md bench/*.png bench/*.pdf
git commit -m "bench: strict frontier at 100 GB -- 100 cells, 2 trials, write-up"
```

---

## Time budget

| Step | Estimate |
|---|---|
| Task 0 to Task 4: guard, disk, snapshot, sync | 45 min |
| Task 5: smoke | 15 min |
| Task 6: chain scripts and dry run | 20 min |
| Task 7: trial 1 | 6 h 02 min |
| Task 8: trial 2 | 6 h 02 min |
| Task 9: extract, plot, write up | 1 h |

**Total: about 14 hours 30 minutes.**

Per-cell arithmetic behind the trial estimate:

- Cassandra: 25 cells x (300 s run + 60 s restore and start + 40 s launch) = 2 h 46 min.
- OzoneDB: 25 cells x (300 s run + 90 s bucket mirror + 40 s start + 40 s launch) = 3 h 16 min.

The 90 s bucket mirror is the largest single part of the OzoneDB overhead. It is
not optional: the log and the bucket must be restored together, or `bootstrap`
throws.

## Levers if 14 hours 30 minutes is too long

Apply these in order. Each one keeps the matrix shape.

1. **Drop workload b.** Workload b sits between a and c and adds little. Saves
   2 h 25 min.
2. **Drop the 2-writer point.** The 1 GB run showed it adds no shape. Saves
   1 h 12 min.
3. **Cut the cell to 240 s and the window to 90 s.** Saves 1 h 40 min. Do not
   go below 240 s. At 180 s the tier reaches only 10 % of its budget.

## Follow-ups (not in this sweep)

- **The warm-cache point.** Two long cells at 8 writers let the tier fill and
  give the steady value that the 16 GiB and 50 GiB budgets describe. It costs
  about 1 h 45 min and needs no new corpus:

  ```bash
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable --log-trim \
    --record-cnt 100000000 --lru-cache-bytes 17179869184 \
    --disk-cache-bytes 53687091200 --disk-cache-dir /tank/cache \
    --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" \
    --trial 1 --duration 2700 --run-tag strict100-20260830-warm
  python3 bench/scripts/extract_steady_throughput.py \
    bench/results/local/strict100-20260830-warm --window 300
  ```

  In 2700 s the tier reaches its full 50 GiB and the 16 GiB LRU reaches about
  5 GiB. Expect workload c near 10,088 ops/s and workload a near 2,800 ops/s.
- A third trial, with `chain_trial.sh 3`. The chain takes any trial number.
- The relaxed points of the frontier: Cassandra `--consistency quorum` and
  OzoneDB without `--linearizable`, same matrix. That completes a
  two-point-per-system frontier figure.
- A thread sweep at a fixed writer count, to find the server ceiling rather
  than the per-request latency.
- A 3+3 replicated configuration, with replication on both sides.
- Elasticity: the time for a new writer to join and reach steady state. The
  trimmed join is 612 ms to 730 ms at 100 GB, and Cassandra has no equivalent.
