# Plan: cost campaign 2 — `PLAN-cost.md` rerun on the native client and the cache work

**Written:** 2026-08-28. **Base plan:** `bench/PLAN-cost.md` (model, prices, fairness rules).
**Previous results:** `bench/RESULTS-cost.md`, campaigns `cost-20260827` to `disk2-20260828`.
**Branch:** `worktree-plan-cost-2`, cut from `visibility` at `2e5181dc`.

## 0. Premise check: what is in `visibility` today

The request was to rerun the cost benchmark "now that `visibility` has the cache
optimizations and the native Corfu client". The first half is true. The second half is not.

| Change | Commits | In `visibility` at `2e5181dc`? |
|---|---|---|
| 4 KiB SSTable blocks | `dd0f195e` | yes (`table_builder.cpp:16`) |
| Compaction range reads, `compaction_read_bytes` | `2dc2ed24`, `2380ddf0` | yes |
| Drop a compacted SSTable's blocks, per-level counters, warm worker (off by default) | `9ddaf573` .. `f987cd54` | yes |
| A file removed under a reader is a miss, NOT_FOUND retry | `96b9265d`, `fc9f0d5c` | yes |
| Disk-cache tier, round 1 (file mode) | `8ed9b9c1` .. `dcc9a438` | yes |
| Disk-cache tier, round 2 (chunk entries + TinyLFU, now the defaults) | `a0667667` .. `2e5181dc` | yes |
| **Native Corfu client**, `corfu_client = native` default, `--corfu-client`, label `-native` | `9b84fadc`, `b3a7ee5e`, `3a3d2226` | **no** — only on `worktree-native-corfu` |

`git rev-list --left-right --count visibility...worktree-native-corfu` reads `56 3`: the
native branch has three commits `visibility` lacks, and it lacks the 56 commits from the
crash fix onward. `origin/visibility` equals the local branch. No local branch, no
worktree and no node tree holds the merge (client `amd160` has `src/db/corfu/` on disk,
but those are leftovers of the 2026-08-27 rsync. Its runner has no `--corfu-client`,
and its `shared_config_base.json` has no `corfu_client` key).

So the rerun has a code task before its first cell: merge the native client into
`visibility`. A dry run (`git merge-tree --write-tree visibility worktree-native-corfu`)
reports six conflicts, all in bench scripts. The C++ side merges clean. Task 0 below
lists the files and the resolution rule for each.

## 1. Goal

Same figure as `PLAN-cost.md`: projected monthly cost against dataset size, one line
per system, on one engine, one cluster, one tag, both systems. The five campaigns since
`cost-20260827` each measured one change against the previous engine. None of them
re-measured Cassandra, and none ran the native client together with the cache work. The
result is a set of projections that mix rows from four campaigns
(`combine_disk_corpus.py`). This campaign replaces that corpus with one measured set.

What the engine changes since `cost-20260827` do to the model, and which coefficient
each one moves:

| Change | Coefficient | Measured so far | Expected here |
|---|---|---|---|
| 4 KiB blocks + range reads | `h(c/D)`, `get_per_write` | `h` 0.679 .. 0.032 at 52 % .. 0.013 %; 0.00015 GETs per put | unchanged: the client does not touch the block cache |
| Native client | `cpuO`, RSS | 0.42–0.59x the JNI CPU per op (phase 5, one trial, no tier) | 0.5–0.65 ms per op on workload a at 8 writers |
| Codec NONE (native branch) | `L`, `L0` | untrimmed 1 GB load is 1.3 GB on disk (ZSTD: 1.1 GB) | `L0` 1.1 -> ~1.3 KB per put; `L` 187 MiB -> ~220 MiB, still flat in D |
| Disk tier, chunk + frequency | `disk_h`, `cpu_O_disk`, `fill_get_per_op` | `disk_h` 0.29 / 0.51 / 0.99 at ratios 0.26 / 0.52 / 2.1; 0.32 ms per op at the full tier | `disk_h` unchanged; `cpu_O_disk` down with the client |
| Drop on compact, NOT_FOUND retry | none | 0 failed reads | 0 failed reads, 8/8 writers per cell |

The model itself does not change. `plot_cost_model.py --tier-variant ch64k-adm` over one
TSV from this campaign is the whole extraction.

## 2. Prediction, before the measurement

The committed round-2 corpus, re-run locally on 2026-08-28, reproduces the published
numbers exactly. The same corpus with every OzoneDB `client_cpu_s_per_op` scaled by 0.5
(the phase-5 median) and by 0.42 (the phase-5 best) gives:

| Line, USD per month | Published (`disk2-20260828`) | Client CPU x 0.5 | Client CPU x 0.42 |
|---|--:|--:|--:|
| `cpuO`, workload a, 8 writers | 1.157 ms | 0.579 ms | 0.486 ms |
| OzoneDB clients at 10,000 ops/s | 5 ($700) | 3 ($420) | 2 ($280) |
| 1 TB, no tier, 16 GB cache | 5,230 | 4,950 | 4,810 |
| 1 TB, 2 TB tier per client | 1,936 | 1,336 | 1,336 |
| 10 TB, no tier, 16 GB cache | 5,992 | 5,712 | 5,572 |
| 10 TB, no tier, 4 GB cache | 6,332 | 6,052 | 5,912 |
| 10 TB, 2 TB tier per client | 5,515 | 4,915 | 4,615 |
| 100 TB, 2 TB tier per client | 8,359 | 7,759 | 7,459 |
| Cassandra RF=3, EBS, 10 TB | 4,742 | 4,742 | 4,742 |
| Crossover, 4 GB-cache line | 14.7 TB | 14.7 TB | 14.7 TB |
| First crossover, tier line | 14.7 TB | 1.47 TB | 1.47 TB |

Three things to read from that table:

1. The client line is the only line the native client moves. Everything else in the
   model is a request count or a byte count, and phase 5 showed `h`, GETs per op and the
   server busy fraction equal on both clients within 0.5 points.
2. At 0.5x the tier line goes below the cheapest Cassandra layout at 1.5 TB and comes back
   above the EBS layout near 10 TB by $173 (4,915 against 4,742). At 0.42x it stays below
   at every decade. One trial resolves about 5 %, and $173 is 3.6 %. The write-up must
   state the band, not a single crossover.
3. The 4 GB-cache crossover does not move, because the Cassandra EBS line steps with the
   node count and the $280 shift falls inside one step.

The campaign refutes or confirms the 0.5x row. Nothing in it is tuned to hit it.

## 3. Fixed parameters

Everything in `PLAN-cost.md` "Fixed parameters" holds, with these changes:

| Knob | Value | Why |
|---|---|---|
| Engine | `visibility` + the native merge (Task 0), one commit hash in every result directory | one engine for every OzoneDB cell |
| Corfu client | `native` (the merged default), label token `-native` | the change under test; two JNI control cells only |
| Block cache | `--lru-cache-bytes` per cell; the sweep sizes of `PLAN-cost.md` phase 2 | the `h` curve |
| Block size, compaction reads | 4 KiB, `compaction_read_bytes` 64 MiB (defaults) | the read path that the 4 KiB re-run and the range-read campaign settled |
| Warm | off (default) | +7 % throughput, L2 stays cold; it is not part of the paper's line |
| Disk tier | off in the no-tier cells; `--disk-cache-bytes` at 256 MB, 512 MB, 2 GB with the chunk + frequency defaults (label `-dc<size>-ch64k-adm`) | the same three ratios as `disk2-20260828`, so `--tier-variant ch64k-adm` applies |
| Trimming | `--log-trim` in every OzoneDB cell and in the trimmed load | one checkpoint every 30 s |
| Duration | 600 s for the `h` sweep, every workload-a cell and every tier cell; 120 s for linearizable, scaling and Cassandra cells | workload a needs compactions inside the cell; the crash and the cold-cache findings both needed 600 s |
| Window | last 60 s of activity (`--window 60`) | as in every campaign since the 4 KiB re-run |
| Trial | 1 | coefficients are ratios; the JNI control pair is the repeatability check |
| Tag | `TAG=cost2-$(date +%Y%m%d)`, one date for both systems, sub-tags per table below | |

Result tag directories, and which of them feed the model:

| Tag | Cells | In the model corpus? |
|---|---|---|
| `$TAG` | OzoneDB 1 GB, 8 writers, no tier: `h` sweep, workload a, linearizable | yes (the model drops `linearizable` labels itself) |
| `$TAG-tier` | OzoneDB 1 GB, 8 writers, tier at three ratios | yes |
| `$TAG-cass` | Cassandra 1 GB: quorum and serial at 8 writers, quorum at 2 and 4 | yes |
| `$TAG-scale` | OzoneDB workload a at 2 and 4 writers | **no** — `cpu_O` is a median over workload-a cells with no writer filter (`plot_cost_model.py`, `pick`). A 2-writer cell pulls it down |
| `$TAG-jni` | the JNI control pair | **no** — its label `ozonedb-corfu-lru512m` matches the OzoneDB prefix and enters the same median |
| `$TAG-10g`, `$TAG-cass-10g` | the 10 GB scale check, both systems | **no** — reported as a check table; `get_per_write` no longer depends on depth (0.00015 with range reads) |
| results root | the load rows (`_rc<N>` samples) | yes, after the root is archived (Task 4) |

## 4. Fairness rules

Rules 1–5 of `PLAN-cost.md` hold. Add:

6. Never build on a client while a Cassandra cell runs. `cpuC` is a client CPU number.
   Task 2 (Cassandra) runs before Task 3 (sync + build) for this reason.
7. Run the Cassandra 10 GB cells first. The 10 GB snapshot (`/tank/cassandra/data.load`,
   11 GB) is still on the server, and the 1 GB load wipes it. Running them first saves
   a 10 GB Cassandra load and 11 GB of the 24 GB that the root disk has free.
8. Stop Corfu before the first Cassandra cell. Start it again after the last one. Both
   servers idle on the same box, but rule 2 says one server at a time.
9. Reload OzoneDB after the merge. The native client reads codec NONE only. Every
   snapshot under `/mnt/corfu/` today was written by the JNI client with ZSTD.

## 5. Tasks

### Task 0. Merge the native client into `visibility`

Do this on the laptop, in a worktree cut from `visibility`. The user runs the build.

1. Run `git merge --no-commit worktree-native-corfu` from the worktree.
2. Resolve the six conflicts with these rules:

   | File | `visibility` side | native side | Resolution |
   |---|---|---|---|
   | `bench/scripts/extract_cost_coefficients.py` | `DISK_RE`, the disk columns, the steady window | `REPLAY_RE` accepts a `(jni)` / `(native)` token | keep both regex changes |
   | `bench/scripts/local/load_local_ycsb_multiproc.py` | `disk_cache_corfu_settings`, `--disk-cache-*`, `--cache-warm*`, label tokens `-dc`, `-ch`, `-adm`, `-warm` | `corfu_client_corfu_settings`, `--corfu-client`, `NATIVE_LABEL_SUFFIX` in `result_label` | keep both. In `result_label` the order must be: engine, `-linearizable`, `-native`, `-lru…`, `-dc…-ch…-adm`, `-warm…`. The model's `tier_variant()` takes the tokens after `-dc<size>`, so `-native` must come before `-dc` |
   | `bench/scripts/local/run_local_ycsb_multiproc.py` | same as the loader | same as the loader | same as the loader |
   | `bench/scripts/local/run_multinode_ycsb.py` | the disk and warm pass-through | `--corfu-client` pass-through | keep both |
   | `bench/scripts/local/run_multinode_ycsb_with_corfu.sh` | disk and warm flags, the mount guard | `--corfu-client`, `corfu_client=` in the sweep banner | keep both |
   | `bench/scripts/local/load_corfu_dataset.sh` | `--cache-warm`, `_rc` sample name | `--corfu-client` | keep both |

   `CMakeLists.txt`, `CLAUDE.md`, `src/db/db.cpp`, `src/include/ozonedb/metadata.h` and
   `src/config/corfu/shared_config_base.json` auto-merge. Read the merged `CMakeLists.txt`
   once: the disk-cache sources and the `OZONEDB_CORFU_NATIVE` block must both be there.
3. Check the labels without a cluster:
   ```bash
   bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --dry-run --log-trim \
     --lru-cache-bytes 8388608 --disk-cache-bytes 536870912 --workloads c --writers-list 1
   ```
   The printed per-client command must carry `--corfu-client` (or the default) and the
   disk flags together. Then, in `run_local_ycsb_multiproc.py`, call `result_label` on a
   settings dict with `corfu_client=native`, `lru_cache_bytes=8388608`,
   `disk_cache_bytes=536870912` and confirm that the string is
   `ozonedb-corfu-native-lru8m-dc512m-ch64k-adm`. Feed that name through
   `extract_cost_coefficients.py`'s `RUN_RE` and `plot_cost_model.tier_variant`. The
   variant must read `ch64k-adm`.
4. Commit as one merge commit. Push the branch. Do not fast-forward `visibility` from a
   background job. The user merges.

Exit: the merge commit exists, the dry run prints both flag families, the three label
checks pass.

### Task 1. Cluster check and disk space

1. Confirm that no other session drives the cluster: local `pgrep -af
   'run_multinode|ansible-playbook|load_corfu_dataset|load_multinode'`, YCSB JVMs on the
   clients, a `corfu_server` you did not start. Ask the user if anything is running.
2. Confirm the lease: `ssh amd127 uptime` and one client. On 2026-08-28 16:40 UTC-6
   the nodes had been up 2 days 3 h.
3. Free the root disk on `amd127` (63 GB, 24 GB free, 37 GB used). Remove the retired
   Corfu snapshots `ctrl`, `fast`, `fix`, `fix2`, `flip_test`, `native_test`, `test`,
   `tests`, `tests2` under `/mnt/corfu/` (about 3.6 GB). Keep `load-zstd-20260827` and
   `load-bucket-zstd-20260827` until the new load exists, then remove them too (1.4 GB).
   The 10 GB phase needs about 14 GB (12 GB of bucket, 1.5 GB of log) after the
   Cassandra data (22 GB) is gone.
4. Check `/tank/cache` is a mount point on all eight clients (`mountpoint /tank/cache`).
   The runner refuses a tier root that is not one.
5. Archive the results root so that this campaign's load rows are the only ones:
   ```bash
   mkdir -p bench/results/local/archive-pre-cost2
   mv bench/results/local/*insert* bench/results/local/*_rc[0-9]* bench/results/local/archive-pre-cost2/ 2>/dev/null
   ```
   Do the same on every client's `bench/results/local` (the orchestrator pulls the load
   files from there).

### Task 2. Cassandra cells, both sizes (before any build)

Cassandra did not change. The cells are re-run so that both systems share one tag, one
day and one box state, and so that the corpus stops carrying `cost-20260827` rows.

```bash
TAG=cost2-$(date +%Y%m%d)
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -sd,)
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -sd,; }
# the runner's own stop pattern (run_multinode_ycsb_with_corfu.sh, stop_corfu)
ssh oliverr3@amd127.utah.cloudlab.us "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true"

# 10 GB first: the snapshot is on the box today
bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency quorum \
  --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --record-cnt 10000000 \
  --trial 1 --duration 120 --run-tag $TAG-cass-10g

# 1 GB: load (wipes the 10 GB snapshot), then the four frontier cells and two scaling cells
bash bench/scripts/local/load_multinode_cassandra.sh --writers 8
for mode in quorum serial; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency $mode \
    --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass
done
for n in 2 4; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency quorum \
    --workloads a --writers-list 1 --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG-cass
done
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh space; /tank/cassandra/cassandra_ctl.sh compact; /tank/cassandra/cassandra_ctl.sh space'
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh stop'
```

Record `sC` from the two `space` calls (peak, then after compact). After the last cell,
delete `/tank/cassandra/data` and `data.load` to free 22 GB. The Cassandra rows are then
complete for the campaign. Nothing later touches Cassandra.

Exit: 8 cells, `rc=0`, 0 failed ops, `cpuC` 0.06 ms per op within 10 % of
`cost-20260827`, quorum workload a at 8 writers 43,000 ops/s within 5 %.

### Task 3. Sync, build, verify the native client on the cluster

1. Sync the merged tree to every node: `ansible-playbook bench/ansible/sync.yml` (the
   touch step must skip `target/`). `sync.yml` does not delete by default, so the stale
   `src/db/corfu/` on the clients is overwritten by the real one.
2. Build on every client: `bash bench/scripts/build.sh` on each, in parallel. Record
   the commit hash that `build/libOzoneDB.so` was built from.
3. Start a **fresh, empty** Corfu on `amd127` (a new log directory). Run on one client:
   ```bash
   cd build && CORFU_TEST_CLIENT=native CORFU_TEST_ENDPOINT=10.10.1.1:9090 \
     CORFU_BRIDGE_JAR=... ./runUnitTests --gtest_filter='CorfuStorageTest.*:DiskCacheStorageTest.*'
   ```
   `runUnitTests` is not built by `build.sh`. Use `cmake --build build --target
   runUnitTests`. The storage tests fail-stop on a trimmed log, which is why the server
   must be fresh. Expect 13/13 Corfu and every disk-cache test green.
4. Stop that server. The campaign's Corfu is started by the load wrapper.

Exit: eight clients at one hash, tests green on native, `corfu_native_probe` present.

### Task 4. Phase 1 at 1 GB: two loads

1. Untrimmed load, for `L0` under codec NONE:
   ```bash
   bash bench/scripts/local/load_corfu_dataset.sh --writers 8      # no --log-trim
   ssh oliverr3@amd127.utah.cloudlab.us 'du -sb /mnt/corfu/load'   # L0 = bytes / 1,000,000 puts
   ```
   Expect about 1.3 KB per put. Write it into `space.json` as `L0_kb_per_put`. Move the
   load's result files and `_rc1000000` sample to `bench/results/local/$TAG-loads-notrim/`
   so the trimmed load below is the only `load` row in the root.
2. Trimmed load, the dataset every OzoneDB cell restores:
   ```bash
   bash bench/scripts/local/load_corfu_dataset.sh --writers 8 --log-trim
   ```
   From its extractor row (`--tsv` over the root) take `bucket_bytes` (`sO` after the
   checkpoints are excluded), `checkpoint_bytes`, `du_corfu_kb` (`L`), `s3_bytes_in` /
   dataset bytes (`wa`), `get_per_write`, `ckpt_objects / ckpt_count` (`k`). Fill
   `bench/scripts/plot/space.json` and keep the old one as `space-disk2.json`.
3. Remove the two `-zstd-20260827` snapshots now.

Exit: `sO` 1.17 within 3 %, `L` at most 250 MiB, `wa` 2.3 within 10 %, `k` 5–7,
`get_per_write` at most 0.001, load rate at least 9,000 puts/s, `slow=0 spurious=0`.

### Task 5. Phase 2 at 1 GB: `h`, `cpuO`, linearizable, scaling, JNI control

```bash
# h sweep and the workload-c CPU, 600 s
for c in 536870912 67108864 8388608 1048576 131072; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes $c \
    --workloads c --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG \
    || { echo "FAILED at lru=$c"; break; }
done
# workload a at the two ratios the projection reads, 600 s
for c in 536870912 8388608; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes $c \
    --workloads a --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG
done
# strict reads, 120 s
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --linearizable --lru-cache-bytes 536870912 \
  --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG
# server scaling, 120 s, own tag
for n in 2 4; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 536870912 \
    --workloads a --writers-list 1 --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG-scale
done
# JNI control on the same engine and dataset, 600 s, own tag
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --corfu-client jni --lru-cache-bytes 536870912 \
  --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-jni
```

Thirteen cells. Launch the chain detached (`setsid nohup bash chain.sh > chain.log 2>&1 &`)
with `set -e`: the harness kills background chains, and a failed cell must not fall
through to the next one.

Exit:

- `h_steady` on workload c within 0.03 of the 4 KiB curve at every ratio.
- 8/8 writers with an `[OVERALL]` block in every cell, 0 failed ops.
- `cpuO` on workload a at 512 MB at most 0.65 ms, and at least 35 % below the JNI control.
- RSS per writer at most 1.5 GB.
- Linearizable workload c within 3 % of the default throughput, every linearizable READ OK.

### Task 6. Tier cells at 1 GB

The five cells of `disk2-20260828` that the round-2 projection used, on the native client:

```bash
run_tier() { # $1 bytes, $2 workloads
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 8388608 \
    --disk-cache-bytes $1 --workloads "$2" --writers-list 1 --client-hosts "$HOSTS" \
    --trial 1 --duration 600 --run-tag $TAG-tier
}
run_tier 268435456 c
run_tier 536870912 "a c"
run_tier 2147483648 "a c"
```

The mode and admission flags are omitted on purpose: `chunk` and `frequency` are the
merged defaults, and the label must read `-dc<size>-ch64k-adm` for `--tier-variant
ch64k-adm` to select the rows.

Exit:

- `disk_h` within 0.05 of 0.29 / 0.51 / 0.99 at the three ratios.
- `disk_amp` at most 16 at 512 MB.
- `disk_fill_gets_per_op` 0 at 512 MB and 2 GB on workload a, `disk_punch_failed` 0.
- `cpu_O_disk` (workload a, 2 GB) at most 0.25 ms.
- The 2 GB workload-c cell at least 45,000 ops/s.

### Task 7. Scale check at 10 GB

```bash
bash bench/scripts/local/load_corfu_dataset.sh --writers 8 --log-trim --record-cnt 10000000
for c in 10485760 1310720; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes $c --record-cnt 10000000 \
    --workloads c --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g
done
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 10485760 --record-cnt 10000000 \
  --workloads a --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g
# one tier point at ratio 0.26 (2.6 GB per writer against 10 GB), workload c
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 8388608 --record-cnt 10000000 \
  --disk-cache-bytes 2684354560 --workloads c --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g
```

The join numbers (`j`, `b_j`, replay time, time to first op) come from the replay line
of every writer at cell start. No extra cell is needed. Record `L` and `sO` at 10 GB from
the load row as in Task 4.

Exit:

- `h` at ratio 0.1 % and 0.013 % within 0.06 of the 1 GB twins.
- `disk_h` at 0.26 within 0.05 of the 1 GB point.
- `L` within 20 % of the 1 GB value.
- Replay under 5 s from the checkpoint.
- `cpuO` on workload a within 20 % of the 1 GB value. On JNI it was 2.0–2.3 ms against
  1.58 ms, and the tailer share is what the native client removed.

### Task 8. Extraction, model, figure, write-up

```bash
python3 bench/scripts/extract_cost_coefficients.py \
    bench/results/local/$TAG bench/results/local/$TAG-tier bench/results/local/$TAG-cass \
    bench/results/local --window 60 --tsv bench/results-$TAG.tsv
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/$TAG-scale \
    bench/results/local/$TAG-jni --window 60 --tsv bench/results-$TAG-controls.tsv
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/$TAG-10g \
    bench/results/local/$TAG-cass-10g bench/results/local --window 60 --tsv bench/results-$TAG-10g.tsv
python3 bench/scripts/plot/plot_cost_model.py bench/results-$TAG.tsv bench/scripts/plot/prices.json \
    --space bench/scripts/plot/space.json --tier-variant ch64k-adm \
    --out-dir bench/scripts/plot/out --table bench/results-$TAG-projection.tsv
```

No `combine_disk_corpus.py`: every coefficient comes from `results-$TAG.tsv`. The model
must print `measured` for every source line. If a source prints `ASSUMED`, a cell is
missing. Rerun it before writing anything.

Write-up, as a new top section of `bench/RESULTS-cost.md` named "Campaign 2: one engine,
both systems (`$TAG`)": the coefficient table with the `cost-20260827` column beside it,
the throughput and CPU table for both systems, the `h` curve, the tier table, the 10 GB
check, the JNI control pair, the projection at the decades, the crossover band from §2
against the measured one, the caveats (one trial, uniform keys, `disk_h` clamped below
0.26). Update the "Findings" numbers that the campaign changes, in place, with a date.
Commit the three TSVs, the projection TSV, the figure, `space.json` and `prices.json`
(re-read the prices, update `as_of`).

## 6. Cell count and time budget

| Task | Cells | Time |
|---|--:|---|
| 0. merge, label checks, push | | 2–3 h on the laptop |
| 1. cluster check, disk space, archive | | 20 min |
| 2. Cassandra: 2 cells at 10 GB, 1 GB load, 6 cells at 1 GB | 8 | 45 min |
| 3. sync, build, tests on a fresh server | | 40 min |
| 4. two 1 GB loads | | 15 min |
| 5. sweep 5, workload a 2, linearizable 2, scaling 2, JNI 2 | 13 | 9 x 15 min + 4 x 4 min = 2 h 30 |
| 6. tier | 5 | 75 min |
| 7. 10 GB load and 4 cells | 4 | 20 min + 60 min |
| 8. extraction, model, write-up | | 2 h |
| **Total** | **30** | about 7 h on the cluster, 1.5 working days end to end |

Tasks 5, 6 and 7 are one detached chain of 22 OzoneDB cells (about 4 h 45). Task 2 is
a second chain, run first. The server is idle between chains.

## 7. What to watch

Everything in `PLAN-cost.md` "What to watch" holds. Add what the five campaigns since
then taught:

- **A JVM abort leaves `rc=0` at every layer.** Count the writers with an `[OVERALL]`
  block per cell (`grep -l OVERALL *.result | wc -l`) before trusting a sum. Every
  workload-a cell of `cost-20260827` lost 1 to 4 writers this way.
- **The bucket restore must force-copy `LATEST`.** `start_corfu` does since `1127b7cc`.
  A writer that dies at open with "LATEST that cannot be read" is that bug back.
- **`CorfuStorageTest` fail-stops on a trimmed log.** Run it against a fresh server
  (Task 3), never the loaded one.
- **The native client reads codec NONE only.** Every snapshot under `/mnt/corfu/` on
  2026-08-28 is ZSTD. Never restore one of them under the merged engine.
- **The h sweep's `h_steady` assumes one lookup per read.** Workload a does about 1.3.
  Quote the counter `h` for workload a, `h_steady` for workload c.
- **A whole-file read is served from the tier only when every chunk is present.** That is
  the 6 % workload-a deficit at the full tier in round 2. It is expected, not a regression.
- **`disk_fill_failed` is only meaningful in chunk mode.** Every tier cell here is chunk
  mode, so it must read 0.
- **`DB::openDB` on an empty stream takes 6 s** on both clients. Not a regression.
- **The load samples must carry `_rc<N>`.** The 10 GB load of `cost-20260827` overwrote
  the 1 GB sample. The wrappers do this now. Task 1 archives the old files so that the
  root holds one load per size.
- **Orchestrator ssh needs `ServerAliveInterval`** (in place) and the chain needs to be
  detached from the harness (`setsid nohup`).
- **The cluster is shared.** Check for another session's drivers before every chain
  (Task 1, step 1), kill only pids this job started, keep them in a file.
- **The trimmer is global writer 0**, the first writer on the first host. The scaling
  cells with `hosts_n 2` keep `amd160` first, so the trimmer does not move.
- **One trial.** Differences under 5 % are not resolved. The JNI control pair is the only
  repeat: its workload-c throughput must agree with the native cell within 3 % (phase 5
  measured 0.99x), which is the noise floor for the write-up.

## 8. Exit criteria for the campaign

| Check | Pass |
|---|---|
| Every cell `rc=0`, 0 failed ops, 8/8 writers with `[OVERALL]` (2/2 and 4/4 in scaling) | required |
| `cpuO`, workload a, 8 writers, 600 s, 512 MB | at most 0.65 ms, at least 35 % below the JNI control |
| `cpuO`, workload c, 8 writers | at most 0.25 ms |
| RSS per writer, workload a, 8 writers | at most 1.5 GB (phase 5: 1.44 GB) |
| `h_steady`, workload c, five ratios | within 0.03 of 0.679 / 0.341 / 0.216 / 0.141 / 0.032 |
| `disk_h` at 0.26 / 0.52 / 2.1 | within 0.05 of 0.29 / 0.51 / 0.99 |
| `get_per_write` (trimmed load) | at most 0.001 |
| `L` (1 GB and 10 GB) | at most 250 MiB, and the two within 20 % |
| `L0` under codec NONE | 1.2–1.4 KB per put |
| Cassandra quorum, workload a, 8 writers; `cpuC` | 43,000 ops/s within 5 %; 0.06 ms within 10 % |
| Model sources | every line `measured`, none `ASSUMED` |
| 10 TB, no tier, 16 GB cache | at most $5,750 (prediction $5,712) |
| 10 TB, 2 TB tier | at most $4,950 (prediction $4,915); state whether the line is below Cassandra EBS ($4,742) or not, with the one-trial caveat |

## 9. Not in this plan

- Trials 2 and 3 of the native phase 5, and the phase 4 restart and `ss -K` tests
  (`PLAN-native-corfu.md`).
- The 16 KiB tier entries at three ratios, the shared cross-client tier, the skewed-key
  cell (`RESULTS-cost.md`, "Disk-cache tier, round 2", what to do next).
- The real S3 check (`PLAN-cost.md` phase 6).
- A profile of the native client (phase 0 of `PLAN-native-corfu.md`).
- An extractor check that counts writers with an `[OVERALL]` block per cell and flags a
  short cell. This is still by hand (§7, first bullet). It is a two-hour change and the
  first thing to add if Task 5 has to be re-run.
