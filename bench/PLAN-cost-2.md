# Plan: cost campaign 2 — `PLAN-cost.md` rerun at 10 GB and 100 GB on the native client and the cache work

**Written:** 2026-08-28, revised the same day for the 10 GB / 100 GB datasets.
**Base plan:** `bench/PLAN-cost.md` (model, prices, fairness rules).
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

The datasets are **10 GB and 100 GB** (10 M and 100 M records of 1 KB). The 1 GB cells
are dropped. What the larger sizes buy:

- The cache ratios that the 10 TB and 100 TB read-offs use (0.16 % and 0.016 % with a
  16 GB client cache) are measured with caches of 100 MB to 800 MB at 100 GB, not with
  1 MB caches at 1 GB. A 1 MB cache holds 256 blocks of 4 KiB, which is a different
  regime from a 160 MB cache at the same ratio.
- The tier ratio of a 2 TB SSD against 100 TB (0.02) becomes a measured point (2.6 GB
  against 100 GB). The round-2 projection clamps `disk_h` below 0.26 today.
- Space amplification `sO`, write amplification `wa` and the compaction GETs per put are
  measured on a four-level tree (100 GB spans L1 to L4 with the base config's level
  sizes), which is the shape the projection range has.
- `L`, `k` and the join cost are checked flat against a tenfold larger dataset.
- A second request distribution. YCSB's `requestdistribution=zipfian` is a
  `ScrambledZipfianGenerator`: a zipfian (θ = 0.99) over a fixed 10^10 items
  (`ScrambledZipfianGenerator.java:33-35`), folded onto the key space by `fnvhash64(item)
  % itemcount` (line 103). Over 10 M or 100 M keys that is a few hundred very hot keys on
  a uniform background, which is the measured `h` curve (0.216 at a 0.8 % cache, then
  about the capacity fraction). Task 7b adds a zipfian over the key space itself
  (`zipfian_keyspace`, Task 0b) as a second, labelled OzoneDB line. Every cell of the
  main campaign keeps YCSB's default, because that is what every published YCSB number
  is.

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

The 100 GB cells can move two more numbers, and the table above does not predict them:
`disk_h` at ratio 0.026 (today clamped to the 0.26 value, 0.29) and `wa` on a four-level
tree (2.34 at 1 GB, 3.41 at 10 GB). A lower `disk_h` at 0.026 raises the 100 TB tier
line. A higher `wa` moves the compaction PUT line, which is $2 a month today and stays
small at any plausible value.

The campaign refutes or confirms the 0.5x row. Nothing in it is tuned to hit it.

The zipfian-over-the-key-space line (Task 7b) is a hand estimate from the model's terms,
not a model run. With θ = 0.99 the top k keys of N carry `zeta(k) / zeta(N)` of the
reads, and a hot key costs one 4 KiB block (keys are hashed on insert, so there is no
block locality). A 16 GB cache holds 4.2 M keys:

| Cache / dataset | Keys held | `h`, YCSB zipfian (measured curve) | `h`, zipfian over the key space |
|---|--:|--:|--:|
| 16 GB / 10 TB | 4.2 M of 10^10 | 0.157 | about 0.65 |
| 4 GB / 10 TB | 1.0 M of 10^10 | 0.07 | about 0.58 |
| 16 GB / 100 TB | 4.2 M of 10^11 | 0.044 | about 0.58 |

GETs scale with `1 - h`. At 10 TB the GET line falls from about $4,430 to about $1,840,
so the no-tier 16 GB line (client CPU at 0.5x) goes from $5,712 to about $3,100, under
the Cassandra EBS layout's $4,742, and the crossover of the 4 GB line moves from 14.7 TB
to about 2 to 3 TB. Cassandra does not move: its cells are client-bound at 17 % server
busy. Under a key-space zipfian `h` is not scale-free (`zeta(k) / zeta(N)` rises with N
at a fixed ratio), so a projection that reads the 100 GB curve is conservative at 10 TB
by about 0.08 in `h`. The write-up says so.

## 3. Fixed parameters

Everything in `PLAN-cost.md` "Fixed parameters" holds, with these changes:

| Knob | Value | Why |
|---|---|---|
| Engine | `visibility` + the native merge (Task 0), one commit hash in every result directory | one engine for every OzoneDB cell |
| Corfu client | `native` (the merged default), label token `-native` | the change under test; two JNI control cells only |
| Request distribution | YCSB's default (`workloada`, `workloadc`) in every main cell; `workloadaz` / `workloadcz` = the same mixes with `requestdistribution=zipfian_keyspace` in the Task 7b cells | see §1, last bullet; the distribution rides on the workload name, so no runner flag and no label token |
| Datasets | D10 = 10,000,000 x 1 KB, D100 = 100,000,000 x 1 KB, `--record-cnt` on every load and every cell | see §1 |
| Server storage | `/dev/sdb` on `amd127` (447 GiB SATA SSD, unused today) as one ext4 mount `/tank/ssd`, holding MinIO, the Corfu log and snapshots, and the Cassandra data | the 100 GB states do not fit the 63 GB root disk (24 GB free); see Task 1 |
| Client storage | unchanged: `/dev/sdb` is the tier at `/tank/cache` (440 GB), enough for a 52 GB tier per writer | |
| Block cache | `--lru-cache-bytes` per cell, sizes in the ratio table below | the `h` curve |
| Block size, compaction reads | 4 KiB, `compaction_read_bytes` 64 MiB (defaults) | the read path that the 4 KiB re-run and the range-read campaign settled |
| Warm | off (default) | +7 % throughput, L2 stays cold; it is not part of the paper's line |
| Disk tier | off in the no-tier cells; `--disk-cache-bytes` at the sizes in the ratio table, chunk + frequency defaults (label `-dc<size>-ch64k-adm`) | the round-2 ratios plus 0.026, so `--tier-variant ch64k-adm` applies |
| Trimming | `--log-trim` in every OzoneDB cell and in every trimmed load | one checkpoint every 30 s |
| Duration | 300 s for a cell whose cache or tier fills in under 30 s at the miss rate (RAM caches up to 800 MiB, 2.5 GiB tiers); 600 s for every other cell and for every workload-a cell (compactions must happen inside the cell); 120 s for linearizable, scaling and Cassandra cells | the last-60 s window must sit after the fill; a 100 GB RAM cell at 600 s beats its 300 s twin at 10 GB in the model's per-ratio tie-break (longest cell) |
| Window | last 60 s of activity (`--window 60`) | as in every campaign since the 4 KiB re-run |
| Trial | 1 | coefficients are ratios; the JNI control pair is the repeatability check |
| Tag | `TAG=cost2-$(date +%Y%m%d)`, one date for both systems, sub-tags per table below | |

Cache and tier sizes per dataset. Dataset bytes are records x 1,024. The extractor
computes the ratio from the `[lru_cache]` capacity, so the sizes only need to give the
same ratio at both sizes where a twin is wanted:

| Ratio | 10 GB (`--lru-cache-bytes`) | 100 GB (`--lru-cache-bytes`) | Stands for |
|---|---|---|---|
| 52 % | 5368709120 (5 GiB) | — | hot set fits |
| 6.6 % | 671088640 (640 MiB) | — | |
| 0.82 % | 83886080 (80 MiB) | 838860800 (800 MiB) | 10 TB with an 80 GB cache |
| 0.10 % | 10485760 (10 MiB) | 104857600 (100 MiB) | 10 TB with 16 GB |
| 0.013 % | 1310720 (1.25 MiB) | 13107200 (12.5 MiB) | 10 TB with 1 GB; 100 TB with 16 GB |

| Tier ratio | 10 GB (`--disk-cache-bytes`) | 100 GB (`--disk-cache-bytes`) | Stands for |
|---|---|---|---|
| 2.1 | 21474836480 (20 GiB) | — | the full tier |
| 0.52 | 5368709120 (5 GiB) | 53687091200 (50 GiB) | |
| 0.26 | 2684354560 (2.5 GiB) | 26843545600 (25 GiB) | 2 TB against 10 TB |
| 0.026 | — | 2684354560 (2.5 GiB) | 2 TB against 100 TB (clamped today) |

Result tag directories, and which of them feed the model:

| Tag | Cells | In the model corpus? |
|---|---|---|
| `$TAG-10g` | OzoneDB 10 GB, 8 writers, no tier: `h` sweep, workload a, linearizable | yes (the model drops `linearizable` labels itself) |
| `$TAG-10g-tier` | OzoneDB 10 GB, 8 writers, tier at three ratios | yes |
| `$TAG-100g` | OzoneDB 100 GB, 8 writers, no tier | yes |
| `$TAG-100g-tier` | OzoneDB 100 GB, 8 writers, tier at three ratios | yes |
| `$TAG-cass-10g`, `$TAG-cass-100g` | Cassandra quorum and serial at 8 writers, quorum at 2 and 4 (10 GB only) | yes |
| `$TAG-scale` | OzoneDB workload a at 2 and 4 writers, 10 GB | **no** — `cpu_O` is a median over workload-a cells with no writer filter (`plot_cost_model.py`, `pick`). A 2-writer cell pulls it down |
| `$TAG-jni` | the JNI control pair, 10 GB | **no** — its label `ozonedb-corfu-lru5g` matches the OzoneDB prefix and enters the same median |
| `$TAG-10g-zipf`, `$TAG-100g-zipf`, `$TAG-cass-zipf` | the `az` / `cz` cells of Task 7b, both systems | **no** — their own TSV and their own model run (`--h-workload cz --cpu-workload az`); the model keys `h` and `cpu_O` on the workload name, so the two distributions never share a corpus |
| results root | the load rows (`_rc<N>` samples) | yes, after the root is archived (Task 1) |

How the model combines the two sizes: `h` keeps one point per ratio, and at the three
twin ratios the 600 s cell at 100 GB wins over the 300 s cell at 10 GB. `cpu_O` is the
median of the three workload-a no-tier cells (5 GiB and 80 MiB at 10 GB, 100 MiB at
100 GB). `get_per_write` is the median over both trimmed loads. `disk_h` interpolates over
four tier ratios: 0.026 (100 GB), 0.26 and 0.52 (the 10 GB cells, which the model picks
because they run behind the smaller RAM cache) and 2.1 (10 GB). The 100 GB cells at 0.26
and 0.52 are the scale check, not model inputs. `space.json` takes `sO`, `wa` and `L` from the 100 GB
load, `L0` from the untrimmed 10 GB load and `sC` from the 100 GB Cassandra `space` after
`compact`.

## 4. Fairness rules

Rules 1–5 of `PLAN-cost.md` hold. Add:

6. Never build on a client while a Cassandra cell runs. `cpuC` is a client CPU number.
   Task 2 (Cassandra) runs before Task 3 (sync + build) for this reason.
7. Run the Cassandra 10 GB cells first. The 10 GB snapshot (`/tank/cassandra/data.load`,
   11 GB) is on the server today, and the 100 GB load wipes it.
8. Stop Corfu before the first Cassandra cell. Start it again after the last one. Both
   servers idle on the same box, but rule 2 says one server at a time.
9. Reload OzoneDB after the merge. The native client reads codec NONE only. Every
   snapshot under `/mnt/corfu/` today was written by the JNI client with ZSTD.
10. One SSD for both systems on the server. Every measured server-side byte count
    (`du`, `space`) comes from the same ext4 filesystem on the same device, and the two
    systems never hold live 100 GB state at the same time (Task 2 deletes the Cassandra
    `data` tree before Task 7 loads OzoneDB at 100 GB).

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
   bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --dry-run --log-trim --record-cnt 10000000 \
     --lru-cache-bytes 10485760 --disk-cache-bytes 2684354560 --workloads c --writers-list 1
   ```
   The printed per-client command must carry `--corfu-client` (or the default) and the
   disk flags together. Then, in `run_local_ycsb_multiproc.py`, call `result_label` on a
   settings dict with `corfu_client=native`, `lru_cache_bytes=10485760`,
   `disk_cache_bytes=2684354560` and confirm that the string starts with
   `ozonedb-corfu-native-lru10m-dc` and ends with `-ch64k-adm`. Feed that name through
   `extract_cost_coefficients.py`'s `RUN_RE` and `plot_cost_model.tier_variant`. The
   variant must read `ch64k-adm`.
4. Commit as one merge commit. Push the branch. Do not fast-forward `visibility` from a
   background job. The user merges.

Exit: the merge commit exists, the dry run prints both flag families, the three label
checks pass.

### Task 0b. Zipfian over the key space (on the merged tree, no C++)

1. `ycsb/core/src/main/java/site/ycsb/workloads/CoreWorkload.java`, a branch next to
   the `zipfian` one at line 519:
   ```java
   } else if (requestdistrib.compareTo("zipfian_keyspace") == 0) {
     // A zipfian (theta 0.99) over THIS key space. YCSB's "zipfian" is a
     // ScrambledZipfianGenerator over 10^10 items folded onto the key space by
     // fnvhash64, which is close to uniform beyond a few hundred hot keys.
     keychooser = new ZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys - 1);
   ```
   Mirror the bounds of the `zipfian` branch. The hot values are the low key numbers,
   and `insertorder=hashed` (the default) hashes the key number into the key string, so
   the hot records are scattered over every SSTable. `ZipfianGenerator` computes its
   normalizer with a loop over N at construction: seconds at 100 M, once per process.
   An unknown distribution name makes `CoreWorkload` throw, so a typo fails loudly.
2. Two workload files: `ycsb/workloads/workloadaz` and `workloadcz`, copies of
   `workloada` and `workloadc` with `requestdistribution=zipfian_keyspace` and a comment
   that says why. `generate_workload.py` names its output after the file, the result
   files carry `workloadcz`, `RUN_RE` captures `wl = cz`, and nothing else in the chain
   reads the letter (checked: the wrappers split on spaces, the runner on commas, no
   validation anywhere).
3. `plot_cost_model.py`: a `--cpu-workload` option (default: `c` when the read fraction
   is at least 0.95, else `a`, as today at line 208), so the zipf corpus is read with
   `--h-workload cz --cpu-workload az`.
4. A small overlay script, `bench/scripts/plot/overlay_projections.py`: two projection
   TSVs in, one figure out, the Cassandra lines once, the OzoneDB lines twice (solid for
   YCSB's zipfian, dashed for the key-space zipfian). About 30 lines of matplotlib.
5. The functional check needs a loaded dataset, so it is the first cell of Task 5's
   chain: one `cz` cell at 10 GB, 100 MiB cache, 300 s. Its `[lru_cache]` hit rate must
   be far above the scrambled cell at the same cache (expect above 0.4 against about
   0.14), with 0 failed reads. If it is not, stop the chain: the branch did not take.

About 45 min on the laptop. It changes no file that the merge in Task 0 conflicts on.

### Task 1. Cluster check, server SSD, results root

1. Confirm that no other session drives the cluster: local `pgrep -af
   'run_multinode|ansible-playbook|load_corfu_dataset|load_multinode'`, YCSB JVMs on the
   clients, a `corfu_server` you did not start. Ask the user if anything is running.
2. Confirm the lease: `ssh amd127 uptime` and one client. On 2026-08-28 16:40 UTC-6
   the nodes had been up 2 days 3 h.
3. Put the server state on the free SSD. `lsblk` on `amd127` on 2026-08-28: `sda` is the
   boot disk (64 GB root, 8 GB swap, about 375 GB unallocated), `sdb` is 447 GiB with no
   filesystem. Use `sdb` whole. Do not partition `sda`: `setup_disk_cache.sh` refuses the
   root disk for a reason, and the unallocated part of `sda` is the fallback only if
   `sdb` turns out to be too small (see the space table below).
   ```bash
   # on amd127; the script formats, labels and writes the fstab entry (idempotent)
   bash ~/ozonedb/bench/scripts/setup_disk_cache.sh --device /dev/sdb --mount /tank/ssd --label ozssd
   sudo systemctl stop minio
   pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true
   /tank/cassandra/cassandra_ctl.sh stop
   sudo mkdir -p /tank/ssd && sudo chown $USER /tank/ssd
   # MinIO: the unit's ExecStart keeps the path /tank/minio, which becomes a symlink
   mv /tank/minio /tank/ssd/minio && ln -s /tank/ssd/minio /tank/minio
   # Corfu: keep load, load-bucket and the two -zstd-20260827 snapshots for now; drop the rest
   mkdir /tank/ssd/corfu
   for d in load load-bucket load-zstd-20260827 load-bucket-zstd-20260827; do mv /mnt/corfu/$d /tank/ssd/corfu/; done
   sudo rm -rf /mnt/corfu && sudo ln -s /tank/ssd/corfu /mnt/corfu
   # Cassandra: the whole install dir (JDK, scripts, data, data.load) moves; install_dir stays /tank/cassandra
   mv /tank/cassandra /tank/ssd/cassandra && ln -s /tank/ssd/cassandra /tank/cassandra
   sudo systemctl start minio && mc ls ozonedb-local/ozonedb-ycsb | head -3   # the runners' MC_ALIAS default
   /tank/cassandra/cassandra_ctl.sh start && /tank/cassandra/cassandra_ctl.sh wait && /tank/cassandra/cassandra_ctl.sh stop
   df -h /tank/ssd
   ```
   Every consumer reaches the state through the old paths: the MinIO unit, the runner's
   `/mnt/corfu/{load,run_batch,load-bucket}`, `cassandra.install_dir`, and the sampler's
   `du` on `/tank/minio` and `/tank/cassandra/data` (an intermediate symlink is followed).

   Peak space on `/tank/ssd` (440 GB usable), in the order the tasks run:

   | After | Cassandra | OzoneDB | Total |
   |---|--:|--:|--:|
   | Task 2, 10 GB cells | 11 + 11 GB | 1.4 GB (old snapshots) | 24 GB |
   | Task 2, 100 GB load and cells | 107 + 107 GB | 1.4 GB | 216 GB |
   | Task 2 end, both 100 GB trees deleted | 11 GB (`data.load-10g`) | 0 | 11 GB |
   | Task 4, 10 GB load | 11 GB | 12 + 12 GB bucket and snapshot | 35 GB |
   | Task 7, 100 GB load and cells | 11 GB | 117 + 117 GB bucket and snapshot, log under 1 GB | 234 GB + 24 GB of 10 GB snapshots = 269 GB |
   | Task 7b, Cassandra `cz` / `az` at 10 GB | 11 + 11 GB | as above | 280 GB |

   If the 100 GB bucket comes out above 130 GB (`sO` above 1.3), delete the 10 GB
   OzoneDB snapshots before the 100 GB load. The 375 GB on `sda` is the last resort.
4. Check `/tank/cache` is a mount point on all eight clients (`mountpoint /tank/cache`).
   The runner refuses a tier root that is not one. 440 GB per client covers the 50 GiB
   tier with room.
5. Archive the results root so that this campaign's load rows are the only ones:
   ```bash
   mkdir -p bench/results/local/archive-pre-cost2
   mv bench/results/local/*insert* bench/results/local/*_rc[0-9]* bench/results/local/archive-pre-cost2/ 2>/dev/null
   ```
   Do the same on every client's `bench/results/local` (the orchestrator pulls the load
   files from there).

### Task 2. Cassandra cells, both sizes (before any build)

Cassandra did not change. The cells are re-run so that both systems share one tag, one
day and one box, and so that the corpus stops carrying `cost-20260827` rows.

```bash
TAG=cost2-$(date +%Y%m%d)
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -sd,)
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -sd,; }
# the runner's own stop pattern (run_multinode_ycsb_with_corfu.sh, stop_corfu)
ssh oliverr3@amd127.utah.cloudlab.us "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true"

# 10 GB: the snapshot is on the box today (moved to /tank/ssd in Task 1)
for mode in quorum serial; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency $mode --record-cnt 10000000 \
    --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-10g
done
for n in 2 4; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency quorum --record-cnt 10000000 \
    --workloads a --writers-list 1 --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG-cass-10g
done

# 100 GB: load (about 45 min at 40,000 puts/s, plus the save-load copy), then four cells.
# Read-only cells first, without a restore: they leave the drained load state as it is.
ssh oliverr3@amd127.utah.cloudlab.us 'cp -a /tank/cassandra/data.load /tank/cassandra/data.load-10g'   # kept for Task 7b
bash bench/scripts/local/load_multinode_cassandra.sh --writers 8 --record-cnt 100000000
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh space; du -sk /tank/cassandra/data.load'   # sC: peak, then drained
for mode in quorum serial; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency $mode --record-cnt 100000000 --no-restore \
    --workloads c --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-100g
done
for mode in quorum serial; do
  bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency $mode --record-cnt 100000000 \
    --workloads a --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-100g
done
```

A restore copies 107 GB on one SSD (about 8 min). The order above needs two restores
(before each workload-a cell) instead of four. `nodetool compact` at 100 GB takes tens of
minutes and is skipped: `sC` was 1.066 before and after `compact` at both 1 GB and 10 GB,
and the drained `data.load` after the load is the steady footprint. After the last cell
delete `/tank/cassandra/data` and `data.load` (214 GB). `data.load-10g` (11 GB) stays
for the Task 7b cells.

Task 2 needs no new build, so it runs while Task 0 (the merge) is in progress on the
laptop. It is off the critical path as long as the merge takes longer than it does.

Exit: 10 cells, `rc=0`, 0 failed ops, `cpuC` 0.06 ms per op within 10 % of
`cost-20260827`, quorum workload a at 8 writers 43,000 ops/s within 5 % at 10 GB, `sC`
1.066 within 3 % at 100 GB.

### Task 3. Sync, build, verify the native client on the cluster

1. Sync the merged tree to every node: `ansible-playbook bench/ansible/sync.yml` (the
   touch step must skip `target/`). `sync.yml` does not delete by default, so the stale
   `src/db/corfu/` on the clients is overwritten by the real one.
2. Build on every client: `bash bench/scripts/build.sh` on each, in parallel. Record
   the commit hash that `build/libOzoneDB.so` was built from.
3. Start a **fresh, empty** Corfu on `amd127` (a new log directory under `/tank/ssd/corfu`).
   Run on one client:
   ```bash
   cd build && CORFU_TEST_CLIENT=native CORFU_TEST_ENDPOINT=10.10.1.1:9090 \
     CORFU_BRIDGE_JAR=... ./runUnitTests --gtest_filter='CorfuStorageTest.*:DiskCacheStorageTest.*'
   ```
   `runUnitTests` is not built by `build.sh`. Use `cmake --build build --target
   runUnitTests`. The storage tests fail-stop on a trimmed log, which is why the server
   must be fresh. Expect 13/13 Corfu and every disk-cache test green.
4. Stop that server. The campaign's Corfu is started by the load wrapper.

Exit: eight clients at one hash, tests green on native, `corfu_native_probe` present.

### Task 4. Phase 1 at 10 GB: one load

1. There is no untrimmed load. `L0` under codec NONE is already measured on this cluster:
   the native reload of 2026-08-27 (`PLAN-native-corfu.md` §0, trimming off) left 1.3 GB
   on disk for 1 M puts, so `L0_kb_per_put` = 1.3 goes into `space.json` with that
   source. It only draws the dashed "no trimming" line.
2. Trimmed load with **16** loader processes on the load host. The put path is bound per
   writer by the sequencer (about 1,100 puts/s each), not by the host's 32 cores, so 16
   processes are the cheap way to find out whether the 100 GB load can run in 2 h
   instead of 3 h. About 12 min if it scales, 18 min if it does not.
   ```bash
   bash bench/scripts/local/load_corfu_dataset.sh --writers 16 --record-cnt 10000000 --log-trim
   ```
   Read the aggregate puts/s from the loader log. **If it is at least 13,000, Task 7
   loads with 16 writers. If it is below, Task 7 loads with 8.** The snapshot on disk is
   the dataset either way: the key partition covers the same 10 M keys, and the load
   row's coefficients are ratios.
   From its extractor row take `bucket_bytes`, `checkpoint_bytes`, `du_corfu_kb` (`L`),
   `s3_bytes_in` / dataset bytes (`wa`), `get_per_write`, `ckpt_objects / ckpt_count`
   (`k`). These are the 10 GB column of the coefficient table. `space.json` takes the
   100 GB values from Task 7.
3. Remove the two `-zstd-20260827` snapshots now.

Exit: `sO` 1.17 within 3 %, `L` at most 250 MiB, `wa` 3.4 within 15 %, `k` 5–7,
`get_per_write` at most 0.001, `slow=0 spurious=0`, and the 16-writer puts/s recorded.

### Task 5. Phase 2 at 10 GB: `h`, `cpuO`, linearizable, scaling, JNI control

```bash
RC=10000000
# Task 0b check first: one key-space-zipfian cell; stop the chain if its h is not far above 0.14
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes 104857600 \
  --workloads cz --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 300 --run-tag $TAG-10g-zipf
# h sweep and the workload-c CPU: 600 s where the cache takes minutes to fill, 300 s where it takes seconds
for c in 5368709120 671088640; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes $c \
    --workloads c --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g \
    || { echo "FAILED at lru=$c"; break; }
done
for c in 83886080 10485760 1310720; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes $c \
    --workloads c --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 300 --run-tag $TAG-10g \
    || { echo "FAILED at lru=$c"; break; }
done
# workload a at the two ratios the projection reads, 600 s
for c in 5368709120 83886080; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes $c \
    --workloads a --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g
done
# strict reads, 120 s
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --linearizable --record-cnt $RC --lru-cache-bytes 5368709120 \
  --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-10g
# server scaling, 120 s, own tag
for n in 2 4; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes 5368709120 \
    --workloads a --writers-list 1 --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG-scale
done
# JNI control on the same engine and dataset, workload a only (the CPU claim), 600 s, own tag
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --corfu-client jni --record-cnt $RC --lru-cache-bytes 5368709120 \
  --workloads a --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-jni
```

Thirteen cells with the Task 0b check. Launch the chain detached (`setsid nohup bash chain.sh > chain.log 2>&1 &`)
with `set -e`: the harness kills background chains, and a failed cell must not fall
through to the next one. The 5 GiB cache fills at 20–40 MB/s of misses and slows as it
fills, so it needs the 600 s. An 80 MiB cache fills in seconds, so 300 s is past the
fill by a wide margin, and the 100 GB twin at 600 s wins the model's tie-break.

Exit:

- `h_steady` on workload c within 0.03 of the 4 KiB curve at every ratio
  (0.679 / 0.341 / 0.216 / 0.141 / 0.032).
- 8/8 writers with an `[OVERALL]` block in every cell, 0 failed ops.
- `cpuO` on workload a at 5 GiB at most 0.65 ms, and at least 35 % below the JNI control.
- RSS per writer at most 1.5 GB plus the cache (the 5 GiB cell holds 5 GiB of blocks).
- Linearizable workload c within 3 % of the default throughput, every linearizable READ OK.

### Task 6. Tier cells at 10 GB

The five cells of `disk2-20260828`, scaled tenfold, on the native client:

```bash
run_tier() { # $1 record count, $2 tier bytes, $3 workloads, $4 tag, $5 seconds
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $1 --lru-cache-bytes 83886080 \
    --disk-cache-bytes $2 --workloads "$3" --writers-list 1 --client-hosts "$HOSTS" \
    --trial 1 --duration $5 --run-tag $4
}
run_tier 10000000 2684354560  c     $TAG-10g-tier 300   # fills in about 10 s
run_tier 10000000 5368709120  "a c" $TAG-10g-tier 600
run_tier 10000000 21474836480 "a c" $TAG-10g-tier 600   # the full tier converges as it fills, 3 to 5 min
```

The RAM cache behind the tier is 80 MiB, the 0.82 % ratio, so the tier is what is
measured (round 2 ran behind 8 MB at 1 GB, the same ratio). The mode and admission flags
are omitted on purpose: `chunk` and `frequency` are the merged defaults, and the label
must read `-dc<size>-ch64k-adm` for `--tier-variant ch64k-adm` to select the rows.

Exit:

- `disk_h` within 0.05 of 0.29 / 0.51 / 0.99 at the three ratios.
- `disk_amp` at most 16 at 5 GiB.
- `disk_fill_gets_per_op` 0 at 5 GiB and 20 GiB on workload a, `disk_punch_failed` 0.
- `cpu_O_disk` (workload a, 20 GiB) at most 0.25 ms.
- The 20 GiB workload-c cell at least 40,000 ops/s.

### Task 7. 100 GB: load, RAM cells, tier cells, join

1. Load. At the 8-writer rate (9,600 puts/s) this is about 2.9 h. If the 16-writer
   variant in Task 4 was at least 30 % faster, use `--writers 16` here. The snapshot copy
   of the 117 GB bucket adds about 10 min.
   ```bash
   bash bench/scripts/local/load_corfu_dataset.sh --writers 8 --record-cnt 100000000 --log-trim
   ```
   From its extractor row fill `space.json`: `sO` (`bucket_bytes` minus
   `checkpoint_bytes`, over 1.024e11), `wa`, `L_gb` (`du_corfu_kb`), `k`. `L0` stays the
   Task 4 value. Keep the previous `space.json` as `space-disk2.json`.
2. While the load runs (2 to 3 h, the server is busy, the laptop is not): extract the
   10 GB tags, run the model on them alone, and draft the write-up's tables from that
   preliminary projection. The 100 GB rows then only replace numbers.
3. RAM cells, 600 s, no tier. The largest cache here (800 MiB) fills in under a minute.
   The 600 s is for the tie-break against the 300 s twins at 10 GB:
   ```bash
   RC=100000000
   for c in 838860800 104857600 13107200; do
     bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes $c \
       --workloads c --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-100g
   done
   bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes 104857600 \
     --workloads a --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-100g
   ```
4. Tier cells behind an 800 MiB RAM cache (the 0.82 % ratio again). 300 s for the
   2.5 GiB tier, 600 s for the rest:
   ```bash
   run_tier100() { bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt 100000000 \
       --lru-cache-bytes 838860800 --disk-cache-bytes $1 --workloads "$2" --writers-list 1 --client-hosts "$HOSTS" \
       --trial 1 --duration $3 --run-tag $TAG-100g-tier; }
   run_tier100 2684354560  c     300
   run_tier100 26843545600 c     600
   run_tier100 53687091200 "a c" 600
   ```
   A 50 GiB tier fills at the miss rate times 64 KiB, about 300 MB/s on a SATA SSD, so it
   holds its budget after about 3 min, and the last-60 s window of a 600 s cell is past
   that. If the cumulative `h` and `h_steady` of a tier cell differ by more than 0.1,
   the cell was still warming: rerun it at 1200 s. No full tier at 100 GB: the ratio-2.1
   point comes from the 20 GiB cell at 10 GB, and a 117 GB fill does not converge in one
   cell.
5. The join numbers (`j`, `b_j`, replay time, time to first op) come from the replay line
   of every writer at every cell start. No extra cell is needed. Report the 100 GB
   values next to the 10 GB ones: both restore live log files, not SSTables, so both must
   be tens of MB and under 5 s.

The restore before each cell copies only the objects the previous cell changed (`mc
mirror --overwrite --remove`) plus the trimmed log, so a restore after a workload-c cell
is seconds and after a workload-a cell a few minutes.

Exit:

- `h_steady` at 0.82 %, 0.10 % and 0.013 % within 0.06 of the 10 GB twins.
- `disk_h` at 0.26 and 0.52 within 0.05 of the 10 GB points.
- `disk_h` at 0.026 reported. There is no prior value. Expect 0.03–0.06 under uniform keys.
- `L` at most 250 MiB and within 20 % of the 10 GB value.
- `k` 5–7.
- `sO` 1.17 within 5 %.
- `wa` reported. Expect 4–6 on four levels.
- `get_per_write` at most 0.001.
- Replay under 5 s from the checkpoint, restored bytes under 100 MB.
- `cpuO` on workload a within 20 % of the 10 GB value.
- 8/8 writers with `[OVERALL]` in every cell, 0 failed ops, `disk_punch_failed` 0.

### Task 7b. Zipfian over the key space, both sizes, both systems

The same ratios the projection reads, under `workloadcz` / `workloadaz` (Task 0b). At
10 GB the three twins run inside Task 5's chain, after the sweep. The 100 GB cells run
after Task 7. The Cassandra cells run last, at 10 GB, from the snapshot Task 2 kept.

```bash
# 10 GB twins (in the Task 5 chain; the 100 MiB one is the Task 0b check and is already done)
for c in 83886080 1310720; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt 10000000 --lru-cache-bytes $c \
    --workloads cz --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 300 --run-tag $TAG-10g-zipf
done

# 100 GB (after Task 7): the h curve, the CPU cell, one tier cell
RC=100000000
for c in 838860800 104857600 13107200; do
  bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes $c \
    --workloads cz --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 300 --run-tag $TAG-100g-zipf
done
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes 104857600 \
  --workloads az --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-100g-zipf
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt $RC --lru-cache-bytes 838860800 \
  --disk-cache-bytes 26843545600 --workloads cz --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-100g-zipf

# Cassandra, 10 GB, last: Corfu stopped (rule 8), the 10 GB snapshot restored by hand, no restore in the runner
ssh oliverr3@amd127.utah.cloudlab.us "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true"
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh stop; rm -rf /tank/cassandra/data; cp -a /tank/cassandra/data.load-10g /tank/cassandra/data'
bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency quorum --record-cnt 10000000 --no-restore \
  --workloads "cz az" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-zipf
ssh oliverr3@amd127.utah.cloudlab.us '/tank/cassandra/cassandra_ctl.sh stop'
```

Under a key-space zipfian a 100 MiB cache converges in seconds: the hot keys are re-read
within the first status intervals. 300 s is generous. The tier cell gets 600 s because
TinyLFU has skew to work with for the first time and its admission decisions take a
window to settle.

Exit:

- `h_steady`, `cz` at 100 GB: at least 0.55 at 800 MiB, 0.45 at 100 MiB, 0.35 at 12.5 MiB
  (the estimates are 0.65 / 0.54 / 0.43 for a perfect top-k cache, and an LRU under
  churn sits below them).
- `h_steady`, `cz` at 10 GB: **below** the 100 GB twin by 0.03 to 0.10. The sign is the
  check: `zeta(k) / zeta(N)` falls with N at a fixed k, and the 10 GB cell holds the
  same k in a tenfold smaller N.
- `disk_h` at the 25 GiB tier above the YCSB-zipfian cell at the same ratio (0.29 in
  round 2).
- `cpuO` on `az` within 20 % of the `a` cell at the same cache: the client does the same
  work per op, it just misses less.
- Cassandra `cz` / `az` within 5 % of the `c` / `a` cells at 10 GB: the skew changes
  nothing on a client-bound server.
- 8/8 writers with `[OVERALL]`, 0 failed ops.

### Task 8. Extraction, model, figure, write-up

```bash
python3 bench/scripts/extract_cost_coefficients.py \
    bench/results/local/$TAG-10g bench/results/local/$TAG-10g-tier \
    bench/results/local/$TAG-100g bench/results/local/$TAG-100g-tier \
    bench/results/local/$TAG-cass-10g bench/results/local/$TAG-cass-100g \
    bench/results/local --window 60 --tsv bench/results-$TAG.tsv
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/$TAG-scale \
    bench/results/local/$TAG-jni bench/results/local/$TAG-loads-notrim \
    --window 60 --tsv bench/results-$TAG-controls.tsv
python3 bench/scripts/plot/plot_cost_model.py bench/results-$TAG.tsv bench/scripts/plot/prices.json \
    --space bench/scripts/plot/space.json --tier-variant ch64k-adm \
    --out-dir bench/scripts/plot/out --table bench/results-$TAG-projection.tsv
# the key-space zipfian: its own corpus, its own run, the same space.json and prices
python3 bench/scripts/extract_cost_coefficients.py \
    bench/results/local/$TAG-10g-zipf bench/results/local/$TAG-100g-zipf bench/results/local/$TAG-cass-zipf \
    bench/results/local --window 60 --tsv bench/results-$TAG-zipf.tsv
python3 bench/scripts/plot/plot_cost_model.py bench/results-$TAG-zipf.tsv bench/scripts/plot/prices.json \
    --space bench/scripts/plot/space.json --tier-variant ch64k-adm --h-workload cz --cpu-workload az \
    --out-dir bench/scripts/plot/out-zipf --table bench/results-$TAG-zipf-projection.tsv
python3 bench/scripts/plot/overlay_projections.py bench/results-$TAG-projection.tsv \
    bench/results-$TAG-zipf-projection.tsv --out bench/results-$TAG.png
```

The zipf corpus has one tier ratio (0.26), so its tier line is an extrapolation and is
reported as a number at 10 TB, not drawn. Its Cassandra rows are the 10 GB `cz` / `az`
cells. The model's Cassandra terms are flat in D, so that is enough.

No `combine_disk_corpus.py`: every coefficient comes from `results-$TAG.tsv`. The model
must print `measured` for every source line and "4 tier ratios" for `disk_h`. If a
source prints `ASSUMED`, a cell is missing. Rerun it before writing anything. The
"measured cells" dots on the figure land at 10 GB and 100 GB. The tables were drafted
during the 100 GB load (Task 7, step 2), so this task is about one hour after the last
cell.

Write-up, as a new top section of `bench/RESULTS-cost.md` named "Campaign 2: one engine,
both systems, 10 GB and 100 GB (`$TAG`)": the coefficient table with 10 GB and 100 GB
columns and the `cost-20260827` values beside them, the throughput and CPU table for both
systems at both sizes, the `h` curve with the twin-ratio check, the tier table with the
new 0.026 point, the JNI control pair, the projection at the decades, the crossover band
from §2 against the measured one, the key-space zipfian line with one paragraph on what
YCSB's `zipfian` is (§1, last bullet) and why `h` under it is not scale-free, the caveats
(one trial, no full tier at 100 GB, one tier ratio on the zipf line). Update the "Findings" numbers that the campaign changes, in place, with a date.
Commit the three TSVs, the projection TSV, the figure, `space.json` and `prices.json`
(re-read the prices, update `as_of`).

## 6. Cell count and time budget

A 600 s cell costs about 14 min with the restart and the restore, a 300 s cell about
9 min, a 120 s cell about 4 min.

| Task | Cells | Time | On the critical path? |
|---|--:|---|---|
| 1. cluster check, server SSD, archive | | 45 min | yes, first |
| 0. merge, label checks, push (laptop) | | 2–3 h | yes, in parallel with Task 2 |
| 0b. `zipfian_keyspace` branch, two workload files, `--cpu-workload`, overlay script (laptop) | | 45 min | yes, after Task 0, still in parallel with Task 2 |
| 2. Cassandra: 6 cells at 10 GB, 100 GB load, 4 cells (2 restores) | 10 | 35 + 55 + 30 min = 2 h | no: runs during Task 0 |
| 3. sync, build, tests on a fresh server | | 40 min | yes |
| 4. one 10 GB load, 16 writers | | 12–18 min | yes |
| 5. Task 0b check 1, sweep 2 x 600 s + 3 x 300 s, workload a 2, linearizable 2, scaling 2, JNI 1, `cz` twins 2 | 15 | 9 + 28 + 27 + 28 + 8 + 8 + 14 + 18 = 2 h 20 | yes |
| 6. tier at 10 GB, 1 x 300 s + 4 x 600 s | 5 | 65 min | yes |
| 7. 100 GB load, then 4 RAM cells and 4 tier cells (7 x 600 s + 1 x 300 s) | 8 | 2–3 h + 1 h 50 | yes |
| 7b. `cz` / `az` at 100 GB (3 x 300 s + 2 x 600 s), Cassandra `cz` / `az` at 10 GB | 7 | 55 + 12 min = 1 h 10 | yes |
| 8. extraction, model, write-up (tables drafted during the 100 GB load) | | 1 h | yes |
| **Total** | **45** | **about 13.5 h from Task 1 to the write-up**, 11 h of OzoneDB cluster time | |

Against the first version of this plan (14 h on the cluster, then 2.5 h of write-up,
after a 2–3 h merge: about 19 h in sequence), the schedule above is one long day plus a
morning. What was cut and why it costs no paper number:

| Cut | Saved | What it costs |
|---|--:|---|
| Cassandra runs during the merge, not after it | 2 h of wall clock | nothing: Task 2 needs no new build |
| Read-only Cassandra cells first, `--no-restore`, no `nodetool compact` at 100 GB | 45 min | `sC` from the drained snapshot; it was 1.066 before and after `compact` at 1 GB and 10 GB |
| No untrimmed 10 GB load | 20 min | `L0` from the 2026-08-27 native reload (1.3 GB per 1 M puts), same engine, same codec |
| One 10 GB load at 16 writers instead of an 8-writer load plus a 16-writer trial | 20 min, and up to 1 h on the 100 GB load | none if 16 writers scale; the 8-writer fallback is the old plan |
| 300 s for cells whose cache fills in seconds; 600 s instead of 900 s at 100 GB | 1 h 10 | none: every window sits well after the fill, and the warming check in §7 catches the exception |
| JNI control on workload a only | 15 min | the workload-c repeat; the twin-ratio cells at 100 GB are the repeat now |
| Write-up drafted during the 100 GB load | 1 h 30 of wall clock | nothing |

Two detached chains. Task 2 (Cassandra) starts as soon as Task 1 is done, while Tasks 0
and 0b are in progress. Tasks 4 to 7b follow as one chain of two loads, 33 OzoneDB cells
and the two Cassandra `cz` / `az` cells (about 9.5 h). The server is idle between them.

### Optional cuts, not applied

Each of these is a paper number lost. Take them only if the schedule above still does not
fit.

| Cut | Saves | What the paper loses |
|---|--:|---|
| Drop the 100 GB Cassandra load and cells | 1 h 25, but only off the critical path if the merge takes under 2 h | the measured Cassandra dot at 100 GB on the figure; every Cassandra coefficient is flat in D and the 10 GB cells give them |
| Drop the 5 GiB and 640 MiB cells at 10 GB (workload c twice, workload a once) | 45 min | `h` at 52 % and 6.6 %, which the projection uses only below 30 GB; the "hot set fits" throughput row; `cpu_O` becomes the median of the two low-ratio workload-a cells, which is the more conservative value for the TB range |
| Drop the scaling cells, both systems | 16 min | the server-scaling table (OzoneDB sublinear in writers, Cassandra linear) |
| Drop the linearizable pair | 8 min | the "strict reads cost nothing on OzoneDB" row at 10 GB |
| A multi-host loader (`--writer_offset` / `--writers_total` in `load_local_ycsb_multiproc.py`, fan-out in `load_corfu_dataset.sh`) | up to 2 h on the 100 GB load, if 16 writers on one host do not scale | 1–2 h of tooling before Task 4; the loader partitions keys over its local `--num_writers` only today (`load_local_ycsb_multiproc.py:593`) |

## 7. What to watch

Everything in `PLAN-cost.md` "What to watch" holds. Add what the five campaigns since
then taught, and what 100 GB adds:

- **A JVM abort leaves `rc=0` at every layer.** Count the writers with an `[OVERALL]`
  block per cell (`grep -l OVERALL *.result | wc -l`) before trusting a sum. Every
  workload-a cell of `cost-20260827` lost 1 to 4 writers this way.
- **The bucket restore must force-copy `LATEST`.** `start_corfu` does since `1127b7cc`.
  A writer that dies at open with "LATEST that cannot be read" is that bug back.
- **`CorfuStorageTest` fail-stops on a trimmed log.** Run it against a fresh server
  (Task 3), never the loaded one.
- **The native client reads codec NONE only.** Every snapshot under `/mnt/corfu/` on
  2026-08-28 is ZSTD. Never restore one of them under the merged engine.
- **The 100 GB load is the long pole** (about 3 h). Launch it detached, in its own
  chain step with `set -e`, and check `slow=0 spurious=0` and the puts/s in the loader
  log before the cells start. A load that dies halfway leaves a bucket and a log that do
  not match. Wipe both and reload.
- **Space on `/tank/ssd`.** `df -h /tank/ssd` before every load. The peak is 365 GB
  (Task 1 table). A full disk under MinIO fails PUTs mid-cell and looks like a compaction
  bug.
- **Cold-start windows.** The last-60 s window is what is reported. A cache up to
  800 MiB fills in under a minute, a 5 GiB cache in minutes, a 50 GiB tier in about
  3 min. Check `h_steady` against the cumulative `h`: a cell whose two values differ by
  more than 0.1 is still warming. Rerun it at twice the duration, do not accept it.
- **The h sweep's `h_steady` assumes one lookup per read.** Workload a does about 1.3.
  Quote the counter `h` for workload a, `h_steady` for workload c.
- **A whole-file read is served from the tier only when every chunk is present.** That is
  the 6 % workload-a deficit at the full tier in round 2. It is expected, not a regression.
- **`disk_fill_failed` is only meaningful in chunk mode.** Every tier cell here is chunk
  mode, so it must read 0.
- **`DB::openDB` on an empty stream takes 6 s** on both clients. Not a regression. At
  100 GB the open-time SSTable scan lists about 1,800 objects. If the time to first op
  grows past 15 s, report it as the join cost, not as a fault.
- **The load samples must carry `_rc<N>`.** The 10 GB load of `cost-20260827` overwrote
  the 1 GB sample. The wrappers do this now. Task 1 archives the old files so that the
  root holds one load per size, and Task 4 moves the untrimmed load out.
- **Orchestrator ssh needs `ServerAliveInterval`** (in place) and the chain needs to be
  detached from the harness (`setsid nohup`).
- **The cluster is shared.** Check for another session's drivers before every chain
  (Task 1, step 1), kill only pids this job started, keep them in a file.
- **The trimmer is global writer 0**, the first writer on the first host. The scaling
  cells with `hosts_n 2` keep `amd160` first, so the trimmer does not move.
- **`zipfian_keyspace` computes its normalizer at process start**: a loop over N with a
  `pow` per key, seconds at 100 M. The first status lines of an `az` / `cz` cell show
  0 ops for those seconds. Not a fault.
- **Under a key-space zipfian `h` is not scale-free.** The 10 GB twin sits below the
  100 GB cell by design (Task 7b exit). Do not "fix" it, and do not average the two.
- **The Cassandra `cz` / `az` cells restore by hand** from `data.load-10g` with
  `--no-restore`, and Corfu must be stopped before them (rule 8). The runner's own
  restore copies the 100 GB `data.load`, which Task 2 deleted, so the flag is not
  optional.
- **Symlinked state directories.** `/tank/minio`, `/mnt/corfu` and `/tank/cassandra` are
  symlinks into `/tank/ssd` after Task 1. `rm -rf /mnt/corfu/run_batch/` in the runner
  follows the link into the directory, which is what is wanted. Never `rm -rf` the link
  itself with a trailing slash omitted.
- **One trial.** Differences under 5 % are not resolved. The three twin-ratio cells at
  100 GB are the only repeat of a measurement: their `h_steady` must agree with the 10 GB
  cells within 0.06, which is the noise floor the write-up quotes.

## 8. Exit criteria for the campaign

| Check | Pass |
|---|---|
| Every cell `rc=0`, 0 failed ops, 8/8 writers with `[OVERALL]` (2/2 and 4/4 in scaling) | required |
| `cpuO`, workload a, 8 writers, 600 s, 5 GiB cache, 10 GB | at most 0.65 ms, at least 35 % below the JNI control |
| `cpuO`, workload c, 8 writers | at most 0.25 ms |
| RSS per writer, workload a, 8 writers, 80 MiB cache | at most 1.5 GB (phase 5: 1.44 GB) |
| `h_steady`, workload c, 10 GB, five ratios | within 0.03 of 0.679 / 0.341 / 0.216 / 0.141 / 0.032 |
| `h_steady`, workload c, 100 GB, three twin ratios | within 0.06 of the 10 GB values |
| `disk_h` at 0.26 / 0.52 / 2.1 (10 GB) | within 0.05 of 0.29 / 0.51 / 0.99 |
| `disk_h` at 0.26 / 0.52 (100 GB) | within 0.05 of the 10 GB points |
| `disk_h` at 0.026 (100 GB) | reported; the model prints "4 tier ratios" |
| `get_per_write` (both trimmed loads) | at most 0.001 |
| `L` (10 GB and 100 GB) | at most 250 MiB, and the two within 20 % |
| `L0` under codec NONE (10 GB, untrimmed) | 1.2–1.4 KB per put |
| `sO` at 100 GB; `wa` at 100 GB | 1.17 within 5 %; reported |
| Cassandra quorum, workload a, 8 writers, 10 GB; `cpuC`; `sC` at 100 GB | 43,000 ops/s within 5 %; 0.06 ms within 10 %; 1.066 within 3 % |
| `h_steady`, `cz`, 100 GB, 800 MiB / 100 MiB / 12.5 MiB | at least 0.55 / 0.45 / 0.35 |
| `h_steady`, `cz`, 10 GB twins | below the 100 GB values by 0.03 to 0.10 |
| Cassandra `cz` / `az` at 10 GB | within 5 % of `c` / `a` |
| Model sources | every line `measured`, none `ASSUMED`, in both runs |
| Zipf run, 10 TB, no tier, 16 GB cache | at most $3,500 (estimate $3,100); crossover of the 4 GB line reported (estimate 2 to 3 TB) |
| 10 TB, no tier, 16 GB cache | at most $5,750 (prediction $5,712) |
| 10 TB, 2 TB tier | at most $4,950 (prediction $4,915); state whether the line is below Cassandra EBS ($4,742) or not, with the one-trial caveat |
| 100 TB, 2 TB tier | reported against $7,759; the new 0.026 point decides it |

## 9. Not in this plan

- The 1 GB cells. Every 1 GB number stays in `RESULTS-cost.md` as history.
- A full tier (ratio above 1) at 100 GB. It needs a 117 GB fill per cell.
- Trials 2 and 3 of the native phase 5, and the phase 4 restart and `ss -K` tests
  (`PLAN-native-corfu.md`).
- The 16 KiB tier entries at three ratios and the shared cross-client tier
  (`RESULTS-cost.md`, "Disk-cache tier, round 2", what to do next). The skewed-key
  cell from that list is Task 7b now.
- A three-ratio tier sweep under the key-space zipfian. Task 7b has one ratio, so the
  zipf tier line is a number, not a curve.
- The real S3 check (`PLAN-cost.md` phase 6).
- A profile of the native client (phase 0 of `PLAN-native-corfu.md`).
- A multi-host loader. The 100 GB load runs 8 or 16 processes on one client. Spreading
  it over the eight hosts cuts the 3 h to about 1 h. See the optional cuts in §6: the
  loader needs a global writer offset first.
- An extractor check that counts writers with an `[OVERALL]` block per cell and flags a
  short cell. This is still by hand (§7, first bullet). It is a two-hour change and the
  first thing to add if Task 5 has to be re-run.
- The unallocated 375 GB on each node's `sda`. Partitioning the boot disk is left out on
  purpose. `sdb` on the server carries the campaign.
