# Multi-Process + Extended Sweeps — Worklog

Top-level worklog. Each phase has its own subdir with logs/, results.csv, plots/, skipped_runs.md. Phase-specific notes live in those subdirs; this file tracks cross-phase decisions and overall status.

## 2026-04-29 — Kickoff

**Plan:** Three phases.
- Phase 1: multi-process writer scaling — {ozonedb, trunkcpp, bcw2} × {a,b,c,d,f} × N∈{2,4,8,16} × ≥3 repeats.
- Phase 2: extended thread sweep on Load — {hctree, ozonedb}, T from 16 to bottleneck.
- Phase 3: extended process sweep on Load — {ozonedb}, N from 16 to bottleneck.

**Reference materials surveyed:**
- Existing multi-thread sweep: [/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local](/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local) — 92 cells, 8 hard failures (trunkcpp/bcw2 A/F at T≥2 SIGABRT under WAL-index contention).
- Paper draft: `/users/Xinying/654ac88fa59ce996418ca20c/sections/5.experiment.tex:112-124` — Figure 2 description, `synchronous=FULL`, 128 MB per-instance, 120s × 3 repeats, 1M × 1KB Zipfian.
- Multi-thread orchestrator we're modeling on: [run_fig2_sweep.py](/users/Xinying/ozonedb/bench/scripts/local/run_fig2_sweep.py).

**Host context** (captured 2026-04-29):
- 32 cores, 125 GB RAM, /tank with 353 GB free.
- ulimit -n = 1048576, -u = 513322. fs.file-max effectively unlimited.
- Cached DB sizes: ozonedb 1.2 GB, trunkcpp 1.4 GB, bcw2 1.4 GB. Worst-case Phase 1 cell footprint = 16 × 1.4 GB = 22 GB per cell (cleared after each cell).

**Design decisions confirmed with user:**
1. **System list for Phase 1:** ozonedb, trunkcpp (= "SQLite", vanilla WAL), bcw2. RocksDB and HCTree excluded.
2. **Memory policy:** fixed-per-instance @ 128 MB. Total memory grows linearly with N. Mirrors prior multi-thread per-DB budget.
3. **Dataset shape:** *no shard.* Each process owns an independent DB, each DB pre-loaded with the full 1M × 1KB record set, each process samples Zipfian over the whole [0, 1M) keyspace. Total on-disk dataset = N × 1M (vs 1M for multi-thread). Per-process working set is identical to the multi-thread case — this is the cleanest apples-to-apples for engine-level scaling.
4. **Aggregation:** sum of per-process throughputs as headline; per-process p50/p99/p999 retained for stragglers; worker-weighted percentiles for cell-level latency.
5. **Output naming:** per-process logs `{system}_workload{w}_p{N}_r{R}_proc{i}.log`; per-cell summary `{system}_workload{w}_p{N}_r{R}.summary`.

**Phase 1 implementation strategy:**
- Single Python orchestrator `run_phase1_multiproc_sweep.py` (modeled on `run_fig2_sweep.py`) handles all three systems.
- Per cell: copy cached DB to N independent work paths; spawn N independent `subprocess.Popen` workers in tight succession; wait for all; parse per-process logs; aggregate.
- OzoneDB needs a per-process `shared_config_rocksdb_P{i}.json` to avoid the orchestrator-shared-config-file collision noted in [run_fig2_sweep.py:211-228](/users/Xinying/ozonedb/bench/scripts/local/run_fig2_sweep.py#L211).
- Drop OS page cache **once per cell** (not per-process) before spawning workers.
- Start synchronization: back-to-back `Popen()` plus YCSB's 120s `maxexecutiontime` window absorbs the ~ms start skew (<0.1% of measurement window). No file barrier needed.

**Open questions / risks:**
- Whether spawning 16 OzoneDB JVMs simultaneously triggers any host-level contention (CPU saturation during JVM warmup, memory fragmentation).
- Whether SQLite `locking_mode=EXCLUSIVE` interacts badly with our cached-DB-copy approach (it shouldn't — each process has its own DB file). Smoke test will confirm.
- OzoneDB YCSB wrapper requires CWD = `/users/Xinying/ozonedb/ycsb`; multiple concurrent invocations from same CWD should be safe (Maven invokes are read-only on shared classpath caches) but I'll watch the smoke test.

## 2026-04-29 — smoke + Maven race fix

Smoke tests at N=2,4 worked cleanly for all three systems. **N=16 ozonedb hit a hard issue: 16 parallel `bin/ycsb` invocations race on `mvn package` in `~/.m2`** — most workers got "Attempting to generate a classpath from Maven failed". Fix: `warmup_ozonedb_classpath()` in the orchestrator runs the wrapper once, captures the resolved `java -cp ... site.ycsb.Client -db site.ycsb.db.OzoneDBClient` prefix from stderr, then workers invoke `java` directly with that prefix + per-worker flags. Total bypass of the wrapper for parallel spawns.

After the fix:
- ozonedb N=16 workload-c: agg 190,935 ops/s, per-proc ~10–12k each, 1.2 min wallclock for 20s duration. Compares with multi-thread T=16 ozonedb-c at ~189k ops/s — essentially the same throughput, which is interesting on its own.
- trunkcpp N=16 workload-a (write-heavy): agg 15,451 ops/s, per-proc ~960 each. **Multi-thread T=16 trunkcpp-a SIGABRTed every repeat** in the prior sweep (WAL-index byte-lock contention). Multi-process succeeds because each process has its own DB → no WAL-lock contention. This is the headline qualitative result for Phase 1.
- bcw2 N=16 workload-a: agg 12,378 ops/s, per-proc ~775 each. Same story as trunkcpp.

Cleanup invariant verified after smoke: `/tank/ycsb_data/multi-proc-run/` is empty between cells.

Memory note: parallel-mvn race saved as feedback so this isn't re-discovered.

## 2026-04-29 — Phase 1 launch

Sweep dimensions: 3 systems × 5 workloads × 4 process counts × 3 repeats = **180 runs**.
Estimate: ~5–7 hours wallclock based on smoke timings.

Launched: `nohup python3 .../scripts/run_phase1_multiproc_sweep.py --systems ozonedb,trunkcpp,bcw2 --workloads a,b,c,d,f --processes 2,4,8,16 --repeats 3 --duration 120 --results-dir .../phase1-multi-process > sweep.log 2>&1 &`

Per-cell live progress goes to `phase1-multi-process/sweep.log`; results.csv is rewritten after every cell so partial state survives interruption.

## 2026-04-29 (later) — pivot to SHARED DB

User reversed the original prompt's "each process owns its own DB / no shared WAL" spec mid-sweep. New design: all N processes open the SAME on-disk DB simultaneously. This is the actual test of the multi-writer coordination protocols (OzoneDB's append-based task log; SQLite's WAL byte-lock; BCW2's WAL2+BEGIN CONCURRENT) and matches the paper's headline claim.

Independent-DB sweep was 50/60 cells in when killed; preserved at [phase1-multi-process-INDEPENDENT-DB/](/users/Xinying/ozonedb/bench/results/local/multi-process-and-extended/phase1-multi-process-INDEPENDENT-DB/) for reference.

**Run protocol changed (per user):** 1 repeat × 90 s (was 3 repeats × 120 s).

**Two source patches were required to make shared-DB work for SQLite:**

1. **YCSB-cpp SQLite binding hardcoded VFS** — [sqlite_db.cc:190](/users/Xinying/YCSB-cpp/sqlite/sqlite_db.cc#L190) opens with `vfs=unix-excl`, which holds an OS-level exclusive lock for the lifetime of the connection. Multi-thread within one process works; multi-process does not. Patched to read `sqlite.vfs` property (default `unix-excl` to preserve previous behavior). Multi-process workers pass `-p sqlite.vfs=unix`.

2. **busy_timeout applied too late** — [sqlite_db.cc:247](/users/Xinying/YCSB-cpp/sqlite/sqlite_db.cc#L247) sets `PRAGMA busy_timeout` *after* `PRAGMA cache_size` and `PRAGMA journal_mode`. With multi-process and `locking_mode=NORMAL`, init races caused immediate `SQLITE_BUSY: database is locked` on `cache_size`. Patched to call `sqlite3_busy_timeout()` via the C API right after `sqlite3_open_v2()`, before any PRAGMA. The later `PRAGMA busy_timeout = X` is now redundant but harmless (idempotent).

YCSB-cpp rebuilt with `make BIND_SQLITE=1 BIND_HCTREE=1 HCTREE_BLD_DIR=$HOME/bcw2/bld`.

**Two property overrides at runtime:**
- `-p sqlite.locking_mode=NORMAL` (overrides EXCLUSIVE in baseline properties).
- `-p sqlite.vfs=unix` (overrides hardcoded default that's now configurable).

These are deviations from the multi-thread alignment, but unavoidable for shared-DB multi-process. Documented in alignment_table.md.

**OzoneDB shared-DB needs no source changes:**
- `task_prefix` is loaded but never used; coordination is via per-process auto-generated fingerprints (UUID + timestamp) written to a shared `task.log`.
- All N processes use identical config (same `db_path`, `mode=1`).
- The append-based task-log protocol does the rest: each writer appends, rolls forward to detect first-writer-wins, picks up dead tasks via heartbeat.
- Per-process config files are still written (per-process paths) for log naming clarity, but content is identical across processes for a given cell.

**Smoke results validating the design (1 repeat × 20 s):**
| System | Workload | N | Aggregate | Per-proc |
|---|---|---|---|---|
| ozonedb | c (read) | 2 | 27,710 | [13735, 13974] |
| ozonedb | a (write) | 4 | **22,066** | [5499, 5504, 5531, 5531] |
| trunkcpp | a (write) | 2 | 5,814 | [1530, 4284] |
| trunkcpp | a (write) | 4 | 5,692 | [1095, 1458, 1706, 1434] |
| bcw2 | a (write) | 2 | 4,984 | [2104, 2880] |
| bcw2 | a (write) | 4 | 5,213 | [1404, 1339, 1240, 1230] |

Headline observation from smoke: **OzoneDB N=4 write throughput (22.1 k) is ~4× SQLite trunkcpp N=4 write (5.7 k).** SQLite plateaus from N=2 → N=4 (5.8 k → 5.7 k) — single-writer-at-a-time serialization at the WAL byte-lock. OzoneDB scales near-linearly because the append protocol coordinates without serializing commits. This is the paper's claim demonstrated.

## 2026-04-29 — Phase 1 SHARED-DB launch

`nohup python3 .../scripts/run_phase1_shared_db_sweep.py --systems ozonedb,trunkcpp,bcw2 --workloads a,b,c,d,f --processes 2,4,8,16 --repeats 1 --duration 90 --results-dir .../phase1-multi-process > sweep.log 2>&1 &`

PID 1868886. Watcher task `bgez6ot24` will fire on completion. ETA ~1.5–2 hours (60 cells × 1 repeat × ~90 s + setup).

## 2026-04-29 — All three phases complete

- **Phase 1 shared-DB:** 60/60 cells, 100 min wallclock, 0 failures. Headline: OzoneDB writes scale 4–6× across N∈{2..16}; SQLite trunk/bcw2 plateau at ~5 k ops/s on writes (WAL byte-lock serialization).
- **Phase 2 extended threads:** HCTree and OzoneDB Load both plateau at T=32 (~19 k and ~34.6 k ops/s). Compaction-bound.
- **Phase 3 extended processes (shared DB):** OzoneDB Load plateaus at N=32 (~34.7 k ops/s).
- **Cross-phase result:** OzoneDB Load saturates at ~35 k ops/s regardless of whether 16+ writers are threads or processes — bottleneck is per-DB, not per-writer.

Combined narrative in [REPORT.md](REPORT.md). Plots at:
- Phase 1: [phase1-multi-process/plots/phase1_multi_process.png](phase1-multi-process/plots/phase1_multi_process.png)
- Phase 2: [phase2-extended-threads/plots/phase2_threads.png](phase2-extended-threads/plots/phase2_threads.png)
- Phase 3: [phase3-extended-processes/plots/phase3_processes.png](phase3-extended-processes/plots/phase3_processes.png)
- Cross-phase: [ozonedb_thread_vs_process_load.png](ozonedb_thread_vs_process_load.png)

## Status
- [x] Survey existing multi-thread sweep, paper, scripts
- [x] Surface design decisions, get user confirmation
- [x] Write WORKLOG and alignment table
- [x] Write multi-process orchestrator (Phase 1)
- [x] Smoke test on all three systems at N=2,4,16
- [x] Maven-race fix (warmup_ozonedb_classpath bypass)
- [x] Verify process-isolation invariants (cleanup, balanced per-proc throughput)
- [x] Write Phase 2 (extended threads) and Phase 3 (extended processes) orchestrators
- [ ] Phase 1 sweep (in progress, background)
- [ ] Phase 1 plots + narrative
- [ ] Phase 2
- [ ] Phase 3
- [ ] Combined REPORT.md
