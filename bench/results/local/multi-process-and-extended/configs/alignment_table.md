# Phase 1 Alignment Table — Multi-Process Writer Scaling

> **Update 2026-04-29:** Phase 1 design pivoted from "each process owns its own DB" (original prompt) to **shared DB** (all N processes open the same on-disk database simultaneously). This makes the experiment exercise each system's actual multi-writer coordination protocol. Two source patches and two runtime overrides for SQLite were required; see "Source patches" and "SQLite knobs" sections below. Original independent-DB sweep preserved at `phase1-multi-process-INDEPENDENT-DB/`.

## Source patches (required for shared-DB SQLite)

1. **YCSB-cpp SQLite VFS made configurable** — `sqlite/sqlite_db.cc:190` previously hardcoded `vfs=unix-excl`, which holds an OS-level exclusive lock on the DB file for the lifetime of the connection. Now reads `sqlite.vfs` property, default `unix-excl` (preserves previous multi-thread behavior).

2. **busy_timeout applied immediately on open** — `sqlite/sqlite_db.cc` now calls `sqlite3_busy_timeout()` via the C API right after `sqlite3_open_v2()`. Without this, `PRAGMA cache_size` (the first PRAGMA in `SetPragma()`) immediately fails with `SQLITE_BUSY` if any other process is mid-init.

## SQLite knobs (Phase 1 multi-process deviations from multi-thread)

- `sqlite.locking_mode=NORMAL` (overrides `EXCLUSIVE` in baseline `sqlite-trunk.properties` / `sqlite-bcw.properties`). EXCLUSIVE has the first connection grab the lock and starve all others.
- `sqlite.vfs=unix` (overrides default `unix-excl`). The default Unix VFS uses POSIX advisory locks and supports concurrent multi-process access.

These two overrides are passed via `-p` flags by the orchestrator. All other properties unchanged from the multi-thread alignment.

## OzoneDB knobs (no patches needed)

- `task_prefix` is loaded by `Metadata` but never referenced after — writer identity comes from per-process auto-generated fingerprints (UUID + timestamp). All N processes can safely use identical config.
- `mode=1` (MultipleProcesses) — already the default in `shared_config_rocksdb_base.json`.
- All processes point at the same `db_path`. Per-process config files are still written (per-process paths) only for log naming clarity; content is identical for all N within a cell.
- Multi-process safety primitives: append-only task log + heartbeat-based dead-task detection. No file locks needed.



This table records the per-system configuration for the Phase 1 multi-process sweep and how each knob is matched (or deliberately not) across systems and against the prior multi-thread sweep.

Source-of-truth for prior multi-thread alignment: [REPORT.md §1](/users/Xinying/ozonedb/bench/results/local/fig2-multi-writer-local/REPORT.md). This table mirrors that, with deltas explicitly noted.

## 1. Per-instance memory budget

| System    | Knob                          | Value                  | Notes |
|-----------|-------------------------------|------------------------|-------|
| ozonedb   | LRU cache (hardcoded)         | 32 MB                  | Hardcoded at [`ozonedb/src/db/db.cpp:21`](/users/Xinying/ozonedb/src/db/db.cpp#L21). Same as multi-thread; no JSON knob. JNI buffers + JVM heap drive RSS independently. |
| ozonedb   | log_file_size_limit           | 32 MB                  | From `shared_config_rocksdb_base.json`. Per-writer task log; with N=1 process this is the only writer. |
| trunkcpp  | sqlite.cache_size             | -131072 (128 MB)       | Same as multi-thread. |
| trunkcpp  | sqlite.mmap_size              | 0                      | Disabled — keeps memory budget honest, matches multi-thread. |
| bcw2      | sqlite.cache_size             | -131072 (128 MB)       | Same as multi-thread. |
| bcw2      | sqlite.mmap_size              | 0                      | Disabled. |
| bcw2      | sqlite.journal_size_limit     | 16 MB                  | WAL2 rollover threshold, unchanged from multi-thread. |

**Policy:** fixed-per-instance. Total RAM scales with N. Confirmed with user 2026-04-29.
At N=16: total budget across all instances = 16 × 128 MB = 2 GB per system (SQLite); OzoneDB's actual LRU is 32 MB × 16 = 512 MB but RSS is JVM-driven. Comfortable on 125 GB host.

## 2. Durability / WAL

| System    | Knob                              | Value      | Notes |
|-----------|-----------------------------------|------------|-------|
| ozonedb   | per-writer task log               | always     | One task log per process (since each process has its own DB). |
| ozonedb   | sync semantics                    | f-sync after each commit | Matches multi-thread. |
| trunkcpp  | sqlite.synchronous                | FULL       | Same as multi-thread. |
| trunkcpp  | sqlite.journal_mode               | WAL        | Same as multi-thread. |
| bcw2      | sqlite.synchronous                | FULL       | Same as multi-thread. |
| bcw2      | sqlite.journal_mode               | WAL2       | Required for BEGIN CONCURRENT. |
| bcw2      | sqlite.begin_concurrent_transactions | true    | Same as multi-thread. |

## 3. Compaction / LSM (n/a for SQLite)

| System    | Knob                          | Value                  | Notes |
|-----------|-------------------------------|------------------------|-------|
| ozonedb   | level_size                    | [256MB, 2.56GB, 25.6GB, …] (10× multiplier) | From `shared_config_rocksdb_base.json`; unchanged. |
| ozonedb   | level_file_size_limit         | 64 MB per level        | Unchanged. |
| ozonedb   | max_level                     | 7                      | Unchanged. |
| ozonedb   | compaction_policy             | 0 (leveled)            | Unchanged. |

## 4. Block / page size & filters

| System    | Knob                          | Value                  | Notes |
|-----------|-------------------------------|------------------------|-------|
| ozonedb   | block_size                    | 4 KB                   | Implicit in RocksDB-style storage. |
| ozonedb   | bloom_bits                    | 10 bits/key            | Unchanged. |
| trunkcpp  | sqlite.page_size              | 4096                   | Same as multi-thread. |
| bcw2      | sqlite.page_size              | 4096                   | Same as multi-thread. |

## 5. Dataset shape

| Item                     | Value                                          | Delta vs multi-thread |
|--------------------------|------------------------------------------------|------------------------|
| Records per DB           | 1,000,000                                      | **Same** (each process gets a full 1M-record DB; not sharded). |
| Record size              | 1 KB (`fieldcount=10`, `fieldlength=100`)      | Same. |
| Key distribution         | Zipfian                                        | Same. |
| Workload A file          | `workloads/workloada_hctree`                   | Same as multi-thread (1 KB fields). |
| Workloads B/C/D/F files  | `workloads/workload{b,c,d,f}`                  | Same. |
| Total on-disk dataset    | N × 1M records (= 1.2–1.4 GB × N)              | **Multi-thread had 1M total; multi-process has N × 1M.** This is the cleanest engine-level apples-to-apples (per-process working set identical), at the cost of N× disk volume. |

## 6. Run protocol

| Item                     | Value         | Notes |
|--------------------------|---------------|-------|
| maxexecutiontime         | 120 s         | Same as multi-thread. |
| Repeats per cell         | 3 (re-run if rel_stddev > 10%) | Same. |
| Cache drop between cells | yes (`echo 3 > drop_caches` once per cell, before workers spawn) | Same as multi-thread. |
| Cleanup between cells    | tear down all N work DBs and aux files | New: must clean N copies, not 1. |
| Straggler kill           | pkill matching system binary + orchestrator-tracked PIDs | Adapted from multi-thread. |
| Start synchronization    | back-to-back `subprocess.Popen()` of N workers; ~ms-level skew over 120 s window <0.1% | Acceptable; no file barrier required. |

## 7. Storage substrate

| Item             | Value                          | Notes |
|------------------|--------------------------------|-------|
| Mount            | /tank (single ZFS dataset)     | Same as multi-thread. |
| All N DB copies  | under `/tank/ycsb_data/multi-proc-run/{system}_p{N}_proc{i}/` | New parent dir; tear down between cells. |
| ZFS arc          | host-level                     | drop_caches is page-cache only; ZFS ARC is a separate pool but stays consistent across cells. |

## 8. Per-process isolation invariants (verified before launch)

| Invariant | How enforced |
|-----------|--------------|
| Each process has its own DB directory/file | `{work_path}_proc{i}` per process, copied from cached_DB at cell start. |
| No shared SQLite WAL or shm files across processes | Different DB files → different sidecars. |
| No shared OzoneDB config file | Per-process `shared_config_rocksdb_proc{i}.json` (one per process per cell). Avoids the orchestrator-shared-config-file collision in [run_fig2_sweep.py:211-228](/users/Xinying/ozonedb/bench/scripts/local/run_fig2_sweep.py#L211). |
| No shared lock files | RocksDB excluded from Phase 1 (would have hit `LOCK` file issue); SQLite locks are per-DB-file. |
| Workload generator race-free | Pre-generate all workload files before any worker spawns. |
| Shared classpath / jar caches | Maven `~/.m2` is read-only at runtime; concurrent reads are safe. |

## 9. Background work

| System    | Background threads            | Per-process effect |
|-----------|-------------------------------|---------------------|
| ozonedb   | compaction service per writer | N processes × per-writer compactor — N-fold compaction concurrency on disk. |
| trunkcpp  | none (in-thread WAL)          | No background. |
| bcw2      | none (WAL2 in-thread)         | No background. |

## 10. Known fairness caveats

- **OzoneDB LRU cache hardcoded at 32 MB** — same as multi-thread. Already documented as not biasing in OzoneDB's favor.
- **Per-process disk pressure**: at N=16, OzoneDB runs 16 independent compaction services hammering one ZFS pool. SQLite/BCW2 do not. This is a *legitimate* property of the comparison (you'd see the same in a real shared-nothing deployment) but it is a deviation from the multi-thread study (which had one engine on one disk at all T).
- **JVM warmup**: 16 OzoneDB JVMs starting simultaneously takes seconds; the 120 s measurement window absorbs this but the early seconds may underrepresent steady-state. Acceptable; matches multi-thread sweep's behavior at T=1.
