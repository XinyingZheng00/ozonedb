# Plan: a compaction-aware block cache (branch `worktree-plan-cost`)

**Status (2026-08-28):** DONE and measured. Commits `9ddaf573` (C, counters), `d2c61f43`
(A, drop on REMOVE), `5760dff1` (B, warm worker), `5867a321`, `74099ed4` (dead-block scan
on a fresh View snapshot), `5c98526e` (runner pass-through `--cache-warm-max-level` /
`--cache-warm-max-fraction`), `1127b7cc` (bucket restore force-copies `LATEST`),
`41fd65a0` (loader sample name), `4facdec7` (affinity for log-input compactions),
`f987cd54` (counters before start count as disabled), `fc9f0d5c` (default-mode `get`
retries against a fresh view when a listed file could not be read; `-cache3` cells read 0
failed). Campaigns `cost-20260828-cache` and
`-cache2`, write-up in the section "Compaction-aware block cache" of `RESULTS-cost.md`,
rows in `results-cost-20260828-cache.tsv`. Results against the goal at 512 MB, workload a,
600 s: `h` 0.18 to 0.40 (goal 0.5), GETs per op 0.54 to 0.40 (goal 0.25), throughput +7 %
(goal within 2 %), about 1,100 extra requests (goal under 2,000), RSS 4.2 GB (goal 3.9).
Deviations from the plan below: `invalidateSSTable` returns the block count instead of an
out parameter; the affinity signal for a log-to-L1 compaction is the number of
destination-level lookups since the previous such event, because log inputs never have a
cached SSTable block (the first warm-on cell warmed nothing for that reason); `Table::warm`
scans a private copy of the index block so it is safe on a shared `Table`; the worker
checks liveness through a fresh View snapshot from `MetadataLogHandler`, not the cache's
raw `latest_view` pointer; `TableBuilder::finish` marks its file complete so the builder
never reads its own output back. Three quarters of the misses are on L2 (every L1-to-L2
compaction rewrites the L2 files an L1 file overlaps), and whole-file warming of L2
outputs is 85 % waste. **Next:** a key-range-selective warm (publish only the blocks whose
key range overlaps the dropped inputs' cached blocks), then the compaction shape. Follow-up
to `PLAN-compaction-range-read.md` and the section "Compaction range reads" of
`RESULTS-cost.md` (finding 4: `h` 0.18 under workload a against 0.65 under workload c at a
52 % cache ratio). Depends on the chunked reader of `2dc2ed24`.

## Goal

Raise the block-cache hit rate under a write-heavy mix to the read-only value at the same
cache size, without adding per-block GETs. Target at 512 MB on a 1 GB dataset, workload a,
600 s, 8 writers: `h` from 0.18 to at least 0.5 (workload c gives 0.65), GETs per op from
0.53 to at most 0.25, throughput unchanged within 2 %, at most 2,000 extra GETs per cell.

The second goal is hygiene that the same change delivers: no dead blocks of deleted
SSTables in the budget and no leaked `Table*`.

## What the code does today

A read (`SSTableHandler::readRecordFromAllLevel`, `src/db/sstable/sstable_handler.cpp:7`)
walks the levels, skips files whose View key range does not cover the key, and calls
`Table::get`. On a block miss, `LRUCache::readDataBlocks` (`src/db/cache.cpp:232`) issues one
GET for that block and publishes the parsed records under `(file_name, index_value)`. The
budget (`current_size`, `capacity`) and the recency list (`lru_list`) hold SSTable blocks only;
log records are outside both.

A COMPACT record is applied by `MetadataLogHandler::rollforwardSingleOperationRecord`
(`src/db/metadata_log_handler.cpp:225`), in two branches: not-last-level (`:256-290`) and
last-level (`:291-325`). Both pop the inputs from the View and push the outputs with their key
ranges and sizes. Both run under `unique_lock<view_mutex>`, from the periodic thread
(`rollForwardMetadataLogPeriodically`, `:336`) or inline from the compactor
(`rollForwardMetadataLog`, `:453`). Neither touches the cache. Three consequences:

1. The inputs' cached blocks stay in the budget until they reach the LRU tail, and their
   `Table*` are never freed: `invalidateLogFile` (`cache.cpp:438`), the only method that
   erases an entry, runs only for log-prefix names (`LogHandler::onRemoteAppend`).
2. The outputs are warm only in the builder: `TableBuilder::flush`
   (`src/db/sstable/table_builder.cpp:146-165`) publishes each block write-through into the
   builder's cache. The other processes fetch the output one block per GET on demand.
3. In a zipfian write mix the hot keys are the most updated keys, so their newest versions sit
   in the log and in L1, and L1 is rewritten faster than a 4 KiB-block file warms. In a 600 s
   workload-a cell the writers commit 160 compactions.

`printCacheStats` (`cache.cpp:517`) prints one line at DB close that the extractor parses:
`[lru_cache] sstable hits=N misses=N hit_rate=X% capacity=N current_size=N files=N`
(`bench/scripts/extract_cost_coefficients.py:65`).

## The change

Three parts, three commits, in this order.

### C. Counters first (no behaviour change)

- Misses per level: an array indexed by the level number parsed from `file_name` (prefix
  `sstable_level_prefix` + digits), incremented next to `sstable_cache_misses_` in
  `readDataBlocks`. Hits per level the same way.
- Dead bytes: at `printCacheStats`, the sum of `block_size` over entries whose file is not in
  `latest_view` (the cache holds the View pointer). Before part A this measures the problem;
  after part A it must be 0.
- Warm counters (used by part B): files warmed, bytes published, files skipped by each policy
  rule, and hits on warmed blocks. A warmed block is marked in its `CacheEntry`
  (`std::unordered_set<std::string> warmed_blocks`); a hit on a marked block counts once and
  clears the mark.
- Output: keep the existing line unchanged. Add a second line
  `[lru_cache] levels hits=h1,h2,... misses=m1,m2,... dead_bytes=N warm files=N bytes=N skipped_budget=N skipped_level=N skipped_affinity=N warm_hits=N`.
  Extend the extractor with one regex and columns `cache_dead_bytes`, `warm_files`,
  `warm_hits`, and per-level miss columns `misses_l1..l3`.

### A. Drop on REMOVE

- `LRUCache::invalidateSSTable(std::string const& file_name, size_t& blocks_dropped)`: under
  the cache mutex, unlink every block of the entry from `lru_list`, subtract its bytes from
  `current_size`, delete the block maps, move the `Table*` to `retired_tables_` (a reader can
  still hold the raw pointer: the retired deque is the existing answer), erase the entry.
  Return the number of blocks that were cached: that is the affinity signal for part B.
- Hook: not inside `rollforwardSingleOperationRecord`. That function runs under
  `unique_lock<view_mutex>`, and the cache mutex must not be taken there (the tailer's
  listener takes the cache mutex; keep the lock order one-way). Instead, both COMPACT
  branches append `{inputs, outputs, dest_level}` to a member `pending_cache_events_`. The two
  callers drain it after their `view_mutex` scope ends: the periodic loop after
  `publishSnapshotLocked()` (`:357`), and `rollForwardMetadataLog` after its unique-lock scope.
  Draining calls `invalidateSSTable` per input and hands `{outputs, dest_level, cached_input_blocks}`
  to part B.
- Order: the View drops the inputs first (already the case), then the cache drops their
  blocks. A reader that took the old View snapshot may still probe an input; it then misses
  and issues a GET on a file that may be gone, which is the documented transient miss.

### B. Warm outputs in the peers

- `Table::warm(LRUCache* cache, size_t max_read_bytes, size_t& bytes_published)`: the chunk
  loop of `Table::getAll` (`src/db/sstable/table_reader.cpp`), refactored into a private
  `scanBlocks(max_read_bytes, callback)` shared by both. The callback for `warm` builds the
  per-block records map and calls `putSSTableRecords(file, map, id_bytes, size)`, the same
  call and the same key bytes as `TableBuilder::flush`. `getAll` keeps its behaviour.
- A warm queue and one worker thread inside `LRUCache` (`warm_thread_`, a deque and a
  condition variable, started by `DB::open` after `setLatestView`, stopped in `DB::~DB`
  before the cache is deleted). One worker bounds the in-flight buffer to one
  `compaction_read_bytes` chunk. The worker: `getSSTable(file)` (the open every peer pays on
  first read today), then `table->warm(...)`.
- Policy, evaluated at enqueue time, each rule with its skip counter:
  1. `cache_warm_enabled` (default `false`, so the default build is A + C only and the
     campaign can A/B the warm).
  2. Level: `dest_level <= cache_warm_max_level` (default 1).
  3. Budget: `output_bytes <= cache_warm_max_fraction * capacity` (default 0.25); the size
     comes from the COMPACT record (`output_bytes`), no HEAD needed.
  4. Affinity: the sum of `cached_input_blocks` over the compaction's inputs is at least
     `cache_warm_min_input_blocks` (default 1). A process that never read the region does not
     warm it; a pure load warms nothing.
- The worker skips a file that is no longer in the View when it reaches the front of the queue
  (a later compaction already replaced it).
- Config keys (`Metadata`): `cache_warm_enabled`, `cache_warm_max_level`,
  `cache_warm_max_fraction`, `cache_warm_min_input_blocks`. Chunk size reuses
  `compaction_read_bytes`.

### Design decisions

- Warm at COMPACT apply, not at first read. The apply is the only moment every process learns
  about the output; a first-read trigger would still pay the per-block GETs for the reads that
  arrive before the warm finishes.
- One ranged read per output per process. Bytes are free in-region on S3; requests are what
  cost. In the 512 MB workload-a cell that is at most 1,280 GETs (160 outputs, 8 processes)
  against 887,912 read-miss GETs today.
- Affinity uses what the process already had cached, not the key ranges of the outputs
  against a hot-key list: it is free, it is per process, and it is zero during a load.
- The retired-Table grace (30 s) stays as it is; `invalidateSSTable` is its intended user.
- No change to the read path, the block format, the write-through publish, or `getAll`'s
  results.

## Phases

### P0. Counters (commit 1)

- P0.1 `cache.h` / `cache.cpp`: per-level hit and miss arrays (size `max_level + 1`, the
  level parsed once per `readDataBlocks` call from the file name), dead-bytes scan, warm
  counters and the `warmed_blocks` mark, second stats line.
- P0.2 `bench/scripts/extract_cost_coefficients.py`: parse the second line; new columns; the
  existing line and columns unchanged.
- P0.3 Test: `LRUCacheTest` (new file `tests/test_cache.cpp`, added to `OZONEDB_TEST_SOURCES`)
  with `FileStorage`: build two tables in levels 1 and 2, read one key from each, check the
  per-level counters; set a View that lists only one of them, check `dead_bytes` equals the
  other's cached bytes.

### P1. Drop on REMOVE (commit 2)

- P1.1 `LRUCache::invalidateSSTable`.
- P1.2 `pending_cache_events_` in `MetadataLogHandler`, appended in both COMPACT branches,
  drained after the lock in both callers. `MetadataLogHandler` already holds the cache pointer
  (`setLRUCache`, `db.cpp:95`).
- P1.3 Tests: after `invalidateSSTable`, `current_size` drops by the file's cached bytes,
  `needReadBlock` reports a read is needed, the `Table*` is in the retired deque and is freed
  after the grace period (use a test setter for the grace, or check the deque size).
  End-to-end: a `DBTest`-style test is not available out of the box (see CLAUDE.md), so the
  cluster campaign is the end-to-end check: `dead_bytes` 0 in every cell.

### P2. Warm (commit 3)

- P2.1 `Table::scanBlocks` refactor, `Table::warm`; `getAll` re-expressed on `scanBlocks`
  with the existing `SSTableTest.GetAll*` tests as the regression check.
- P2.2 Warm queue, worker thread, policy, config keys, start and stop in `DB`.
- P2.3 Tests: `Table::warm` on a 120-block table publishes 120 blocks and `current_size`
  equals the sum of the block sizes; every index entry then reports no read needed. Policy
  as a pure function `warmDecision(enabled, level, bytes, capacity, input_blocks)` with one
  test per rule. Worker: enqueue a file, wait on the counter, check the blocks; enqueue a
  file that is not in the View, check it is skipped.
- P2.4 Build on a node (`bench/scripts/build.sh`, then `cmake --build build --target
  runUnitTests`; `build.sh` does not rebuild the test binary), run `SSTableTest.*` and
  `LRUCacheTest.*`.

### P3. Cluster campaign (tag `cost-2026MMDD-cache`)

Check for another session's drivers before any sync. Sync with `ansible-playbook
bench/ansible/sync.yml -e build=true`, verify the source and fresh `.so` files on every client.

- P3.1 Load, 1M x 1 KB, `--log-trim`, warm on. Expected: `get_per_write` unchanged at about
  0.00015 (affinity skips every output), `dead_bytes` 0, `warm files` 0.
- P3.2 Workload a, 600 s, 8 writers, 512 MB, warm **off** (A + C only). Isolates the drop.
  Expected: `dead_bytes` 0, `h` up a little (no dead weight), GETs per op about 0.5.
- P3.3 Workload a, 600 s, 512 MB, warm **on**. The main cell. Expected: `h` at least 0.5,
  GETs per op at most 0.25, `warm files` about 160 with most of them L1, `warm_hits / warm
  blocks` above 0.5, extra GETs under 2,000, throughput within 2 % of 2,674 ops/s, client
  CPU per op down (each GET costs about 2.6 ms of CPU), RSS peak not above 3.9 GB.
- P3.4 Workload a, 600 s, 8 MB, warm on. The budget rule skips every 64 MiB output:
  expected identical to the range-read cell (2,608 ops/s, 0.67 GETs per op), `warm files` 0,
  `skipped_budget` about 160.
- P3.5 Workload c, 600 s, 512 MB, warm on. No compaction, so no warm: must equal the control
  of `cost-20260829-rr` (14,331 ops/s, `h` 0.679).
- The warm-on/warm-off pair is the A/B; the config flag is passed through the per-writer
  generated config (add a `--cache-warm` flag to `run_multinode_ycsb_with_corfu.sh` and the
  multiproc runner, next to `--lru-cache-bytes`, never by editing `ycsb.yaml`).

### P4. Write-up

- P4.1 Extract with the new columns; a section "Compaction-aware block cache" in
  `RESULTS-cost.md` with the five cells, the per-level miss split, the warm counters and
  the A/B.
- P4.2 The projection does not change (its `h` is the read-only sweep). Add one sentence on
  what the write-heavy `h` now is at 52 %, and whether the model's `h` is still an
  overstatement for a 50 % write mix.
- P4.3 Status lines in `PLAN-cost.md` and here.

## Checks that gate each phase

| Phase | Gate |
|---|---|
| P0 | Existing stats line unchanged; extractor parses both lines; `LRUCacheTest` passes on a node |
| P1 | `dead_bytes` 0 in the tests; `SSTableTest.*` and `LRUCacheTest.*` pass; no lock taken under `view_mutex` |
| P2 | `GetAll*` tests unchanged; warm publishes every block; policy tests pass; worker stops cleanly in `DB::~DB` |
| P3 | 8/8 writers per cell, `dead_bytes` 0, warm-off and 8 MB cells match the range-read cells, warm-on cell meets the targets or the counters say which rule to change |
| P4 | Numbers from the tsv only |

## Risks

- **Warming evicts hot blocks.** The budget and affinity rules bound it; `warm_hits` measures
  it. If `warm_hits / warm blocks` is low, raise `cache_warm_min_input_blocks` before
  anything else.
- **Lock order.** The cache mutex must never be taken under `view_mutex`, and the tailer must
  never enter the cache synchronously. The pending-events drain and the worker thread are the
  two boundaries; a test cannot prove this, review must.
- **Worker falls behind.** Under a fast compaction rate the queue grows; the worker skips
  files no longer in the View, and the queue is bounded (drop the oldest beyond 64 entries,
  counted).
- **Memory.** One chunk buffer (64 MiB) plus the parsed blocks of one file in flight; the
  budget rule keeps a file at a quarter of the cache. Peak RSS is a P3 number.
- **A reader on a dropped input.** Same transient miss as today; the retired deque keeps the
  `Table*` alive for 30 s.

## Out of scope

- Per-level block sizes (64 KiB for L1, 4 KiB for the last level): a cheaper alternative,
  worth one cell if B disappoints.
- A byte budget for log records (bounds RSS, does not move `h`).
- Changing the projection's `h` source; that is a paper decision once the write-heavy `h`
  is known.
