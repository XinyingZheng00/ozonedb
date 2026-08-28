# Plan: compaction reads SSTable inputs as ranges, not one GET per block (branch `worktree-plan-cost`)

**Status (2026-08-27):** planned, not started. Follow-up to `PLAN-cost.md` and the section
"Re-run with 4 KiB blocks" of `RESULTS-cost.md`. This plan covers the SSTable tier
(`sstable_storage`, S3/MinIO). Range reads on the Corfu log tier are a different plan
(`PLAN-range-read.md`, untracked, kept by the user) and are out of scope here.

## Goal

One coefficient: compaction GETs per put (`get_per_write` in the cost model). It is 0.015 with
64 KiB blocks and 0.205 with 4 KiB blocks. The target is below 0.001 with 4 KiB blocks.

At 10 TB and 10k ops/s, that coefficient is $1,078 per month of S3 GETs. With it removed, the
4 KiB block curve pays off: the projection drops from $7,070 to $5,993 per month, and the
crossover against Cassandra RF=3 moves from 17.8 TB to 14.7 TB
(`results-cost-20260828-4k-projection-rangeread.tsv`).

## What the code does today

Compaction (`src/db/compaction.cpp:322-323`) opens each SSTable input with `Table::open` and
calls `Table::getAll()`. The call chain is:

| Step | Code | Storage calls |
|---|---|---|
| `Table::open` | `src/db/sstable/table_reader.cpp:30` | 1 `size` (HEAD), 1 footer read, 1 index read, 1 meta-index read, 2 filter reads |
| `Table::getAll` | `table_reader.cpp:209-228` | one `blockReader` per index entry |
| `blockReader` | `table_reader.cpp:136-150` | one `readBlock` |
| `readBlock` | `src/db/sstable/block_handler.cpp:88-96` | one `storage->read(file, buf, offset, length)` |

On `S3Storage`, `read(file, buf, offset, length)` is one ranged `GetObject`
(`s3_storage.cpp:214-227`). An SSTable at `level_file_size_limit` (64 MiB) holds 16,384 blocks
of 4 KiB, so one compaction input costs 16,384 GETs plus 6 requests for the open. Data blocks
are written in file order by `TableBuilder::writeBlock` (`table_builder.cpp:168`), so the data
section is one contiguous range `[first.offset, last.offset + last.length)` and the index lists
the blocks in that order.

`getAll` has no error path. If `blockReader` returns null, `block_iter->seekToFirst()` crashes.
If `Table::open` fails, `table` is null and `table->getAll()` crashes. The log-input branch of
the same loop (`compaction.cpp:293-300`) skips an unreadable input and counts it in
`inputs_skipped`. The SSTable branch must do the same.

The point-read path (`Table::get` through `LRUCache::readDataBlocks`) reads one block per GET
on purpose and does not change.

## The change

`Table::getAll` reads the data section in a few large ranged reads and slices the blocks out of
the buffer. It does not call `readBlock`.

Design decisions:

1. **Chunks follow block boundaries.** A chunk is a run of consecutive index entries whose
   total length is at most `max_read_bytes`. A block larger than `max_read_bytes` gets its own
   read. No block is ever split across two reads, so the slice logic stays trivial.
2. **One parameter, one config key.**
   `Status Table::getAll(std::unordered_map<...>& out, size_t max_read_bytes)`.
   Compaction passes `metadata->compaction_read_bytes`, a new key parsed in the `Metadata`
   constructor (`src/include/ozonedb/metadata.h`), default 67108864. That equals
   `level_file_size_limit`, so the default is one read per input file. Tests pass small values.
3. **Status, not a crash.** `getAll` returns `Status`. A failed read, an index entry outside the
   data section, or a block that does not parse returns `kFailure` (or the storage status), and
   the partial map is discarded. Compaction treats a failed open or a failed `getAll` like an
   unreadable log input: log it, `++inputs_skipped`, `continue`.
4. **Slice validation.** Before the read, walk the index once and check that offsets are
   non-decreasing and that `offset + length` is at most the end of the data section. Any
   violation returns `kFailure`. This protects against a corrupt index turning into an
   out-of-range `memcpy`.
5. **Parse per slice.** Each slice goes through `protobuf::deserializeMessages` exactly as
   `readBlock` does now, then `newIterator` and the existing record loop. No change to the
   block format, the index, or the builder.
6. **All backends keep working.** `FileStorage::read` (seek + read), `CorfuDBStorage::read`
   (`memcpy` with a bounds check, `corfu_storage.cpp:1560`), `S3Storage` and Azure all
   implement the ranged read. When `sstable_backend` is unset, `sstable_storage` aliases the
   log storage; the ranged read on Corfu is a local `memcpy`, so the change is harmless there.
7. **Memory bound.** One chunk buffer of at most `max_read_bytes` is live per compaction, in
   addition to the record map that `getAll` already builds. At the default that is 64 MiB per
   in-flight compaction.

Optional, separate commit: `Table::open` reads the meta-index and both filter blocks. Compaction
never probes filters. A flag `Table::open(storage, file, table, /*read_meta=*/false)` saves 3
requests per input file. Do this only if the per-file overhead is visible after step P2.

## Phases

### P0. Engine change (laptop edits, build on a node)

- P0.1 `src/include/ozonedb/sstable/table_reader.h`: change the `getAll` declaration to the
  `Status` form with `size_t max_read_bytes = 67108864`. Add a header comment with decisions
  1, 3 and 4 (chunking, error contract, validation).
- P0.2 `src/db/sstable/table_reader.cpp`: rewrite `getAll` as described. Keep `blockReader`
  and `readBlock` unchanged.
- P0.3 `src/include/ozonedb/metadata.h`: add `uint64_t compaction_read_bytes = 67108864;` and
  its parse next to `lru_cache_bytes` (`metadata.h:230`).
- P0.4 `src/db/compaction.cpp:317-340`: check the `Table::open` status and the `getAll` status.
  On failure, log `[compaction] input <file> unreadable (status=N), skipped`, delete the
  table if it was created, `++inputs_skipped`, `continue`.
- P0.5 Optional log line per compaction: `[compaction] sstable inputs=N range_reads=M bytes=B`.
  It makes P2 checkable from the writer output without MinIO counters.
- P0.6 Tests in `tests/test_sstable.cpp`:
  - `SSTableReadWholeTableTest` (`test_sstable.cpp:74`) must pass unchanged (default chunk).
  - New: build a table with about 50 blocks, call `getAll` with `max_read_bytes` of 1 byte,
    one block, 2.5 blocks and the full file. Every result must equal the per-block result.
  - New: a table whose index entry points past the end of the file returns `kFailure`.
  - New: `getAll` on a removed file returns a non-success status, no crash.
- P0.7 Build and run on a client node: `bash bench/scripts/build.sh`, then from `build/`
  `./runUnitTests --gtest_filter='SSTableTest.*'`. Do not build on the laptop.

### P1. Cluster: sync, build, check

Check for another session's drivers before any sync (`feedback_shared_cluster`).

- P1.1 `ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg ansible-playbook bench/ansible/sync.yml -e build=true`.
- P1.2 On every client, confirm that `grep -c compaction_read_bytes src/db/compaction.cpp` is
  at least 1 and that both `.so` files are fresh, as `chain4k_fix.sh` does for `caller_table`.
- P1.3 Run `./runUnitTests --gtest_filter='SSTableTest.*'` on one client.

### P2. Fresh 1 GB load, measure `get_per_write`

The load phase is where compaction GETs are measured (`extract_cost_coefficients.py` reads the
MinIO counters of the `_rc1000000` load sample). The current `/mnt/corfu/{load,load-bucket}`
snapshot was written by the 4 KiB engine without this change and is replaced.

- P2.1 `bash bench/scripts/local/load_corfu_dataset.sh --writers 8 --log-trim` with a new
  run tag, for example `cost-20260829-rr`.
- P2.2 Extract:
  `python3 bench/scripts/extract_cost_coefficients.py bench/results/local/<tag> bench/results/local --window 60 --tsv <out>.tsv`
  (the `extract_4k.sh` chain in the job tmp dir does this and the projection).
- P2.3 Read the load row: `s3_get`, `s3_head`, `writes`, `get_per_write`, load puts/s, client
  and server CPU per put.

Expected: `get_per_write` below 0.001 (from 0.205), load rate back near the 64 KiB value
(9,417 puts/s, 120 s; the 4 KiB load was 8,054 puts/s, 147 s), server CPU per put back near
0.32 ms (from 0.56 ms), client CPU per put near 0.86 ms (from 0.91 ms). PUTs per put stay at
4.4e-5. The bucket size stays at about 204 + 953 MiB.

If `get_per_write` stays above 0.01, count requests per input file from the P0.5 log line and
check whether the remaining GETs come from `Table::open` (6 per file) or from another reader.

### P3. Workload cells

Every cell restores the P2 snapshot first (the wrapper does this).

- P3.1 Workload a, 600 s, 8 writers, at 512 MB and 8 MB:
  `bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes <c> --workloads a --writers-list 1 --trial 1 --duration 600 --run-tag <tag>-long`.
  Confirm 8/8 writers finished (`survivors.py` in the job tmp dir) and 0 failed operations
  beyond the documented transient misses.
- P3.2 Workload c, 600 s, at 512 MB only, as a control. The read path did not change, so `h`
  must stay at 0.675 and ops/s within a few percent of the 4 KiB value.

Expected on workload a: client CPU per op below 1.37 ms, because each S3 GET costs about 2.6 ms
of client CPU and the compaction GETs are gone. Aggregate ops/s at or above 2,708 (512 MB) and
2,566 (8 MB).

### P4. Projection and write-up

- P4.1 Build the combined tsv (4 KiB rows of this campaign + the committed Cassandra rows) and
  run `plot_cost_model.py` with `--table`. The measured `get_per_write` now enters the
  projection directly; the `-rangeread` variant table becomes the measured line.
- P4.2 Commit `bench/results-<tag>.tsv`, `-projection.tsv`, `.png`, `.pdf`.
- P4.3 `RESULTS-cost.md`: a section "Compaction range reads" with the load table (64 KiB, 4 KiB,
  4 KiB + range reads), the workload-a rows, the projection line and the crossover. Update
  conclusion 1 of the 4 KiB section from "the change is not yet made" to the measured result.
- P4.4 `PLAN-cost.md` status paragraph: one sentence with the measured coefficient and the new
  10 TB number. Update this plan's status line.

## Checks that gate each phase

| Phase | Gate |
|---|---|
| P0 | `SSTableTest.*` passes on a node, including the chunk-boundary and failure tests |
| P1 | Every client has the new source and fresh `.so` files, unit tests pass on one client |
| P2 | Load finishes 8/8, `get_per_write` below 0.001, bucket size unchanged |
| P3 | 8/8 writers per cell, workload-c control within a few percent of the 4 KiB values |
| P4 | Projection table regenerated from measured rows, no hand-edited numbers |

## Risks

- **A corrupt or foreign index.** Decision 4 (validation) turns it into `kFailure` and a skipped
  input, not a crash. A skipped input is logged and counted, as the log path does today.
- **Memory on small nodes.** 64 MiB per in-flight compaction plus the record map. The thread pool
  bounds the number of in-flight compactions. Lower `compaction_read_bytes` if RSS matters.
- **Peer REMOVE under the reader.** The same race as `readDataBlocks` (`96b9265d`). A range read
  on a removed object fails at the storage layer and becomes a skipped input. The
  metadata-log rollforward reconciles the task.
- **Old snapshots.** The SSTable format does not change, so old buckets stay readable. No reload
  is needed for compatibility, only for measurement (P2).

## Out of scope

- Corfu tailer range reads and codec `NONE` (`PLAN-range-read.md`, the user's untracked plan).
- A config key for the data block size (still `BLOCK_SIZE` in `table_builder.cpp:16`).
- The extractor check that counts writers with an `[OVERALL]` block per cell.
- The disk-backed read-through `Storage` wrapper and the split LRU.
