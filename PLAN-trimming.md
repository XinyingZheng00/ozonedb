# PLAN-trimming

Bound the log that a process replays when it joins. Move the trimmed prefix of the
shared log into the object store as a checkpoint, then trim the Corfu stream behind it.

## 0. Status — implemented and validated on the cluster (2026-08-26)

Branch: `worktree-plan-trimming`, from `visibility` at `25dab3a0`. That head already contains
the durability fixes (`PLAN-durability.md`, merged at `ca03ba2b`) and the batched open-time
replay.

Results on the 2026-08-26 cluster (amd127 = corfu + MinIO, amd160 = client, 8 writer
processes on one host, 1M × 1 KB load):

| Check | Result |
|---|---|
| `CheckpointTest` (FileStorage, no Corfu) | 4/4 pass |
| `CorfuStorageTest` (live server) | 10/10 pass, incl. join, refuse-without-checkpoint, snapshot under concurrent writes, two trimmer cycles |
| `corfu_smoke` 3000 keys, trimmer on | PASS 3000/3000 across a reopen that restored `C=5000` and replayed 1 entry |
| `corfu_multiwriter_smoke` | PASS 1000/1000 from 2 writers |
| Load with `--log-trim` | 4 cycles; final `C=954059`, live 61 MB, snapshot pause 16–31 ms, upload ≤ 0.5 s, trim ≤ 0.5 s |
| Corfu disk after the load | 293 MB (stream was ~1.1 GB); bucket holds 2 checkpoints (33 MB + 62 MB) + `LATEST` |
| **Join from the checkpoint** | restore `C=954059` (61 MB), replay **55,268 entries in 1.6 s**, first YCSB op at **9 s** |
| Control, same load without trimming | replay **1,003,655 entries in 16 s**, first YCSB op at **23 s** |
| Live fraction of the stream (phase 0) | 51 MB live of 1155 MB (4.4 %); `task.log` 560 KB, `metadata.log` 21 KB |
| Workload c reads after the join, default / `--linearizable` | 0.44 % / 0.47 % NOT_FOUND (before the read-miss fix below) |
| Workload c reads on the control (no trimming) | **0.63 % / 0.62 % NOT_FOUND** (before the fix; 0 after) |

### Read misses: diagnosed and fixed (2026-08-26, later the same day)

The read misses are **not** a trimming effect: the control without trimming misses at the
same rate or more, in both read modes, with no writer active. A per-partition probe on the
control dataset (20k uniform keys from each load writer's 125k-key range) misses 111–154
keys in every one of the eight ranges (0.55–0.77 %), so it is not one writer's log.

"All inserts returned OK" meant nothing: `OzoneDBClient.insert` ignored the return value of
`db.put` (fixed: a refused put is now `Status.ERROR`). The load logs, which capture the JNI
`Failed to put key-value pair` line, show **zero** refused puts. So every lost record was
acked.

**Cause 1 — the ack outran the seal (data loss).** `CorfuDBStorage::submitBatch` spliced a
batch into `file_buffers_` and acked it right after the JNI append returned, checking
`sealed_at_addr_` as the local tailer knew it at that moment. A peer's `SEAL(F)` at address
`S` that the tailer had not applied yet is invisible, so an append at `addr > S` was acked.
Every other process applies `S` first and drops that append (`applyEntryBytes`); the
compactor, another process seven times out of eight, reads `F` without it and its `REMOVE`
makes the loss permanent. `corfu_stream_stats` (new, `tests/corfu_stream_stats.cpp`) on the
control's Corfu log: **7,407 appends (8.5 MB) sequenced above their file's seal and dropped
by every peer** — 0.74 % of 1M puts, ×7/8 ≈ 6,500 lost, against the 0.62 % measured miss.

**Cause 2 — the compaction election reads arrival order (duplicate work, diverging Views).**
The same self-splice put every file buffer in *arrival* order, not address order: own bytes
were spliced at submit time, a peer's earlier record arrived later via the tailer.
`TaskLogHandler::isFirstWriterForTask` judges "first claim in `task.log`", so every
claimant saw its own claim first. Control load: task `datalog/1+2` was executed by three
writers, `datalog/27+28` by six; 83 `COMPACT` records for 23 real tasks, 60 of them never
applicable in address order. Each process applied *its own* duplicate first, so the Views
diverged per process and second-level compactions deleted SSTables from the bucket that
other Views still referenced. `MetadataLogHandler` applies a `COMPACT` only when its first
input is at the front of the level's deque, otherwise it buffers it silently — so one
never-executed task blocks every later `COMPACT` on that level (seen at full scale in the
first fix attempt: task `datalog/1+2` claimed by all eight writers, executed by none, all
fifteen later `COMPACT`s buffered, 87 % NOT_FOUND). Dead-task reclaim cannot rescue that:
`task_heartbeat_threshold = 10000000` (× 100 ms ≈ 11 days).

**Fix (`src/db/corfu_storage.cpp`).** The tailer is now the *only* writer of
`file_buffers_`, own entries included, in address order; `submitBatch` publishes
`last_written_addr_`, waits until the tailer passed the batch's address, and reads the
outcome (spliced → `kSuccess`, dropped above a seal → `kSealed`, tailer stalled → `kFailure`,
never an assumed success). One rule fixes both causes: the seal decision is made with
complete knowledge, and `task.log` / `metadata.log` / every data log are byte-identical in
every process, so the election and the rollforward agree everywhere. The checkpoint
snapshot becomes trivially exact. Regression test:
`CorfuStorageTest.ack_never_outruns_a_peer_seal`. Compaction now logs
`records_in / unique / inputs_skipped` per task (conservation check: on an insert-only load
`records_in == unique`), and `LogHandler::addRecord` logs a refused append.

**Cost of the tailer wait, and the fast path restored.** With the ack tied to the tailer a
put paid one more Corfu round trip: single-threaded YCSB writers went from ~1,000 to ~350
puts/s each (average wait 2.2 ms, 8 writers on one host). The wait existed for one question
only — *was a `SEAL`/`REMOVE` of this file sequenced between what my tailer applied and my
new address?* — and the **sequencer** can answer it at token time. Corfu's
`StreamsView.append(payload, TxResolutionInfo, stream)` sends a read set and a write set
with the token request; `SequencerServer.txnCanCommit` refuses the token
(`TX_ABORT_CONFLICT`, naming the offending address) when a read key was written at an
address `>` the snapshot, and write keys are recorded at token issue (verified on the pinned
commit `8f144d4`). `CorfuDBStorage::submitBatchFast` therefore sends the file name as a read
key at snapshot `S = last_applied_addr_`, and `seal()`/`remove()` send it as a write key
(`jniAppendKeyed`, `CorfuBridge.appendChecked`). A granted token at `addr` proves no
`SEAL`/`REMOVE` of the file in `(S, addr]`; the tailer applied every entry `<= S`, so
`sealed_at_addr_`/`removed_files_` decide the rest exactly, and the put returns after the
token and the log-unit write — the pre-fix cost. A refused token has no address, so nothing
lands: the append is reported `kSealed` once the tailer reaches the offending address (a
refusal that names an address holding no seal — the key's write became a hole — is retried
with a fresh snapshot; `spurious=0` in every run so far). The other refusals
(`TX_ABORT_NEWSEQ` after a sequencer restart, `SEQ_OVERFLOW` after a conflict-cache
eviction, `SEQ_TRIM` below the trim mark) send the batch to the slow path — a plain token
plus the tailer wait, unchanged. The tailer stays the only writer of `file_buffers_`, in
address order, so `task.log`/`metadata.log` and the election are untouched; reads still
fence on `last_written_addr_`, so read-my-writes is unchanged. Two rules follow: **every
`SEAL`/`REMOVE` carries the key** regardless of `corfu_fast_ack` (a keyless seal is
invisible to every peer's fast path), and a cell must not mix builds that predate the key.
`corfu_fast_ack = false` selects the slow path for every append (A/B only). Counters are
printed at teardown: `[corfu] ack: fast=… slow=… conflict=… spurious=… slow_by_cause(…)`.
Regression test: `CorfuStorageTest.fast_ack_taken_and_refused_by_a_peer_seal`.

**Results after the fix** (same cell: amd127 corfu + MinIO, 8 writers on amd160, 1M × 1 KB,
no trimming; the control is the run before the fix on the same cell):

| | Control (before) | Ack waits for the tailer | Sequencer-keyed fast ack (current) |
|---|---|---|---|
| Puts refused (`Failed to put`) | 0 | 0 | 0 |
| Appends above a seal in the stream (`corfu_stream_stats`) | **7,407 acked, dropped by peers** | 101, all reported `kSealed` and retried | **0** — 25–31 refusals per writer at token time, none landed |
| Compaction tasks / `COMPACT` records / applied in address order | 23 / 83 / 23 (60 duplicates) | 24 / 24 / 24 | 24 / 24 / 24 |
| Executors per task | 1–6 | exactly 1 | exactly 1 |
| `records_in == unique` for every compaction | (not logged) | yes, no skipped input | yes, no skipped input |
| Workload c NOT_FOUND, default / `--linearizable` | 0.63 % / 0.62 % | **0 / 52,926 and 0 / 52,559** | **0 / 53,143 and 0 / 53,271** |
| Reader live set after replay | 51 MB | 28 MB | — |
| Put throughput per single-threaded writer | ~1,000/s | ~350/s (ack wait avg 2.1 ms, max 0.17 s) | **~1,090/s** (fast=125k, slow=0, spurious=0 per writer; aggregate 8,395/s) |
| `CorfuStorageTest` / smoke | 10/10 | 11/11 (+`ack_never_outruns_a_peer_seal`), both smokes PASS | 12/12 (+`fast_ack_taken_and_refused_by_a_peer_seal`), both smokes PASS |

Left as is, documented: `MetadataLogHandler` still buffers a `COMPACT` whose input is not
at the level's front with no log line, and a task whose owner dies mid-compaction blocks
its level until the (effectively disabled) dead-task reclaim fires.

The first-op time includes ~7 s of JVM start, Maven classpath resolve and `initSSTMetadata`
(70+ SSTables), which trimming does not touch. The replay itself went from 16 s to 1.6 s.

Not run: the two restore-mismatch checks of §5 (server restarted from the trimmed dir;
bucket without the log dir). Both paths are covered by unit tests
(`trimmed_stream_without_checkpoint_refuses_to_open`) but not by the cluster procedure yet.

Implemented on the branch:

| Phase | Where |
|---|---|
| 0 replay line with live bytes | `drainInitialEntries` prints `stream_MB`, `live_files`, `live_MB`, `top_level={…}` |
| 1 bridge | `CorfuBridge.prefixTrim/trimMark/seekPollView`, TRIMMED = zero-length array |
| 2 snapshot + format | `write_gate_`, `takeSnapshot`, `CheckpointManifest`, `src/db/checkpoint.{h,cpp}` |
| 3 join | `bootstrap`, `DB::DB` builds the SSTable store first, TRIMMED retry, fail-stop |
| 4 trimmer + bench | `src/db/log_trimmer.{h,cpp}`, five `Metadata` keys, `corfu.log_trim` in `ycsb.yaml`, `--log-trim` on the loader, runner, orchestrator and cluster wrapper |
| tests | `tests/test_checkpoint.cpp` (FileStorage, no Corfu), four new `CorfuStorageTest` cases |

Two deviations from the text below, both deliberate:

- **The completeness guard uses a `TRIM` entry, not the log unit's trim mark** (§3.5). The
  trimmer appends `CorfuEntry{op=TRIM, payload=X}` right before `prefixTrim(X)`. A replay that
  starts at `S` and meets `X >= S` throws. The log unit's mark is global: a fresh stream on a
  server trimmed for another stream must still open. The sequencer's per-stream mark is in
  memory only. The entry is per stream and survives a restart. `bootstrap` also throws when
  the checkpoint is ahead of the stream tail (a log dir older than the bucket).
- **The gate is taken at the entry of every path that pushes into `pending_`**, not inside
  `submitBatch` (§3.3). A batch queued by a thread that still waits for the gate blocks the
  stamped batch behind it, whose writer holds the gate, which blocks the snapshot, which
  blocks the first thread. Taking the gate before the push removes the cycle.

Baseline, measured 2026-08-25 on the 1M × 1 KB load (`PLAN-transactions.md`, "Load time",
and the message of `25dab3a0`):

| Quantity | Value |
|---|---|
| Entries in the stream after the load | 1,022,163 |
| Stream size on the Corfu disk | 1.1 GB |
| `openDB` replay before `25dab3a0` (Corfu batch sizes 10/10) | 41 s, first YCSB op at 48–49 s |
| `openDB` replay at `25dab3a0` (`bulkReadSize`/`streamBatchSize` 1000) | 30 s, first YCSB op at 37 s |
| Server cost per address, one sequential request stream | ~28 µs |
| Replayers in parallel (1 vs 7) | same per-entry time |

Target after this plan, same dataset: first YCSB op in **< 5 s** on every joining process,
and a Corfu data directory that holds at most two checkpoint intervals of entries.

## 1. Purpose

A process that opens the DB replays the whole Corfu stream from address 0
(`CorfuDBStorage::drainInitialEntries`, `src/db/corfu_storage.cpp:427`). The stream holds
every byte ever appended. Compaction moves data-log bytes into SSTables on the object store
and then appends a REMOVE for the data log (`src/db/compaction.cpp:472`). The REMOVE makes the
old entries dead, but they stay in the stream. A joiner reads and parses all of them.

Replay time therefore grows with the history of the DB, not with its live state. The 1M load
costs 30 s per process after the batch-size fix, and the cost is per entry on the server
side. The throughput campaign quotes numbers with that init time subtracted, which is a
workaround, not a fix.

This plan adds three things:

- A **checkpoint**: the live state of the log at one address `C`, stored in the object store.
- A **trimmer**: one process that writes checkpoints and calls Corfu `prefixTrim` behind them.
- A **join path** that loads the newest checkpoint and replays only the entries after it.

## 2. What the stream holds today

Facts from the code that shape the design:

- One Corfu stream holds every file. Each entry is a `CorfuEntry` {`file_name`, `op` = APPEND /
  SEAL / REMOVE, `payload`, `client_id`} (`src/include/ozonedb/record.proto`).
- SSTables go to `sstable_storage`, which is S3 in every bench config
  (`src/config/corfu/shared_config_base.json`: `sstable_backend = s3`). They are **not** in
  the stream. The stream holds only `datalog/*`, `metadata.log`, and `task.log`.
- The live state at any address is small: per-file byte buffers (`file_buffers_`), the
  sealed set, the removed set, and `sealed_at_addr_`, the global address of each SEAL
  (`src/include/ozonedb/corfu_storage.h:165`).
- A data log lives for a short time. `LogHandler::newTail` seals it and writes LOGCREATE
  (`src/db/log_handler.cpp:14`). Compaction reads it, writes SSTables, appends COMPACT, and
  removes it. With `log_file_size_limit = 32 MB`, the live data logs are the current tail
  plus the sealed logs that compaction did not consume yet.
- `metadata.log` and `task.log` are append-only forever. They are small (about 100 records
  per 1M puts for the metadata log). Their growth is a follow-up, not a blocker (§4, phase 6).
- *(Superseded, see §0 "Read misses".)* The local writer used to self-apply its own bytes
  into `file_buffers_` **before** the tailer reached them, and the tailer skipped APPEND
  entries with our `client_id`. So `file_buffers_` was not always a clean prefix of the
  stream, and §3.3 was written to handle that. Since the read-miss fix the tailer applies
  own entries too, in address order, and `submitBatch` waits for it; `file_buffers_` under
  `mtx_` is always exactly the state at `last_applied_addr_`.
- `DB::DB` builds `log_storage` before `sstable_storage` (`src/db/db.cpp:65-68`). The Corfu
  constructor drains the stream at once. The join path needs the object store first, so
  that order changes (§3.5).

## 3. Design

### 3.1 The checkpoint is the trimmed prefix, compacted

A checkpoint at address `C` is the exact state after every entry with address `<= C`:

- the bytes of every file that is not removed,
- the sealed set, with `sealed_at_addr` where the branch has it,
- the names of removed files.

A joiner that loads checkpoint `C` and then applies entries `C+1 …` ends in the same state as
a joiner that replays from address 0. That is the correctness invariant of the whole plan.

The checkpoint drops the bytes of removed files. That is what bounds the replay. A raw
archive of the trimmed entries does not bound anything: a joiner must still parse every
entry. The SSTables already hold the removed data durably in the object store, so the
checkpoint loses no data. (Alternatives: §7.)

The checkpoint goes to `sstable_storage`. That store already holds the SSTables the
checkpoint's `metadata.log` refers to. When `sstable_storage` aliases `log_storage`, the
checkpoint lands in the log that is trimmed. `Metadata` rejects `log_trim_enabled`
in that case (§3.8).

### 3.2 Object layout and manifest

```
<sstable_dir>/checkpoint/LATEST                 text: "<C>\n"; written last
<sstable_dir>/checkpoint/<C>/manifest           CheckpointManifest proto
<sstable_dir>/checkpoint/<C>/files/<file_name>  raw bytes, one object per live file
```

One object per file keeps each `PutObject` at most `log_file_size_limit` bytes and lets the
joiner fetch files in parallel. `S3Storage` buffers a whole object and writes it with one
`PutObject` (`src/include/ozonedb/s3_storage.h`), so no new storage primitive is needed.
`LATEST` is a fixed key. An S3 `PutObject` is atomic per key, so a reader sees the old value
or the new value, never a mix. `LATEST` is written after every other object of the
checkpoint. A crash before that write leaves an orphan directory and no visible change.

New message in `record.proto`:

```proto
message CheckpointManifest {
  required int64 covered_addr = 1;      // C: every entry <= C is applied
  required int64 prev_covered_addr = 2; // C of the previous checkpoint, -1 if none
  message File {
    required string name = 1;
    required int64 size = 2;
    optional bool sealed = 3;
    optional int64 sealed_at_addr = 4;  // durability branch; -1 when unknown
  }
  repeated File files = 3;
  repeated string removed = 4;
  optional string creator = 5;          // writer fingerprint
  optional int64 created_unix_ms = 6;
  optional int64 stream_tail_at_snapshot = 7;
  // 20-29 reserved for the cas branch (key_versions), see 3.10.
}
```

The manifest lists the file names, so no object listing is required. `S3Storage` has no list
operation and none is added.

### 3.3 A clean snapshot: the write gate

The hazard: `file_buffers_` can hold local bytes with addresses above the tailer's position,
and can miss local bytes that sit in `pending_` with an address below it. A copy of
`file_buffers_` at an arbitrary moment is not the state at any single address. A joiner that
loads such a copy and replays from `C+1` duplicates bytes or loses bytes.

Fix: a `std::shared_mutex write_gate_` in `CorfuDBStorage`.

- Every path that pushes a batch into `pending_` or appends to the log holds the gate
  **shared** from its entry until its batch is settled: `append`, `appendInBatch`, `flush`,
  `drainForRead`, `seal`, `remove`. `submitBatch` assumes the caller holds it. The
  destructor flush runs after the trimmer stops, so it needs no gate.
- `takeSnapshot()` holds the gate **exclusive**. Under the gate no local write is between
  "sequenced" and "reconciled", so `pending_` is empty. Then, under `mtx_`:
  1. Wait until `last_applied_addr_ >= last_written_addr_`.
  2. Set `C = last_applied_addr_`.
  3. Copy `file_buffers_` (minus removed files), `sealed_files_`, `sealed_at_addr_`,
     `removed_files_`.
- Release both locks. The upload runs from the copy, outside every lock.

Why the copy is the state at exactly `C`: the tailer applies each entry under `mtx_` and
stores `last_applied_addr_` under the same lock, so every remote entry `<= C` is applied and
none above it. Every local entry has an address `<= last_written_addr_ <= C` and is
reconciled. `cached_file_` (bytes not yet flushed to Corfu) is excluded: those bytes are not
in the log and will land above `C`. *(Since the read-miss fix the tailer applies local
entries as well, so the "reconciled" clause is trivially true and the gate only keeps the
snapshot from being starved by a stream of own appends.)*

Cost: writers pause for the copy once per trim interval. The copy is a `memcpy` of the live
files, about 50–150 MB, so the pause is tens of milliseconds. Phase 5 measures it.

### 3.4 The trim cycle

A `LogTrimmer` thread runs in the process with `log_trim_enabled = true`. One process per
cluster in this plan. Two trimmers are safe but wasteful (each checks `LATEST` and skips a
write when its `C` is not greater). Election through the task log is a follow-up (phase 6).

Every `log_trim_interval_ms`, the trimmer reads the stream tail. If
`tail - last_checkpoint_addr < log_trim_min_entries`, it sleeps. Otherwise, cycle `N`:

1. `takeSnapshot()` → `C_N`.
2. Upload every file, then the manifest, then `LATEST = C_N`.
3. `prefixTrim(C_{N-1})` — trim behind the **previous** checkpoint, not the new one.
4. `AddressSpaceView.gc()` to make the log unit delete whole segments below the mark.
5. Delete checkpoint `C_{N-2}` (best effort, `log_trim_keep_checkpoints`, default 2).
6. Before step 3, append `TRIM(C_{N-1})` to the stream. It is the per-stream, persisted
   record of the trim that §3.5 checks. `LATEST` names the checkpoint, the `TRIM` entry names
   the trim.

Step 3 is the grace rule. A live member that lags the tailer by less than one interval is
never below the trim mark. A joiner always uses `LATEST`, which is above the mark by one full
interval. A trim exactly at `C_N` gives a member zero grace.

`DB::closeDB` runs one final cycle when the trimmer is enabled. The bench needs that: the
loader's last trim is what makes the load snapshot small (§5).

### 3.5 Join from a checkpoint

`drainInitialEntries` becomes `bootstrap(Storage* checkpoint_store)`:

1. If `checkpoint_store` is null, replay from address 0 as today. Then check step 5.
2. Read `LATEST`. If absent, go to step 5.
3. Read the manifest for `C`, then every file object into `file_buffers_`. Restore the
   sealed set, `sealed_at_addr_`, and the removed set. Set `last_applied_addr_ = C`.
4. Call the bridge `seekPollView(C + 1)`. Drain from there with `pollBatch` and
   `kDrainBatchSize`, as today.
5. Two completeness checks. (a) Before the seek: the stream tail must be `>= C`, or the log
   dir is older than the bucket (or empty). (b) After the drain: the highest `X` of any `TRIM`
   entry replayed must be `< C + 1` (or `< 0` without a checkpoint), or the log was trimmed
   past what we loaded. Throw in both cases. This is the guard against a bucket restored
   without its log, or a log restored without its bucket (§5).
6. If a poll returns the TRIMMED sentinel, discard all state and retry from step 2, at most
   three times. Then throw. A Corfu error during the drain throws at once.

Check (b) reads the `TRIM` entry, not Corfu's marks, because the sequencer's per-stream mark
is in memory only and the log unit's mark is global (§0, deviations). After a server restart
a fresh view without `seek` can start at the first surviving entry with no exception; the
`TRIM` entry above that point is what makes the partial state visible.

`DB::DB` must build `sstable_storage` before `log_storage` when `sstable_backend_set`, and
pass it to `bootstrap`. `makeStorage` for S3 does not depend on the log backend, so the reorder
is safe. `bootstrap` runs before `LogHandler` installs the remote listener, so the replay
generates no listener events, as today. `warmKeyIndex` and `initSSTMetadata` then work from
the restored buffers with no change: they read through `storage->read`, which serves
`file_buffers_`.

The unit tests and the smoke binaries construct `CorfuDBStorage` directly. The constructor
keeps its current signature and gains an optional `Storage* checkpoint_store = nullptr`.

### 3.6 Members that fall behind the trim mark

A live member whose tailer is more than one interval behind gets `TrimmedException` from
`pollView.next()`. The bridge returns the TRIMMED sentinel. The storage then fails stop:

- log one line with the applied address, the trim mark, and the lag,
- set `trimmed_out_ = true`,
- return `kFailure` from every append, and `kNotFound` from every read.

A re-sync of a live member with in-flight writes, a warm LRU cache, and a key index is out
of scope. The interval and `log_trim_min_entries` are the knobs that make this rare. The
tailer already logs `gap=` every second (`src/db/corfu_storage.cpp:555`), so a member near
the limit is visible before it fails.

### 3.7 Corfu API facts (verified at commit `8f144d4`, `0.9.1.0-SNAPSHOT`)

The bench pins that commit (`bench/scripts/setup.sh:43-49`). Verified against its source:

| Need | API | Behaviour |
|---|---|---|
| Trim | `AddressSpaceView.prefixTrim(Token)` | Marks every address `<= sequence` trimmed. Sequencer trim first, then every log unit. `Token.of(epoch, addr)`; epoch from `getLayoutView().getLayout().getEpoch()`. |
| Read the mark | `AddressSpaceView.getTrimMark()` | Max over log units of the persisted `startingAddress` = **first untrimmed address** (`trimmed + 1`). |
| Seek | `IStreamView.seek(long)` | Next read starts at that global address, inclusive. `ThreadSafeStreamView.seek` is synchronized. |
| Fresh view below the mark | `AddressMapStreamView.moveToReadQueue` | Throws `TrimmedException` when the sequencer's per-stream trim mark is above the view's stop address. After `seek(first untrimmed)` the stop address equals the trimmed address, so no exception. |
| Reclaim disk | `AddressSpaceView.gc()` → `LogUnitClient.compact()` | `StreamLogFiles.compact()` deletes whole segments (`RECORDS_PER_LOG_FILE = 10000` entries) below the mark. `prefixTrim` alone frees nothing. The server also compacts on its own after 10 min, then every 45 min. The `--compact` flag is parsed and never read. |
| Client caches | `AddressSpaceView.gc(long)`, `invalidateClientCache()` | Client read cache only. Optional. |
| Corfu checkpoints | `CheckpointWriter`, `MultiCheckpointWriter` | SMR objects and `CorfuTable` only. Not usable for a raw `byte[]` stream. |

Do not set `StreamOptions.ignoreTrimmed(true)` on the poll view. It drops trimmed addresses in
silence. A lagging member then skips bytes with no error. The default (`false`) turns
the lag into `TrimmedException`, which §3.6 handles.

Unverified: whether the sequencer's in-memory trim mark is restored after a `corfu_server`
restart. §3.5 step 5 does not depend on it.

### 3.8 Config keys

All parsed in the `Metadata` constructor (`src/include/ozonedb/metadata.h`), all strings:

| Key | Default | Meaning |
|---|---|---|
| `log_trim_enabled` | `false` | This process runs the trimmer. |
| `log_trim_interval_ms` | `30000` | Cycle period. |
| `log_trim_min_entries` | `100000` | Minimum entries since the last checkpoint before a cycle runs. |
| `log_trim_keep_checkpoints` | `2` | Checkpoints kept in the store. Never below 2 (§3.4). |
| `checkpoint_dir` | `checkpoint` | Key prefix under `sstable_dir`. |

Constraint: `log_trim_enabled` requires `backend = corfu` and a `sstable_backend` that is not
`corfu`. The constructor throws otherwise, in the same style as the
`linearizable_reads` / `trust_background_tail` check.

The join path needs no key. Every process loads a checkpoint when `LATEST` exists.

### 3.9 The replay bound

After this plan a joiner does at most:

```
download(live files at C_latest)  +  replay(tail - C_latest) entries
```

`tail - C_latest` is at most `log_trim_min_entries + interval × append rate`. With the
defaults and 10k puts/s that is ~400k entries in the worst case and ~100k after a quiet
period. At the measured 28 µs per address that is 3–11 s of replay, so phase 5 must tune
the defaults down (for example `log_trim_min_entries = 20000`, interval 10 s) and report the
first-op time for both settings. The download is the live data logs (≤ ~150 MB) plus the two
small logs, at LAN object-store speed.

### 3.10 Interaction with the other branches

- **`worktree-plan-durability`** is merged into `visibility` (`ca03ba2b`), so this plan
  builds on its Corfu backend: one `submitBatch` path and `sealed_at_addr_`. The gate wraps
  `submitBatch`, `seal`, `remove`. The manifest carries `sealed_at_addr` per file, so a joiner
  rejects a late APPEND above a SEAL the same way a member does (`applyEntryBytes`, `:302`).
- **`cas`**. `key_versions_` is rebuilt from the full stream at every open and is documented
  as "complete, not a cache" (`corfu_storage.h`, cas worktree, line ~160). After a trim a
  joiner cannot rebuild it. The manifest reserves field numbers 20–29 for a `key_versions`
  section: one entry per live key {key, addr, deleted, optional inline value, file}. Size is
  about (key + 48 B) per live key, ~70 MB for 1M keys. The alternative is to redefine "not
  in map" as version −1 with an SSTable lookup. That decision belongs to the `cas` branch and
  is out of scope here. This plan must not break `cas`: the snapshot copies `key_versions_`
  when the field exists, and the loader restores it.

## 4. Phases

Each phase ends with its tests green on the two-node cluster (§5) or in the local unit suite.

### Phase 0 — Measure the live fraction

Goal: know the checkpoint size before any code depends on it.

1. `drainInitialEntries` already prints entries, batches, and time. Add payload bytes,
   live bytes (sum of `file_buffers_`), live file count, and the sizes of `metadata.log`
   and `task.log` to that line.
2. Open the loaded 1M dataset once with one process. Record the line.

Exit: one table in §0 with those numbers. Expected: live bytes ≤ 5 % of total.

Files: `src/db/corfu_storage.cpp`.

### Phase 1 — Bridge: trim, seek, trim mark, sentinel

1. `CorfuBridge.prefixTrim(long addr)`: `Token.of(epoch, addr)` → `prefixTrim`, then
   `addressSpaceView.gc()` (compact) and `gc(addr + 1)` on the client cache.
2. `CorfuBridge.trimMark()`: `getTrimMark().getSequence()`.
3. `CorfuBridge.seekPollView(long addr)`: `pollView.seek(addr)`.
4. `pollNext` and `pollBatch` catch `TrimmedException` and return a **zero-length** array.
   `null` keeps its meaning (timeout). C++ treats length 0 as TRIMMED.
5. C++: `mid_prefixTrim_`, `mid_trimMark_`, `mid_seekPollView_` in `loadBridge`, plus
   `jniPrefixTrim`, `jniTrimMark`, `jniSeekPollView`.

Tests (`tests/test_corfu_storage.cpp`, live server, self-skip without env):

- `trim_then_fresh_view_throws`: append 50 entries, trim at 25 through the bridge, open a
  second `CorfuDBStorage` on the same stream without a checkpoint. Expect the constructor to
  throw (§3.5 step 5).
- `seek_reads_only_above`: same setup, `seekPollView(26)` on a bridge, count entries = 24.

Files: `CorfuBridge.java`, `src/db/corfu_storage.cpp`, `src/include/ozonedb/corfu_storage.h`.

### Phase 2 — Snapshot and checkpoint format

1. `write_gate_` and `takeSnapshot()` (§3.3). Assert `pending_` is empty under the gate.
2. `record.proto`: `CheckpointManifest`.
3. New `src/db/checkpoint.{h,cpp}`: `CheckpointWriter::write(Storage&, Snapshot const&)` and
   `CheckpointReader::readLatest(Storage&, Snapshot&)`. Both take a `Storage&`, so
   `FileStorage` works too and the round trip is unit-testable with no S3 and no Corfu.
4. `CorfuDBStorage::restoreSnapshot(Snapshot&&)`: install buffers, sets, and
   `last_applied_addr_`.

Tests:

- `CheckpointTest.round_trip_file_storage`: three files, one sealed, one removed, C = 41.
  Write, read back, compare byte for byte. No Corfu needed.
- `CheckpointTest.latest_written_last`: inject a failure into the last file `PutObject`.
  Expect no `LATEST` change.
- `CorfuStorageTest.snapshot_is_a_prefix`: two storages on one stream. Storage A appends
  in a loop on a thread. Storage B calls `takeSnapshot()` ten times. For each snapshot
  `C`, replay the stream from 0 to `C` with a third bridge and compare to the snapshot.

Files: `src/include/ozonedb/record.proto`, `src/db/checkpoint.{h,cpp}`,
`src/db/corfu_storage.cpp`, `corfu_storage.h`, `CMakeLists.txt`, `tests/test_checkpoint.cpp`.

### Phase 3 — Join from a checkpoint

1. `bootstrap(Storage* checkpoint_store)` (§3.5). Remove the drain from the constructor.
2. `DB::DB`: build `sstable_storage` first when `sstable_backend_set`, then `log_storage`,
   then `bootstrap`.
3. TRIMMED retry loop and the trim-mark guard.
4. Fail-stop for live members (§3.6).

Tests:

- `CorfuStorageTest.join_from_checkpoint`: storage A writes 1,000 entries across three
  files, seals one, removes one. Checkpoint at `C`, `prefixTrim(C)`, then open storage B
  with the checkpoint store. Compare every `read`, `size`, `isSealed`, `exist` between A
  and B. Then A appends 100 more entries. B must see them.
- `CorfuStorageTest.join_after_second_trim`: as above, but trim again after B read `LATEST`
  and before B seeks. Expect B to retry and succeed.
- `corfu_smoke` and `corfu_multiwriter_smoke`: reopen after a checkpoint + trim, 200/200 keys.
- `DBTest`-level: `openDB`, put 10k keys, close with a final trim, reopen, get all 10k.

Files: `src/db/corfu_storage.cpp`, `src/db/db.cpp`, `tests/corfu_smoke.cpp`.

### Phase 4 — The trimmer and the bench wiring

1. `src/db/log_trimmer.{h,cpp}`: the thread from §3.4. Started in `openDB` when enabled,
   stopped and run once more in `closeDB`.
2. `Metadata`: the five keys and the constraint (§3.8).
3. `ycsb.yaml`: `corfu.log_trim: {enabled, interval_ms, min_entries}`.
   `_make_corfu_config_per_writer` (`bench/scripts/local/run_local_ycsb_multiproc.py`)
   sets `log_trim_enabled = true` only for writer 0 on the first client host.
   `run_multinode_ycsb.py` gets `--log-trim` in the style of `--linearizable`, forwarded to
   every host.
4. Log one line per cycle: `C`, live bytes, upload time, gate pause, trim mark, disk before
   and after `gc()`.

Tests: `CorfuStorageTest.trimmer_two_cycles` — a trimmer with a 1 s interval and
`min_entries = 100` runs two cycles under a writer. Expect trim mark = `C_1 + 1` after cycle
2 and both checkpoints present.

Files: as listed, plus `src/include/ozonedb/metadata.h`, `bench/scripts/config/ycsb.yaml`,
`bench/scripts/ycsb_config.py`.

### Phase 5 — Cluster validation

Runs on the two-node cluster. See §5 for the procedure.

Exit criteria:

| Check | Pass condition |
|---|---|
| Load with the trimmer on, final trim at close | Corfu data dir ≤ 2 intervals of entries; `LATEST` present |
| Join time, 1 writer, then 8 writers | first YCSB op < 5 s on every process |
| Correctness after join | workload c over all 1M keys: 0 misses, default and `--linearizable` |
| Steady state, workload a, trimmer on vs off | sum throughput within 3 %; gate pause ≤ 100 ms per cycle |
| Restart the Corfu server from the trimmed dir | joiners still open; §3.5 step 5 does not fire |
| Bucket restored without the log dir | `openDB` throws with the step 5 message |

### Phase 6 — Follow-ups (not in this plan)

- Compact `metadata.log` in the checkpoint into a `View` snapshot, and drop COMPLETE tasks
  from `task.log`. Both logs then stop growing across checkpoints.
- Trimmer election through the task log (`TaskRecord` with a reserved task id).
- Re-sync of a live member that fell behind the mark, instead of fail-stop.
- Parallel file download in `bootstrap` (one thread per file object).

## 5. Bench integration and the validation procedure

The multinode wrapper restarts `corfu_server` per cell from a copy of `/mnt/corfu/load`
(`run_multinode_ycsb_with_corfu.sh:234`). With trimming, the log dir and the bucket form
one unit. The checkpoint in the bucket and the trim mark in the log dir must match. The
`cas` branch already snapshots and restores the bucket per cell (`load_corfu_dataset.sh`,
commit `5cacc48b`). Port that to `visibility` before phase 5, or run phase 5 on `cas`.

Procedure for phase 5 (the cluster is shared, check for other drivers first):

1. Stop `corfu_server`. Wipe `/mnt/corfu/load` and the bucket.
2. Start `corfu_server -l /mnt/corfu/load -s -a <lan> 9090`.
3. Load 1M × 1 KB with 8 writers, `--log-trim`. Writer 0 runs the trimmer.
4. Record `du -sh /mnt/corfu/load` and the object count under `checkpoint/`.
5. Snapshot the log dir and the bucket.
6. Run workload c, 60 s, 8 writers. Read the first-op time from the `-s` status stream.
7. Run the same cell with `--linearizable`. Expect 0 misses.
8. Run workload a, 120 s, trimmer on and off, 3 trials each. Compare sum throughput.
9. Restart the server from the snapshot and repeat step 6.
10. Delete `checkpoint/` from the bucket and repeat step 6. Expect every `openDB` to throw.

## 6. Risks and open questions

- **Gate pause.** The exclusive gate stops every writer for the copy. If phase 5 shows more
  than 100 ms, switch to a per-file copy under the gate with the gate released between files.
  That needs a per-file `C`, which the manifest format already allows (add `addr` to `File`).
- **Sequencer trim mark after a restart** (unverified). Covered by §3.5 step 5.
- **A trim below a member's position.** Only when a member lags by more than one interval.
  Fail-stop is deliberate. The `gap=` line gives warning.
- **Segment granularity.** Corfu frees whole segments of 10,000 entries. The last partial
  segment stays. Not a problem for the bound, only for the disk number in phase 5.
- **`cas` needs `key_versions` in the manifest.** Reserved fields, not implemented here.
- **Concurrent trimmers.** Safe, wasteful. One process per cluster in this plan.
- **Object-store outage during a cycle.** The cycle aborts before `LATEST`. The next cycle
  retries. `prefixTrim` never runs without a complete newer checkpoint.

## 7. Alternatives considered

- **Archive the raw trimmed entries to the object store.** Keeps history but does not bound
  the replay: a joiner must still parse every entry. Rejected as the primary artifact. It
  can be added later as an audit option.
- **Build the checkpoint from a second replay of the stream** instead of from local state.
  Removes the gate and every self-apply subtlety. Costs a full replay per cycle, the exact
  cost this plan removes. Rejected. The gate is small and testable (phase 2).
- **Corfu's own `CheckpointWriter`.** SMR objects only (§3.7). Rejected.
- **Force a flush of every sealed data log before the snapshot** to make the checkpoint
  tiny. Couples the trimmer to compaction scheduling and the task log. Not needed: the
  checkpoint is already bounded by compaction lag × `log_file_size_limit`.
- **A CHECKPOINT entry in the stream** as the pointer. A joiner cannot read it before it
  loads the checkpoint, so it needs `LATEST` anyway. Rejected as redundant.

## 8. Files to touch

| File | Change |
|---|---|
| `ozonedb-jni-maven/corfu-bridge/.../CorfuBridge.java` | `prefixTrim`, `trimMark`, `seekPollView`, TRIMMED sentinel |
| `src/include/ozonedb/corfu_storage.h`, `src/db/corfu_storage.cpp` | write gate, `takeSnapshot`, `restoreSnapshot`, `bootstrap`, fail-stop, JNI ids |
| `src/include/ozonedb/record.proto` | `CheckpointManifest` |
| `src/db/checkpoint.{h,cpp}` (new) | writer and reader over `Storage&` |
| `src/db/log_trimmer.{h,cpp}` (new) | the trim cycle |
| `src/include/ozonedb/metadata.h` | five keys and the constraint |
| `src/db/db.cpp` | storage build order, `bootstrap`, trimmer start/stop, final trim |
| `CMakeLists.txt` | new sources, `tests/test_checkpoint.cpp` |
| `tests/test_corfu_storage.cpp`, `tests/test_checkpoint.cpp`, `tests/corfu_smoke.cpp` | tests listed per phase |
| `bench/scripts/config/ycsb.yaml`, `ycsb_config.py`, `run_local_ycsb_multiproc.py`, `run_multinode_ycsb.py` | `corfu.log_trim`, `--log-trim` |
| `CLAUDE.md` | one paragraph on checkpoints and the bucket + log dir pairing |
