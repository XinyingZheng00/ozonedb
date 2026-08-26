# PLAN-durability

Fix the paths that let an acked write disappear or duplicate.

## 1. Purpose

The `visibility` branch claims strict cross-writer visibility. A review found fifteen
confirmed defects. Five of them break that claim directly. A write returns success, and
then the value is not readable. This plan puts the fixes in dependency order.

The plan holds six phases. Phase 0 restores the build. Phases 1 and 2 make the write path
honest. Phase 3 makes the read path complete. Phase 4 makes compaction safe. Phase 5 fixes
the backends and the client.

## 2. The theme

Five paths let an acked write disappear or duplicate:

- A failed append returns success (finding 1).
- An unsequenced append returns success (finding 2).
- A record lands on a tail that a peer already sealed (finding 3).
- One put appends the same record twice (finding 6).
- A late remote record overwrites a newer local one (finding 7).

The first four share one root cause. The code decides whether an append succeeded from
**local** state. The shared log is the only authority on order. Every fix in phases 1 and 2
moves that decision to the log.

## 3. Test hardware

Two CloudLab nodes. Node A runs the Corfu server, MinIO, and writer processes. Node B runs
the second writer set and the isolated reader.

Two nodes are enough for every one of the fifteen findings. The writers are separate
processes that coordinate only through the shared log. Two nodes also test the real network
path, which a single-node layout hides.

Before any run, update `nodes.clients` in `bench/scripts/config/ycsb.yaml` with both hosts.
Then check the resolved plan:

```bash
python3 bench/scripts/ycsb_config.py --check
```

The cluster is shared between sessions. Before a sync, a load, or a Corfu restart, look for
another session's drivers. Kill only your own process ids.

## 4. Order of work

Phase 0 comes first. No fix is testable while `runUnitTests` fails to compile.

Dependency notes:

- W2.2 needs W1.2 and W2.1. The retry is safe only after a rejected append is unreadable.
- W2.2 also needs W3.1. A record on a sealed tail must stay visible.
- W3.2 needs W1.1. Both change the same `pending_` structure.
- W1.1 fixes findings 2 and 8 in one refactor. Do not split them.
- Phase 4 and phase 5 are independent of phases 1 to 3. Run them in parallel if you want.

## 5. Phase 0 — restore the build

`cmake --build build -j` fails today. Only the explicit targets in `build.sh` survive.

### W0.1 — `tests/test_db.cpp:44`

The test calls the old two-argument `DB::get`. The signature is now
`get(key, value, guard)`, and `guard` keeps the value bytes alive.

1. Add a `std::shared_ptr<Record> guard` local.
2. Pass it as the third argument.
3. Keep `guard` in scope for every `*value` dereference.
4. Note that `DBTest` needs `src/config/cloud/shared_config_rocksdb_base.json`, which is
   absent from the tree. Add the config, or move the test out of the default suite.

### W0.2 — `tests/test_sstable.cpp:62`

The test declares `Record* record` and passes it to `Table::get`. That parameter is now
`std::shared_ptr<Record>&`.

1. Change every `Record*` local in the loop to `std::shared_ptr<Record>`.
2. Initialize `Table* table = nullptr` before `getSSTable`.
3. Assert that `table` is not null before the read loop.

### W0.3 — `src/config/corfu/shared_config_base.json:3,21`

The Corfu port and the MinIO port are transposed. `ycsb_config.py` sets
`DEFAULT_CORFU_PORT = 9090` and `DEFAULT_S3_PORT = 9000`. The base config holds
`corfu_endpoint` on 9000 and `s3_endpoint` on 9090. The updated `ycsb.yaml` agrees with
`ycsb_config.py`.

1. Set `corfu_endpoint` to `127.0.0.1:9090`.
2. Set `s3_endpoint` to `http://127.0.0.1:9000`.
3. Check that no generated per-writer config keeps the old ports.

**Check for phase 0:** `cmake -B build -DOZONEDB_ENABLE_CORFU=ON && cmake --build build -j`
completes, and `./runUnitTests` runs.

## 6. Phase 1 — do not ack bytes that are not durable

### W1.1 — batch ownership in `CorfuDBStorage` (findings 2 and 8)

Files: `src/db/corfu_storage.cpp`, `src/include/ozonedb/corfu_storage.h`.

Two defects share one structure. At line 705 the `sync_mode_` wait path returns `kSuccess`
when the `cached_file_` entry vanished during the wait. Another thread drained those bytes
into its own placeholder. That thread can still fail the JNI submit and erase the
placeholder. The bytes are then gone, and the caller already heard success.

At line 288 the tailer erases `pending_[fn]` on a REMOVE. A writer holds a list iterator
into that same list across the JNI call. The header comment at line 120 promises that the
tailer never touches `pending_`. The comment is wrong.

1. Add a `Batch` struct: `long addr`, `std::vector<unsigned char> payload`, and a
   `std::shared_ptr<std::promise<Status>> done`.
2. Change `pending_` to `std::unordered_map<std::string, std::list<std::shared_ptr<Batch>>>`.
3. Make every writer hold a `std::shared_ptr<Batch>`, never a list iterator.
4. Add a per-file promise for bytes that sit in `cached_file_`.
5. Create that promise when the first byte enters `cached_file_[fn]`.
6. Move the promise into the new `Batch` when a thread drains that file.
7. In the wait path, take the shared future **before** the wait.
8. Wait on that future. Delete the "entry vanished" test.
9. Set the batch result to `kSuccess` only when the returned address is not negative.
10. On REMOVE, do not erase `pending_[fn]`. Mark the file removed, and fail every batch.
11. In `reconcilePendingFrontLocked`, drop the payload of a removed file. Do not splice it.
12. Correct the header comment at line 120.

**Risk:** a promise must never be set twice. Route every completion through one helper.

**Check:** run `corfu_multiwriter_smoke` while you stop the Corfu server mid-run. Every
failed append must return `kFailure`.

### W1.2 — propagate the append status (finding 1)

File: `src/db/log_handler.cpp:72`.

`addRecord` retries only on `kSealed`. A `kFailure` from `appendInBatch` leaves the inner
loop, and the record still reaches the cache and the index. The function returns
`kSuccess`. Exhaustion of `kMaxRetries` also returns `kSuccess`, with an empty
`final_target`.

1. Store the result of `appendInBatch` in a named variable.
2. Retry only on `kSealed`.
3. On `kFailure`, free the buffer and return `kFailure`.
4. Do not touch `cache` or `key_index_` when the append fails.
5. Bound the inner `kSealed` loop as well as the outer loop.
6. When the loop ends with an empty `final_target`, return `kFailure`.

**Check:** a unit test with a storage stub that returns `kFailure`. `addRecord` must return
`kFailure`, and a later `readRecord` must miss.

## 7. Phase 2 — one append, one effect point

### W2.1 — the seal is a barrier at a global address (finding 3)

Files: `src/db/corfu_storage.cpp`, `src/include/ozonedb/corfu_storage.h`.

The post-append tail check reads the stale unfenced `View`. `newTail` reads the local
`sealed_files_`. Both are local guesses. A peer can seal and compact a tail while our
append is in flight. The record is acked, and the compaction then deletes it.

The shared log already holds the answer. If the sequencer puts our APPEND after the peer's
SEAL, our bytes belong to no file. Make every process apply that same rule.

1. Add `std::unordered_map<std::string, long> sealed_at_addr_`.
2. In `applyEntry`, on SEAL, store the address of that entry.
3. In `applyEntry`, on APPEND, drop the payload when the seal address for that file is lower.
4. After the JNI submit, compare the returned address with the seal address.
5. If the file is sealed below the returned address, return `kSealed`.
6. Drop that batch. Do not splice it into `file_buffers_`.
7. Check the order inside `seal()`, because the local pre-seal flush passes the same rule.

**Result:** a retry after `kSealed` cannot duplicate. The rejected bytes are unreadable in
every process.

### W2.2 — remove the re-append (finding 6)

File: `src/db/log_handler.cpp:79`.

A full tail makes the outer loop re-issue the same buffer against the new tail. The first
append already landed. One put then holds two effect points. The later copy shadows a
peer's newer write for that key.

1. Delete the outer re-append loop.
2. Keep one loop that retries only on `kSealed`, because `kSealed` means no bytes landed.
3. Bound that loop, and return `kFailure` on exhaustion.
4. Delete the post-append tail check that triggers a re-append.
5. If you want the diagnostic, keep a fenced check that only prints.

**Check:** run four writer processes over one key set. Count the records per key in the log.
No key must hold two copies from one put.

## 8. Phase 3 — every acked byte must be readable

### W3.1 — fenced bound for a sealed log (finding 5)

Files: `src/db/cache.cpp`, `src/db/metadata_log_handler.cpp:237`, `src/db/log_handler.cpp`.

`checkReadMoreLog` uses `latest_view->getFileSize(file_name)` as the read bound for a sealed
file. That size comes from `sealed_input_bytes` on the LOGCREATE record. The emitter fills
that field from its own unfenced view. Peer records written just before the seal fall
outside the bound. Those records stay invisible, and `linearizable_reads` does not help.

Two comments call the field heuristic-only. Both are wrong. The field is a read bound.

1. In `checkReadMoreLog`, ask storage for the size of a sealed file.
2. Cache that fenced size per file, because a sealed file never grows.
3. Use the view size only as a fallback when storage returns 0.
4. Correct the comment in `log_handler.cpp` at `newTail`.
5. Correct the comment in `metadata_log_handler.cpp` at line 237.

**Cost:** one extra fence per sealed file, once per process. Measure it before you keep it.

### W3.2 — append-stable offsets (finding 9)

File: `src/db/corfu_storage.cpp:926` and the `read` overloads above it.

`read()` and `size()` splice `file_buffers_`, then `pending_`, then `cached_file_`. The
tailer appends remote bytes to `file_buffers_`, which sits first. A remote append therefore
shifts every unsequenced byte to a higher offset. The same offset then names different
bytes.

`MetadataLogHandler::readMetadataLog` reads a range and sets `this->offset = file_size`. One
shift desynchronizes that reader. The protobuf parse fails, and the reader never recovers.

1. Before the read lock, flush this process's `cached_file_[fn]`.
2. Wait until `pending_[fn]` is empty.
3. Serve `read()` and `size()` from `file_buffers_` only.
4. Keep the tailer fence at the start of both calls.
5. Run the drain outside `mtx_`, because it makes a JNI call.

**Result:** offsets become a function of sequenced bytes alone. Global order makes them
append-stable.

**Risk:** this adds a flush to the read path for a file with buffered writes. Check the
effect on the read latency in workload C.

### W3.3 — stream tail, not global tail (finding 10)

File: `ozonedb-jni-maven/corfu-bridge/src/main/java/site/ycsb/db/corfu/CorfuBridge.java:202`.

`tailAddress()` returns `getSequencerView().query().getSequence()`. That is the global log
tail. The tailer advances only over its own stream. A second stream on the same cluster, or
a hole, leaves the target above anything the tailer reaches. `waitForTailerLocked` then
blocks forever, with no timeout. Every fenced read stops.

1. Query the tail of the stream that this bridge polls, not the global tail.
2. Check the CorfuDB 0.9.1 `SequencerView` API for the per-stream query form.
3. Return that stream tail address.
4. Add a timeout to `waitForTailerLocked` in `src/db/corfu_storage.cpp:578`.
5. On timeout, print the target address and the applied address, then return an error.
6. Make every caller of the fence handle that error.

**Check:** start a second stream on the same Corfu cluster. Fenced reads must still finish.

### W3.4 — ordered index upsert (finding 7)

Files: `src/db/log_key_index.cpp:20`, `src/include/ozonedb/log_key_index.h`.

`upsert` is blind last-writer-wins by arrival time. The tailer callback and the local write
path both call it. A late remote record can overwrite a newer local one, and the index never
corrects itself.

1. Add a monotonic rank field to `Entry`.
2. Use the Corfu global address of the source entry as the rank.
3. Pass that address from the tailer callback.
4. Pass that address from the writer path.
5. Skip an upsert whose rank is below the stored rank.

**Design note:** the local writer knows its address only after the batch flushes. Two
options are open. Publish to the index after `reconcilePendingFrontLocked`, or publish
early with a provisional rank and correct it later. The first option is simpler and costs
read-my-writes latency inside one process. Pick one before you start, because this item has
the most design freedom of the fifteen.

## 9. Phase 4 — compaction must not delete live or unwritten data

### W4.1 — check the flush status (finding 4)

Files: `src/db/sstable/table_builder.cpp:253`, `src/db/compaction.cpp`.

`finish()` calls `r->storage->flush(r->fileName)` and discards the result. A failed S3
PutObject leaves no object. The compactor still appends a COMPACT record for that SSTable,
and then deletes the inputs. The data is gone.

1. Assign the result of `flush` to `r->status`.
2. Keep an earlier failure, because `r->status` can already hold one.
3. In `compaction.cpp`, check the result of `finish()`.
4. On failure, do not append the COMPACT record.
5. On failure, do not delete the inputs.
6. Release the task claim so that another writer retries the task.

### W4.2 — apply the COMPACT before you remove (finding 11)

File: `src/db/compaction.cpp:420`.

`appendToMetadataLog` only writes the record. The local view rolls forward later, on the
periodic thread. The code invalidates the index and removes the inputs in between. A
concurrent default-mode get then finds the file in the view, reads it, and gets nothing. The
get returns `kFailure` for a key that exists.

1. Roll the local metadata log forward right after the append.
2. Check that the local view no longer lists the inputs.
3. Invalidate the index and the cache only after that check.
4. Delete the inputs only after that check.
5. In `LogHandler::readRecord`, treat `kNotFound` for a listed file as skip, not failure.

### W4.3 — remove through the right backend (finding 13)

File: `src/db/compaction.cpp:432`.

The loop calls `this->storage->remove(input)` for every input. `storage` is the log backend.
The shipped config sets `sstable_backend: s3`. SSTable inputs therefore never leave MinIO,
which grows without bound. Corfu receives a REMOVE for a file it never held. Line 444 then
calls `this->storage->size(input)` on a file that is already removed.

1. Branch on `log_level_compaction`.
2. Delete log inputs through `storage`.
3. Delete SSTable inputs through `sstable_storage`.
4. Capture the input sizes before the deletes.
5. Use the captured sizes in the listener call at line 444.
6. Remember that the two pointers alias when `sstable_backend` is unset.

### W4.4 — null guards on the SSTable read path (finding 12)

Files: `src/db/sstable/sstable_handler.cpp:32`, `src/db/sstable/table_reader.cpp`,
`src/db/cache.cpp`.

`getSSTable` now returns null on three paths: a failed `Table::open`, a caught exception,
and a follower whose leader failed. `readRecordFromAllLevel` calls `table->get(key, record)`
with no test. `this->latest_view` is also a snapshot pointer that is null before the first
publish.

1. Return `kFailure` when `latest_view` is null.
2. Skip the file when `getSSTable` returns null.
3. Check `rep_->index_iter` in `Table::get`.
4. Free `footer_block` on the `Table::open` failure path.
5. Return `kFailure` from `Table::open` when the size is below `FOOTER_BLOCK_SIZE`.

## 10. Phase 5 — backends, JNI, and the client

### W5.1 — lock the local append (finding 14)

Files: `src/db/metadata_log_handler.cpp:66`, `src/db/storage.cpp:45`.

`appendToMetadataLog` lost its lock. The comment says that the storage layer owns its own
concurrency. `CorfuDBStorage` does. `FileStorage::append` does not. It writes to one shared
`ofstream` per file with no mutex. Two threads interleave bytes, and `metadata.log` is
corrupt on disk.

Fix this in the backend, because the comment states the right contract.

1. Add a per-file mutex to `FileStorage`.
2. Hold it across the write, the flush, and the `fsync` in `append`.
3. Hold it in `appendNoFlush` and in `flush` too.
4. Leave `appendToMetadataLog` lock-free, so that readers do not block.

### W5.2 — `S3Storage::append` truncates

File: `src/db/s3_storage.cpp`.

`append` buffers the bytes and calls `flushLocked`. `flushLocked` moves the buffer out and
erases the map entry. A second `append` therefore puts only the new bytes, and the object
loses everything before them.

1. Keep the buffer after a successful put.
2. Put the whole buffer on every call.
3. Add a comment that S3 must never host a log file.
4. Consider a hard failure on a second `append` instead, because no caller needs it.

### W5.3 — `update()` skips the put (finding 15)

File: `ycsb/ozonedb/src/main/java/site/ycsb/db/OzoneDBClient.java:68`.

`update()` reads the key first. On a null result it returns `NOT_FOUND` and never calls
`put`. In a multi-writer cell the key often belongs to a peer that is not visible yet. The
write is lost, and YCSB counts a fast operation. Throughput reads high for work that never
happened.

1. Treat a null current value as an empty base map.
2. Apply the new fields and call `put`.
3. Return `Status.ERROR` when `serializeValues` or `put` throws.
4. Do not report throughput on the error path.
5. Guard `insert()` too, because it calls `db.put` with a null value after an exception.

**Note:** this changes measured throughput. Every earlier multi-writer number is suspect.
Re-run the baseline cells after this fix.

### W5.4 — JNI exception barrier

File: `src/jni/jni_OzoneDBJNI.cpp:58`.

A C++ exception crosses the JNI boundary. A config error therefore aborts the whole JVM,
with no Java stack trace.

1. Wrap the body of every entry point in `try` and `catch (...)`.
2. Throw a Java `RuntimeException` with the C++ message.
3. Return a safe default after the throw.
4. Check `db_instance` for null in `openDB`, because the current code dereferences it before
   the status test.

### W5.5 — `ConsistencyProbe` hangs the driver

File: `ycsb/ozonedb/src/main/java/site/ycsb/db/ConsistencyProbe.java:796`.

`System.exit(0)` runs only on the success path. An exception from a mode function leaves
main. The Corfu threads are not daemon threads, so the JVM stays up. `consistency.py` then
blocks in `proc.wait()` forever.

1. Wrap the body of `main` in `try` and `catch`.
2. Print the stack trace in the catch block.
3. Call `System.exit` from a `finally` block on every path.
4. Use a non-zero code on the failure path.

### W5.6 — contention script and the linearizable label

File: `bench/scripts/local/run_corfu_compaction_contention.py`.

The script has no `--linearizable` flag. Its result glob at line 72 matches
`ozonedb-corfu_w*of{writers}` only. A linearizable run writes
`ozonedb-corfu-linearizable_w*`, so the script finds no results.

1. Add a `--linearizable` argument.
2. Pass it into the per-writer config through `linearizable_corfu_settings`.
3. Derive the engine token from the effective settings.
4. Use that token in the glob at line 72.

## 11. Test plan

Run each level only after the level below passes.

**Level 1 — unit.** From `build/`, run `./runUnitTests`. Add the two new tests from W1.2 and
W3.4.

**Level 2 — smoke.**

```bash
./corfu_smoke <shared-config.json> 10000
./corfu_multiwriter_smoke <shared-config.json> 10000
```

Repeat the multi-writer smoke while you restart the Corfu server mid-run. No acked key may
disappear.

**Level 3 — cross-node visibility.** Put the writers on node A and the reader on node B.
Run `consistency.py` in both modes:

- default mode, with `trust_background_tail`
- `--linearizable`

Every acked write must become readable on node B. Under `--linearizable` it must be readable
at once.

**Level 4 — workload.** Load 1M records with four writer processes across the two nodes.
Then run workload A for 300 seconds. Workload A is the one that exercises W5.3.

**Level 5 — compaction contention.** Run `run_corfu_compaction_contention.py` with and
without `--linearizable`, after W5.6.

Re-run level 4 after phase 5. The W5.3 fix changes the throughput numbers, so the earlier
multi-writer results are not comparable.

## 12. Findings outside the top fifteen

The review cap cut these. Phase 0 covers the first two. Phase 5 covers the rest.

- `runUnitTests` fails to compile (W0.1, W0.2).
- The Corfu and MinIO ports are transposed (W0.3).
- C++ exceptions cross the JNI boundary (W5.4).
- `ConsistencyProbe` exits only on the success path (W5.5).
- `S3Storage::append` truncates on a second call (W5.2).
- The contention script lacks `--linearizable` (W5.6).

## 13. Refutations

These three came up in review and are not defects. The plan lists them so that nobody files
them again.

- The empty-COMPACT `output_file(0)` crash cannot fire. The front-of-layout guard parks the
  record first.
- The orchestrator does not forward `corfu.endpoint`. That is the documented sync contract.
- The `pkill -f` self-match in the Corfu wrappers is real and harmless, because `pkill`
  signals all matches in one pass.
