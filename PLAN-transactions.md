# Plan: OCC transactions on the shared log (branch `cas`)

## Goal

Add serializable multi-key transactions to OzoneDB and produce the evidence a
paper section needs. The mechanism is the one the paper already describes as
Algorithm 1 (append-based CAS): a commit record carries the read-set versions
and the write set, every replica validates it at its log position, and every
replica applies it or skips it identically. The single-key CAS on this branch
is that protocol with a read set of one key. This plan generalizes it and
measures it.

The deliverable has four parts:

1. Engine: a commit record with a read set and a multi-record write set.
2. Client API: a `Transaction` object in C++ and one new JNI call.
3. Correctness evidence: lost updates, write skew, atomicity, and crash
   recovery, all at zero anomalies, on two or more client nodes.
4. Performance evidence: latency breakdown, contention curve, write-set size
   sweep, and a regression check with the feature off.

The paper currently says "OzoneKV does not support transactions" (Related
Work). This plan turns that sentence into a section. The related-work
framing stays: Hyder and Tango use append order as the serialization
backbone. OzoneDB reaches the same point from the coordination protocol it
already has for compaction.

## What the branch already provides

| Piece | Where | Status |
|---|---|---|
| Global version per key = log address of the last accepted write | `CorfuDBStorage::key_versions_` | Done |
| Deterministic validation at the entry's log position, on every replica | `applyConditionalLocked` (`corfu_storage.cpp:800`) | Done, one key only |
| Writer never self-applies a conditional entry; verdict via `cas_outcomes_` | `appendConditional` (`corfu_storage.cpp:854`) | Done |
| Key-only payload decode outside `mtx_`, handles many records per payload | `decodeRecordHeaders` | Done |
| Inline value per accepted CAS key, released at `REMOVE` | `KeyVersion`, `releaseInlineValuesLocked` | Done |
| One fence per versioned read, reuse of a `DB::sync()` token | `DB::getVersioned` | Done |
| Strict read forced on the versioned path | `DB::get(..., force_strict)` | Done |
| Bridge wakes the poller on a local append | `CorfuBridge.awaitAppendOrTick` | Done |
| Conflict backoff in the probe | `ConsistencyProbe` counter-worker | Done |
| `track_versions` gate, off by default | `Metadata::track_versions` | Done |

Only one line blocks multi-key commits: `recs.size() == 1` at
`corfu_storage.cpp:805`.

## Semantics to implement

### Commit record

A transaction commits with ONE `CorfuEntry`:

- `file_name` = the current data-log tail.
- `op` = `APPEND`, `conditional` = true.
- `payload` = the write set, serialized as consecutive `Record`s. This is the
  same layout that `appendInBatch` and `decodeRecordHeaders` already use.
- `read_set` = a new repeated field `{key, expected_version}`.

Validation rule, evaluated under `mtx_` at the entry's log position:

- Accept only if, for every read-set entry, `key_versions_[key].addr ==
  expected_version` (`-1` = unwritten).
- On accept: append the whole payload to `file_buffers_[file]`, and for every
  write-set record set `addr`, `deleted`, and the inline value.
- On reject: append nothing. Record the verdict in `cas_outcomes_` if the
  entry is our own.

Write-set keys that are not in the read set are blind writes. They are not
validated. Two transactions that blind-write the same key both commit, and
log order decides the final value. That is serializable.

An empty write set with a non-empty read set is a read-only validation
record. It appends no bytes. It gives a read-only transaction the same
serializability proof as a read-write one, at the cost of one append.

### Isolation

Read-write transactions are strictly serializable. The commit position is
the linearization point. All read-set versions equal the versions at that
position, so the transaction is equivalent to one that ran alone there.

Reads inside a transaction are not a snapshot. Each `getVersioned` reads
state that is at least as new as the fence. A later read can see a newer
write. Validation rejects the transaction if any read key changed, so this
cannot produce a wrong commit. It can only produce an abort.

Range predicates are out of scope. The API has no scan (`OzoneDBClient.scan`
logs "not supported").

### Version-value coupling (prerequisite, T1)

Validation trusts that the version and the value from `getVersioned` come
from the same write. Today this holds for CAS-written keys only. A blind put
whose bytes land past a LOGCREATE-frozen file size is invisible to scans
until compaction, while `key_versions_` already carries its address. A
transaction that reads that pair validates against the fresh version and
commits a value derived from the stale one. That is a lost update.

Fix: under `track_versions`, keep the inline value for every tracked write,
blind or conditional, until the file is `REMOVE`d. The tailer already has
the bytes in hand (`entry.payload()` at `value_off`). `applyBlindVersionsLocked`
copies the value instead of clearing it. Memory bound: one extra copy of the
uncompacted log tail. The tailer already holds `file_buffers_` for the same
files, so the footprint at most doubles for the tail. With `track_versions`
off nothing changes.

Optional refinement, later: store the byte offset per key and drop the inline
copy at LOGCREATE-apply time when `offset + len <= sealed_input_bytes`
(`record.proto` field 8). This bounds retention to the invisible window. Do
it only if E5 shows the memory matters.

## Engine changes (all on branch `cas`)

**T1. Inline every tracked write.** `applyBlindVersionsLocked` gains the
payload pointer and copies the value. Add the file bookkeeping that
`applyConditionalLocked` does (`inline_keys_by_file_`, pin on sealed or
removed). Update the `KeyVersion` header comment. ~40 lines.

**T2. Proto.** Add to `CorfuEntry` in `src/include/ozonedb/record.proto`:

```
message ReadSetEntry {
  required bytes key = 1;
  required sint64 expected_version = 2;
}
repeated ReadSetEntry read_set = 7;
```

Keep fields 5 and 6. An entry with an empty `read_set` and `conditional`
set is a legacy single-key CAS. ~15 lines.

**T3. Validation.** Rewrite `applyConditionalLocked`:

- Build the read set: `entry.read_set()` if non-empty, else
  `{payload key, expected_version}` (legacy path, requires one record).
- Accept only if every entry matches. Reject also if `decode_ok` is false
  and the payload is non-empty.
- On accept, loop over `recs` and update every key as the single-key path
  does today.
- Drop the `recs.size() == 1` check. Allow an empty payload (read-only
  validation). ~60 lines.

**T4. Storage API.** Add `Storage::appendTransaction(fileName, data, length,
read_set, result_version)` next to `appendConditional`. Implement it in
`CorfuDBStorage` by generalizing `appendConditional` (same sealed check, same
`jniAppendEntry`, same `waitForTailerLocked`, same `cas_outcomes_`). Make
`appendConditional` call it with a read set of one. `jniAppendEntry` and
`CorfuBridge.append` gain the read set as parallel arrays (`byte[][] keys`,
`long[] versions`). ~120 lines across C++ and Java.

**T5. LogHandler.** `addTransaction(std::vector<Record> const&, read_set,
new_version)` serializes the records into one buffer and retries on
`kSealed` exactly like `addRecordConditional`. ~40 lines.

**T6. DB API.** In `db.h`:

```cpp
class Transaction {
 public:
  Status get(std::string const& key, std::string& value);   // adds to read set
  void put(std::string const& key, std::string const& value);
  void remove(std::string const& key);
  Status commit(int64_t& version);   // kSuccess | kCasConflict | kFailure
  void abort();
};
Transaction DB::begin(bool validate_read_only = true);
```

- `begin()` calls `DB::sync()` once. `get()` consults the write buffer
  first, then `getVersioned` (reusing the token) and records
  `{key, version}`. `commit()` and `abort()` call `clearSync()`. A
  `Transaction` that goes out of scope without either calls `abort()` — a
  leaked token pins later reads on that thread.
- `commit()` with an empty write set appends a validation record when
  `validate_read_only` is true, else returns `kSuccess` without an append.
- Return `kFailure` when `track_versions` is off. ~150 lines.

**T7. JNI.** One new native method through all three layers
(`jni_OzoneDBJNI.{h,cpp}`, `OzoneDBJNI.java`):

```java
public native long txnCommit(byte[][] readKeys, long[] readVersions,
                             byte[][] writeKeys, byte[][] writeValues,
                             boolean[] deletes);
// >= 0 new version, -2 conflict, -1 error
```

The read phase uses the existing `sync()` / `getVersioned()` /
`clearSync()`. Keep `jni_OzoneDBJNI.h` in sync by hand. ~100 lines.

**T8. Config and docs.** No new config key. Update the "Compare-and-put"
section of `CLAUDE.md` into "Compare-and-put and transactions" with the
validation rule, the inline-value rule from T1, and the read-only
validation option.

**T9. Hardening (do after E1 passes).** A replica with `track_versions` off
applies conditional entries unconditionally and warns once. For a commit
record that is a correctness violation on that replica. Write a stream
marker entry at open (`file_name` = `__stream_config__`, payload =
`track_versions`) and fail `openDB` on a mismatch. ~50 lines.

Total: roughly 600 lines of engine and binding code, plus the harness below.

## Harness changes

**B1. Probe modes** in `ConsistencyProbe.java`:

- `txn-counter-worker`: the counter-worker loop through `txnCommit`
  (read set of one, write set of one). Same flags, same `worker-N.json`,
  plus `aborts`.
- `txn-transfer-worker`: N accounts seeded to 100 each. Each transaction
  reads two random accounts, moves a random amount, and commits. Emits
  `commits`, `aborts`, `max_attempts`, and per-commit latency in a CSV
  (`fence_us`, `read_us`, `append_us`, `verdict_us`, `keys`).
- `txn-audit`: a read-only transaction that reads every account and checks
  that the sum equals `N × 100`. Runs in a loop during the transfer phase.
  Emits the number of audits and the number of violations. Runs with and
  without read-only validation.
- `txn-skew-worker`: two keys `A`, `B` seeded to 1. Each round, two workers
  read both keys and one of them writes `A = 0` while the other writes
  `B = 0`. Under serializability at most one commit per round succeeds.
  Emits rounds and the count of rounds where both committed (must be 0).
- `txn-mixed-worker`: the transfer loop with `--keys K` and Zipfian key
  choice, for the contention and write-set-size sweeps.

**B2. `consistency.py` subcommands** that mirror `check-lost-updates`:
`check-txn-lost-updates`, `check-txn-transfer` (runs transfer workers and
one audit), `check-txn-skew`, `check-txn-crash` (kills one worker with
SIGKILL at a random point, restarts it, and repeats the audit). All pass
`track_versions=True` to `_make_configs`. `summary.json` keeps the existing
keys and adds `aborts`, `audits`, `audit_violations`, `skew_rounds`,
`skew_double_commits`.

**B3. Cross-node fan-out.** `consistency.py` runs every instance on one
client node. Add `--hosts K` to the transaction subcommands, driven from the
laptop the way `run_visibility_cross_node.sh` does: one stale-probe kill per
host, workers split across the first K `nodes.clients`, output dirs pulled
with `scp` and merged locally. Home directories are not shared between
nodes. Use the file barrier per host and a start time chosen by the driver.

**B4. Throughput path.** Add `txn-mixed-worker` as a workload the multinode
runner can launch (`run_multinode_ycsb.py` already fans out one process per
writer). Result token `ozonedb-corfu-txn`, with `-k{K}` and `-z{theta}`
suffixes on the label. `db_name` never changes.

**B5. Plots.** Extend `plot_consistency.py` with the four verdict tables,
and add `plot_txn.py`: latency stack per key count, throughput and abort rate
against writers, and against Zipfian theta.

## Experiments

### E0. Build and gate check (before anything else)

- `bash bench/scripts/build.sh` on every client node. The bridge jar must be
  rebuilt: the T4 append signature changes.
- `track_versions` off: run workload a and c at w8 for 60 s. Throughput and
  latency must equal the current `visibility` numbers within trial noise.
  If not, stop. The apply loop must stay byte-for-byte the old one when the
  gate is off.

### E1. Correctness (one client node, then two, then eight)

| check | command | pass condition |
|---|---|---|
| lost updates | `check-txn-lost-updates --workers 8 --increments 4000` | `lost_updates == 0` |
| atomicity + read-only serializability | `check-txn-transfer --workers 8 --accounts 100 --duration 60` | `audit_violations == 0` with validation on |
| write skew | `check-txn-skew --rounds 1000` | `skew_double_commits == 0` |
| crash recovery | `check-txn-crash --workers 4 --kills 3` | final counter equals acked increments, `audit_violations == 0` after restart |
| mixed blind and transactional | `check-txn-lost-updates --seed-blind` | `lost_updates == 0` (this is the T1 test) |

Run each on 1, 2 and 8 hosts (`--hosts`). Run the transfer check with
read-only validation off as well and report the violation count. A
non-zero count there is expected. It shows what the validation buys.

### E2. Latency breakdown (2 hosts, 1 worker each)

Per commit, at write-set sizes 1, 2, 4, 8, 16 keys with the same read set:
fence, reads, serialize + append, verdict wait. Compare to the single-key
CAS RMW (`check-lost-updates --cas`) and to the blind get + put RMW. Expect
fence ~0.7 ms, reads ~0.1 ms per key, append + verdict ~1.5 ms, roughly
constant in key count until the payload dominates.

### E3. Contention curve (8 hosts, the PLAN-throughput writer layout)

Writers 2, 4, 8, 16, 32, 64. Key space 1M. Zipfian theta 0 (uniform), 0.9,
0.99. Write set 2 keys. 120 s, 3 trials. Report committed transactions per
second and abort fraction. Plot both against writers, one line per theta.
Add the blind put throughput at the same layout as the upper bound.

### E4. Write-set size sweep (8 hosts, w8, uniform keys)

Keys per transaction 1, 2, 4, 8, 16, 32. Report commits per second and
p50/p99 commit latency. This shows the cost is one append regardless of
size, until the payload size dominates.

### E5. Cost of tracking (8 hosts, w8, workload a)

Three configurations on the same load: gate off, gate on with blind puts
only, gate on with the mixed-worker at theta 0. Report throughput, tail
latency, and the peak RSS of one writer process. This is the number that
justifies "off by default".

### E6. Analysis and figure

One figure with three panels: latency stack (E2), contention curve (E3),
write-set sweep (E4). One table with the E1 verdicts and the E5 cost. Every
number carries n and trial count in the caption.

## Implementation order

1. T1, T2, T3 (engine core). Syntax-check on the laptop as before. Build on a
   node.
2. T4, T5, T6 (API). Add a `corfu_txn_smoke` binary next to
   `corfu_multiwriter_smoke`: two writers, transfer loop, audit at the end.
3. T7 (JNI) and B1 (probe modes).
4. B2 and E1 on one host. Fix until every verdict is zero.
5. B3 and E1 on 2 and 8 hosts.
6. E0 gate check. Do it before any performance number is recorded.
7. B4, B5, then E2 through E5.
8. T8 docs. T9 hardening.
9. E6, then the paper section.

## Risks and caveats

- **T1 memory.** Inline values for every tracked write make the tail
  footprint up to twice `file_buffers_`. E5 measures it. If it is too high,
  add the offset-based release described above.
- **Verdict wait.** Every commit waits for its own entry to be applied. The
  bridge wake removes the 5 ms quantization. The remaining cost is the
  sequencer round-trip plus one poll batch. It cannot go below one apply.
- **Hot keys.** OCC aborts scale with contention. The backoff is jittered
  and exponential. Report the abort fraction next to every throughput
  number. Do not hide it.
- **Unknown outcome on crash.** A writer that dies between append and
  verdict does not know if it committed. The application must re-read.
  State this as a limitation. It is the same as in Tango.
- **Uniform gate.** `track_versions` must be on for every writer of a
  stream. T9 turns the warning into a refusal.
- **`kSealed` retry.** A commit on a sealed tail is retried up to 8 times
  with `newTail()`. Each retry is a new append. Count these as aborts of a
  separate kind in the probe output.
- **Payload size.** One commit record is one Corfu write. Check the
  runtime's maximum write size on the nodes before E4 and state the bound.
- **No range predicates.** Say so in the paper. Phantoms cannot occur
  because the API cannot express a range read.

## Status (2026-08-24, evening)

Steps 1–3 of the implementation order are written and committed on `cas`
(`95ab122d`: T1–T8 and `corfu_txn_smoke`; the following commit: B1 probe
modes and B2 `consistency.py` subcommands). Nothing is built or run yet.
The C++ passed `clang -fsyntax-only` on the laptop (only the pre-existing
Azure/libstdc++ lines in `storage.h` fail on macOS) and the Java passed
`javac` against the built YCSB core jar. Next: `bash bench/scripts/build.sh`
on a client node, then `./corfu_txn_smoke <config-with-track_versions>`,
then E0 and E1 on one host (step 4). B3–B5 and T9 are not started.

Deviations from the plan text above, all deliberate:

- **T4.** `CorfuBridge.append(byte[])` is untouched: the read set travels
  inside the serialized `CorfuEntry` (C++ builds the proto), so the bridge
  jar needs no source change (`build.sh` rebuilds it anyway).
- **T3.** Legacy CAS and commit records share `conditional = true` and are
  told apart by `expected_version` *presence*, not by an empty `read_set`:
  a commit record with an empty read set is a legal atomic blind
  multi-record write, so "empty read set" cannot mean "legacy".
- **T6.** `DB::commitTransaction(read_set, write_set, version)` is public
  under `Transaction::commit`; the JNI uses it directly and leaves the
  fence token to the Java caller. `getVersioned` (C++ and JNI) reports a
  never-written key as `kNotFound` with version `-1` (JNI: an 8-byte
  version with no value) so a transaction can validate "still absent".
- **B1.** The per-commit CSV has `fence_us, read_us, commit_us`: Java
  cannot split append from verdict wait, both happen inside `txnCommit`.
  `txn-mixed-worker` is `txn-transfer-worker` with `--keys` and `--zipf`.
- **B2.** `check-txn-crash` kills and restarts *transfer* workers and
  judges with the final validated audit (torn transfers are the crash
  hazard); the counter variant would need per-worker durable ack logs.
  `--seed-blind` on `check-txn-lost-updates` makes every worker blind-put
  the counter to 0 before the go barrier (race-free), so first reads pair
  a blind-written value with its version. A blind put *during* the run
  cannot be made race-free (blind wins), so the roll-window case of T1
  is covered only by the transfer checks' blind seeds plus log rolls.

The prerequisites listed under "What the branch already provides" are
committed on `cas` (`e0e39b2f`, `80ca93f1`, `88e47612`, `578e239c`).

CAS base validated on the new 9-node cluster (amd217 log/store, 8 clients,
`49e76a49`; bootstrapped from this worktree with `bootstrap.yml -e
target=nodes -e include_git=false`, harness fix `371e41f4`). One client
node, `check-lost-updates`:

| run | workers | increments | lost | conflicts | elapsed |
|---|---|---|---|---|---|
| blind get+put | 2 | 1000 | 500 (50%) | — | 1.22 s |
| `--cas` | 2 | 1000 | 0 | 197 | 2.36 s |
| `--cas` | 8 | 4000 | 0 | 3698 | 6.71 s |

At w8 the max attempts per increment were 11–19, so the backoff holds on
one hot key.

## Decisions taken here (change them if wrong)

- Serializable OCC, not snapshot isolation. Snapshot reads need MVCC through
  the LSM and are out of scope.
- Blind writes inside a transaction are not validated. Reads are.
- Read-only validation is an option with a default of on. E1 measures both.
- One JNI call for commit. The read phase reuses `sync`/`getVersioned`.
- `track_versions` stays off by default. Transactions require it.
- Result token `ozonedb-corfu-txn`, label suffixes for keys and theta.
- Correctness runs on 1, 2 and 8 hosts. Performance runs on 8 hosts with the
  PLAN-throughput writer layout.
