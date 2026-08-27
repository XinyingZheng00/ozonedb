# PLAN-native-corfu

Replace the embedded JVM and `CorfuBridge.java` with a C++ Corfu client that speaks the
Corfu protobuf protocol over TCP. The put path then crosses one JNI boundary (YCSB to
OzoneDB) instead of three, and the tailer applies peer entries with no Java code.

## 0. Status — plan only (2026-08-27)

Written after the cost campaign `cost-20260827` (`bench/RESULTS-cost.md`). Nothing is
implemented. Base the work on `visibility` at `35f5cb90` or later, in a new worktree
`worktree-native-corfu`. The plan does not depend on `worktree-plan-cost`, but phase 5
reuses its extractor and its result tables.

## 1. Purpose

The cost model found that OzoneDB's client CPU per op is 2.16 ms at 8 writers on workload
a, 35x Cassandra's 0.061 ms. The client CPU line is $1,120 of $3,693 per month at 1 GB.
The per-op cost grows with the writer count (0.85 / 1.09 / 1.58 ms at 2 / 4 / 8 writers),
because every process tails the whole log.

The 10 GB replay lines split the tailer cost: `poll 1447–1813 ms, apply 105–108 ms` for
50,758 entries. That is 29–36 µs per entry on the Java side and 2.1 µs in C++. At 8
writers each process applies about 9,200 entries per second, so the Java side of the
tailer costs about 1 ms of the 2.16 ms per own op.

Per put the bytes cross JNI three times and are copied about nine times: YCSB to
`DB::put`, `CorfuEntry` to `appendChecked`, and back through `pollBatch`. Each
`appendChecked` also allocates a new file-name `byte[]`, a `UUID.randomUUID()` and a
`TxResolutionInfo`. The Corfu runtime's default codec is ZSTD, so every entry is
compressed once and decompressed by every tailer.

A native client removes the second and third crossings, the copies on the Java side, the
runtime's 500,000-entry read cache (`maxCacheEntries`), the Netty pools and one G1 heap per
writer (RSS 1.7–3.6 GB per writer today).

Targets (phase 5 measures them):

| Metric | Today | Target |
|---|---|---|
| Client CPU per op, workload a, 8 writers | 2.16 ms | ≤ 1.0 ms |
| Client CPU per op, workload a, 2 writers | 0.85 ms | ≤ 0.5 ms |
| Tailer cost per peer entry | ~30 µs | ≤ 5 µs |
| Replay at join, 50k entries | 1.5–1.9 s | ≤ 0.3 s |
| Client RSS per writer (YCSB JVM included) | 1.7–3.6 GB | ≤ 1.0 GB |
| Read misses, workload c after an 8-writer load | 0 | 0 |

## 2. Scope

In scope:

- One Corfu server with `-s`: layout server, sequencer and log unit on one node, one
  segment, one stripe, one log server, `CHAIN_REPLICATION`. That is how every bench script
  starts it (`run_multinode_ycsb_with_corfu.sh:271`).
- No TLS, no SASL.
- One stream per DB, as today (`corfu_stream_name`).
- CorfuDB at commit `8f144d4` (`0.9.1.0-SNAPSHOT`, `bench/scripts/setup.sh:49`).
- Entries readable by the Java runtime, and Java-written entries readable by the native
  client, so that mixed cells and the existing tools keep working.

Out of scope:

- Chain replication over more than one log unit, layout changes at run time, and
  reconfiguration. The client refuses any other layout at connect time.
- The first JNI hop (YCSB to OzoneDB). It is one call per op and does not grow with the
  writer count.
- Corfu checkpoints, SMR objects and `CorfuStore`.

## 3. The contract the native client must match

`src/db/corfu_storage.cpp` calls eleven Java methods (`loadBridge`, lines 205–231). The
native client implements the same eleven operations with the same results.

| Operation | Java today | Native RPCs |
|---|---|---|
| `append(payload) -> addr` | `IStreamView.append` | `TokenRequest{TK_MULTI_STREAM, 1, [S]}` then `WriteLogRequest` |
| `appendChecked(payload, snapshot, readKey, writeKey) -> {addr, -1} or {-2..-6, offending}` | `StreamsView.append(payload, TxResolutionInfo, S)` | `TokenRequest{TK_TX, 1, [S], txn_resolution}` then `WriteLogRequest` |
| `globalTail()` | `SequencerView.query()` | `TokenRequest{TK_QUERY, 0, []}` → `token.sequence` |
| `tailAddress()` | `SequencerView.query(S)` | `TokenRequest{TK_QUERY, 0, [S]}` → `stream_tails[S]` |
| `pollBatch(timeout, max)` | `pollView.next()` in a loop | `ReadLogRequest{[a..b]}` over the global address space (§4.3) |
| `seekPollView(addr)` | `IStreamView.seek` | local cursor only |
| `gcPollView(mark)` | `IStreamView.gc` | no-op (the native reader keeps no queue) |
| `prefixTrim(addr) -> trimMark` | `AddressSpaceView.prefixTrim` + `gc` | `SequencerTrimRequest`, `TrimLogRequest`, `CompactRequest`, `TrimMarkRequest` in that order |
| `trimMark()` | `AddressSpaceView.getTrimMark` | `TrimMarkRequest` |
| `close()` | `runtime.shutdown()` | close both sockets |
| `pollNext` | looked up, never called | drop the lookup |

Semantics that must not change (`corfu_storage.cpp`, `corfu_storage.h`):

- The tailer is the only writer of `file_buffers_`, own entries included, in address
  order. `last_applied_addr_` is stored last, under `mtx_` (`applyEntryBytes`, line 493).
- A refused token consumes no address. The abort codes are `-2` CONFLICT, `-3`
  NEW_SEQUENCER, `-4` SEQUENCER_OVERFLOW, `-5` SEQUENCER_TRIM, `-6` other. On CONFLICT the
  second value is the offending address (`token.sequence` of the abort response).
- The conflict key of a file is its raw name bytes. `CorfuBridge.appendChecked` builds the
  `TxResolutionInfo` directly, so no hash is applied (`CorfuBridge.java:160-167`). The
  native client sends the same bytes. A native SEAL and a Java SEAL then collide on the
  same key, which keeps mixed cells safe.
- `pollBatch` never pads a batch, returns early on the first empty read, returns "idle"
  after `timeout` and "trimmed" when the cursor is below the trim mark. It never skips a
  trimmed address in silence (`CorfuBridge.java:74-75`).
- An own append never waits for the 5 ms idle tick (`CorfuBridge.java:56-67`).
- `tailAddress()` is the per-stream tail, not the global tail (`CorfuBridge.java:340-349`).

## 4. Design

### 4.1 The seam: `CorfuClient`

Add `src/include/ozonedb/corfu_client.h`:

```cpp
class CorfuClient {
 public:
  struct AppendResult { int64_t addr; int64_t offending; };   // addr < 0 = abort code
  enum class Poll { kEntries, kIdle, kTrimmed };
  using EntrySink = std::function<void(int64_t addr, char const* data, size_t len)>;

  virtual ~CorfuClient() = default;
  virtual int64_t append(std::string_view payload) = 0;
  virtual AppendResult appendChecked(std::string_view payload, int64_t snapshot,
                                     std::string_view const* read_key,
                                     std::string_view const* write_key) = 0;
  virtual int64_t globalTail() = 0;
  virtual int64_t streamTail() = 0;
  virtual Poll pollBatch(int timeout_ms, int max_entries, EntrySink const& sink) = 0;
  virtual bool seek(int64_t addr) = 0;
  virtual void gc(int64_t mark) = 0;
  virtual int64_t prefixTrim(int64_t addr) = 0;
  virtual int64_t trimMark() = 0;
  virtual void close() = 0;
};
```

Two implementations:

- `JniCorfuClient` (`src/db/corfu_client_jni.cpp`): the JVM code moved out of
  `corfu_storage.cpp` with no behaviour change. `pollBatch` parses the existing
  `int32 count, count × (int32 len, int64 addr, payload)` array and calls the sink.
- `NativeCorfuClient` (`src/db/corfu/`): this plan.

`CorfuDBStorage` keeps every lock, counter and invariant. `applyBatchFromJava` becomes
`applyEntry(addr, data, len)`, called from the sink under the tailer thread. The
`CorfuEntry` protobuf envelope stays in this plan (see §9 for the follow-up).

The config key `corfu_client` selects the implementation (`jni` or `native`). The default
is `jni` until phase 5 flips it. The key is the A/B switch for every measurement and the
differential tests.

### 4.2 Transport (`corfu_transport.{h,cpp}`)

Frame layout (`NettyClientRouter.java:318-321`, `NettyCorfuMessageEncoder.java:38-64`):

```
int32 BE length          // covers marker + protobuf
byte  marker             // 0x01 = RequestMsg, 0x02 = ResponseMsg
bytes protobuf
```

One `Connection` = one TCP socket, one reader thread, one map from `request_id` to a
promise. Callers block on the promise with a 5 s timeout (`requestTimeout`). Several
threads can have requests in flight on one connection, because the server echoes the
request header and the reader demultiplexes on `request_id`.

Handshake on connect, before any other request (`ClientHandshakeHandler.java:156-169`):
`HandshakeRequestMsg{client_id = C, server_id = 0/0}` with header
`{epoch 0, cluster_id 0/0, ignore_cluster_id false, ignore_epoch true}`. The server drops
every other request until the handshake completes. The reply carries the server's
`corfu_source_code_version`. Log a warning when it differs from the pinned commit, do not
fail.

Keepalive: send `PingRequestMsg` with both ignore flags set when the socket is write-idle
for 2 s. Treat 7 s of read-idle as a dead connection (`RuntimeParameters.java:88-95`).
Reconnect, re-handshake, refetch the layout, then retry the request once.

Two connections per client: `ctl` for tokens, writes and trim, `tail` for the reader's
bulk reads. A 1000-entry read response must not delay a token request.

Header of every request after bootstrap: `epoch = E`, `cluster_id = K`, `client_id = C`,
both ignore flags false. `C` is one random UUID per `NativeCorfuClient`.
`corfu_source_code_version` can be 0 (`ClientHandshakeHandler.java:102-105`).

Errors (`server_errors.proto`, `ServerErrorMsg` is response payload 200):

| Error | Action |
|---|---|
| `wrong_epoch_error{correct_epoch}` | refetch the layout, retry once |
| `wrong_cluster_error` | fatal, throw |
| `not_bootstrapped_error`, `not_ready_error` | sleep 1 s, retry, up to 20 times |
| `trimmed_error` | `Poll::kTrimmed` on reads, `-1` on writes |
| `overwrite_error{cause}` | writer: new token, retry (§4.4) |
| `unknown_error` | Java-serialized `Throwable`, opaque: log, return `-1` |
| no reply in 5 s | the server drops unknown payload types in silence: reconnect, retry once |

### 4.3 Layout and bootstrap (`corfu_layout.{h,cpp}`)

1. `LayoutRequestMsg{epoch = -1}` with both ignore flags set (`LayoutClient.java:37-39`).
2. Parse `layout_json` with `google::protobuf::util::JsonStringToMessage` into a
   `google.protobuf.Struct`. Protobuf is already linked, so no JSON library is added.
3. Read `epoch`, `clusterId`, `sequencers[0]`, `segments[0].stripes[0].logServers[0]`.
4. Refuse the layout unless there is one segment, one stripe, one log server and
   `replicationMode == "CHAIN_REPLICATION"`. The error names the offending field.
5. Connect `ctl` and `tail` to the log server address. In the supported layout the
   sequencer and the log unit are the same endpoint as the layout server.

Open item for phase 1: the Gson form of `clusterId` (a string, or
`{mostSigBits, leastSigBits}`). The probe prints the raw JSON and the parser accepts both.

### 4.4 Writer (`corfu_writer.{h,cpp}`)

Stream id: `UUID.nameUUIDFromBytes(name)` = MD5 of the name bytes with the version bits
set to 3 and the variant bits to `10`. Use OpenSSL's MD5 (already linked). Pin the value
for `"ozonedb-ycsb"` in a unit test, computed once with `jshell`.

`appendChecked`:

1. `TokenRequestMsg{request_type TK_TX, num_tokens 1, streams [S], txn_resolution}` with
   `TxResolutionInfoMsg{tx_id, snapshot_timestamp {E, snapshot}, conflict_set {S: [readKey]},
   write_conflict_params_set {S: [writeKey]}}`. Omit an absent key's map entry. `tx_id` is a
   per-client counter, not a random UUID.
2. `resp_type != TX_NORMAL` → return the abort code and `token.sequence` as the offending
   address. No write happens (`SequencerServer.java:686-717`).
3. Build the `LogData` bytes (§4.6) with `GLOBAL_ADDRESS = token.sequence`, `EPOCH =
   token.epoch`, `BACKPOINTER_MAP = backpointer_map` from the token, `CLIENT_ID = C`.
4. `WriteLogRequestMsg{log_data}` on `ctl`.
5. On `overwrite_error`: request a new token with `snapshot = previous token.sequence`, as
   `StreamsView.java:196-200` does, up to 5 times (`writeRetry`).
6. Publish the address to the reader's wake channel (§4.5) and return it.

`append` is the same with `TK_MULTI_STREAM` and no `txn_resolution`.

`globalTail`: `TK_QUERY` with no streams → `token.sequence` (= global tail − 1).
`streamTail`: `TK_QUERY` with `[S]` → `stream_tails[S]`, `-6` when the stream has no entry.

`prefixTrim(addr)`, in the Java order (`AddressSpaceView.java:631-679`):
`SequencerTrimRequestMsg{addr}`, `TrimLogRequestMsg{address {E, addr}}`,
`CompactRequestMsg{}` with `ignore_epoch true`, then `TrimMarkRequestMsg{}` and return
`trim_mark`.

### 4.5 Reader (`corfu_reader.{h,cpp}`)

The reader keeps a cursor `next_` and a known stream tail `tail_`. It reads the **global**
address space in order, `[next_, tail_]`, in batches of `corfu_read_batch` addresses per
`ReadLogRequestMsg{address [...], cache_results true}` on `tail`. It does not use
`StreamsAddressRequest` or the Roaring address maps.

Why this is correct here: the OzoneDB DB is the only client, and all its tokens name
stream `S`. Every address in the log is one of three things:

| Entry | Detection | Action |
|---|---|---|
| ours | `DataType DATA` and `BACKPOINTER_MAP` contains `S` | deliver `(addr, payload)` |
| foreign | `DATA` without `S`, or `HOLE` | skip, advance |
| hole | `EMPTY` | wait and fill (below) |
| trimmed | `trimmed_error` or `DataType TRIMMED` | return `Poll::kTrimmed` |

Phase 1 verifies the assumption on the loaded datasets: the probe counts addresses by
type. If a server-written stream appears, switch to `StreamsAddressRequest` with CRoaring
(§8, alternative D).

Hole policy (`AddressSpaceView.java:73-78`, `Layout.java:376-384`): an `EMPTY` address at
or below the stream tail is a token without a write. Re-read with a backoff of 1 ms, 2 ms,
4 ms up to `corfu_hole_fill_timeout_ms` (default 10,000, the Java `holeFillTimeout`). Then
write a `HOLE` entry at that address: `DataType HOLE`, metadata `GLOBAL_ADDRESS` and
`EPOCH` only. On `overwrite_error` the writer landed first: re-read. A hole is never
delivered to the sink, but the cursor passes it.

`pollBatch(timeout_ms, max_entries, sink)`:

1. If `next_ <= tail_`, read one batch, deliver, return `kEntries`. Stop after
   `max_entries` and keep the rest of the decoded batch for the next call.
2. Else read the wake channel: `max(own_addr_, tail_)`. Own appends publish their address,
   so the tailer needs no sequencer round trip to see its own write.
3. Else query `streamTail()`. If it is above `next_ - 1`, go to 1.
4. Else wait on the wake condition variable for `corfu_idle_poll_ms` (default 5) and repeat
   until `timeout_ms` elapsed. Return `kIdle`.

The decoded entries alias the response buffer: the sink receives a pointer into the
`ReadLogResponseMsg` bytes. `CorfuDBStorage::applyEntry` parses the `CorfuEntry` from
there and appends the payload to `file_buffers_`. That is one copy per entry instead of
seven.

Pipelining: issue the read for the next batch before the sink processes the current one
(two buffers). Phase 2 implements the simple loop first and adds this only if the
per-entry cost stays above the target.

`seek(addr)`: `next_ = addr`, `tail_ = addr - 1`, drop the decoded remainder. No RPC
(`AbstractQueuedStreamView.java:920-941` is local too).

### 4.6 Codec (`corfu_codec.{h,cpp}`)

`LogData` bytes inside `LogDataMsg.entry` (`LogData.java:306-342`):

```
byte  data_type              // DATA 0, EMPTY 1, HOLE 2, TRIMMED 3, RANK_ONLY 4
if DATA:
  int32 BE payload_len
  byte  0x00                 // CorfuSerializer magic for byte[]; 0x42 = SMR entry
  bytes user payload         // payload_len - 1 bytes
byte  metadata_count
metadata_count × { byte type_id, value }
```

Metadata values (`IMetadata.java:199-227`, `CorfuProtocolCommon.java:501-593`), all
big-endian: `BACKPOINTER_MAP` 3 = `int32 count, count × (int64 msb, int64 lsb, int64
value)`, `GLOBAL_ADDRESS` 4 = `int64`, `CLIENT_ID` 10 = `int64 msb, int64 lsb`,
`THREAD_ID` 11 = `int64`, `EPOCH` 12 = `int64`, `PAYLOAD_CODEC` 13 = `int32`.

The writer emits `BACKPOINTER_MAP`, `GLOBAL_ADDRESS`, `CLIENT_ID`, `EPOCH` and no
`PAYLOAD_CODEC` (codec NONE). `BACKPOINTER_MAP` must contain `S`, or every Java reader
skips the entry (`AddressMapStreamView.java:88-99`).

Payload codec: the Java runtime default is ZSTD (`CorfuRuntime.java:413`). Phase 0 sets
`.codecType(Codec.Type.NONE)` in `CorfuBridge` so that both clients write uncompressed
entries. The native reader rejects `PAYLOAD_CODEC != 0` with a clear message. Datasets
loaded before phase 0 must be loaded again. Optional: add the `zstd` vcpkg port and decode
codec 2 on the read side, only if an old dataset must be read.

`UuidMsg` is `{int64 lsb = 1, int64 msb = 2}` (`rpc_common.proto:10-13`), the reverse of
the metadata order. Wrap both in one helper.

### 4.7 Config keys

All parsed in the `Metadata` constructor, all strings:

| Key | Default | Meaning |
|---|---|---|
| `corfu_client` | `jni` (phase 5: `native`) | `jni` or `native` |
| `corfu_read_batch` | `1000` | addresses per `ReadLogRequest` (Java `bulkReadSize` in the bridge is 1000) |
| `corfu_idle_poll_ms` | `5` | idle wait between stream-tail queries |
| `corfu_hole_fill_timeout_ms` | `10000` | wait before a HOLE write |
| `corfu_request_timeout_ms` | `5000` | per-RPC timeout |

`corfu_jar_path` and `corfu_jvm_opts` stay, and are ignored when `corfu_client = native`.

### 4.8 Build

- Vendor the proto files under `src/corfu_proto/` with the commit hash in a `README`:
  `rpc_common`, `log_data`, `tx_resolution`, `server_errors`, `service/base`,
  `service/layout`, `service/sequencer`, `service/log_unit`, and a trimmed
  `service/corfu_message.proto` that keeps only the payload cases used (10–14, 20, 30–34,
  40–53) with the original field numbers. Unknown oneof members are skipped on the wire,
  so the subset parses every server reply. `java_package` options are harmless to protoc
  for C++.
- `protobuf_generate_cpp` into `build/generated/protobuf/corfu/`, the same way as
  `record.proto`.
- New CMake option `OZONEDB_CORFU_JNI` (default ON in this plan, OFF in phase 6). The JNI
  include and `libjvm` link move under it. `OZONEDB_ENABLE_CORFU` alone builds the native
  client.
- No new vcpkg port. MD5 comes from OpenSSL, JSON from protobuf's `json_util`.

## 5. Phases

Each phase ends with its tests green, on the cluster for anything that needs a server.
The cluster is shared: check for other sessions' drivers before every run.

### Phase 0 — Seam, baseline, profile (2 days)

1. Extract `CorfuClient`, move the JVM code into `JniCorfuClient`. No behaviour change.
2. Replace `applyBatchFromJava` with the sink and `applyEntry(addr, data, len)`.
3. Add `corfu_client` to `Metadata`, with `native` rejected until phase 3.
4. Set `codecType(NONE)` in `CorfuBridge`. Load the 1 GB dataset again.
5. Profile one writer of an 8-writer workload a cell for 60 s: `perf record -g` on the
   process with `-DOZONEDB_PROFILING=ON`, and `async-profiler` on the same pid for the
   Java side. Record the Java CPU per tailed entry and the ZSTD share.

Exit: `CorfuStorageTest` 10/10, `corfu_smoke` and `corfu_multiwriter_smoke` pass, the
profile table is in §0, and the codec NONE cell gives the first CPU-per-op data point.

Files: `src/include/ozonedb/corfu_client.h`, `src/db/corfu_client_jni.cpp`,
`src/db/corfu_storage.{cpp,h}`, `src/include/ozonedb/metadata.h`, `CorfuBridge.java`.

### Phase 1 — Transport, layout, probe (3 days)

1. `corfu_transport`, `corfu_layout`, `corfu_codec` (header and frame only).
2. `tests/corfu_native_probe.cpp`: `corfu_native_probe <endpoint> <stream>` connects,
   handshakes, prints the raw layout JSON, the epoch, the server's source version, the
   global tail, the stream tail, the trim mark, and a histogram of the addresses `[trim
   mark, tail]` by `DataType` and by stream membership.
3. Golden frames: run `corfu_smoke` through the JNI client with `tcpdump -w` on the
   server, extract one handshake, one layout, one `TK_TX` token, one write and one read
   frame into `tests/fixtures/corfu/`. A unit test decodes them and re-encodes them
   byte-identical.
4. Unit tests without a server: frame round trip, `UuidMsg` conversion, stream id for
   `"ozonedb-ycsb"`, `LogData` encode/decode against a Java-written entry from the fixtures.

Exit: the probe runs against `/mnt/corfu/load` and reports 0 foreign entries, or the plan
switches to alternative D before phase 2.

### Phase 2 — Reader (4 days)

1. `corfu_reader` with the sequential batch loop, the hole policy, `kTrimmed`, `seek`.
2. `NativeCorfuClient` with the reader and the tail queries. Writes still throw.
3. Differential test: `corfu_stream_stats` gains `--client jni|native`. Both clients read
   the same stream. Compare the entry count, the last address, and a hash of every
   `file_buffers_` entry. Run it on the 1 GB and 10 GB control datasets (no trim) and on a
   trimmed dataset with a checkpoint.
4. Replay benchmark: `drainInitialEntries` already prints entries, batches and time. Add
   the client name. Record both clients on the 1M-entry control stream.

Exit: byte-identical state on all three datasets, replay ≤ 5 µs per entry of client CPU.

### Phase 3 — Writer (4 days)

1. `corfu_writer`: `append`, `appendChecked`, the overwrite retry, the abort mapping, the
   wake channel.
2. `CorfuStorageTest` with `corfu_client = native`: the fixture reads the client from
   `CORFU_TEST_CLIENT` and the whole suite runs twice.
3. Cross-client test: two storages on one stream, one JNI and one native, in both
   directions. Append, seal, remove and read from each side. Both processes must read the
   acked bytes (the `ack_never_outruns_a_peer_seal` pattern).
4. `corfu_smoke` and `corfu_multiwriter_smoke` with the native client. The multiwriter
   smoke covers concurrent LOGCREATE, the compaction claim and the cross-writer fence.

Exit: 10/10 on both clients, the cross-client test passes, `fastAcks() >= 40/45`,
`spuriousConflicts() == 0`, `droppedAboveSeal() == 0` on the native client.

### Phase 4 — Trim, bootstrap, failure paths (3 days)

1. `prefixTrim`, `trimMark`, the bootstrap seek and the `TRIMMED` marker.
2. Tests: `join_from_checkpoint`, `trimmed_stream_without_checkpoint_refuses_to_open`,
   `snapshot_under_concurrent_writes_is_a_prefix`, `trimmer_two_cycles`, all native.
3. Epoch test: restart `corfu_server` from the same log dir during a test. The next
   request gets `wrong_epoch_error`, the client refetches the layout and the test passes.
4. Reconnect test: kill the TCP connection with `ss -K` on the server. The client
   reconnects, re-handshakes and continues.
5. `LogTrimmer` on the native client for 4 cycles on the cluster (the phase 5 load).

Exit: every `CorfuStorageTest` passes on both clients, the two failure tests pass.

### Phase 5 — Bench and measurement (3 days plus cluster time)

1. `run_local_ycsb_multiproc.py --corfu-client jni|native` writes `corfu_client` into every
   generated per-writer config. `run_multinode_ycsb_with_corfu.sh` passes it through and
   suffixes the result label: `ozonedb-corfu-native`. A flag, never a `ycsb.yaml` edit.
2. Cells, 1 GB dataset, 120 s, 3 trials each: load with 8 writers, workload a and c at 2,
   4 and 8 writers, both clients. Then workload c with `--linearizable` at 8 writers.
3. Extract with `extract_cost_coefficients.py`: client CPU per op, RSS, throughput, ack
   latency, fast-ack share, first-op time at join.
4. Update `bench/RESULTS-cost.md`: a new `cpu_O`, the projection table, and the crossover.
   Note that `client_rss_max_kb` still includes the YCSB JVM.
5. Flip the `corfu_client` default to `native`. Keep `jni` selectable.

Exit criteria:

| Check | Pass condition |
|---|---|
| Workload a, 8 writers, client CPU per op | ≤ 1.0 ms, and ≤ 50 % of the JNI cell |
| Workload c, 8 writers, misses | 0, default and `--linearizable` |
| Throughput, workload a, 8 writers | ≥ the JNI cell |
| Join, 8 writers | first YCSB op ≤ 3 s |
| RSS per writer | ≤ 1.0 GB |
| `LogTrimmer`, 4 cycles | trim mark advances, joiner restores, 0 misses |

### Phase 6 — Retire the JVM path (follow-up, not in this plan)

- `OZONEDB_CORFU_JNI = OFF` by default. Maven then builds only the YCSB binding.
- `setup.sh --role client --no-corfu-runtime` becomes the default for clients.
- Remove `CorfuBridge.java` after one full campaign on the native client.
- Drop the `CorfuEntry` protobuf for a fixed header, and skip the payload decode for own
  entries (the writer still holds the bytes). Both are listed in §9.

## 6. Validation procedure on the cluster

1. Check `ps` on amd127 and every client for another session's drivers. Stop if any run.
2. Stop `corfu_server`. Wipe `/mnt/corfu/load` and the bucket.
3. Start `corfu_server -l /mnt/corfu/load -s -a <lan> 9090`.
4. Run `corfu_native_probe <lan>:9090 ozonedb-ycsb` on a client. Record the layout JSON.
5. Load 1M × 1 KB with 8 writers, `--corfu-client native`. Snapshot the log dir.
6. Run `corfu_stream_stats --client jni` and `--client native`. Compare.
7. Run the phase 5 cells. Pull the results with the existing `scp` path.
8. Restart the server from the snapshot and run workload c once more. Expect 0 misses.

## 7. Risks and open questions

- **Protocol drift.** The client matches commit `8f144d4`. The probe warns on a different
  `corfu_source_code_version`. `setup.sh` pins the commit, so the risk is a deliberate
  upgrade, not drift.
- **The sequential-read assumption** (§4.5). The phase 1 gate covers it. Alternative D is
  the fallback and costs one dependency.
- **Layout JSON shape.** The `clusterId` form is unverified. The probe shows it in phase 1.
- **Holes.** A crashed writer leaves a hole that every tailer waits 10 s on, then fills.
  Java behaves the same. The fence timeout in `waitForTailerLocked` is 10 s, so a fenced
  read during that window times out as today.
- **Epoch changes.** The bench restarts the server per cell, so a client that outlives a
  restart sees `wrong_epoch_error`. Phase 4 tests it.
- **`unknown_error`.** A Java-serialized throwable that C++ cannot decode. The client logs
  it and returns `-1`, the same result as a Java exception today.
- **Server read cache.** `cache_results true` on tail reads keeps the server cache warm
  for the other seven tailers. Keep it, and measure server CPU in phase 5.
- **Mixed cells.** Both clients write codec NONE entries with `S` in the backpointer map
  and raw file-name conflict keys, so a cell can mix them. Do not mix a phase 0 build
  with an older build: the older one writes ZSTD entries that the native reader refuses.
- **The YCSB JVM stays.** The client process is still Java. The plan removes the Corfu
  runtime from it, not the process. RSS numbers must be read with that in mind.
- **Sequencer load.** Idle tailers query the stream tail every 5 ms, 1,600 requests per
  second at 8 writers. That is the Java rate today. `corfu_idle_poll_ms` is the knob.

## 8. Alternatives considered

- **A. Optimize the bridge and keep the JVM.** Direct buffers, no per-put allocations,
  own-entry skip, range reads. Each removes a part of the 30 µs. None removes the runtime's
  read cache, the second heap, or the JNI copies on the put path. Kept as the fallback if
  phase 2 misses its target.
- **B. Run the Corfu runtime in the YCSB JVM.** Removes one heap, not one hop. The tailer
  still decodes in Java.
- **C. A Rust client through `cxx`.** A new toolchain on every bench node and in
  `setup.sh`. The C++ toolchain is already there.
- **D. Address-map tailing, as the Java view does it.** `StreamsAddressRequest` returns a
  Roaring64 bitmap (big-endian scalars around little-endian portable bitmaps,
  `StreamAddressSpace.java:329-350`). Needs CRoaring. Only needed if the log holds
  entries from other streams. Fallback for §4.5.
- **E. Batch several puts into one Corfu entry.** Helps multi-threaded clients only. The
  measured cells run one YCSB thread per writer.

## 9. Follow-ups outside this plan

- Fixed entry header instead of the `CorfuEntry` protobuf: `{op, client_id, name_len,
  name, payload}`. The tailer then parses with pointer arithmetic.
- Own-entry skip: the writer keeps the bytes, the reader delivers only the address for
  own entries. Saves 1/N of the tailer decode at N writers.
- Key-first records so the tailer indexes without a value decode.
- The first JNI hop: `GetPrimitiveArrayCritical` in `jni_OzoneDBJNI.cpp`.

## 10. Files to touch

| File | Change |
|---|---|
| `src/include/ozonedb/corfu_client.h` (new) | the interface (§4.1) |
| `src/db/corfu_client_jni.cpp` (new) | the JVM code moved out of `corfu_storage.cpp` |
| `src/db/corfu/corfu_transport.{h,cpp}` (new) | frames, handshake, keepalive, reconnect |
| `src/db/corfu/corfu_layout.{h,cpp}` (new) | layout fetch and validation |
| `src/db/corfu/corfu_codec.{h,cpp}` (new) | `LogData`, UUIDs, stream id |
| `src/db/corfu/corfu_writer.{h,cpp}` (new) | tokens, writes, trim |
| `src/db/corfu/corfu_reader.{h,cpp}` (new) | sequential tailer, holes, seek |
| `src/db/corfu/native_corfu_client.{h,cpp}` (new) | `CorfuClient` over the four above |
| `src/corfu_proto/**` (new) | vendored protos with the commit hash |
| `src/db/corfu_storage.{cpp,h}` | use `CorfuClient`, `applyEntry(addr, data, len)` |
| `src/include/ozonedb/metadata.h` | five keys (§4.7) |
| `ozonedb-jni-maven/corfu-bridge/.../CorfuBridge.java` | `codecType(NONE)`, drop `pollNext` |
| `CMakeLists.txt` | `OZONEDB_CORFU_JNI`, proto generation, new sources and tests |
| `tests/test_corfu_codec.cpp` (new), `tests/fixtures/corfu/` (new) | golden frames, unit tests |
| `tests/corfu_native_probe.cpp` (new) | phase 1 probe |
| `tests/test_corfu_storage.cpp`, `tests/corfu_stream_stats.cpp` | `CORFU_TEST_CLIENT`, `--client`, cross-client test |
| `bench/scripts/local/run_local_ycsb_multiproc.py`, `run_multinode_ycsb_with_corfu.sh`, `load_corfu_dataset.sh` | `--corfu-client` |
| `bench/RESULTS-cost.md`, `CLAUDE.md` | new `cpu_O`, one paragraph on the two clients |
