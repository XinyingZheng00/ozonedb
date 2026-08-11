# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

OzoneDB is a research LSM-tree key-value store whose entire durable state lives behind an
append-only "shared log" storage abstraction. Multiple independent writer *processes* (possibly on
different machines) coordinate through the log itself — there is no server. It is built as a shared
library (`libOzoneDB.so`), driven through a JNI binding from a YCSB benchmark client.

## Environment

`OZONEDB_HOME` must be exported and point at the repo root. `CMakeLists.txt` resolves the vcpkg
toolchain and `link_directories` from it, and every bench script reads it. The build targets Ubuntu
bench nodes (apt packages, openjdk-8, a writable `/tank` data directory) — it is not set up to build
on macOS.

First-time machine setup (must be *sourced*, it appends `OZONEDB_HOME`/`JAVA_HOME` to `~/.bashrc`):

```bash
. bench/scripts/setup_node.sh          # submodules, apt deps, JDK 8, then update_jni.sh
bash bench/scripts/setup_corfu.sh      # clone + mvn install CorfuDB (separate node)
```

## Build

```bash
mkdir -p build && cd build
cmake ..                          && make -j$(nproc)          # local FS + Azure + S3
cmake -DOZONEDB_ENABLE_CORFU=ON .. && make -j$(nproc)         # adds JNI + CorfuDB backend
```

- `OZONEDB_ENABLE_S3` defaults **ON** (needs `aws-sdk-cpp` from vcpkg); `OZONEDB_ENABLE_CORFU`
  defaults **OFF**. Both set a `-D` compile definition that guards the corresponding
  `src/db/*_storage.cpp` and its header — new backend code must be inside those guards.
- `-DOZONEDB_PROFILING=ON` keeps `-O3` but adds frame pointers + line tables for perf/flamegraph.
- Enabling Corfu makes CMake shell out to Maven to build `corfu-bridge-all.jar` (the Java shim the
  embedded JVM loads), so the C++ build depends on a working `mvn` and JDK.
- Protobuf headers are generated into `src/include/ozonedb/protobuf/` (gitignored) at configure time
  from `record.proto` / `sstable.proto`.

**After any C++ change that a YCSB run must see, rebuild the whole chain:**

```bash
bash bench/scripts/update_jni.sh
```

That builds `OzoneDB` + `corfu-bridge`, copies `libOzoneDB.so` into `ozonedb-jni-maven/native/`,
runs `mvn package`, installs both `.so`s into `/usr/lib`, and installs the JNI jar into `~/.m2`.
Skipping it means YCSB keeps running the previously installed library.

## Tests

```bash
cd build && ./runUnitTests                               # all
./runUnitTests --gtest_filter=DBTest.single_put_get      # one test
./runUnitTests --gtest_filter='StorageTest.*'            # one suite
```

- **Run from `build/`** — tests reference configs by relative path (`../src/config/...`).
- `/tank` must exist and be writable: `sudo mkdir -p /tank && sudo chmod 777 /tank`.
- `CorfuStorageTest.*` self-skip unless both `CORFU_TEST_ENDPOINT` and `CORFU_BRIDGE_JAR` are set.
- `CloudStorageTest.*` talk to real Azure Blob Storage and fail without network/credentials.
- `tests/test_log.cpp` and `tests/test_compaction.cpp` are entirely commented out; `DBTest`
  references `src/config/cloud/shared_config_rocksdb_base.json`, which is **not** in the tree — the
  DB suite does not currently pass out of the box.
- Corfu end-to-end smoke binaries (only built with `OZONEDB_ENABLE_CORFU=ON`) are the practical way
  to exercise the shared-log path:
  ```bash
  ./corfu_smoke <shared-config.json> [num_keys]
  ./corfu_multiwriter_smoke <shared-config.json> [keys_per_writer]
  ```

## Benchmarks

Everything is driven by `bench/scripts/config/ycsb.yaml` (record counts, workloads, thread/writer
counts, Corfu endpoint, S3/MinIO settings, CloudLab hosts).

```bash
python3 bench/scripts/local/load_local_ycsb_multiproc.py   # load phase, N parallel writer procs
python3 bench/scripts/local/run_local_ycsb_multiproc.py    # run phase
bash    bench/scripts/local/run_ycsb_with_corfu.sh --workloads "a c" --writers-list "1 2 4"
python3 bench/scripts/local/run_multinode_ycsb.py          # fan out across CloudLab hosts via scp/ssh
python3 bench/scripts/local/run_corfu_compaction_contention.py
```

- `run_ycsb_with_corfu.sh` restarts the remote Corfu server between every (trial, workload, writers)
  iteration and tags all results with one `OZONEDB_RUN_TAG`.
- Each writer process gets its **own generated config**: `_make_local_config_per_writer` /
  `_make_corfu_config_per_writer` template `src/config/{local,corfu}/shared_config_base.json` into
  `shared_config_w{i}.json`. Never point multiple writers at a single `shared_config.json`.
- Results go to `bench/results/local` (gitignored); plotting scripts live in `bench/scripts/plot/`.

## Architecture

### Storage abstraction

`Storage` (`src/include/ozonedb/storage.h`) is the seam: named "files" supporting
`append`/`appendInBatch`/`read`/`size`/`seal`/`remove`. Implementations: `FileStorage` (local FS),
`AzureBlobStorage`, `S3Storage` (also MinIO/R2/Wasabi), `CorfuDBStorage` (shared log).

A DB holds **two** `Storage*`: `log_storage` (needs atomic append — the shared log) and
`sstable_storage` (SSTables are immutable and fit a plain object store better; paper §3.5). Set via
the `sstable_backend` config key. When unset the two pointers **alias**, and `DB::~DB` guards the
double-free. Anything touching either pointer must respect that aliasing.

### The three logs (all on `log_storage`)

| Log | Handler | Contents |
|---|---|---|
| data log (`datalog*`) | `LogHandler` | `Record` {key, value, type} — the memtable equivalent; sealed at `log_file_size_limit` |
| metadata log (`metadata.log`) | `MetadataLogHandler` | `OperationRecord` {LOGCREATE, COMPACT} |
| task log (`task.log`) | `TaskLogHandler` | `TaskRecord` — distributed compaction claims |

Every process rolls the metadata log forward into a **`View`**: storage layout per level, per-file
key ranges, file sizes, current log tail. The View is the shared source of truth about which files
exist where; it is published as an immutable `shared_ptr<View const>` snapshot and read lock-free
(readers must not deep-copy it — that was a hot-path regression that has already been removed once).

The task log is how concurrent writers avoid duplicating compaction: a writer claims a task by
appending a `TaskRecord` (owner fingerprint + `owner_generation`), heartbeats it, and dead owners'
tasks get reclaimed.

### Read / write paths

`DB::put` appends a `Record` to the data log. Under `compaction_policy = kHoAl` a background
`CompactionWatcher` thread picks up compaction; under `kHoSe`, puts probabilistically pick up a
compaction task themselves and dispatch it to the thread pool.

`DB::get`:
1. optional `LogKeyIndex` fast path (only when `trust_background_tail` is set) — skips the View load
   and the storage fence entirely;
2. atomic View snapshot, handed down to the child handlers as a raw pointer that aliases into it;
3. `LogHandler::readRecord` across log files;
4. `SSTableHandler::readRecordFromAllLevel`.

The returned `value` **aliases bytes inside the `guard` `shared_ptr<Record>`** — callers must keep
the guard alive while dereferencing.

### Caching

`LRUCache` (`cache.h`) holds parsed log records *and* SSTable blocks/tables, bounded by
`lru_cache_bytes`. Records are `shared_ptr`-owned so eviction/compaction can't free bytes under a
concurrent reader; cold block and table loads are single-flighted. `TailCache` is legacy — its write
side is commented out in `db.cpp`, so lookups always miss.

### CorfuDB backend

All files are packed into one Corfu stream; each entry is a `CorfuEntry` protobuf
{`file_name`, `op` (APPEND/SEAL/REMOVE), `payload`, `client_id`}. A background tailer reconstructs
per-file buffers; `client_id` lets it skip entries this process already self-applied. Reads fence on
the writer's last-known address for read-my-writes. **The tailer never invokes the remote-append
listener synchronously** — a dedicated dispatch thread does, because the listener takes the
`LRUCache` mutex that a foreground thread may hold while fencing on the tailer. Preserve that
separation when touching `corfu_storage.cpp`.

### Config

One flat JSON file per DB instance, parsed by `Metadata` (`metadata.h`) via `parseJSON`, which
returns `map<string,string>` — so *every* value is read as a string and arrays are literal strings
like `"[268435456, 2684354560]"`. Adding a config key means adding the parse in `Metadata`'s
constructor. Base configs live in `src/config/{local,cloud,corfu}/`.

### JNI / YCSB chain

`libOzoneDB.so` → `ozonedb-jni-maven/native/src/main/cpp/jni_OzoneDBJNI.cpp` →
`jni/src/main/java/jni/OzoneDBJNI.java` → `ycsb/ozonedb/.../OzoneDBClient.java`. YCSB selects the
config with `-p shared_config=<path>`. Changing the JNI signature means touching all three layers
plus `update_jni.sh`'s install steps.

## Conventions

- `.clang-format`: Google style, `ColumnLimit: 0` (no wrapping), `int& foo` pointer/ref alignment,
  **east const** (`std::string const&`, not `const std::string&`).
- Everything lives in `namespace ozonedb`; the public API returns `Status`
  (`kSuccess`/`kFailure`/`kSealed`/`kNotFound`) rather than throwing. `Storage` base methods throw
  `std::runtime_error` for unimplemented operations — new backends override what they support.
- Non-obvious invariants (lifetime, lock ordering, aliasing) are documented in header comments; keep
  that density when changing those areas.
