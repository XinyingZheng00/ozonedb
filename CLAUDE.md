# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

OzoneDB is a research LSM-tree key-value store whose entire durable state lives behind an
append-only "shared log" storage abstraction. Multiple independent writer *processes* (possibly on
different machines) coordinate through the log itself — there is no server. It is built as a shared
library (`libOzoneDB.so`), driven through a JNI binding from a YCSB benchmark client.

## Environment

`OZONEDB_HOME` must be exported and point at the repo root — every bench script reads it, and
`jni.OzoneDBJNI` uses it to locate `libozonedb.so`. CMake no longer needs it. The build targets
Ubuntu bench nodes (apt packages, a writable `/tank`) — it is not set up to build on macOS.

Setup is one idempotent, role-based script. Re-running never duplicates shell config or
reinstalls what is present:

```bash
bash bench/scripts/setup.sh --role client         # apt deps, JDK, python, /tank,
                                                  # CorfuDB runtime into ~/.m2, then build.sh
bash bench/scripts/setup.sh --role corfu-server   # CorfuDB + /mnt/corfu/{load,run_batch}
bash bench/scripts/setup.sh --role minio          # minio + bucket (sstable_backend defaults to s3)
bash bench/scripts/setup_zfs.sh --device /dev/sdXN
```

- It writes `~/.ozonedb.env` and wires a marker-guarded include into `~/.profile` **and**
  `~/.bashrc`, rewriting the block rather than appending. Sourcing the script is no longer needed.
  Note `~/.bashrc` alone is not enough: Ubuntu's default returns early for non-interactive shells,
  which is why `run_multinode_ycsb.py` uses `bash -lc`.
- **JDK policy.** Three constraints conflict: YCSB compiles with `<source>1.8</source>`
  (`ycsb/pom.xml`), `corfu-bridge` targets Java 11 so anything running it needs JDK ≥ 11, and
  CorfuDB itself wants a much newer JDK. Hence `--jdk` (default 17, the node's persistent
  `JAVA_HOME`) and `--corfu-jdk` (default 25, used only to build CorfuDB).
- **`corfu-bridge` needs CorfuDB in the local `~/.m2`.** Its pom depends on
  `org.corfudb:runtime:0.9.1.0-SNAPSHOT` and declares no `<repositories>`, so the artifact resolves
  only from a local `mvn install`. That is why the *client* role installs CorfuDB too, not just the
  corfu-server role. It is skipped when the artifact is already present.
- `setup_node.sh` and `setup_corfu.sh` are thin wrappers kept for compatibility.
- Python dependencies are declared in `bench/scripts/requirements.txt`.

## Build

```bash
cmake -B build                           && cmake --build build -j$(nproc)  # local FS + Azure + S3
cmake -B build -DOZONEDB_ENABLE_CORFU=ON && cmake --build build -j$(nproc)  # adds JNI + CorfuDB
```

CMake no longer reads `OZONEDB_HOME` — only the bench scripts do. The vcpkg toolchain resolves
from an explicit `CMAKE_TOOLCHAIN_FILE`, else `$VCPKG_ROOT`, else the in-repo `vcpkg/` tree.
**`vcpkg` is vendored, not a submodule**: `git ls-tree HEAD vcpkg` is a plain `040000 tree` of
~11.7k committed files and `git submodule status` is empty, so `.gitmodules` is vestigial and
`git submodule update` fetches nothing. A clone *or* an rsync of the working tree is sufficient.

`CMakePresets.json` defines `default` / `corfu` / `profiling`, but presets need CMake ≥ 3.21 and
the Ubuntu 20.04 bench image ships 3.16 — hence the plain commands above as the primary path.
`CMakeUserPresets.json` is per-user and gitignored.

- `OZONEDB_ENABLE_S3` defaults **ON** (needs `aws-sdk-cpp` from vcpkg); `OZONEDB_ENABLE_CORFU`
  defaults **OFF**. Both set a `-D` compile definition that guards the corresponding
  `src/db/*_storage.cpp` and its header — new backend code must be inside those guards.
- `-DOZONEDB_PROFILING=ON` keeps `-O3` but adds frame pointers + line tables for perf/flamegraph.
- Enabling Corfu makes CMake shell out to Maven to build `corfu-bridge-1.0-all.jar` (the Java shim the
  embedded JVM loads), so the C++ build depends on a working `mvn` and JDK. That filename is what
  maven-shade actually emits (`artifactId-version-classifier`); it ignores the pom's `<finalName>`.
  If `CORFU_BRIDGE_JAR` ever stops matching, Maven silently re-runs on **every** build.
- Protobuf headers are generated into `build/generated/protobuf/` at configure time from
  `record.proto` / `sstable.proto`. Sources include them as `"protobuf/record.pb.h"`, and
  `build/generated` is deliberately **first** on the include path so a stale in-tree copy from a
  pre-move build can't shadow them (configure also deletes that directory if it finds it).
- Header/link deps come from `target_include_directories(OzoneDB PUBLIC …)`, so anything linking
  `OzoneDB` inherits them. Don't reintroduce global `include_directories()` or `link_directories()`.
- `vcpkg.json` carries only what is actually linked. `sqlite3`, `fmt`, `log4cxx` (+ `apr`,
  `apr-util`, `expat`) and `abseil` were removed — nothing under `src/` or `tests/` referenced them.

**After any C++ change that a YCSB run must see, rebuild the whole chain:**

```bash
bash bench/scripts/build.sh
```

That builds `OzoneDB`, `ozonedb_jni` and `corfu-bridge`, then the YCSB `ozonedb-binding`. Nothing
is installed outside the repo — no `sudo`, no `/usr/lib`, no `~/.m2` — because `libozonedb.so` has
an `$ORIGIN` rpath to reach `libOzoneDB.so` beside it in `build/`. Skipping it means YCSB keeps
running the previously built library.

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
- `tests/test_log.cpp` and `tests/test_compaction.cpp` are entirely commented out and are no longer
  in `OZONEDB_TEST_SOURCES` — re-add them there if they ever come back. `DBTest` references
  `src/config/cloud/shared_config_rocksdb_base.json`, which is **not** in the tree, so the DB suite
  does not currently pass out of the box.
- Corfu end-to-end smoke binaries (only built with `OZONEDB_ENABLE_CORFU=ON`) are the practical way
  to exercise the shared-log path:
  ```bash
  ./corfu_smoke <shared-config.json> [num_keys]
  ./corfu_multiwriter_smoke <shared-config.json> [keys_per_writer]
  ```

## Benchmarks

Everything is driven by `bench/scripts/config/ycsb.yaml`, loaded through
`bench/scripts/ycsb_config.py` — never `yaml.safe_load` directly. The `nodes:` block names each
machine once with **two non-interchangeable addresses**: `ssh` (public, how the orchestrator
reaches it) and `lan` (internal 10.10.1.x, how other nodes reach it). `corfu_server -a` binds the
*lan* one, so clients can only connect if that is what `corfu.endpoint` resolves to. The loader
derives `corfu.endpoint`, `s3.endpoint` and `cloudlab.{hosts,ssh_user,ssh_private_key_path}` from
`nodes:` and materialises them under the keys consumers already read, and **rejects** any of those
set by hand. `python3 bench/scripts/ycsb_config.py --check` prints the resolved plan; the shell
wrappers read the corfu node through its `--node`/`--get` CLI rather than a hardcoded constant.

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
- `bench/ansible/` fans out to every `cloudlab.hosts` entry in parallel. `bootstrap.yml` pushes the
  full tree (vcpkg included, minus the Mach-O `vcpkg/vcpkg` and `vcpkg/downloads/`) and then runs
  `setup.sh` — no `git clone` and no GitHub credentials on the node, because vcpkg is vendored.
  `sync.yml` is the incremental follow-up (442 files / 2.2 MB vs 14.9k / 19 MB) and excludes `.git/`
  and `vcpkg/`, so it **cannot** bootstrap a node; `setup.sh` hard-fails early if `vcpkg/ports` is
  absent, which is what that mistake looks like. Exclude lists are shared in
  `bench/ansible/group_vars/all.yml`; `inventory.py` is a dynamic inventory reading
  `cloudlab.{hosts,ssh_user,ssh_private_key_path}` straight out of `ycsb.yaml`, so the host list is
  never duplicated. The result pull stays on `scp` inside `run_multinode_ycsb.py`.
- **The Azure benchmark path is gone.** `bench/scripts/cloud/` (Azure VM/CosmosDB provisioning,
  `azureozonedb`/`cosmosdb` load+run drivers), the `cloud`/`azure`/`storage`/`cosmosdb`/
  `resource_group`/`network`/`vm`/`id` config blocks and the `postgres`/`azuresql` YCSB properties
  were all deleted — those experiments are retired. `ycsb.yaml` is now only the CloudLab shared-log
  workflow. The **C++ `AzureBlobStorage` backend is deliberately still here**
  (`src/db/azure_blob_storage.cpp`, `CloudStorageTest.*`, the two `azure-*-cpp` vcpkg ports, which
  are `find_package(... REQUIRED)`); it is a `Storage` implementation, not an experiment driver.

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

`linearizable_reads` (mutually exclusive with `trust_background_tail` — `Metadata` throws on both)
makes every get strict, at the cost of exactly **one fence per get**: `DB::get` opens a
`Storage::SyncScope` (one sequencer round-trip + tailer wait — the linearization point), which
records a thread-local fence token that every later storage call in the get reuses instead of
re-fencing; `MetadataLogHandler::syncView()` then rolls the view forward from local state, the
unfenced key-index probe in `readRecord` is bypassed, the log scan runs inline on the caller thread
(the token is thread-local, and post-fence reads are local-memory splices), and a post-scan size
check on `metadata.log` retries the get if a LOGCREATE/COMPACT was applied mid-scan (otherwise a
peer compaction's REMOVE can make a key transiently invisible; retries reuse the original fence).
The end-to-end guarantee assumes the sync write defaults (`commit_interval_ = 0`,
`sync_mode_ = true`).

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

`libOzoneDB.so` → `src/jni/jni_OzoneDBJNI.cpp` (CMake target `ozonedb_jni` → `libozonedb.so`) →
`ycsb/ozonedb/src/main/java/jni/OzoneDBJNI.java` → `ycsb/ozonedb/.../OzoneDBClient.java`. YCSB
selects the config with `-p shared_config=<path>`. Changing the JNI signature means touching all
three layers — and `jni_OzoneDBJNI.h` is javah-generated and checked in, so keep it in sync.

- The Java class must stay in package `jni`: the exported symbols are `Java_jni_OzoneDBJNI_*`, so
  renaming the package breaks the lookup at load time with no compile error.
- `OzoneDBJNI` resolves the shim itself, in order: `-Dozonedb.native.lib`, then
  `$OZONEDB_HOME/build/libozonedb.so`, then `System.loadLibrary`. Resolution lives in the class
  rather than the runners because YCSB is launched several ways — the multiproc runners build
  their own `java` command, while the single-process scripts and manual runs go through `bin/ycsb`,
  which builds its own.
- This used to be four Maven modules (`jni-demo` parent, `demoproc-native`, `demoproc-jni`,
  `corfu-bridge`) plus `mvn install:install-file` and two `sudo cp` into `/usr/lib`. Only
  `corfu-bridge` is left.

## Conventions

- `.clang-format`: Google style, `ColumnLimit: 0` (no wrapping), `int& foo` pointer/ref alignment,
  **east const** (`std::string const&`, not `const std::string&`).
- Everything lives in `namespace ozonedb`; the public API returns `Status`
  (`kSuccess`/`kFailure`/`kSealed`/`kNotFound`) rather than throwing. `Storage` base methods throw
  `std::runtime_error` for unimplemented operations — new backends override what they support.
- Non-obvious invariants (lifetime, lock ordering, aliasing) are documented in header comments; keep
  that density when changing those areas.
