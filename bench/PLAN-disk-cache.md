# Plan: a disk-backed read-through tier for SSTables on the clients' SATA SSDs (branch `worktree-plan-cost`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Status (2026-08-28):** planned, not started. Follow-up to `PLAN-compaction-cache.md`
(the RAM block cache is compaction-aware, `h` 0.40 under workload a at a 52 % ratio) and to
the section "Compaction-aware block cache" of `RESULTS-cost.md`. The cost model says a
local disk tier is the largest remaining lever: at 10 TB, 2 TB of gp3 per client or an
`i4i.4xlarge` cuts the bill by about $1,100 per month (19 %), at 100 TB by about $600.
Those numbers assume a disk hit costs nothing and extrapolate the read-only `h` curve.
This plan builds the tier, measures it on the cluster, and feeds the model with measured
coefficients. Every CloudLab node has two Micron `MTFDDAK480TDN` 447 GiB SATA SSDs:
`sda` holds the OS (64 GB root, 8 GB swap, 375 GB unpartitioned) and `sdb` is unused.
`/tank` sits on the 63 GB root filesystem today.

**Goal:** serve SSTable block reads from a local SSD copy of the SSTable when the RAM
block cache misses, so the object store sees one GET per SSTable fill instead of one GET
per block miss.

**Architecture:** a `Storage` decorator (`DiskCacheStorage`) wraps `sstable_storage`. It
keeps whole, immutable SSTable objects as local files under a byte budget with file-level
LRU, fills a file in the background on its first miss with chunked ranged reads, takes
the builder's write-through for free, and drops a file on the same COMPACT hook that
the RAM cache uses. Checkpoint objects (`LATEST`, manifests) pass through untouched.

**Tech stack:** C++17 (`<filesystem>`, POSIX `pread`, `posix_fadvise`), gtest, the
existing Python runners, bash + ansible for provisioning, `plot_cost_model.py`.

**Spec:** this document (sections "Goal", "What the code does today", "Build or borrow"
and "Design" are the spec; the tasks below implement it).

## Global constraints

- East const, Google style, `ColumnLimit: 0`, everything in `namespace ozonedb`, public
  API returns `Status`, never throws (`CLAUDE.md`, "Conventions").
- The build targets Ubuntu 20.04 with CMake 3.16 and GCC 9. No new vcpkg port, no new
  system package on the clients beyond `e2fsprogs` (already installed).
- The user builds remotely. Never run `cmake`/`make` on the macOS checkout. The chain is
  `ansible-playbook bench/ansible/sync.yml -e build=true`, then
  `cmake --build build --target runUnitTests` on one node (`build.sh` does not build the
  tests).
- Unit tests run from `build/` and need a writable `/tank`. New tests go in
  `OZONEDB_TEST_SOURCES` (`CMakeLists.txt:217-226`).
- Lock rule from `cache.h:383-392`: nothing that takes the cache mutex runs under
  `view_mutex`. The tier's mutex follows the same rule.
- The cluster is shared. Check for another session's drivers before a sync, a load or a
  Corfu restart. Kill only your own pids.
- Config values are strings (`parseJSON`). Every new key is parsed in `Metadata`'s
  constructor with the idioms at `metadata.h:252-271`.
- Do not touch the user's untracked `PLAN-range-read.md` in the worktree root.
- Runner flags select an experiment, never an edit of `ycsb.yaml`. A flag reaches the
  clients through `run_multinode_ycsb_with_corfu.sh` → `run_multinode_ycsb.py` →
  `run_local_ycsb_multiproc.py` → the generated `shared_config_w{i}.json`.

---

## Goal in numbers

Dataset 1 GB (1 M records), 8 writers on 8 clients, 600 s cells, trimming on, MinIO on
the log node, 4 KiB blocks, compaction range reads. The RAM block cache is set to 8 MB
in the sweep so that the disk tier is the measured quantity.

| Cell | Today (no tier) | Target |
|---|---|---|
| Workload c, RAM 8 MB, disk 2 GB (whole dataset) | `h` 0.215 at 8 MB (0.8 % ratio), 0.679 at 512 MB (52 %) | `h_total` ≥ 0.95 in the last 60 s, GETs per op ≤ 0.05, throughput within 10 % of the RAM 512 MB cell (14,331 ops/s) |
| Workload c, RAM 8 MB, disk 128 / 256 / 512 MB | — | a measured `h_total(disk ratio)` curve with four points |
| Workload a, RAM 8 MB, disk 2 GB | 2,600 ops/s, 0.657 GETs per op | GETs per op ≤ 0.05, throughput ≥ 2,600 ops/s |
| Workload a, RAM 512 MB, disk 2 GB | 2,894 ops/s, `h` 0.40, 0.397 GETs per op | GETs per op ≤ 0.05 |
| Client CPU per op with a disk hit | 1.14 ms (RAM hit path) | ≤ 1.3 ms |
| NOT_FOUND reads | 0 | 0 |
| Fill traffic | — | fill GETs per op ≤ 0.001 in the last 60 s of every c cell |

Then the projection at 10 TB and 100 TB with a `disk_gb` term fed by measured
coefficients (disk `h` curve, CPU per op with the tier, fill GETs per op).

## What the code does today

`DB::DB` (`src/db/db.cpp:62-144`) builds `sstable_storage` first (`:73-77`, an
`S3Storage` when `sstable_backend = s3`), then `log_storage` (`:78-80`); without
`sstable_backend` the two pointers alias (`:81-84`) and `~DB` (`:146-167`) deletes
`sstable_storage` only when `!= log_storage` (`:160-163`). The `LogTrimmer` gets
`db->sstable_storage` (`db.cpp:202`) for the checkpoints.

Every SSTable read is a ranged `Storage::read(fileName, data, offset, length)`
(`storage.h:90`) against `sstable_storage`, by four callers:

1. `Table::open` (`src/db/sstable/table_reader.cpp:37-80`): `size()` (`:41`, a HEAD on
   S3), the 50-byte footer (`:46`), the index block (`:58`), the metaindex and filter
   blocks (`:88`, `:114`, `:124`) — three to four round trips per cold file.
2. `Table::blockReader` (`:145-161`): one `readBlock` (`block_handler.cpp:96`) per data
   block miss. This is the hot path (`LRUCache::readDataBlocks`, `cache.cpp:357`).
3. `Table::scanBlocks` (`:222-297`): chunks of up to `compaction_read_bytes` (64 MiB) for
   compaction inputs (`getAll`) and the RAM warm (`warm`).
4. `checkpoint::getObject` (`src/db/checkpoint.cpp:41-53`): full reads of
   `checkpoint/LATEST` (mutable, rewritten at every checkpoint), `checkpoint/<C>/manifest`
   and `checkpoint/<C>/files/<name>`.

`S3Storage::read` (`src/db/s3_storage.cpp:214-228`) is one `GetObject` with a `Range`
header and returns an owning `new unsigned char[length]`. `S3Storage::size`
(`:230-249`) is a HEAD. There are no request counters in the process; `s3_get` in the
bench TSV comes from MinIO's Prometheus counters through `server_sampler.sh`.

Writes: `TableBuilder` appends every block with `appendNoFlush` (`table_builder.cpp:177`,
`:252`) and `flush` (`:264`) is the single PUT. The bytes of every new SSTable pass
through the process that builds it. Names are
`sstable<L>/<version>_<fingerprint><nanoseconds>.sst` (`compaction.cpp:412-421`): unique
forever, never reused.

Removal: the compacting process calls `sstable_storage->remove(input)`
(`compaction.cpp:513`). Every other process learns the removal when it applies the
COMPACT record: `MetadataLogHandler::drainCacheEvents`
(`src/db/metadata_log_handler.cpp:343-367`) calls `lru_cache->invalidateSSTable(input)`
(`:359`) outside `view_mutex`.

Liveness: `View` (`metadata_log_handler.h:18-24`) holds `key_range` (file → key range)
and `file_size` (file → bytes); `db.cpp:110-116` shows the liveness lambda the RAM warm
worker uses (`latestViewSnapshot()` + `key_range.find`).

Close: `DB::closeDB` (`db.cpp:210-229`) stops the warm worker, prints
`[lru_cache] …` (two lines, `cache.cpp:795-833`, stderr), then `delete db`. The
extractor parses those lines with `CACHE_RE` (`extract_cost_coefficients.py:64-66`) and
`LEVELS_RE` (`:70-79`), and adds the columns at `COLUMNS` (`:622-645`).

## Build or borrow

The question "is there a C++ caching library for this" has four real answers. The plan
builds the tier by hand, for the reasons in the last row.

| Option | What it gives | Why not here |
|---|---|---|
| **Meta CacheLib** (`github.com/facebook/CacheLib`) | The one library built for this shape: a hybrid DRAM + flash cache ("Navy": `BlockCache` for 4 KiB items, `BigHash` for small ones), LRU/2Q/TinyLFU, admission policies, persistence across restarts. Used by Meta's CDN and social-graph caches. | Not in vcpkg. It builds through its own `contrib/build.sh` and pulls folly, fbthrift, fizz, wangle, glog, gflags, double-conversion and boost, about an hour per node and a GCC ≥ 10 toolchain — the bench image has GCC 9 and CMake 3.16. Its keys are items, not files. A file removal must delete every block key of the file, which needs a per-file key list in RAM (about 128 KB per 64 MiB file, 4 GB at 30 k files). The right choice for a product; too heavy for a measurement whose subject is the request count. |
| **RocksDB `PersistentCache`** (`utilities/persistent_cache`, `NewPersistentCache(env, path, size, logger, optimized_for_nvm)`) and the newer `SecondaryCache` | A block-level SSD cache with an LRU index, `Insert(key, data, size)` / `Lookup(key, data, size)`. RocksDB is in vcpkg. | Marked experimental for years, few users, and the dependency is all of RocksDB for one utility. Same per-file invalidation problem as CacheLib. |
| **A caching HTTP proxy in front of the S3 endpoint**: nginx `proxy_cache` + `slice` (`proxy_cache_key $uri$slice_range`, `proxy_cache_path /tank/cache max_size=400g`, `proxy_cache_valid 200 206 1y`, `proxy_set_header Host $http_host` so the SigV4 signature still verifies, `proxy_cache_bypass` for `/…/checkpoint/`), or Apache Traffic Server | No C++ at all. Point `s3_endpoint` at `127.0.0.1:8080` on every client. The MinIO counters still give the GETs saved. Half a day to a first number. | Slice granularity (1 MB) is not block granularity, so every miss costs a 1 MB GET and the `h` curve is a different curve. The proxy's CPU is outside the client sampler (`/usr/bin/time` wraps the JVM only). It cannot drop a removed file, so dead objects hold budget until `inactive=` purges them. Kept as an optional cross-check (Task 12), not as the engine's tier. |
| **Header-only LRU containers** (`lru-cache`, vpetrigo `caches`, `boost::compute::detail::lru_cache`), **libcuckoo**, **Alluxio / JuiceFS / mountpoint-s3 with a local block cache** | In-memory LRU maps, or a FUSE filesystem with its own disk cache under `FileStorage`. | The in-memory maps solve the part `LRUCache` already has (`std::list` + `unordered_map`). The FUSE mounts change the request pattern (JuiceFS reads 4 MB chunks) and hide the requests the experiment counts. |
| **Hand-rolled `DiskCacheStorage`** (this plan) | About 500 lines: one local file per SSTable, a directory scan at open instead of a persisted index, POSIX `pread` + `posix_fadvise`, the same fill-worker shape as `LRUCache`'s warm worker. Whole-file entries make invalidation one `unlink`, and the names are unique forever. | The cost is fill bandwidth, not requests: a 64 MiB file is one chunked GET. A hot 4 KiB block in a cold file waits for the fill; the read itself is served from S3 meanwhile, so no latency cliff. |

If the product later needs block granularity plus DRAM/flash in one library, CacheLib is
the port to make. The measurement does not.

## Design

**Placement.** `DiskCacheStorage : public Storage` owns the backing store
(`std::unique_ptr<Storage>`). `DB::DB` wraps `sstable_storage` when `disk_cache_dir` is
set; `db->sstable_storage` becomes the wrapper, so `SSTableHandler`, `LRUCache`,
`CompactionWatcher` and `LogTrimmer` see one pointer as before and `~DB` deletes it once
(the wrapper deletes the backing). The `!= log_storage` guards stay meaningful because
the wrapper is only created when `sstable_backend_set`.

**What is cached.** Only names with the prefix `sstable_level_prefix` (`"sstable"`).
Everything else (`checkpoint/LATEST`, manifests, checkpoint file copies) passes through
and is counted as `passthrough`. SSTables are write-once and their names are never
reused, so a local copy is valid until the file leaves the View.

**Granularity.** Whole files. A local entry is either complete (`<dir>/<name>`) or in
progress (`<dir>/<name>.part`). Reads never touch a `.part`.

**Population, three paths.**
1. Write-through: `appendNoFlush`/`append`/`appendInBatch` on a cacheable name append the
   bytes to `<name>.part` after the backing accepted them; `flush` publishes the part
   (rename into place, admit under the budget) after the backing PUT succeeded, or
   discards it after a failure.
2. Fill on miss: a ranged read that misses is served from the backing directly, and the
   name is queued once (`queued_` set, queue bound `max_queue`, oldest dropped). One
   worker thread fills a file with `chunk_bytes` ranged reads (64 MiB, the compaction
   read size) into `.part`, then publishes. A file larger than the capacity is skipped
   (`fill_skipped_budget`). A file the backing no longer has (`size() == 0`) is skipped
   (`fill_gone`). A read failure mid-fill discards the part (`fill_failed`).
3. Reconciliation at open: the directory is scanned, `.part` files and files not in the
   View (or with a size that differs from `View::file_size`) are deleted, the rest are
   admitted oldest-first. The bench wipes the directory before every cell, so at bench
   scale this path only proves persistence; in production it is what makes a restart
   cheap.

**Budget.** `capacity_bytes` over complete files. LRU by file: a hit moves the file to the
front, `admit` evicts from the back until the new file fits (`unlink`; a concurrent
reader holds its own fd, so an unlink under it is safe on POSIX). `capacity_bytes = 0`
disables the tier (everything passes through).

**Invalidation, two paths.** The compacting process calls `remove(input)` → the wrapper
forwards and unlinks the local copy. Every process, that one included, applies the
COMPACT record → `drainCacheEvents` calls the new
`MetadataLogHandler::sstable_removed_listener_` after `invalidateSSTable`, which the DB
wires to `tier->invalidate(input)`. `invalidate` on an absent name is a no-op.

**Page cache.** Every tier read is `open` + `pread` + `posix_fadvise(fd, off, len,
POSIX_FADV_DONTNEED)` + `close` when `drop_pages` is true (the default). Without it the
125 GB of RAM on the nodes turns every disk hit into a page-cache hit and the campaign
would measure RAM, not the SSD. `--disk-cache-keep-pages` is the A/B.

**Lock order.** The tier has two mutexes: `mtx_` (index, LRU, budget, counters that are
not atomics) and `fill_mtx_` (queue, worker state). Neither is held while calling the
backing store or while doing file I/O, except `unlink` in eviction. Nothing in the tier
takes a cache or view mutex, and the DB calls the tier only outside `view_mutex`.

**Stats.** One stderr line at close, printed by `DB::closeDB` after `[lru_cache]`:

```
[disk_cache] hits=N misses=N hit_bytes=N miss_bytes=N passthrough=N fills=N fill_bytes=N fill_gets=N fill_skipped_budget=N fill_skipped_present=N fill_gone=N fill_failed=N fill_dropped=N writethrough_files=N evictions=N evicted_bytes=N invalidated=N files=N bytes=N capacity=N
```

**Config keys** (`Metadata`): `disk_cache_dir` (string, empty = off; a trailing `/` is
added), `disk_cache_bytes` (uint64, required when the dir is set), `disk_cache_chunk_bytes`
(uint64, default 67108864), `disk_cache_drop_pages` (bool, default true),
`disk_cache_fill_queue` (uint64, default 256). `disk_cache_dir` without
`sstable_backend` throws.

**Failure modes.** A local read failure (short `pread`, `EIO`) drops the entry and falls
back to the backing. A write-through failure poisons the part, which `flush` discards. A
full disk (`ENOSPC`) while filling discards the part and counts `fill_failed`; the
budget must stay below the filesystem size (the runner sets 2 GB on a 447 GB disk).

## File structure

- Create `src/include/ozonedb/disk_cache_storage.h` — the class, `Options`, `Stats`,
  the invariants above as header comments.
- Create `src/db/disk_cache_storage.cpp` — pass-through, write-through, local reads,
  fill worker, budget, reconciliation, stats.
- Create `tests/test_disk_cache_storage.cpp` — `DiskCacheStorageTest.*` on a
  `FileStorage` backing (plus a counting subclass), and `Metadata` parsing.
- Modify `CMakeLists.txt:102-127` (`OZONEDB_SOURCES`) and `:217-226`
  (`OZONEDB_TEST_SOURCES`).
- Modify `src/include/ozonedb/metadata.h` (fields after `:109`, parsing after `:271`).
- Modify `src/include/ozonedb/metadata_log_handler.h` / `src/db/metadata_log_handler.cpp`
  (the removed-file listener).
- Modify `src/include/ozonedb/db.h` (`DiskCacheStorage* disk_cache`), `src/db/db.cpp`
  (wrap, wire, reconcile, start/stop, print).
- Create `bench/scripts/setup_disk_cache.sh`, `bench/ansible/disk_cache.yml`.
- Modify `bench/scripts/local/load_local_ycsb_multiproc.py`,
  `run_local_ycsb_multiproc.py`, `run_multinode_ycsb.py`,
  `run_multinode_ycsb_with_corfu.sh` (flags, label token, per-writer config, wipe,
  mount guard).
- Modify `bench/scripts/extract_cost_coefficients.py` (`DISK_RE`, columns).
- Modify `bench/scripts/plot/plot_cost_model.py`, `bench/scripts/plot/prices.json`
  (`disk_gb` term, third OzoneDB line).
- Modify `bench/RESULTS-cost.md`, this file's status paragraph, `CLAUDE.md` (one
  paragraph under "Caching").

---

### Task 1: `DiskCacheStorage` skeleton — pass-through and the name filter

**Files:**
- Create: `src/include/ozonedb/disk_cache_storage.h`
- Create: `src/db/disk_cache_storage.cpp`
- Create: `tests/test_disk_cache_storage.cpp`
- Modify: `CMakeLists.txt:111` (add the source), `CMakeLists.txt:226` (add the test)

**Interfaces:**
- Produces: `class DiskCacheStorage : public Storage` with
  `DiskCacheStorage(std::unique_ptr<Storage> backing, Options options)`,
  `struct Options {std::string dir; uint64_t capacity_bytes = 0; std::string prefix = "sstable"; size_t chunk_bytes = 64u << 20; bool drop_pages = true; size_t max_queue = 256;}`,
  `struct Stats {…}` (every counter in the stats line), `Stats stats()`,
  `bool cacheable(std::string const& name) const`, `Storage* backing()`.
  Later tasks add methods to the same class.

- [ ] **Step 1: Write the failing tests**

```cpp
// tests/test_disk_cache_storage.cpp
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "disk_cache_storage.h"
#include "metadata.h"
#include "storage.h"

using namespace ozonedb;

namespace {

std::string const kRoot = "/tank/test/disk_cache/";

std::string stamp() {
  auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
  return std::to_string(getpid()) + "_" + std::to_string(ns);
}

// A FileStorage that counts what the tier forwards to it.
class CountingStorage : public FileStorage {
 public:
  explicit CountingStorage(std::string const& path) : FileStorage(path) {}
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override {
    ++ranged_reads;
    return FileStorage::read(fileName, data, a, length);
  }
  Status read(std::string const& fileName, unsigned char*& data, size_t& size) override {
    ++full_reads;
    return FileStorage::read(fileName, data, size);
  }
  size_t size(std::string fileName) override {
    ++sizes;
    return FileStorage::size(fileName);
  }
  int ranged_reads = 0;
  int full_reads = 0;
  int sizes = 0;
};

struct TierFixture {
  std::string backing_dir;
  std::string tier_dir;
  CountingStorage* backing;  // owned by the tier
  std::unique_ptr<DiskCacheStorage> tier;

  explicit TierFixture(uint64_t capacity, size_t chunk = 64u << 20, bool drop_pages = true) {
    std::string const s = stamp();
    backing_dir = kRoot + "backing_" + s + "/";
    tier_dir = kRoot + "tier_" + s + "/";
    std::filesystem::create_directories(backing_dir + "sstable1");
    std::filesystem::create_directories(backing_dir + "checkpoint");
    std::filesystem::create_directories(tier_dir);
    auto owned = std::make_unique<CountingStorage>(backing_dir);
    backing = owned.get();
    DiskCacheStorage::Options o;
    o.dir = tier_dir;
    o.capacity_bytes = capacity;
    o.chunk_bytes = chunk;
    o.drop_pages = drop_pages;
    tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);
  }
  ~TierFixture() {
    tier.reset();
    std::filesystem::remove_all(backing_dir);
    std::filesystem::remove_all(tier_dir);
  }
  // Writes `n` bytes (i % 251) straight into the backing, bypassing the tier.
  std::vector<unsigned char> seed(std::string const& name, size_t n) {
    std::vector<unsigned char> bytes(n);
    for (size_t i = 0; i < n; ++i) bytes[i] = static_cast<unsigned char>(i % 251);
    std::filesystem::create_directories(std::filesystem::path(backing_dir + name).parent_path());
    std::ofstream out(backing_dir + name, std::ios::binary);
    out.write(reinterpret_cast<char const*>(bytes.data()), static_cast<std::streamsize>(n));
    return bytes;
  }
  bool local(std::string const& name) const { return std::filesystem::exists(tier_dir + name); }
  bool part(std::string const& name) const { return std::filesystem::exists(tier_dir + name + ".part"); }
};

}  // namespace

TEST(DiskCacheStorageTest, NamesOutsideThePrefixPassThrough) {
  TierFixture f(1u << 20);
  std::vector<unsigned char> bytes = {'4', '2', '\n'};
  ASSERT_EQ(f.tier->appendNoFlush("checkpoint/LATEST", bytes.data(), 3), Status::kSuccess);
  ASSERT_EQ(f.tier->flush("checkpoint/LATEST"), Status::kSuccess);
  EXPECT_FALSE(f.local("checkpoint/LATEST"));
  EXPECT_FALSE(f.part("checkpoint/LATEST"));
  unsigned char* data = nullptr;
  size_t size = 0;
  ASSERT_EQ(f.tier->read("checkpoint/LATEST", data, size), Status::kSuccess);
  EXPECT_EQ(size, 3u);
  delete[] data;
  EXPECT_EQ(f.backing->full_reads, 1);
  EXPECT_EQ(f.tier->stats().passthrough, 1u);
  EXPECT_TRUE(f.tier->cacheable("sstable1/x.sst"));
  EXPECT_FALSE(f.tier->cacheable("checkpoint/LATEST"));
  EXPECT_FALSE(f.tier->cacheable("datalog1"));
}

TEST(DiskCacheStorageTest, ZeroCapacityCachesNothing) {
  TierFixture f(0);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 1000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 10, 100), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, bytes.data() + 10, 100));
  delete[] data;
  EXPECT_FALSE(f.tier->cacheable(name));
  EXPECT_EQ(f.tier->stats().passthrough, 1u);
  EXPECT_EQ(f.tier->stats().misses, 0u);
}
```

- [ ] **Step 2: Add the sources to CMake and run the tests to verify they fail**

In `CMakeLists.txt`, add `src/db/disk_cache_storage.cpp` after `src/db/storage.cpp`
(line 111) and `tests/test_disk_cache_storage.cpp` after `tests/test_cache.cpp` (line
226). On a build node:

```bash
cmake --build build --target runUnitTests -j"$(nproc)"
```

Expected: compile error, `disk_cache_storage.h` not found.

- [ ] **Step 3: Write the header**

```cpp
// src/include/ozonedb/disk_cache_storage.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "storage.h"

namespace ozonedb {

// A read-through tier for immutable SSTable objects on a local disk, in
// front of the object store that holds them (bench/PLAN-disk-cache.md).
//
// Entries are whole files: "<dir>/<name>" is a complete local copy,
// "<dir>/<name>.part" is one in progress. Only names with `prefix` are
// cached; everything else passes straight through to the backing store
// (checkpoint/LATEST is mutable and must never be cached).
//
// Population: the builder's write-through (append* + flush), a background
// fill on the first ranged-read miss (chunked ranged reads of the backing),
// and reconcile() at open. Eviction: LRU by file under capacity_bytes.
// Invalidation: remove() (this process compacted) and invalidate() (a peer
// compacted; wired to the COMPACT apply in MetadataLogHandler).
//
// Lock order: mtx_ (index, LRU, budget) and fill_mtx_ (queue, worker) are
// never held across a backing-store call or file I/O other than unlink.
// Nothing here takes a cache or view mutex; the DB calls the tier only
// outside view_mutex.
class DiskCacheStorage : public Storage {
 public:
  struct Options {
    std::string dir;                 // local directory, "/" appended if missing
    uint64_t capacity_bytes = 0;     // 0 = cache nothing (pure pass-through)
    std::string prefix = "sstable";  // names starting with this are cached
    size_t chunk_bytes = 64u << 20;  // ranged-read size of the fill worker
    bool drop_pages = true;          // POSIX_FADV_DONTNEED after each tier read
    size_t max_queue = 256;          // fill queue bound; the oldest is dropped
  };

  struct Stats {
    uint64_t hits = 0, misses = 0, hit_bytes = 0, miss_bytes = 0, passthrough = 0;
    uint64_t fills = 0, fill_bytes = 0, fill_gets = 0;
    uint64_t fill_skipped_budget = 0, fill_skipped_present = 0, fill_gone = 0, fill_failed = 0, fill_dropped = 0;
    uint64_t writethrough_files = 0, evictions = 0, evicted_bytes = 0, invalidated = 0;
    uint64_t files = 0, bytes = 0, capacity = 0;
  };

  DiskCacheStorage(std::unique_ptr<Storage> backing, Options options);
  ~DiskCacheStorage() override;

  // Storage. Every method forwards to the backing store; the cacheable ones
  // also maintain the local copy as documented in the .cpp.
  void createDirectory(std::string name) override;
  Status append(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendInBatch(std::string const& fileName, unsigned char* const& data, int length) override;
  Status flush(std::string const& fileName) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t& size) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override;
  size_t size(std::string fileName) override;
  void seal(std::string fileName) override;
  bool isSealed(std::string fileName) override;
  void remove(std::string fileName) override;
  bool exist(std::string fileName) override;
  void setRemoteAppendListener(RemoteAppendListener listener) override;
  long lastAppendAddressForThread() const override;
  void sync() override;
  void clearSync() override;
  bool hasSyncToken() const override;

  // Tier API.
  bool cacheable(std::string const& name) const;
  // Drops the local copy (complete or in progress); the object stays.
  void invalidate(std::string const& name);
  // Scans dir: deletes .part files and complete files for which live(name,
  // bytes) is false, admits the rest oldest first. Returns the number deleted.
  size_t reconcile(std::function<bool(std::string const&, size_t)> const& live);
  void startFillWorker();
  void stopFillWorker();
  void waitFillIdle();
  Stats stats();
  void printStats();
  Storage* backing() { return backing_.get(); }
  Options const& options() const { return options_; }

 private:
  struct Entry {
    size_t bytes = 0;
    std::list<std::string>::iterator lru;
  };

  std::string localPath(std::string const& name) const { return options_.dir + name; }
  std::string partPath(std::string const& name) const { return options_.dir + name + ".part"; }
  // Returns true and moves the entry to the LRU front when a complete copy exists.
  bool touch(std::string const& name);
  bool present(std::string const& name);
  // pread of [a, a+length) from the local copy into a new buffer; false on any short read.
  bool readLocal(std::string const& name, unsigned char*& data, size_t a, size_t length);
  // Appends to the .part stream (opened on first use). Failures poison the part.
  void writePart(std::string const& name, unsigned char const* data, size_t length);
  // Closes the .part, renames it into place and admits it; discards it on any failure.
  bool publishPart(std::string const& name);
  void discardPart(std::string const& name);
  // Under mtx_: evicts until `bytes` fits, then inserts. False when bytes > capacity.
  bool admit(std::string const& name, size_t bytes);
  void evictToFitLocked(size_t bytes);
  void eraseLocked(std::string const& name, bool count_as_invalidated);
  void enqueueFill(std::string const& name);
  void fillLoop();
  void fillOne(std::string const& name);

  std::unique_ptr<Storage> backing_;
  Options options_;

  std::mutex mtx_;
  std::unordered_map<std::string, Entry> index_;
  std::list<std::string> lru_;  // front = most recently used
  uint64_t current_bytes_ = 0;

  std::mutex parts_mtx_;
  std::unordered_map<std::string, std::unique_ptr<std::ofstream>> parts_;
  std::unordered_set<std::string> poisoned_parts_;

  std::mutex fill_mtx_;
  std::condition_variable fill_cv_;
  std::deque<std::string> fill_queue_;
  std::unordered_set<std::string> queued_;
  bool fill_started_ = false;
  bool fill_stop_ = false;
  bool fill_busy_ = false;
  std::thread fill_thread_;

  std::atomic<uint64_t> hits_{0}, misses_{0}, hit_bytes_{0}, miss_bytes_{0}, passthrough_{0};
  std::atomic<uint64_t> fills_{0}, fill_bytes_{0}, fill_gets_{0};
  std::atomic<uint64_t> fill_skipped_budget_{0}, fill_skipped_present_{0}, fill_gone_{0}, fill_failed_{0}, fill_dropped_{0};
  std::atomic<uint64_t> writethrough_files_{0}, evictions_{0}, evicted_bytes_{0}, invalidated_{0};
};

}  // namespace ozonedb
```

- [ ] **Step 4: Write the pass-through implementation**

```cpp
// src/db/disk_cache_storage.cpp
#include "disk_cache_storage.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <iostream>
#include <system_error>
#include <vector>

namespace ozonedb {

namespace fs = std::filesystem;

DiskCacheStorage::DiskCacheStorage(std::unique_ptr<Storage> backing, Options options)
    : Storage(options.dir), backing_(std::move(backing)), options_(std::move(options)) {
  if (!options_.dir.empty() && options_.dir.back() != '/') options_.dir += '/';
  storage_path = options_.dir;
  std::error_code ec;
  fs::create_directories(options_.dir, ec);
  if (ec) std::cerr << "[disk_cache] cannot create " << options_.dir << ": " << ec.message() << "\n";
}

DiskCacheStorage::~DiskCacheStorage() {
  stopFillWorker();
  std::lock_guard<std::mutex> lk(parts_mtx_);
  parts_.clear();
}

bool DiskCacheStorage::cacheable(std::string const& name) const {
  return options_.capacity_bytes > 0 && name.size() >= options_.prefix.size() && name.compare(0, options_.prefix.size(), options_.prefix) == 0;
}

void DiskCacheStorage::createDirectory(std::string name) {
  backing_->createDirectory(name);
  if (cacheable(name)) {
    std::error_code ec;
    fs::create_directories(localPath(name), ec);
  }
}

Status DiskCacheStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  // Task 2 adds the write-through.
  return backing_->append(fileName, data, length);
}

Status DiskCacheStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  return backing_->appendNoFlush(fileName, data, length);
}

Status DiskCacheStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  return backing_->appendInBatch(fileName, data, length);
}

Status DiskCacheStorage::flush(std::string const& fileName) {
  return backing_->flush(fileName);
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  if (!cacheable(fileName)) passthrough_.fetch_add(1);
  return backing_->read(fileName, data, size);
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  if (!cacheable(fileName)) passthrough_.fetch_add(1);
  return backing_->read(fileName, data, a, length);
}

size_t DiskCacheStorage::size(std::string fileName) { return backing_->size(fileName); }
void DiskCacheStorage::seal(std::string fileName) { backing_->seal(fileName); }
bool DiskCacheStorage::isSealed(std::string fileName) { return backing_->isSealed(fileName); }
void DiskCacheStorage::remove(std::string fileName) { backing_->remove(fileName); }
bool DiskCacheStorage::exist(std::string fileName) { return backing_->exist(fileName); }
void DiskCacheStorage::setRemoteAppendListener(RemoteAppendListener listener) { backing_->setRemoteAppendListener(std::move(listener)); }
long DiskCacheStorage::lastAppendAddressForThread() const { return backing_->lastAppendAddressForThread(); }
void DiskCacheStorage::sync() { backing_->sync(); }
void DiskCacheStorage::clearSync() { backing_->clearSync(); }
bool DiskCacheStorage::hasSyncToken() const { return backing_->hasSyncToken(); }

// --- tier API stubs completed in Tasks 2-5 ---
void DiskCacheStorage::invalidate(std::string const&) {}
size_t DiskCacheStorage::reconcile(std::function<bool(std::string const&, size_t)> const&) { return 0; }
void DiskCacheStorage::startFillWorker() {}
void DiskCacheStorage::stopFillWorker() {}
void DiskCacheStorage::waitFillIdle() {}

DiskCacheStorage::Stats DiskCacheStorage::stats() {
  Stats s;
  s.hits = hits_.load();
  s.misses = misses_.load();
  s.hit_bytes = hit_bytes_.load();
  s.miss_bytes = miss_bytes_.load();
  s.passthrough = passthrough_.load();
  s.fills = fills_.load();
  s.fill_bytes = fill_bytes_.load();
  s.fill_gets = fill_gets_.load();
  s.fill_skipped_budget = fill_skipped_budget_.load();
  s.fill_skipped_present = fill_skipped_present_.load();
  s.fill_gone = fill_gone_.load();
  s.fill_failed = fill_failed_.load();
  s.fill_dropped = fill_dropped_.load();
  s.writethrough_files = writethrough_files_.load();
  s.evictions = evictions_.load();
  s.evicted_bytes = evicted_bytes_.load();
  s.invalidated = invalidated_.load();
  std::lock_guard<std::mutex> lk(mtx_);
  s.files = index_.size();
  s.bytes = current_bytes_;
  s.capacity = options_.capacity_bytes;
  return s;
}

void DiskCacheStorage::printStats() {
  Stats s = stats();
  std::cerr << "[disk_cache] hits=" << s.hits << " misses=" << s.misses
            << " hit_bytes=" << s.hit_bytes << " miss_bytes=" << s.miss_bytes
            << " passthrough=" << s.passthrough << " fills=" << s.fills
            << " fill_bytes=" << s.fill_bytes << " fill_gets=" << s.fill_gets
            << " fill_skipped_budget=" << s.fill_skipped_budget
            << " fill_skipped_present=" << s.fill_skipped_present
            << " fill_gone=" << s.fill_gone << " fill_failed=" << s.fill_failed
            << " fill_dropped=" << s.fill_dropped
            << " writethrough_files=" << s.writethrough_files
            << " evictions=" << s.evictions << " evicted_bytes=" << s.evicted_bytes
            << " invalidated=" << s.invalidated << " files=" << s.files
            << " bytes=" << s.bytes << " capacity=" << s.capacity << "\n";
}

}  // namespace ozonedb
```

Note: `FileStorage`'s overrides are not marked `override`, but they are virtual through
the base, so `CountingStorage`'s `override` compiles. `FileStorage::read` on a name whose
parent directory does not exist fails; the fixture creates `sstable1/` and `checkpoint/`.

- [ ] **Step 5: Build and run the two tests**

```bash
cmake --build build --target runUnitTests -j"$(nproc)" && cd build && ./runUnitTests --gtest_filter='DiskCacheStorageTest.*'
```

Expected: 2 PASSED.

- [ ] **Step 6: Commit**

```bash
git add src/include/ozonedb/disk_cache_storage.h src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp CMakeLists.txt
git commit -m "disk cache: Storage decorator skeleton, pass-through and the sstable name filter"
```

---

### Task 2: Write-through on `flush`, local reads, local `size`

**Files:**
- Modify: `src/db/disk_cache_storage.cpp` (`append*`, `flush`, both `read`, `size`,
  `touch`, `present`, `readLocal`, `writePart`, `publishPart`, `discardPart`, `admit`,
  `evictToFitLocked`, `eraseLocked`)
- Test: `tests/test_disk_cache_storage.cpp`

**Interfaces:**
- Consumes: Task 1's class.
- Produces: `bool admit(name, bytes)` (private, used by Tasks 3 and 5), the counters
  `hits_`, `misses_`, `writethrough_files_`, `evictions_`.

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(DiskCacheStorageTest, WriteThroughPublishesOnFlushAndServesReads) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> a(3000, 'a'), b(2000, 'b');
  ASSERT_EQ(f.tier->appendNoFlush(name, a.data(), 3000), Status::kSuccess);
  ASSERT_EQ(f.tier->appendNoFlush(name, b.data(), 2000), Status::kSuccess);
  EXPECT_TRUE(f.part(name));
  EXPECT_FALSE(f.local(name));
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  EXPECT_TRUE(f.local(name));
  EXPECT_FALSE(f.part(name));
  EXPECT_EQ(std::filesystem::file_size(f.tier_dir + name), 5000u);

  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 2990, 20), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, "aaaaaaaaaabbbbbbbbbb", 20));
  delete[] data;
  EXPECT_EQ(f.backing->ranged_reads, 0);
  auto s = f.tier->stats();
  EXPECT_EQ(s.hits, 1u);
  EXPECT_EQ(s.hit_bytes, 20u);
  EXPECT_EQ(s.misses, 0u);
  EXPECT_EQ(s.writethrough_files, 1u);
  EXPECT_EQ(s.files, 1u);
  EXPECT_EQ(s.bytes, 5000u);

  // size() answers locally for a complete copy.
  int const sizes_before = f.backing->sizes;
  EXPECT_EQ(f.tier->size(name), 5000u);
  EXPECT_EQ(f.backing->sizes, sizes_before);
}

TEST(DiskCacheStorageTest, MissIsServedFromTheBackingAndCounted) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 10000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 4096, 4096), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, bytes.data() + 4096, 4096));
  delete[] data;
  EXPECT_EQ(f.backing->ranged_reads, 1);
  auto s = f.tier->stats();
  EXPECT_EQ(s.misses, 1u);
  EXPECT_EQ(s.miss_bytes, 4096u);
  EXPECT_EQ(s.hits, 0u);
  // A read of an absent file fails and is not a local hit.
  EXPECT_EQ(f.tier->read("sstable1/absent.sst", data, 0, 10), Status::kFailure);
}

TEST(DiskCacheStorageTest, ShortLocalCopyIsDroppedAndTheBackingServes) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> a(3000, 'a');
  ASSERT_EQ(f.tier->appendNoFlush(name, a.data(), 3000), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  std::filesystem::resize_file(f.tier_dir + name, 100);  // corrupt the copy
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 2000, 500), Status::kSuccess);
  EXPECT_EQ(data[0], 'a');
  delete[] data;
  EXPECT_FALSE(f.local(name));
  EXPECT_EQ(f.tier->stats().files, 0u);
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
}

TEST(DiskCacheStorageTest, FailedFlushDiscardsThePart) {
  // The backing rejects the flush: the tier must not publish the part.
  class RejectingStorage : public CountingStorage {
   public:
    explicit RejectingStorage(std::string const& path) : CountingStorage(path) {}
    Status flush(std::string const&) override { return Status::kFailure; }
  };
  std::string const s = stamp();
  std::string const backing_dir = kRoot + "backing_" + s + "/";
  std::string const tier_dir = kRoot + "tier_" + s + "/";
  std::filesystem::create_directories(backing_dir + "sstable1");
  DiskCacheStorage::Options o;
  o.dir = tier_dir;
  o.capacity_bytes = 1u << 20;
  DiskCacheStorage tier(std::make_unique<RejectingStorage>(backing_dir), o);
  std::string const name = "sstable1/x.sst";
  std::vector<unsigned char> a(10, 'a');
  ASSERT_EQ(tier.appendNoFlush(name, a.data(), 10), Status::kSuccess);
  EXPECT_EQ(tier.flush(name), Status::kFailure);
  EXPECT_FALSE(std::filesystem::exists(tier_dir + name));
  EXPECT_FALSE(std::filesystem::exists(tier_dir + name + ".part"));
  EXPECT_EQ(tier.stats().files, 0u);
  std::filesystem::remove_all(backing_dir);
  std::filesystem::remove_all(tier_dir);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd build && ./runUnitTests --gtest_filter='DiskCacheStorageTest.*'
```

Expected: the four new tests FAIL (`f.part(name)` false, `hits` 0, `files` 0).

- [ ] **Step 3: Implement the write-through, the local read path and the budget primitives**

Replace the Task 1 stubs of `append*`, `flush`, both `read`s and `size` with:

```cpp
Status DiskCacheStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  Status s = backing_->append(fileName, data, length);
  if (cacheable(fileName)) {
    if (s == Status::kSuccess) {
      writePart(fileName, data, static_cast<size_t>(length));
      if (publishPart(fileName)) writethrough_files_.fetch_add(1);  // append() leaves the object complete
    } else {
      discardPart(fileName);
    }
  }
  return s;
}

Status DiskCacheStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  Status s = backing_->appendNoFlush(fileName, data, length);
  if (cacheable(fileName) && s == Status::kSuccess) writePart(fileName, data, static_cast<size_t>(length));
  return s;
}

Status DiskCacheStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  Status s = backing_->appendInBatch(fileName, data, length);
  if (cacheable(fileName) && s == Status::kSuccess) writePart(fileName, data, static_cast<size_t>(length));
  return s;
}

Status DiskCacheStorage::flush(std::string const& fileName) {
  Status s = backing_->flush(fileName);
  if (!cacheable(fileName)) return s;
  if (s == Status::kSuccess) {
    if (publishPart(fileName)) writethrough_files_.fetch_add(1);
  } else {
    discardPart(fileName);
  }
  return s;
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  if (!cacheable(fileName)) {
    passthrough_.fetch_add(1);
    return backing_->read(fileName, data, size);
  }
  if (touch(fileName)) {
    std::error_code ec;
    auto n = fs::file_size(localPath(fileName), ec);
    if (!ec && readLocal(fileName, data, 0, static_cast<size_t>(n))) {
      size = static_cast<size_t>(n);
      hits_.fetch_add(1);
      hit_bytes_.fetch_add(size);
      return Status::kSuccess;
    }
    invalidate(fileName);
  }
  Status s = backing_->read(fileName, data, size);
  misses_.fetch_add(1);
  if (s == Status::kSuccess) {
    miss_bytes_.fetch_add(size);
    enqueueFill(fileName);
  }
  return s;
}

Status DiskCacheStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  if (!cacheable(fileName)) {
    passthrough_.fetch_add(1);
    return backing_->read(fileName, data, a, length);
  }
  if (touch(fileName)) {
    if (readLocal(fileName, data, a, length)) {
      hits_.fetch_add(1);
      hit_bytes_.fetch_add(length);
      return Status::kSuccess;
    }
    invalidate(fileName);  // the copy is short or unreadable: drop it
  }
  Status s = backing_->read(fileName, data, a, length);
  misses_.fetch_add(1);
  if (s == Status::kSuccess) {
    miss_bytes_.fetch_add(length);
    enqueueFill(fileName);
  }
  return s;
}

size_t DiskCacheStorage::size(std::string fileName) {
  if (cacheable(fileName)) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = index_.find(fileName);
    if (it != index_.end()) return it->second.bytes;
  }
  return backing_->size(fileName);
}

bool DiskCacheStorage::touch(std::string const& name) {
  std::lock_guard<std::mutex> lk(mtx_);
  auto it = index_.find(name);
  if (it == index_.end()) return false;
  lru_.splice(lru_.begin(), lru_, it->second.lru);
  return true;
}

bool DiskCacheStorage::present(std::string const& name) {
  std::lock_guard<std::mutex> lk(mtx_);
  return index_.count(name) != 0;
}

bool DiskCacheStorage::readLocal(std::string const& name, unsigned char*& data, size_t a, size_t length) {
  int fd = ::open(localPath(name).c_str(), O_RDONLY);
  if (fd < 0) return false;
  unsigned char* buf = new unsigned char[length];
  size_t done = 0;
  while (done < length) {
    ssize_t n = ::pread(fd, buf + done, length - done, static_cast<off_t>(a + done));
    if (n <= 0) break;
    done += static_cast<size_t>(n);
  }
  if (options_.drop_pages) ::posix_fadvise(fd, static_cast<off_t>(a), static_cast<off_t>(length), POSIX_FADV_DONTNEED);
  ::close(fd);
  if (done != length) {
    delete[] buf;
    return false;
  }
  data = buf;
  return true;
}

void DiskCacheStorage::writePart(std::string const& name, unsigned char const* data, size_t length) {
  std::lock_guard<std::mutex> lk(parts_mtx_);
  if (poisoned_parts_.count(name)) return;
  auto it = parts_.find(name);
  if (it == parts_.end()) {
    std::error_code ec;
    fs::create_directories(fs::path(partPath(name)).parent_path(), ec);
    auto out = std::make_unique<std::ofstream>(partPath(name), std::ios::binary | std::ios::trunc);
    if (!out->is_open()) {
      poisoned_parts_.insert(name);
      return;
    }
    it = parts_.emplace(name, std::move(out)).first;
  }
  it->second->write(reinterpret_cast<char const*>(data), static_cast<std::streamsize>(length));
  if (!*it->second) {
    parts_.erase(it);
    poisoned_parts_.insert(name);
    std::error_code ec;
    fs::remove(partPath(name), ec);
  }
}

bool DiskCacheStorage::publishPart(std::string const& name) {
  {
    std::lock_guard<std::mutex> lk(parts_mtx_);
    if (poisoned_parts_.erase(name)) {
      std::error_code ec;
      fs::remove(partPath(name), ec);
      return false;
    }
    auto it = parts_.find(name);
    if (it == parts_.end()) return false;
    it->second->flush();
    bool ok = static_cast<bool>(*it->second);
    parts_.erase(it);  // closes the stream
    if (!ok) {
      std::error_code ec;
      fs::remove(partPath(name), ec);
      return false;
    }
  }
  std::error_code ec;
  auto bytes = fs::file_size(partPath(name), ec);
  if (ec) return false;
  if (bytes > options_.capacity_bytes) {
    fs::remove(partPath(name), ec);
    fill_skipped_budget_.fetch_add(1);
    return false;
  }
  fs::rename(partPath(name), localPath(name), ec);
  if (ec) {
    fs::remove(partPath(name), ec);
    return false;
  }
  return admit(name, static_cast<size_t>(bytes));  // the caller counts a write-through or a fill
}

void DiskCacheStorage::discardPart(std::string const& name) {
  std::lock_guard<std::mutex> lk(parts_mtx_);
  parts_.erase(name);
  poisoned_parts_.erase(name);
  std::error_code ec;
  fs::remove(partPath(name), ec);
}

bool DiskCacheStorage::admit(std::string const& name, size_t bytes) {
  std::lock_guard<std::mutex> lk(mtx_);
  if (bytes > options_.capacity_bytes) return false;
  auto old = index_.find(name);
  if (old != index_.end()) {  // re-created name: replace the accounting
    current_bytes_ -= old->second.bytes;
    lru_.erase(old->second.lru);
    index_.erase(old);
  }
  evictToFitLocked(bytes);
  lru_.push_front(name);
  index_[name] = Entry{bytes, lru_.begin()};
  current_bytes_ += bytes;
  return true;
}

void DiskCacheStorage::evictToFitLocked(size_t bytes) {
  while (current_bytes_ + bytes > options_.capacity_bytes && !lru_.empty()) {
    std::string victim = lru_.back();
    evictions_.fetch_add(1);
    evicted_bytes_.fetch_add(index_[victim].bytes);
    eraseLocked(victim, /*count_as_invalidated=*/false);
  }
}

void DiskCacheStorage::eraseLocked(std::string const& name, bool count_as_invalidated) {
  auto it = index_.find(name);
  if (it == index_.end()) return;
  current_bytes_ -= it->second.bytes;
  lru_.erase(it->second.lru);
  index_.erase(it);
  std::error_code ec;
  fs::remove(localPath(name), ec);
  if (count_as_invalidated) invalidated_.fetch_add(1);
}

void DiskCacheStorage::invalidate(std::string const& name) {
  discardPart(name);
  std::lock_guard<std::mutex> lk(mtx_);
  eraseLocked(name, /*count_as_invalidated=*/true);
}
```

`enqueueFill` stays a no-op until Task 3 (`void DiskCacheStorage::enqueueFill(std::string const&) {}`).
Replace the Task 1 stub of `invalidate` with the version above.

- [ ] **Step 4: Run the tests**

```bash
cd build && ./runUnitTests --gtest_filter='DiskCacheStorageTest.*'
```

Expected: 6 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk cache: write-through on flush, local pread with fadvise, local size, budget primitives"
```

---

### Task 3: Fill on miss — the worker

**Files:**
- Modify: `src/db/disk_cache_storage.cpp` (`enqueueFill`, `startFillWorker`,
  `stopFillWorker`, `waitFillIdle`, `fillLoop`, `fillOne`)
- Test: `tests/test_disk_cache_storage.cpp`

**Interfaces:**
- Consumes: `admit`, `present`, `writePart`/`publishPart` from Task 2.
- Produces: `startFillWorker()`, `stopFillWorker()`, `waitFillIdle()` (the DB calls the
  first two; tests call the third). Counters `fills_`, `fill_bytes_`, `fill_gets_`,
  `fill_skipped_*`, `fill_gone_`, `fill_failed_`, `fill_dropped_`.

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(DiskCacheStorageTest, MissQueuesAChunkedFillAndTheNextReadHits) {
  TierFixture f(1u << 20, /*chunk=*/1024);
  f.tier->startFillWorker();
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 3 * 1024 + 1);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(name, data, 5, 10), Status::kSuccess);
  delete[] data;
  f.tier->waitFillIdle();
  EXPECT_TRUE(f.local(name));
  EXPECT_FALSE(f.part(name));
  auto s = f.tier->stats();
  EXPECT_EQ(s.fills, 1u);
  EXPECT_EQ(s.fill_bytes, 3u * 1024 + 1);
  EXPECT_EQ(s.fill_gets, 4u);  // 3 full chunks + 1 byte
  EXPECT_EQ(s.files, 1u);
  std::ifstream in(f.tier_dir + name, std::ios::binary);
  std::vector<unsigned char> copy((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_EQ(copy, bytes);

  int const before = f.backing->ranged_reads;
  ASSERT_EQ(f.tier->read(name, data, 2048, 100), Status::kSuccess);
  EXPECT_EQ(0, memcmp(data, bytes.data() + 2048, 100));
  delete[] data;
  EXPECT_EQ(f.backing->ranged_reads, before);
  EXPECT_EQ(f.tier->stats().hits, 1u);
}

TEST(DiskCacheStorageTest, FillSkipsFilesLargerThanTheCapacityAndFilesThatAreGone) {
  TierFixture f(2000, /*chunk=*/1024);
  f.tier->startFillWorker();
  std::string const big = "sstable1/" + stamp() + "_big.sst";
  f.seed(big, 5000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(big, data, 0, 10), Status::kSuccess);
  delete[] data;
  f.tier->waitFillIdle();
  EXPECT_FALSE(f.local(big));
  EXPECT_EQ(f.tier->stats().fill_skipped_budget, 1u);

  // Queue a name, then delete it from the backing before the worker runs.
  f.tier->stopFillWorker();
  std::string const gone = "sstable1/" + stamp() + "_gone.sst";
  f.seed(gone, 100);
  ASSERT_EQ(f.tier->read(gone, data, 0, 10), Status::kSuccess);
  delete[] data;
  std::filesystem::remove(f.backing_dir + gone);
  f.tier->startFillWorker();
  f.tier->waitFillIdle();
  EXPECT_FALSE(f.local(gone));
  EXPECT_EQ(f.tier->stats().fill_gone, 1u);
  EXPECT_EQ(f.tier->stats().fills, 0u);
}

TEST(DiskCacheStorageTest, FillQueueIsBoundedAndDeduplicated) {
  TierFixture f(1u << 20);
  DiskCacheStorage::Options o = f.tier->options();
  // Rebuild with a queue of 2 (Options are read at construction).
  f.tier.reset();
  auto owned = std::make_unique<CountingStorage>(f.backing_dir);
  f.backing = owned.get();
  o.max_queue = 2;
  f.tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);
  // The worker is not started, so the queue only grows.
  std::vector<std::string> names;
  for (int i = 0; i < 4; ++i) {
    names.push_back("sstable1/" + stamp() + "_" + std::to_string(i) + ".sst");
    f.seed(names.back(), 100);
  }
  unsigned char* data = nullptr;
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(f.tier->read(names[i], data, 0, 10), Status::kSuccess);
    delete[] data;
    ASSERT_EQ(f.tier->read(names[i], data, 0, 10), Status::kSuccess);  // duplicate
    delete[] data;
  }
  EXPECT_EQ(f.tier->stats().fill_dropped, 2u);  // 4 distinct names, bound 2
  f.tier->startFillWorker();
  f.tier->waitFillIdle();
  EXPECT_EQ(f.tier->stats().fills, 2u);
  EXPECT_FALSE(f.local(names[0]));
  EXPECT_FALSE(f.local(names[1]));
  EXPECT_TRUE(f.local(names[2]));
  EXPECT_TRUE(f.local(names[3]));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Expected: the three new tests FAIL (`fills` 0, `local` false).

- [ ] **Step 3: Implement the worker**

```cpp
void DiskCacheStorage::enqueueFill(std::string const& name) {
  std::lock_guard<std::mutex> lk(fill_mtx_);
  if (queued_.count(name)) return;
  while (fill_queue_.size() >= options_.max_queue && !fill_queue_.empty()) {
    queued_.erase(fill_queue_.front());
    fill_queue_.pop_front();
    fill_dropped_.fetch_add(1);
  }
  fill_queue_.push_back(name);
  queued_.insert(name);
  fill_cv_.notify_all();
}

void DiskCacheStorage::startFillWorker() {
  std::lock_guard<std::mutex> lk(fill_mtx_);
  if (fill_started_) return;
  fill_started_ = true;
  fill_stop_ = false;
  fill_thread_ = std::thread([this] { fillLoop(); });
}

void DiskCacheStorage::stopFillWorker() {
  {
    std::lock_guard<std::mutex> lk(fill_mtx_);
    if (!fill_started_) return;
    fill_stop_ = true;
    fill_cv_.notify_all();
  }
  fill_thread_.join();
  std::lock_guard<std::mutex> lk(fill_mtx_);
  fill_started_ = false;
}

void DiskCacheStorage::waitFillIdle() {
  std::unique_lock<std::mutex> lk(fill_mtx_);
  fill_cv_.wait(lk, [this] { return !fill_started_ || (fill_queue_.empty() && !fill_busy_); });
}

void DiskCacheStorage::fillLoop() {
  for (;;) {
    std::string name;
    {
      std::unique_lock<std::mutex> lk(fill_mtx_);
      fill_cv_.wait(lk, [this] { return fill_stop_ || !fill_queue_.empty(); });
      if (fill_stop_) return;
      name = fill_queue_.front();
      fill_queue_.pop_front();
      queued_.erase(name);
      fill_busy_ = true;
    }
    fillOne(name);
    {
      std::lock_guard<std::mutex> lk(fill_mtx_);
      fill_busy_ = false;
    }
    fill_cv_.notify_all();
  }
}

void DiskCacheStorage::fillOne(std::string const& name) {
  if (present(name)) {
    fill_skipped_present_.fetch_add(1);
    return;
  }
  {
    std::lock_guard<std::mutex> lk(parts_mtx_);
    if (parts_.count(name)) return;  // the builder is writing it through right now
  }
  size_t const total = backing_->size(name);
  if (total == 0) {
    fill_gone_.fetch_add(1);
    return;
  }
  if (total > options_.capacity_bytes) {
    fill_skipped_budget_.fetch_add(1);
    return;
  }
  for (size_t off = 0; off < total; off += options_.chunk_bytes) {
    size_t const len = std::min(options_.chunk_bytes, total - off);
    unsigned char* buf = nullptr;
    Status s = backing_->read(name, buf, off, len);
    fill_gets_.fetch_add(1);
    if (s != Status::kSuccess || buf == nullptr) {
      discardPart(name);
      fill_failed_.fetch_add(1);
      return;
    }
    writePart(name, buf, len);
    delete[] buf;
  }
  if (publishPart(name)) {
    fills_.fetch_add(1);
    fill_bytes_.fetch_add(total);
  } else {
    fill_failed_.fetch_add(1);
  }
}
```

Add `#include <algorithm>` for `std::min`. Note the destructor already calls
`stopFillWorker()` (Task 1), so a tier that is destroyed with a running worker joins it.

- [ ] **Step 4: Run the tests**

Expected: 9 PASSED. If `FillQueueIsBoundedAndDeduplicated` is flaky, check that
`enqueueFill` is not called for a name whose read failed (only successful reads queue).

- [ ] **Step 5: Commit**

```bash
git add src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk cache: background fill on the first miss, chunked ranged reads, bounded deduplicated queue"
```

---

### Task 4: Budget eviction, `remove`, `invalidate`

**Files:**
- Modify: `src/db/disk_cache_storage.cpp` (`remove`)
- Test: `tests/test_disk_cache_storage.cpp`

**Interfaces:**
- Consumes: `admit`/`evictToFitLocked`/`eraseLocked` from Task 2.
- Produces: `remove(name)` drops the local copy after forwarding; `invalidate(name)` is
  the peer path (Task 6 wires it).

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(DiskCacheStorageTest, BudgetEvictsTheLeastRecentlyReadFile) {
  TierFixture f(2500);
  auto put = [&](std::string const& name, size_t n) {
    std::vector<unsigned char> v(n, 'x');
    ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), static_cast<int>(n)), Status::kSuccess);
    ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  };
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  std::string const c = "sstable1/" + stamp() + "_c.sst";
  put(a, 1000);
  put(b, 1000);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read(a, data, 0, 10), Status::kSuccess);  // a is now the most recent
  delete[] data;
  put(c, 1000);  // 3000 > 2500: evict b, the least recently used
  EXPECT_TRUE(f.local(a));
  EXPECT_FALSE(f.local(b));
  EXPECT_TRUE(f.local(c));
  auto s = f.tier->stats();
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.evicted_bytes, 1000u);
  EXPECT_EQ(s.files, 2u);
  EXPECT_EQ(s.bytes, 2000u);
  // b is still on the backing: the tier evicts copies, never objects.
  EXPECT_TRUE(f.backing->exist(b));
}

TEST(DiskCacheStorageTest, RemoveDropsTheObjectAndTheCopy) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> v(100, 'x');
  ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), 100), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  ASSERT_TRUE(f.local(name));
  f.tier->remove(name);
  EXPECT_FALSE(f.local(name));
  EXPECT_FALSE(f.backing->exist(name));
  EXPECT_EQ(f.tier->stats().files, 0u);
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
}

TEST(DiskCacheStorageTest, InvalidateDropsOnlyTheCopy) {
  TierFixture f(1u << 20);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> v(100, 'x');
  ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), 100), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  f.tier->invalidate(name);
  EXPECT_FALSE(f.local(name));
  EXPECT_TRUE(f.backing->exist(name));
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
  f.tier->invalidate(name);  // absent: a no-op
  EXPECT_EQ(f.tier->stats().invalidated, 1u);
  // A part in progress is discarded too.
  std::string const other = "sstable1/" + stamp() + "_part.sst";
  ASSERT_EQ(f.tier->appendNoFlush(other, v.data(), 100), Status::kSuccess);
  ASSERT_TRUE(f.part(other));
  f.tier->invalidate(other);
  EXPECT_FALSE(f.part(other));
  ASSERT_EQ(f.tier->flush(other), Status::kSuccess);  // the backing still flushes
  EXPECT_FALSE(f.local(other));                        // but nothing is published
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Expected: `RemoveDropsTheObjectAndTheCopy` FAILS (`local` still true). The other two
pass already if Task 2 was implemented as written — that is fine, keep them.

- [ ] **Step 3: Implement `remove`**

```cpp
void DiskCacheStorage::remove(std::string fileName) {
  backing_->remove(fileName);
  if (cacheable(fileName)) invalidate(fileName);
}
```

- [ ] **Step 4: Run the tests**

Expected: 12 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk cache: remove drops the local copy; eviction and invalidate covered by tests"
```

---

### Task 5: Reconciliation at open

**Files:**
- Modify: `src/db/disk_cache_storage.cpp` (`reconcile`)
- Test: `tests/test_disk_cache_storage.cpp`

**Interfaces:**
- Produces: `size_t reconcile(std::function<bool(std::string const&, size_t)> const& live)`;
  Task 6 calls it from `openDB` with a View-backed lambda.

- [ ] **Step 1: Write the failing test**

```cpp
TEST(DiskCacheStorageTest, ReconcileDropsPartsDeadFilesAndForeignNames) {
  TierFixture f(1u << 20);
  auto write = [&](std::string const& rel, size_t n) {
    std::filesystem::create_directories(std::filesystem::path(f.tier_dir + rel).parent_path());
    std::ofstream out(f.tier_dir + rel, std::ios::binary);
    std::string s(n, 'z');
    out.write(s.data(), static_cast<std::streamsize>(n));
  };
  write("sstable1/live.sst", 300);
  write("sstable1/dead.sst", 300);
  write("sstable1/wrongsize.sst", 300);
  write("sstable1/inflight.sst.part", 50);
  write("checkpoint/LATEST", 8);  // not cacheable: a leftover of another config
  std::vector<std::string> asked;
  size_t const removed = f.tier->reconcile([&](std::string const& name, size_t bytes) {
    asked.push_back(name);
    if (name == "sstable1/live.sst") return bytes == 300;
    if (name == "sstable1/wrongsize.sst") return bytes == 999;
    return false;
  });
  EXPECT_EQ(removed, 4u);
  EXPECT_TRUE(f.local("sstable1/live.sst"));
  EXPECT_FALSE(f.local("sstable1/dead.sst"));
  EXPECT_FALSE(f.local("sstable1/wrongsize.sst"));
  EXPECT_FALSE(f.part("sstable1/inflight.sst"));
  EXPECT_FALSE(f.local("checkpoint/LATEST"));
  EXPECT_EQ(asked.size(), 3u);  // the part and the foreign name are not asked
  auto s = f.tier->stats();
  EXPECT_EQ(s.files, 1u);
  EXPECT_EQ(s.bytes, 300u);
  unsigned char* data = nullptr;
  ASSERT_EQ(f.tier->read("sstable1/live.sst", data, 0, 10), Status::kSuccess);
  delete[] data;
  EXPECT_EQ(f.tier->stats().hits, 1u);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Expected: FAIL (`removed` 0).

- [ ] **Step 3: Implement `reconcile`**

```cpp
size_t DiskCacheStorage::reconcile(std::function<bool(std::string const&, size_t)> const& live) {
  size_t removed = 0;
  std::error_code ec;
  if (!fs::exists(options_.dir, ec)) return 0;
  struct Found {
    std::string name;
    size_t bytes;
    fs::file_time_type mtime;
  };
  std::vector<Found> keep;
  for (auto it = fs::recursive_directory_iterator(options_.dir, ec); !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
    if (!it->is_regular_file(ec)) continue;
    std::string const rel = fs::relative(it->path(), options_.dir, ec).generic_string();
    if (ec) continue;
    bool const is_part = rel.size() > 5 && rel.compare(rel.size() - 5, 5, ".part") == 0;
    size_t const bytes = static_cast<size_t>(it->file_size(ec));
    if (is_part || !cacheable(rel) || !live(rel, bytes)) {
      fs::remove(it->path(), ec);
      ++removed;
      continue;
    }
    keep.push_back({rel, bytes, it->last_write_time(ec)});
  }
  std::sort(keep.begin(), keep.end(), [](Found const& a, Found const& b) { return a.mtime < b.mtime; });
  for (Found const& k : keep) {
    if (!admit(k.name, k.bytes)) {  // larger than the capacity
      fs::remove(localPath(k.name), ec);
      ++removed;
    }
  }
  return removed;
}
```

Note: `rel` is asked of `live` only for cacheable, complete names, so the View lambda
in Task 6 never sees `checkpoint/…`. Files are admitted oldest first, so the LRU tail
after a restart is the oldest copy.

- [ ] **Step 4: Run the tests**

Expected: 13 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk cache: reconcile the directory against the live set at open"
```

---

### Task 6: Config keys and DB wiring

**Files:**
- Modify: `src/include/ozonedb/metadata.h:109` (fields), `:271` (parsing)
- Modify: `src/include/ozonedb/metadata_log_handler.h:135` (listener setter),
  `src/db/metadata_log_handler.cpp:359` (call it)
- Modify: `src/include/ozonedb/db.h:60` (member), `src/db/db.cpp:73-77` (wrap),
  `:95` (wire the listener), `:181-185` (reconcile + start), `:225-226` (stop + print)
- Test: `tests/test_disk_cache_storage.cpp` (Metadata parsing)

**Interfaces:**
- Consumes: `DiskCacheStorage` (Tasks 1-5).
- Produces: `Metadata::disk_cache_dir`, `disk_cache_bytes`, `disk_cache_chunk_bytes`,
  `disk_cache_drop_pages`, `disk_cache_fill_queue`;
  `MetadataLogHandler::setSSTableRemovedListener(std::function<void(std::string const&)>)`;
  `DB::disk_cache` (non-owning; owned through `sstable_storage`).

- [ ] **Step 1: Write the failing Metadata test**

```cpp
TEST(DiskCacheStorageTest, MetadataParsesTheDiskCacheKeys) {
  std::string const dir = kRoot + "cfg_" + stamp() + "/";
  std::filesystem::create_directories(dir);
  auto write = [&](std::string const& body) {
    std::string const path = dir + "cfg.json";
    std::ofstream(path) << body;
    return path;
  };
  // Copy every key of src/config/local/shared_config_base.json that Metadata
  // requires and add the tier keys.
  std::string const base = R"({"backend": "local", "db_path": ")" + dir + R"(db/", "sstable_backend": "local", "sstable_dir": ")" + dir + R"(sst/", "lru_cache_bytes": 1048576)";
  {
    Metadata md(write(base + R"(, "disk_cache_dir": "/tank/cache/w0", "disk_cache_bytes": "2147483648", "disk_cache_drop_pages": "false"})"));
    EXPECT_EQ(md.disk_cache_dir, "/tank/cache/w0/");  // slash appended
    EXPECT_EQ(md.disk_cache_bytes, 2147483648ull);
    EXPECT_FALSE(md.disk_cache_drop_pages);
    EXPECT_EQ(md.disk_cache_chunk_bytes, 67108864ull);
    EXPECT_EQ(md.disk_cache_fill_queue, 256ull);
  }
  {
    Metadata md(write(base + "}"));
    EXPECT_TRUE(md.disk_cache_dir.empty());
    EXPECT_TRUE(md.disk_cache_drop_pages);
  }
  EXPECT_THROW(Metadata(write(base + R"(, "disk_cache_dir": "/tank/cache/w0"})")), std::runtime_error);  // bytes missing
  std::string const no_backend = R"({"backend": "local", "db_path": ")" + dir + R"(db/", "disk_cache_dir": "/tank/cache/w0", "disk_cache_bytes": "1"})";
  EXPECT_THROW(Metadata(write(no_backend)), std::runtime_error);  // no sstable_backend
  std::filesystem::remove_all(dir);
}
```

If `Metadata`'s constructor requires more keys than `backend`, `db_path`,
`sstable_backend`, `sstable_dir` and `lru_cache_bytes`, copy the missing ones from
`src/config/local/shared_config_base.json` into `base`.

- [ ] **Step 2: Run the test to verify it fails**

Expected: compile error, `disk_cache_dir` is not a member.

- [ ] **Step 3: Add the fields and the parsing**

After `metadata.h:109` (`cache_warm_min_input_blocks`):

```cpp
  // Disk-backed read-through tier for SSTables (bench/PLAN-disk-cache.md).
  // Empty dir = off. The dir gets a trailing '/'. Requires sstable_backend.
  std::string disk_cache_dir;
  uint64_t disk_cache_bytes = 0;
  uint64_t disk_cache_chunk_bytes = 67108864;
  bool disk_cache_drop_pages = true;
  uint64_t disk_cache_fill_queue = 256;
```

After `metadata.h:271` (the `cache_warm_min_input_blocks` parse):

```cpp
  if (auto it = result.find("disk_cache_dir"); it != result.end() && !it->second.empty()) {
    disk_cache_dir = it->second;
    if (disk_cache_dir.back() != '/') disk_cache_dir += '/';
  }
  if (auto it = result.find("disk_cache_bytes"); it != result.end()) {
    disk_cache_bytes = std::stoull(it->second);
  }
  if (auto it = result.find("disk_cache_chunk_bytes"); it != result.end()) {
    disk_cache_chunk_bytes = std::stoull(it->second);
  }
  if (auto it = result.find("disk_cache_drop_pages"); it != result.end()) {
    disk_cache_drop_pages = !(it->second == "false" || it->second == "0");
  }
  if (auto it = result.find("disk_cache_fill_queue"); it != result.end()) {
    disk_cache_fill_queue = std::stoull(it->second);
  }
  if (!disk_cache_dir.empty() && !sstable_backend_set) {
    throw std::runtime_error("disk_cache_dir requires sstable_backend: the tier fronts the object store that holds the SSTables");
  }
  if (!disk_cache_dir.empty() && disk_cache_bytes == 0) {
    throw std::runtime_error("disk_cache_dir requires disk_cache_bytes > 0");
  }
```

Check that `sstable_backend_set` is assigned before this block (it is set in the block
at `metadata.h:206-231`). If the parse order differs, move the checks below it.

- [ ] **Step 4: Add the removed-file listener to `MetadataLogHandler`**

In `metadata_log_handler.h`, next to `setLRUCache` (`:135`):

```cpp
  // Called with each COMPACT input after the RAM cache dropped it, outside
  // view_mutex (drainCacheEvents). The DB wires the disk tier here.
  void setSSTableRemovedListener(std::function<void(std::string const&)> listener) { this->sstable_removed_listener_ = std::move(listener); }
```

and a private member `std::function<void(std::string const&)> sstable_removed_listener_;`
(add `#include <functional>` if missing). In `metadata_log_handler.cpp:359`, after
`cached_input_blocks += this->lru_cache->invalidateSSTable(input);`:

```cpp
      if (this->sstable_removed_listener_) this->sstable_removed_listener_(input);
```

- [ ] **Step 5: Wire the DB**

`db.h:60`, after `Storage* sstable_storage = nullptr;`:

```cpp
  // The disk tier in front of sstable_storage when disk_cache_dir is set;
  // then sstable_storage IS this object and owns the backing store. Non-owning.
  DiskCacheStorage* disk_cache = nullptr;
```

with `#include "disk_cache_storage.h"` (or a forward declaration `class DiskCacheStorage;`
in the header and the include in `db.cpp`).

`db.cpp:73-77`, after `sstable_storage` is built:

```cpp
    if (!metadata->disk_cache_dir.empty()) {
      DiskCacheStorage::Options o;
      o.dir = metadata->disk_cache_dir;
      o.capacity_bytes = metadata->disk_cache_bytes;
      o.prefix = metadata->sstable_level_prefix;
      o.chunk_bytes = static_cast<size_t>(metadata->disk_cache_chunk_bytes);
      o.drop_pages = metadata->disk_cache_drop_pages;
      o.max_queue = static_cast<size_t>(metadata->disk_cache_fill_queue);
      auto* tier = new DiskCacheStorage(std::unique_ptr<Storage>(this->sstable_storage), o);
      this->sstable_storage = tier;
      this->disk_cache = tier;
    }
```

`db.cpp:95`, after `this->metadata_log->setLRUCache(this->lru_cache);`:

```cpp
  if (this->disk_cache != nullptr) {
    DiskCacheStorage* tier = this->disk_cache;
    this->metadata_log->setSSTableRemovedListener([tier](std::string const& file_name) { tier->invalidate(file_name); });
  }
```

`db.cpp:185`, after `db->lru_cache->startWarmWorker();`:

```cpp
  if (db->disk_cache != nullptr) {
    std::shared_ptr<View const> view = db->latest_view_snapshot;
    size_t const removed = db->disk_cache->reconcile([view](std::string const& name, size_t bytes) {
      if (!view || view->key_range.find(name) == view->key_range.end()) return false;
      auto sz = view->file_size.find(name);
      return sz == view->file_size.end() || sz->second == bytes;
    });
    std::cerr << "[disk_cache] reconciled " << db->disk_cache->stats().files << " files, removed " << removed << "\n";
    db->disk_cache->startFillWorker();
  }
```

`db.cpp:225-226`, in `closeDB`:

```cpp
  db->lru_cache->stopWarmWorker();
  if (db->disk_cache != nullptr) db->disk_cache->stopFillWorker();
  db->lru_cache->printCacheStats();
  if (db->disk_cache != nullptr) db->disk_cache->printStats();
```

`~DB` needs no change: `delete this->sstable_storage` deletes the tier, which deletes
the backing.

- [ ] **Step 6: Build, run every unit test that does not need the network**

```bash
cmake --build build --target runUnitTests -j"$(nproc)" && cd build && ./runUnitTests --gtest_filter='DiskCacheStorageTest.*:LRUCacheTest.*:SSTableTest.*:StorageTest.*:CheckpointTest.*'
```

Expected: all PASSED, `MetadataParsesTheDiskCacheKeys` included.

- [ ] **Step 7: Commit**

```bash
git add src/include/ozonedb/metadata.h src/include/ozonedb/metadata_log_handler.h src/db/metadata_log_handler.cpp src/include/ozonedb/db.h src/db/db.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk cache: config keys, DB wraps sstable_storage, peers drop inputs on COMPACT, reconcile at open, stats at close"
```

---

### Task 7: Provision the SSD — `setup_disk_cache.sh` and an ansible playbook

**Files:**
- Create: `bench/scripts/setup_disk_cache.sh`
- Create: `bench/ansible/disk_cache.yml`
- Modify: `bench/ansible/README.md` (one paragraph), `CLAUDE.md` "Environment" (one
  line)

**Interfaces:**
- Produces: `/tank/cache` mounted from the ext4 label `ozcache` on every client, mode
  777, in `/etc/fstab` with `nofail`. Task 8 writes per-writer directories under it.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# setup_disk_cache.sh -- format one unused local SSD as the OzoneDB disk-cache
# tier and mount it (bench/PLAN-disk-cache.md).
#
# Idempotent: a device that already carries the label is only mounted; a
# mount that is already up is left alone. The script formats WHOLE DISKS
# only and refuses a disk with partitions, a foreign filesystem signature
# (unless --force), a mount, or the root filesystem.
#
#   bash bench/scripts/setup_disk_cache.sh --list
#   bash bench/scripts/setup_disk_cache.sh --device /dev/sdb
#   bash bench/scripts/setup_disk_cache.sh            # auto: the single unused SSD
set -euo pipefail

DEVICE=""
MOUNT="/tank/cache"
LABEL="ozcache"
FSTYPE="ext4"
FORCE=0

usage() {
  cat <<EOF
Usage: $0 [--device /dev/sdX] [--mount DIR] [--label NAME] [--force] [--list]

  --device DEV   whole disk to format (default: the single non-rotational disk
                 that has no partitions, no filesystem signature and no mount,
                 and is not the root disk; the script refuses to guess between two)
  --mount DIR    mount point (default $MOUNT)
  --label NAME   filesystem label (default $LABEL); the fstab entry uses LABEL=
  --force        wipe a device that carries another filesystem signature
  --list         print lsblk and exit
EOF
}

log() { echo "[disk-cache] $*"; }
die() { echo "[disk-cache] error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --device) DEVICE="$2"; shift 2 ;;
    --mount) MOUNT="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    --list) lsblk -o NAME,SIZE,TYPE,ROTA,FSTYPE,LABEL,MOUNTPOINT; exit 0 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (try --help)" ;;
  esac
done

# 1. Already mounted from our label: nothing to do.
if findmnt -n "$MOUNT" >/dev/null 2>&1; then
  src=$(findmnt -n -o SOURCE "$MOUNT")
  if [[ "$(sudo blkid -s LABEL -o value "$src" 2>/dev/null || true)" == "$LABEL" ]]; then
    log "$MOUNT is already mounted from $src (label $LABEL) -- nothing to do"
    sudo chmod 777 "$MOUNT"
    df -h "$MOUNT"
    exit 0
  fi
  die "$MOUNT is mounted from $src, which does not carry the label $LABEL"
fi

# 2. Pick the device.
root_disk=$(lsblk -no PKNAME "$(findmnt -n -o SOURCE /)")
if [[ -z "$DEVICE" ]]; then
  candidates=()
  while read -r name type rota; do
    [[ "$type" == "disk" && "$rota" == "0" && "$name" != "$root_disk" ]] || continue
    [[ "$(lsblk -n "/dev/$name" | wc -l)" -eq 1 ]] || continue
    [[ -z "$(sudo blkid -o value -s TYPE "/dev/$name" 2>/dev/null || true)" ]] || continue
    candidates+=("/dev/$name")
  done < <(lsblk -dn -o NAME,TYPE,ROTA)
  if [[ ${#candidates[@]} -ne 1 ]]; then
    lsblk -o NAME,SIZE,TYPE,ROTA,FSTYPE,LABEL,MOUNTPOINT
    die "expected exactly one unused SSD, found ${#candidates[@]}: ${candidates[*]:-none}. Pass --device."
  fi
  DEVICE="${candidates[0]}"
fi
[[ -b "$DEVICE" ]] || die "$DEVICE is not a block device"
[[ "$(basename "$DEVICE")" != "$root_disk" ]] || die "$DEVICE holds the root filesystem"
if lsblk -no MOUNTPOINT "$DEVICE" 2>/dev/null | grep -q .; then
  die "$DEVICE (or a child) is mounted. Unmount it first."
fi
[[ "$(lsblk -n "$DEVICE" | wc -l)" -eq 1 ]] || die "$DEVICE has partitions; this script formats whole disks only"

# 3. Format unless our label is already there.
existing_label=$(sudo blkid -s LABEL -o value "$DEVICE" 2>/dev/null || true)
existing_type=$(sudo blkid -s TYPE -o value "$DEVICE" 2>/dev/null || true)
if [[ "$existing_label" == "$LABEL" && "$existing_type" == "$FSTYPE" ]]; then
  log "$DEVICE already carries $FSTYPE label $LABEL -- keeping the filesystem"
elif [[ -n "$existing_type" && $FORCE -ne 1 ]]; then
  die "$DEVICE has a $existing_type filesystem (label '${existing_label}'). Pass --force to wipe it."
else
  log "formatting $DEVICE as $FSTYPE (label $LABEL)"
  sudo wipefs -a "$DEVICE"
  sudo mkfs.ext4 -q -F -L "$LABEL" -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "$DEVICE"
fi

# 4. fstab by label, mount, permissions.
sudo mkdir -p "$MOUNT"
if ! grep -qE "^LABEL=${LABEL}[[:space:]]" /etc/fstab; then
  echo "LABEL=$LABEL $MOUNT $FSTYPE defaults,noatime,nofail 0 2" | sudo tee -a /etc/fstab >/dev/null
  log "fstab entry added"
fi
sudo mount "$MOUNT"
sudo chmod 777 "$MOUNT"
log "mounted $DEVICE at $MOUNT"
df -h "$MOUNT"
```

- [ ] **Step 2: Test on one node, list first**

```bash
ssh oliverr3@amd160.utah.cloudlab.us 'cd ozonedb && bash bench/scripts/setup_disk_cache.sh --list'
```

Expected: `sda` with four partitions and `/`, `sdb` 447.1G with no children.

```bash
ssh oliverr3@amd160.utah.cloudlab.us 'cd ozonedb && bash bench/scripts/setup_disk_cache.sh --device /dev/sdb && bash bench/scripts/setup_disk_cache.sh --device /dev/sdb'
```

Expected: the first run prints `formatting /dev/sdb as ext4 (label ozcache)`, `fstab
entry added`, `mounted /dev/sdb at /tank/cache` and a `df` line of about 440 GB. The
second run prints `nothing to do`. Then
`ssh amd160 'findmnt /tank/cache; touch /tank/cache/x && rm /tank/cache/x'` succeeds
without `sudo`.

- [ ] **Step 3: Write the playbook**

```yaml
# bench/ansible/disk_cache.yml -- format and mount the disk-cache SSD on the
# clients (bench/PLAN-disk-cache.md, Task 7). Idempotent; re-run at will.
#
#   ansible-playbook bench/ansible/disk_cache.yml                 # auto-detect
#   ansible-playbook bench/ansible/disk_cache.yml -e device=/dev/sdb
- name: Format and mount the disk-cache SSD on the CloudLab clients
  hosts: "{{ target | default('clients') }}"
  gather_facts: false
  strategy: free
  vars:
    device: ""
    mount: /tank/cache
    extra_args: ""
  tasks:
    - name: Resolve the repository location on the node
      ansible.builtin.include_tasks: tasks/resolve_dest.yml

    - name: Run setup_disk_cache.sh
      ansible.builtin.shell: >-
        bash -lc 'cd {{ repo_dest | quote }} &&
        bash bench/scripts/setup_disk_cache.sh
        {{ ("--device " ~ device) if device else "" }}
        --mount {{ mount | quote }} {{ extra_args }}'
      register: disk_cache
      changed_when: "'formatting' in disk_cache.stdout"

    - name: Report
      ansible.builtin.debug:
        var: disk_cache.stdout_lines
```

Check `tasks/resolve_dest.yml` sets `repo_dest` the way `bootstrap.yml` and `sync.yml`
consume it; if it needs `repo_src`, add `repo_src: "{{ lookup('env', 'OZONEDB_HOME') }}"`
to `vars` the way `sync.yml` does.

- [ ] **Step 4: Run it on every client**

```bash
ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg ansible-playbook bench/ansible/sync.yml   # ships the script
ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg ansible-playbook bench/ansible/disk_cache.yml -e device=/dev/sdb
```

Expected: 8 hosts `ok`, seven `changed` (amd160 was done in Step 2). Verify:

```bash
for h in amd160 amd126 amd138 amd135 amd166 amd146 amd159 amd133; do ssh oliverr3@$h.utah.cloudlab.us 'echo -n "$(hostname -s) "; findmnt -n -o SOURCE,FSTYPE,SIZE /tank/cache'; done
```

Expected: eight lines `/dev/sdb ext4 440G`.

- [ ] **Step 5: Document and commit**

Add to `CLAUDE.md` under "Environment", after `setup_zfs.sh`:
`bash bench/scripts/setup_disk_cache.sh --device /dev/sdb   # ext4 label ozcache at /tank/cache (disk-cache tier)`
and one sentence to `bench/ansible/README.md` about `disk_cache.yml`.

```bash
git add bench/scripts/setup_disk_cache.sh bench/ansible/disk_cache.yml bench/ansible/README.md CLAUDE.md
git commit -m "bench: setup_disk_cache.sh formats the unused SATA SSD as ext4 at /tank/cache; ansible disk_cache.yml"
```

---

### Task 8: Runner flags — `--disk-cache-bytes`, `--disk-cache-dir`, `--disk-cache-keep-pages`

**Files:**
- Modify: `bench/scripts/local/load_local_ycsb_multiproc.py` (helpers at `:167-239`,
  `result_label` `:299-335`, `_make_corfu_config_per_writer` `:338-439`)
- Modify: `bench/scripts/local/run_local_ycsb_multiproc.py` (imports `:10-29`,
  argparse `:407-448`, wiring `:491-497`, prints `:510-519`, `run_ycsb` call `:520-539`)
- Modify: `bench/scripts/local/run_multinode_ycsb.py` (`build_remote_command`
  `:213-250`, both call sites `:683-692` and `:712-721`, `cell_label` `:261-288`,
  argparse `:542-546`)
- Modify: `bench/scripts/local/run_multinode_ycsb_with_corfu.sh` (vars `:79-84`, usage
  `:144-149`, parse `:183-196`, validation `:230-237`, `COMMON_ARGS` `:360-371`)

**Interfaces:**
- Produces (loader module): `DEFAULT_DISK_CACHE_DIR = "/tank/cache"`,
  `size_label_token(n) -> str` (`"512m"`, `"2g"`, `"128k"`, `"1000b"`),
  `disk_cache_corfu_settings(corfu_settings, disk_cache_bytes, disk_cache_dir=None, keep_pages=False) -> dict`,
  `disk_cache_label_token(corfu_settings) -> str` (`""`, `"-dc512m"`, `"-dc2g-kp"`),
  `require_disk_cache_mount(path)` (raises `SystemExit` unless `path` is on a
  filesystem other than `/`, or `OZONEDB_DISK_CACHE_ANY_FS=1`).
- Per-writer config keys: `disk_cache_dir = <dir>/w{i}/` (wiped before each phase),
  `disk_cache_bytes`, `disk_cache_drop_pages`.

- [ ] **Step 1: Write the failing check**

There is no Python test suite; the check is a one-liner run from the repo root on the
Mac (pure-Python helpers, no cluster):

```bash
python3 - <<'PY'
import sys; sys.path.insert(0, "bench/scripts/local")
import load_local_ycsb_multiproc as L
assert L.size_label_token(536870912) == "512m"
assert L.size_label_token(2 << 30) == "2g"
assert L.lru_label_token(536870912) == "lru512m"
s = L.disk_cache_corfu_settings({"lru_cache_bytes": 8388608}, 2 << 30)
assert s["disk_cache_bytes"] == 2 << 30 and s["disk_cache_dir"] == "/tank/cache"
assert L.disk_cache_label_token(s) == "-dc2g"
assert L.disk_cache_label_token(L.disk_cache_corfu_settings(s, 2 << 30, keep_pages=True)) == "-dc2g-kp"
assert L.disk_cache_label_token({}) == ""
assert L.result_label("ozonedb-corfu", L.cache_warm_corfu_settings(s), None, 8388608) == "ozonedb-corfu-lru8m-warm-dc2g"
try:
    L.disk_cache_corfu_settings(s, 0); raise AssertionError("0 accepted")
except ValueError:
    pass
print("ok")
PY
```

Expected now: `AttributeError: module has no attribute 'size_label_token'`.

- [ ] **Step 2: Add the helpers to the loader**

Replace `lru_label_token` (`:219-230`) with:

```python
DEFAULT_DISK_CACHE_DIR = "/tank/cache"


def size_label_token(n):
    """`512m`, `2g`, `128k`, `1000b`: a byte count as a filename token. Exact
    powers of 1024 stay short; anything else is bytes, so two different sizes
    can never share a label."""
    n = int(n)
    if n % (1 << 30) == 0:
        return f"{n >> 30}g"
    if n % (1 << 20) == 0:
        return f"{n >> 20}m"
    if n % (1 << 10) == 0:
        return f"{n >> 10}k"
    return f"{n}b"


def lru_label_token(lru_cache_bytes):
    """`lru512m`, `lru64m`, `lru128k`, `lru1000b` (see size_label_token)."""
    return "lru" + size_label_token(lru_cache_bytes)


def disk_cache_corfu_settings(corfu_settings, disk_cache_bytes, disk_cache_dir=None, keep_pages=False):
    """`--disk-cache-bytes N [--disk-cache-dir DIR] [--disk-cache-keep-pages]`
    as corfu settings. Every writer gets `<DIR>/w{i}/`, wiped before each
    phase (bench/PLAN-disk-cache.md, Task 8)."""
    n = int(disk_cache_bytes)
    if n <= 0:
        raise ValueError("--disk-cache-bytes must be a positive byte count")
    s = dict(corfu_settings or {})
    s["disk_cache_bytes"] = n
    s["disk_cache_dir"] = disk_cache_dir or s.get("disk_cache_dir") or DEFAULT_DISK_CACHE_DIR
    s["disk_cache_drop_pages"] = not keep_pages
    return s


def disk_cache_label_token(corfu_settings):
    """`-dc512m`, `-dc2g-kp` (page cache kept), or `` when the tier is off."""
    s = corfu_settings or {}
    if not s.get("disk_cache_bytes"):
        return ""
    token = "-dc" + size_label_token(s["disk_cache_bytes"])
    if not _truthy(s.get("disk_cache_drop_pages", True)):
        token += "-kp"
    return token


def require_disk_cache_mount(path):
    """Refuses a disk-cache dir that lives on the root filesystem: the
    experiment measures the SSD, not the OS disk. OZONEDB_DISK_CACHE_ANY_FS=1
    overrides (single-node tests)."""
    if os.environ.get("OZONEDB_DISK_CACHE_ANY_FS") == "1":
        return
    os.makedirs(path, exist_ok=True)
    if os.stat(path).st_dev == os.stat("/").st_dev:
        sys.exit(f"disk cache dir {path} is on the root filesystem; run bench/scripts/setup_disk_cache.sh (or set OZONEDB_DISK_CACHE_ANY_FS=1)")
```

`_truthy` is at `:129`; it must accept Python `bool` (check its body; if it only handles
strings, extend it with `if isinstance(v, bool): return v`).

In `result_label` (`:334`), after `label += cache_warm_label_token(corfu_settings)`:

```python
        label += disk_cache_label_token(corfu_settings)
```

In `_make_corfu_config_per_writer`, after the `cache_warm_*` loop (`:392`):

```python
    dc_bytes = int(corfu_settings.get("disk_cache_bytes") or 0)
    if dc_bytes > 0:
        # One directory per writer process, cold at every phase: the bucket is
        # restored between cells, so a copy from the previous cell may carry a
        # name the new cell re-creates with other bytes.
        dc_root = corfu_settings.get("disk_cache_dir") or DEFAULT_DISK_CACHE_DIR
        require_disk_cache_mount(dc_root)
        dc_dir = os.path.join(dc_root, f"w{writer_idx}") + "/"
        shutil.rmtree(dc_dir, ignore_errors=True)
        os.makedirs(dc_dir, exist_ok=True)
        data["disk_cache_dir"] = dc_dir
        data["disk_cache_bytes"] = dc_bytes
        data["disk_cache_drop_pages"] = "true" if _truthy(corfu_settings.get("disk_cache_drop_pages", True)) else "false"
    else:
        for key in ("disk_cache_dir", "disk_cache_bytes", "disk_cache_drop_pages", "disk_cache_chunk_bytes", "disk_cache_fill_queue"):
            data.pop(key, None)
```

(`import shutil` at the top if absent.) Add the loader CLI flags next to
`--cache-warm-max-fraction` (`:886-890`):

```python
    parser.add_argument("--disk-cache-bytes", type=int, default=None,
                        help="Disk-cache tier capacity per writer in bytes (bench/PLAN-disk-cache.md); off when absent")
    parser.add_argument("--disk-cache-dir", default=None,
                        help=f"Root of the per-writer disk-cache dirs (default {DEFAULT_DISK_CACHE_DIR}, the SSD mounted by setup_disk_cache.sh)")
    parser.add_argument("--disk-cache-keep-pages", action="store_true",
                        help="Do not drop the page cache after tier reads (A/B of the SSD cost)")
```

and after the `--cache-warm` wiring (`:955-958`):

```python
    if args.disk_cache_bytes is not None:
        corfu_settings = disk_cache_corfu_settings(corfu_settings, args.disk_cache_bytes, args.disk_cache_dir, args.disk_cache_keep_pages)
```

- [ ] **Step 3: The run-phase runner**

`run_local_ycsb_multiproc.py`: add `disk_cache_corfu_settings` and
`disk_cache_label_token` to the import list (`:10-29`); add the same three argparse
flags after `--cache-warm-max-fraction` (`:420-424`); after the cache-warm wiring
(`:491-497`) apply `disk_cache_corfu_settings` exactly as in the loader; add
`disk_cache=<token or off>` to the status print (`:510-519`).

- [ ] **Step 4: The multinode fan-out**

`run_multinode_ycsb.py`: extend `build_remote_command` with
`disk_cache_bytes=None, disk_cache_dir=None, disk_cache_keep_pages=False` and append

```python
        + (["--disk-cache-bytes", str(int(disk_cache_bytes))] if disk_cache_bytes is not None else [])
        + (["--disk-cache-dir", disk_cache_dir] if disk_cache_dir else [])
        + (["--disk-cache-keep-pages"] if disk_cache_keep_pages else [])
```

to the flag chain (`:246`). Pass the three from `args` at both call sites (`:683-692`
and `:712-721`). In `cell_label` (`:275-278`), after the cache-warm settings:

```python
        if args.disk_cache_bytes is not None:
            corfu = disk_cache_corfu_settings(corfu, args.disk_cache_bytes, args.disk_cache_dir, args.disk_cache_keep_pages)
```

(import it). Argparse after `--cache-warm-max-fraction` (`:542-546`): the same three
flags.

- [ ] **Step 5: The shell wrapper**

`run_multinode_ycsb_with_corfu.sh`: vars after `:81`:

```bash
DISK_CACHE_BYTES=""
DISK_CACHE_DIR=""
DISK_CACHE_KEEP_PAGES=0
```

usage after `:147`:

```
  --disk-cache-bytes N     Disk-cache tier capacity per writer (bytes); label token -dc<size>
  --disk-cache-dir DIR     Root of the per-writer tier dirs (default /tank/cache)
  --disk-cache-keep-pages  Keep the page cache after tier reads (label -kp)
```

parse after `:185`:

```bash
    --disk-cache-bytes) DISK_CACHE_BYTES="$2"; shift 2 ;;
    --disk-cache-dir) DISK_CACHE_DIR="$2"; shift 2 ;;
    --disk-cache-keep-pages) DISK_CACHE_KEEP_PAGES=1; shift ;;
```

validation after `:233` (copy the `--lru-cache-bytes` regex check), and `COMMON_ARGS`
after `:370`:

```bash
[[ -n "$DISK_CACHE_BYTES" ]] && COMMON_ARGS+=(--disk-cache-bytes "$DISK_CACHE_BYTES")
[[ -n "$DISK_CACHE_BYTES" && -n "$DISK_CACHE_DIR" ]] && COMMON_ARGS+=(--disk-cache-dir "$DISK_CACHE_DIR")
[[ -n "$DISK_CACHE_BYTES" && "$DISK_CACHE_KEEP_PAGES" -eq 1 ]] && COMMON_ARGS+=(--disk-cache-keep-pages)
```

- [ ] **Step 6: Run the checks**

The Step 1 one-liner prints `ok`. Then a dry run:

```bash
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 8388608 --disk-cache-bytes 2147483648 --workloads c --writers-list 1 --trial 1 --duration 600 --run-tag dc-dry --dry-run
```

Expected: the printed remote command carries `--disk-cache-bytes 2147483648` and the
cell name contains `ozonedb-corfu-lru8m-dc2g`.

- [ ] **Step 7: Commit**

```bash
git add bench/scripts/local/load_local_ycsb_multiproc.py bench/scripts/local/run_local_ycsb_multiproc.py bench/scripts/local/run_multinode_ycsb.py bench/scripts/local/run_multinode_ycsb_with_corfu.sh
git commit -m "bench: --disk-cache-bytes/--disk-cache-dir/--disk-cache-keep-pages through the three runner layers, -dc<size> label, per-writer dir wiped per phase, mount guard"
```

---

### Task 9: Extractor columns

**Files:**
- Modify: `bench/scripts/extract_cost_coefficients.py` (`DISK_RE` after `:79`, parse
  after `:181`, aggregation in `build_row`, `COLUMNS` `:622-645`, `LABEL_RE` `:120`)

**Interfaces:**
- Produces TSV columns `disk_hits, disk_misses, disk_h, disk_hit_bytes, disk_miss_bytes,
  disk_fills, disk_fill_bytes, disk_fill_gets, disk_fill_gets_per_op, disk_evictions,
  disk_invalidated, disk_files, disk_bytes, disk_capacity, disk_ratio, h_total`
  (sums over the writer files; `disk_h = hits / (hits + misses)`;
  `h_total = 1 - (1 - h) * (1 - disk_h)`; `disk_ratio = disk_capacity_per_writer /
  dataset_bytes`; `disk_fill_gets_per_op = fill_gets / ops`).

- [ ] **Step 1: Write the failing check**

Create a synthetic writer result file under the job tmp dir with the YCSB lines the
extractor already parses (copy one real `.result` from
`bench/results/local/cost-20260828-cache3-long/`) and append:

```
[disk_cache] hits=900 misses=100 hit_bytes=3686400 miss_bytes=409600 passthrough=3 fills=12 fill_bytes=805306368 fill_gets=12 fill_skipped_budget=0 fill_skipped_present=1 fill_gone=0 fill_failed=0 fill_dropped=0 writethrough_files=4 evictions=2 evicted_bytes=134217728 invalidated=6 files=14 bytes=939524096 capacity=2147483648
```

Run the extractor over that directory with `--tsv` and check the columns:
`disk_hits=900`, `disk_misses=100`, `disk_h=0.9`, `disk_fill_gets=12`, `disk_files=14`.
Expected now: the columns do not exist.

- [ ] **Step 2: Implement**

```python
DISK_RE = re.compile(
    r"\[disk_cache\] hits=(?P<hits>\d+) misses=(?P<misses>\d+) hit_bytes=(?P<hit_bytes>\d+)"
    r" miss_bytes=(?P<miss_bytes>\d+) passthrough=(?P<passthrough>\d+) fills=(?P<fills>\d+)"
    r" fill_bytes=(?P<fill_bytes>\d+) fill_gets=(?P<fill_gets>\d+)"
    r" fill_skipped_budget=(?P<fill_skipped_budget>\d+) fill_skipped_present=(?P<fill_skipped_present>\d+)"
    r" fill_gone=(?P<fill_gone>\d+) fill_failed=(?P<fill_failed>\d+) fill_dropped=(?P<fill_dropped>\d+)"
    r" writethrough_files=(?P<writethrough_files>\d+) evictions=(?P<evictions>\d+)"
    r" evicted_bytes=(?P<evicted_bytes>\d+) invalidated=(?P<invalidated>\d+) files=(?P<files>\d+)"
    r" bytes=(?P<bytes>\d+) capacity=(?P<capacity>\d+)"
)
```

In the per-writer loop (after the `LEVELS_RE` block, `:175-181`): `m = DISK_RE.search(line)`
→ `w["disk"] = {k: int(v) for k, v in m.groupdict().items()}`. In `build_row`, sum every
key over the writers that have `disk`, then derive `disk_h`, `h_total`, `disk_ratio`
(`capacity` of one writer over `dataset_bytes`) and `disk_fill_gets_per_op`; leave the
columns empty when no writer printed the line. Append the sixteen names to `COLUMNS`
after `cache_ratio`. Extend `LABEL_RE` so `-dc(\d+)([gmkb])(-kp)?` is tolerated in
labels (the label parser must not reject the new token).

- [ ] **Step 3: Run the check from Step 1, then the real extractor over the cache3 tag**

```bash
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/cost-20260828-cache3-long --window 60 --tsv /tmp/x.tsv
```

Expected: identical rows to before (empty disk columns), no error.

- [ ] **Step 4: Commit**

```bash
git add bench/scripts/extract_cost_coefficients.py
git commit -m "bench: extractor parses the [disk_cache] line -- disk_h, h_total, fill GETs per op, disk_ratio"
```

---

### Task 10: Cluster build, unit tests, smoke, campaign `disk-20260829`

**Files:**
- Create: `bench/results-disk-20260829.tsv` (the extractor's rows for the tag)
- Job tmp scripts (`launch.sh`, a chain script, `moncc.sh`), not committed.

**Interfaces:**
- Consumes: everything above, synced and built on all 8 clients.

- [ ] **Step 1: Check the cluster is free, sync and build**

```bash
for h in amd127 amd160; do ssh oliverr3@$h.utah.cloudlab.us 'pgrep -af "run_local_ycsb|ycsb|corfu_server" | grep -v pgrep | head'; done
ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg ansible-playbook bench/ansible/sync.yml -e build=true
```

Expected: no foreign drivers; `PLAY RECAP` 8 ok, 0 failed; on every client
`find build -maxdepth 1 -name 'lib*.so' -mmin -15 | wc -l` is 2.

- [ ] **Step 2: Unit tests on amd160**

```bash
ssh oliverr3@amd160.utah.cloudlab.us 'cd ozonedb && cmake --build build --target runUnitTests -j"$(nproc)" >/tmp/ut.log 2>&1 && cd build && ./runUnitTests --gtest_filter="DiskCacheStorageTest.*:LRUCacheTest.*:SSTableTest.*:StorageTest.*:CheckpointTest.*"'
```

Expected: all PASSED.

- [ ] **Step 3: Single-node smoke against the cluster's MinIO**

On amd160, with the 1 GB load snapshot restored on amd127 (the wrapper does this per
cell; for the smoke use `run_multinode_ycsb_with_corfu.sh` with one client host):

```bash
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 8388608 --disk-cache-bytes 2147483648 --workloads c --writers-list 1 --trial 1 --duration 120 --run-tag disk-smoke <client-host option>
```

(`<client-host option>` is the wrapper's flag that sets `CLIENT_HOSTS` at
`run_multinode_ycsb_with_corfu.sh:66`; read the `usage()` text for its name. Without it
the smoke runs on all 8 clients, which is also acceptable.)

Expected in the writer's `.result`: `[disk_cache] reconciled 0 files, removed 0` at
open; at close `hits` ≫ `misses`, `fills` equal to the number of SSTables (about 16
for a 1 GB dataset at 64 MiB files), `fill_failed=0`, `passthrough` small (the
checkpoint reads), YCSB `failed=0`. On amd160 `du -sh /tank/cache/w0` is about 1 GB
and `ls /tank/cache/w0/sstable*/*.part` finds nothing.

- [ ] **Step 4: The campaign chain**

Eight cells, 8 writers, 600 s, `--log-trim`, tag `disk-20260829-long`, run through
`launch.sh` and watched with `moncc.sh` as in the earlier campaigns:

| # | Workload | `--lru-cache-bytes` | `--disk-cache-bytes` | Extra | Purpose |
|---|---|---|---|---|---|
| 1 | c | 8388608 | 134217728 (128 MB, 12.5 %) | | `h_total(ratio)` point |
| 2 | c | 8388608 | 268435456 (256 MB, 25 %) | | point |
| 3 | c | 8388608 | 536870912 (512 MB, 52 %) | | point |
| 4 | c | 8388608 | 2147483648 (2 GB, all) | | ceiling, disk-hit CPU |
| 5 | c | 8388608 | 2147483648 | `--disk-cache-keep-pages` | page-cache A/B |
| 6 | a | 8388608 | 2147483648 | | write mix, tier only |
| 7 | a | 536870912 | 2147483648 | | RAM + tier |
| 8 | a | 8388608 | 536870912 | | write mix at 52 % |

Controls exist: workload c at `lru8m` and `lru512m` (range-read campaign), workload a
at `lru8m` (2,600 ops/s) and `lru512m` (2,724 / 2,894 ops/s) (`cost-20260828-cache*`).
Each cell is about 11 minutes with the Corfu restart: about 1.5 h.

- [ ] **Step 5: Extract, commit the rows**

```bash
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/disk-20260829-long bench/results/local --window 60 --tsv bench/results-disk-20260829.tsv
```

Check every row: `failed=0`, `have=8`, `disk_fill_failed=0`, `disk_fill_dropped` small.

```bash
git add bench/results-disk-20260829.tsv
git commit -m "bench: disk-cache campaign disk-20260829 rows"
```

---

### Task 11: Cost model `disk_gb` term, write-up

**Files:**
- Modify: `bench/scripts/plot/plot_cost_model.py` (`Coefficients` `:78-210`,
  `Model.ozonedb` `:255-280`, the figure and the TSV table `:374-379`)
- Modify: `bench/scripts/plot/prices.json` (`projection.disk_gb_per_client`)
- Modify: `bench/RESULTS-cost.md` (new section before "Method notes and caveats"),
  this file's status paragraph, `bench/PLAN-cost.md` status paragraph

**Interfaces:**
- Produces: `Coefficients.h_disk(ratio)` (log-linear interpolation over the
  `(disk_ratio, h_total)` points of the workload-c rows with `disk_capacity > 0`),
  `Coefficients.cpu_O_disk` (client CPU per op of cell 4), `Coefficients.fill_get_per_op`
  (cell 4, last 60 s), `Model.ozonedb(d_bytes, cache_gb, trimming=True, disk_gb=0)`.

- [ ] **Step 1: Write the failing check**

```bash
python3 - <<'PY'
import json, sys
sys.path.insert(0, "bench/scripts/plot")
import plot_cost_model as pcm
prices = json.load(open("bench/scripts/plot/prices.json"))
space = {k: v for k, v in json.load(open("bench/scripts/plot/space.json")).items() if not k.startswith("_")}
rows = pcm.load_rows("bench/results-disk-20260829.tsv") + pcm.load_rows("bench/results-cost-20260828-cache.tsv")
coef = pcm.Coefficients(rows, space, "c", read_fraction=0.5)
assert len(coef.h_disk_points) == 4, coef.h_disk_points  # cells 1-4; the -kp row is excluded
m = pcm.Model(coef, prices)
base = m.ozonedb(10_000 * pcm.GB, 16)
disk = m.ozonedb(10_000 * pcm.GB, 16, disk_gb=2000)
assert disk["h"] > base["h"] and disk["disk_cost"] > 0 and disk["total"] < base["total"], (base, disk)
print(round(base["total"]), round(disk["total"]))
PY
```

Expected now: `TypeError: unexpected keyword 'disk_gb'`.

- [ ] **Step 2: Implement**

In `Coefficients.__init__`: collect `h_disk_points = sorted((r["disk_ratio"], r["h_total"]) for r in rows if r["workload"] == "c" and r.get("disk_capacity") and not r["label"].endswith("-kp"))`, `cpu_O_disk` and `fill_get_per_op` from the row with the largest `disk_ratio`; `h_disk(ratio)` = the same interpolation as `h()` over those points, clamped at the ends. In `Model.ozonedb`, add `disk_gb=0`:

```python
        if disk_gb > 0:
            h = self.c.h_disk(disk_gb * GB / d_bytes)
            cpu = self.c.cpu_O_disk
            fill = self.R * self.c.fill_get_per_op * self.S
        else:
            h = self.c.h(cache_gb * GB / d_bytes)
            cpu = self.c.cpu_O
            fill = 0.0
        gets = (self.R * (1.0 - h) * self.c.g + self.W * self.c.get_per_write) * self.S + fill
        ...
        nc, client_cost = self.clients(cpu, "ozonedb_client")
        disk_cost = nc * disk_gb * st["gp3_usd_gb_month"]
        total = log_tier + bulk + req + client_cost + disk_cost
```

and return `disk_cost` and `disk_gb` in the dict. `prices.json`: `"disk_gb_per_client": 2000` under `projection`. The figure gains a third OzoneDB line, "16 GB RAM + 2 TB gp3 per client", and the TSV table a `disk_gb` column. Rerun the plotter over both TSVs and check the figure renders.

- [ ] **Step 3: Write the results section**

In `RESULTS-cost.md`, before "Method notes and caveats", a section
"## Disk-cache tier (campaign `disk-20260829`, engine commits `<first>` to `<last>`)"
with: what the tier is (three sentences), the load, the workload-c table (cell, ops/s,
`h`, `disk_h`, `h_total`, GETs per op run / last 60 s, fill GETs per op, fills,
evictions, client CPU per op, server CPU per op, RSS), the workload-a table against the
`lru8m` / `lru512m` controls, the page-cache A/B (cell 4 against 5: CPU per op and
throughput), findings against the "Goal in numbers" table, the projection table at
10 TB and 100 TB with and without 2 TB per client, and the caveats (whole-file fills,
uniform keys, SATA not NVMe, the tier is cold at cell start). Update this file's status
paragraph (commits, campaign, numbers against goal, deviations) and the `PLAN-cost.md`
status paragraph.

- [ ] **Step 4: Commit and push**

```bash
git add bench/scripts/plot/plot_cost_model.py bench/scripts/plot/prices.json bench/RESULTS-cost.md bench/PLAN-disk-cache.md bench/PLAN-cost.md
git commit -m "bench: cost model disk_gb term from the disk-20260829 coefficients; RESULTS section for the disk-cache tier"
git push origin worktree-plan-cost
```

---

### Task 12 (optional): nginx read-through cross-check

**Files:** job tmp only (an nginx config and a chain script); nothing committed unless
the numbers go into `RESULTS-cost.md`.

Purpose: a second, code-free measurement of "GETs saved by a local disk" at 1 MB slice
granularity, to bound the whole-file design's fill overhead.

- [ ] **Step 1:** on every client `sudo apt-get install -y nginx-core` and write
  `/etc/nginx/conf.d/s3cache.conf`:

```nginx
proxy_cache_path /tank/cache/nginx levels=1:2 keys_zone=s3:64m max_size=2g inactive=1d use_temp_path=off;
server {
  listen 127.0.0.1:8090;
  location / {
    proxy_pass http://10.10.1.1:9000;
    proxy_set_header Host $http_host;
    slice 1m;
    proxy_set_header Range $slice_range;
    proxy_cache s3;
    proxy_cache_key $uri$slice_range;
    proxy_cache_valid 200 206 1y;
    proxy_cache_methods GET HEAD;
    proxy_cache_bypass $arg_nocache;
    proxy_no_cache $is_args;
    add_header X-Cache $upstream_cache_status;
  }
  location ~ /checkpoint/ { proxy_pass http://10.10.1.1:9000; proxy_set_header Host $http_host; }
}
```

- [ ] **Step 2:** run cell 4's configuration with `s3.endpoint` overridden per writer to
  `http://127.0.0.1:8090` (a `--s3-endpoint` flag is not in the runners; edit the
  generated `shared_config_w0.json` on the clients for this one cell, or add the flag if
  the check is repeated) and `--disk-cache-bytes` absent.
- [ ] **Step 3:** compare the MinIO GET count of the last 60 s with cell 4's, and
  `grep -c 'HIT' /var/log/nginx/access.log` against `MISS`. Record the pair in the
  RESULTS section as a footnote. Remove the nginx config afterwards (`apt-get remove
  nginx-core`), so the clients return to the plain state.

---

## Time budget

| Task | Estimate |
|---|---|
| 1-5 tier + tests | 1 day |
| 6 config + wiring | 2 h |
| 7 provisioning | 1 h (plus one node reboot test: optional, `nofail` covers it) |
| 8 runner flags | 2 h |
| 9 extractor | 1 h |
| 10 build, smoke, campaign | 3 h, mostly waiting |
| 11 model + write-up | 2 h |
| 12 nginx cross-check | 2 h, optional |

Two and a half days end to end.

## What to watch

- **Thrash under a capped tier.** Uniform keys and whole-file LRU: at 25 % ratio every
  miss queues a fill that evicts a file another miss just queued. The bound is the one
  worker (about one 64 MiB file per second from MinIO), so `fill_dropped` will be large
  and `disk_h` will sit near the ratio. That is the expected behaviour and the number the
  model needs; if `fill_gets_per_op` in the last 60 s exceeds 0.001, report it as a
  finding and consider admission on the second miss.
- **The page cache.** Cell 5 against cell 4 tells how much of the SSD cost the fadvise
  actually exposes. If they are equal, `POSIX_FADV_DONTNEED` did not drop the pages
  (dirty pages of a fresh fill are not dropped until written back); add
  `fdatasync` before the fadvise in `publishPart` if so.
- **Reads of a file being filled.** They go to the backing until the rename; expect
  `misses` of one file to continue for about a second after its first miss.
- **`size()` from the tier.** `Table::open` calls `size` before the footer read; with
  a complete copy it is answered locally, so a cold open of a cached file costs zero
  requests. Check `s3_head` in the cell rows drops accordingly.
- **Aliasing.** `Metadata` throws when `disk_cache_dir` is set without
  `sstable_backend`; the runners never generate that combination.
- **ENOSPC.** The budget (2 GB) is far below the 440 GB filesystem; a `fill_failed`
  count above zero means something else (a read failure against MinIO).
- **Shared cluster.** The chain restarts Corfu and restores the bucket per cell as the
  earlier campaigns did; check for other sessions' drivers first.

## Follow-ups (not in this plan)

- Admission control (fill on the second miss within N seconds) if thrash dominates.
- Sharing one tier across the writer processes of a node (a lock file per entry).
- Block-granular tier (CacheLib) if the product needs DRAM/flash in one library.
- NVMe instead of SATA: the CloudLab `c6525-25g`/`r650` types have NVMe; the plan's
  numbers are SATA numbers.
