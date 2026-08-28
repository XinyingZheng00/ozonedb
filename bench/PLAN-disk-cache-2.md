# Plan: admission control and sub-file entries for the disk-cache tier (branch `worktree-plan-cost`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a disk-cache tier that is smaller than the dataset stop losing to no tier at all, by two changes to `DiskCacheStorage`: TinyLFU admission control, and chunk entries in place of whole-file entries.

**Architecture:** `DiskCacheStorage` (`src/db/disk_cache_storage.cpp`) gains a frequency sketch that every cacheable read records, and an admission decision that a fill or a write-through must win before it takes budget from a resident entry. A second mode, `chunk`, stores fixed-size chunks of each SSTable in one sparse local file, fetches the chunks that cover a missed range on the caller's thread, and evicts chunk by chunk with a CLOCK hand and `fallocate(PUNCH_HOLE)`. The bench chain, the extractor and the cost model learn the new switches, and one campaign measures both changes against the round-1 cells.

**Tech Stack:** C++17 (GCC 13.3 on the nodes), gtest, POSIX `pread`/`pwrite`/`fallocate`, Python 3 bench scripts, matplotlib for the projection.

**Spec:** The "Design" section of this file. It argues from the measurement in `bench/RESULTS-cost.md`, section "Disk-cache tier", and from `bench/PLAN-disk-cache.md`. There is no separate spec file.

---

## Global constraints

- East const, Google style, `ColumnLimit: 0`, everything in `namespace ozonedb`. The public
  API returns `Status` and never throws, except constructors on a configuration error
  (`CLAUDE.md`, "Conventions"; `DiskCacheStorage`'s constructor already throws).
- The nodes run Ubuntu 24.04, GCC 13.3, CMake 3.28 (ruling in the round-1 ledger). The tier
  is Linux-only code already (`posix_fadvise`). `fallocate` with `FALLOC_FL_PUNCH_HOLE` is
  allowed. No new vcpkg port, no new system package.
- The user builds remotely. Never run `cmake` or `make` on the macOS checkout. The test
  chain for one node is in "How to build and test" below.
- Unit tests run from `build/` and need a writable `/tank`. New test files go in
  `OZONEDB_TEST_SOURCES` (`CMakeLists.txt:218-228`).
- Lock rule from `cache.h`: nothing that takes a cache mutex runs under `view_mutex`. The
  tier's mutexes follow the same rule. The lock order in `disk_cache_storage.h` is
  documentation that must stay true. New locks are added to that comment.
- The cluster is shared. Check for another session's drivers before a sync, a load or a
  Corfu restart. Kill only your own pids. Launch a campaign chain detached (the harness kills
  a background chain that outlives the turn).
- Config values are strings (`parseJSON`). Every new key is parsed in `Metadata`'s
  constructor with the idioms at `metadata.h:281-302`.
- Do not touch the user's untracked `PLAN-range-read.md` in the worktree root.
- Runner flags select an experiment, never an edit of `ycsb.yaml`. A flag reaches the
  clients through `run_multinode_ycsb_with_corfu.sh` → `run_multinode_ycsb.py` →
  `run_local_ycsb_multiproc.py` → the generated `shared_config_w{i}.json`.
- The `[disk_cache]` stats line is append-only. New fields go at the end, in the order this
  plan gives. The extractor's `DISK_RE` must still parse the round-1 result files.
- The round-1 defaults (`disk_cache_mode = file`, `disk_cache_admission = always`) stay the
  defaults until Task 8 flips them on measured evidence. Every existing test must pass
  unchanged until then.
- Label tokens are appended in this order: `-dc<size>`, `-ch<entry>`, `-adm`, `-kp`.

---

## Design

### Why the round-1 tier loses below the dataset size

From `bench/results-disk-20260829.tsv`, workload c, 8 MB RAM cache, 8 clients, 600 s:

| Tier | Demand bytes from S3 | Fill bytes | MinIO egress, steady | Client CPU per op | ops/s |
|---|---|---|---|---|---|
| 512 MB (ratio 0.52) | 12.0 GB | 1,198 GB | 2,061 MB/s | 1.22 ms | 8,744 |
| 256 MB (ratio 0.26) | 12.9 GB | 1,199 GB | 2,025 MB/s | 1.56 ms | 6,646 |
| 2 GB (full) | 0.2 GB | 9.7 GB | 0 MB/s | 0.12 ms | 46,741 |
| none (control) | ~13 GB | 0 | ~330 MB/s | 0.42 ms | 6,700–7,057 |

The unit of the tier is a whole SSTable of about 120 MB. A miss on one 4 KiB block queues
a fill of the whole file. Under YCSB's hashed keys every file is equally hot, so a budget
that holds 4 of 10 files rotates its content every few seconds: `fills` equals
`evictions` to within 0.3 %. The fill worker never stops, moves about 250 MB/s per client,
and pushes MinIO to 2 GB/s. The demand reads that still miss queue behind that traffic.
The tier moves 100x the bytes the workload asked for, and on workload a with a 512 MB
budget it is slower than no tier (2,469 against 2,600 ops/s, 4.2 ms of CPU per op).

### The two changes

**1. Admission control (TinyLFU).** A `FrequencySketch` (count-min, four rows of 8-bit
counters, halved every `window` samples) records every cacheable read, hit or miss. When a
candidate needs budget that is not free, it is admitted only if its estimated frequency is
strictly above the frequency of the entry the eviction policy names as the victim. Under
uniform keys the candidate and the victim tie, so nothing is admitted once the tier is full,
and the fill traffic stops. Under a skewed distribution a hot absent entry overtakes a cold
resident one. A write-through (a compaction output this process built) has no access
history, so under frequency admission it is admitted only into free budget. Free budget is
always taken without a contest.

**2. Chunk entries.** In `chunk` mode the tier keeps, for each SSTable, one sparse local
file and a per-chunk state byte. A ranged read that misses fetches the chunk-aligned range
that covers the request from the backing in **one** ranged GET, returns the requested
slice, writes the absent chunks at their own offsets with `pwrite`, and marks them present.
There is no fill worker in chunk mode: the fill is the demand read. The demand GET count
per miss stays 1, and the bytes per miss are one to two chunks instead of one file. Eviction
is per chunk with a CLOCK hand over the files (one byte per chunk: present, referenced,
pending) and `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`, which returns the
SSD space and leaves the file size unchanged. Full-file reads (compaction inputs) are
served locally only when every chunk is present, and are never admitted.

Entry size. Data blocks are 4 KiB (`table_builder.cpp`, `BLOCK_SIZE 4096`). The default
entry is 64 KiB. At the control's miss rate (0.76 GETs per op, about 665 misses/s per
client) that is 42 MB/s per client and 340 MB/s across the cluster, one sixth of the
round-1 fill traffic, with no extra GETs. The CLOCK state costs one byte per chunk:
32 MB for a 2 TB tier at 64 KiB. Task 7 measures 16 KiB and 256 KiB too.

### Decisions, with the alternatives that lost

| Decision | Alternative rejected | Why |
|---|---|---|
| CLOCK with one state byte per chunk | LRU list of present chunks | 32M nodes at 2 TB / 64 KiB is 1.5 GB of RAM. CLOCK is 32 MB and approximates LRU. |
| One sparse file per SSTable | One local file per chunk | 32M small files per tier. Sparse files keep the offsets and one `open` per read. |
| Fetch the whole covering range on a miss, present chunks included | Fetch only the absent chunks | One GET either way. A straddle is 1 in 16 misses at 4 KiB blocks and 64 KiB chunks. |
| No single-flight per chunk | A per-chunk in-flight map | Two concurrent misses write the same immutable bytes at the same offset. The second sees `pending` and skips the write. |
| Reconcile in chunk mode deletes every local file | `SEEK_DATA`/`SEEK_HOLE` recovery | The bench wipes the tier per phase. Warm restarts are a follow-up. |
| `open`/`pread`/`close` per read | An fd cache | A few microseconds per read. A follow-up if the full-tier cell regresses. |
| Write-through under frequency admission takes free budget only | Contest with a synthetic frequency | An output has no history. Under `always` it evicts, as in round 1. |
| Admission is checked before a byte moves | Check at publish time | The point is to stop the transfer, not the rename. |

Out of scope: a shared cross-client tier, a skewed-key workload cell (the runner cannot set
YCSB's `requestdistribution`), the Cassandra side of the projection.

### Config keys (`Metadata`)

| Key | Default | Meaning |
|---|---|---|
| `disk_cache_mode` | `file` | `file` (round 1) or `chunk` |
| `disk_cache_entry_bytes` | `65536` | chunk size in `chunk` mode. A power of two, at least 4096. |
| `disk_cache_admission` | `always` | `always` (round 1) or `frequency` (TinyLFU) |
| `disk_cache_admit_window` | `0` | sketch aging window in samples. `0` = 8 x the entries the budget holds. |

### Stats line, final form

Round 1 printed twenty fields. This plan appends six, in this order, after `capacity=`:

```
fetch_bytes=N admit_rejected=N entries=N mode=file|chunk entry_bytes=N punch_failed=N
```

`fetch_bytes` counts every byte the tier pulled from the backing for cacheable names:
demand reads plus fills in `file` mode, the covering ranges in `chunk` mode. The
amplification of a cell is `disk_amp = fetch_bytes / miss_bytes`. It is about 100 for the
round-1 partial tier. In `chunk` mode with 64 KiB chunks and 4 KiB blocks it is 16 to 32 by
construction, so the absolute egress is the number to compare, not the ratio.
`admit_rejected` counts fills, chunks and write-throughs that admission refused.
`entries` is `files` in `file` mode and the count of present chunks in `chunk` mode.

### Runner flags and label tokens

`--disk-cache-mode file|chunk`, `--disk-cache-entry-bytes N`,
`--disk-cache-admission always|frequency`, on all three runner layers. Labels:
`-dc512m` (round 1, unchanged), `-dc512m-adm`, `-dc512m-ch64k`, `-dc512m-ch64k-adm`,
`-dc512m-ch16k-adm`, `-dc2g-ch64k-adm-kp`.

### Goal in numbers

Dataset 1 GB (1 M records), 8 writers on 8 clients, 600 s cells, trimming on, MinIO on the
log node, 4 KiB blocks, RAM block cache 8 MB. Controls: no tier c 6,700 ops/s, 0.42 ms CPU
per op, 0.76 GETs per op; no tier a 2,600 ops/s, 1.18 ms per op; round-1 file tier c 512 MB
8,744 ops/s, 1.22 ms per op, 1,198 GB of fills, 2,061 MB/s egress.

| Cell | Metric | Goal |
|---|---|---|
| c 512 MB, chunk 64 KiB + admission | MinIO egress, steady | ≤ 400 MB/s |
| c 512 MB, chunk 64 KiB + admission | `disk_fill_gets_per_op` | 0 |
| c 512 MB, chunk 64 KiB + admission | `evicted_bytes` over the cell | ≤ 2 x capacity |
| c 512 MB, chunk 64 KiB + admission | `disk_h` | ≥ 0.40 |
| c 512 MB, chunk 64 KiB + admission | client CPU per op | ≤ 0.42 ms |
| c 512 MB, chunk 64 KiB + admission | ops/s | ≥ 9,000 |
| c 512 MB, file + admission | `disk_fill_gets_per_op` | ≤ 0.0005 (was 0.0051) |
| c 512 MB, file + admission | fill bytes | ≤ 3 x capacity (was 1,198 GB) |
| a 512 MB, chunk 64 KiB + admission | ops/s | ≥ 2,700 (was 2,469) |
| a 512 MB, chunk 64 KiB + admission | client CPU per op | ≤ 1.2 ms (was 4.2) |
| c 2 GB, chunk 64 KiB + admission | ops/s | ≥ 44,000 (round 1: 46,741) |
| every cell | `failed`, `disk_fill_failed`, `disk_punch_failed` | 0 |

### What the projection will say

State this before the measurement so the result is not a surprise. Under uniform keys a
per-client tier cannot beat `disk_h = capacity / dataset`. This plan removes the fill
amplification and the CPU penalty. It does not lift that ceiling. At 10 TB with a 2 TB tier
per client, `disk_h` is about 0.2, which saves about $880 of the $4,405 in GETs against
$800 of gp3. The expected outcome is that the tier becomes about neutral at 10 TB (round 1:
+9 %), that the break-even data size moves from 5.9 TB to about 10 TB, and that the tier is
safe to leave on by default. A shared tier or a skewed workload is what changes the 10 TB
number, and both are outside this plan.

---

## How to build and test

The unit tests build and run on one client (amd160). From the repo root on the Mac, with
`OZONEDB_HOME` set to the worktree and `ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg`:

```bash
# 1. sync the tree to one node (from the repo root; ANSIBLE_CONFIG names the inventory)
ansible-playbook bench/ansible/sync.yml --limit amd160.utah.cloudlab.us
# 2. build the test binary there (build.sh does not build it)
ssh -o BatchMode=yes oliverr3@amd160.utah.cloudlab.us 'bash -lc "cd ozonedb && cmake --build build --target runUnitTests -j\$(nproc)"'
# 3. run one filter
ssh -o BatchMode=yes oliverr3@amd160.utah.cloudlab.us 'bash -lc "cd ozonedb/build && ./runUnitTests --gtest_filter=DiskCacheStorageTest.*:FrequencySketchTest.*"'
```

Before the campaign (Task 7) the whole chain is rebuilt on every client with
`ansible-playbook bench/ansible/sync.yml -e build=true`.

---

## File structure

| File | Responsibility |
|---|---|
| `src/include/ozonedb/frequency_sketch.h` (new, header-only) | TinyLFU count-min sketch with aging |
| `tests/test_frequency_sketch.cpp` (new) | sketch unit tests |
| `src/include/ozonedb/disk_cache_storage.h` | `Mode`, `Admission`, `AdmitMode`, chunk state, new members, lock-order comment |
| `src/db/disk_cache_storage.cpp` | admission, size memo, chunk read path, CLOCK, sparse-file I/O |
| `tests/test_disk_cache_storage.cpp` | fixture gains `admission`, `mode`, `entry` parameters and the new tests |
| `src/include/ozonedb/metadata.h` | four config keys and their validation |
| `src/db/db.cpp` | maps the keys onto `DiskCacheStorage::Options` |
| `bench/scripts/local/run_multinode_ycsb_with_corfu.sh`, `run_multinode_ycsb.py`, `run_local_ycsb_multiproc.py`, `load_local_ycsb_multiproc.py` | the three flags, the settings helper, the label token, the per-writer keys |
| `bench/scripts/extract_cost_coefficients.py` | `DISK_RE` tail, seven new columns |
| `bench/scripts/plot/plot_cost_model.py`, `combine_disk_corpus.py` | `--tier-variant`, the new campaign as a source |
| `bench/results-disk2-<date>.tsv`, `bench/RESULTS-cost.md`, `bench/PLAN-disk-cache.md`, `CLAUDE.md` | rows, write-up, status, docs |

---

### Task 1: `FrequencySketch`

**Files:**
- Create: `src/include/ozonedb/frequency_sketch.h`
- Create: `tests/test_frequency_sketch.cpp`
- Modify: `CMakeLists.txt:218-228` (add the test source)

**Interfaces:**
- Consumes: nothing.
- Produces: `class FrequencySketch { FrequencySketch(size_t counters_per_row, uint64_t window); void record(std::string_view key); uint32_t estimate(std::string_view key) const; uint64_t samples() const; size_t width() const; }`. Not thread-safe: the owner serialises calls.

- [ ] **Step 1: Write the failing tests**

```cpp
// tests/test_frequency_sketch.cpp
#include <gtest/gtest.h>

#include <string>

#include "frequency_sketch.h"

using namespace ozonedb;

TEST(FrequencySketchTest, CountsRecordsOfOneKey) {
  FrequencySketch s(1024, 1u << 20);
  EXPECT_EQ(s.estimate("a"), 0u);
  for (int i = 0; i < 5; ++i) s.record("a");
  EXPECT_EQ(s.estimate("a"), 5u);
  EXPECT_EQ(s.estimate("b"), 0u);
  EXPECT_EQ(s.samples(), 5u);
}

TEST(FrequencySketchTest, SaturatesAt255) {
  FrequencySketch s(1024, 1u << 20);
  for (int i = 0; i < 300; ++i) s.record("a");
  EXPECT_EQ(s.estimate("a"), 255u);
}

TEST(FrequencySketchTest, HalvesEveryWindow) {
  FrequencySketch s(1024, 8);
  for (int i = 0; i < 6; ++i) s.record("a");
  for (int i = 0; i < 2; ++i) s.record("b");  // the 8th record triggers the halving
  EXPECT_EQ(s.estimate("a"), 3u);
  EXPECT_EQ(s.estimate("b"), 1u);
  EXPECT_EQ(s.samples(), 4u);  // halved with the counters
}

TEST(FrequencySketchTest, WidthRoundsUpToAPowerOfTwo) {
  FrequencySketch s(1000, 8);
  EXPECT_EQ(s.width(), 1024u);
  FrequencySketch t(1, 8);
  EXPECT_EQ(t.width(), 16u);
}

TEST(FrequencySketchTest, AnUnseenKeyStaysNearZeroUnderLoad) {
  // count-min over-estimates only on a collision in all four rows: with 1000
  // keys in 4096 slots per row that is about 0.4 % per key. The keys are
  // fixed, the hash is fixed, so this is deterministic.
  FrequencySketch s(4096, 1u << 20);
  for (int i = 0; i < 1000; ++i) s.record("sstable1/" + std::to_string(i) + ".sst#0");
  EXPECT_LE(s.estimate("sstable1/never.sst#0"), 2u);
}
```

Add `tests/test_frequency_sketch.cpp` to `OZONEDB_TEST_SOURCES` after
`tests/test_disk_cache_storage.cpp`.

- [ ] **Step 2: Run the tests to verify they fail**

Run the chain in "How to build and test" with `--gtest_filter='FrequencySketchTest.*'`.
Expected: the build fails with `frequency_sketch.h: No such file or directory`.

- [ ] **Step 3: Write the sketch**

```cpp
// src/include/ozonedb/frequency_sketch.h
#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace ozonedb {

// TinyLFU frequency sketch (Einziger, Friedman and Manes, "TinyLFU: A Highly
// Efficient Cache Admission Policy", 2017): a count-min sketch of four rows
// of 8-bit counters. Every `window` records halve all counters, so an
// estimate is a recent frequency, not an all-time count. A count-min
// estimate is never below the true recent count. The class takes no lock:
// the owner serialises calls (DiskCacheStorage: sketch_mtx_).
class FrequencySketch {
 public:
  // counters_per_row is rounded up to a power of two, minimum 16.
  FrequencySketch(size_t counters_per_row, uint64_t window) : window_(window < 2 ? 2 : window) {
    size_t w = 16;
    while (w < counters_per_row) w <<= 1;
    mask_ = w - 1;
    for (auto& row : rows_) row.assign(w, 0);
  }

  void record(std::string_view key) {
    uint64_t const h = hash(key);
    for (size_t i = 0; i < 4; ++i) {
      uint8_t& c = rows_[i][index(h, i)];
      if (c < 255) ++c;
    }
    if (++samples_ >= window_) halve();
  }

  // The minimum over the four rows.
  uint32_t estimate(std::string_view key) const {
    uint64_t const h = hash(key);
    uint32_t m = 255;
    for (size_t i = 0; i < 4; ++i) {
      uint8_t const c = rows_[i][index(h, i)];
      if (c < m) m = c;
    }
    return m;
  }

  uint64_t samples() const { return samples_; }
  size_t width() const { return mask_ + 1; }

 private:
  static uint64_t hash(std::string_view key) {
    uint64_t h = 1469598103934665603ull;  // FNV-1a, 64 bit
    for (unsigned char ch : key) {
      h ^= ch;
      h *= 1099511628211ull;
    }
    return h;
  }
  // Four row indexes from one hash: each row mixes with its own constant.
  size_t index(uint64_t h, size_t row) const {
    static constexpr uint64_t kSeed[4] = {0xc3a5c85c97cb3127ull, 0xb492b66fbe98f273ull, 0x9ae16a3b2f90404full, 0xcbf29ce484222325ull};
    uint64_t x = (h ^ kSeed[row]) * 0x9e3779b97f4a7c15ull;
    x ^= x >> 32;
    return static_cast<size_t>(x) & mask_;
  }
  void halve() {
    for (auto& row : rows_) {
      for (uint8_t& c : row) c >>= 1;
    }
    samples_ /= 2;
  }

  size_t mask_ = 15;
  uint64_t window_;
  uint64_t samples_ = 0;
  std::vector<uint8_t> rows_[4];
};

}  // namespace ozonedb
```

- [ ] **Step 4: Run the tests to verify they pass**

Same filter. Expected: `[  PASSED  ] 5 tests.` If `AnUnseenKeyStaysNearZeroUnderLoad`
fails with an estimate of 3 or more, change the probe key to
`"sstable1/never2.sst#0"` and record the change in the commit message. Do not widen the
bound.

- [ ] **Step 5: Commit**

```bash
git add src/include/ozonedb/frequency_sketch.h tests/test_frequency_sketch.cpp CMakeLists.txt
git commit -m "disk-cache: TinyLFU FrequencySketch (PLAN-disk-cache-2 T1)"
```

---

### Task 2: Admission control in `file` mode

**Files:**
- Modify: `src/include/ozonedb/disk_cache_storage.h`
- Modify: `src/db/disk_cache_storage.cpp`
- Test: `tests/test_disk_cache_storage.cpp`

**Interfaces:**
- Consumes: `FrequencySketch` (Task 1).
- Produces: `Options::admission` (`Admission::kAlways` | `Admission::kFrequency`), `Options::admit_window`, `Stats::fetch_bytes`, `Stats::admit_rejected`; private `enum class AdmitMode { kForce, kContest, kFreeOnly }`, `fillMode()`, `writeThroughMode()`, `recordAccess(key)`, `frequency(key)`, `mayTakeLocked(key, bytes, victim, how)`, `admit(name, bytes, how)`, `publishPartFile(name, part_path, expected_size, how)`, `sizeOf(name)`, members `sizes_`, `sketch_mtx_`, `sketch_`. Task 3 and Task 4 reuse all of these.

- [ ] **Step 1: Extend the fixture and write the failing tests**

Change the `TierFixture` constructor signature to

```cpp
  explicit TierFixture(uint64_t capacity, size_t chunk = 64u << 20, bool drop_pages = true,
                       DiskCacheStorage::Admission admission = DiskCacheStorage::Admission::kAlways) {
```

and set `o.admission = admission;` before the tier is built. Add a `touch` helper to the
fixture:

```cpp
  // A 10-byte ranged read at offset 0, then wait for the fill worker.
  void touch(std::string const& name) {
    unsigned char* d = nullptr;
    ASSERT_EQ(tier->read(name, d, 0, 10), Status::kSuccess);
    delete[] d;
    tier->waitFillIdle();
  }
```

Append these tests:

```cpp
TEST(DiskCacheStorageTest, FrequencyAdmissionRefusesAFillThatWouldEvictAnEquallyUsedFile) {
  TierFixture f(2500, /*chunk=*/1024, /*drop_pages=*/true, DiskCacheStorage::Admission::kFrequency);
  f.tier->startFillWorker();
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  std::string const c = "sstable1/" + stamp() + "_c.sst";
  f.seed(a, 1000);
  f.seed(b, 1000);
  f.seed(c, 1000);
  f.touch(a);  // free budget: filled without a contest
  f.touch(b);
  EXPECT_TRUE(f.local(a));
  EXPECT_TRUE(f.local(b));
  f.touch(c);  // 3000 > 2500, and c (one access) is not above the LRU victim a (one access)
  EXPECT_FALSE(f.local(c));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.fill_bytes, 2000u);         // the refused fill moved no bytes
  EXPECT_EQ(s.fetch_bytes, 2000u + 30u);  // two fills plus three 10-byte misses
  EXPECT_EQ(s.files, 2u);
}

TEST(DiskCacheStorageTest, FrequencyAdmissionLetsAHotterFileDisplaceTheColdestOne) {
  TierFixture f(2500, /*chunk=*/1024, /*drop_pages=*/true, DiskCacheStorage::Admission::kFrequency);
  f.tier->startFillWorker();
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  std::string const c = "sstable1/" + stamp() + "_c.sst";
  f.seed(a, 1000);
  f.seed(b, 1000);
  f.seed(c, 1000);
  f.touch(a);
  f.touch(b);
  f.touch(c);  // c = 1, victim a = 1: refused
  f.touch(c);  // c = 2 > a = 1: admitted, a evicted
  EXPECT_TRUE(f.local(c));
  EXPECT_FALSE(f.local(a));
  EXPECT_TRUE(f.local(b));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.files, 2u);
}

TEST(DiskCacheStorageTest, WriteThroughUnderFrequencyAdmissionTakesOnlyFreeBudget) {
  TierFixture f(2500, /*chunk=*/64u << 20, /*drop_pages=*/true, DiskCacheStorage::Admission::kFrequency);
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
  put(c, 1000);  // no free budget and no history: refused, nothing evicted
  EXPECT_TRUE(f.local(a));
  EXPECT_TRUE(f.local(b));
  EXPECT_FALSE(f.local(c));
  EXPECT_FALSE(f.part(c));
  EXPECT_TRUE(f.backing->exist(c));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.writethrough_files, 2u);
}

TEST(DiskCacheStorageTest, FillUsesTheMemoisedObjectSize) {
  TierFixture f(1u << 20, /*chunk=*/1024);
  f.tier->startFillWorker();
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  f.seed(a, 3000);
  f.touch(a);  // miss + fill: one size() on the backing
  EXPECT_EQ(f.backing->sizes, 1);
  f.touch(a);  // hit: none
  EXPECT_EQ(f.backing->sizes, 1);
  f.tier->invalidate(a);  // drops the memo with the copy
  f.touch(a);
  EXPECT_EQ(f.backing->sizes, 2);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Filter `DiskCacheStorageTest.*`. Expected: a compile error on `Admission`.

- [ ] **Step 3: Add the options, the stats and the private interface**

In `disk_cache_storage.h`, include `"frequency_sketch.h"`, and add inside the class:

```cpp
  enum class Admission { kAlways, kFrequency };
```

to `Options`:

```cpp
    Admission admission = Admission::kAlways;  // kFrequency: TinyLFU contest for non-free budget
    uint64_t admit_window = 0;                 // sketch aging window in samples; 0 = 8 x the entries the budget holds
```

to `Stats`:

```cpp
    uint64_t fetch_bytes = 0, admit_rejected = 0;
```

to the private section:

```cpp
  // How a candidate takes budget that is not free (bench/PLAN-disk-cache-2.md, Task 2).
  //   kForce    evict until it fits: the round-1 behaviour. reconcile(), and every
  //             path under Admission::kAlways.
  //   kContest  only if the candidate's sketch frequency is strictly above the
  //             victim's: fills under Admission::kFrequency.
  //   kFreeOnly never: write-throughs under Admission::kFrequency, because a
  //             compaction output has no access history to contest with.
  // Free budget is always taken without a contest.
  enum class AdmitMode { kForce, kContest, kFreeOnly };
  AdmitMode fillMode() const { return options_.admission == Admission::kFrequency ? AdmitMode::kContest : AdmitMode::kForce; }
  AdmitMode writeThroughMode() const { return options_.admission == Admission::kFrequency ? AdmitMode::kFreeOnly : AdmitMode::kForce; }
  void recordAccess(std::string const& key);
  uint32_t frequency(std::string const& key);  // 0 without a sketch
  // Under mtx_. Decides whether `bytes` more for `key` is allowed under `how`,
  // given `victim` (empty = none). Counts a refusal in admit_rejected_.
  bool mayTakeLocked(std::string const& key, size_t bytes, std::string const& victim, AdmitMode how);
  // backing_->size() memoised per cacheable name (objects are immutable);
  // dropped by eraseLocked/invalidate. Used by the fill path only: the public
  // size() keeps its round-1 behaviour for a file the builder is appending.
  size_t sizeOf(std::string const& name);
```

Change the two signatures:

```cpp
  bool publishPartFile(std::string const& name, std::string const& part_path, size_t expected_size, AdmitMode how);
  bool admit(std::string const& name, size_t bytes, AdmitMode how);
```

and add the members:

```cpp
  std::unordered_map<std::string, size_t> sizes_;  // under mtx_
  // Leaf lock for sketch_: taken under mtx_ by mayTakeLocked(), and alone by
  // recordAccess(). Never held across any I/O.
  std::mutex sketch_mtx_;
  std::unique_ptr<FrequencySketch> sketch_;
  std::atomic<uint64_t> fetch_bytes_{0}, admit_rejected_{0};
```

Extend the lock-order comment at the top of the header with one line:
`mtx_ -> sketch_mtx_ (leaf)`.

- [ ] **Step 4: Implement**

Constructor, after the write probe:

```cpp
  if (options_.admission == Admission::kFrequency) {
    // Sized from the entries the budget holds: a file-mode entry is about one
    // SSTable, taken as 64 MiB here (Task 4 uses entry_bytes in chunk mode).
    uint64_t const unit = 64u << 20;
    uint64_t const expected = std::max<uint64_t>(16, options_.capacity_bytes / unit);
    uint64_t const window = options_.admit_window ? options_.admit_window : 8 * expected;
    sketch_ = std::make_unique<FrequencySketch>(static_cast<size_t>(std::min<uint64_t>(4 * expected, 1u << 22)), window);
  }
```

New functions:

```cpp
void DiskCacheStorage::recordAccess(std::string const& key) {
  if (!sketch_) return;
  std::lock_guard<std::mutex> lk(sketch_mtx_);
  sketch_->record(key);
}

uint32_t DiskCacheStorage::frequency(std::string const& key) {
  if (!sketch_) return 0;
  std::lock_guard<std::mutex> lk(sketch_mtx_);
  return sketch_->estimate(key);
}

bool DiskCacheStorage::mayTakeLocked(std::string const& key, size_t bytes, std::string const& victim, AdmitMode how) {
  if (current_bytes_ + bytes <= options_.capacity_bytes) return true;  // free budget: no contest
  if (how == AdmitMode::kForce) return true;
  bool const allowed = how == AdmitMode::kContest && !victim.empty() && frequency(key) > frequency(victim);
  if (!allowed) admit_rejected_.fetch_add(1);
  return allowed;
}

size_t DiskCacheStorage::sizeOf(std::string const& name) {
  {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = sizes_.find(name);
    if (it != sizes_.end()) return it->second;
  }
  size_t const n = backing_->size(name);
  if (n > 0) {
    std::lock_guard<std::mutex> lk(mtx_);
    sizes_.emplace(name, n);
  }
  return n;
}
```

`admit`:

```cpp
bool DiskCacheStorage::admit(std::string const& name, size_t bytes, AdmitMode how) {
  std::lock_guard<std::mutex> lk(mtx_);
  if (bytes > options_.capacity_bytes) return false;
  auto old = index_.find(name);
  if (old != index_.end()) {  // re-created name: replace the accounting
    current_bytes_ -= old->second.bytes;
    lru_.erase(old->second.lru);
    index_.erase(old);
  }
  if (!mayTakeLocked(name, bytes, lru_.empty() ? std::string() : lru_.back(), how)) return false;
  evictToFitLocked(bytes);
  lru_.push_front(name);
  index_[name] = Entry{bytes, lru_.begin()};
  current_bytes_ += bytes;
  return true;
}
```

`publishPartFile` tail, after the rename:

```cpp
  if (!admit(name, static_cast<size_t>(bytes), how)) {
    fs::remove(localPath(name), ec);  // refused: nothing indexes the copy, so it must not stay
    return false;
  }
  return true;
```

`publishPart` passes `writeThroughMode()`. `fillOne` uses `sizeOf(name)` for `total`,
passes `fillMode()` to `publishPartFile`, counts `fetch_bytes_.fetch_add(len)` after each
successful chunk read, and checks admission before the transfer, right after the
`total > options_.capacity_bytes` check:

```cpp
  {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!mayTakeLocked(name, total, lru_.empty() ? std::string() : lru_.back(), fillMode())) return;  // refused before a byte moves
  }
```

`reconcile` passes `AdmitMode::kForce`. Both `read` overloads call `recordAccess(fileName)`
right after the `cacheable` check, and add `fetch_bytes_.fetch_add(...)` next to the
`miss_bytes_` increment. `eraseLocked` starts with `sizes_.erase(name);` before the index
lookup. `stats()` copies the two new counters. `printStats()` appends
` fetch_bytes=` and ` admit_rejected=` after `capacity=`.

- [ ] **Step 5: Run the tests to verify they pass**

Filter `DiskCacheStorageTest.*:FrequencySketchTest.*`. Expected: every round-1 test passes
unchanged (the default is `kAlways`, and `kForce` is the round-1 path), plus the four new
ones. A refusal can be counted twice when the budget changes between the fill's check and
its publish. That is documented in the header comment, not fixed.

- [ ] **Step 6: Commit**

```bash
git add src/include/ozonedb/disk_cache_storage.h src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk-cache: TinyLFU admission control in file mode, memoised object sizes (PLAN-disk-cache-2 T2)"
```

---

### Task 3: Chunk mode: sparse-file entries, the miss path, CLOCK eviction

**Files:**
- Modify: `src/include/ozonedb/disk_cache_storage.h`
- Modify: `src/db/disk_cache_storage.cpp`
- Test: `tests/test_disk_cache_storage.cpp`

**Interfaces:**
- Consumes: `AdmitMode`, `mayTakeLocked`, `sizeOf`, `recordAccess` (Task 2).
- Produces: `Options::mode` (`Mode::kFile` | `Mode::kChunk`), `Options::entry_bytes`, `Stats::entries`, `Stats::punch_failed`; private `struct ChunkFile`, `chunks_`, `ring_`, the CLOCK hand, `readChunked`, `chunksPresentLocked`, `reserveChunkLocked`, `clockVictimLocked`, `evictChunkLocked`, `dropChunkFileLocked`, `pwriteLocal`, `punchHole`, `chunkCount`, `chunkBytes`, `chunkKey`. The tests of this task run under `Admission::kAlways`, so every reservation takes `AdmitMode::kForce`; the contest path is exercised in Task 4. `Options::chunk_bytes` (the fill worker's read size) applies to `file` mode only: say so in its comment.

- [ ] **Step 1: Extend the fixture and write the failing tests**

Give `CountingStorage` two fields, set in the ranged `read`:

```cpp
  size_t last_a = 0;
  size_t last_len = 0;
```

(`last_a = a; last_len = length;` before the delegate call.)

Extend the fixture constructor:

```cpp
  explicit TierFixture(uint64_t capacity, size_t chunk = 64u << 20, bool drop_pages = true,
                       DiskCacheStorage::Admission admission = DiskCacheStorage::Admission::kAlways,
                       DiskCacheStorage::Mode mode = DiskCacheStorage::Mode::kFile, size_t entry = 65536) {
```

with `o.mode = mode; o.entry_bytes = entry;`. Add a helper to the fixture:

```cpp
  // A ranged read of `n` bytes at `off`, checked against `bytes`.
  void readAt(std::string const& name, std::vector<unsigned char> const& bytes, size_t off, size_t n) {
    unsigned char* d = nullptr;
    ASSERT_EQ(tier->read(name, d, off, n), Status::kSuccess);
    EXPECT_EQ(0, memcmp(d, bytes.data() + off, n));
    delete[] d;
  }
```

Append these tests (`using Mode = DiskCacheStorage::Mode; using Adm = DiskCacheStorage::Admission;`
at file scope inside the anonymous namespace):

```cpp
TEST(DiskCacheStorageTest, ChunkMissFetchesTheCoveringChunksAndTheNextReadHits) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 3 * 1024 + 1);
  f.readAt(name, bytes, 5, 10);  // chunk 0
  EXPECT_EQ(f.backing->ranged_reads, 1);
  EXPECT_EQ(f.backing->last_a, 0u);
  EXPECT_EQ(f.backing->last_len, 1024u);
  auto s = f.tier->stats();
  EXPECT_EQ(s.misses, 1u);
  EXPECT_EQ(s.miss_bytes, 10u);
  EXPECT_EQ(s.fetch_bytes, 1024u);
  EXPECT_EQ(s.fills, 1u);
  EXPECT_EQ(s.fill_bytes, 1024u);
  EXPECT_EQ(s.fill_gets, 0u);  // the fill is the demand read
  EXPECT_EQ(s.entries, 1u);
  EXPECT_EQ(s.bytes, 1024u);
  EXPECT_EQ(s.files, 1u);
  f.readAt(name, bytes, 100, 200);  // inside chunk 0: a hit
  EXPECT_EQ(f.backing->ranged_reads, 1);
  EXPECT_EQ(f.tier->stats().hits, 1u);
  f.readAt(name, bytes, 1020, 10);  // straddles chunks 0 and 1; chunk 1 is absent
  EXPECT_EQ(f.backing->ranged_reads, 2);
  EXPECT_EQ(f.backing->last_a, 0u);  // the whole covering range, present chunk included
  EXPECT_EQ(f.backing->last_len, 2048u);
  s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.fills, 2u);
  EXPECT_EQ(s.fill_skipped_present, 1u);
  EXPECT_EQ(s.fetch_bytes, 1024u + 2048u);
  f.readAt(name, bytes, 3 * 1024, 1);  // the short last chunk
  EXPECT_EQ(f.backing->last_a, 3072u);
  EXPECT_EQ(f.backing->last_len, 1u);
  EXPECT_EQ(f.tier->stats().bytes, 2048u + 1u);
  // The local copy holds each fetched chunk at its own offset; chunk 2 is a hole.
  std::ifstream in(f.tier_dir + name, std::ios::binary);
  std::vector<unsigned char> copy((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  ASSERT_EQ(copy.size(), bytes.size());
  EXPECT_EQ(0, memcmp(copy.data(), bytes.data(), 2048));
  EXPECT_EQ(copy[3072], bytes[3072]);
}

TEST(DiskCacheStorageTest, ChunkBudgetEvictsWithClockAndTheVictimIsFetchedAgain) {
  TierFixture f(2048, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 4096);
  auto chunk = [&](size_t c) { f.readAt(name, bytes, c * 1024 + 1, 8); };
  chunk(0);
  chunk(1);  // 2048 of 2048
  EXPECT_EQ(f.tier->stats().entries, 2u);
  chunk(2);  // hand: 0 referenced -> cleared, 1 cleared, wrap, 0 evicted
  auto s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.evicted_bytes, 1024u);
  EXPECT_EQ(s.bytes, 2048u);
  EXPECT_EQ(s.punch_failed, 0u);
  int const before = f.backing->ranged_reads;
  chunk(0);  // absent again: fetched; this time chunk 1 is the victim
  EXPECT_EQ(f.backing->ranged_reads, before + 1);
  s = f.tier->stats();
  EXPECT_EQ(s.evictions, 2u);
  EXPECT_EQ(s.entries, 2u);
  chunk(2);  // still present: a hit
  EXPECT_EQ(f.backing->ranged_reads, before + 1);
  EXPECT_EQ(f.tier->stats().hits, 1u);
  EXPECT_TRUE(f.local(name));  // the sparse file stays while any chunk is present
}

TEST(DiskCacheStorageTest, ChunkEvictingTheLastChunkDropsTheFile) {
  TierFixture f(1024, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const a = "sstable1/" + stamp() + "_a.sst";
  std::string const b = "sstable1/" + stamp() + "_b.sst";
  auto ba = f.seed(a, 1024);
  auto bb = f.seed(b, 1024);
  f.readAt(a, ba, 0, 8);
  f.readAt(b, bb, 0, 8);  // evicts a's only chunk
  EXPECT_FALSE(f.local(a));
  EXPECT_TRUE(f.local(b));
  auto s = f.tier->stats();
  EXPECT_EQ(s.files, 1u);
  EXPECT_EQ(s.entries, 1u);
  EXPECT_EQ(s.bytes, 1024u);
}

TEST(DiskCacheStorageTest, ChunkInvalidateAndRemoveDropTheCopy) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 2048);
  f.readAt(name, bytes, 0, 8);
  EXPECT_TRUE(f.local(name));
  f.tier->invalidate(name);
  EXPECT_FALSE(f.local(name));
  auto s = f.tier->stats();
  EXPECT_EQ(s.invalidated, 1u);
  EXPECT_EQ(s.entries, 0u);
  EXPECT_EQ(s.bytes, 0u);
  EXPECT_TRUE(f.backing->exist(name));
  f.readAt(name, bytes, 0, 8);  // fetched again
  EXPECT_EQ(f.backing->ranged_reads, 2);
  f.tier->remove(name);
  EXPECT_FALSE(f.local(name));
  EXPECT_FALSE(f.backing->exist(name));
  EXPECT_EQ(f.tier->stats().entries, 0u);
}

TEST(DiskCacheStorageTest, ChunkModeHasNoFillWorkerAndReconcileStartsCold) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  f.tier->startFillWorker();  // a no-op in chunk mode
  f.tier->waitFillIdle();     // returns at once
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 2048);
  f.readAt(name, bytes, 0, 8);
  f.tier->stopFillWorker();
  EXPECT_EQ(f.tier->stats().fill_gets, 0u);
  // reconcile() in chunk mode deletes every local file: which chunks of a
  // leftover sparse file are data is not recorded.
  std::filesystem::create_directories(f.tier_dir + "sstable1");
  std::ofstream(f.tier_dir + "sstable1/leftover.sst") << "x";
  EXPECT_EQ(f.tier->reconcile([](std::string const&, size_t) { return true; }), 2u);
  EXPECT_FALSE(f.local(name));
  EXPECT_EQ(f.tier->stats().entries, 0u);
}

// Two threads miss on the same chunk while the first fetch is still in the
// backing. Both fetch, both return the right bytes, and exactly one of them
// writes the chunk: the other finds it kPending or kPresent and skips.
TEST(DiskCacheStorageTest, ChunkTwoConcurrentMissesFetchTwiceAndWriteOnce) {
  std::string const s = stamp();
  std::string const backing_dir = kRoot + "backing_" + s + "/";
  std::string const tier_dir = kRoot + "tier_" + s + "/";
  std::filesystem::create_directories(backing_dir + "sstable1");
  std::filesystem::create_directories(tier_dir);
  auto owned = std::make_unique<SlowStorage>(backing_dir, std::chrono::milliseconds(300));
  SlowStorage* slow = owned.get();
  DiskCacheStorage::Options o;
  o.dir = tier_dir;
  o.capacity_bytes = 1u << 20;
  o.mode = DiskCacheStorage::Mode::kChunk;
  o.entry_bytes = 1024;
  auto tier = std::make_unique<DiskCacheStorage>(std::move(owned), o);
  std::string const name = "sstable1/" + s + ".sst";
  std::vector<unsigned char> bytes(2048);
  for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<unsigned char>(i % 251);
  std::ofstream(backing_dir + name, std::ios::binary).write(reinterpret_cast<char const*>(bytes.data()), 2048);

  unsigned char* d1 = nullptr;
  Status s1 = Status::kFailure;
  std::thread t([&] { s1 = tier->read(name, d1, 0, 8); });
  ASSERT_TRUE(slow->waitForReadsAtLeast(1, std::chrono::seconds(5)));  // the first read is inside the backing
  unsigned char* d2 = nullptr;
  ASSERT_EQ(tier->read(name, d2, 4, 8), Status::kSuccess);  // chunk 0 is not present: a second miss
  t.join();
  ASSERT_EQ(s1, Status::kSuccess);
  EXPECT_EQ(0, memcmp(d1, bytes.data(), 8));
  EXPECT_EQ(0, memcmp(d2, bytes.data() + 4, 8));
  delete[] d1;
  delete[] d2;
  EXPECT_EQ(slow->reads(), 2);
  auto st = tier->stats();
  EXPECT_EQ(st.misses, 2u);
  EXPECT_EQ(st.entries, 1u);
  EXPECT_EQ(st.bytes, 1024u);
  EXPECT_EQ(st.fills + st.fill_skipped_present, 2u);
  tier.reset();
  std::filesystem::remove_all(backing_dir);
  std::filesystem::remove_all(tier_dir);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Filter `DiskCacheStorageTest.Chunk*`. Expected: a compile error on `Mode`.

- [ ] **Step 3: Add the chunk state to the header**

Inside the class, next to `Admission`:

```cpp
  enum class Mode { kFile, kChunk };
```

to `Options`:

```cpp
    Mode mode = Mode::kFile;       // kChunk: sub-file entries, no fill worker (PLAN-disk-cache-2)
    size_t entry_bytes = 64u << 10;  // chunk mode: entry size, a power of two >= 4096
```

to `Stats`:

```cpp
    uint64_t entries = 0, punch_failed = 0;
```

to the private section:

```cpp
  // Chunk mode (bench/PLAN-disk-cache-2.md, Task 3). One sparse local file per
  // SSTable under localPath(name); one state byte per chunk. A chunk is
  // charged chunkBytes() of budget while kPresent or kPending. kPending is a
  // reservation: the bytes are on their way to the file, so a reader must
  // not treat the chunk as a hit (a hole reads as zeros), and the CLOCK hand
  // must not evict it. Only the thread that set kPending clears it.
  static constexpr uint8_t kPresent = 1, kReferenced = 2, kPending = 4;
  struct ChunkFile {
    size_t size = 0;              // object size
    std::vector<uint8_t> state;   // chunkCount(size) bytes
    size_t present = 0;           // chunks with kPresent
    size_t pending = 0;           // chunks with kPending
    size_t ring = 0;              // this file's slot in ring_
  };
  size_t chunkCount(size_t size) const { return (size + options_.entry_bytes - 1) / options_.entry_bytes; }
  size_t chunkBytes(size_t size, size_t c) const { return std::min(options_.entry_bytes, size - c * options_.entry_bytes); }
  std::string chunkKey(std::string const& name, size_t c) const { return name + '#' + std::to_string(c); }
  Status readChunked(std::string const& fileName, unsigned char*& data, size_t a, size_t length);
  // Under mtx_. True when every chunk of [c0, c1) is kPresent; sets kReferenced on them.
  bool chunksPresentLocked(ChunkFile& cf, size_t c0, size_t c1);
  // Under mtx_. Marks chunk c kPending and charges its bytes, evicting through the
  // CLOCK hand as needed under `how`. False (nothing changed) when refused.
  bool reserveChunkLocked(std::string const& name, ChunkFile& cf, size_t c, AdmitMode how);
  // Under mtx_. Advances the hand to the next kPresent chunk without kReferenced,
  // clearing kReferenced on the way. False after two idle rotations.
  bool clockVictimLocked(std::string& name, size_t& chunk);
  void evictChunkLocked(std::string const& name, size_t chunk);
  // Under mtx_. Forgets the file's accounting and its ring slot. `unlink`
  // also removes the local file.
  void dropChunkFileLocked(std::string const& name, bool unlink, bool count_as_invalidated);
  bool pwriteLocal(std::string const& name, size_t off, unsigned char const* buf, size_t len);
  bool punchHole(std::string const& name, size_t off, size_t len);

  std::unordered_map<std::string, ChunkFile> chunks_;  // under mtx_
  std::vector<std::string> ring_;                      // CLOCK order of the chunks_ keys
  size_t hand_file_ = 0, hand_chunk_ = 0;              // the CLOCK hand: ring_ slot, chunk
  uint64_t present_chunks_ = 0;                        // under mtx_
  std::atomic<uint64_t> punch_failed_{0};
```

Extend the lock-order comment: in chunk mode `mtx_` is held across `punchHole` (a syscall,
like the unlink it already covers), never across `pwriteLocal` or a backing call.

- [ ] **Step 4: Implement the chunk store**

Includes: `<cstring>`, `<fcntl.h>` (`fallocate`, `FALLOC_FL_*` under `_GNU_SOURCE`, which
`g++` defines).

Dispatch in the ranged `read`, right after the `cacheable` check:

```cpp
  if (options_.mode == Mode::kChunk) return readChunked(fileName, data, a, length);
```

The miss path:

```cpp
Status DiskCacheStorage::readChunked(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  size_t const E = options_.entry_bytes;
  recordAccess(chunkKey(fileName, a / E));
  bool hit = false;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = chunks_.find(fileName);
    if (it != chunks_.end() && a + length <= it->second.size && chunksPresentLocked(it->second, a / E, (a + length + E - 1) / E)) hit = true;
  }
  if (hit) {
    if (readLocal(fileName, data, a, length)) {
      hits_.fetch_add(1);
      hit_bytes_.fetch_add(length);
      return Status::kSuccess;
    }
    invalidate(fileName);  // the copy is unreadable: drop it
  }
  misses_.fetch_add(1);
  size_t const total = sizeOf(fileName);
  if (total == 0 || a + length > total) {  // unknown object, or a read past its end: the backing decides
    Status s = backing_->read(fileName, data, a, length);
    if (s == Status::kSuccess) {
      miss_bytes_.fetch_add(length);
      fetch_bytes_.fetch_add(length);
    }
    return s;
  }
  size_t const c0 = a / E, c1 = (a + length + E - 1) / E;
  size_t const off = c0 * E;
  size_t const len = std::min(c1 * E, total) - off;
  unsigned char* buf = nullptr;
  Status s = backing_->read(fileName, buf, off, len);
  if (s != Status::kSuccess || buf == nullptr) {
    delete[] buf;
    return s == Status::kSuccess ? Status::kFailure : s;
  }
  miss_bytes_.fetch_add(length);
  fetch_bytes_.fetch_add(len);
  data = new unsigned char[length];
  std::memcpy(data, buf + (a - off), length);

  // Reserve the absent chunks under mtx_, write them outside it, then flip
  // kPending to kPresent under mtx_ again.
  std::vector<size_t> to_write;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    ChunkFile& cf = chunks_[fileName];
    if (cf.state.empty()) {
      cf.size = total;
      cf.state.assign(chunkCount(total), 0);
      cf.ring = ring_.size();
      ring_.push_back(fileName);
    }
    for (size_t c = c0; c < c1; ++c) {
      if (cf.state[c] & (kPresent | kPending)) {
        fill_skipped_present_.fetch_add(1);
        continue;
      }
      if (reserveChunkLocked(fileName, cf, c, fillMode())) to_write.push_back(c);
    }
    if (cf.present == 0 && cf.pending == 0) dropChunkFileLocked(fileName, /*unlink=*/false, false);  // nothing kept: no empty entry
  }
  uint64_t written = 0, wrote = 0;
  for (size_t c : to_write) {
    size_t const n = chunkBytes(total, c);
    bool const ok = pwriteLocal(fileName, c * E, buf + (c * E - off), n);
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = chunks_.find(fileName);
    if (it == chunks_.end()) {  // invalidated meanwhile: the pwrite re-created a dead file
      std::error_code ec;
      fs::remove(localPath(fileName), ec);
      current_bytes_ -= n;
      continue;
    }
    ChunkFile& cf = it->second;
    cf.state[c] &= static_cast<uint8_t>(~kPending);
    --cf.pending;
    if (ok) {
      cf.state[c] |= kPresent | kReferenced;
      ++cf.present;
      ++present_chunks_;
      ++wrote;
      written += n;
    } else {
      current_bytes_ -= n;
      fill_failed_.fetch_add(1);
      if (cf.present == 0 && cf.pending == 0) dropChunkFileLocked(fileName, /*unlink=*/true, false);
    }
  }
  delete[] buf;
  fills_.fetch_add(wrote);
  fill_bytes_.fetch_add(written);
  return Status::kSuccess;
}
```

`invalidate()` in chunk mode must also release the pending reservations it forgets: when
it drops a `ChunkFile` with `pending > 0`, the writer of each pending chunk subtracts its
bytes when it finds no entry (the `it == chunks_.end()` branch above), so `invalidate` must
not subtract them itself. `dropChunkFileLocked` subtracts only the present chunks.

The helpers:

```cpp
bool DiskCacheStorage::chunksPresentLocked(ChunkFile& cf, size_t c0, size_t c1) {
  if (c1 > cf.state.size()) return false;
  for (size_t c = c0; c < c1; ++c) {
    if (!(cf.state[c] & kPresent)) return false;
  }
  for (size_t c = c0; c < c1; ++c) cf.state[c] |= kReferenced;
  return true;
}

bool DiskCacheStorage::reserveChunkLocked(std::string const& name, ChunkFile& cf, size_t c, AdmitMode how) {
  size_t const need = chunkBytes(cf.size, c);
  if (need > options_.capacity_bytes) {
    fill_skipped_budget_.fetch_add(1);
    return false;
  }
  // Reserve first: while this chunk is kPending its file cannot be dropped
  // by an eviction below, so `cf` stays valid.
  cf.state[c] |= kPending;
  ++cf.pending;
  current_bytes_ += need;
  while (current_bytes_ > options_.capacity_bytes) {
    std::string vname;
    size_t vchunk = 0;
    if (!clockVictimLocked(vname, vchunk) || !mayTakeLocked(chunkKey(name, c), 0, chunkKey(vname, vchunk), how)) {
      cf.state[c] &= static_cast<uint8_t>(~kPending);
      --cf.pending;
      current_bytes_ -= need;
      if (vname.empty()) admit_rejected_.fetch_add(1);  // nothing evictable: counted here, mayTakeLocked did not run
      return false;
    }
    evictChunkLocked(vname, vchunk);
  }
  return true;
}
```

`mayTakeLocked(key, 0, victim, how)` is called with `bytes = 0` *after* the reservation, so
its free-budget test reads `current_bytes_ <= capacity`, which is false inside this loop.
That is the intent: the loop runs only while the reservation does not fit.

```cpp
bool DiskCacheStorage::clockVictimLocked(std::string& name, size_t& chunk) {
  size_t wraps = 0;
  while (wraps < 3 && !ring_.empty()) {
    if (hand_file_ >= ring_.size()) {
      hand_file_ = 0;
      hand_chunk_ = 0;
      ++wraps;
      continue;
    }
    ChunkFile& cf = chunks_[ring_[hand_file_]];
    if (cf.present == 0) {
      ++hand_file_;
      hand_chunk_ = 0;
      continue;
    }
    for (; hand_chunk_ < cf.state.size(); ++hand_chunk_) {
      uint8_t& st = cf.state[hand_chunk_];
      if (!(st & kPresent)) continue;
      if (st & kReferenced) {
        st &= static_cast<uint8_t>(~kReferenced);
        continue;
      }
      name = ring_[hand_file_];
      chunk = hand_chunk_++;
      return true;
    }
    ++hand_file_;
    hand_chunk_ = 0;
  }
  return false;
}

void DiskCacheStorage::evictChunkLocked(std::string const& name, size_t c) {
  auto it = chunks_.find(name);
  if (it == chunks_.end()) return;
  ChunkFile& cf = it->second;
  size_t const n = chunkBytes(cf.size, c);
  if (!punchHole(name, c * options_.entry_bytes, n)) punch_failed_.fetch_add(1);  // the state byte is what says "absent"
  cf.state[c] &= static_cast<uint8_t>(~(kPresent | kReferenced));
  --cf.present;
  --present_chunks_;
  current_bytes_ -= n;
  evictions_.fetch_add(1);
  evicted_bytes_.fetch_add(n);
  if (cf.present == 0 && cf.pending == 0) dropChunkFileLocked(name, /*unlink=*/true, false);
}

void DiskCacheStorage::dropChunkFileLocked(std::string const& name, bool unlink, bool count_as_invalidated) {
  sizes_.erase(name);
  auto it = chunks_.find(name);
  if (it == chunks_.end()) return;
  ChunkFile& cf = it->second;
  for (size_t c = 0; c < cf.state.size(); ++c) {
    if (cf.state[c] & kPresent) current_bytes_ -= chunkBytes(cf.size, c);
  }
  present_chunks_ -= cf.present;
  // Swap-remove the ring slot. The hand tolerates it: at worst the moved
  // file gets one extra second chance.
  size_t const slot = cf.ring;
  size_t const last = ring_.size() - 1;
  if (slot != last) {
    ring_[slot] = ring_[last];
    chunks_[ring_[slot]].ring = slot;
  }
  ring_.pop_back();
  if (hand_file_ == slot) hand_chunk_ = 0;
  chunks_.erase(it);
  if (unlink) {
    std::error_code ec;
    fs::remove(localPath(name), ec);
  }
  if (count_as_invalidated) invalidated_.fetch_add(1);
}

bool DiskCacheStorage::pwriteLocal(std::string const& name, size_t off, unsigned char const* buf, size_t len) {
  std::error_code ec;
  fs::create_directories(fs::path(localPath(name)).parent_path(), ec);
  int fd = ::open(localPath(name).c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd < 0) return false;
  size_t done = 0;
  while (done < len) {
    ssize_t n = ::pwrite(fd, buf + done, len - done, static_cast<off_t>(off + done));
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    done += static_cast<size_t>(n);
  }
  if (options_.drop_pages) ::posix_fadvise(fd, static_cast<off_t>(off), static_cast<off_t>(len), POSIX_FADV_DONTNEED);
  ::close(fd);
  return done == len;
}

bool DiskCacheStorage::punchHole(std::string const& name, size_t off, size_t len) {
  int fd = ::open(localPath(name).c_str(), O_WRONLY);
  if (fd < 0) return false;
  int const rc = ::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, static_cast<off_t>(off), static_cast<off_t>(len));
  ::close(fd);
  return rc == 0;
}
```

Mode-aware paths:

- `invalidate()` and `remove()`: in chunk mode, after `discardPart`, take `mtx_` and call
  `dropChunkFileLocked(name, /*unlink=*/true, /*count_as_invalidated=*/true)` (for
  `remove`, `invalidate` is what it already calls).
- `reconcile()`: in chunk mode, delete every regular file under `options_.dir`, return the
  count, and return before the round-1 walk.
- `startFillWorker()`: return at once in chunk mode. `waitFillIdle()` then returns because
  `fill_started_` is false. `stopFillWorker()` is already a no-op when nothing started.
- `enqueueFill()` is never called in chunk mode (the miss path does not call it).
- `stats()`: `entries` = `index_.size()` in file mode, `present_chunks_` in chunk mode;
  `files` = `chunks_.size()` in chunk mode; `punch_failed` from its counter.
- `printStats()`: append ` entries=`, ` mode=` (`file` or `chunk`), ` entry_bytes=`
  (`options_.entry_bytes` in chunk mode, `0` in file mode) and ` punch_failed=` after
  ` admit_rejected=`.
- The public `size()`: in chunk mode consult `chunks_` first (`cf.size`), then the backing.
  `chunks_` never holds a file under construction, so the builder is unaffected.

- [ ] **Step 5: Run the tests to verify they pass**

Filter `DiskCacheStorageTest.*`. Expected: all round-1 tests, the Task 2 tests and the six
`Chunk*` tests pass. `/tank` on the nodes is ZFS, and `punch_failed` must be 0 there too.
If it is not, print the `errno` from `fallocate` once to stderr, report it, and stop:
that is a plan defect for the controller, not a test to loosen.

- [ ] **Step 6: Commit**

```bash
git add src/include/ozonedb/disk_cache_storage.h src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk-cache: chunk mode -- sparse-file entries, inline fill on the miss path, CLOCK eviction with punched holes (PLAN-disk-cache-2 T3)"
```

---

### Task 4: Chunk mode: write-through, full-file reads, admission contest, sketch sizing

**Files:**
- Modify: `src/include/ozonedb/disk_cache_storage.h`
- Modify: `src/db/disk_cache_storage.cpp`
- Test: `tests/test_disk_cache_storage.cpp`

**Interfaces:**
- Consumes: everything from Tasks 2 and 3.
- Produces: private `admitWholeFileLocked(name, bytes, how)`; the chunk-mode branches of `publishPartFile` and the full-file `read`; the sketch sized by `entry_bytes` in chunk mode.

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(DiskCacheStorageTest, ChunkWriteThroughPublishesEveryChunk) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  std::vector<unsigned char> v(2500);
  for (size_t i = 0; i < v.size(); ++i) v[i] = static_cast<unsigned char>(i % 251);
  ASSERT_EQ(f.tier->appendNoFlush(name, v.data(), 2500), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(name), Status::kSuccess);
  auto s = f.tier->stats();
  EXPECT_EQ(s.writethrough_files, 1u);
  EXPECT_EQ(s.entries, 3u);
  EXPECT_EQ(s.bytes, 2500u);
  EXPECT_FALSE(f.part(name));
  f.readAt(name, v, 2040, 20);  // straddles chunks 1 and 2: a hit
  EXPECT_EQ(f.backing->ranged_reads, 0);
  EXPECT_EQ(f.tier->stats().hits, 1u);
}

TEST(DiskCacheStorageTest, ChunkWholeFileReadIsServedOnlyWhenComplete) {
  TierFixture f(1u << 20, 64u << 20, true, Adm::kAlways, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 2048);
  unsigned char* d = nullptr;
  size_t n = 0;
  ASSERT_EQ(f.tier->read(name, d, n), Status::kSuccess);  // incomplete: the backing, never admitted
  EXPECT_EQ(n, 2048u);
  EXPECT_EQ(0, memcmp(d, bytes.data(), 2048));
  delete[] d;
  EXPECT_EQ(f.backing->full_reads, 1);
  EXPECT_EQ(f.tier->stats().entries, 0u);
  f.readAt(name, bytes, 0, 8);
  f.readAt(name, bytes, 1024, 8);  // both chunks present now
  ASSERT_EQ(f.tier->read(name, d, n), Status::kSuccess);  // complete: local
  EXPECT_EQ(0, memcmp(d, bytes.data(), 2048));
  delete[] d;
  EXPECT_EQ(f.backing->full_reads, 1);
  EXPECT_EQ(f.tier->stats().hits, 1u);
}

TEST(DiskCacheStorageTest, ChunkFrequencyAdmissionKeepsTheHotterChunk) {
  TierFixture f(2048, 64u << 20, true, Adm::kFrequency, Mode::kChunk, /*entry=*/1024);
  std::string const name = "sstable1/" + stamp() + ".sst";
  auto bytes = f.seed(name, 4096);
  auto chunk = [&](size_t c) { f.readAt(name, bytes, c * 1024 + 1, 8); };
  chunk(0);  // free budget
  chunk(1);  // free budget
  chunk(2);  // 2 (one access) is not above the victim (one access): refused, still fetched
  auto s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.fetch_bytes, 3 * 1024u);
  chunk(2);  // 2 accesses > 1: admitted, one victim evicted
  s = f.tier->stats();
  EXPECT_EQ(s.entries, 2u);
  EXPECT_EQ(s.evictions, 1u);
  EXPECT_EQ(s.admit_rejected, 1u);
  int const before = f.backing->ranged_reads;
  chunk(2);  // a hit now
  EXPECT_EQ(f.backing->ranged_reads, before);
}

TEST(DiskCacheStorageTest, ChunkWriteThroughUnderFrequencyAdmissionTakesOnlyFreeBudget) {
  TierFixture f(2048, 64u << 20, true, Adm::kFrequency, Mode::kChunk, /*entry=*/1024);
  std::string const x = "sstable1/" + stamp() + "_x.sst";
  std::string const y = "sstable1/" + stamp() + "_y.sst";
  auto bx = f.seed(x, 2048);
  f.readAt(x, bx, 0, 8);
  f.readAt(x, bx, 1024, 8);  // the budget is full
  std::vector<unsigned char> v(1000, 'y');
  ASSERT_EQ(f.tier->appendNoFlush(y, v.data(), 1000), Status::kSuccess);
  ASSERT_EQ(f.tier->flush(y), Status::kSuccess);
  EXPECT_FALSE(f.local(y));
  EXPECT_FALSE(f.part(y));
  auto s = f.tier->stats();
  EXPECT_EQ(s.admit_rejected, 1u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.entries, 2u);
  EXPECT_TRUE(f.backing->exist(y));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Filter `DiskCacheStorageTest.Chunk*`. Expected: the four new tests fail (a write-through
in chunk mode is not indexed yet, and the full read goes to the backing every time).

- [ ] **Step 3: Implement**

Header, private:

```cpp
  // Under mtx_, chunk mode. Indexes localPath(name) as a complete file of
  // `bytes` (every chunk kPresent | kReferenced), taking budget under `how`
  // chunk by chunk through the CLOCK hand. False (nothing indexed) when refused.
  bool admitWholeFileLocked(std::string const& name, size_t bytes, AdmitMode how);
```

Implementation:

```cpp
bool DiskCacheStorage::admitWholeFileLocked(std::string const& name, size_t bytes, AdmitMode how) {
  if (bytes > options_.capacity_bytes) return false;
  dropChunkFileLocked(name, /*unlink=*/false, false);  // a re-created name: the new bytes are already in place
  if (current_bytes_ + bytes > options_.capacity_bytes) {
    if (how != AdmitMode::kForce) {  // a write-through never contests: no history
      admit_rejected_.fetch_add(1);
      return false;
    }
    while (current_bytes_ + bytes > options_.capacity_bytes) {
      std::string vname;
      size_t vchunk = 0;
      if (!clockVictimLocked(vname, vchunk)) {
        admit_rejected_.fetch_add(1);
        return false;
      }
      evictChunkLocked(vname, vchunk);
    }
  }
  ChunkFile& cf = chunks_[name];
  cf.size = bytes;
  cf.state.assign(chunkCount(bytes), static_cast<uint8_t>(kPresent | kReferenced));
  cf.present = cf.state.size();
  cf.pending = 0;
  cf.ring = ring_.size();
  ring_.push_back(name);
  present_chunks_ += cf.present;
  current_bytes_ += bytes;
  return true;
}
```

`publishPartFile`, in place of the `admit(...)` call, in chunk mode:

```cpp
  bool admitted;
  if (options_.mode == Mode::kChunk) {
    std::lock_guard<std::mutex> lk(mtx_);
    admitted = admitWholeFileLocked(name, static_cast<size_t>(bytes), how);
  } else {
    admitted = admit(name, static_cast<size_t>(bytes), how);
  }
  if (!admitted) {
    fs::remove(localPath(name), ec);
    return false;
  }
  return true;
```

The full-file `read(fileName, data, size)`, chunk-mode branch placed *before* the
`recordAccess(fileName)` call (a whole-file read is a compaction scan, not an access the
sketch must learn):

```cpp
  if (options_.mode == Mode::kChunk) {
    size_t total = 0;
    bool complete = false;
    {
      std::lock_guard<std::mutex> lk(mtx_);
      auto it = chunks_.find(fileName);
      if (it != chunks_.end() && it->second.present == it->second.state.size()) {
        complete = true;
        total = it->second.size;
        for (uint8_t& st : it->second.state) st |= kReferenced;
      }
    }
    if (complete && readLocal(fileName, data, 0, total)) {
      size = total;
      hits_.fetch_add(1);
      hit_bytes_.fetch_add(total);
      return Status::kSuccess;
    }
    // A whole-file read is a compaction input on its way out: never admitted.
    Status s = backing_->read(fileName, data, size);
    misses_.fetch_add(1);
    if (s == Status::kSuccess) {
      miss_bytes_.fetch_add(size);
      fetch_bytes_.fetch_add(size);
    }
    return s;
  }
```

Constructor: the sketch unit becomes
`uint64_t const unit = options_.mode == Mode::kChunk ? options_.entry_bytes : (64u << 20);`.

- [ ] **Step 4: Run the tests to verify they pass**

Filter `DiskCacheStorageTest.*:FrequencySketchTest.*`. Expected: everything passes. Then run
the wider filter `DiskCacheStorageTest.*:FrequencySketchTest.*:LRUCacheTest.*:SSTableTest.*:StorageTest.*:CheckpointTest.*`
once. Expected: no failure.

- [ ] **Step 5: Commit**

```bash
git add src/include/ozonedb/disk_cache_storage.h src/db/disk_cache_storage.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk-cache: chunk-mode write-through and whole-file reads, per-chunk admission contest (PLAN-disk-cache-2 T4)"
```

---

### Task 5: Config keys and DB wiring

**Files:**
- Modify: `src/include/ozonedb/metadata.h:111-118` (fields) and `:281-302` (parse)
- Modify: `src/db/db.cpp:83-94`
- Test: `tests/test_disk_cache_storage.cpp` (`MetadataParsesTheDiskCacheKeys`)

**Interfaces:**
- Consumes: `Options::mode`, `entry_bytes`, `admission`, `admit_window` (Tasks 2 and 3).
- Produces: `Metadata::disk_cache_mode` (string), `disk_cache_entry_bytes`, `disk_cache_admission` (string), `disk_cache_admit_window`. Task 6 writes these keys into the per-writer config.

- [ ] **Step 1: Extend the parse test**

In the explicit-value block add the four keys with non-default values and assert them:

```cpp
        "\"disk_cache_fill_queue\": \"8\",\n"
        "\"disk_cache_mode\": \"chunk\",\n"
        "\"disk_cache_entry_bytes\": \"16384\",\n"
        "\"disk_cache_admission\": \"frequency\",\n"
        "\"disk_cache_admit_window\": \"4096\""));
    ...
    EXPECT_EQ(md.disk_cache_mode, "chunk");
    EXPECT_EQ(md.disk_cache_entry_bytes, 16384ull);
    EXPECT_EQ(md.disk_cache_admission, "frequency");
    EXPECT_EQ(md.disk_cache_admit_window, 4096ull);
```

In the defaults block:

```cpp
    EXPECT_EQ(md.disk_cache_mode, "file");
    EXPECT_EQ(md.disk_cache_entry_bytes, 65536ull);
    EXPECT_EQ(md.disk_cache_admission, "always");
    EXPECT_EQ(md.disk_cache_admit_window, 0ull);
```

Before the `no_backend` block add the rejections (each on the tier-enabled `base` with
`disk_cache_dir` and `disk_cache_bytes` set, as one helper string `tier_on`):

```cpp
  std::string const tier_on = base + ",\n\"disk_cache_dir\": \"/tank/cache/w0\",\n\"disk_cache_bytes\": \"1\"";
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_mode\": \"block\"")), std::runtime_error);
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_admission\": \"lru\"")), std::runtime_error);
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_entry_bytes\": \"3000\"")), std::runtime_error);  // not a power of two
  EXPECT_THROW(Metadata(write(tier_on + ",\n\"disk_cache_entry_bytes\": \"2048\"")), std::runtime_error);  // below 4096
```

- [ ] **Step 2: Run the test to verify it fails**

Filter `DiskCacheStorageTest.MetadataParsesTheDiskCacheKeys`. Expected: a compile error on
`disk_cache_mode`.

- [ ] **Step 3: Implement**

Fields, after `disk_cache_fill_queue`:

```cpp
  // Round 2 (bench/PLAN-disk-cache-2.md): "file" keeps whole SSTables, "chunk"
  // keeps disk_cache_entry_bytes pieces of them; "frequency" is TinyLFU
  // admission, "always" the round-1 evict-to-fit.
  std::string disk_cache_mode = "file";
  uint64_t disk_cache_entry_bytes = 65536;
  std::string disk_cache_admission = "always";
  uint64_t disk_cache_admit_window = 0;
```

Parse, after the `disk_cache_fill_queue` block and before the two existing throws:

```cpp
    if (auto it = result.find("disk_cache_mode"); it != result.end()) {
      disk_cache_mode = it->second;
    }
    if (auto it = result.find("disk_cache_entry_bytes"); it != result.end()) {
      disk_cache_entry_bytes = std::stoull(it->second);
    }
    if (auto it = result.find("disk_cache_admission"); it != result.end()) {
      disk_cache_admission = it->second;
    }
    if (auto it = result.find("disk_cache_admit_window"); it != result.end()) {
      disk_cache_admit_window = std::stoull(it->second);
    }
    if (disk_cache_mode != "file" && disk_cache_mode != "chunk") {
      throw std::runtime_error("disk_cache_mode must be \"file\" or \"chunk\", got \"" + disk_cache_mode + "\"");
    }
    if (disk_cache_admission != "always" && disk_cache_admission != "frequency") {
      throw std::runtime_error("disk_cache_admission must be \"always\" or \"frequency\", got \"" + disk_cache_admission + "\"");
    }
    if (disk_cache_entry_bytes < 4096 || (disk_cache_entry_bytes & (disk_cache_entry_bytes - 1)) != 0) {
      throw std::runtime_error("disk_cache_entry_bytes must be a power of two of at least 4096");
    }
```

`db.cpp`, after `o.max_queue = ...`:

```cpp
    o.mode = this->metadata->disk_cache_mode == "chunk" ? DiskCacheStorage::Mode::kChunk : DiskCacheStorage::Mode::kFile;
    o.entry_bytes = static_cast<size_t>(this->metadata->disk_cache_entry_bytes);
    o.admission = this->metadata->disk_cache_admission == "frequency" ? DiskCacheStorage::Admission::kFrequency : DiskCacheStorage::Admission::kAlways;
    o.admit_window = this->metadata->disk_cache_admit_window;
```

- [ ] **Step 4: Run the tests to verify they pass**

Filter `DiskCacheStorageTest.*`. Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/include/ozonedb/metadata.h src/db/db.cpp tests/test_disk_cache_storage.cpp
git commit -m "disk-cache: disk_cache_mode / entry_bytes / admission / admit_window config keys (PLAN-disk-cache-2 T5)"
```

---

### Task 6: Runner flags, label tokens, extractor columns, model variant filter

**Files:**
- Modify: `bench/scripts/local/run_multinode_ycsb_with_corfu.sh:82-84,151-153,192-194,384-386`
- Modify: `bench/scripts/local/run_multinode_ycsb.py:220-253,285-286,555-560,705-745`
- Modify: `bench/scripts/local/run_local_ycsb_multiproc.py:425-432,506-507`
- Modify: `bench/scripts/local/load_local_ycsb_multiproc.py:242-265,460-477,975-985,1050-1051`
- Modify: `bench/scripts/extract_cost_coefficients.py:83-93,195,531-562,699-703`
- Modify: `bench/scripts/plot/plot_cost_model.py:200-280` and its argument parser
- Modify: `bench/scripts/plot/combine_disk_corpus.py` (`SOURCES`)

**Interfaces:**
- Consumes: the config keys (Task 5), the stats line (Tasks 2 to 4).
- Produces: flags `--disk-cache-mode`, `--disk-cache-entry-bytes`, `--disk-cache-admission`; `disk_cache_corfu_settings(corfu_settings, disk_cache_bytes, disk_cache_dir=None, keep_pages=False, mode=None, entry_bytes=None, admission=None)`; label tokens `-ch<entry>` and `-adm`; TSV columns `disk_fetch_bytes`, `disk_amp`, `disk_admit_rejected`, `disk_entries`, `disk_mode`, `disk_entry_bytes`, `disk_punch_failed`; `plot_cost_model.py --tier-variant TOKENS`; `tier_variant(label)`.

- [ ] **Step 1: The settings helper and the label token**

```python
def disk_cache_corfu_settings(corfu_settings, disk_cache_bytes, disk_cache_dir=None, keep_pages=False,
                              mode=None, entry_bytes=None, admission=None):
    """`--disk-cache-bytes N [--disk-cache-dir DIR] [--disk-cache-keep-pages]
    [--disk-cache-mode file|chunk] [--disk-cache-entry-bytes N]
    [--disk-cache-admission always|frequency]` as corfu settings. Every writer
    gets `<DIR>/w{i}/`, wiped before each phase (bench/PLAN-disk-cache.md, Task 8;
    the round-2 keys are bench/PLAN-disk-cache-2.md, Task 6)."""
    n = int(disk_cache_bytes)
    if n <= 0:
        raise ValueError("--disk-cache-bytes must be a positive byte count")
    s = dict(corfu_settings or {})
    s["disk_cache_bytes"] = n
    s["disk_cache_dir"] = disk_cache_dir or s.get("disk_cache_dir") or DEFAULT_DISK_CACHE_DIR
    s["disk_cache_drop_pages"] = not keep_pages
    if mode is not None:
        if mode not in ("file", "chunk"):
            raise ValueError("--disk-cache-mode must be file or chunk")
        s["disk_cache_mode"] = mode
    if entry_bytes is not None:
        e = int(entry_bytes)
        if e < 4096 or e & (e - 1):
            raise ValueError("--disk-cache-entry-bytes must be a power of two of at least 4096")
        s["disk_cache_entry_bytes"] = e
    if admission is not None:
        if admission not in ("always", "frequency"):
            raise ValueError("--disk-cache-admission must be always or frequency")
        s["disk_cache_admission"] = admission
    return s


def disk_cache_label_token(corfu_settings):
    """`-dc512m`, `-dc512m-ch64k-adm`, `-dc2g-kp`: the capacity, then `-ch<entry>`
    in chunk mode, `-adm` under frequency admission, `-kp` when the page cache
    is kept. `` when the tier is off."""
    s = corfu_settings or {}
    if not s.get("disk_cache_bytes"):
        return ""
    token = "-dc" + size_label_token(s["disk_cache_bytes"])
    if s.get("disk_cache_mode") == "chunk":
        token += "-ch" + size_label_token(s.get("disk_cache_entry_bytes") or 65536)
    if s.get("disk_cache_admission") == "frequency":
        token += "-adm"
    if not _truthy(s.get("disk_cache_drop_pages", True)):
        token += "-kp"
    return token
```

Per-writer config (`load_local_ycsb_multiproc.py:470-477`): after `disk_cache_drop_pages`
add

```python
            for k in ("disk_cache_mode", "disk_cache_entry_bytes", "disk_cache_admission", "disk_cache_admit_window"):
                if corfu_settings.get(k) is not None:
                    data[k] = str(corfu_settings[k])
```

and extend the `pop` tuple in the `else` branch with the same four keys.

- [ ] **Step 2: The flags on the three layers**

`load_local_ycsb_multiproc.py` and `run_local_ycsb_multiproc.py` argument parsers, next to
`--disk-cache-keep-pages`:

```python
    parser.add_argument("--disk-cache-mode", choices=("file", "chunk"), default=None,
                        help="Tier entry unit: whole SSTables (file, the default) or disk_cache_entry_bytes chunks; label -ch<entry>")
    parser.add_argument("--disk-cache-entry-bytes", type=int, default=None,
                        help="Chunk size in chunk mode (power of two >= 4096, engine default 65536)")
    parser.add_argument("--disk-cache-admission", choices=("always", "frequency"), default=None,
                        help="always: evict to fit (default); frequency: TinyLFU contest for non-free budget; label -adm")
```

Both call sites of `disk_cache_corfu_settings` pass
`mode=args.disk_cache_mode, entry_bytes=args.disk_cache_entry_bytes, admission=args.disk_cache_admission`.

`run_multinode_ycsb.py`: the same three arguments on its parser, three new keyword
parameters on `build_remote_command`, forwarded as

```python
            + (["--disk-cache-mode", disk_cache_mode] if disk_cache_mode else [])
            + (["--disk-cache-entry-bytes", str(int(disk_cache_entry_bytes))] if disk_cache_entry_bytes is not None else [])
            + (["--disk-cache-admission", disk_cache_admission] if disk_cache_admission else [])
```

and passed at both `build_remote_command` call sites (load and run) and into
`disk_cache_corfu_settings`.

`run_multinode_ycsb_with_corfu.sh`: variables

```bash
DISK_CACHE_MODE=""         # --disk-cache-mode file|chunk: tier entry unit, label -ch<entry>
DISK_CACHE_ENTRY_BYTES=""  # --disk-cache-entry-bytes N: chunk size in chunk mode
DISK_CACHE_ADMISSION=""    # --disk-cache-admission always|frequency: label -adm
```

usage lines, `case` arms (`--disk-cache-mode) DISK_CACHE_MODE="$2"; shift 2 ;;` and the two
others), validation next to the `--disk-cache-bytes` check:

```bash
if [[ -n "$DISK_CACHE_MODE" && "$DISK_CACHE_MODE" != file && "$DISK_CACHE_MODE" != chunk ]]; then
  echo "--disk-cache-mode must be file or chunk (got: $DISK_CACHE_MODE)" >&2; exit 2
fi
if [[ -n "$DISK_CACHE_ADMISSION" && "$DISK_CACHE_ADMISSION" != always && "$DISK_CACHE_ADMISSION" != frequency ]]; then
  echo "--disk-cache-admission must be always or frequency (got: $DISK_CACHE_ADMISSION)" >&2; exit 2
fi
if [[ -n "$DISK_CACHE_ENTRY_BYTES" ]] && ! [[ "$DISK_CACHE_ENTRY_BYTES" =~ ^[1-9][0-9]*$ ]]; then
  echo "--disk-cache-entry-bytes must be a positive integer (got: $DISK_CACHE_ENTRY_BYTES)" >&2; exit 2
fi
```

and the `COMMON_ARGS` lines after the existing three:

```bash
[[ -n "$DISK_CACHE_BYTES" && -n "$DISK_CACHE_MODE" ]] && COMMON_ARGS+=(--disk-cache-mode "$DISK_CACHE_MODE")
[[ -n "$DISK_CACHE_BYTES" && -n "$DISK_CACHE_ENTRY_BYTES" ]] && COMMON_ARGS+=(--disk-cache-entry-bytes "$DISK_CACHE_ENTRY_BYTES")
[[ -n "$DISK_CACHE_BYTES" && -n "$DISK_CACHE_ADMISSION" ]] && COMMON_ARGS+=(--disk-cache-admission "$DISK_CACHE_ADMISSION")
```

Add `disk_cache_mode=${DISK_CACHE_MODE:-file}` and `disk_cache_admission=${DISK_CACHE_ADMISSION:-always}`
to the `=== sweep:` echo.

- [ ] **Step 3: Check the label and the dry run**

```bash
python3 -c "
import sys; sys.path.insert(0, 'bench/scripts/local')
from load_local_ycsb_multiproc import disk_cache_corfu_settings as s, disk_cache_label_token as t
print(t(s({}, 536870912)))
print(t(s({}, 536870912, admission='frequency')))
print(t(s({}, 536870912, mode='chunk')))
print(t(s({}, 536870912, mode='chunk', entry_bytes=16384, admission='frequency', keep_pages=True)))
"
```

Expected, one per line: `-dc512m`, `-dc512m-adm`, `-dc512m-ch64k`, `-dc512m-ch16k-adm-kp`.

```bash
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --lru-cache-bytes 8388608 \
  --disk-cache-bytes 536870912 --disk-cache-mode chunk --disk-cache-admission frequency \
  --workloads c --writers-list 1 --trial 1 --duration 600 --run-tag plan2-dry --dry-run
```

Expected: the printed remote command carries `--disk-cache-mode chunk --disk-cache-admission frequency`
and no `--disk-cache-entry-bytes`. Nothing runs.

- [ ] **Step 4: The extractor**

`DISK_RE`: append an optional tail after `capacity=(?P<capacity>\d+)`:

```python
    r"(?: fetch_bytes=(?P<fetch_bytes>\d+) admit_rejected=(?P<admit_rejected>\d+)"
    r" entries=(?P<entries>\d+) mode=(?P<mode>\w+) entry_bytes=(?P<entry_bytes>\d+)"
    r" punch_failed=(?P<punch_failed>\d+))?"
```

Where the match becomes a dict (line 195), convert every numeric group with
`int(v) if v is not None else 0`, and `mode` with `v or "file"`. In `build_row`, after
`disk_ratio`:

```python
            "disk_fetch_bytes": disk_sums["fetch_bytes"],
            # bytes pulled from the backing per byte the workload asked for:
            # ~100 for the round-1 partial tier, the chunk-to-block ratio in chunk mode
            "disk_amp": round(disk_sums["fetch_bytes"] / disk_sums["miss_bytes"], 2) if disk_sums["miss_bytes"] else "",
            "disk_admit_rejected": disk_sums["admit_rejected"],
            "disk_entries": disk_sums["entries"],
            "disk_mode": disks[0]["mode"],
            "disk_entry_bytes": disks[0]["entry_bytes"],
            "disk_punch_failed": disk_sums["punch_failed"],
            "disk_evicted_bytes": disk_sums["evicted_bytes"],  # parsed in round 1, never a column
```

`disk_sums` sums ints, so `mode` must be excluded from the sum comprehension
(`if k != "mode"`). Add the eight names to the column list after `"h_total"`.

Regression: re-extract the round-1 campaign and compare the shared columns:

```bash
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/disk-20260829-long bench/results/local --window 60 --tsv /tmp/plan2-disk1.tsv
python3 - <<'EOF'
import csv
old = list(csv.DictReader(open("bench/results-disk-20260829.tsv"), delimiter="\t"))
new = list(csv.DictReader(open("/tmp/plan2-disk1.tsv"), delimiter="\t"))
assert len(old) == len(new), (len(old), len(new))
for o, n in zip(old, new):
    for k in o:
        assert o[k] == n[k], (o["label"], o["workload"], k, o[k], n[k])
    if n["disk_capacity"]:
        assert n["disk_mode"] == "file" and n["disk_fetch_bytes"] == "0", n["label"]
print("shared columns identical; round-1 rows read as file mode")
EOF
```

If `bench/results/local/disk-20260829-long` is not on the Mac any more, pull it from one
client (`scp -r oliverr3@amd160.utah.cloudlab.us:ozonedb/bench/results/local/disk-20260829-long bench/results/local/`)
before this step. Expected: the last line prints.

- [ ] **Step 5: The model's variant filter and the corpus source**

`plot_cost_model.py`: a module-level helper and one argument.

```python
def tier_variant(label):
    """`ozonedb-corfu-lru8m-dc512m-ch64k-adm-kp` -> `ch64k-adm`: the tokens after
    `-dc<size>`, without `-kp` (the page-cache A/B is not a variant). `` for a
    round-1 label and for a label without a tier."""
    m = re.search(r"-dc\d+[gmkb](.*)$", label)
    if not m:
        return ""
    return "-".join(t for t in m.group(1).strip("-").split("-") if t and t != "kp")
```

```python
    ap.add_argument("--tier-variant", default="",
                    help="which disk-cache variant's rows feed the tier coefficients: the label tokens after -dc<size>, "
                         "e.g. ch64k-adm (default: the round-1 file-mode rows, whose labels carry no variant tokens)")
```

Where the `tier` row list is built (the rows with a `disk_capacity`), keep only
`tier_variant(r["label"]) == args.tier_variant`, pass the variant into the coefficient
loader, and append `, variant '<variant>'` to `src["h_disk"]`, `src["cpu_s_per_op_O_disk"]`
and `src["fill_get_per_op"]`. When the filter leaves no tier rows, print
`no disk-cache rows for --tier-variant '<v>'` and take the existing "no tier cells"
fallback.

`combine_disk_corpus.py`: add the round-2 file as the first source, same filter as the
round-1 line, with the file name taken from a `--disk2 PATH` argument (default
`bench/results-disk2-20260829.tsv`, skipped when absent). Keep the round-1 source.

Regression: the committed projection must not move.

```bash
python3 bench/scripts/plot/combine_disk_corpus.py /tmp/plan2-corpus.tsv
python3 bench/scripts/plot/plot_cost_model.py /tmp/plan2-corpus.tsv bench/scripts/plot/prices.json \
    --space bench/scripts/plot/space.json --out-dir /tmp/plan2-plot --table /tmp/plan2-projection.tsv
diff bench/results-disk-20260829-projection.tsv /tmp/plan2-projection.tsv && echo projection unchanged
```

Expected: `projection unchanged`. matplotlib lives in a venv on the Mac; create one with
`python3 -m venv /tmp/plan2-venv && /tmp/plan2-venv/bin/pip install matplotlib` if none is
at hand.

- [ ] **Step 6: Commit**

```bash
git add bench/scripts/local/run_multinode_ycsb_with_corfu.sh bench/scripts/local/run_multinode_ycsb.py \
        bench/scripts/local/run_local_ycsb_multiproc.py bench/scripts/local/load_local_ycsb_multiproc.py \
        bench/scripts/extract_cost_coefficients.py bench/scripts/plot/plot_cost_model.py bench/scripts/plot/combine_disk_corpus.py
git commit -m "bench: --disk-cache-mode/-entry-bytes/-admission through the chain, -ch/-adm label tokens, disk_fetch_bytes/disk_amp/... columns, model --tier-variant (PLAN-disk-cache-2 T6)"
```

---

### Task 7: Campaign `disk2`

**Files:**
- Create: `bench/results-disk2-<date>.tsv` (the extractor's output; `<date>` is the run day, for example `20260829`)
- Scratch: the chain script below, outside the repo

**Interfaces:**
- Consumes: the whole chain (Tasks 1 to 6), the cluster, the load snapshot in `/mnt/corfu/load` and the bucket snapshot the wrapper restores per cell.
- Produces: one result directory `bench/results/local/disk2-<date>-long/` on every client and on the Mac, and the TSV.

- [ ] **Step 1: Sync, build, and check the cluster is idle**

```bash
ansible-playbook bench/ansible/sync.yml -e build=true
```

Expected: `failed=0 unreachable=0` for all 9 hosts. Then confirm nobody else runs a driver:
`pgrep -u oliverr3 -f '[r]un_local_ycsb_multiproc|[o]zonedb-binding'` on every client must
print nothing. If it prints pids that are not yours, stop and report.

Run the unit tests on amd160 once more against the synced build (the filter from Task 4).
Expected: no failure.

- [ ] **Step 2: Write the chain**

```bash
#!/usr/bin/env bash
# chaindc3.sh -- PLAN-disk-cache-2 Task 7, campaign $TAG-long. Launch detached.
set -euo pipefail
export OZONEDB_HOME="${OZONEDB_HOME:?}"
export ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg
cd "$OZONEDB_HOME"
TAG="${TAG:-disk2-20260829}"
W=bench/scripts/local/run_multinode_ycsb_with_corfu.sh
FIRST="${FIRST:-1}"

cell() {
  if [ "$1" -lt "$FIRST" ]; then return 0; fi
  echo "##### $(date -u +%FT%TZ) cell $1: wl=$2 lru=$3 disk=$4 extra='${5:-}'"
  # shellcheck disable=SC2086
  bash "$W" --log-trim --lru-cache-bytes "$3" --disk-cache-bytes "$4" ${5:-} \
    --workloads "$2" --writers-list 1 --trial 1 --duration 600 --run-tag "$TAG-long"
}
ADM="--disk-cache-admission frequency"
CH="--disk-cache-mode chunk"
cell 1  c 8388608 536870912  "$ADM"                                        # file + admission
cell 2  c 8388608 536870912  "$CH"                                         # chunk 64k, always
cell 3  c 8388608 536870912  "$CH $ADM"                                    # chunk 64k + admission
cell 4  c 8388608 536870912  "$CH --disk-cache-entry-bytes 16384 $ADM"
cell 5  c 8388608 536870912  "$CH --disk-cache-entry-bytes 262144 $ADM"
cell 6  c 8388608 268435456  "$CH $ADM"
cell 7  c 8388608 2147483648 "$CH $ADM"                                    # full tier: regression
cell 8  a 8388608 536870912  "$ADM"
cell 9  a 8388608 536870912  "$CH $ADM"
cell 10 a 8388608 2147483648 "$CH $ADM"
echo "##### $(date -u +%FT%TZ) chaindc3 done"
```

Launch it in its own session so the harness cannot kill it:

```bash
python3 - <<'EOF'
import os, subprocess
t = os.environ["CLAUDE_JOB_DIR"] + "/tmp" if "CLAUDE_JOB_DIR" in os.environ else "/tmp"
log = open(f"{t}/chaindc3.log", "ab")
p = subprocess.Popen(["bash", f"{t}/chaindc3.sh"], stdout=log, stderr=subprocess.STDOUT,
                     stdin=subprocess.DEVNULL, start_new_session=True, cwd=t)  # OZONEDB_HOME must be exported
print("pid", p.pid, "log", f"{t}/chaindc3.log")
EOF
```

Ten cells take about 2.5 hours. Poll the log every 15 to 20 minutes with
`grep -c '^#####' chaindc3.log`; do not poll faster.

- [ ] **Step 3: Verify every cell, then extract**

Every cell must have 8 result files with the expected label token and 0 failed reads:

```bash
python3 bench/scripts/extract_cost_coefficients.py bench/results/local/disk2-20260829-long bench/results/local \
    --window 60 --tsv bench/results-disk2-20260829.tsv
python3 - <<'EOF'
import csv
rows = [r for r in csv.DictReader(open("bench/results-disk2-20260829.tsv"), delimiter="\t") if r.get("disk_capacity")]
assert len(rows) == 10, len(rows)
for r in rows:
    assert r["have"] == "8", (r["label"], r["workload"], r["have"])
    for k in ("failed", "disk_fill_failed", "disk_punch_failed"):
        assert r[k] in ("", "0"), (r["label"], r["workload"], k, r[k])
    print(f'{r["label"]:44s} {r["workload"]} ops/s={r["steady_ops_s"]:>8} disk_h={r["disk_h"]:>7} get/op={r["get_per_op"]:>7} '
          f'fill_get/op={r["disk_fill_gets_per_op"]:>7} amp={r["disk_amp"]:>6} evicted_GB={int(r["disk_evicted_bytes"] or 0)/1e9:6.1f} '
          f'egress_MB/s={float(r["s3_bytes_out_rate_steady"] or 0)/1e6:6.0f} cpu_ms/op={float(r["client_cpu_s_per_op"] or 0)*1e3:5.2f}')
EOF
```

A cell with `have < 8` is re-run alone (`FIRST=<n>` and a trailing `exit` after that cell),
never patched by hand.

- [ ] **Step 4: Commit**

```bash
git add bench/results-disk2-20260829.tsv
git commit -m "bench: campaign disk2-20260829 -- admission control and chunk entries on the 512 MB, 256 MB and 2 GB tiers (PLAN-disk-cache-2 T7)"
```

---

### Task 8: Model, write-up, defaults

**Files:**
- Create: `bench/results-disk2-<date>-projection.tsv`, `bench/results-disk2-<date>.png`
- Modify: `bench/RESULTS-cost.md` (new section after "Disk-cache tier", before "Method notes and caveats")
- Modify: `bench/PLAN-disk-cache.md` (status paragraph: point at this plan's result), this file (status paragraph at the top)
- Modify: `CLAUDE.md` ("Caching" paragraph)
- Modify: `src/include/ozonedb/metadata.h`, `src/include/ozonedb/disk_cache_storage.h`, `tests/test_disk_cache_storage.cpp` (only if the defaults flip)

**Interfaces:**
- Consumes: the TSV (Task 7), `--tier-variant` (Task 6).
- Produces: the projection and the text.

- [ ] **Step 1: The projection for the chunk + admission variant**

```bash
python3 bench/scripts/plot/combine_disk_corpus.py /tmp/plan2-corpus.tsv
python3 bench/scripts/plot/plot_cost_model.py /tmp/plan2-corpus.tsv bench/scripts/plot/prices.json \
    --space bench/scripts/plot/space.json --tier-variant ch64k-adm \
    --out-dir /tmp/plan2-plot --table bench/results-disk2-20260829-projection.tsv
cp /tmp/plan2-plot/cost_model.png bench/results-disk2-20260829.png
```

The coefficient printout must say `variant 'ch64k-adm'` on the three tier lines, and
`disk_h` must list the ratios 0.262, 0.524 and 2.097 from cells 6, 3 and 7. Record the
totals at 1 GB, 1 TB, 10 TB and 100 TB for `ozone_hi_cache` and `ozone_disk_cache`, and the
break-even data size. The `ozone_hi_cache` column must equal the round-1 projection's to
the dollar.

- [ ] **Step 2: The write-up**

A new section `## Disk-cache tier, round 2: admission control and chunk entries (campaign `disk2-<date>`)`
in `RESULTS-cost.md` with:

1. The cell table: label, workload, ops/s, `disk_h`, GETs per op, fill GETs per op,
   `disk_amp`, evicted GB, MinIO egress, client CPU per op. The round-1 512 MB and 2 GB
   cells and the no-tier controls as reference rows.
2. The goal table from this plan with a measured column and a met / missed column.
3. Findings, one paragraph each: what admission alone did on the file tier; what chunk
   entries alone did; the two together; the entry-size sweep (16, 64, 256 KiB); the
   full-tier regression check; workload a.
4. The projection: the four decades, the break-even, the sentence from "What the projection
   will say" confirmed or corrected by the numbers.
5. Caveats: one trial per cell; uniform keys; the `disk_h` clamp below the smallest ratio;
   `punch_failed` and `fill_failed` totals.
6. A Reproduce block in the round-1 form, with `--tier-variant ch64k-adm`.

- [ ] **Step 3: The defaults ruling**

Flip the engine defaults to `disk_cache_mode = chunk` and `disk_cache_admission = frequency`
only if all three hold: cell 3 beats cell 1 and the round-1 512 MB cell on ops/s and on CPU
per op; cell 9 is at or above the workload-a control (2,600 ops/s); cell 7 is at or above
44,000 ops/s. Then:

- `Options::mode = Mode::kChunk`, `Options::admission = Admission::kFrequency`,
  `Metadata::disk_cache_mode = "chunk"`, `Metadata::disk_cache_admission = "frequency"`;
- the defaults block of `MetadataParsesTheDiskCacheKeys` and the fixture default
  (`Mode::kFile`, `Admission::kAlways` stay the fixture defaults, so the round-1 tests keep
  their meaning; only the `Options` and `Metadata` defaults change);
- the label must name what ran. Make `disk_cache_corfu_settings` write `disk_cache_mode`
  and `disk_cache_admission` whenever the tier is on, with the new engine defaults when the
  caller gave none. A plain `--disk-cache-bytes` run is then labelled `-dc512m-ch64k-adm`,
  which is what it is;
- rerun the unit tests on amd160.

If any condition fails, leave the defaults and write the reason into the status paragraph.
Either way the decision and its evidence go into both plan files' status paragraphs.

- [ ] **Step 4: `CLAUDE.md`**

In the "Caching" paragraph, after the `disk_cache_fill_queue` sentence, add the four keys,
their defaults, the two label tokens, the three flags, and one sentence on each mechanism.
Keep the paragraph's density.

- [ ] **Step 5: Commit**

```bash
git add bench/RESULTS-cost.md bench/PLAN-disk-cache.md bench/PLAN-disk-cache-2.md CLAUDE.md \
        bench/results-disk2-20260829-projection.tsv bench/results-disk2-20260829.png
# plus the engine/test files if the defaults flipped
git commit -m "bench: disk2 write-up and projection; disk-cache defaults ruling (PLAN-disk-cache-2 T8)"
```

---

## Follow-ups this plan does not do

- A shared cross-client tier (hash-partitioned peers, or one cache node in front of MinIO).
  It is the only change that moves the 10 TB number under uniform keys.
- A skewed-key cell (`requestdistribution=hotspot`). Needs a runner flag for YCSB
  properties first.
- `SEEK_DATA` recovery of a chunk-mode tier at open, and an fd cache for the local files.
- Trials 2 and 3 of the winning cells.
