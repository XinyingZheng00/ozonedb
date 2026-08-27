#ifdef OZONEDB_ENABLE_CORFU
#include "checkpoint.h"
#include "corfu_storage.h"
#include "log_trimmer.h"
#include "gtest/gtest.h"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace ozonedb;

namespace {

class CorfuStorageEnv {
 public:
  static std::string endpoint() {
    char const* e = std::getenv("CORFU_TEST_ENDPOINT");
    return e ? std::string(e) : std::string();
  }
  static std::string jarPath() {
    char const* j = std::getenv("CORFU_BRIDGE_JAR");
    return j ? std::string(j) : std::string();
  }
  static bool available() {
    return !endpoint().empty() && !jarPath().empty();
  }
  static std::string uniqueStream(std::string const& prefix) {
    return prefix + "_" + std::to_string(time(nullptr));
  }
};

#define SKIP_IF_NO_CORFU()                                                                  \
  do {                                                                                      \
    if (!CorfuStorageEnv::available()) {                                                    \
      GTEST_SKIP() << "CORFU_TEST_ENDPOINT / CORFU_BRIDGE_JAR not set; skipping Corfu test"; \
    }                                                                                       \
  } while (0)

CorfuDBStorage* makeStorage(std::string const& stream) {
  return new CorfuDBStorage(
      CorfuStorageEnv::endpoint(),
      CorfuStorageEnv::jarPath(),
      /*jvm_opts=*/"-Xmx512m",
      stream,
      /*db_path=*/"test/");
}

}  // namespace

TEST(CorfuStorageTest, append_and_read_back) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_rw");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  EXPECT_EQ(Status::kSuccess, storage->append(file, data.data(), data.size()));

  unsigned char* read_data = nullptr;
  size_t size = 0;
  EXPECT_EQ(Status::kSuccess, storage->read(file, read_data, size));
  EXPECT_EQ(data.size(), size);
  for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], read_data[i]);
  delete[] read_data;
  delete storage;
}

TEST(CorfuStorageTest, partial_read) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_partial");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(Status::kSuccess, storage->append(file, data.data(), data.size()));
  }
  unsigned char* read_data = nullptr;
  EXPECT_EQ(Status::kSuccess, storage->read(file, read_data, 25, 5));
  for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], read_data[i]);
  delete[] read_data;
  delete storage;
}

TEST(CorfuStorageTest, seal_returns_kSealed_on_further_append) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_seal");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3};
  EXPECT_EQ(Status::kSuccess, storage->append(file, data.data(), data.size()));
  storage->seal(file);
  EXPECT_TRUE(storage->isSealed(file));
  EXPECT_EQ(Status::kSealed, storage->append(file, data.data(), data.size()));
  // idempotent
  storage->seal(file);
  EXPECT_TRUE(storage->isSealed(file));
  delete storage;
}

TEST(CorfuStorageTest, seal_persists_across_reopen) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_seal_reopen");
  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3};

  {
    Storage* s = makeStorage(stream);
    s->append(file, data.data(), data.size());
    s->seal(file);
    delete s;
  }
  {
    Storage* s = makeStorage(stream);
    // Give the tailer a moment to replay the stream.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(s->isSealed(file));
    delete s;
  }
}

TEST(CorfuStorageTest, batch_flush_round_trip) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_batch");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(Status::kSuccess, storage->appendNoFlush(file, data.data(), data.size()));
  }
  EXPECT_EQ(Status::kSuccess, storage->flush(file));

  unsigned char* read_data = nullptr;
  size_t size = 0;
  EXPECT_EQ(Status::kSuccess, storage->read(file, read_data, size));
  EXPECT_EQ(data.size() * 10, size);
  delete[] read_data;
  delete storage;
}

TEST(CorfuStorageTest, remove_clears_file) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_remove");
  Storage* storage = makeStorage(stream);

  std::string file = "file1";
  std::vector<uint8_t> data = {1, 2, 3};
  storage->append(file, data.data(), data.size());
  EXPECT_TRUE(storage->exist(file));
  storage->remove(file);
  EXPECT_FALSE(storage->exist(file));
  delete storage;
}

// Regression for the multi-writer read miss: an append that a peer's SEAL
// outran must not be acked. Writer A appends in a tight loop while B seals
// the same file a few milliseconds in. Whatever A got kSuccess for must be
// exactly what B (and everyone else) holds for the file — no more, no
// less. Before the ack waited for the tailer, A acked appends that were
// sequenced above B's SEAL whenever A's tailer had not applied the SEAL
// yet, and every other process dropped those bytes.
TEST(CorfuStorageTest, ack_never_outruns_a_peer_seal) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_seal_race");
  CorfuDBStorage* a = makeStorage(stream);
  CorfuDBStorage* b = makeStorage(stream);
  int rounds_with_seal_refusal = 0;
  for (int round = 0; round < 20; ++round) {
    std::string file = "f" + std::to_string(round);
    std::vector<unsigned char> acked;
    std::thread sealer([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(3 + round % 5));
      b->seal(file);
    });
    for (int i = 0; i < 400; ++i) {
      std::vector<unsigned char> p(16, static_cast<unsigned char>(i));
      Status s = a->appendInBatch(file, p.data(), static_cast<int>(p.size()));
      if (s == Status::kSuccess) {
        acked.insert(acked.end(), p.begin(), p.end());
        continue;
      }
      ASSERT_EQ(Status::kSealed, s) << "round " << round << " append #" << i;
      ++rounds_with_seal_refusal;
      break;
    }
    sealer.join();

    b->sync();
    unsigned char* data = nullptr;
    size_t size = 0;
    std::vector<unsigned char> seen;
    if (b->read(file, data, size) == Status::kSuccess) seen.assign(data, data + size);
    delete[] data;
    b->clearSync();
    EXPECT_EQ(acked.size(), seen.size()) << "round " << round;
    EXPECT_EQ(acked, seen) << "round " << round;

    // A's own copy agrees with the peer's.
    a->sync();
    data = nullptr;
    size = 0;
    std::vector<unsigned char> own;
    if (a->read(file, data, size) == Status::kSuccess) own.assign(data, data + size);
    delete[] data;
    a->clearSync();
    EXPECT_EQ(acked, own) << "round " << round;
  }
  // The seal must have cut A off in most rounds, or the race was not
  // exercised.
  EXPECT_GE(rounds_with_seal_refusal, 10);
  delete b;
  delete a;
}

namespace {
std::vector<unsigned char> readAllFenced(CorfuDBStorage* s, std::string const& file) {
  s->sync();
  unsigned char* data = nullptr;
  size_t size = 0;
  std::vector<unsigned char> out;
  if (s->read(file, data, size) == Status::kSuccess) out.assign(data, data + size);
  delete[] data;
  s->clearSync();
  return out;
}
}  // namespace

// The fast path (PLAN-trimming.md §0): a data append is acked on the
// sequencer's answer, not after the tailer applied it, because its token
// request carries the file's conflict key. Checks that the fast path is
// taken on a warm stream, that a peer's SEAL turns the next append into
// kSealed with no bytes landing (the sequencer refused the token, or the
// seal was already known locally), that a second file keeps flowing fast,
// and that both processes read exactly the acked bytes.
TEST(CorfuStorageTest, fast_ack_taken_and_refused_by_a_peer_seal) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_fast_ack");
  CorfuDBStorage* a = makeStorage(stream);
  CorfuDBStorage* b = makeStorage(stream);
  std::vector<unsigned char> acked;
  auto put = [&](std::string const& file, int i) {
    std::vector<unsigned char> p(16, static_cast<unsigned char>(i));
    Status s = a->appendInBatch(file, p.data(), static_cast<int>(p.size()));
    if (s == Status::kSuccess && file == "f") acked.insert(acked.end(), p.begin(), p.end());
    return s;
  };
  // Warm-up: the first appends of a fresh stream may take the slow path
  // (a snapshot below the tail at sequencer bootstrap).
  for (int i = 0; i < 5; ++i) ASSERT_EQ(Status::kSuccess, put("f", i));
  uint64_t const fast_before = a->fastAcks();
  for (int i = 5; i < 50; ++i) ASSERT_EQ(Status::kSuccess, put("f", i));
  EXPECT_GE(a->fastAcks() - fast_before, 40u) << "fast path not taken on a warm stream";
  EXPECT_EQ(0u, a->spuriousConflicts());

  b->seal("f");
  std::vector<unsigned char> p(16, 0xEE);
  EXPECT_EQ(Status::kSealed, a->appendInBatch("f", p.data(), static_cast<int>(p.size())));
  EXPECT_EQ(Status::kSealed, a->appendInBatch("f", p.data(), static_cast<int>(p.size())));

  uint64_t const fast_mid = a->fastAcks();
  for (int i = 0; i < 20; ++i) ASSERT_EQ(Status::kSuccess, put("g", i));
  EXPECT_GE(a->fastAcks() - fast_mid, 18u) << "fast path lost after a peer seal";

  EXPECT_EQ(acked, readAllFenced(b, "f"));
  EXPECT_EQ(acked, readAllFenced(a, "f"));
  EXPECT_EQ(50u * 16u, acked.size());
  EXPECT_EQ(20u * 16u, readAllFenced(b, "g").size());
  EXPECT_EQ(0u, a->droppedAboveSeal()) << "a refused token must never land";
  EXPECT_EQ(0u, b->droppedAboveSeal());
  delete b;
  delete a;
}

// ---------------------------------------------------------------------------
// Checkpoints and trimming (PLAN-trimming.md). These trim the shared test
// server's log, so they sit at the end of the file: a later test on a
// fresh stream is unaffected (the sequencer's per-stream mark for a new
// stream is empty), but a reopen of an EARLIER stream would not be.
// ---------------------------------------------------------------------------
namespace {

std::string checkpointRoot(std::string const& tag) {
  auto p = std::filesystem::temp_directory_path() /
           ("ozonedb_corfu_ckpt_" + std::to_string(getpid()) + "_" + tag);
  std::filesystem::remove_all(p);
  std::filesystem::create_directories(p);
  return p.string() + "/";
}

CorfuDBStorage* makeStorageWithStore(std::string const& stream, Storage* store) {
  return new CorfuDBStorage(
      CorfuStorageEnv::endpoint(),
      CorfuStorageEnv::jarPath(),
      /*jvm_opts=*/"-Xmx512m",
      stream,
      /*db_path=*/"test/",
      store,
      "checkpoint");
}

std::vector<unsigned char> readAll(Storage* s, std::string const& file) {
  unsigned char* data = nullptr;
  size_t size = 0;
  std::vector<unsigned char> out;
  if (s->read(file, data, size) == Status::kSuccess) out.assign(data, data + size);
  delete[] data;
  return out;
}

std::vector<uint8_t> payload(int i) {
  return {static_cast<uint8_t>(i & 0xff), static_cast<uint8_t>((i >> 8) & 0xff), 7, 9};
}

void appendN(Storage* s, std::string const& file, int from, int to) {
  for (int i = from; i < to; ++i) {
    auto p = payload(i);
    ASSERT_EQ(Status::kSuccess, s->append(file, p.data(), p.size())) << file << " #" << i;
  }
}

}  // namespace

TEST(CorfuStorageTest, join_from_checkpoint) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_join");
  std::string root = checkpointRoot("join");
  FileStorage store(root);
  CorfuDBStorage* a = makeStorage(stream);
  std::string const f1 = "datalog/1", f2 = "datalog/2", f3 = "datalog/3";

  appendN(a, f1, 0, 200);
  appendN(a, f2, 0, 50);
  a->seal(f2);
  appendN(a, f3, 0, 1);
  a->remove(f3);

  checkpoint::State snap = a->takeSnapshot();
  ASSERT_GE(snap.covered_addr, 0);
  EXPECT_EQ(2u, snap.files.size());
  EXPECT_EQ(800u, snap.files[f1].size());
  EXPECT_EQ(1u, snap.sealed.count(f2));
  EXPECT_GE(snap.sealed[f2], 0);  // our own SEAL address is known
  EXPECT_EQ(1u, snap.removed.count(f3));
  snap.creator = "test";
  ASSERT_EQ(Status::kSuccess, checkpoint::write(store, "checkpoint", snap));
  ASSERT_TRUE(a->prefixTrim(snap.covered_addr));
  EXPECT_EQ(snap.covered_addr + 1, a->trimMark());

  // Writes after the trim land above the checkpoint; the joiner must
  // replay exactly those.
  appendN(a, f1, 200, 300);

  CorfuDBStorage* b = makeStorageWithStore(stream, &store);
  EXPECT_TRUE(b->loadedFromCheckpoint());
  EXPECT_FALSE(b->trimmedOut());
  EXPECT_EQ(readAll(a, f1), readAll(b, f1));
  EXPECT_EQ(1200u, readAll(b, f1).size());
  EXPECT_EQ(200u, readAll(b, f2).size());
  EXPECT_TRUE(b->isSealed(f2));
  EXPECT_FALSE(b->exist(f3));
  {
    auto p = payload(0);
    EXPECT_EQ(Status::kSealed, b->append(f2, p.data(), p.size()));
  }
  // A live append after the join is visible to the joiner (fenced read).
  appendN(a, f1, 300, 301);
  EXPECT_EQ(readAll(a, f1), readAll(b, f1));
  // And the joiner's own appends are visible to the original member.
  appendN(b, f1, 301, 302);
  EXPECT_EQ(readAll(b, f1), readAll(a, f1));

  delete b;
  delete a;
  std::filesystem::remove_all(root);
}

TEST(CorfuStorageTest, trimmed_stream_without_checkpoint_refuses_to_open) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_trim_nockpt");
  std::string root = checkpointRoot("nockpt");
  FileStorage store(root);
  CorfuDBStorage* a = makeStorage(stream);
  appendN(a, "f", 0, 50);
  checkpoint::State snap = a->takeSnapshot();
  ASSERT_GE(snap.covered_addr, 0);
  ASSERT_EQ(Status::kSuccess, checkpoint::write(store, "checkpoint", snap));
  ASSERT_TRUE(a->prefixTrim(snap.covered_addr));
  appendN(a, "f", 50, 60);

  // No checkpoint store: a replay from address 0 cannot be complete.
  EXPECT_THROW({ delete makeStorage(stream); }, std::runtime_error);
  // With the store it opens, and holds everything.
  CorfuDBStorage* b = makeStorageWithStore(stream, &store);
  EXPECT_EQ(240u, readAll(b, "f").size());
  delete b;
  delete a;
  std::filesystem::remove_all(root);
}

TEST(CorfuStorageTest, snapshot_under_concurrent_writes_is_a_prefix) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_snap_conc");
  std::string root = checkpointRoot("conc");
  FileStorage store(root);
  CorfuDBStorage* a = makeStorage(stream);

  constexpr int kWrites = 1500;
  std::thread writer([&] {
    for (int i = 0; i < kWrites; ++i) {
      auto p = payload(i);
      if (a->append("f", p.data(), p.size()) != Status::kSuccess) break;
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  checkpoint::State snap = a->takeSnapshot();
  writer.join();
  ASSERT_GE(snap.covered_addr, 0);
  ASSERT_EQ(Status::kSuccess, checkpoint::write(store, "checkpoint", snap));
  ASSERT_TRUE(a->prefixTrim(snap.covered_addr));

  // The joiner restores the snapshot and replays covered_addr+1..tail. If
  // the snapshot were not an exact prefix, bytes would be duplicated or
  // missing and the two reads would differ.
  CorfuDBStorage* b = makeStorageWithStore(stream, &store);
  EXPECT_EQ(static_cast<size_t>(kWrites) * 4, readAll(a, "f").size());
  EXPECT_EQ(readAll(a, "f"), readAll(b, "f"));
  delete b;
  delete a;
  std::filesystem::remove_all(root);
}

TEST(CorfuStorageTest, trimmer_two_cycles) {
  SKIP_IF_NO_CORFU();
  std::string stream = CorfuStorageEnv::uniqueStream("corfu_trimmer");
  std::string root = checkpointRoot("trimmer");
  FileStorage store(root);
  CorfuDBStorage* a = makeStorage(stream);
  long c1 = -1, c2 = -1;
  {
    LogTrimmer::Options options;
    options.dir = "checkpoint";
    options.interval_ms = 3600 * 1000;  // never fires on its own here
    options.min_entries = 10;
    options.keep = 2;
    options.creator = "test";
    LogTrimmer trimmer(a, &store, options);

    // Nothing to checkpoint yet.
    EXPECT_FALSE(trimmer.runCycle(false));

    appendN(a, "f", 0, 20);
    ASSERT_TRUE(trimmer.runCycle(false));
    c1 = trimmer.lastCheckpointAddr();
    EXPECT_GE(c1, 0);
    EXPECT_EQ(-1, trimmer.lastTrimAddr());  // first cycle trims nothing
    EXPECT_EQ(1u, trimmer.checkpointsKept());

    appendN(a, "f", 20, 40);
    ASSERT_TRUE(trimmer.runCycle(false));
    c2 = trimmer.lastCheckpointAddr();
    EXPECT_GT(c2, c1);
    EXPECT_EQ(c1, trimmer.lastTrimAddr());  // trimmed behind the PREVIOUS one
    EXPECT_EQ(c1 + 1, a->trimMark());
    EXPECT_EQ(2u, trimmer.checkpointsKept());

    // Too little since the last checkpoint: no cycle.
    EXPECT_FALSE(trimmer.runCycle(false));

    appendN(a, "f", 40, 60);
    ASSERT_TRUE(trimmer.runCycle(true));
    EXPECT_EQ(2u, trimmer.checkpointsKept());
    EXPECT_EQ(c2, trimmer.lastTrimAddr());
    checkpoint::State gone;
    EXPECT_NE(Status::kSuccess, checkpoint::read(store, "checkpoint", c1, gone));
    checkpoint::State kept;
    EXPECT_EQ(Status::kSuccess, checkpoint::read(store, "checkpoint", c2, kept));
  }
  // A restarted trimmer picks the chain up from LATEST.
  {
    LogTrimmer::Options options;
    options.dir = "checkpoint";
    options.min_entries = 10;
    LogTrimmer again(a, &store, options);
    EXPECT_GT(again.lastCheckpointAddr(), c2);
    EXPECT_EQ(2u, again.checkpointsKept());
  }
  CorfuDBStorage* b = makeStorageWithStore(stream, &store);
  EXPECT_TRUE(b->loadedFromCheckpoint());
  EXPECT_EQ(240u, readAll(b, "f").size());
  EXPECT_EQ(readAll(a, "f"), readAll(b, "f"));
  delete b;
  delete a;
  std::filesystem::remove_all(root);
}

#endif  // OZONEDB_ENABLE_CORFU
