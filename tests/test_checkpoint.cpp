// Checkpoint format round trip over FileStorage. No Corfu, no S3: the
// writer and reader only use the Storage interface, so the format is
// tested here and the Corfu join path is tested in test_corfu_storage.cpp.
#include "checkpoint.h"
#include "storage.h"
#include "gtest/gtest.h"
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

using namespace ozonedb;

namespace {

std::string freshRoot(std::string const& tag) {
  auto p = std::filesystem::temp_directory_path() /
           ("ozonedb_ckpt_" + std::to_string(getpid()) + "_" + tag);
  std::filesystem::remove_all(p);
  std::filesystem::create_directories(p);
  return p.string() + "/";
}

std::vector<unsigned char> pattern(size_t n, unsigned char seed) {
  std::vector<unsigned char> v(n);
  for (size_t i = 0; i < n; ++i) v[i] = static_cast<unsigned char>(seed + i * 7);
  return v;
}

checkpoint::State sampleState(long covered, long prev) {
  checkpoint::State st;
  st.covered_addr = covered;
  st.prev_covered_addr = prev;
  st.creator = "test";
  st.files["datalog/1"] = pattern(70000, 1);
  st.files["datalog/2"] = pattern(333, 2);
  st.files["metadata.log"] = pattern(128, 3);
  st.sealed["datalog/1"] = 17;
  st.sealed["datalog/0"] = 3;  // sealed, never appended: no buffer
  st.removed.insert("old/7");
  return st;
}

// A FileStorage that fails to flush one key. Lets the test check that
// LATEST is written last: a failure before it must leave no visible
// checkpoint.
class FailingFlushStorage : public FileStorage {
 public:
  explicit FailingFlushStorage(std::string const& path, std::string needle)
      : FileStorage(path), needle_(std::move(needle)) {}
  Status flush(std::string const& fileName) override {
    if (fileName.find(needle_) != std::string::npos) return Status::kFailure;
    return FileStorage::flush(fileName);
  }

 private:
  std::string needle_;
};

}  // namespace

TEST(CheckpointTest, no_checkpoint_is_not_an_error) {
  std::string root = freshRoot("none");
  FileStorage store(root);
  long addr = 99;
  bool found = true;
  EXPECT_EQ(Status::kSuccess, checkpoint::readLatestAddr(store, "checkpoint", addr, found));
  EXPECT_FALSE(found);
  EXPECT_EQ(-1, addr);
  checkpoint::State st;
  EXPECT_EQ(Status::kSuccess, checkpoint::readLatest(store, "checkpoint", st, found));
  EXPECT_FALSE(found);
  std::filesystem::remove_all(root);
}

TEST(CheckpointTest, round_trip_file_storage) {
  std::string root = freshRoot("rt");
  FileStorage store(root);
  checkpoint::State st = sampleState(41, -1);
  ASSERT_EQ(Status::kSuccess, checkpoint::write(store, "checkpoint", st));

  long addr = -1;
  bool found = false;
  ASSERT_EQ(Status::kSuccess, checkpoint::readLatestAddr(store, "checkpoint", addr, found));
  ASSERT_TRUE(found);
  EXPECT_EQ(41, addr);

  checkpoint::State back;
  ASSERT_EQ(Status::kSuccess, checkpoint::readLatest(store, "checkpoint", back, found));
  ASSERT_TRUE(found);
  EXPECT_EQ(41, back.covered_addr);
  EXPECT_EQ(-1, back.prev_covered_addr);
  EXPECT_EQ("test", back.creator);
  ASSERT_EQ(st.files.size(), back.files.size());
  for (auto const& kv : st.files) {
    ASSERT_EQ(1u, back.files.count(kv.first)) << kv.first;
    EXPECT_EQ(kv.second, back.files[kv.first]) << kv.first;
  }
  EXPECT_EQ(st.sealed, back.sealed);
  EXPECT_EQ(st.removed, back.removed);
  EXPECT_EQ(st.liveBytes(), back.liveBytes());

  // A second checkpoint: LATEST moves, the old one stays readable, and
  // remove() drops only the old one.
  checkpoint::State st2 = sampleState(90, 41);
  st2.files["datalog/3"] = pattern(10, 9);
  ASSERT_EQ(Status::kSuccess, checkpoint::write(store, "checkpoint", st2));
  ASSERT_EQ(Status::kSuccess, checkpoint::readLatestAddr(store, "checkpoint", addr, found));
  EXPECT_EQ(90, addr);
  checkpoint::State old;
  EXPECT_EQ(Status::kSuccess, checkpoint::read(store, "checkpoint", 41, old));
  EXPECT_EQ(41, old.covered_addr);
  EXPECT_EQ(Status::kSuccess, checkpoint::remove(store, "checkpoint", 41));
  EXPECT_NE(Status::kSuccess, checkpoint::read(store, "checkpoint", 41, old));
  ASSERT_EQ(Status::kSuccess, checkpoint::readLatestAddr(store, "checkpoint", addr, found));
  EXPECT_EQ(90, addr);
  checkpoint::State newest;
  ASSERT_EQ(Status::kSuccess, checkpoint::read(store, "checkpoint", 90, newest));
  EXPECT_EQ(41, newest.prev_covered_addr);
  EXPECT_EQ(4u, newest.files.size());
  // Removing a checkpoint that is already gone is not an error.
  EXPECT_EQ(Status::kSuccess, checkpoint::remove(store, "checkpoint", 41));
  std::filesystem::remove_all(root);
}

TEST(CheckpointTest, latest_written_last) {
  std::string root = freshRoot("fail");
  FailingFlushStorage store(root, "files/datalog/2");
  checkpoint::State st = sampleState(41, -1);
  EXPECT_EQ(Status::kFailure, checkpoint::write(store, "checkpoint", st));
  long addr = -1;
  bool found = true;
  EXPECT_EQ(Status::kSuccess, checkpoint::readLatestAddr(store, "checkpoint", addr, found));
  EXPECT_FALSE(found);
  std::filesystem::remove_all(root);
}

TEST(CheckpointTest, rejects_negative_address) {
  std::string root = freshRoot("neg");
  FileStorage store(root);
  checkpoint::State st;
  EXPECT_EQ(Status::kFailure, checkpoint::write(store, "checkpoint", st));
  std::filesystem::remove_all(root);
}
