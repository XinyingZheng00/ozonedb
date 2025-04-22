#include "gtest/gtest.h"
#include "shared_log_storage.h"
#include <iostream>
#include <string>
#include <thread>
#include <hdr/hdr_histogram.h>
#include <unordered_map>
#include <chrono>
#include <atomic>
//need to be run in the root mode

using namespace ozonedb;
TEST(SharedLogStorageTest, AppendAndSizeIncreases) {
  SharedLogStorage* storage = new SharedLogStorage();
  std::string test_data = "test_entry";
  size_t size_before = storage->size();
  
  EXPECT_EQ(storage->append(test_data), Status::kSuccess);

  size_t size_after = storage->size();
  std::cout << "Size before: " << size_before << ", Size after: " << size_after << std::endl; 
  EXPECT_GT(size_after, size_before);  // size should increase after append
}

TEST(SharedLogStorageTest, ReadAppendedEntry) {
  SharedLogStorage* storage = new SharedLogStorage();
  std::string test_data = "test_read_entry";
  int num_entries = 10;
  for (int i = 0; i < num_entries; ++i) {
    storage->append(test_data + std::to_string(i));
  }
  size_t end = storage->size();
  std::vector<std::string> entries;
  Status status = storage->read(entries, end - num_entries, end);

  EXPECT_EQ(status, Status::kSuccess);
  ASSERT_EQ(entries.size(), num_entries);
  for (int i = 0; i < num_entries; ++i) {
    EXPECT_EQ(entries[i], test_data + std::to_string(i));
  }
}

TEST(SharedLogStorageTest, ExistAlwaysTrue) {
  SharedLogStorage* storage = new SharedLogStorage();
  EXPECT_TRUE(storage->exist());
}

using namespace std::chrono;
std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_reads;
std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_writes;

void reader_thread(int thd_id, hdr_histogram* histogram, int runtime_secs) {
  uint64_t idx = 0;
  SharedLogStorage* storage = new SharedLogStorage(2*thd_id);
  auto begin = high_resolution_clock::now();

  while (duration_cast<seconds>(high_resolution_clock::now() - begin).count() < runtime_secs) {
    std::vector<std::string> entries;
    auto start = high_resolution_clock::now();
    storage->read(entries, idx, idx + 1);
    hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
    idx++;
  }

  num_requests_and_durations_reads[thd_id] = {
      idx, duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
}

void writer_thread(int thd_id, hdr_histogram* histogram, int runtime_secs) {
  uint64_t idx = 0;
  std::string data(1024, 'W');
  SharedLogStorage* storage = new SharedLogStorage(2*thd_id + 1);
  auto begin = high_resolution_clock::now();

  while (duration_cast<seconds>(high_resolution_clock::now() - begin).count() < runtime_secs) {
    auto start = high_resolution_clock::now();
    storage->append(data);
    hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
    idx++;
  }

  num_requests_and_durations_writes[thd_id] = {
      idx, duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
}

double compute_throughput(const std::unordered_map<int, std::pair<uint64_t, uint64_t>>& stats) {
  double tput = 0;
  for (const auto& [_, p] : stats) {
    tput += static_cast<double>(p.first) * 1.0e9 / p.second;
  }
  return tput;
}

TEST(SharedLogStorageTest, MixedReadWriteThroughput) {
  int runtime_secs = 5;
  int thread_count = 3;

  hdr_histogram* read_hist;
  hdr_init(1, INT64_C(3600000000), 3, &read_hist);
  hdr_histogram* write_hist;
  hdr_init(1, INT64_C(3600000000), 3, &write_hist);

  std::vector<std::thread> readers, writers;

  for (int i = 0; i < thread_count; ++i) {
    writers.emplace_back(writer_thread, i, write_hist, runtime_secs);
    readers.emplace_back(reader_thread, i, read_hist, runtime_secs);
  }
  for (auto& t : writers) t.join();
  for (auto& t : readers) t.join();

  double write_tput = compute_throughput(num_requests_and_durations_writes);
  double read_tput = compute_throughput(num_requests_and_durations_reads);

  std::cout << "[MixedTest] write throughput: " << write_tput << " ops/sec\n";
  std::cout << "[MixedTest] read throughput: " << read_tput << " ops/sec\n";

  EXPECT_GT(write_tput, 0.0);
  EXPECT_GT(read_tput, 0.0);

  std::cout << "[MixedTest] read latencies:\n"
            << "\tp50: " << hdr_value_at_percentile(read_hist, 50.0) << " ns\n"
            << "\tp95: " << hdr_value_at_percentile(read_hist, 95.0) << " ns\n"
            << "\tp99: " << hdr_value_at_percentile(read_hist, 99.0) << " ns\n";

  std::cout << "[MixedTest] write latencies:\n"
            << "\tp50: " << hdr_value_at_percentile(write_hist, 50.0) << " ns\n"
            << "\tp95: " << hdr_value_at_percentile(write_hist, 95.0) << " ns\n"
            << "\tp99: " << hdr_value_at_percentile(write_hist, 99.0) << " ns\n";

  hdr_close(read_hist);
  hdr_close(write_hist);
}
