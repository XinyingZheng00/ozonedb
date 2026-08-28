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
