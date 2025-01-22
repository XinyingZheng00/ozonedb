#include "gtest/gtest.h"

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// task heartbeat
// some benchmarks(eg. ycsb), expected: read performance degradation