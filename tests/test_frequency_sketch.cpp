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
