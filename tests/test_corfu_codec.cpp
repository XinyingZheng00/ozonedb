// Unit tests of the native Corfu client's codecs: no server needed.
// PLAN-native-corfu.md phase 1, item 4.
#ifdef OZONEDB_ENABLE_CORFU
#include "corfu/corfu_codec.h"
#include "corfu/corfu_layout.h"
#include "corfu/corfu_transport.h"
#include "rpc_common.pb.h"
#include <gtest/gtest.h>
#include <string>

using namespace ozonedb::corfu;

TEST(CorfuCodecTest, frame_round_trip) {
  std::string body = "\x0a\x03xyz";
  std::string frame = encodeFrame(kRequestMarker, body);
  ASSERT_EQ(frame.size(), 4 + 1 + body.size());
  EXPECT_EQ(static_cast<unsigned char>(frame[3]), body.size() + 1);  // BE length, low byte
  EXPECT_EQ(static_cast<unsigned char>(frame[4]), kRequestMarker);
  uint8_t marker = 0;
  std::string_view out;
  size_t consumed = 0;
  ASSERT_TRUE(decodeFrame(frame, marker, out, consumed));
  EXPECT_EQ(marker, kRequestMarker);
  EXPECT_EQ(std::string(out), body);
  EXPECT_EQ(consumed, frame.size());
}

TEST(CorfuCodecTest, frame_partial_and_bad_marker) {
  std::string frame = encodeFrame(kResponseMarker, "abc");
  uint8_t marker = 0;
  std::string_view out;
  size_t consumed = 0;
  EXPECT_FALSE(decodeFrame(std::string_view(frame).substr(0, 3), marker, out, consumed));
  EXPECT_FALSE(decodeFrame(std::string_view(frame).substr(0, frame.size() - 1), marker, out, consumed));
  EXPECT_EQ(consumed, 0u);
  std::string bad = frame;
  bad[4] = 0x07;
  EXPECT_THROW(decodeFrame(bad, marker, out, consumed), TransportError);
}

TEST(CorfuCodecTest, uuid_text_round_trip) {
  Uuid u;
  u.msb = static_cast<int64_t>(0x43728ecd825d3b1dULL);
  u.lsb = static_cast<int64_t>(0x889610411e144c36ULL);
  EXPECT_EQ(u.str(), "43728ecd-825d-3b1d-8896-10411e144c36");
  Uuid back;
  ASSERT_TRUE(Uuid::parse("43728ecd-825d-3b1d-8896-10411e144c36", back));
  EXPECT_EQ(back, u);
  EXPECT_FALSE(Uuid::parse("43728ecd825d3b1d889610411e144c36", back));
  EXPECT_FALSE(Uuid::parse("43728ecd-825d-3b1d-8896-10411e144c3g", back));
}

TEST(CorfuCodecTest, stream_id_matches_java_nameUUIDFromBytes) {
  // UUID.nameUUIDFromBytes("ozonedb-ycsb".getBytes()), the bench stream.
  Uuid s = streamIdOf("ozonedb-ycsb");
  EXPECT_EQ(s.str(), "43728ecd-825d-3b1d-8896-10411e144c36");
  // Version 3, IETF variant.
  EXPECT_EQ((static_cast<uint64_t>(s.msb) >> 12) & 0xf, 3u);
  EXPECT_EQ((static_cast<uint64_t>(s.lsb) >> 62) & 0x3, 2u);
}

TEST(CorfuCodecTest, uuid_msg_field_order) {
  Uuid u;
  u.msb = 2;
  u.lsb = 1;
  org::corfudb::runtime::UuidMsg m = toMsg(u);
  EXPECT_EQ(m.msb(), 2);
  EXPECT_EQ(m.lsb(), 1);
  EXPECT_EQ(fromMsg(m), u);
}

TEST(CorfuCodecTest, data_entry_round_trip) {
  Uuid stream = streamIdOf("s");
  Uuid client = randomUuid();
  std::string payload = "hello corfu";
  std::string bytes = encodeDataEntry(payload, {{stream, 41}}, 42, 7, client);
  // type, int32 len, magic, payload, count, then metadata
  ASSERT_GT(bytes.size(), 1 + 4 + 1 + payload.size() + 1);
  EXPECT_EQ(bytes[0], 0);                                          // DATA
  EXPECT_EQ(static_cast<unsigned char>(bytes[4]), payload.size() + 1);  // length covers the magic
  EXPECT_EQ(bytes[5], 0);                                          // byte[] magic
  EXPECT_EQ(bytes.substr(6, payload.size()), payload);
  EXPECT_EQ(bytes[6 + payload.size()], 5);                         // BACKPOINTER_MAP, GLOBAL_ADDRESS, CLIENT_ID, EPOCH, PAYLOAD_CODEC

  LogData d;
  std::string err;
  ASSERT_TRUE(decodeLogData(bytes, d, err)) << err;
  EXPECT_EQ(d.type, DataType::kData);
  EXPECT_TRUE(d.has_payload);
  EXPECT_FALSE(d.corfu_payload);
  EXPECT_EQ(std::string(d.payload), payload);
  ASSERT_TRUE(d.has_backpointers);
  ASSERT_EQ(d.backpointers.size(), 1u);
  EXPECT_EQ(d.backpointers[0].first, stream);
  EXPECT_EQ(d.backpointers[0].second, 41);
  EXPECT_TRUE(d.containsStream(stream));
  EXPECT_FALSE(d.containsStream(streamIdOf("t")));
  EXPECT_TRUE(d.has_global_address);
  EXPECT_EQ(d.global_address, 42);
  EXPECT_TRUE(d.has_client_id);
  EXPECT_EQ(d.client_id, client);
  EXPECT_TRUE(d.has_epoch);
  EXPECT_EQ(d.epoch, 7);
  EXPECT_TRUE(d.has_codec);
  EXPECT_EQ(d.codec, kCodecNone);
  EXPECT_FALSE(d.has_thread_id);
  // Re-encoding a decoded entry is byte-identical.
  EXPECT_EQ(encodeLogData(d), bytes);
}

TEST(CorfuCodecTest, hole_and_empty) {
  std::string hole = encodeHole(99, 3);
  LogData d;
  std::string err;
  ASSERT_TRUE(decodeLogData(hole, d, err)) << err;
  EXPECT_EQ(d.type, DataType::kHole);
  EXPECT_FALSE(d.has_payload);
  EXPECT_EQ(d.global_address, 99);
  EXPECT_EQ(d.epoch, 3);
  EXPECT_FALSE(d.has_backpointers);
  EXPECT_EQ(hole.size(), 1u + 1 + (1 + 8) + (1 + 8));

  // LogData.getEmpty(address): EMPTY + GLOBAL_ADDRESS.
  std::string empty;
  empty.push_back(1);
  empty.push_back(1);
  empty.push_back(4);
  putBe64(empty, 5);
  ASSERT_TRUE(decodeLogData(empty, d, err)) << err;
  EXPECT_EQ(d.type, DataType::kEmpty);
  EXPECT_EQ(d.global_address, 5);
}

TEST(CorfuCodecTest, java_layout_with_thread_id_and_zstd_codec) {
  // A CorfuBridge entry as the Java runtime writes it with the ZSTD
  // default: metadata in id order including THREAD_ID and PAYLOAD_CODEC
  // = 2. The payload bytes are opaque here (a compressed frame). The
  // decoder must report the codec, not fail.
  Uuid stream = streamIdOf("ozonedb-ycsb");
  std::string b;
  b.push_back(0);                 // DATA
  putBe32(b, 4);                  // int32 len: [int32 inner len][zstd...] -- opaque
  b.append("\x00\x00\x00\x00", 4);
  b.push_back(6);                 // 6 metadata entries
  b.push_back(3);                 // BACKPOINTER_MAP
  putBe32(b, 1);
  putBe64(b, stream.msb);
  putBe64(b, stream.lsb);
  putBe64(b, 10);
  b.push_back(4);                 // GLOBAL_ADDRESS
  putBe64(b, 11);
  b.push_back(10);                // CLIENT_ID
  putBe64(b, 1);
  putBe64(b, 2);
  b.push_back(11);                // THREAD_ID
  putBe64(b, 77);
  b.push_back(12);                // EPOCH
  putBe64(b, 0);
  b.push_back(13);                // PAYLOAD_CODEC
  putBe32(b, kCodecZstd);
  LogData d;
  std::string err;
  ASSERT_TRUE(decodeLogData(b, d, err)) << err;
  EXPECT_EQ(d.effectiveCodec(), kCodecZstd);
  EXPECT_TRUE(d.has_thread_id);
  EXPECT_EQ(d.thread_id, 77);
  EXPECT_TRUE(d.containsStream(stream));
  EXPECT_EQ(encodeLogData(d), b);
}

TEST(CorfuCodecTest, truncated_entries_are_rejected) {
  LogData d;
  std::string err;
  EXPECT_FALSE(decodeLogData("", d, err));
  std::string b;
  b.push_back(0);
  putBe32(b, 100);  // claims 100 payload bytes
  b.push_back(0);
  EXPECT_FALSE(decodeLogData(b, d, err));
  std::string c;
  c.push_back(2);
  c.push_back(1);
  c.push_back(4);  // GLOBAL_ADDRESS with only 3 bytes
  c.append("\x00\x00\x00", 3);
  EXPECT_FALSE(decodeLogData(c, d, err));
  std::string u;
  u.push_back(2);
  u.push_back(1);
  u.push_back(99);  // unknown metadata id
  EXPECT_FALSE(decodeLogData(u, d, err));
  EXPECT_NE(err.find("unknown metadata"), std::string::npos);
}

namespace {
char const* kSingleNodeLayout = R"({
  "layoutServers": ["10.10.1.1:9090"],
  "sequencers": ["10.10.1.1:9090"],
  "segments": [{
    "replicationMode": "CHAIN_REPLICATION",
    "start": 0,
    "end": -1,
    "stripes": [{"logServers": ["10.10.1.1:9090"]}]
  }],
  "unresponsiveServers": [],
  "epoch": 3,
  "clusterId": "43728ecd-825d-3b1d-8896-10411e144c36"
})";
}

TEST(CorfuCodecTest, layout_json_single_node) {
  Layout l = parseLayout(kSingleNodeLayout);
  EXPECT_EQ(l.epoch, 3);
  ASSERT_TRUE(l.has_cluster_id);
  EXPECT_EQ(l.cluster_id.str(), "43728ecd-825d-3b1d-8896-10411e144c36");
  EXPECT_EQ(l.sequencer(), "10.10.1.1:9090");
  EXPECT_EQ(l.logUnit(), "10.10.1.1:9090");
  EXPECT_EQ(l.replication_mode, "CHAIN_REPLICATION");
  EXPECT_EQ(l.segment_end, -1);
  EXPECT_NO_THROW(validateSingleNode(l));
}

TEST(CorfuCodecTest, layout_json_null_cluster_id_and_rejections) {
  std::string json = kSingleNodeLayout;
  auto p = json.find("\"clusterId\"");
  ASSERT_NE(p, std::string::npos);
  json = json.substr(0, p) + "\"clusterId\": null\n}";
  Layout l = parseLayout(json);
  EXPECT_FALSE(l.has_cluster_id);
  EXPECT_NO_THROW(validateSingleNode(l));

  std::string two_stripes = kSingleNodeLayout;
  auto q = two_stripes.find("\"stripes\": [");
  two_stripes.insert(q + std::string("\"stripes\": [").size(),
                     "{\"logServers\": [\"10.10.1.2:9090\"]}, ");
  Layout t = parseLayout(two_stripes);
  EXPECT_EQ(t.num_stripes, 2);
  EXPECT_THROW(validateSingleNode(t), std::runtime_error);

  std::string quorum = kSingleNodeLayout;
  auto r = quorum.find("CHAIN_REPLICATION");
  quorum.replace(r, std::string("CHAIN_REPLICATION").size(), "QUORUM_REPLICATION");
  EXPECT_THROW(validateSingleNode(parseLayout(quorum)), std::runtime_error);

  EXPECT_THROW(parseLayout("{\"epoch\": 1}"), std::runtime_error);
  EXPECT_THROW(parseLayout("not json"), std::runtime_error);
}
#endif  // OZONEDB_ENABLE_CORFU
