#ifndef OZONEDB_CORFU_CODEC_H
#define OZONEDB_CORFU_CODEC_H
#ifdef OZONEDB_ENABLE_CORFU
// Byte-level codecs of the Corfu protocol that are NOT protobuf: UUIDs,
// the stream id derivation, and the LogData entry format inside
// LogDataMsg.entry. Every layout here is what the Java runtime writes
// (LogData.java doSerializeInternal, CorfuProtocolCommon.serialize,
// IMetadata.LogUnitMetadataType), so an entry the native client writes
// is readable by CorfuBridge and the reverse.
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace org::corfudb::runtime {
class UuidMsg;
}

namespace ozonedb::corfu {

struct Uuid {
  int64_t msb = 0;
  int64_t lsb = 0;
  bool operator==(Uuid const& o) const { return msb == o.msb && lsb == o.lsb; }
  bool operator!=(Uuid const& o) const { return !(*this == o); }
  bool isNil() const { return msb == 0 && lsb == 0; }
  // 8-4-4-4-12 lower-case hex, the java.util.UUID.toString form.
  std::string str() const;
  // Parse the 8-4-4-4-12 form. False on anything else.
  static bool parse(std::string_view text, Uuid& out);
};

// The stream id of a stream name: java.util.UUID.nameUUIDFromBytes, i.e.
// MD5 of the name bytes with the version bits set to 3 and the variant
// bits to 10 (CorfuRuntime.getStreamID).
Uuid streamIdOf(std::string_view name);
// A random (version 4) UUID.
Uuid randomUuid();

// UuidMsg is {lsb = 1, msb = 2}, the reverse of the metadata order.
org::corfudb::runtime::UuidMsg toMsg(Uuid const& u);
Uuid fromMsg(org::corfudb::runtime::UuidMsg const& m);

enum class DataType : uint8_t {
  kData = 0,
  kEmpty = 1,
  kHole = 2,
  kTrimmed = 3,
  kRankOnly = 4,
};
char const* dataTypeName(DataType t);

// IMetadata.LogUnitMetadataType ids.
enum class MetadataType : uint8_t {
  kBackpointerMap = 3,   // Map<UUID, Long>: int32 count, count x (msb, lsb, value)
  kGlobalAddress = 4,    // int64
  kCheckpointType = 6,   // byte
  kCheckpointId = 7,     // uuid (msb, lsb)
  kCheckpointedStreamId = 8,
  kCheckpointedStreamStart = 9,  // int64
  kClientId = 10,        // uuid (msb, lsb)
  kThreadId = 11,        // int64
  kEpoch = 12,           // int64
  kPayloadCodec = 13,    // int32: 0 NONE, 1 LZ4, 2 ZSTD
};

constexpr int32_t kCodecNone = 0;
constexpr int32_t kCodecLz4 = 1;
constexpr int32_t kCodecZstd = 2;

// One decoded LogData. `payload` aliases the buffer given to
// decodeLogData: the caller keeps that buffer alive while it reads.
struct LogData {
  DataType type = DataType::kEmpty;
  // DATA only. corfu_payload is true when the serializer magic was 0x42
  // (an SMR LogEntry, never written by OzoneDB); payload then holds the
  // bytes after the magic, whatever they are.
  bool has_payload = false;
  bool corfu_payload = false;
  std::string_view payload;
  // Metadata, in the order Java's EnumMap emits them (by id).
  bool has_backpointers = false;
  std::vector<std::pair<Uuid, int64_t>> backpointers;
  bool has_global_address = false;
  int64_t global_address = -1;
  bool has_client_id = false;
  Uuid client_id;
  bool has_thread_id = false;
  int64_t thread_id = 0;
  bool has_epoch = false;
  int64_t epoch = -1;
  bool has_codec = false;
  int32_t codec = kCodecNone;

  bool containsStream(Uuid const& stream) const;
  // The effective codec (absent = NONE).
  int32_t effectiveCodec() const { return has_codec ? codec : kCodecNone; }
};

// Decode the bytes of LogDataMsg.entry. False (with `err` set) on a
// truncated or unknown layout. Checkpoint metadata (ids 6-9) is decoded
// and dropped.
bool decodeLogData(std::string_view bytes, LogData& out, std::string& err);

// Encode a LogData the way Java does: type byte, then for DATA the
// int32 length + 0x00 magic + payload (compression is never applied:
// the writer sets codec NONE), then the metadata count and each present
// field in id order.
std::string encodeLogData(LogData const& in);

// A DATA entry as the native writer emits it: BACKPOINTER_MAP,
// GLOBAL_ADDRESS, CLIENT_ID, EPOCH, PAYLOAD_CODEC = NONE. That is the
// metadata set of a CorfuBridge entry minus THREAD_ID.
std::string encodeDataEntry(std::string_view payload,
                            std::vector<std::pair<Uuid, int64_t>> const& backpointers,
                            int64_t global_address, int64_t epoch, Uuid const& client_id);

// A HOLE at global_address (LogData.getHole(token)): type HOLE,
// GLOBAL_ADDRESS and EPOCH only.
std::string encodeHole(int64_t global_address, int64_t epoch);

// Big-endian scalar helpers, shared with the transport.
void putBe32(std::string& out, int32_t v);
void putBe64(std::string& out, int64_t v);
int32_t getBe32(unsigned char const* p);
int64_t getBe64(unsigned char const* p);

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
#endif  // OZONEDB_CORFU_CODEC_H
