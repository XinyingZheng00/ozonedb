#ifdef OZONEDB_ENABLE_CORFU
#include "corfu/corfu_codec.h"
#include "rpc_common.pb.h"
#include <openssl/evp.h>
#include <cstdio>
#include <cstring>
#include <random>

namespace ozonedb::corfu {

// ---- scalars ---------------------------------------------------------

void putBe32(std::string& out, int32_t v) {
  uint32_t u = static_cast<uint32_t>(v);
  char b[4] = {static_cast<char>(u >> 24), static_cast<char>(u >> 16),
               static_cast<char>(u >> 8), static_cast<char>(u)};
  out.append(b, 4);
}

void putBe64(std::string& out, int64_t v) {
  uint64_t u = static_cast<uint64_t>(v);
  char b[8];
  for (int i = 7; i >= 0; --i) {
    b[i] = static_cast<char>(u & 0xff);
    u >>= 8;
  }
  out.append(b, 8);
}

int32_t getBe32(unsigned char const* p) {
  uint32_t u = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
               (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
  return static_cast<int32_t>(u);
}

int64_t getBe64(unsigned char const* p) {
  uint64_t u = 0;
  for (int i = 0; i < 8; ++i) u = (u << 8) | p[i];
  return static_cast<int64_t>(u);
}

// ---- UUIDs -----------------------------------------------------------

std::string Uuid::str() const {
  char buf[37];
  uint64_t m = static_cast<uint64_t>(msb);
  uint64_t l = static_cast<uint64_t>(lsb);
  std::snprintf(buf, sizeof buf, "%08llx-%04llx-%04llx-%04llx-%012llx",
                static_cast<unsigned long long>(m >> 32),
                static_cast<unsigned long long>((m >> 16) & 0xffff),
                static_cast<unsigned long long>(m & 0xffff),
                static_cast<unsigned long long>(l >> 48),
                static_cast<unsigned long long>(l & 0xffffffffffffULL));
  return std::string(buf);
}

bool Uuid::parse(std::string_view text, Uuid& out) {
  if (text.size() != 36) return false;
  uint64_t parts[5] = {0, 0, 0, 0, 0};
  int const widths[5] = {8, 4, 4, 4, 12};
  size_t pos = 0;
  for (int i = 0; i < 5; ++i) {
    if (i > 0) {
      if (text[pos] != '-') return false;
      ++pos;
    }
    for (int k = 0; k < widths[i]; ++k) {
      char c = text[pos++];
      int v;
      if (c >= '0' && c <= '9') v = c - '0';
      else if (c >= 'a' && c <= 'f') v = c - 'a' + 10;
      else if (c >= 'A' && c <= 'F') v = c - 'A' + 10;
      else return false;
      parts[i] = (parts[i] << 4) | static_cast<uint64_t>(v);
    }
  }
  out.msb = static_cast<int64_t>((parts[0] << 32) | (parts[1] << 16) | parts[2]);
  out.lsb = static_cast<int64_t>((parts[3] << 48) | parts[4]);
  return true;
}

Uuid streamIdOf(std::string_view name) {
  unsigned char md[EVP_MAX_MD_SIZE] = {0};
  unsigned int md_len = 0;
  // EVP rather than MD5(): the one-shot API is deprecated in OpenSSL 3.
  EVP_Digest(name.data(), name.size(), md, &md_len, EVP_md5(), nullptr);
  // java.util.UUID.nameUUIDFromBytes
  md[6] = static_cast<unsigned char>((md[6] & 0x0f) | 0x30);  // version 3
  md[8] = static_cast<unsigned char>((md[8] & 0x3f) | 0x80);  // IETF variant
  Uuid u;
  u.msb = getBe64(md);
  u.lsb = getBe64(md + 8);
  return u;
}

Uuid randomUuid() {
  std::random_device rd;
  std::mt19937_64 rng(rd());
  uint64_t m = rng();
  uint64_t l = rng();
  m = (m & 0xffffffffffff0fffULL) | 0x0000000000004000ULL;  // version 4
  l = (l & 0x3fffffffffffffffULL) | 0x8000000000000000ULL;  // IETF variant
  Uuid u;
  u.msb = static_cast<int64_t>(m);
  u.lsb = static_cast<int64_t>(l);
  return u;
}

org::corfudb::runtime::UuidMsg toMsg(Uuid const& u) {
  org::corfudb::runtime::UuidMsg m;
  m.set_lsb(u.lsb);
  m.set_msb(u.msb);
  return m;
}

Uuid fromMsg(org::corfudb::runtime::UuidMsg const& m) {
  Uuid u;
  u.lsb = m.lsb();
  u.msb = m.msb();
  return u;
}

// ---- LogData ---------------------------------------------------------

char const* dataTypeName(DataType t) {
  switch (t) {
    case DataType::kData: return "DATA";
    case DataType::kEmpty: return "EMPTY";
    case DataType::kHole: return "HOLE";
    case DataType::kTrimmed: return "TRIMMED";
    case DataType::kRankOnly: return "RANK_ONLY";
  }
  return "?";
}

bool LogData::containsStream(Uuid const& stream) const {
  for (auto const& kv : backpointers) {
    if (kv.first == stream) return true;
  }
  return false;
}

namespace {
struct Cursor {
  unsigned char const* p;
  size_t left;
  bool take(size_t n) {
    if (left < n) return false;
    p += n;
    left -= n;
    return true;
  }
  bool u8(uint8_t& v) {
    if (left < 1) return false;
    v = *p;
    return take(1);
  }
  bool i32(int32_t& v) {
    if (left < 4) return false;
    v = getBe32(p);
    return take(4);
  }
  bool i64(int64_t& v) {
    if (left < 8) return false;
    v = getBe64(p);
    return take(8);
  }
  bool uuid(Uuid& u) { return i64(u.msb) && i64(u.lsb); }
};

void putUuid(std::string& out, Uuid const& u) {
  putBe64(out, u.msb);
  putBe64(out, u.lsb);
}
}  // namespace

bool decodeLogData(std::string_view bytes, LogData& out, std::string& err) {
  out = LogData();
  Cursor c{reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size()};
  uint8_t type = 0;
  if (!c.u8(type)) {
    err = "empty LogData";
    return false;
  }
  if (type > static_cast<uint8_t>(DataType::kRankOnly)) {
    err = "unknown DataType " + std::to_string(type);
    return false;
  }
  out.type = static_cast<DataType>(type);
  if (out.type == DataType::kData) {
    int32_t len = 0;
    if (!c.i32(len) || len < 0 || static_cast<size_t>(len) > c.left) {
      err = "truncated DATA payload";
      return false;
    }
    out.has_payload = true;
    if (len == 0) {
      out.payload = std::string_view();
    } else {
      uint8_t magic = c.p[0];
      out.corfu_payload = magic == 0x42;
      out.payload = std::string_view(reinterpret_cast<char const*>(c.p) + 1,
                                     static_cast<size_t>(len) - 1);
    }
    c.take(static_cast<size_t>(len));
  }
  uint8_t count = 0;
  if (!c.u8(count)) {
    err = "missing metadata count";
    return false;
  }
  for (uint8_t i = 0; i < count; ++i) {
    uint8_t id = 0;
    if (!c.u8(id)) {
      err = "truncated metadata";
      return false;
    }
    bool ok = true;
    switch (static_cast<MetadataType>(id)) {
      case MetadataType::kBackpointerMap: {
        int32_t n = 0;
        ok = c.i32(n) && n >= 0;
        for (int32_t k = 0; ok && k < n; ++k) {
          Uuid u;
          int64_t v = 0;
          ok = c.uuid(u) && c.i64(v);
          if (ok) out.backpointers.emplace_back(u, v);
        }
        out.has_backpointers = ok;
        break;
      }
      case MetadataType::kGlobalAddress:
        ok = c.i64(out.global_address);
        out.has_global_address = ok;
        break;
      case MetadataType::kCheckpointType: {
        uint8_t v = 0;
        ok = c.u8(v);
        break;
      }
      case MetadataType::kCheckpointId:
      case MetadataType::kCheckpointedStreamId: {
        Uuid u;
        ok = c.uuid(u);
        break;
      }
      case MetadataType::kCheckpointedStreamStart: {
        int64_t v = 0;
        ok = c.i64(v);
        break;
      }
      case MetadataType::kClientId:
        ok = c.uuid(out.client_id);
        out.has_client_id = ok;
        break;
      case MetadataType::kThreadId:
        ok = c.i64(out.thread_id);
        out.has_thread_id = ok;
        break;
      case MetadataType::kEpoch:
        ok = c.i64(out.epoch);
        out.has_epoch = ok;
        break;
      case MetadataType::kPayloadCodec:
        ok = c.i32(out.codec);
        out.has_codec = ok;
        break;
      default:
        err = "unknown metadata type " + std::to_string(id);
        return false;
    }
    if (!ok) {
      err = "truncated metadata type " + std::to_string(id);
      return false;
    }
  }
  return true;
}

std::string encodeLogData(LogData const& in) {
  std::string out;
  out.push_back(static_cast<char>(in.type));
  if (in.type == DataType::kData) {
    // int32 length covers the magic byte and the payload.
    putBe32(out, static_cast<int32_t>(in.payload.size() + 1));
    out.push_back(in.corfu_payload ? static_cast<char>(0x42) : static_cast<char>(0x00));
    out.append(in.payload.data(), in.payload.size());
  }
  uint8_t count = 0;
  if (in.has_backpointers) ++count;
  if (in.has_global_address) ++count;
  if (in.has_client_id) ++count;
  if (in.has_thread_id) ++count;
  if (in.has_epoch) ++count;
  if (in.has_codec) ++count;
  out.push_back(static_cast<char>(count));
  if (in.has_backpointers) {
    out.push_back(static_cast<char>(MetadataType::kBackpointerMap));
    putBe32(out, static_cast<int32_t>(in.backpointers.size()));
    for (auto const& kv : in.backpointers) {
      putUuid(out, kv.first);
      putBe64(out, kv.second);
    }
  }
  if (in.has_global_address) {
    out.push_back(static_cast<char>(MetadataType::kGlobalAddress));
    putBe64(out, in.global_address);
  }
  if (in.has_client_id) {
    out.push_back(static_cast<char>(MetadataType::kClientId));
    putUuid(out, in.client_id);
  }
  if (in.has_thread_id) {
    out.push_back(static_cast<char>(MetadataType::kThreadId));
    putBe64(out, in.thread_id);
  }
  if (in.has_epoch) {
    out.push_back(static_cast<char>(MetadataType::kEpoch));
    putBe64(out, in.epoch);
  }
  if (in.has_codec) {
    out.push_back(static_cast<char>(MetadataType::kPayloadCodec));
    putBe32(out, in.codec);
  }
  return out;
}

std::string encodeDataEntry(std::string_view payload,
                            std::vector<std::pair<Uuid, int64_t>> const& backpointers,
                            int64_t global_address, int64_t epoch, Uuid const& client_id) {
  LogData d;
  d.type = DataType::kData;
  d.has_payload = true;
  d.payload = payload;
  d.has_backpointers = !backpointers.empty();
  d.backpointers = backpointers;
  d.has_global_address = true;
  d.global_address = global_address;
  d.has_client_id = true;
  d.client_id = client_id;
  d.has_epoch = true;
  d.epoch = epoch;
  d.has_codec = true;
  d.codec = kCodecNone;
  return encodeLogData(d);
}

std::string encodeHole(int64_t global_address, int64_t epoch) {
  LogData d;
  d.type = DataType::kHole;
  d.has_global_address = true;
  d.global_address = global_address;
  d.has_epoch = true;
  d.epoch = epoch;
  return encodeLogData(d);
}

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
