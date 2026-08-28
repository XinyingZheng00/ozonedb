#ifdef OZONEDB_ENABLE_CORFU
#include "corfu/corfu_layout.h"
#include "corfu/corfu_transport.h"
#include "service/corfu_message.pb.h"
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <cmath>
#include <stdexcept>

namespace ozonedb::corfu {

namespace {
using google::protobuf::ListValue;
using google::protobuf::Struct;
using google::protobuf::Value;

Value const& field(Struct const& s, char const* name) {
  auto it = s.fields().find(name);
  if (it == s.fields().end()) throw std::runtime_error(std::string("layout: missing field ") + name);
  return it->second;
}

bool hasField(Struct const& s, char const* name) {
  return s.fields().find(name) != s.fields().end();
}

int64_t asInt(Value const& v, char const* name) {
  if (v.kind_case() != Value::kNumberValue) {
    throw std::runtime_error(std::string("layout: field ") + name + " is not a number");
  }
  double d = v.number_value();
  if (std::floor(d) != d) throw std::runtime_error(std::string("layout: field ") + name + " is not an integer");
  return static_cast<int64_t>(d);
}

std::vector<std::string> asStrings(Value const& v, char const* name) {
  if (v.kind_case() != Value::kListValue) {
    throw std::runtime_error(std::string("layout: field ") + name + " is not a list");
  }
  std::vector<std::string> out;
  for (auto const& e : v.list_value().values()) {
    if (e.kind_case() != Value::kStringValue) {
      throw std::runtime_error(std::string("layout: field ") + name + " holds a non-string");
    }
    out.push_back(e.string_value());
  }
  return out;
}
}  // namespace

Layout parseLayout(std::string const& json) {
  Struct root;
  auto status = google::protobuf::util::JsonStringToMessage(json, &root);
  if (!status.ok()) {
    throw std::runtime_error("layout: JSON parse failed: " + std::string(status.message()));
  }
  Layout l;
  l.raw_json = json;
  l.epoch = asInt(field(root, "epoch"), "epoch");
  l.layout_servers = asStrings(field(root, "layoutServers"), "layoutServers");
  l.sequencers = asStrings(field(root, "sequencers"), "sequencers");
  if (hasField(root, "unresponsiveServers")) {
    l.unresponsive_servers = asStrings(field(root, "unresponsiveServers"), "unresponsiveServers");
  }
  if (hasField(root, "clusterId")) {
    Value const& cid = field(root, "clusterId");
    if (cid.kind_case() == Value::kStringValue) {
      // Gson's default UUID adapter: the 8-4-4-4-12 string.
      if (!Uuid::parse(cid.string_value(), l.cluster_id)) {
        throw std::runtime_error("layout: clusterId is not a UUID string: " + cid.string_value());
      }
      l.has_cluster_id = true;
    } else if (cid.kind_case() == Value::kStructValue) {
      // Defensive: a {mostSigBits, leastSigBits} object.
      Struct const& s = cid.struct_value();
      l.cluster_id.msb = asInt(field(s, "mostSigBits"), "clusterId.mostSigBits");
      l.cluster_id.lsb = asInt(field(s, "leastSigBits"), "clusterId.leastSigBits");
      l.has_cluster_id = true;
    } else if (cid.kind_case() != Value::kNullValue) {
      throw std::runtime_error("layout: clusterId has an unexpected JSON type");
    }
  }
  Value const& segments = field(root, "segments");
  if (segments.kind_case() != Value::kListValue) throw std::runtime_error("layout: segments is not a list");
  l.num_segments = segments.list_value().values_size();
  if (l.num_segments > 0) {
    Value const& seg0 = segments.list_value().values(0);
    if (seg0.kind_case() != Value::kStructValue) throw std::runtime_error("layout: segments[0] is not an object");
    Struct const& seg = seg0.struct_value();
    Value const& mode = field(seg, "replicationMode");
    if (mode.kind_case() != Value::kStringValue) throw std::runtime_error("layout: replicationMode is not a string");
    l.replication_mode = mode.string_value();
    l.segment_start = asInt(field(seg, "start"), "segments[0].start");
    l.segment_end = asInt(field(seg, "end"), "segments[0].end");
    Value const& stripes = field(seg, "stripes");
    if (stripes.kind_case() != Value::kListValue) throw std::runtime_error("layout: stripes is not a list");
    l.num_stripes = stripes.list_value().values_size();
    if (l.num_stripes > 0) {
      Value const& st0 = stripes.list_value().values(0);
      if (st0.kind_case() != Value::kStructValue) throw std::runtime_error("layout: stripes[0] is not an object");
      l.log_servers = asStrings(field(st0.struct_value(), "logServers"), "logServers");
    }
  }
  return l;
}

void validateSingleNode(Layout const& l) {
  if (l.epoch < 0) throw std::runtime_error("layout: epoch " + std::to_string(l.epoch) + " is invalid");
  if (l.sequencers.size() != 1) {
    throw std::runtime_error("layout: " + std::to_string(l.sequencers.size()) +
                             " sequencers; the native client supports exactly one");
  }
  if (l.num_segments != 1) {
    throw std::runtime_error("layout: " + std::to_string(l.num_segments) +
                             " segments; the native client supports exactly one");
  }
  if (l.num_stripes != 1) {
    throw std::runtime_error("layout: " + std::to_string(l.num_stripes) +
                             " stripes; the native client supports exactly one");
  }
  if (l.log_servers.size() != 1) {
    throw std::runtime_error("layout: " + std::to_string(l.log_servers.size()) +
                             " log servers in the stripe; the native client supports exactly one");
  }
  if (l.replication_mode != "CHAIN_REPLICATION") {
    throw std::runtime_error("layout: replicationMode " + l.replication_mode +
                             "; the native client supports CHAIN_REPLICATION only");
  }
}

Layout fetchLayout(Connection& conn) {
  org::corfudb::runtime::RequestPayloadMsg payload;
  payload.mutable_layout_request()->set_epoch(-1);
  auto resp = conn.call(payload, /*ignore_epoch=*/true, /*ignore_cluster_id=*/true);
  if (resp.has_server_error()) {
    throw std::runtime_error("layout: server error case " +
                             std::to_string(resp.server_error().error_case()));
  }
  if (!resp.has_layout_response()) {
    throw std::runtime_error("layout: unexpected reply (payload case " +
                             std::to_string(resp.payload_case()) + ")");
  }
  Layout l = parseLayout(resp.layout_response().layout().layout_json());
  validateSingleNode(l);
  return l;
}

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
