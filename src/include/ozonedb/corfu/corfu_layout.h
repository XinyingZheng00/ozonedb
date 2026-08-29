#ifndef OZONEDB_CORFU_LAYOUT_H
#define OZONEDB_CORFU_LAYOUT_H
#ifdef OZONEDB_ENABLE_CORFU
// The Corfu layout: fetched as Gson JSON (LayoutMsg.layout_json) and
// parsed with protobuf's json_util into a google.protobuf.Struct, so no
// JSON library is added. Only the single-node shape the bench uses is
// accepted (PLAN-native-corfu.md §2, §4.3).
#include "corfu/corfu_codec.h"
#include <cstdint>
#include <string>
#include <vector>

namespace ozonedb::corfu {

class Connection;

struct Layout {
  int64_t epoch = -1;
  bool has_cluster_id = false;
  Uuid cluster_id;
  std::vector<std::string> layout_servers;
  std::vector<std::string> sequencers;
  std::vector<std::string> unresponsive_servers;
  // The first segment (validateSingleNode requires exactly one).
  int num_segments = 0;
  int num_stripes = 0;  // of the first segment
  std::string replication_mode;
  int64_t segment_start = 0;
  int64_t segment_end = -1;
  std::vector<std::string> log_servers;  // of the first stripe
  std::string raw_json;

  // The endpoints the client talks to. Valid after validateSingleNode.
  std::string const& sequencer() const { return sequencers.front(); }
  std::string const& logUnit() const { return log_servers.front(); }
};

// Parse the Gson form written by Layout.java. Throws std::runtime_error
// naming the missing or malformed field.
Layout parseLayout(std::string const& json);

// Require one segment, one stripe, one log server, one sequencer and
// CHAIN_REPLICATION. Throws std::runtime_error naming the offending
// field.
void validateSingleNode(Layout const& layout);

// LayoutRequestMsg{epoch = -1} with both ignore flags (LayoutClient),
// parsed and validated. Throws TransportError or std::runtime_error.
Layout fetchLayout(Connection& conn);

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
#endif  // OZONEDB_CORFU_LAYOUT_H
