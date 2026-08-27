// Phase-1 probe of the native Corfu client (PLAN-native-corfu.md §5).
//
// Usage:
//   corfu_native_probe <host:port> <stream-name> [--from A] [--to B] [--batch N]
//
// Connects with the native transport, handshakes, prints the raw layout
// JSON, the epoch, the server's source version, the global tail, the
// stream tail and the trim mark, then reads every address in
// [max(trim mark, --from), min(tail, --to)] and prints a histogram by
// DataType and by stream membership. The plan's phase-1 gate is
// "0 foreign entries" on the loaded datasets: every DATA entry carries
// this stream in its BACKPOINTER_MAP. It also reports the payload
// codecs seen, so a dataset written with ZSTD is caught before phase 2.
#include "corfu/corfu_codec.h"
#include "corfu/corfu_layout.h"
#include "corfu/corfu_rpc.h"
#include "corfu/corfu_transport.h"
#include "corfu_client.h"
#include "service/corfu_message.pb.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

using namespace ozonedb;
using namespace ozonedb::corfu;

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <host:port> <stream-name> [--from A] [--to B] [--batch N]\n";
    return 1;
  }
  CorfuClientOptions options;
  options.endpoint = argv[1];
  options.stream_name = argv[2];
  options.client = "native";
  int64_t from = -1;
  int64_t to = -1;
  int batch = 1000;
  for (int i = 3; i + 1 < argc; i += 2) {
    std::string flag = argv[i];
    if (flag == "--from") from = std::atoll(argv[i + 1]);
    else if (flag == "--to") to = std::atoll(argv[i + 1]);
    else if (flag == "--batch") batch = std::atoi(argv[i + 1]);
    else {
      std::cerr << "unknown flag " << flag << "\n";
      return 1;
    }
  }

  Uuid const client_id = randomUuid();
  Uuid const stream = streamIdOf(options.stream_name);
  std::cout << "[probe] client " << client_id.str() << " stream " << options.stream_name
            << " = " << stream.str() << "\n";

  Rpc rpc(options, client_id);
  Layout layout = rpc.layout();
  std::cout << "[probe] layout json: " << layout.raw_json << "\n";
  std::cout << "[probe] epoch=" << layout.epoch
            << " cluster_id=" << (layout.has_cluster_id ? layout.cluster_id.str() : "(null)")
            << " sequencer=" << layout.sequencer() << " log_unit=" << layout.logUnit()
            << " mode=" << layout.replication_mode
            << " server_version=0x" << std::hex << rpc.serverVersion() << std::dec << "\n";

  org::corfudb::runtime::TokenRequestMsg q;
  q.set_request_type(org::corfudb::runtime::TokenRequestMsg::TK_QUERY);
  q.set_num_tokens(0);
  *q.add_streams() = toMsg(stream);
  auto tok = rpc.token(q);
  int64_t const global_tail = tok.token().sequence();
  int64_t stream_tail = -6;
  for (auto const& st : tok.stream_tails()) {
    if (fromMsg(st.key()) == stream) stream_tail = st.value();
  }
  int64_t const trim_mark = rpc.trimMark();
  std::cout << "[probe] global_tail=" << global_tail << " stream_tail=" << stream_tail
            << " trim_mark=" << trim_mark << "\n";

  int64_t lo = std::max<int64_t>(0, std::max(trim_mark, from));
  int64_t hi = to >= 0 ? std::min(to, global_tail) : global_tail;
  if (hi < lo) {
    std::cout << "[probe] nothing to scan ([" << lo << ", " << hi << "])\n";
    return 0;
  }
  std::cout << "[probe] scanning [" << lo << ", " << hi << "] in batches of " << batch << "\n";

  std::map<std::string, uint64_t> by_type;
  std::map<std::string, uint64_t> by_codec;
  uint64_t ours = 0, foreign = 0, corfu_payload = 0, decode_errors = 0, missing = 0;
  uint64_t bytes_ours = 0;
  std::map<std::string, uint64_t> other_streams;
  std::vector<int64_t> first_foreign;
  auto const t0 = std::chrono::steady_clock::now();
  int64_t addr = lo;
  while (addr <= hi) {
    int64_t last = std::min(hi, addr + batch - 1);
    std::vector<int64_t> addrs;
    for (int64_t a = addr; a <= last; ++a) addrs.push_back(a);
    org::corfudb::runtime::ReadLogResponseMsg resp;
    try {
      resp = rpc.read(addrs, /*cache_results=*/false);
    } catch (ServerError const& e) {
      std::cout << "[probe] read [" << addr << ", " << last << "]: " << e.what() << "\n";
      if (e.kind() == ServerError::Kind::kTrimmed) {
        by_type["trimmed_error"] += static_cast<uint64_t>(last - addr + 1);
        addr = last + 1;
        continue;
      }
      return 2;
    }
    std::map<int64_t, int> seen;
    for (int i = 0; i < resp.response_size(); ++i) seen[resp.response(i).address()] = i;
    for (int64_t a = addr; a <= last; ++a) {
      auto it = seen.find(a);
      if (it == seen.end()) {
        ++missing;
        continue;
      }
      LogData ld;
      std::string err;
      if (!decodeLogData(resp.response(it->second).log_data().entry(), ld, err)) {
        ++decode_errors;
        if (decode_errors <= 5) std::cout << "[probe] address " << a << ": " << err << "\n";
        continue;
      }
      ++by_type[dataTypeName(ld.type)];
      if (ld.type != DataType::kData) continue;
      ++by_codec[std::to_string(ld.effectiveCodec())];
      if (ld.corfu_payload) ++corfu_payload;
      if (ld.containsStream(stream)) {
        ++ours;
        bytes_ours += ld.payload.size();
      } else {
        ++foreign;
        if (first_foreign.size() < 10) first_foreign.push_back(a);
        for (auto const& bp : ld.backpointers) ++other_streams[bp.first.str()];
      }
    }
    addr = last + 1;
  }
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

  std::cout << "[probe] scanned " << (hi - lo + 1) << " addresses in " << ms << " ms\n";
  for (auto const& kv : by_type) std::cout << "[probe]   " << kv.first << "=" << kv.second << "\n";
  std::cout << "[probe] DATA: ours=" << ours << " (" << (bytes_ours >> 20) << " MB) foreign=" << foreign
            << " corfu_payload=" << corfu_payload << " decode_errors=" << decode_errors
            << " missing_in_reply=" << missing << "\n";
  for (auto const& kv : by_codec) {
    std::cout << "[probe]   codec " << kv.first << (kv.first == "0" ? " (NONE)" : kv.first == "2" ? " (ZSTD)" : "")
              << "=" << kv.second << "\n";
  }
  for (auto const& kv : other_streams) std::cout << "[probe]   other stream " << kv.first << "=" << kv.second << "\n";
  if (!first_foreign.empty()) {
    std::cout << "[probe] first foreign addresses:";
    for (int64_t a : first_foreign) std::cout << " " << a;
    std::cout << "\n";
  }
  bool const gate = foreign == 0 && corfu_payload == 0 && decode_errors == 0 && by_codec.size() <= 1 &&
                    (by_codec.empty() || by_codec.count("0") == 1);
  std::cout << "[probe] phase-1 gate (0 foreign, codec NONE only): " << (gate ? "PASS" : "FAIL") << "\n";
  return gate ? 0 : 3;
}
