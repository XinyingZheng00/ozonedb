#ifndef OZONEDB_CORFU_RPC_H
#define OZONEDB_CORFU_RPC_H
#ifdef OZONEDB_ENABLE_CORFU
// Typed Corfu RPCs over two connections, with the retry policy of
// PLAN-native-corfu.md §4.2:
//   wrong_epoch_error       -> refetch the layout, retry once
//   not_ready / not_bootstrapped -> sleep 1 s, retry, up to 20 times
//   no reply / dead socket  -> reconnect, refetch the layout, retry once
//   everything else         -> ServerError to the caller
// `ctl` carries tokens, writes and trims; `tail` carries the reader's
// bulk reads, so a 1000-entry read never delays a token request.
#include "corfu/corfu_codec.h"
#include "corfu/corfu_layout.h"
#include "corfu/corfu_transport.h"
#include "corfu_client.h"
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace org::corfudb::runtime {
class RequestPayloadMsg;
class ResponsePayloadMsg;
class ServerErrorMsg;
class TokenRequestMsg;
class TokenResponseMsg;
class ReadLogResponseMsg;
}  // namespace org::corfudb::runtime

namespace ozonedb::corfu {

// A ServerErrorMsg the retry policy does not absorb.
class ServerError : public std::runtime_error {
 public:
  enum class Kind {
    kUnknown,         // Java-serialized throwable, opaque
    kWrongEpoch,      // detail = correct_epoch
    kNotReady,
    kWrongCluster,
    kTrimmed,
    kOverwrite,       // detail = overwrite cause (HOLE 0, SAME_DATA 1, DIFF_DATA 2, TRIM 3, NONE 4)
    kDataCorruption,  // detail = address
    kBootstrapped,
    kNotBootstrapped,
    kNone,            // the oneof was empty
  };
  ServerError(Kind kind, std::string const& what, int64_t detail = -1)
      : std::runtime_error(what), kind_(kind), detail_(detail) {}
  Kind kind() const { return kind_; }
  int64_t detail() const { return detail_; }
  static Kind kindOf(org::corfudb::runtime::ServerErrorMsg const& e, int64_t& detail);
  static char const* name(Kind k);

 private:
  Kind kind_;
  int64_t detail_;
};

// Overwrite causes (OverwriteCause.java).
constexpr int64_t kOverwriteHole = 0;
constexpr int64_t kOverwriteSameData = 1;
constexpr int64_t kOverwriteDiffData = 2;
constexpr int64_t kOverwriteTrim = 3;
constexpr int64_t kOverwriteNone = 4;

class Rpc {
 public:
  // Connects `ctl` to options.endpoint, fetches and validates the layout,
  // then connects `tail` to the log unit. Throws on failure.
  Rpc(CorfuClientOptions const& options, Uuid const& client_id);
  ~Rpc();

  Uuid const& clientId() const { return client_id_; }
  Layout layout() const;
  int64_t epoch() const { return epoch_; }
  int64_t serverVersion() const;
  // Refetch the layout on `ctl` and restamp the epoch on both connections.
  void refreshLayout();

  // --- sequencer (ctl) ---
  org::corfudb::runtime::TokenResponseMsg token(org::corfudb::runtime::TokenRequestMsg const& req);
  void sequencerTrim(int64_t trim_mark);

  // --- log unit ---
  // WriteLogRequest on ctl. Throws ServerError(kOverwrite, cause) when
  // the address holds data, kTrimmed below the trim mark.
  void write(std::string const& log_data_bytes);
  // ReadLogRequest on tail. One ReadResponseMsg per requested address,
  // EMPTY for an address with no write. Throws ServerError(kTrimmed)
  // when the log unit refuses an address below the trim mark.
  org::corfudb::runtime::ReadLogResponseMsg read(std::vector<int64_t> const& addresses, bool cache_results);
  // TrimLogRequest{Token{epoch, addr}} on ctl.
  void trimLog(int64_t addr);
  // CompactRequest on ctl, epoch ignored (LogUnitClient.compact).
  void compact();
  // TrimMarkRequest on ctl -> trim_mark.
  int64_t trimMark();

  void close();

 private:
  CorfuClientOptions options_;
  Uuid client_id_;
  mutable std::mutex layout_mtx_;
  Layout layout_;
  int64_t epoch_ = -1;
  std::unique_ptr<Connection> ctl_;
  std::unique_ptr<Connection> tail_;
  std::mutex reconnect_mtx_;
  bool closed_ = false;

  void applyLayout(Layout const& l);
  // Reopen a dead connection under reconnect_mtx_ (once per failure, not
  // once per failed caller) and refresh the layout.
  void ensureOpen(Connection& c);
  org::corfudb::runtime::ResponsePayloadMsg call(Connection& c,
                                                 org::corfudb::runtime::RequestPayloadMsg const& payload,
                                                 bool ignore_epoch, bool ignore_cluster_id);
};

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
#endif  // OZONEDB_CORFU_RPC_H
