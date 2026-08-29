#ifdef OZONEDB_ENABLE_CORFU
#include "corfu/corfu_rpc.h"
#include "service/corfu_message.pb.h"
#include <chrono>
#include <iostream>
#include <thread>

namespace ozonedb::corfu {

using org::corfudb::runtime::ReadLogResponseMsg;
using org::corfudb::runtime::RequestPayloadMsg;
using org::corfudb::runtime::ResponsePayloadMsg;
using org::corfudb::runtime::ServerErrorMsg;
using org::corfudb::runtime::TokenRequestMsg;
using org::corfudb::runtime::TokenResponseMsg;

ServerError::Kind ServerError::kindOf(ServerErrorMsg const& e, int64_t& detail) {
  detail = -1;
  switch (e.error_case()) {
    case ServerErrorMsg::kUnknownError: return Kind::kUnknown;
    case ServerErrorMsg::kWrongEpochError:
      detail = e.wrong_epoch_error().correct_epoch();
      return Kind::kWrongEpoch;
    case ServerErrorMsg::kNotReadyError: return Kind::kNotReady;
    case ServerErrorMsg::kWrongClusterError: return Kind::kWrongCluster;
    case ServerErrorMsg::kTrimmedError: return Kind::kTrimmed;
    case ServerErrorMsg::kOverwriteError:
      detail = e.overwrite_error().overwrite_cause_id();
      return Kind::kOverwrite;
    case ServerErrorMsg::kDataCorruptionError:
      detail = e.data_corruption_error().address();
      return Kind::kDataCorruption;
    case ServerErrorMsg::kBootstrappedError: return Kind::kBootstrapped;
    case ServerErrorMsg::kNotBootstrappedError: return Kind::kNotBootstrapped;
    case ServerErrorMsg::ERROR_NOT_SET: return Kind::kNone;
  }
  return Kind::kNone;
}

char const* ServerError::name(Kind k) {
  switch (k) {
    case Kind::kUnknown: return "unknown_error";
    case Kind::kWrongEpoch: return "wrong_epoch_error";
    case Kind::kNotReady: return "not_ready_error";
    case Kind::kWrongCluster: return "wrong_cluster_error";
    case Kind::kTrimmed: return "trimmed_error";
    case Kind::kOverwrite: return "overwrite_error";
    case Kind::kDataCorruption: return "data_corruption_error";
    case Kind::kBootstrapped: return "bootstrapped_error";
    case Kind::kNotBootstrapped: return "not_bootstrapped_error";
    case Kind::kNone: return "empty_error";
  }
  return "?";
}

namespace {
TransportOptions transportOptions(CorfuClientOptions const& o) {
  TransportOptions t;
  t.request_timeout_ms = o.request_timeout_ms;
  return t;
}
}  // namespace

Rpc::Rpc(CorfuClientOptions const& options, Uuid const& client_id)
    : options_(options), client_id_(client_id) {
  ctl_ = std::make_unique<Connection>(options.endpoint, client_id, transportOptions(options), "ctl");
  ctl_->open();
  Layout l = fetchLayout(*ctl_);
  applyLayout(l);
  // In the supported layout the sequencer and the log unit are the node
  // we connected to. Open the read connection to the log unit's own
  // endpoint anyway, so a layout that names it differently still works.
  tail_ = std::make_unique<Connection>(l.logUnit(), client_id, transportOptions(options), "tail");
  tail_->open();
  tail_->setEpoch(epoch_);
  if (l.has_cluster_id) tail_->setClusterId(l.cluster_id);
  if (ctl_->serverVersion() != 0) {
    std::cerr << "[corfu-native] connected to " << options.endpoint << " epoch " << epoch_
              << " server version 0x" << std::hex << ctl_->serverVersion() << std::dec << "\n";
  }
}

Rpc::~Rpc() {
  close();
}

void Rpc::close() {
  std::lock_guard<std::mutex> lk(reconnect_mtx_);
  closed_ = true;
  if (tail_) tail_->close();
  if (ctl_) ctl_->close();
}

Layout Rpc::layout() const {
  std::lock_guard<std::mutex> lk(layout_mtx_);
  return layout_;
}

int64_t Rpc::serverVersion() const {
  return ctl_ ? ctl_->serverVersion() : 0;
}

void Rpc::applyLayout(Layout const& l) {
  {
    std::lock_guard<std::mutex> lk(layout_mtx_);
    layout_ = l;
    epoch_ = l.epoch;
  }
  for (Connection* c : {ctl_.get(), tail_.get()}) {
    if (!c) continue;
    c->setEpoch(l.epoch);
    if (l.has_cluster_id) c->setClusterId(l.cluster_id);
  }
}

void Rpc::refreshLayout() {
  Layout l = fetchLayout(*ctl_);
  if (l.epoch != epoch_) {
    std::cerr << "[corfu-native] layout epoch " << epoch_ << " -> " << l.epoch << "\n";
  }
  applyLayout(l);
}

void Rpc::ensureOpen(Connection& c) {
  std::lock_guard<std::mutex> lk(reconnect_mtx_);
  if (closed_) throw TransportError("Rpc is closed");
  if (c.alive()) return;
  std::cerr << "[corfu-native] reconnecting " << c.name() << " to " << c.endpoint() << "\n";
  c.reopen();
  if (&c == ctl_.get()) {
    Layout l = fetchLayout(*ctl_);
    applyLayout(l);
  }
}

ResponsePayloadMsg Rpc::call(Connection& c, RequestPayloadMsg const& payload,
                             bool ignore_epoch, bool ignore_cluster_id) {
  constexpr int kNotReadyRetries = 20;
  constexpr int kEpochRetries = 2;
  constexpr int kTransportRetries = 1;
  int not_ready = 0;
  int epoch_retries = 0;
  int transport_retries = 0;
  for (;;) {
    ensureOpen(c);
    ResponsePayloadMsg resp;
    try {
      resp = c.call(payload, ignore_epoch, ignore_cluster_id);
    } catch (TransportError const& e) {
      if (transport_retries++ < kTransportRetries) {
        std::cerr << "[corfu-native] " << e.what() << "; retrying once\n";
        continue;
      }
      throw;
    }
    if (!resp.has_server_error()) return resp;
    int64_t detail = -1;
    ServerError::Kind kind = ServerError::kindOf(resp.server_error(), detail);
    switch (kind) {
      case ServerError::Kind::kWrongEpoch:
        if (epoch_retries++ < kEpochRetries) {
          std::cerr << "[corfu-native] wrong epoch (server " << detail << ", ours " << epoch_
                    << "); refreshing the layout\n";
          refreshLayout();
          continue;
        }
        break;
      case ServerError::Kind::kNotReady:
      case ServerError::Kind::kNotBootstrapped:
        if (not_ready++ < kNotReadyRetries) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
          continue;
        }
        break;
      default:
        break;
    }
    throw ServerError(kind, std::string("corfu ") + ServerError::name(kind) + " on " + c.name() +
                                " (payload case " + std::to_string(payload.payload_case()) + ", detail " +
                                std::to_string(detail) + ")",
                      detail);
  }
}

TokenResponseMsg Rpc::token(TokenRequestMsg const& req) {
  RequestPayloadMsg payload;
  *payload.mutable_token_request() = req;
  ResponsePayloadMsg resp = call(*ctl_, payload, false, false);
  if (!resp.has_token_response()) {
    throw ServerError(ServerError::Kind::kNone,
                      "corfu token request: unexpected reply case " + std::to_string(resp.payload_case()));
  }
  return resp.token_response();
}

void Rpc::sequencerTrim(int64_t trim_mark) {
  RequestPayloadMsg payload;
  payload.mutable_sequencer_trim_request()->set_trim_mark(trim_mark);
  call(*ctl_, payload, false, false);
}

void Rpc::write(std::string const& log_data_bytes) {
  RequestPayloadMsg payload;
  payload.mutable_write_log_request()->mutable_log_data()->set_entry(log_data_bytes);
  ResponsePayloadMsg resp = call(*ctl_, payload, false, false);
  if (!resp.has_write_log_response()) {
    throw ServerError(ServerError::Kind::kNone,
                      "corfu write: unexpected reply case " + std::to_string(resp.payload_case()));
  }
}

ReadLogResponseMsg Rpc::read(std::vector<int64_t> const& addresses, bool cache_results) {
  RequestPayloadMsg payload;
  auto* r = payload.mutable_read_log_request();
  r->mutable_address()->Reserve(static_cast<int>(addresses.size()));
  for (int64_t a : addresses) r->add_address(a);
  r->set_cache_results(cache_results);
  ResponsePayloadMsg resp = call(*tail_, payload, false, false);
  if (!resp.has_read_log_response()) {
    throw ServerError(ServerError::Kind::kNone,
                      "corfu read: unexpected reply case " + std::to_string(resp.payload_case()));
  }
  return std::move(*resp.mutable_read_log_response());
}

void Rpc::trimLog(int64_t addr) {
  RequestPayloadMsg payload;
  auto* t = payload.mutable_trim_log_request()->mutable_address();
  t->set_epoch(epoch_);
  t->set_sequence(addr);
  call(*ctl_, payload, false, false);
}

void Rpc::compact() {
  RequestPayloadMsg payload;
  payload.mutable_compact_request();
  call(*ctl_, payload, /*ignore_epoch=*/true, false);
}

int64_t Rpc::trimMark() {
  RequestPayloadMsg payload;
  payload.mutable_trim_mark_request();
  ResponsePayloadMsg resp = call(*ctl_, payload, false, false);
  if (!resp.has_trim_mark_response()) {
    throw ServerError(ServerError::Kind::kNone,
                      "corfu trim mark: unexpected reply case " + std::to_string(resp.payload_case()));
  }
  return resp.trim_mark_response().trim_mark();
}

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
