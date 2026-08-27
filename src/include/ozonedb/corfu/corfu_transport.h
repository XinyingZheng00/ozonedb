#ifndef OZONEDB_CORFU_TRANSPORT_H
#define OZONEDB_CORFU_TRANSPORT_H
#ifdef OZONEDB_ENABLE_CORFU
// One TCP connection to a Corfu node, speaking the Netty framing of
// NettyClientRouter / NettyCorfuMessageEncoder:
//
//   int32 BE length   -- covers the marker and the protobuf
//   byte  marker      -- 0x01 RequestMsg, 0x02 ResponseMsg
//   bytes protobuf
//
// The server echoes the request header, so several threads can have
// requests in flight on one socket: the reader thread demultiplexes the
// replies on header.request_id. The handshake (HandshakeRequestMsg) is
// mandatory; the server drops every other request until it completes.
// Keepalive mirrors RuntimeParameters: a PingRequest after 2 s of write
// idle, and 7 s of read idle marks the connection dead.
#include "corfu/corfu_codec.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

namespace org::corfudb::runtime {
class RequestPayloadMsg;
class ResponsePayloadMsg;
}  // namespace org::corfudb::runtime

namespace ozonedb::corfu {

// A dead socket, a timeout, or a malformed frame. The connection is
// unusable until reopen(); every pending call fails with this.
class TransportError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

struct TransportOptions {
  int request_timeout_ms = 5000;     // RuntimeParameters.requestTimeout
  int handshake_timeout_ms = 10000;  // handshakeTimeout
  int keepalive_idle_ms = 2000;      // keepAlivePeriod: write idle before a ping
  int read_idle_ms = 7000;           // idleConnectionTimeout: read idle = dead
  int connect_timeout_ms = 5000;
};

constexpr uint8_t kRequestMarker = 0x01;
constexpr uint8_t kResponseMarker = 0x02;

// Frame helpers, unit-tested without a socket.
std::string encodeFrame(uint8_t marker, std::string const& body);
// Decode one frame at the front of buf. True with `consumed` set when a
// whole frame is there; false with consumed = 0 when more bytes are
// needed. Throws TransportError on a negative length or a bad marker.
bool decodeFrame(std::string_view buf, uint8_t& marker, std::string_view& body, size_t& consumed);

class Connection {
 public:
  // endpoint is "host:port". name tags log lines ("ctl", "tail").
  Connection(std::string endpoint, Uuid client_id, TransportOptions options, std::string name);
  ~Connection();
  Connection(Connection const&) = delete;
  Connection& operator=(Connection const&) = delete;

  // Connect, start the reader and keepalive threads, handshake. Throws
  // TransportError. Calling it on an open connection is a no-op.
  void open();
  // Close the socket and join the threads. Pending calls fail. Idempotent.
  void close();
  // close() then open().
  void reopen();
  bool alive() const { return alive_.load(std::memory_order_acquire); }
  std::string const& endpoint() const { return endpoint_; }
  std::string const& name() const { return name_; }
  // corfu_source_code_version from the handshake reply's header.
  int64_t serverVersion() const { return server_version_.load(); }

  // The epoch and cluster id stamped on every request header. Set after
  // the layout is known; the handshake and the layout fetch ignore them.
  void setEpoch(int64_t epoch) { epoch_.store(epoch); }
  int64_t epoch() const { return epoch_.load(); }
  void setClusterId(Uuid const& id);

  // Send one request and wait for its reply (request_timeout_ms). Throws
  // TransportError on a dead connection or a timeout. A ServerErrorMsg
  // reply is returned as-is; the caller interprets it.
  org::corfudb::runtime::ResponsePayloadMsg call(org::corfudb::runtime::RequestPayloadMsg const& payload,
                                                 bool ignore_epoch = false,
                                                 bool ignore_cluster_id = false);

 private:
  using ResponsePtr = std::shared_ptr<org::corfudb::runtime::ResponsePayloadMsg>;
  struct Pending {
    std::promise<ResponsePtr> promise;
  };

  std::string endpoint_;
  std::string name_;
  Uuid client_id_;
  TransportOptions options_;
  std::atomic<int64_t> epoch_{0};
  std::mutex cluster_mtx_;
  Uuid cluster_id_;
  std::atomic<int64_t> server_version_{0};

  int fd_ = -1;
  std::atomic<bool> alive_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<int64_t> next_request_id_{1};
  std::mutex write_mtx_;
  std::mutex pending_mtx_;
  std::unordered_map<int64_t, std::shared_ptr<Pending>> pending_;
  std::thread reader_;
  std::thread keepalive_;
  std::mutex keepalive_mtx_;
  std::condition_variable keepalive_cv_;
  std::atomic<int64_t> last_write_ms_{0};
  std::atomic<int64_t> last_read_ms_{0};

  void connectSocket();
  void handshake();
  std::string buildRequest(int64_t request_id, org::corfudb::runtime::RequestPayloadMsg const& payload,
                           bool ignore_epoch, bool ignore_cluster_id);
  // Write one frame in full, under write_mtx_. Throws TransportError.
  void sendFrame(std::string const& frame);
  // Send and register a waiter; wait up to timeout_ms.
  ResponsePtr roundTrip(org::corfudb::runtime::RequestPayloadMsg const& payload,
                        bool ignore_epoch, bool ignore_cluster_id, int timeout_ms);
  void readerLoop();
  void keepaliveLoop();
  // Mark dead and fail every pending call. Safe from any thread.
  void markDead(std::string const& why);
  static int64_t nowMs();
};

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
#endif  // OZONEDB_CORFU_TRANSPORT_H
