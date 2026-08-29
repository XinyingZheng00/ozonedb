#ifdef OZONEDB_ENABLE_CORFU
#include "corfu/corfu_transport.h"
#include "service/corfu_message.pb.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iostream>

namespace ozonedb::corfu {

using org::corfudb::runtime::HeaderMsg;
using org::corfudb::runtime::RequestMsg;
using org::corfudb::runtime::RequestPayloadMsg;
using org::corfudb::runtime::ResponseMsg;
using org::corfudb::runtime::ResponsePayloadMsg;

// ---- frames ----------------------------------------------------------

std::string encodeFrame(uint8_t marker, std::string const& body) {
  std::string out;
  out.reserve(5 + body.size());
  putBe32(out, static_cast<int32_t>(body.size() + 1));
  out.push_back(static_cast<char>(marker));
  out.append(body);
  return out;
}

bool decodeFrame(std::string_view buf, uint8_t& marker, std::string_view& body, size_t& consumed) {
  consumed = 0;
  if (buf.size() < 4) return false;
  int32_t len = getBe32(reinterpret_cast<unsigned char const*>(buf.data()));
  if (len < 1) throw TransportError("bad frame length " + std::to_string(len));
  if (buf.size() < 4 + static_cast<size_t>(len)) return false;
  marker = static_cast<uint8_t>(buf[4]);
  if (marker != kRequestMarker && marker != kResponseMarker) {
    throw TransportError("bad frame marker " + std::to_string(marker));
  }
  body = buf.substr(5, static_cast<size_t>(len) - 1);
  consumed = 4 + static_cast<size_t>(len);
  return true;
}

// ---- connection ------------------------------------------------------

Connection::Connection(std::string endpoint, Uuid client_id, TransportOptions options, std::string name)
    : endpoint_(std::move(endpoint)), name_(std::move(name)), client_id_(client_id), options_(options) {}

Connection::~Connection() {
  close();
}

int64_t Connection::nowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void Connection::setClusterId(Uuid const& id) {
  std::lock_guard<std::mutex> lk(cluster_mtx_);
  cluster_id_ = id;
}

void Connection::connectSocket() {
  auto colon = endpoint_.rfind(':');
  if (colon == std::string::npos) throw TransportError("endpoint without a port: " + endpoint_);
  std::string host = endpoint_.substr(0, colon);
  std::string port = endpoint_.substr(colon + 1);

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  int rc = getaddrinfo(host.c_str(), port.c_str(), &hints, &res);
  if (rc != 0) throw TransportError("resolve " + endpoint_ + ": " + gai_strerror(rc));

  int fd = -1;
  std::string last_error;
  for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      last_error = std::strerror(errno);
      continue;
    }
    // Bounded connect: non-blocking connect + poll, then back to blocking.
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
    if (rc != 0 && errno == EINPROGRESS) {
      pollfd pfd{fd, POLLOUT, 0};
      int polled = ::poll(&pfd, 1, options_.connect_timeout_ms);
      if (polled == 1) {
        int err = 0;
        socklen_t len = sizeof err;
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        rc = err == 0 ? 0 : -1;
        if (err != 0) last_error = std::strerror(err);
      } else {
        rc = -1;
        last_error = polled == 0 ? "connect timeout" : std::strerror(errno);
      }
    } else if (rc != 0) {
      last_error = std::strerror(errno);
    }
    if (rc == 0) {
      fcntl(fd, F_SETFL, flags);
      break;
    }
    ::close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) throw TransportError("connect " + endpoint_ + ": " + last_error);
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof one);
  fd_ = fd;
}

void Connection::open() {
  if (alive()) return;
  stopping_.store(false);
  connectSocket();
  alive_.store(true, std::memory_order_release);
  int64_t const now = nowMs();
  last_read_ms_.store(now);
  last_write_ms_.store(now);
  reader_ = std::thread(&Connection::readerLoop, this);
  keepalive_ = std::thread(&Connection::keepaliveLoop, this);
  try {
    handshake();
  } catch (...) {
    close();
    throw;
  }
}

void Connection::close() {
  stopping_.store(true);
  markDead("closed");
  keepalive_cv_.notify_all();
  if (reader_.joinable()) {
    if (reader_.get_id() == std::this_thread::get_id()) {
      reader_.detach();
    } else {
      reader_.join();
    }
  }
  if (keepalive_.joinable()) {
    if (keepalive_.get_id() == std::this_thread::get_id()) {
      keepalive_.detach();
    } else {
      keepalive_.join();
    }
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

void Connection::reopen() {
  close();
  open();
}

void Connection::markDead(std::string const& why) {
  bool was_alive = alive_.exchange(false, std::memory_order_acq_rel);
  if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR);  // unblocks the reader's recv
  std::unordered_map<int64_t, std::shared_ptr<Pending>> pending;
  {
    std::lock_guard<std::mutex> lk(pending_mtx_);
    pending.swap(pending_);
  }
  for (auto& kv : pending) {
    kv.second->promise.set_exception(
        std::make_exception_ptr(TransportError("connection " + name_ + " to " + endpoint_ + ": " + why)));
  }
  if (was_alive && why != "closed") {
    std::cerr << "[corfu-native] connection " << name_ << " to " << endpoint_ << " lost: " << why << "\n";
  }
}

void Connection::handshake() {
  // ClientHandshakeHandler.channelActive: epoch 0, cluster id 0/0,
  // ClusterIdCheck.CHECK, EpochCheck.IGNORE; server id 0/0 means "do
  // not verify the node id".
  RequestPayloadMsg payload;
  auto* hs = payload.mutable_handshake_request();
  *hs->mutable_client_id() = toMsg(client_id_);
  *hs->mutable_server_id() = toMsg(Uuid{});
  ResponsePtr resp = roundTrip(payload, /*ignore_epoch=*/true, /*ignore_cluster_id=*/false,
                               options_.handshake_timeout_ms);
  if (!resp->has_handshake_response()) {
    throw TransportError("handshake with " + endpoint_ + ": unexpected reply (payload case " +
                         std::to_string(resp->payload_case()) + ")");
  }
}

std::string Connection::buildRequest(int64_t request_id, RequestPayloadMsg const& payload,
                                     bool ignore_epoch, bool ignore_cluster_id) {
  RequestMsg req;
  HeaderMsg* h = req.mutable_header();
  // version: capability_vector empty, corfu_source_code_version 0. The
  // server only logs a mismatch (ServerHandshakeHandler).
  h->mutable_version();
  h->set_request_id(request_id);
  h->set_priority(org::corfudb::runtime::NORMAL);
  h->set_epoch(epoch_.load());
  {
    std::lock_guard<std::mutex> lk(cluster_mtx_);
    *h->mutable_cluster_id() = toMsg(cluster_id_);
  }
  *h->mutable_client_id() = toMsg(client_id_);
  h->set_ignore_cluster_id(ignore_cluster_id);
  h->set_ignore_epoch(ignore_epoch);
  *req.mutable_payload() = payload;
  std::string body;
  if (!req.SerializeToString(&body)) {
    throw TransportError("RequestMsg serialization failed (payload case " +
                         std::to_string(payload.payload_case()) + ")");
  }
  return encodeFrame(kRequestMarker, body);
}

void Connection::sendFrame(std::string const& frame) {
  std::lock_guard<std::mutex> lk(write_mtx_);
  if (!alive()) throw TransportError("connection " + name_ + " to " + endpoint_ + " is not open");
  size_t off = 0;
  while (off < frame.size()) {
    ssize_t n = ::send(fd_, frame.data() + off, frame.size() - off, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      std::string why = std::string("send: ") + std::strerror(errno);
      markDead(why);
      throw TransportError("connection " + name_ + " to " + endpoint_ + ": " + why);
    }
    off += static_cast<size_t>(n);
  }
  last_write_ms_.store(nowMs());
}

Connection::ResponsePtr Connection::roundTrip(RequestPayloadMsg const& payload, bool ignore_epoch,
                                              bool ignore_cluster_id, int timeout_ms) {
  int64_t const id = next_request_id_.fetch_add(1);
  auto pending = std::make_shared<Pending>();
  std::future<ResponsePtr> fut = pending->promise.get_future();
  {
    std::lock_guard<std::mutex> lk(pending_mtx_);
    if (!alive()) throw TransportError("connection " + name_ + " to " + endpoint_ + " is not open");
    pending_[id] = pending;
  }
  std::string frame = buildRequest(id, payload, ignore_epoch, ignore_cluster_id);
  try {
    sendFrame(frame);
  } catch (...) {
    std::lock_guard<std::mutex> lk(pending_mtx_);
    pending_.erase(id);
    throw;
  }
  if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) != std::future_status::ready) {
    {
      std::lock_guard<std::mutex> lk(pending_mtx_);
      pending_.erase(id);
    }
    // The server drops requests it cannot route in silence (an
    // unregistered payload type, or one sent before the handshake), so a
    // timeout is the only signal. The caller reconnects.
    throw TransportError("connection " + name_ + " to " + endpoint_ + ": no reply to request " +
                         std::to_string(id) + " (payload case " + std::to_string(payload.payload_case()) +
                         ") within " + std::to_string(timeout_ms) + " ms");
  }
  return fut.get();  // rethrows the TransportError set by markDead
}

ResponsePayloadMsg Connection::call(RequestPayloadMsg const& payload, bool ignore_epoch,
                                    bool ignore_cluster_id) {
  ResponsePtr r = roundTrip(payload, ignore_epoch, ignore_cluster_id, options_.request_timeout_ms);
  return std::move(*r);
}

void Connection::readerLoop() {
  std::string buf;
  buf.reserve(1 << 16);
  char chunk[1 << 16];
  while (!stopping_.load() && alive()) {
    ssize_t n = ::recv(fd_, chunk, sizeof chunk, 0);
    if (n == 0) {
      markDead("peer closed the socket");
      return;
    }
    if (n < 0) {
      if (errno == EINTR) continue;
      if (!alive()) return;  // closed by us
      markDead(std::string("recv: ") + std::strerror(errno));
      return;
    }
    last_read_ms_.store(nowMs());
    buf.append(chunk, static_cast<size_t>(n));
    size_t start = 0;
    try {
      while (true) {
        uint8_t marker = 0;
        std::string_view body;
        size_t consumed = 0;
        if (!decodeFrame(std::string_view(buf).substr(start), marker, body, consumed)) break;
        start += consumed;
        if (marker != kResponseMarker) continue;  // a request marker from a server: ignore
        auto resp = std::make_shared<ResponsePayloadMsg>();
        ResponseMsg msg;
        if (!msg.ParseFromArray(body.data(), static_cast<int>(body.size()))) {
          throw TransportError("unparseable ResponseMsg (" + std::to_string(body.size()) + " bytes)");
        }
        int64_t const id = msg.header().request_id();
        if (msg.header().has_version()) {
          server_version_.store(msg.header().version().corfu_source_code_version());
        }
        std::shared_ptr<Pending> waiter;
        {
          std::lock_guard<std::mutex> lk(pending_mtx_);
          auto it = pending_.find(id);
          if (it != pending_.end()) {
            waiter = it->second;
            pending_.erase(it);
          }
        }
        if (waiter) {
          *resp = std::move(*msg.mutable_payload());
          waiter->promise.set_value(std::move(resp));
        }
        // No waiter: a ping reply, or a request that timed out. Dropped.
      }
    } catch (TransportError const& e) {
      markDead(e.what());
      return;
    }
    if (start > 0) buf.erase(0, start);
  }
}

void Connection::keepaliveLoop() {
  std::unique_lock<std::mutex> lk(keepalive_mtx_);
  while (!stopping_.load() && alive()) {
    keepalive_cv_.wait_for(lk, std::chrono::milliseconds(options_.keepalive_idle_ms / 2 + 1));
    if (stopping_.load() || !alive()) break;
    int64_t const now = nowMs();
    if (now - last_read_ms_.load() >= options_.read_idle_ms) {
      markDead("read idle for " + std::to_string(now - last_read_ms_.load()) + " ms");
      break;
    }
    if (now - last_write_ms_.load() >= options_.keepalive_idle_ms) {
      // Fire-and-forget ping: no waiter, the reply only refreshes
      // last_read_ms_. Both ignore flags, as NettyClientRouter.keepAlive.
      RequestPayloadMsg payload;
      payload.mutable_ping_request();
      std::string frame = buildRequest(next_request_id_.fetch_add(1), payload,
                                       /*ignore_epoch=*/true, /*ignore_cluster_id=*/true);
      try {
        sendFrame(frame);
      } catch (TransportError const&) {
        break;  // sendFrame marked the connection dead
      }
    }
  }
}

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
