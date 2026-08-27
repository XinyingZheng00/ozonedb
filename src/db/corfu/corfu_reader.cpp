#ifdef OZONEDB_ENABLE_CORFU
#include "corfu/corfu_reader.h"
#include "service/corfu_message.pb.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

namespace ozonedb::corfu {

using org::corfudb::runtime::ReadLogResponseMsg;
using org::corfudb::runtime::TokenRequestMsg;
using org::corfudb::runtime::TokenResponseMsg;

using Poll = CorfuClient::Poll;

Reader::Reader(Rpc& rpc, Uuid const& stream, CorfuClientOptions const& options)
    : rpc_(rpc), stream_(stream), options_(options) {}

Reader::~Reader() {
  close();
}

void Reader::close() {
  closed_.store(true);
  std::lock_guard<std::mutex> lk(wake_mtx_);
  wake_cv_.notify_all();
}

bool Reader::seek(int64_t addr) {
  if (addr < 0) addr = 0;
  dropBatch();
  next_ = addr;
  tail_ = addr - 1;
  return true;
}

void Reader::wakeOwnAppend(int64_t addr) {
  int64_t prev = own_addr_.load();
  while (addr > prev && !own_addr_.compare_exchange_weak(prev, addr)) {
  }
  std::lock_guard<std::mutex> lk(wake_mtx_);
  wake_cv_.notify_all();
}

void Reader::dropBatch() {
  resp_.reset();
  order_.clear();
  pos_ = 0;
}

Poll Reader::fetchBatch() {
  dropBatch();
  int64_t const last = std::min(tail_, next_ + static_cast<int64_t>(options_.read_batch) - 1);
  std::vector<int64_t> addrs;
  addrs.reserve(static_cast<size_t>(last - next_ + 1));
  for (int64_t a = next_; a <= last; ++a) addrs.push_back(a);
  ReadLogResponseMsg resp;
  try {
    // cache_results: keep the server's read cache warm for the other
    // tailers of the same log.
    resp = rpc_.read(addrs, /*cache_results=*/true);
  } catch (ServerError const& e) {
    if (e.kind() == ServerError::Kind::kTrimmed) return Poll::kTrimmed;
    std::cerr << "[corfu-native] read [" << next_ << ", " << last << "]: " << e.what() << "\n";
    return Poll::kError;
  } catch (TransportError const& e) {
    std::cerr << "[corfu-native] read [" << next_ << ", " << last << "]: " << e.what() << "\n";
    return Poll::kError;
  }
  resp_ = std::make_unique<ReadLogResponseMsg>(std::move(resp));
  order_.resize(static_cast<size_t>(resp_->response_size()));
  for (int i = 0; i < resp_->response_size(); ++i) order_[static_cast<size_t>(i)] = i;
  std::sort(order_.begin(), order_.end(), [&](int a, int b) {
    return resp_->response(a).address() < resp_->response(b).address();
  });
  pos_ = 0;
  return Poll::kEntries;
}

int Reader::deliver(int max_entries, CorfuClient::EntrySink const& sink, bool& hit_empty, bool& error) {
  hit_empty = false;
  error = false;
  int delivered = 0;
  while (resp_ && pos_ < order_.size() && delivered < max_entries) {
    auto const& r = resp_->response(order_[pos_]);
    int64_t const addr = r.address();
    if (addr < next_) {  // a duplicate or a stale address: ignore
      ++pos_;
      continue;
    }
    if (addr > next_) {
      // The server skipped an address we asked for. Treat the gap as
      // EMPTY at next_ and let settleEmpty decide.
      hit_empty = true;
      return delivered;
    }
    LogData ld;
    std::string err;
    std::string const& bytes = r.log_data().entry();
    if (!decodeLogData(bytes, ld, err)) {
      std::cerr << "[corfu-native] address " << addr << ": " << err << "\n";
      error = true;
      return delivered;
    }
    switch (ld.type) {
      case DataType::kTrimmed:
        // The cursor is below the trim mark. Do not advance: hand over
        // what was delivered, and the next call reports kTrimmed (-1
        // when nothing was delivered by this call).
        return delivered == 0 ? -1 : delivered;
      case DataType::kEmpty:
        hit_empty = true;
        return delivered;
      case DataType::kHole:
      case DataType::kRankOnly:
        ++next_;
        ++pos_;
        continue;
      case DataType::kData:
        break;
    }
    if (!ld.containsStream(stream_) || ld.corfu_payload) {
      foreign_skipped_.fetch_add(1);
      ++next_;
      ++pos_;
      continue;
    }
    if (ld.effectiveCodec() != kCodecNone) {
      std::cerr << "[corfu-native] address " << addr << " has payload codec " << ld.effectiveCodec()
                << " (0 = NONE is the only one supported; the dataset was written by a runtime "
                   "with ZSTD on -- reload it with codec NONE)\n";
      error = true;
      return delivered;
    }
    sink(addr, reinterpret_cast<unsigned char const*>(ld.payload.data()), ld.payload.size());
    ++delivered;
    ++next_;
    ++pos_;
  }
  return delivered;
}

Poll Reader::settleEmpty() {
  // AddressSpaceView with waitForHole: re-read with backoff up to the
  // hole-fill timeout, then write a HOLE. An OverwriteException means
  // the writer landed first: re-read.
  auto const deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(options_.hole_fill_timeout_ms);
  int backoff_ms = 1;
  for (;;) {
    if (closed_.load()) return Poll::kError;
    ReadLogResponseMsg resp;
    try {
      resp = rpc_.read({next_}, /*cache_results=*/false);
    } catch (ServerError const& e) {
      if (e.kind() == ServerError::Kind::kTrimmed) return Poll::kTrimmed;
      std::cerr << "[corfu-native] re-read " << next_ << ": " << e.what() << "\n";
      return Poll::kError;
    } catch (TransportError const& e) {
      std::cerr << "[corfu-native] re-read " << next_ << ": " << e.what() << "\n";
      return Poll::kError;
    }
    bool empty = true;
    for (auto const& r : resp.response()) {
      if (r.address() != next_) continue;
      LogData ld;
      std::string err;
      if (!decodeLogData(r.log_data().entry(), ld, err)) {
        std::cerr << "[corfu-native] address " << next_ << ": " << err << "\n";
        return Poll::kError;
      }
      if (ld.type == DataType::kTrimmed) return Poll::kTrimmed;
      empty = ld.type == DataType::kEmpty;
    }
    if (!empty) {
      // Written now. Load a fresh batch from here; the caller delivers.
      dropBatch();
      return Poll::kEntries;
    }
    if (std::chrono::steady_clock::now() >= deadline) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    backoff_ms = std::min(backoff_ms * 2, 64);
  }
  // Fill the hole (ChainReplicationProtocol.holeFill).
  try {
    rpc_.write(encodeHole(next_, rpc_.epoch()));
    holes_filled_.fetch_add(1);
    std::cerr << "[corfu-native] filled a hole at " << next_ << " after "
              << options_.hole_fill_timeout_ms << " ms\n";
  } catch (ServerError const& e) {
    if (e.kind() == ServerError::Kind::kTrimmed) return Poll::kTrimmed;
    if (e.kind() != ServerError::Kind::kOverwrite) {
      std::cerr << "[corfu-native] hole fill at " << next_ << ": " << e.what() << "\n";
      return Poll::kError;
    }
    // Overwrite: the address was written meanwhile. Re-read below.
  } catch (TransportError const& e) {
    std::cerr << "[corfu-native] hole fill at " << next_ << ": " << e.what() << "\n";
    return Poll::kError;
  }
  dropBatch();
  return Poll::kEntries;
}

Poll Reader::pollBatch(int timeout_ms, int max_entries, CorfuClient::EntrySink const& sink) {
  auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  int delivered_total = 0;
  for (;;) {
    if (closed_.load()) return delivered_total > 0 ? Poll::kEntries : Poll::kError;
    // 1. A loaded batch, or addresses known to exist: deliver.
    if ((resp_ && pos_ < order_.size()) || next_ <= tail_) {
      if (!(resp_ && pos_ < order_.size())) {
        Poll f = fetchBatch();
        if (f != Poll::kEntries) return delivered_total > 0 ? Poll::kEntries : f;
      }
      bool hit_empty = false;
      bool error = false;
      int n = deliver(max_entries - delivered_total, sink, hit_empty, error);
      if (n < 0) {
        // A TRIMMED entry at next_ with nothing delivered from this batch.
        dropBatch();
        return delivered_total > 0 ? Poll::kEntries : Poll::kTrimmed;
      }
      delivered_total += n;
      if (error) {
        dropBatch();
        return delivered_total > 0 ? Poll::kEntries : Poll::kError;
      }
      if (delivered_total >= max_entries) return Poll::kEntries;
      if (hit_empty) {
        // Never pad: hand over what we have and settle the hole on the
        // next call.
        if (delivered_total > 0) return Poll::kEntries;
        Poll s = settleEmpty();
        if (s != Poll::kEntries) return s;
        continue;
      }
      if (resp_ && pos_ >= order_.size()) dropBatch();
      if (delivered_total > 0 && next_ > tail_) return Poll::kEntries;
      continue;
    }
    // 2. Nothing known beyond next_. Own appends first (no round trip).
    int64_t own = own_addr_.load();
    if (own > tail_) {
      tail_ = own;
      continue;
    }
    // 3. Ask the sequencer for the stream tail.
    if (delivered_total > 0) return Poll::kEntries;  // never pad a batch with a query
    TokenRequestMsg req;
    req.set_request_type(TokenRequestMsg::TK_QUERY);
    req.set_num_tokens(0);
    *req.add_streams() = toMsg(stream_);
    int64_t t = -1;
    try {
      TokenResponseMsg resp = rpc_.token(req);
      for (auto const& st : resp.stream_tails()) {
        if (fromMsg(st.key()) == stream_) t = st.value();
      }
    } catch (std::exception const& e) {
      std::cerr << "[corfu-native] stream tail query: " << e.what() << "\n";
      return Poll::kError;
    }
    if (t > tail_) {
      tail_ = t;
      continue;
    }
    // 4. Idle: wait for an own append or the idle tick, until the deadline.
    auto const now = std::chrono::steady_clock::now();
    if (now >= deadline) return Poll::kIdle;
    auto wait = std::min(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now),
                         std::chrono::milliseconds(std::max(options_.idle_poll_ms, 1)));
    std::unique_lock<std::mutex> lk(wake_mtx_);
    wake_cv_.wait_for(lk, wait, [&] { return closed_.load() || own_addr_.load() > tail_; });
  }
}

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
