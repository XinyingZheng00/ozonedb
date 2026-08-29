// The native C++ CorfuClient (PLAN-native-corfu.md): tokens and writes
// on the `ctl` connection, the sequential tailer on `tail`, no JVM.
#ifdef OZONEDB_ENABLE_CORFU
#include "corfu/corfu_codec.h"
#include "corfu/corfu_reader.h"
#include "corfu/corfu_rpc.h"
#include "corfu_client.h"
#include "service/corfu_message.pb.h"
#include <atomic>
#include <iostream>

namespace ozonedb {

namespace {
using corfu::Reader;
using corfu::Rpc;
using corfu::ServerError;
using corfu::TransportError;
using corfu::Uuid;
using org::corfudb::runtime::TokenRequestMsg;
using org::corfudb::runtime::TokenResponseMsg;
using org::corfudb::runtime::TxResolutionInfoMsg;

// StreamsView.append retries a token + write this many times on an
// OverwriteException (RuntimeParameters.writeRetry).
constexpr int kWriteRetry = 5;
// Address.NON_EXIST: what the sequencer reports for a stream with no entry.
constexpr int64_t kNonExist = -6;

class NativeCorfuClient : public CorfuClient {
 public:
  explicit NativeCorfuClient(CorfuClientOptions const& options)
      : client_id_(corfu::randomUuid()),
        stream_(corfu::streamIdOf(options.stream_name)),
        rpc_(options, client_id_),
        reader_(rpc_, stream_, options) {
    std::cerr << "[corfu-native] client " << client_id_.str() << " stream " << options.stream_name
              << " = " << stream_.str() << "\n";
  }

  ~NativeCorfuClient() override { close(); }

  int64_t append(std::string_view payload) override {
    if (closed_.load()) return -1;
    try {
      for (int attempt = 0; attempt < kWriteRetry; ++attempt) {
        TokenRequestMsg req;
        req.set_request_type(TokenRequestMsg::TK_MULTI_STREAM);
        req.set_num_tokens(1);
        *req.add_streams() = corfu::toMsg(stream_);
        TokenResponseMsg tok = rpc_.token(req);
        if (tok.resp_type() != TokenResponseMsg::TX_NORMAL) {
          std::cerr << "[corfu-native] append: token refused (" << tok.resp_type() << ")\n";
          return -1;
        }
        int64_t addr = writeAt(tok, payload);
        if (addr >= 0) return addr;
        if (addr == kRetry) continue;
        return -1;
      }
      std::cerr << "[corfu-native] append: " << kWriteRetry << " overwrites in a row; giving up\n";
      return -1;
    } catch (std::exception const& e) {
      std::cerr << "[corfu-native] append failed: " << e.what() << "\n";
      return -1;
    }
  }

  CheckedAppend appendChecked(std::string_view payload, int64_t snapshot,
                              std::string_view const* read_key,
                              std::string_view const* write_key) override {
    CheckedAppend out;
    if (closed_.load()) {
      out.abort = kAbortOther;
      return out;
    }
    try {
      int64_t snapshot_epoch = rpc_.epoch();
      int64_t snapshot_seq = snapshot;
      for (int attempt = 0; attempt < kWriteRetry; ++attempt) {
        TokenRequestMsg req;
        req.set_request_type(TokenRequestMsg::TK_TX);
        req.set_num_tokens(1);
        *req.add_streams() = corfu::toMsg(stream_);
        TxResolutionInfoMsg* tx = req.mutable_txn_resolution();
        Uuid tx_id{client_id_.msb, static_cast<int64_t>(tx_counter_.fetch_add(1))};
        *tx->mutable_tx_id() = corfu::toMsg(tx_id);
        tx->mutable_snapshot_timestamp()->set_epoch(snapshot_epoch);
        tx->mutable_snapshot_timestamp()->set_sequence(snapshot_seq);
        if (read_key != nullptr) {
          auto* cs = tx->add_conflict_set();
          *cs->mutable_key() = corfu::toMsg(stream_);
          cs->add_value(std::string(*read_key));
        }
        if (write_key != nullptr) {
          auto* ws = tx->add_write_conflict_params_set();
          *ws->mutable_key() = corfu::toMsg(stream_);
          ws->add_value(std::string(*write_key));
        }
        TokenResponseMsg tok = rpc_.token(req);
        if (tok.resp_type() != TokenResponseMsg::TX_NORMAL) {
          // A refused token consumed no address. token.sequence names
          // the offending address (StreamsView -> TransactionAbortedException).
          switch (tok.resp_type()) {
            case TokenResponseMsg::TX_ABORT_CONFLICT: out.abort = kAbortConflict; break;
            case TokenResponseMsg::TX_ABORT_NEWSEQ: out.abort = kAbortNewSequencer; break;
            case TokenResponseMsg::TX_ABORT_SEQ_OVERFLOW: out.abort = kAbortSeqOverflow; break;
            case TokenResponseMsg::TX_ABORT_SEQ_TRIM: out.abort = kAbortSeqTrim; break;
            default: out.abort = kAbortOther; break;
          }
          out.offending = tok.token().sequence();
          return out;
        }
        int64_t addr = writeAt(tok, payload);
        if (addr >= 0) {
          out.addr = addr;
          return out;
        }
        if (addr == kRetry) {
          // StreamsView.append: check for conflicts only from the
          // previous attempt's position on.
          snapshot_epoch = tok.token().epoch();
          snapshot_seq = tok.token().sequence();
          continue;
        }
        out.abort = kAbortOther;
        return out;
      }
      std::cerr << "[corfu-native] appendChecked: " << kWriteRetry << " overwrites in a row; giving up\n";
    } catch (std::exception const& e) {
      std::cerr << "[corfu-native] appendChecked failed: " << e.what() << "\n";
    }
    out.abort = kAbortOther;
    return out;
  }

  int64_t globalTail() override {
    if (closed_.load()) return -1;
    try {
      TokenRequestMsg req;
      req.set_request_type(TokenRequestMsg::TK_QUERY);
      req.set_num_tokens(0);
      return rpc_.token(req).token().sequence();
    } catch (std::exception const& e) {
      std::cerr << "[corfu-native] globalTail failed: " << e.what() << "\n";
      return -1;
    }
  }

  int64_t streamTail() override {
    if (closed_.load()) return -1;
    try {
      TokenRequestMsg req;
      req.set_request_type(TokenRequestMsg::TK_QUERY);
      req.set_num_tokens(0);
      *req.add_streams() = corfu::toMsg(stream_);
      TokenResponseMsg tok = rpc_.token(req);
      for (auto const& st : tok.stream_tails()) {
        if (corfu::fromMsg(st.key()) == stream_) return st.value();
      }
      return kNonExist;
    } catch (std::exception const& e) {
      std::cerr << "[corfu-native] streamTail failed: " << e.what() << "\n";
      return -1;
    }
  }

  Poll pollBatch(int timeout_ms, int max_entries, EntrySink const& sink) override {
    if (closed_.load()) return Poll::kError;
    return reader_.pollBatch(timeout_ms, max_entries, sink);
  }

  bool seek(int64_t addr) override { return reader_.seek(addr); }

  void gc(int64_t /*mark*/) override {}  // the reader keeps no queue

  int64_t prefixTrim(int64_t addr) override {
    if (closed_.load()) return -1;
    try {
      // AddressSpaceView.prefixTrim + gc(): sequencer first, so a reader
      // cannot loop on an address the sequencer still hands out.
      rpc_.sequencerTrim(addr);
      rpc_.trimLog(addr);
      rpc_.compact();
      return rpc_.trimMark();
    } catch (std::exception const& e) {
      std::cerr << "[corfu-native] prefixTrim(" << addr << ") failed: " << e.what() << "\n";
      return -1;
    }
  }

  int64_t trimMark() override {
    if (closed_.load()) return -1;
    try {
      return rpc_.trimMark();
    } catch (std::exception const& e) {
      std::cerr << "[corfu-native] trimMark failed: " << e.what() << "\n";
      return -1;
    }
  }

  void close() override {
    if (closed_.exchange(true)) return;
    reader_.close();
    rpc_.close();
  }

 private:
  static constexpr int64_t kRetry = -2;  // writeAt: overwritten, take a new token

  Uuid client_id_;
  Uuid stream_;
  Rpc rpc_;
  Reader reader_;
  std::atomic<uint64_t> tx_counter_{1};
  std::atomic<bool> closed_{false};

  // Write payload at the token's address. Returns the address, kRetry on
  // an overwrite by different data, -1 on any other failure.
  int64_t writeAt(TokenResponseMsg const& tok, std::string_view payload) {
    std::vector<std::pair<Uuid, int64_t>> backpointers;
    for (auto const& bp : tok.backpointer_map()) {
      backpointers.emplace_back(corfu::fromMsg(bp.key()), bp.value());
    }
    if (backpointers.empty()) {
      // Every Java reader skips an entry whose BACKPOINTER_MAP lacks the
      // stream (AddressMapStreamView). The sequencer always returns the
      // map for a stream token; guard anyway.
      backpointers.emplace_back(stream_, -1);
    }
    int64_t const addr = tok.token().sequence();
    std::string entry = corfu::encodeDataEntry(payload, backpointers, addr, tok.token().epoch(), client_id_);
    try {
      rpc_.write(entry);
    } catch (ServerError const& e) {
      if (e.kind() == ServerError::Kind::kOverwrite) {
        if (e.detail() == corfu::kOverwriteSameData) return addr;  // our retry landed already
        std::cerr << "[corfu-native] address " << addr << " overwritten (cause " << e.detail()
                  << "); taking a new token\n";
        return kRetry;
      }
      std::cerr << "[corfu-native] write at " << addr << ": " << e.what() << "\n";
      return -1;
    }
    reader_.wakeOwnAppend(addr);
    return addr;
  }
};
}  // namespace

std::unique_ptr<CorfuClient> makeNativeCorfuClient(CorfuClientOptions const& options) {
  return std::make_unique<NativeCorfuClient>(options);
}

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_CORFU
