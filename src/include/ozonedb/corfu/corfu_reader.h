#ifndef OZONEDB_CORFU_READER_H
#define OZONEDB_CORFU_READER_H
#ifdef OZONEDB_ENABLE_CORFU
// The sequential tailer of PLAN-native-corfu.md §4.5. It reads the
// GLOBAL address space in order, [next_, tail_], in batches of
// read_batch addresses, and classifies every address:
//
//   DATA with the stream in BACKPOINTER_MAP  -> delivered to the sink
//   DATA without it, or HOLE                 -> skipped
//   EMPTY                                    -> a token without a write:
//                                               wait, then fill a HOLE
//   TRIMMED, or trimmed_error                -> Poll::kTrimmed
//
// That is correct only while every entry in the log belongs to this one
// stream; the phase-1 probe verifies it on the loaded datasets. It needs
// no address map and no Roaring bitmap.
#include "corfu/corfu_codec.h"
#include "corfu/corfu_rpc.h"
#include "corfu_client.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace org::corfudb::runtime {
class ReadLogResponseMsg;
}

namespace ozonedb::corfu {

class Reader {
 public:
  Reader(Rpc& rpc, Uuid const& stream, CorfuClientOptions const& options);
  ~Reader();

  // CorfuClient::pollBatch semantics. Single consumer: one thread at a
  // time (the tailer, or the constructor during the replay).
  CorfuClient::Poll pollBatch(int timeout_ms, int max_entries, CorfuClient::EntrySink const& sink);
  // CorfuClient::seek: next_ = addr, forget the decoded remainder.
  bool seek(int64_t addr);
  // An own append landed at addr: raise the known tail and wake a
  // waiting poll without the idle tick.
  void wakeOwnAppend(int64_t addr);
  void close();

  // Diagnostics.
  int64_t next() const { return next_; }
  int64_t knownTail() const { return tail_; }
  uint64_t holesFilled() const { return holes_filled_.load(); }
  uint64_t foreignSkipped() const { return foreign_skipped_.load(); }

 private:
  Rpc& rpc_;
  Uuid stream_;
  CorfuClientOptions options_;
  int64_t next_ = 0;   // first address not yet delivered or skipped
  int64_t tail_ = -1;  // highest address known to belong to the stream
  // The decoded batch not yet handed to the sink. `resp_` owns the bytes
  // the sink's pointers alias; `order_` lists the response indexes in
  // address order; `pos_` is the next one to deliver.
  std::unique_ptr<org::corfudb::runtime::ReadLogResponseMsg> resp_;
  std::vector<int> order_;
  size_t pos_ = 0;
  std::mutex wake_mtx_;
  std::condition_variable wake_cv_;
  std::atomic<int64_t> own_addr_{-1};
  std::atomic<bool> closed_{false};
  std::atomic<uint64_t> holes_filled_{0};
  std::atomic<uint64_t> foreign_skipped_{0};

  // Fetch [next_, min(tail_, next_ + read_batch - 1)] into resp_.
  // Returns kTrimmed / kError on failure, kEntries when a batch (possibly
  // with nothing deliverable) is loaded.
  CorfuClient::Poll fetchBatch();
  // Deliver from resp_ until max_entries, an EMPTY, or the end. Returns
  // the count delivered; stops at an EMPTY with next_ on it.
  int deliver(int max_entries, CorfuClient::EntrySink const& sink, bool& hit_empty, bool& error);
  // Re-read next_ with backoff until it is written, else write a HOLE.
  // Returns kEntries when the address is settled (data or hole), else
  // kTrimmed / kError.
  CorfuClient::Poll settleEmpty();
  void dropBatch();
};

}  // namespace ozonedb::corfu
#endif  // OZONEDB_ENABLE_CORFU
#endif  // OZONEDB_CORFU_READER_H
