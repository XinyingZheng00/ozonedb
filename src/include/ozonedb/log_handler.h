#ifndef LOG_HANDLER_H
#define LOG_HANDLER_H

#include "cache.h"
#include "log_key_index.h"
#include "metadata.h"
#include "metadata_log_handler.h"
#include "protobuf/record.pb.h"
#include "protobuf_serializer.h"
#include "storage.h"
#include "thread_pool.h"
#include <list>
#include <memory>
#include <string>
#include <vector>

namespace ozonedb {
class LogHandler {
 private:
  /**
   * @brief prefix for each file
   * for datalog, it is in "/tmp/db/datafile/datalog"
   * for leveled sstable, it is "/tmp/db/datafile/level_x/"
   * for task, it is "/tmp/db/task"
   */
  std::string prefix;
  std::string active_unit;
  View* latest_view = nullptr;
  size_t file_size_limit;
  Storage* storage = nullptr;
  LRUCache* cache = nullptr;
  MetadataLogHandler* metadata_log = nullptr;
  ThreadPool* thread_pool = nullptr;

  /**
   * @brief create a new unit in this layer
   *
   * if reached the end of the file or the file is already sealed, go to next log file
   *
   * @return Status
   */
  Status newTail();

 public:
  /**
   * @brief Construct a new Handler object
   *
   * @param file_size_limit
   * @param log_prefix
   * @param storage
   */
  LogHandler(uint64_t file_size_limit, std::string log_prefix, Storage* storage,
             LRUCache* global_cache, MetadataLogHandler* metadata_log = nullptr,
             bool enable_key_index = false, size_t key_index_capacity = 1000000,
             bool trust_background_tail = false)
      : file_size_limit(file_size_limit), prefix(std::move(log_prefix)), storage(storage),
        trust_background_tail_(trust_background_tail) {
    storage->createDirectory(prefix);
    cache = global_cache;
    this->metadata_log = metadata_log;
    if (enable_key_index) {
      key_index_ = std::make_unique<LogKeyIndex>(key_index_capacity);
      // Wire into the storage's remote-write listener so peer APPENDs
      // and REMOVEs keep the index in sync. No-op on backends that
      // don't override setRemoteAppendListener (local/Azure/S3).
      storage->setRemoteAppendListener(
          [this](std::string const& file, unsigned char const* data,
                 size_t len, RemoteOp op) {
            this->onRemoteAppend(file, data, len, op);
          });
    }
  };

  ~LogHandler() {
    // Clear the storage's listener so a tailer-invoked callback can't
    // call into this object after it's destroyed. Still narrowly racy
    // if the tailer is mid-invocation (it holds a copy of the old
    // callback); Corfu's storage destructor joins the tailer thread
    // before returning so the full process-shutdown ordering is safe
    // as long as DB tears down log_handler before log_storage (see
    // db.cpp ~DB).
    if (storage != nullptr) {
      storage->setRemoteAppendListener(RemoteAppendListener{});
    }
  };

  // set latest view
  void setLatestView(View* view) { latest_view = view; }

  /**
   * @brief write record into this level
   *
   * @param record
   * @return Status
   */
  Status addRecord(Record const& record);

  /**
   * @brief read record from this level, end to the offset
   *
   * @param key
   * @param value
   * @return Status
   */
  Status readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset);

  // View-independent fast path: probe the in-memory key index without
  // touching latest_view or any storage backend. Returns true and sets
  // `record` when the key is present in the index; false otherwise. The
  // index is kept fresh by addRecord (local writes) and the Corfu
  // tailer's onRemoteAppend callback. Callers (DB::get) can short-
  // circuit the View deep-copy and the tail_cache lookup when this
  // hits. Only safe to trust as authoritative when the Metadata
  // `trust_background_tail` flag is enabled.
  bool tryIndexLookup(std::string const& key, Record*& record);

  // Populate key_index_ from log files already materialized in the
  // cache. Called once at openDB after metadata rollforward so the
  // index reflects pre-existing state before the first read.
  Status warmKeyIndex();

  // Drop key_index_ entries whose source_file is in `files`. Called by
  // compaction just after it emits the COMPACT record and before it
  // frees the merged Record*s. Idempotent — safe to call twice for
  // the same file.
  void invalidateCompactedLog(std::vector<std::string> const& files);

  // Invoked by the storage layer when a *remote* APPEND or REMOVE
  // arrives on the shared log. Deserializes log-prefixed APPEND
  // payloads into Records and routes them into the cache + index.
  // Ignores files outside this handler's prefix (SSTables, other
  // log streams). See Storage::setRemoteAppendListener.
  void onRemoteAppend(std::string const& file_name,
                      unsigned char const* data, size_t len, RemoteOp op);

  // set thread pool
  void setThreadPool(ThreadPool* thread_pool) { this->thread_pool = thread_pool; }
  // after seal the file, we may create metadata for the file

 private:
  std::unique_ptr<LogKeyIndex> key_index_;
  bool trust_background_tail_ = false;
};
}  // namespace ozonedb
#endif  // LOG_H