#ifndef COMPACTION_H
#define COMPACTION_H

#include "log_handler.h"
#include "metadata.h"
#include "sstable/sstable_handler.h"
#include "sstable/table_builder.h"
#include "storage.h"
#include "task_log_handler.h"
#include <thread>
#include <unordered_map>
#include <vector>
namespace ozonedb {
class FileMutexManager;
class EventListener;
enum class Mode;
/**
 * @brief Represent a compaction task.
 *
 */
class Compaction {
 public:
  TaskRecord::TaskIdentifier* task_id = nullptr;
  TableBuilder* outputBuilder = nullptr;
  bool finished = false;
  int compaction_version = -1; // this is only used for the last layer compaction
};

class CompactionWatcher {
 private:
  Metadata* metadata = nullptr;
  Storage* storage = nullptr;          // log_storage — atomic-append backend
  Storage* sstable_storage = nullptr;  // sstable_storage — regular object storage
  LogHandler* task_handler = nullptr;
  LogHandler* log_handler = nullptr;
  SSTableHandler* sstable_handler = nullptr;
  MetadataLogHandler* metadata_handler = nullptr;
  TaskLogHandler* task_log_handler = nullptr;
  std::thread* compaction_thread = nullptr;
  ThreadPool* thread_pool = nullptr;
  EventListener* event_listener = nullptr;
  std::string fingerprint;
  View latest_view;
  FileMutexManager* file_mutex_manager = nullptr;
  Mode mode;

  Status watchForCompaction(std::atomic<bool> const* active);
  bool shouldWorkOnTask(TaskRecord::TaskIdentifier* task_id, TaskRecord*& task_record, int owner_generation);

 public:
  /**
   * @brief Construct a new Compaction Watcher object
   *
   * @param metadata
   * @param storage
   * @param level_handlers
   */
  CompactionWatcher(Metadata* metadata, Storage* storage, LogHandler* log_handler, MetadataLogHandler* metadata_handler, SSTableHandler* sstable_handler, std::string fingerprint)
      : metadata(metadata), storage(storage), sstable_storage(storage), log_handler(log_handler), metadata_handler(metadata_handler), sstable_handler(sstable_handler), fingerprint(fingerprint) {
    // sstable_storage defaults to the same backend as the log layer for
    // backward compat. setSSTableStorage(...) overrides it after construction
    // when the config splits SSTable storage off (paper §3.5).
  }
  ~CompactionWatcher() {
    // delete this->compaction_thread;
    // this->compaction_thread = nullptr;
  }
  // set event listener
  void setEventListener(EventListener* event_listener) { this->event_listener = event_listener; }

  // set task log handler
  void setTaskLogHandler(TaskLogHandler* task_log_handler) { this->task_log_handler = task_log_handler; }
  // set the SSTable-only storage backend (paper §3.5 split). When unset
  // sstable_storage falls back to the same Storage* as the log layer.
  void setSSTableStorage(Storage* s) { this->sstable_storage = s; }
  // set file mutex manager
  void setFileMutexManager(FileMutexManager* file_mutex_manager) { this->file_mutex_manager = file_mutex_manager; }
  // set thread pool
  void setThreadPool(ThreadPool* thread_pool) { this->thread_pool = thread_pool; }
  void setMode(Mode mode) { this->mode = mode; }

  Status pickCompaction(Compaction*& compaction, TaskRecord*& task_record, bool& has_worked_on_compaction);
  Status startCompaction(Compaction* compaction, TaskRecord* task_record);
  /**
   * @brief Start the compaction watcher
   *
   * New a thread to continuesly watch for compaction tasks while database is active
   *
   * @param active
   * @return Status
   */
  Status startCompactionWatcher(std::atomic<bool> const* active);

  /**
   * @brief stop the compaction watcher
   *
   * Join the thread that watches for compaction tasks
   *
   * @return Status
   */
  Status stopCompactionWatcher();

 private:
  Status taskHeartbeat(Compaction* compaction, TaskRecord* task_record);
  Status initInputUnit(Compaction* compaction, std::deque<std::string>& units, int limit);
  Status initInputUnitInlastLayer(Compaction* compaction, std::deque<std::string>& units, int limit);
  Status doCompactionWork(Compaction* compaction);
};
}  // namespace ozonedb

#endif