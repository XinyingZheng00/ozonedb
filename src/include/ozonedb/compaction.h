#ifndef COMPACTION_H
#define COMPACTION_H

#include "data_log_handler.h"
#include "event_listener.h"
#include "metadata.h"
#include "sstable/sstable_handler.h"
#include "sstable/table_builder.h"
#include "storage/storage.h"
#include "task_log_handler.h"
#include <thread>
#include <unordered_map>
#include <vector>
namespace ozonedb {

class Compaction {
 public:
  TaskRecord::TaskIdentifier* task_id = nullptr;
  TableBuilder* outputBuilder = nullptr;
  bool finished = false;
  int compaction_version = -1;  // this is only used for the last layer compaction
};

class CompactionWatcher {
 private:
  Metadata* metadata = nullptr;
  Storage* log_storage = nullptr;
  FileStorage* sst_storage = nullptr;

  DataLogHandler* log_handler = nullptr;
  SSTableHandler* sstable_handler = nullptr;
  MetadataLogHandler* metadata_handler = nullptr;
  TaskLogHandler* task_log_handler = nullptr;
  std::thread* compaction_thread = nullptr;
  EventListener* event_listener = nullptr;
  std::string fingerprint;
  View latest_view;

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
  CompactionWatcher(Metadata* metadata, Storage* log_storage, FileStorage* sst_storage, DataLogHandler* log_handler, MetadataLogHandler* metadata_handler,
                    SSTableHandler* sstable_handler, std::string fingerprint, Storage* tasklog_storage)
      : metadata(metadata), log_storage(log_storage), sst_storage(sst_storage), log_handler(log_handler), metadata_handler(metadata_handler), sstable_handler(sstable_handler), fingerprint(fingerprint) {
    this->task_log_handler = new TaskLogHandler(this->metadata->task_log_path, tasklog_storage);
  }
  ~CompactionWatcher() {
    // delete this->compaction_thread;
    // this->compaction_thread = nullptr;
  }
  // set event listener
  void setEventListener(EventListener* event_listener) { this->event_listener = event_listener; }

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