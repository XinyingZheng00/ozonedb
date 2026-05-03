#include "compaction.h"
#include "db.h"
#include "helper.h"
namespace ozonedb {
Status CompactionWatcher::startCompactionWatcher(std::atomic<bool> const* active) {
  std::cout << "Starting compaction watcher" << std::endl;
  this->compaction_thread = new std::thread(&CompactionWatcher::watchForCompaction, this, active);
  return Status::kSuccess;
}
Status CompactionWatcher::stopCompactionWatcher() {
  this->compaction_thread->join();
  return Status::kSuccess;
}

bool CompactionWatcher::shouldWorkOnTask(TaskRecord::TaskIdentifier* task_id, TaskRecord*& task_record, int owner_generation) {
  task_record = new TaskRecord();
  task_record->set_allocated_task_id(task_id);
  task_record->set_owner(this->fingerprint);
  task_record->set_status(TaskRecord::TASK_BEGIN);
  task_record->set_owner_generation(owner_generation);
  if (this->mode == Mode::Singleton) {
    return true;
  }
  bool worth_append = this->task_log_handler->worthAppend(*task_record);
  if (!worth_append) {
    return false;
  }
  this->task_log_handler->appendToTaskLog(*task_record);

  this->task_log_handler->rollForwardTaskLog();
  return this->task_log_handler->isFirstWriterForTask(task_id, this->fingerprint, owner_generation);
}

Status CompactionWatcher::watchForCompaction(std::atomic<bool> const* active) {
  while (*active) {
    // Task log works as follows:
    //  1. Client comes and identifies a task to be done
    //  2. Client writes to the task log
    //  3. Client rolls forward the task log to check if it is the first writer of this task
    //  4. If it is the first writer, work on the task
    //  5. If it is not the first writer, check any dead task, also compete for the first writer to work on the tasks.
    //  6. If there is no dead task, go back to step 1

    // Dead Task logic:
    // 1. the compaction tasks are finished in hundreds of milliseconds, and the heartbeat rate is 100ms, so the threshold is set to 10,
    // which means that the TaskRecord is considered to be dead after 10 heartbeats are not received.
    bool has_worked_on_compaction;
    Compaction* compaction = nullptr;
    TaskRecord* task_record = nullptr;
    
    Status status = pickCompaction(compaction, task_record, has_worked_on_compaction);
    
    if (!has_worked_on_compaction) {
      
      std::this_thread::sleep_for(std::chrono::milliseconds(100)); // only sleep when there's nothing to do
      
    } else {
      
      Status status = startCompaction(compaction, task_record);
      delete compaction;
      delete task_record;
      compaction = nullptr;
      task_record = nullptr;
      
      if (status != Status::kSuccess) {
        
        return Status::kFailure;  // crash
      }
    }
  }
  return Status::kSuccess;
}

Status CompactionWatcher::taskHeartbeat(Compaction* compaction, TaskRecord* task_record) {
  task_record->set_status(TaskRecord::TASK_IN_PROGRESS);
  while (!(compaction->finished) && !(compaction->aborted.load())) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    this->task_log_handler->appendToTaskLog(*task_record);
  }
  // If the compaction was experimentally aborted, do NOT write COMPLETE —
  // leaving the task in BEGIN/IN_PROGRESS state so survivors detect it as
  // dead via the heartbeat-timeout path (Section 4.4 of the paper).
  if (compaction->aborted.load()) {
    return Status::kSuccess;
  }
  task_record->set_status(TaskRecord::TASK_COMPLETE);
  this->task_log_handler->appendToTaskLog(*task_record);
  return Status::kSuccess;
}

Status CompactionWatcher::pickCompaction(Compaction*& compaction, TaskRecord*& task_record, bool& has_worked_on_compaction) {
  this->metadata_handler->getLatestView(this->latest_view);
  // Replay the task log on every pick so the heartbeat-timeout check in
  // rollForwardTaskLog actually runs even when this writer is not currently
  // attempting to claim any new task. As-shipped, rollForwardTaskLog was
  // only invoked from shouldWorkOnTask (compaction.cpp:30) — meaning a
  // quiescent writer (e.g. a drain process with no live work in its view)
  // would never discover that other writers' tasks have gone dead.
  this->task_log_handler->rollForwardTaskLog();

  // Pick-trace instrumentation. With OZONEDB_PICK_TRACE=1 in the env, every
  // pickCompaction call dumps the metadata view (every storage-layout layer's
  // deque) at entry and emits a PICK_RESULT line for whatever TaskID it picks.
  // PICK_TRACE/PICK_RESULT share `ts=` so they can be joined in post-processing.
  static bool pick_trace = std::getenv("OZONEDB_PICK_TRACE") != nullptr;
  long pick_ts = 0;
  if (pick_trace) {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    pick_ts = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    std::cout << "PICK_TRACE pid=" << getpid() << " ts=" << pick_ts;
    for (auto const& entry : this->latest_view.storage_layout) {
      std::cout << " " << entry.first << "=[";
      bool first = true;
      for (auto const& f : entry.second) {
        if (!first) std::cout << ",";
        auto slash = f.find('/');
        std::cout << (slash == std::string::npos ? f : f.substr(slash + 1));
        first = false;
      }
      std::cout << "]";
    }
    std::cout << std::endl;
  }

  // Step 0 (added for the churn-tolerance experiment): check the dead-task
  // queue first. Without this, continuous workload keeps Steps 1-3 finding
  // work, so abandoned tasks marked DEAD by the heartbeat-timeout path can
  // sit unreclaimed indefinitely. The paper (Section 4.4) claims tasks "can
  // continue making progress" once detected — interleaving the dead-task
  // check ensures that in practice.
  {
    auto result = this->task_log_handler->getDeadTask();
    auto dead_task = result.first;
    int owner_generation = result.second;
    while (dead_task.IsInitialized()) {
      auto* dead_task_id = new TaskRecord::TaskIdentifier(dead_task);
      std::cout << getpid() << ":Found dead task " << dead_task_id->ShortDebugString() << std::endl;
      bool is_first_writer = shouldWorkOnTask(dead_task_id, task_record, owner_generation + 1);
      if (is_first_writer) {
        compaction = new Compaction();
        compaction->task_id = dead_task_id;
        if (pick_trace) std::cout << "PICK_RESULT pid=" << getpid() << " ts=" << pick_ts << " step=dead0 taskid=" << compaction->task_id->ShortDebugString() << std::endl;
        has_worked_on_compaction = true;
        return Status::kSuccess;
      } else {
        delete task_record;
        task_record = nullptr;
        result = this->task_log_handler->getDeadTask();
        dead_task = result.first;
        owner_generation = result.second;
      }
    }
  }

  // First step: check if there is any log level compaction
  // log level compaction
  auto files = this->latest_view.getWithPrefix(metadata->log_prefix);
  while (files.size() >= 2) {
    compaction = new Compaction();
    compaction->task_id = new TaskRecord::TaskIdentifier();
    initInputUnit(compaction, files, this->metadata->compaction_input_file_number);
    if (compaction->task_id->input_files_size() < 2) {
      delete compaction;
      delete task_record;
      compaction = nullptr;
      task_record = nullptr;
      continue;
    }
    compaction->task_id->set_compactintonextlevel(true);
    compaction->task_id->set_destinationlevel(1);
    if (this->metadata->max_level == 1) {
      compaction->compaction_version = 0;
    }
    bool is_first_writer = shouldWorkOnTask(compaction->task_id, task_record, 0);
    if (is_first_writer) {
      if (pick_trace) std::cout << "PICK_RESULT pid=" << getpid() << " ts=" << pick_ts << " step=log taskid=" << compaction->task_id->ShortDebugString() << std::endl;
      has_worked_on_compaction = true;
      return Status::kSuccess;
    }
    delete compaction;
    delete task_record;
    compaction = nullptr;
    task_record = nullptr;
  };

  // Second step: check if there is any sstable level compaction
  for (int level = 1; level < metadata->max_level; level++) {  // we don't need to compact the last level
    std::deque<std::string> level_files = this->latest_view.getWithPrefix(metadata->sstable_level_prefix + (std::to_string(level)));
    uint64_t file_size = 0;
    for (std::string const& file : level_files) {
      int each_file_size = this->latest_view.getFileSize(file);
      file_size += each_file_size;
    }
    while (file_size > metadata->level_size[level - 1]) {
      compaction = new Compaction();
      compaction->task_id = new TaskRecord::TaskIdentifier();
      initInputUnit(compaction, level_files, this->metadata->compaction_input_file_number);
      compaction->task_id->set_compactintonextlevel(true);
      compaction->task_id->set_destinationlevel(level + 1);
      bool is_first_writer = shouldWorkOnTask(compaction->task_id, task_record, 0);
      if (is_first_writer) {
        if (pick_trace) std::cout << "PICK_RESULT pid=" << getpid() << " ts=" << pick_ts << " step=sst-promote taskid=" << compaction->task_id->ShortDebugString() << std::endl;
        has_worked_on_compaction = true;
        return Status::kSuccess;
      }
      // substract the input file size from the level size
      for (int i = 0; i < compaction->task_id->input_files_size(); i++) {
        file_size -= this->latest_view.getFileSize(compaction->task_id->input_files(i));
      }
      delete compaction;
      delete task_record;
      compaction = nullptr;
      task_record = nullptr;
    }
  }

  // Third step: check if there is any compaction to the last level
  files = this->latest_view.getWithPrefix(metadata->sstable_level_prefix + std::to_string(metadata->max_level));
  while (files.size() > metadata->last_file_number_limit) {
    compaction = new Compaction();
    compaction->task_id = new TaskRecord::TaskIdentifier();
    Status init_status = initInputUnitInlastLayer(compaction, files, this->metadata->compaction_input_file_number);
    // initInputUnitInlastLayer returns kFailure when no contiguous run of
    // `limit` same-version files exists in the deque. In that case, no inputs
    // were added to task_id; calling shouldWorkOnTask + doCompactionWork
    // with an empty TaskID would dereference input_files(0) and segfault.
    // Bail out cleanly — the next view-update tick will give us new state.
    if (init_status != Status::kSuccess || compaction->task_id->input_files_size() < 2) {
      delete compaction;
      delete task_record;
      compaction = nullptr;
      task_record = nullptr;
      break;
    }
    compaction->task_id->set_compactintonextlevel(false);
    compaction->task_id->set_destinationlevel(this->metadata->max_level);
    bool is_first_writer = shouldWorkOnTask(compaction->task_id, task_record, 0);
    if (is_first_writer) {
      if (pick_trace) std::cout << "PICK_RESULT pid=" << getpid() << " ts=" << pick_ts << " step=last taskid=" << compaction->task_id->ShortDebugString() << std::endl;
      has_worked_on_compaction = true;
      return Status::kSuccess;
    }
    delete compaction;
    delete task_record;
    compaction = nullptr;
    task_record = nullptr;
  };
  // // Fourth step: check if there is any dead task
  // auto result = this->task_log_handler->getDeadTask();
  // auto dead_task = result.first;
  // int owner_generation = result.second;
  // while (dead_task.IsInitialized()) {
  //   auto* dead_task_id = new TaskRecord::TaskIdentifier(dead_task);
  //   std::cout << getpid() << ":Found dead task " << dead_task_id->ShortDebugString() << std::endl;
  //   bool is_first_writer = shouldWorkOnTask(dead_task_id, task_record, owner_generation + 1);
  //   if (is_first_writer) {
  //     // std::cout << getpid() << " is the first writer for dead task " << dead_task_id->ShortDebugString() << std::endl;
  //     compaction = new Compaction();
  //     compaction->task_id = dead_task_id;
  //     has_worked_on_compaction = true;
  //     return Status::kSuccess;
  //   } else {
  //     delete task_record;
  //     task_record = nullptr;
  //     result = this->task_log_handler->getDeadTask();
  //     dead_task = result.first;
  //     owner_generation = result.second;
  //   }
  // }
  has_worked_on_compaction = false;
  return Status::kSuccess;
}

Status CompactionWatcher::initInputUnit(Compaction* compaction, std::deque<std::string>& units, int limit) {
  int count = 0;
  while (count < limit && !units.empty()) {
    if (units.front() == this->latest_view.getCurrentLogTail()) {
      break;
    }
    compaction->task_id->add_input_files(units.front());
    units.pop_front();
    count++;
  }
  return Status::kSuccess;
}

Status CompactionWatcher::initInputUnitInlastLayer(Compaction* compaction, std::deque<std::string>& units, int limit) {
  int count = 0;
  std::vector<std::string> selected_files;  // Temporary storage for selected files
  int chosen_version = getNumberBeforeUnderscore(units.front());
  int selected_index_start = 0;
  for (int i = 0; i < units.size() && count < limit; i++) {
    if (getNumberBeforeUnderscore(units[i]) == chosen_version) {
      selected_files.push_back(units[i]);
      count++;
    } else {
      selected_files.clear();
      selected_index_start = i;
      chosen_version = getNumberBeforeUnderscore(units[i]);
      count = 1;
      selected_files.push_back(units[i]);
    }
  }
  if (count < limit) {
    return Status::kFailure;
  } else {
    for (auto const& file : selected_files) {
      compaction->task_id->add_input_files(file);
    }
    units.erase(units.begin() + selected_index_start, units.begin() + selected_index_start + limit);
  }
  compaction->compaction_version = chosen_version + 1;
  return Status::kSuccess;
}

Status CompactionWatcher::startCompaction(Compaction* compaction, TaskRecord* task_record) {
  // if (rand() % 10 == 0) {
  //   std::cout << getpid() << ":crashing" << std::endl;
  //   return Status::kFailure;  // crash
  // }
  if (this->mode == Mode::Singleton) {
    return doCompactionWork(compaction);
  }
  std::thread t(&CompactionWatcher::taskHeartbeat, this, compaction, task_record);
  doCompactionWork(compaction);
  t.join();
  return Status::kSuccess;
}

Status CompactionWatcher::doCompactionWork(Compaction* compaction) {
  // Step1: fetch the file in compaction -> input;
  //  If compact into last level, also fetch the file in compaction -> output;
  // Step2: compact it
  bool log_level_compaction = false;
  std::string log_string = "Compacting ";
  std::unordered_map<std::string, Record*> key_record;
  // Emitted at the START of the compaction work — used by the churn-tolerance
  // experiment to observe per-compaction events. The "Compacting ... into:..."
  // message printed at the end of this function arrives too late.
  std::cout << getpid() << ":COMPACT_START sstable="
            << (compaction->task_id->input_files(0).find(this->metadata->log_prefix) == std::string::npos ? 1 : 0)
            << " " << compaction->task_id->ShortDebugString() << std::endl;
  // Experimental abort knob: with probability OZONEDB_CHURN_ABORT_RATE, mark
  // the compaction aborted and bail without writing the metadata-log update
  // or COMPLETE record. The heartbeat thread will see compaction->aborted and
  // also exit without finalising. Survivors then detect the task as dead via
  // the heartbeat-timeout path and reclaim it. Read once at static init.
  static double abort_rate = []{
    char const* s = std::getenv("OZONEDB_CHURN_ABORT_RATE");
    if (!s) return 0.0;
    try { return std::stod(s); } catch (...) { return 0.0; }
  }();
  if (abort_rate > 0.0) {
    double r = rand() / (double)RAND_MAX;
    if (r < abort_rate) {
      compaction->aborted.store(true);
      std::cout << getpid() << ":COMPACT_ABORT " << compaction->task_id->ShortDebugString() << std::endl;
      return Status::kFailure;
    }
  }
  if (compaction->task_id->input_files(0).find(this->metadata->log_prefix) != std::string::npos) {
    log_level_compaction = true;
    if (this->event_listener != nullptr) {
      this->event_listener->onLogCompactionStart();
    }
  } else {
    if (this->event_listener != nullptr) {
      this->event_listener->onSSTableCompactionStart();
    }
  }
  
  for (auto const& input : compaction->task_id->input_files()) {
    // read from log level
    
    log_string += input + " ";
    if (input.find(this->metadata->log_prefix) != std::string::npos) {
      
      std::unordered_map<std::string, Record*> records_tmp;
      unsigned char* buffer = nullptr;
      size_t file_size;
      std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(input);
      std::unique_lock file_lock(file_mutex);
      
      this->storage->read(input, buffer, file_size);
      
      file_lock.unlock();
      std::vector<google::protobuf::Message*> messages;
      protobuf::deserializeMessages(buffer, file_size, messages, []() -> google::protobuf::Message* {
        return new Record();
      });
      delete[] buffer;
      buffer = nullptr;
      for (auto* msg : messages) {
        auto* rec = static_cast<Record*>(msg);
        if (key_record.find(rec->key()) != key_record.end()) {
          delete key_record[rec->key()];
        }
        key_record[rec->key()] = rec;
      }
    } else {
      

      std::unordered_map<std::string, Record*> records_tmp;
      Table* table = nullptr;
      std::shared_mutex& file_mutex = this->file_mutex_manager->getMutexForFile(input);
      std::unique_lock file_lock(file_mutex);
      
      Table::open(this->storage, input, table);
      records_tmp = table->getAll();
      
      file_lock.unlock();
      // print the range of records_tmp
      std::string key_start = records_tmp.begin()->first;
      std::string key_end = records_tmp.begin()->first;
      for (auto const& record : records_tmp) {
        if (record.first < key_start) {
          key_start = record.first;
        }
        if (record.first > key_end) {
          key_end = record.first;
        }
        if (key_record.find(record.first) != key_record.end()) {
          delete key_record[record.first];
        }
        key_record[record.first] = record.second;
      }
      delete table;
    }
  }
  
  std::vector<Record*> records;
  records.reserve(key_record.size());
  for (auto const& [key, record] : key_record) {
    records.push_back(record);
  }

  // Step3: write to the destination file, cut the file by the max file size
  // sort based on the key
  
  log_string += "into";
  std::sort(records.begin(), records.end(), [](Record* a, Record* b) {
    return a->key() < b->key();
  });
  std::vector<std::string> output_files;
  std::vector<std::pair<std::string, std::string>> key_ranges;
  std::pair<std::string, std::string> key_range;
  std::string dest_prefix = this->metadata->sstable_level_prefix + std::to_string(compaction->task_id->destinationlevel());
  if (!this->storage->exist(dest_prefix)) {
    this->storage->createDirectory(dest_prefix);
  }
  for (int i = 0; i < records.size(); i++) {
    // For last-level compactions we always emit a single output file —
    // splitting on size_limit produced a non-terminating cascade with the
    // version-incrementing fix (each (v=k, v=k) → 2 v=k+1 files keeps total
    // file count constant; picker never converges to <= last_file_number_limit).
    bool is_last_level = compaction->task_id->destinationlevel() == this->metadata->max_level;
    if (compaction->outputBuilder == nullptr ||
        (!is_last_level && compaction->outputBuilder->fileSize() > this->metadata->level_file_size_limit[compaction->task_id->destinationlevel() - 1])) {
      if (compaction->outputBuilder != nullptr) {
        compaction->outputBuilder->finish();
        key_range.second = records[i - 1]->key();
        key_ranges.push_back(key_range);
        delete compaction->outputBuilder;
        compaction->outputBuilder = nullptr;
      }
      auto now = std::chrono::system_clock::now();
      auto duration = now.time_since_epoch();
      auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
      std::string sstable_name = this->fingerprint + std::to_string(nanoseconds) + ".sst";
      std::string sstable_path = dest_prefix;
      if (compaction->compaction_version >= 0) {
        sstable_path.append("/").append(std::to_string(compaction->compaction_version)).append("_").append(sstable_name);
      } else
        sstable_path.append("/").append(sstable_name);
      output_files.push_back(sstable_path);
      log_string += ":" + sstable_path;
      compaction->outputBuilder = new TableBuilder(this->storage, sstable_path);
      key_range.first = records[i]->key();
    }
    compaction->outputBuilder->add(records[i]->key(), *records[i]);
  }
  
  if (compaction->outputBuilder != nullptr) {
    compaction->outputBuilder->finish();
    key_range.second = records[records.size() - 1]->key();
    // std::cout << "key range: " << key_range.first << " to " << key_range.second << std::endl;
    key_ranges.push_back(key_range);
    delete compaction->outputBuilder;
    compaction->outputBuilder = nullptr;
  }
  
  OperationRecord operation_record;
  operation_record.set_op_type(OperationRecord::COMPACT);
  for (auto const& input : compaction->task_id->input_files()) {
    operation_record.add_input_files(input);
  }
  for (int i = 0; i < key_ranges.size(); i++) {
    operation_record.add_key_start(key_ranges[i].first);
    operation_record.add_key_end(key_ranges[i].second);
    operation_record.add_output_file(output_files[i]);
  }
  if (compaction->task_id->compactintonextlevel()) {
    operation_record.set_compact_in_last_level(false);
  } else {
    operation_record.set_compact_in_last_level(true);
  }
  this->metadata_handler->appendToMetadataLog(operation_record);
  
  // Step4: delete the input files: append to metadata log first, then delete
  for (auto const& input : compaction->task_id->input_files()) {
    // std::cout << "Deleting " << input << std::endl;
    // this->storage->remove(input);
  }
  compaction->finished = true;
  std::cout << std::this_thread::get_id() << ":" << log_string << std::endl;
  // delete the records
  for (auto const& record : records) {
    delete record;
  }
  if (log_level_compaction) {
    if (this->event_listener != nullptr) {
      int input_size = 0;
      for (auto const& input : compaction->task_id->input_files()) {
        input_size += this->storage->size(input);
      }
      this->event_listener->onLogCompactionCompletion(input_size);
    }

  } else if (this->event_listener != nullptr) {
    int source_level = getSSTLayerNumber(compaction->task_id->input_files(0));
    int input_size = 0;
    for (auto const& input : compaction->task_id->input_files()) {
      input_size += this->latest_view.getFileSize(input);
    }
    this->event_listener->onSSTableCompactionCompletion(input_size, source_level);
  }
  return Status::kSuccess;
}

}  // namespace ozonedb
