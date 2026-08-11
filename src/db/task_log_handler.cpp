#include "task_log_handler.h"
#include "helper.h"
#include "protobuf_serializer.h"
#include <chrono>
#include <iostream>
#include <unistd.h>

namespace ozonedb {
//========================================read and write=======================================
void TaskLogHandler::appendToTaskLog(TaskRecord const& record) {
  int buffer_size;
  unsigned char* buffer = protobuf::serializeMessage(record, buffer_size);
  {
    std::unique_lock<std::shared_mutex> lock(task_log_mutex);
    this->storage->append(this->active_unit, buffer, buffer_size);
  }
  delete[] buffer;
  buffer = nullptr;
  // [COMPACTION_NEEDED]: emitted only on the durable append of a brand-new
  // task. Heartbeats use TASK_IN_PROGRESS, completions use TASK_COMPLETE,
  // and dead-task takeovers use owner_generation > 0 — all excluded.
  // Logged outside the task_log_mutex so std::cerr never extends the
  // critical section that heartbeat threads contend on.
  if (record.status() == TaskRecord::TASK_BEGIN &&
      record.owner_generation() == 0) {
    auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
    std::vector<std::string> inputs(record.task_id().input_files().begin(),
                                    record.task_id().input_files().end());
    std::cerr << "[COMPACTION_NEEDED] task_id="
              << formatTaskId(inputs,
                              record.task_id().destinationlevel(),
                              !record.task_id().compactintonextlevel())
              << " wall_ns=" << wall_ns << " writer_pid=" << getpid()
              << std::endl;
  }
}

Status TaskLogHandler::readTaskLog(std::vector<TaskRecord*>& result) {
  std::unique_lock<std::shared_mutex> lock(task_log_mutex);
  if (!this->storage->exist(this->active_unit)) {
    return Status::kSuccess;
  }
  size_t file_size = this->storage->size(this->active_unit);
  if (file_size == this->offset) {
    return Status::kSuccess;
  }
  size_t size = file_size - this->offset;
  std::vector<google::protobuf::Message*> records;
  unsigned char* buffer = nullptr;
  Status read_status =
      this->storage->read(this->active_unit, buffer, this->offset, size);
  this->offset = file_size;
  lock.unlock();
  if (read_status != Status::kSuccess || buffer == nullptr) {
    delete[] buffer;
    return Status::kFailure;
  }
  if (protobuf::deserializeMessages(buffer, size, records, []() -> google::protobuf::Message* {
        return new TaskRecord();
      }) == Status::kFailure) {
    delete[] buffer;
    return Status::kFailure;
  }
  delete[] buffer;
  buffer = nullptr;
  result.reserve(records.size());
  for (auto const& record : records) {
    result.push_back(static_cast<TaskRecord*>(record));
  }
  return Status::kSuccess;
}

bool TaskLogHandler::worthAppend(TaskRecord const& record) {
  if (record.status() == TaskRecord::TASK_BEGIN &&
      (task_map.find(record.task_id()) == task_map.end() || task_map[record.task_id()].owner_generation < record.owner_generation())) {
    return true;
  }
  return false;
}

void TaskLogHandler::rollforwardSingleTask(TaskRecord const& task_record) {
  std::unique_lock<std::shared_mutex> task_map_lock(task_map_mutex);
  if (task_record.status() == TaskRecord::TASK_BEGIN &&
      (task_map.find(task_record.task_id()) == task_map.end() ||
       task_map[task_record.task_id()].owner_generation < task_record.owner_generation())) {
    task_map[task_record.task_id()].owner = task_record.owner();
    task_map[task_record.task_id()].owner_generation = task_record.owner_generation();
    task_map[task_record.task_id()].status = TaskRecord::TASK_BEGIN;
    task_map[task_record.task_id()].heartbeat = 0;
    std::unique_lock<std::shared_mutex> lock(dead_tasks_mutex);
    dead_tasks.erase(task_record.task_id());
    lock.unlock();
  }
  // update the heartbeat count if the owner is the same
  if (task_map[task_record.task_id()].owner == task_record.owner() && task_map[task_record.task_id()].owner_generation == task_record.owner_generation()) {
    if (task_record.status() == TaskRecord::TASK_IN_PROGRESS) {
      task_map[task_record.task_id()].heartbeat = 0;
    } else if (task_record.status() == TaskRecord::TASK_COMPLETE) {
      task_map[task_record.task_id()].status = TaskRecord::TASK_COMPLETE;
    }
    std::unique_lock<std::shared_mutex> lock(dead_tasks_mutex);
    dead_tasks.erase(task_record.task_id());
    lock.unlock();
  }
}

void TaskLogHandler::rollForwardTaskLog() {
  std::vector<TaskRecord*> records;
  while (readTaskLog(records) != Status::kSuccess) {
    for (auto* record : records) {
      delete record;
    }
    records.clear();
  }
  if (!records.empty()) {
    for (auto const& record : records) {
      rollforwardSingleTask(*record);
    }
  }

  std::unique_lock<std::shared_mutex> task_map_lock(task_map_mutex);
  for (auto it = task_map.begin(); it != task_map.end(); it++) {
    if (it->second.heartbeat >= task_heartbeat_threshold && it->second.status != TaskRecord::TASK_COMPLETE && it->second.status != TaskRecord::TASK_DEAD) {
      // std::cout << getpid() << ":Task " << it->first.DebugString() << " is dead" << std::endl;
      std::unique_lock<std::shared_mutex> lock(dead_tasks_mutex);
      dead_tasks[it->first] = it->second.owner_generation;
      lock.unlock();
      it->second.status = TaskRecord::TASK_DEAD;
    }
  }

  // delete records
  for (auto* record : records) {
    delete record;
  }
}

bool TaskLogHandler::isFirstWriterForTask(TaskRecord::TaskIdentifier* task_id, std::string const& client, int owner_generation) {
  std::unique_lock<std::shared_mutex> task_map_lock(task_map_mutex);
  if (task_map.find(*task_id) == task_map.end()) {
    return false;
  }
  if (task_map[*task_id].owner == client && task_map[*task_id].status == TaskRecord::TASK_BEGIN && task_map[*task_id].owner_generation == owner_generation) {
    task_map[*task_id].status = TaskRecord::TASK_IN_PROGRESS;
    return true;
  }
  return false;
}

std::pair<TaskRecord::TaskIdentifier, int> TaskLogHandler::getDeadTask() {
  std::unique_lock<std::shared_mutex> dead_tasks_lock(dead_tasks_mutex);
  if (dead_tasks.empty()) {
    return std::pair(TaskRecord::TaskIdentifier(), -1);
  }
  TaskRecord::TaskIdentifier task_id = dead_tasks.begin()->first;
  int owner_generation = dead_tasks.begin()->second;
  dead_tasks.erase(dead_tasks.begin());
  return std::pair(task_id, owner_generation);
}

// =======================================[heartbeats]=======================================
void TaskLogHandler::updateHeartbeats() {
  while (!stop_thread) {
    std::unique_lock<std::shared_mutex> task_map_lock(task_map_mutex);
    for (auto& task : task_map) {
      task.second.heartbeat++;
    }
    task_map_lock.unlock();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void TaskLogHandler::startHeartbeatThread() {
  this->stop_thread = false;
  this->heartbeat_thread = std::thread(&TaskLogHandler::updateHeartbeats, this);
}

void TaskLogHandler::stopHeartbeatThread() {
  this->stop_thread = true;
  if (this->heartbeat_thread.joinable()) {
    this->heartbeat_thread.join();
  }
}

}  // namespace ozonedb