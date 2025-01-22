#ifndef TASK_LOG_HANDLER_H
#define TASK_LOG_HANDLER_H
#include "protobuf/record.pb.h"
#include "storage.h"
#include <cstring>
#include <thread>
#include <unordered_map>

/*
In the very basic case, we can proof, the compaction is performed at most f times
with (f-1) failure in the system.
*/

namespace ozonedb {

// Define a custom hash function for TaskIdentifier
struct TaskIdentifierHash {
  std::size_t operator()(TaskRecord::TaskIdentifier const& task_id) const {
    std::size_t seed = 0;
    for (auto const& file : task_id.input_files()) {
      seed ^= std::hash<std::string>{}(file) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

// Define equality operator for TaskIdentifier
struct TaskIdentifierEqual {
  bool operator()(TaskRecord::TaskIdentifier const& lhs, TaskRecord::TaskIdentifier const& rhs) const {
    // Compare the repeated fields of input_files
    if (lhs.input_files_size() != rhs.input_files_size()) {
      return false;  // Different sizes mean they are not equal
    }

    for (int i = 0; i < lhs.input_files_size(); ++i) {
      if (lhs.input_files(i) != rhs.input_files(i)) {
        return false;  // Found a mismatch in elements
      }
    }
    return true;  // All elements matched
  }
};

class TaskLogHandler {
  struct TaskStatus {
    std::string owner;
    int heartbeat;
    int owner_generation;
    TaskRecord::TaskStatus status;
  };

 private:
  uint32_t offset;
  std::string active_unit;
  Storage* storage = nullptr;

  std::unordered_map<TaskRecord::TaskIdentifier, TaskStatus, TaskIdentifierHash, TaskIdentifierEqual> task_map;
  std::shared_mutex task_map_mutex;

  std::unordered_map<TaskRecord::TaskIdentifier, int, TaskIdentifierHash, TaskIdentifierEqual> dead_tasks;  // the value here is used to store the owner generation
  int task_heartbeat_threshold = 10000000;
  std::shared_mutex dead_tasks_mutex;
  std::shared_mutex task_log_mutex;

  Status readTaskLog(std::vector<TaskRecord*>& result);
  void rollforwardSingleTask(TaskRecord const& task_record);

  void updateHeartbeats();
  void startHeartbeatThread();
  void stopHeartbeatThread();
  std::thread heartbeat_thread;
  bool stop_thread = false;

 public:
  TaskLogHandler(std::string const& task_log, Storage* storage) {
    this->offset = 0;
    this->storage = storage;
    this->active_unit = task_log;
    startHeartbeatThread();
  };

  ~TaskLogHandler() {
    stopHeartbeatThread();
  };

  void rollForwardTaskLog();

  bool isFirstWriterForTask(TaskRecord::TaskIdentifier* task_id, std::string const& client, int owner_generation);
  bool worthAppend(TaskRecord const& record);

  void appendToTaskLog(TaskRecord const& record);

  std::pair<TaskRecord::TaskIdentifier, int> getDeadTask();
};
}  // namespace ozonedb
#endif

/*
 todo:
 . optimize code to ramdomly select a layer to detect the compaction task.
 . consider other cases.
 */