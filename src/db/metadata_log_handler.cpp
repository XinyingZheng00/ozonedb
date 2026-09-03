#include "metadata_log_handler.h"
#include "db.h"
#include "helper.h"
#include <algorithm>
#include <climits>

namespace ozonedb {

// get file number in the view
int View::getFileNumber(std::string const& prefix) {
  return storage_layout[prefix].size();
}

std::deque<std::string> View::getWithPrefix(std::string const& prefix) {
  return storage_layout[prefix];
}

std::pair<std::string, std::string> View::getKeyRange(std::string const& file_name) {
  return key_range[file_name];
}

size_t View::getFileSize(std::string const& file_name) {
  return file_size[file_name];
}

// Serialize a FileRecord to a file
void MetadataLogHandler::appendToMetadataLog(OperationRecord const& record) {
  int buffer_size;
  unsigned char* buffer = protobuf::serializeMessage(record, buffer_size);
  std::unique_lock<std::shared_mutex> lock(read_mutex);
  this->storage->append(this->active_unit, buffer, buffer_size);
  delete[] buffer;
  buffer = nullptr;
}

Status MetadataLogHandler::startViewUpdate(std::atomic<bool> const* active) {
  this->update_view_thread = new std::thread(&MetadataLogHandler::rollForwardMetadataLogPeriodically, this, active);
  return Status::kSuccess;
}

Status MetadataLogHandler::stopViewUpdate() {
  // EXPERIMENT (not upstream): detach instead of join, so closeDB() doesn't
  // block on the view-update thread's up-to-100ms poll interval
  // (rollForwardMetadataLogPeriodically's sleep_for). Safe today because
  // DB::closeDB() never actually frees `db` (`delete db;` is commented
  // out there), so the detached thread keeps touching valid, merely-leaked
  // memory until it notices `*active == false` on its own. See conversation
  // notes -- this is coupled to that leak and would need revisiting if the
  // leak is ever fixed.
  this->update_view_thread->detach();
  delete this->update_view_thread;
  this->update_view_thread = nullptr;
  return Status::kSuccess;
}

// get latest view
void MetadataLogHandler::getLatestView(View& view) {
  std::shared_lock<std::shared_mutex> lock(view_mutex);
  view = latest_view;
}

void MetadataLogHandler::getLatestScore(double& score) {
  // For sst levels, the score is total size of the level divided by the target
  // size. for log level, the score is the total number of files, divided by the
  // target number of files.
  std::shared_lock<std::shared_mutex> lock(view_mutex);
  for (auto const& entry : this->latest_view.storage_layout) {
    if (entry.first.find("sstable") != std::string::npos) {
      size_t total_size = 0;
      for (auto const& file : entry.second) {
        total_size += this->latest_view.file_size[file];
      }
      double level_score =
          total_size * 1.0 /
          this->metadata->level_size.at(getNumberInTheEnd(entry.first) - 1);
      if (level_score > 1) {
        score += level_score;
      }
    } else {
      double level_score =
          entry.second.size() * 1.0 / this->metadata->base_file_number_limit;
      if (level_score > 1) {
        score += level_score;
      }
    }
  }
}

// double MetadataLogHandler::getLevelScore(std::string level_prefix) {
//   if (level_prefix.find("log") != std::string::npos) {
//     return latest_view.storage_layout[level_prefix].size() * 1.0 / 1;
//   } else if (getNumberInTheEnd(level_prefix) != this->metadata->max_level) {
//     size_t total_size = 0;
//     for (auto const& file : latest_view.storage_layout[level_prefix]) {
//       total_size += latest_view.file_size[file];
//     }
//     return total_size * 1.0 / this->metadata->level_size.at(getNumberInTheEnd(level_prefix) - 1);
//   } else {
//     return latest_view.storage_layout[level_prefix].size() * 1.0 / this->metadata->last_file_number_limit;
//   }
// }

// Deserialize FileRecords from a file
std::vector<OperationRecord*> MetadataLogHandler::readMetadataLog() {
  std::unique_lock<std::shared_mutex> lock(read_mutex);
  if (!this->storage->exist(this->active_unit)) {
    return {};
  }
  size_t file_size = this->storage->size(this->active_unit);
  if (file_size == this->offset) {
    return {};
  }
  std::vector<google::protobuf::Message*> records;
  unsigned char* buffer = nullptr;
  this->storage->read(this->active_unit, buffer, this->offset, file_size - this->offset);
  if (protobuf::deserializeMessages(buffer, file_size - this->offset, records, []() -> google::protobuf::Message* {
        return new OperationRecord();
      }) == Status::kFailure) {
    return {};
  }
  delete[] buffer;
  buffer = nullptr;
  this->offset = file_size;
  std::vector<OperationRecord*> result;
  result.reserve(records.size());
  for (auto const& record : records) {
    result.push_back(static_cast<OperationRecord*>(record));
  }
  return result;
}

std::string MetadataLogHandler::rollforwardSingleOperationRecord(OperationRecord* record) {
  if (record->op_type() == OperationRecord::LOGCREATE) {
    std::string const& input_file = record->input_files()[0];
    std::string const& output_file = record->output_file()[0];
    std::string prefix = getPrefix(output_file);
    std::deque<std::string>& level_layout = this->latest_view.storage_layout[prefix];
    if (this->latest_view.current_log_tail.empty() || this->latest_view.current_log_tail == input_file) {
      level_layout.push_back(output_file);
      this->latest_view.file_size[input_file] = this->storage->size(input_file);
      this->latest_view.current_log_tail = output_file;
      // this->latest_view.tail_size = this->storage->size(output_file);
      // this->latest_view.file_size[output_file] = this->latest_view.tail_size; => update at outside
      delete record;
      return "tail" + prefix;
    }
    if (getNumberInTheEnd(this->latest_view.current_log_tail) < getNumberInTheEnd(input_file)) {
      this->buffer["tail" + prefix].push({getNumberInTheEnd(input_file), record});
      return "buffered";
    }
    delete record;
    return "";
  } else if (record->op_type() == OperationRecord::COMPACT && !record->compact_in_last_level()) {
    std::string const& input_file = record->input_files()[0];
    std::string input_prefix = getPrefix(input_file);
    std::deque<std::string>& level_layout = this->latest_view.storage_layout[input_prefix];
    if (level_layout.front() != input_file) {
      auto it = std::find(level_layout.begin(), level_layout.end(), input_file);
      int index = std::distance(level_layout.begin(), it);
      this->buffer[input_prefix].push({index, record});
      return "buffered";
    }
    for (auto const& input_file : record->input_files()) {
      // The buffer rule above guarantees input[0] is at the front of L1, and
      // the picker (initInputUnit) always took inputs as a consecutive run
      // from the front, so each pop_front removes the next expected input.
      latest_view.storage_layout[input_prefix].pop_front();
      if (latest_view.key_range.find(input_file) != latest_view.key_range.end()) {
        latest_view.key_range.erase(input_file);
      }
      if (latest_view.file_size.find(input_file) != latest_view.file_size.end()) {
        latest_view.file_size.erase(input_file);
      }
      this->tail_cache->addTailChange(input_file, record->output_file(0));
    }
    for (int i = 0; i < record->output_file_size(); i++) {
      std::string const& output_file = record->output_file(i);
      std::string output_prefix = getPrefix(output_file);
      latest_view.storage_layout[output_prefix].push_back(output_file);
      latest_view.key_range[output_file] = std::make_pair(record->key_start(i), record->key_end(i));
      latest_view.file_size[output_file] = this->storage->size(output_file);
    }
    delete record;
    return input_prefix;
  } else if (record->op_type() == OperationRecord::COMPACT && record->compact_in_last_level()) {
    std::string const& input_file = record->input_files()[0];
    std::string input_prefix = getPrefix(input_file);
    std::deque<std::string>& level_layout = this->latest_view.storage_layout[input_prefix];

    // Canonical-replay buffer for last-level COMPACT. The picker
    // (initInputUnitInlastLayer) always picks the first run of `limit`
    // same-version files starting from the front of the deque, so the OLDEST
    // commit within a version has its first input at the front of that
    // version's region. To make replay deterministic across writers, defer
    // any commit whose first input isn't at the front of its version region —
    // it'll drain back when the records before it (in canonical order) have
    // applied. Without this, two writer processes at different replay offsets
    // can apply records in different orders, produce non-canonical deque
    // shapes, and pick divergent (overlapping) input sets in subsequent rounds.
    int input_version = getNumberBeforeUnderscore(input_file);
    size_t version_start = 0;
    while (version_start < level_layout.size() &&
           getNumberBeforeUnderscore(level_layout[version_start]) > input_version) {
      ++version_start;
    }
    if (version_start >= level_layout.size() ||
        level_layout[version_start] != input_file) {
      auto it = std::find(level_layout.begin(), level_layout.end(), input_file);
      int index = (it == level_layout.end())
                      ? INT_MAX
                      : static_cast<int>(std::distance(level_layout.begin(), it));
      this->buffer[input_prefix].push({index, record});
      return "buffered";
    }

    int index = static_cast<int>(version_start);
    for (auto const& input_f : record->input_files()) {
      // Inputs are at consecutive positions starting at `index`. After each
      // erase, the next input shifts to position `index`. The canonical-replay
      // invariant guarantees the file is present.
      level_layout.erase(level_layout.begin() + index);
      if (latest_view.key_range.find(input_f) != latest_view.key_range.end()) {
        latest_view.key_range.erase(input_f);
      }
      if (latest_view.file_size.find(input_f) != latest_view.file_size.end()) {
        latest_view.file_size.erase(input_f);
      }
      this->tail_cache->addTailChange(input_f, record->output_file(0));
    }
    for (int i = 0; i < record->output_file_size(); i++) {
      std::string const& output_file = record->output_file(i);
      int insert_at = std::min(index + i, static_cast<int>(level_layout.size()));
      level_layout.insert(level_layout.begin() + insert_at, output_file);
      latest_view.key_range[output_file] = std::make_pair(record->key_start(i), record->key_end(i));
      latest_view.file_size[output_file] = this->storage->size(output_file);
    }
    delete record;
    return input_prefix;
  }
  return "";
}

// Roll forward the metadata log to get the latest view
void MetadataLogHandler::rollForwardMetadataLogPeriodically(std::atomic<bool> const* active) {
  while (*active) {
    std::vector<OperationRecord*> records = readMetadataLog();
    if (records.empty()) {
      std::unique_lock<std::shared_mutex> lock(view_mutex);
      if (!this->latest_view.current_log_tail.empty()) {
        this->latest_view.tail_size = this->storage->size(this->latest_view.current_log_tail);
        this->latest_view.file_size[this->latest_view.current_log_tail] = this->latest_view.tail_size;
        if (this->event_listener != nullptr)
          this->event_listener->onViewUpdate();
      }
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    std::unique_lock<std::shared_mutex> lock(view_mutex);
    for (auto const& record : records) {
      std::string modified_layer = rollforwardSingleOperationRecord(record);
      if (modified_layer != "buffered") drainBuffer(modified_layer);
    }
    if (!this->latest_view.current_log_tail.empty()) {
      this->latest_view.tail_size = this->storage->size(this->latest_view.current_log_tail);
      this->latest_view.file_size[this->latest_view.current_log_tail] = this->latest_view.tail_size;
    }
    lock.unlock();
    if (this->event_listener != nullptr)
      this->event_listener->onViewUpdate();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

// Roll forward the metadata log to get the latest view
View MetadataLogHandler::rollForwardMetadataLog() {
  std::vector<OperationRecord*> records = readMetadataLog();
  if (records.empty()) {
    std::unique_lock<std::shared_mutex> lock(view_mutex);
    if (!this->latest_view.current_log_tail.empty()) {
      this->latest_view.tail_size = this->storage->size(this->latest_view.current_log_tail);
      this->latest_view.file_size[this->latest_view.current_log_tail] = this->latest_view.tail_size;
    }
    return latest_view;
  }
  std::unique_lock<std::shared_mutex> lock(view_mutex);
  for (auto const& record : records) {
    std::string modified_layer = rollforwardSingleOperationRecord(record);
    if (modified_layer != "buffered") drainBuffer(modified_layer);
  }
  if (!this->latest_view.current_log_tail.empty()) {
    this->latest_view.tail_size = this->storage->size(this->latest_view.current_log_tail);
    this->latest_view.file_size[this->latest_view.current_log_tail] = this->latest_view.tail_size;
  }
  return latest_view;
}
void MetadataLogHandler::drainBuffer(std::string const& layer) {
  while (true) {
    auto& buf = this->buffer[layer];
    if (buf.empty()) return;
    std::vector<OperationRecord*> attempts;
    attempts.reserve(buf.size());
    while (!buf.empty()) {
      attempts.push_back(buf.top().second);
      buf.pop();
    }
    bool any_applied = false;
    for (auto* op : attempts) {
      std::string r = rollforwardSingleOperationRecord(op);
      if (r != "buffered") any_applied = true;
    }
    if (!any_applied) return;
  }
}

void MetadataLogHandler::initSSTMetadata() {
  // this is invoked only when the database starts so no need to lock
  for (auto const& entry : this->latest_view.storage_layout) {
    if (entry.first.find("sstable") == std::string::npos) {
      continue;
    } else {
      for (auto it = entry.second.cbegin(); it != entry.second.cend(); it++) {
        Table* table = nullptr;
        this->lru_cache->getSSTable(*it, table);
      }
    }
  }
}
}  // namespace ozonedb
