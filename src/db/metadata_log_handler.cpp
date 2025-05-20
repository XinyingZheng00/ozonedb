#include "metadata_log_handler.h"
#include "event_listener.h"
#include "helper.h"

namespace ozonedb {

// Serialize a FileRecord to a file
void MetadataLogHandler::appendToMetadataLog(OperationRecord const& record) {
  size_t size;
  this->metadata_log_storage->append(this->active_unit, record, size);
}

Status MetadataLogHandler::startViewUpdate(std::atomic<bool> const* active) {
  this->update_view_thread = new std::thread(&MetadataLogHandler::rollForwardMetadataLogPeriodically, this, active);
  return Status::kSuccess;
}

Status MetadataLogHandler::stopViewUpdate() {
  this->update_view_thread->join();
  delete this->update_view_thread;
  this->update_view_thread = nullptr;
  return Status::kSuccess;
}

void MetadataLogHandler::getLatestView(View& view) {
  std::shared_lock<std::shared_mutex> lock(view_mutex_r);
  view = latest_view_r;
}

void MetadataLogHandler::flushLatestView() {
  // flush latest_view_w to the latest_view_r
  std::unique_lock<std::shared_mutex> lock_r(view_mutex_r);
  latest_view_r = latest_view_w;
  lock_r.unlock();
}

// void MetadataLogHandler::getLatestScore(double& score) {
//   // For sst levels, the score is total size of the level divided by the target
//   // size. for log level, the score is the total number of files, divided by the
//   // target number of files.
//   std::shared_lock<std::shared_mutex> lock(view_mutex_r);
//   for (auto const& entry : this->latest_view_r.storage_layout) {
//     if (entry.first.find("sstable") != std::string::npos) {
//       size_t total_size = 0;
//       for (auto const& file : entry.second) {
//         total_size += this->latest_view_r.file_size[file];
//       }
//       double level_score =
//           total_size * 1.0 /
//           this->metadata->level_size.at(getNumberInTheEnd(entry.first) - 1);
//       if (level_score > 1) {
//         score += level_score;
//       }
//     } else {
//       double level_score =
//           entry.second.size() * 1.0 / this->metadata->log_file_number_limit;
//       if (level_score > 1) {
//         score += level_score;
//       }
//     }
//   }
// }

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
  size_t log_size = this->metadata_log_storage->size(this->active_unit);
  if (log_size == this->offset) {
    return {};
  }
  std::vector<google::protobuf::Message*> messages;
  this->metadata_log_storage->read(
      this->active_unit, this->offset, log_size, []() -> google::protobuf::Message* {
        return new OperationRecord();
      },
      messages);
  std::vector<OperationRecord*> records_vec;
  for (auto& message : messages) {
    auto* record = static_cast<OperationRecord*>(message);
    records_vec.push_back(record);
  }
  this->offset = log_size;
  return records_vec;
}

std::string MetadataLogHandler::rollforwardSingleOperationRecord(OperationRecord* record) {
  if (record->op_type() == OperationRecord::LOGCREATE) {  // there will be no logcreate record in the shared log
    std::string const& input_file = record->input_files()[0];
    std::string const& output_file = record->output_file()[0];
    std::string prefix = getPrefix(output_file);
    std::deque<std::string>& level_layout = this->latest_view_w.storage_layout[prefix];
    if (this->latest_view_w.current_log_tail.empty() || this->latest_view_w.current_log_tail == input_file) {
      level_layout.push_back(output_file);
      if (input_file != "") {
        this->latest_view_w.file_size[input_file] = this->log_storage->size(input_file);
      }
      this->latest_view_w.current_log_tail = output_file;
      delete record;
      return "tail" + prefix;
    }
    if (getNumberInTheEnd(this->latest_view_w.current_log_tail) < getNumberInTheEnd(input_file)) {
      this->buffer["tail" + prefix].push({getNumberInTheEnd(input_file), record});
      return "buffered";
    }
    delete record;
    return "";
  } else if (record->op_type() == OperationRecord::COMPACT && !record->compact_in_last_level()) {
    std::string const& input_file = record->input_files()[0];
    std::string input_prefix = getPrefix(input_file);
    std::deque<std::string>& level_layout = this->latest_view_w.storage_layout[input_prefix];
    if (level_layout.front() != input_file) {
      auto it = std::find(level_layout.begin(), level_layout.end(), input_file);
      int index = std::distance(level_layout.begin(), it);
      this->buffer[input_prefix].push({index, record});
      return "buffered";
    }
    for (auto const& input_file : record->input_files()) {
      // remove input file from latest_view_w
      latest_view_w.storage_layout[input_prefix].pop_front();
      if (latest_view_w.key_range.find(input_file) != latest_view_w.key_range.end()) {
        latest_view_w.key_range.erase(input_file);
      }
      if (latest_view_w.file_size.find(input_file) != latest_view_w.file_size.end()) {
        latest_view_w.file_size.erase(input_file);
      }
      this->tail_cache->addTailChange(input_file, record->output_file(0));
    }
    for (int i = 0; i < record->output_file_size(); i++) {
      std::string const& output_file = record->output_file(i);
      std::string output_prefix = getPrefix(output_file);
      latest_view_w.storage_layout[output_prefix].push_back(output_file);
      latest_view_w.key_range[output_file] = std::make_pair(record->key_start(i), record->key_end(i));
      latest_view_w.file_size[output_file] = this->sst_storage->size(output_file);
    }
    delete record;
    return input_prefix;
  } else if (record->op_type() == OperationRecord::COMPACT && record->compact_in_last_level()) {
    std::string const& input_file = record->input_files()[0];
    std::string input_prefix = getPrefix(input_file);
    std::deque<std::string>& level_layout = this->latest_view_w.storage_layout[input_prefix];
    std::vector<std::string> keys;
    auto it = std::find(level_layout.begin(), level_layout.end(), input_file);
    int index = std::distance(level_layout.begin(), it);
    for (auto const& input_file : record->input_files()) {
      // remove input file from latest_view_w
      // find the index of the input file
      auto it = std::find(level_layout.begin(), level_layout.end(), input_file);
      level_layout.erase(it);

      if (latest_view_w.key_range.find(input_file) != latest_view_w.key_range.end()) {
        latest_view_w.key_range.erase(input_file);
      }
      if (latest_view_w.file_size.find(input_file) != latest_view_w.file_size.end()) {
        latest_view_w.file_size.erase(input_file);
      }
      this->tail_cache->addTailChange(input_file, record->output_file(0));
    }
    for (int i = 0; i < record->output_file_size(); i++) {
      std::string const& output_file = record->output_file(i);
      std::string output_prefix = getPrefix(output_file);
      // place the output file in the last level in the index position
      level_layout.insert(level_layout.begin() + index + i, output_file);
      latest_view_w.key_range[output_file] = std::make_pair(record->key_start(i), record->key_end(i));
      latest_view_w.file_size[output_file] = this->sst_storage->size(output_file);
    }
    delete record;
    return "";
  }
  return "";
}

void MetadataLogHandler::rollForwardMetadataLogPeriodically(std::atomic<bool> const* active) {
  while (*active) {
    std::vector<OperationRecord*> records = readMetadataLog();
    if (records.empty()) {
      std::unique_lock<std::shared_mutex> lock(view_mutex_w);
      log_tail_updater->updateViewTail(latest_view_w);
      flushLatestView();
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    std::unique_lock<std::shared_mutex> lock(view_mutex_w);
    for (auto const& record : records) {
      std::string modified_layer = rollforwardSingleOperationRecord(record);
      while (modified_layer != "buffered" && !this->buffer[modified_layer].empty()) {
        OperationRecord* op_record = this->buffer[modified_layer].top().second;
        this->buffer[modified_layer].pop();
        modified_layer = rollforwardSingleOperationRecord(op_record);
      }
    }
    log_tail_updater->updateViewTail(latest_view_w);
    flushLatestView();
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
    std::unique_lock<std::shared_mutex> lock(view_mutex_w);
    log_tail_updater->updateViewTail(latest_view_w);
    flushLatestView();
    return latest_view_w;
  }
  std::unique_lock<std::shared_mutex> lock(view_mutex_w);
  for (auto const& record : records) {
    std::string modified_layer = rollforwardSingleOperationRecord(record);
    while (modified_layer != "buffered" && !this->buffer[modified_layer].empty()) {
      OperationRecord* op_record = this->buffer[modified_layer].top().second;
      this->buffer[modified_layer].pop();
      modified_layer = rollforwardSingleOperationRecord(op_record);
    }
  }
  log_tail_updater->updateViewTail(latest_view_w);
  flushLatestView();
  return latest_view_w;
}
void MetadataLogHandler::initSSTMetadata() {
  // this is invoked only when the database starts so no need to lock
  for (auto const& entry : this->latest_view_r.storage_layout) {
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
