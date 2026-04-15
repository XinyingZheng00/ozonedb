#include "metadata_log_handler.h"
#include "db.h"
#include "helper.h"

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
  // No lock: active_unit is set once in the constructor and not mutated;
  // the storage layer (e.g. CorfuDBStorage::append) handles its own
  // concurrency and returns a globally ordered address from Corfu. Holding
  // read_mutex here used to block every concurrent readMetadataLog() on
  // peer writers across the Corfu round-trip.
  this->storage->append(this->active_unit, buffer, buffer_size);
  delete[] buffer;
  buffer = nullptr;
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

// Deserialize FileRecords from a file.
//
// read_mutex used to be held exclusively across storage->size() +
// storage->read(), which for Corfu fences the tailer and can stall for
// seconds under concurrent writer load. We now snapshot the offset,
// do the I/O lock-free, then take the lock only long enough to commit
// the new offset — and only if nobody else advanced it in the meantime.
// Loser of a race discards its records and the caller catches up on
// the next tick.
std::vector<OperationRecord*> MetadataLogHandler::readMetadataLog() {
  size_t local_offset;
  {
    std::unique_lock<std::shared_mutex> lock(read_mutex);
    local_offset = this->offset;
  }
  if (!this->storage->exist(this->active_unit)) {
    return {};
  }
  size_t file_size = this->storage->size(this->active_unit);
  if (file_size == local_offset) {
    return {};
  }
  std::vector<google::protobuf::Message*> records;
  unsigned char* buffer = nullptr;
  this->storage->read(this->active_unit, buffer, local_offset, file_size - local_offset);
  if (protobuf::deserializeMessages(buffer, file_size - local_offset, records, []() -> google::protobuf::Message* {
        return new OperationRecord();
      }) == Status::kFailure) {
    delete[] buffer;
    return {};
  }
  delete[] buffer;
  buffer = nullptr;
  {
    std::unique_lock<std::shared_mutex> lock(read_mutex);
    if (this->offset != local_offset) {
      // Another thread advanced the offset while we were doing I/O.
      // Our records either overlap theirs or have already been applied;
      // drop them and let the caller catch up next tick.
      for (auto const& r : records) {
        delete static_cast<OperationRecord*>(r);
      }
      return {};
    }
    this->offset = file_size;
  }
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
      // Sealed tail size is shipped on the record by the rolling writer
      // (see log_handler.cpp:newTail). Avoids a fenced storage->size()
      // call under view_mutex. Older records without the field fall
      // back to 0 — heuristic-only, feeds getLatestScore().
      this->latest_view.file_size[input_file] =
          record->has_sealed_input_bytes() ? record->sealed_input_bytes() : 0;
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
      // remove input file from latest_view
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
      // Size is carried on the record by the compactor (see compaction.cpp);
      // old records without output_bytes fall back to 0 — this field only
      // feeds getLatestScore() heuristics, no correctness impact.
      latest_view.file_size[output_file] =
          (i < record->output_bytes_size()) ? record->output_bytes(i) : 0;
    }
    delete record;
    return input_prefix;
  } else if (record->op_type() == OperationRecord::COMPACT && record->compact_in_last_level()) {
    std::string const& input_file = record->input_files()[0];
    std::string input_prefix = getPrefix(input_file);
    std::deque<std::string>& level_layout = this->latest_view.storage_layout[input_prefix];
    std::vector<std::string> keys;
    auto it = std::find(level_layout.begin(), level_layout.end(), input_file);
    int index = std::distance(level_layout.begin(), it);
    for (auto const& input_file : record->input_files()) {
      // remove input file from latest_view
      //find the index of the input file
      auto it = std::find(level_layout.begin(), level_layout.end(), input_file);
      level_layout.erase(it);

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
      //place the output file in the last level in the index position
      level_layout.insert(level_layout.begin() + index + i, output_file);
      latest_view.key_range[output_file] = std::make_pair(record->key_start(i), record->key_end(i));
      latest_view.file_size[output_file] =
          (i < record->output_bytes_size()) ? record->output_bytes(i) : 0;
    }
    delete record;
    return "";
  }
  return "";
}

// Roll forward the metadata log to get the latest view.
//
// The periodic tail-size refresh used to call storage->size() while holding
// unique_lock<view_mutex>, which stalls all foreground reads/writes during
// Corfu tailer catch-up (matches the multi-second plateaus observed under
// concurrent writers). We now sample the tail name under a brief shared_lock,
// call storage->size() with no lock held, then apply the result under a
// brief unique_lock — but only if the tail hasn't rotated in the meantime.
void MetadataLogHandler::rollForwardMetadataLogPeriodically(std::atomic<bool> const* active) {
  while (*active) {
    std::vector<OperationRecord*> records = readMetadataLog();
    if (records.empty()) {
      refreshTailSizeUnlocked();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    {
      std::unique_lock<std::shared_mutex> lock(view_mutex);
      for (auto const& record : records) {
        std::string modified_layer = rollforwardSingleOperationRecord(record);
        while (modified_layer != "buffered" && !this->buffer[modified_layer].empty()) {
          OperationRecord* op_record = this->buffer[modified_layer].top().second;
          this->buffer[modified_layer].pop();
          modified_layer = rollforwardSingleOperationRecord(op_record);
        }
      }
    }
    refreshTailSizeUnlocked();
    if (this->event_listener != nullptr)
      this->event_listener->onViewUpdate();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

// Refresh latest_view.tail_size / file_size[current_log_tail] without
// holding view_mutex exclusively across the storage->size() call. The
// size query can fence the Corfu tailer for seconds under load; keeping
// it out of the critical section lets foreground ops proceed.
void MetadataLogHandler::refreshTailSizeUnlocked() {
  std::string tail_name;
  {
    std::shared_lock<std::shared_mutex> lock(view_mutex);
    tail_name = this->latest_view.current_log_tail;
  }
  if (tail_name.empty()) return;
  size_t tail_size = this->storage->size(tail_name);
  std::unique_lock<std::shared_mutex> lock(view_mutex);
  // Only apply if the tail hasn't rotated since we sampled.
  if (tail_name == this->latest_view.current_log_tail) {
    this->latest_view.tail_size = tail_size;
    this->latest_view.file_size[tail_name] = tail_size;
  }
}

// Roll forward the metadata log to get the latest view. Called from
// foreground log-roll / put paths. Same invariant as the periodic
// variant: keep storage->size() out of unique_lock<view_mutex>.
View MetadataLogHandler::rollForwardMetadataLog() {
  std::vector<OperationRecord*> records = readMetadataLog();
  if (!records.empty()) {
    std::unique_lock<std::shared_mutex> lock(view_mutex);
    for (auto const& record : records) {
      std::string modified_layer = rollforwardSingleOperationRecord(record);
      while (modified_layer != "buffered" && !this->buffer[modified_layer].empty()) {
        OperationRecord* op_record = this->buffer[modified_layer].top().second;
        this->buffer[modified_layer].pop();
        modified_layer = rollforwardSingleOperationRecord(op_record);
      }
    }
  }
  refreshTailSizeUnlocked();
  std::shared_lock<std::shared_mutex> lock(view_mutex);
  return latest_view;
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
