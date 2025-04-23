#include "log_handler_shared_log.h"
#include "protobuf_serializer.h"
namespace ozonedb {

log_handler_shared_log::log_handler_shared_log(int client_id) {
  shared_log_storage = std::make_unique<SharedLogStorage>(client_id);
}

Status log_handler_shared_log::addRecord(Record const& record) {
  std::string data = record.SerializeAsString();
  ;
  return shared_log_storage->append(data);
}

Status log_handler_shared_log::readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset) {
  std::vector<std::string> entries;
  size_t from = std::stoul(offset);
  size_t to = shared_log_storage->size();
  if (from > to) {
    return Status::kFailure;
  }
  if (from == to) {
    return Status::kNotFound;
  }
  shared_log_storage->read(entries, from, to);
  if (entries.empty()) {
    return Status::kFailure;
  }
  std::unordered_map<std::string, Record*> records_map;
  for (auto& entry : entries) {
    auto* record_tmp = new Record();
    (*record_tmp).ParseFromString(entry);
    records_map[record_tmp->key()] = record_tmp;
  }

  auto it = records_map.find(key);
  if (it == records_map.end()) {
    return Status::kNotFound;
  }
  record = it->second;
  latest_offset = std::to_string(to);
  return Status::kSuccess;
}
}  // namespace ozonedb