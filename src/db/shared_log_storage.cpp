#include "storage.h"
#include <stdexcept>
#include <string>
#include <vector>

SharedLogStorage::SharedLogStorage(const std::string& storage_path)
    : Storage(storage_path) {}

SharedLogStorage::~SharedLogStorage() = default;

void SharedLogStorage::createDirectory(std::string name) {
  throw std::runtime_error("createDirectory() is not supported for SharedLogStorage");
}

Status SharedLogStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  if (fileName.contains("metadatalog")) {
    std::string data_str(reinterpret_cast<const char*>(data), length);
    shared_log.AppendEntryAll(data_str);
  } else if (fileName.contains("tasklog")) {
    std::string data_str(reinterpret_cast<const char*>(data), length);
    shared_log.AppendEntryAll(data_str);
  } else if (fileName.contains("log")) {
    std::string data_str(reinterpret_cast<const char*>(data), length);
    shared_log.AppendEntryAll(data_str);
  } else if(fileName.contains("sstable")) {
    //write to nfs at storage_path
    std::string data_str(reinterpret_cast<const char*>(data), length);

  } else {
    throw std::runtime_error("Unknown file type");
  }
  return Status::kSuccess;
}

Status SharedLogStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  return append(fileName, data, length);
}

Status SharedLogStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  return append(fileName, data, length);
}

Status SharedLogStorage::flush(std::string const& fileName) {
  throw std::runtime_error("flush() is not supported for SharedLogStorage");
}

Status SharedLogStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  std::string entry_data;
  uint64_t index = std::stoull(fileName);
  if (!shared_log.ReadEntry(index, entry_data)) {
    return Status::Failure("ReadEntry failed");
  }
  size = entry_data.size();
  data = new unsigned char[size];
  std::memcpy(data, entry_data.data(), size);
  return Status::Success();
}

Status SharedLogStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  uint64_t from = a;
  uint64_t to = a + length - 1;
  std::vector<LogEntry> entries;
  if (!shared_log.ReadEntries(from, to, entries)) {
    return Status::Failure("ReadEntries failed");
  }
  std::string combined_data;
  for (const auto& entry : entries) {
    combined_data.append(entry.data);
  }
  size_t combined_size = combined_data.size();
  data = new unsigned char[combined_size];
  std::memcpy(data, combined_data.data(), combined_size);
  return Status::Success();
}

size_t SharedLogStorage::size(std::string fileName) {
  auto [last_index, _, __] = shared_log.GetTail();
  return last_index;
}

void SharedLogStorage::seal(std::string fileName) {
  throw std::runtime_error("seal() is not supported for SharedLogStorage");
}

bool SharedLogStorage::isSealed(std::string fileName) {
  return false;
}

void SharedLogStorage::remove(std::string fileName) {
  throw std::runtime_error("remove() is not supported for SharedLogStorage");
}

bool SharedLogStorage::exist(std::string fileName) {
  uint64_t index = std::stoull(fileName);
  std::string entry_data;
  return shared_log.ReadEntry(index, entry_data);
}
