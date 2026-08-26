#include "storage.h"
#include <filesystem>
#include <fstream>
#include <thread>
namespace ozonedb {
std::ifstream* FileStorage::getReadStream(std::string const& name) {
  std::unique_lock<std::shared_mutex> lock(read_mtx);
  if (this->read_streams.find(name) == this->read_streams.end()) {
    // File streams are associated with files either on construction, or by calling member open.
    this->read_streams[name] = std::make_unique<std::ifstream>(this->storage_path + name, std::ios::binary);
  }
  return this->read_streams[name].get();
}

std::ofstream* FileStorage::getWriteStream(std::string const& name) {
  std::unique_lock<std::shared_mutex> lock(write_mtx);
  if (this->write_streams.find(name) == this->write_streams.end()) {
    try {
      this->write_streams[name] = std::make_unique<std::ofstream>(this->storage_path + name, std::ios::binary | std::ios::app);
    } catch (std::exception const& e) {
      std::cerr << "Failed to open output file1." << std::endl;
      return this->write_streams[name].get();
    }
  }
  return this->write_streams[name].get();
}

void FileStorage::createDirectory(std::string name) {
  std::string directory_path = storage_path + name;
  if (!std::filesystem::exists(directory_path)) {
    try {
      std::filesystem::create_directories(directory_path);
    } catch (std::exception const& e) {
      if (std::filesystem::exists(directory_path)) {
        // Directory exists, likely a race condition
        std::cout << "Failed to create directory due to race condition, it's normal." << std::endl;
      } else {
        // Actual error occurred
        std::cerr << "Failed to create directory for unknown reason." << std::endl;
      }
    }
  }
}

// One mutex per file name, created on first use and never erased (the
// write stream it guards is never erased either). append_map_mtx_ guards
// only the map lookup, so two writers on different files never contend.
std::mutex& FileStorage::appendMutexFor(std::string const& name) {
  std::lock_guard<std::mutex> lk(append_map_mtx_);
  auto it = append_mtx_.find(name);
  if (it == append_mtx_.end()) {
    it = append_mtx_.emplace(name, std::make_unique<std::mutex>()).first;
  }
  return *it->second;
}

Status FileStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  std::ofstream* output_file = getWriteStream(fileName);
  if (isSealed(fileName)) {
    // std::cerr << getpid() << ":The file is sealed: " << fileName << std::endl;
    return Status::kSealed;
  } else if (!output_file->is_open()) {
    std::cerr << "Failed to open output file: " << fileName << std::endl;
    return Status::kFailure;
  }
  // Hold the per-file lock across write + flush + fsync. Without it two
  // threads interleave bytes inside one ofstream and the file is corrupt.
  std::lock_guard<std::mutex> lk(appendMutexFor(fileName));
  output_file->write(reinterpret_cast<char const*>(data), length);
  output_file->flush();
  fsync(GetFileDescriptor(*output_file->rdbuf()));
  if (output_file->good()) {
    return Status::kSuccess;
  } else
    return Status::kFailure;
}

Status FileStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  std::ofstream* output_file = getWriteStream(fileName);
  if (isSealed(fileName)) {
    // std::cerr << getpid() << ":The file is sealed: " << fileName << std::endl;
    return Status::kSealed;
  } else if (!output_file->is_open()) {
    std::cerr << "Failed to open output file: " << fileName << std::endl;
    return Status::kFailure;
  }
  std::lock_guard<std::mutex> lk(appendMutexFor(fileName));
  output_file->write(reinterpret_cast<char const*>(data), length);
  // outputFile.flush();
  if (output_file->good()) {
    return Status::kSuccess;
  } else
    return Status::kFailure;
}

Status FileStorage::flush(std::string const& fileName) {
  std::ofstream* output_file = getWriteStream(fileName);
  if (!output_file->is_open()) {
    std::cerr << "Failed to open output file." << std::endl;
    return Status::kFailure;
  }
  std::lock_guard<std::mutex> lk(appendMutexFor(fileName));
  output_file->flush();
  if (output_file->good()) {
    return Status::kSuccess;
  } else
    return Status::kFailure;
}

Status FileStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  std::ifstream* input_file = getReadStream(fileName);
  if (!input_file->is_open()) {
    if (!exist(fileName)) {
      std::cerr << getpid() << ":The file does not exist: " << fileName << std::endl;
      return Status::kNotFound;
    }
    std::cerr << "Failed to open input file: " << fileName
              << ". Error: " << std::strerror(errno) << std::endl;
    return Status::kFailure;
  }
  size = this->size(fileName);
  seek(*input_file, 0);
  data = new unsigned char[size];
  input_file->read(reinterpret_cast<char*>(data), size);
  return Status::kSuccess;
}

Status FileStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  // read data from a, read b bytes in total, similar as previous read method
  std::ifstream* input_file = getReadStream(fileName);
  if (!input_file->is_open()) {
    if (!exist(fileName)) {
      std::cerr << getpid() << ":The file does not exist:" << fileName << std::endl;
      return Status::kNotFound;
    }
    std::cerr << "Failed to open input file: " << fileName
              << ". Error: " << std::strerror(errno) << std::endl;
    return Status::kFailure;
  }
  seek(*input_file, a);
  // Read the specified number of bytes
  data = new unsigned char[length];
  input_file->read(reinterpret_cast<char*>(data), length);
  if (!input_file) {
    if (input_file->eof()) {
      std::cerr << "End of file reached prematurely while reading from: " << fileName << std::endl;
    } else if (input_file->fail()) {
      std::cerr << "Failed to read data correctly from: " << fileName << std::endl;
    } else if (input_file->bad()) {
      std::cerr << "Critical I/O error occurred while reading from: " << fileName << std::endl;
    }
    return Status::kFailure;
  }
  return Status::kSuccess;
}

size_t FileStorage::size(std::string fileName) {
  std::filesystem::path full_path = this->storage_path + fileName;
  std::error_code ec;  // To avoid throwing exceptions
  auto file_size = std::filesystem::file_size(full_path, ec);
  if (ec) {
    // std::cout <<"File does not exist or some other error occurred"<<std::endl;
    return 0;
  }
  return file_size;
}

void FileStorage::seal(std::string fileName) {
  // make the file readonly
  std::filesystem::permissions(
      this->storage_path + fileName,
      std::filesystem::perms::owner_read | std::filesystem::perms::others_read);
}

bool FileStorage::isSealed(std::string fileName) {
  std::filesystem::path full_path = this->storage_path + fileName;
  std::error_code ec;  // To avoid throwing exceptions
  auto sealed = std::filesystem::status(this->storage_path + fileName, ec).permissions() ==
                (std::filesystem::perms::owner_read | std::filesystem::perms::others_read);
  if (ec) {
    // std::cout <<"File does not exist or some other error occurred"<<std::endl;
    return false;
  }
  return sealed;
}

void FileStorage::remove(std::string fileName) {
  try {
    // Attempt to remove the file
    std::filesystem::remove(this->storage_path + fileName);
  } catch (const std::filesystem::filesystem_error &e) {
    // Output the error message and error code
    std::cerr << "Error deleting file: " << e.what() << '\n';
    std::cerr << "Error code: " << e.code().message() << '\n';
  }
}

bool FileStorage::exist(std::string fileName) {
  return std::filesystem::exists(this->storage_path + fileName);
}

void FileStorage::seek(std::ifstream& file, int position) {
  // seek to the position in the file
  if (file.tellg() == -1) {
    file.clear();
  }
  file.seekg(position, std::ios::beg);
  if (!file) {
    std::cerr << "Failed to seek to position: " << position << std::endl;
  }
}
}  // namespace ozonedb