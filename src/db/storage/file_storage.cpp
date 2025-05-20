#include "storage/file_storage.h"
#include <filesystem>
#include <fstream>
#include <thread>
namespace ozonedb {

FileStorage::FileStorage(std::string path) : Storage(path) {
  // if the directory does not exist, create it
  if (!std::filesystem::exists(path)) {
    if (!std::filesystem::create_directories(path)) {
      if (std::filesystem::exists(path)) {
        std::cerr << "Directory already created." << std::endl;
      } else {
        std::cerr << "Failed to create db directory." << std::endl;
      }
    }
  };
};

FileStorage::~FileStorage() {
  for (auto& [key, value] : read_streams) {
    value->close();
    value.reset();
  }
  for (auto& [key, value] : write_streams) {
    value->close();
    value.reset();
  }
};

Status FileStorage::append(std::string const& file_name, google::protobuf::Message const& message, size_t& size) {
  std::string data = protobuf::serializeMessage(message);
  size = data.size();
  return this->append(file_name, data);
}

Status FileStorage::read(std::string const& file_name, size_t from, size_t to, std::function<google::protobuf::Message*()> const& messageFactory,
                         std::vector<google::protobuf::Message*>& outMessages) {
  std::string buffer;
  this->read(file_name, buffer, from, to - from);
  protobuf::deserializeMessages(buffer, outMessages, messageFactory);
  return Status::kSuccess;
}

Status FileStorage::read(std::string const& file_name, std::function<google::protobuf::Message*()> const& messageFactory,
                         std::vector<google::protobuf::Message*>& outMessages) {
  std::string buffer;
  this->read(file_name, buffer);
  protobuf::deserializeMessages(buffer, outMessages, messageFactory);
  return Status::kSuccess;
};

size_t FileStorage::size(std::string fileName) {
  std::filesystem::path full_path = this->path + fileName;
  std::error_code ec;  // To avoid throwing exceptions
  auto file_size = std::filesystem::file_size(full_path, ec);
  if (ec) {
    std::cout << "File does not exist or some other error occurred" << std::endl;
    return 0;
  }
  return file_size;
}

//=================================================================================
void FileStorage::createDirectory(std::string dirName) {
  std::string directory_path = path + dirName;

  if (!std::filesystem::exists(directory_path)) {
    try {
      std::filesystem::create_directories(directory_path);
    } catch (std::exception const& e) {
      if (std::filesystem::exists(directory_path)) {
        std::cout << "Failed to create directory due to race condition, it's normal." << std::endl;
      } else {
        std::cerr << "Failed to create directory for unknown reason." << std::endl;
      }
    }
  }
}

Status FileStorage::appendNoFlush(std::string const& file_name, google::protobuf::Message const& message, size_t& size) {
  std::string data = protobuf::serializeMessage(message);
  size = data.size();
  return appendNoFlush(file_name, data);
}

Status FileStorage::flush(std::string const& fileName) {
  std::ofstream* output_file = getWriteStream(fileName);
  if (!output_file->is_open()) {
    std::cerr << "Failed to open output file." << std::endl;
    return Status::kFailure;
  }
  output_file->flush();
  if (output_file->good()) {
    return Status::kSuccess;
  } else
    return Status::kFailure;
}

void FileStorage::seal(std::string fileName) {
  std::filesystem::permissions(
      this->path + fileName,
      std::filesystem::perms::owner_read | std::filesystem::perms::others_read);
}

void FileStorage::remove(std::string fileName) {
  try {
    // Attempt to remove the file
    std::filesystem::remove(this->path + fileName);
  } catch (std::filesystem::filesystem_error const& e) {
    // Output the error message and error code
    std::cerr << "Error deleting file: " << e.what() << '\n';
    std::cerr << "Error code: " << e.code().message() << '\n';
  }
}

bool FileStorage::exist(std::string fileName) {
  return std::filesystem::exists(this->path + fileName);
}

//=============================private=============================================

Status FileStorage::append(std::string const& fileName, std::string const& data) {
  std::ofstream* output_file = getWriteStream(fileName);

  if (isSealed(fileName)) {
    return Status::kSealed;
  } else if (!output_file->is_open()) {
    std::cerr << "Failed to open output file: " << fileName << std::endl;
    return Status::kFailure;
  }

  output_file->write(data.data(), data.size());  // write the string data
  output_file->flush();
  fsync(GetFileDescriptor(*output_file->rdbuf()));

  if (output_file->good()) {
    return Status::kSuccess;
  } else {
    return Status::kFailure;
  }
}

Status FileStorage::appendNoFlush(std::string const& fileName, std::string const& data) {
  std::ofstream* output_file = getWriteStream(fileName);
  if (isSealed(fileName)) {
    return Status::kSealed;
  } else if (!output_file->is_open()) {
    std::cerr << "Failed to open output file: " << fileName << std::endl;
    return Status::kFailure;
  }

  output_file->write(data.data(), data.size());  // binary-safe
  return output_file->good() ? Status::kSuccess : Status::kFailure;
}

Status FileStorage::read(std::string const& fileName, std::string& data) {
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

  const size_t file_size = this->size(fileName);
  data.resize(file_size);
  seek(*input_file, 0);
  input_file->read(data.data(), file_size);
  return input_file->good() ? Status::kSuccess : Status::kFailure;
}

Status FileStorage::read(std::string const& fileName, std::string& data, size_t offset, size_t length) {
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

  seek(*input_file, offset);
  data.resize(length);
  input_file->read(data.data(), length);
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

std::ifstream* FileStorage::getReadStream(std::string const& name) {
  std::unique_lock<std::shared_mutex> lock(read_mtx);
  if (this->read_streams.find(name) == this->read_streams.end()) {
    // File streams are associated with files either on construction, or by calling member open.
    this->read_streams[name] = std::make_unique<std::ifstream>(this->path + name, std::ios::binary);
  }
  return this->read_streams[name].get();
}

std::ofstream* FileStorage::getWriteStream(std::string const& name) {
  std::unique_lock<std::shared_mutex> lock(write_mtx);
  if (this->write_streams.find(name) == this->write_streams.end()) {
    try {
      this->write_streams[name] = std::make_unique<std::ofstream>(this->path + name, std::ios::binary | std::ios::app);
    } catch (std::exception const& e) {
      std::cerr << "Failed to open output file1." << std::endl;
      return this->write_streams[name].get();
    }
  }
  return this->write_streams[name].get();
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

bool FileStorage::isSealed(std::string fileName) {
  std::filesystem::path full_path = this->path + fileName;
  std::error_code ec;  // To avoid throwing exceptions
  auto sealed = std::filesystem::status(this->path + fileName, ec).permissions() ==
                (std::filesystem::perms::owner_read | std::filesystem::perms::others_read);
  if (ec) {
    // std::cout <<"File does not exist or some other error occurred"<<std::endl;
    return false;
  }
  return sealed;
}

int FileStorage::GetFileDescriptor(std::filebuf& filebuf) {
  class MyFileBuf : public std::filebuf {
   public:
    int handle() { return _M_file.fd(); }
  };

  return static_cast<MyFileBuf&>(filebuf).handle();
}

}  // namespace ozonedb