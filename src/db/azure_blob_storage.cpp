#include "storage.h"

using namespace Azure::Storage::Blobs;
using namespace Azure::Identity;
namespace ozonedb {

BlobClient AzureBlobStorage::getBlobClient(std::string const& fileName) {
  return containerClient->GetBlobClient(this->storage_path + fileName);
}

BlockBlobClient AzureBlobStorage::getBlockBlobClient(std::string const& fileName) {
  return containerClient->GetBlockBlobClient(this->storage_path + fileName);
}

AppendBlobClient AzureBlobStorage::getAppendBlobClient(std::string const& fileName) {
  auto appendClient = containerClient->GetAppendBlobClient(this->storage_path + fileName);
  appendClient.CreateIfNotExists();
  return appendClient;
}

void AzureBlobStorage::createDirectory(std::string name) {
  return;
}

Status AzureBlobStorage::append(std::string const& fileName, unsigned char* const& data, int length) {
  // append is only applicable for append blobs, basically, for log layer, task log, metadata log
  try {
    auto blobClient = getAppendBlobClient(fileName);
    Azure::Core::IO::MemoryBodyStream content(data, length);
    blobClient.AppendBlock(content);
    return Status::kSuccess;
  } catch (Azure::Core::RequestFailedException const& e) {
    if (e.ErrorCode == "BlobIsSealed") {
      return Status::kSealed;
    }
    return Status::kFailure;
  } catch (std::exception const& e) {
    return Status::kFailure;
  }
}

#include <fstream>
#include <sstream>
thread_local std::unique_ptr<std::ofstream> logFile = nullptr;
thread_local std::string logFileName = "";
void AzureBlobStorage::logBatch(std::string const& fileName, unsigned char* const& data, int length) {
  // Logs the batch data for durability
  // Create or append to a log file
  if (logFileName.empty() || logFileName != fileName) {
    logFileName = fileName;
    if (logFile) {
      logFile->close();
    }
    logFile = std::make_unique<std::ofstream>(fileName, std::ios::binary);
    if (!logFile->is_open()) {
      std::filesystem::create_directories(std::filesystem::path(fileName).parent_path());
      logFile = std::make_unique<std::ofstream>(fileName, std::ios::binary);
      if (!logFile->is_open()) {
        throw std::runtime_error("Failed to open log file");
      }
    }
  }
  logFile->write(reinterpret_cast<char const*>(data), length);  // Write buffer contents
  logFile->flush();
  fsync(GetFileDescriptor(*logFile->rdbuf()));  // todo: test without fsync, test fsyc performance
}

Status AzureBlobStorage::appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
  // logBatch(fileName, data, length);
  appendNoFlush(fileName, data, length);
  auto now = std::chrono::high_resolution_clock::now();
  commit_count_++;
  if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_commited_time_).count() > commit_interval_) {
    std::cout << std::this_thread::get_id() << ":committing batch for " << commit_count_ << std::endl;
    commit_count_ = 0;
    std::unique_lock lock(mtx);
    auto it = cached_file.find(fileName);
    if (it == cached_file.end()) {
      return Status::kNotFound;
    }
    auto& buffer = it->second;
    try {
      Azure::Core::IO::MemoryBodyStream content(buffer.data(), buffer.size());
      auto blobClient = getAppendBlobClient(fileName);
      blobClient.AppendBlock(content);
      // std::cout<<"uploading blob : "<< std::to_string(buffer.size())<<std::endl;
      cached_file.erase(it);
      // std::filesystem::remove(fileName);
      last_commited_time_ = std::chrono::high_resolution_clock::now();
      return Status::kSuccess;
    } catch (Azure::Core::RequestFailedException const& e) {
      if (e.ErrorCode == "BlobIsSealed") {
        return Status::kSealed;
      }
      return Status::kFailure;
    } catch (std::exception const& e) {
      return Status::kFailure;
    }
    /*
    std::thread([this, fileName, buffer = std::move(buffer)]() {
            try {
                // Perform blob upload asynchronously
                std::cout<<"uploading blob : "<< std::to_string(buffer.size())<<std::endl;
                Azure::Core::IO::MemoryBodyStream content(buffer.data(), buffer.size());
                auto blobClient = getAppendBlobClient(fileName);
                blobClient.AppendBlock(content);
                cached_file.erase(fileName);
                // std::filesystem::remove(fileName);
            } catch (Azure::Core::RequestFailedException const& e) {
                if (e.ErrorCode == "BlobIsSealed") {
                    // Handle sealed blob error
                }
                // Handle other errors
            } catch (std::exception const& e) {
                // Handle general exceptions
            }
        }).detach();  // Detach the thread to run asynchronously
    */
  }
  return Status::kSuccess;
}

Status AzureBlobStorage::appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) {
  // Azure Blob storage doesn't support append operations directly
  // We cache the data first, then append together at flush
  std::unique_lock lock(mtx);
  auto& buffer = cached_file[fileName];
  buffer.insert(buffer.end(), data, data + length);
  return Status::kSuccess;
}

Status AzureBlobStorage::flush(std::string const& fileName) {
  // invoke only after appendNoFlush
  std::unique_lock lock(mtx);
  auto it = cached_file.find(fileName);
  if (it == cached_file.end()) {
    return Status::kNotFound;
  }
  Azure::Core::IO::MemoryBodyStream content(it->second.data(), it->second.size());
  auto blobClient = getBlockBlobClient(fileName);
  Azure::Storage::Blobs::UploadBlockBlobOptions options;
  Azure::Core::Context context;
  try {
    blobClient.Upload(content, options, context);
    cached_file.erase(it);
  } catch (std::exception const& e) {
    return Status::kFailure;
  }
  return Status::kSuccess;
}

Status AzureBlobStorage::read(std::string const& fileName, unsigned char*& data, size_t& size) {
  try {
    auto blobClient = getBlobClient(fileName);
    auto properties = blobClient.GetProperties();
    size = properties.Value.BlobSize;
    // Allocate memory for the data
    data = new unsigned char[size];
    DownloadBlobOptions options;
    Azure::Core::Context context;
    auto response = blobClient.Download(options, context);
    size_t totalBytesRead = 0;
    while (totalBytesRead < size) {
      size_t bytesRead = response.Value.BodyStream->Read(
          data + totalBytesRead, size - totalBytesRead, context);
      if (bytesRead == 0) break;  // End of stream
      totalBytesRead += bytesRead;
    }
    return Status::kSuccess;
  } catch (std::exception const&) {
    return Status::kFailure;
  }
}

Status AzureBlobStorage::read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) {
  try {
    auto blobClient = getBlobClient(fileName);
    DownloadBlobOptions options;
    Azure::Core::Http::HttpRange range;
    range.Offset = a;
    range.Length = length;
    options.Range = range;
    Azure::Core::Context context;
    auto response = blobClient.Download(options, context);
    data = new unsigned char[length];
    size_t totalBytesRead = 0;
    while (totalBytesRead < length) {
      size_t bytesRead = response.Value.BodyStream->Read(
          data + totalBytesRead, length - totalBytesRead, context);
      if (bytesRead == 0) break;  // End of stream
      totalBytesRead += bytesRead;
    }
    return Status::kSuccess;
  } catch (std::exception const&) {
    return Status::kFailure;
  }
}

size_t AzureBlobStorage::size(std::string fileName) {
  auto blobClient = getBlobClient(fileName);
  try {
    auto properties = blobClient.GetProperties();
    return properties.Value.BlobSize;
  } catch (Azure::Core::RequestFailedException const& e) {
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
      return 0;
    } else {
      throw;  // For other exceptions, rethrow the error
    }
  }
}

void AzureBlobStorage::seal(std::string fileName) {
  // seal is only applicable for append blobs
  auto blobClient = getAppendBlobClient(fileName);
  blobClient.Seal();
}

bool AzureBlobStorage::isSealed(std::string fileName) {
  try {
    auto blobClient = getBlobClient(fileName);
    auto properties = blobClient.GetProperties().Value;
    return properties.IsSealed.HasValue() ? properties.IsSealed.Value() : false;
  } catch (Azure::Storage::StorageException const& e) {
    // Check for "404 Not Found" error, indicating the blob does not exist
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
      return false;  // Blob does not exist, return false
    } else {
      // Rethrow if it's a different error
      throw;
    }
  }
}

void AzureBlobStorage::remove(std::string fileName) {
  auto blobClient = getBlobClient(fileName);
  blobClient.DeleteIfExists();
}

bool AzureBlobStorage::exist(std::string fileName) {
  auto blobClient = getBlobClient(fileName);
  try {
    auto properties = blobClient.GetProperties();
    return true;  // If GetProperties succeeds, the blob exists
  } catch (Azure::Core::RequestFailedException const& e) {
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
      return false;  // If a 404 error occurs, the blob does not exist
    } else {
      throw;  // For other exceptions, rethrow the error
    }
  }
}

void AzureBlobStorage::seek(std::ifstream& file, int position) {
  // This method is not applicable for Azure Blob storage
  throw std::runtime_error("seek() is not applicable for Azure Blob storage");
}

}  // namespace ozonedb