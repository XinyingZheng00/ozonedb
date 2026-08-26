#ifndef STORAGE_H
#define STORAGE_H
#include "protobuf/record.pb.h"
#include <azure/identity/client_secret_credential.hpp>
#include <azure/storage/blobs.hpp>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
namespace ozonedb {

enum class Status { kSuccess,
                    kFailure,
                    kSealed,
                    kNotFound };

// Signalled by shared-log backends (e.g. Corfu) when a *remote* writer
// appends/removes a file entry. Lets the LogHandler maintain a key
// index that stays correct under multi-writer semantics without
// requiring every reader to fence against the shared log.
enum class RemoteOp { kAppend,
                      kRemove };
// `addr` is the entry's position in the backend's global order (the
// Corfu global address). It ranks the records the payload carries, so a
// peer entry that the tailer delivers late cannot overwrite a newer
// record in LogKeyIndex. Backends with no global order pass -1.
using RemoteAppendListener =
    std::function<void(std::string const& file_name,
                       unsigned char const* data, size_t len, RemoteOp op,
                       long addr)>;

/**
 * @brief Base class for all the storage classes
 *
 */
class Storage {
 protected:
  std::string storage_path;

 public:
  Storage(std::string storage_path) {
    this->storage_path = storage_path;
  };
  virtual ~Storage() = default;

  /**
   * @brief Create Directory
   *
   * @param name
   */
  virtual void createDirectory(std::string name) { throw std::runtime_error("createDirectory() is not implemented"); };

  /**
   * @brief Append a record to the storage unit
   *
   * @param record
   * @return Status
   */
  virtual Status append(std::string const& fileName, unsigned char* const& data, int length) { throw std::runtime_error("append is not implemented"); };
  virtual Status appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) { throw std::runtime_error("appendNoFlush is not implemented"); };
  virtual Status appendInBatch(std::string const& fileName, unsigned char* const& data, int length) { throw std::runtime_error("appendInBatch is not implemented"); };
  /**
   * @brief Flush the file
   *
   * @param fileName
   * @return Status
   */
  virtual Status flush(std::string const& fileName) { throw std::runtime_error("flush is not implemented"); };

  /**
   * @brief Read all bytes from the storage unit
   *
   * @param records the results will be stored in this vector
   * @return Status
   */
  virtual Status read(std::string const& fileName, unsigned char*& data, size_t& size) { throw std::runtime_error("read() is not implemented"); };

  /**
   * @brief Read all bytes from the file position a, read length bytes.
   *
   */
  virtual Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) { throw std::runtime_error("read() is not implemented"); };

  /**
   * @brief Return the size of the storage unit
   *
   * @return size_t
   */
  virtual size_t size(std::string fileName) { throw std::runtime_error("size() is not implemented"); };

  /**
   * @brief Make the storage unit readonly, seal is idempotent
   *
   */
  virtual void seal(std::string fileName) { throw std::runtime_error("seal() is not implemented"); };

  /**
   * @brief Check whether the unit is sealed
   *
   */
  virtual bool isSealed(std::string fileName) { throw std::runtime_error("isSealed() is not implemented"); };

  /**
   * @brief Delete the storage unit
   *
   */
  virtual void remove(std::string fileName) { throw std::runtime_error("remove() is not implemented"); };

  /**
   * @brief Check whether the unit is exist
   *
   */
  virtual bool exist(std::string fileName) { throw std::runtime_error("exist() is not implemented"); };

  /**
   * @brief Register a callback for remote (peer-writer) appends and
   * removes. Default no-op — only shared-log backends like CorfuDB
   * override. Should be called once before reads begin.
   */
  virtual void setRemoteAppendListener(RemoteAppendListener /*listener*/) {}

  /**
   * @brief Global-order position of this thread's last successful append.
   *
   * Valid only immediately after an append/appendInBatch/flush on the
   * same thread returned kSuccess. Lets a caller rank the record it just
   * wrote against records the tailer delivers later — without it, the
   * local writer has no way to say which of two copies of a key is
   * newer. Returns -1 on backends with no global order.
   */
  virtual long lastAppendAddressForThread() const { return -1; }

  /**
   * @brief Establish a linearization fence for the CALLING THREAD.
   *
   * On shared-log backends this samples the log's global tail once,
   * waits for local state to cover it, and records a thread-local
   * fence token; until clearSync(), every read/size/exist/isSealed on
   * this thread reuses the token instead of re-fencing (their answers
   * are then served from already-synced local state). Default no-op:
   * on direct backends (local FS, S3, Azure) reads hit the backing
   * store, which is always "synced".
   *
   * Token scope is one logical operation (a strict DB::get). Always
   * clear via SyncScope — a leaked token would silently unfence later
   * operations on the same thread.
   */
  virtual void sync() {}
  virtual void clearSync() {}
  // True when the CALLING THREAD holds a live sync() token for this
  // storage. Lets DB::get reuse a caller-established fence (batch
  // strict reads: one fence amortized over many gets, each linearized
  // at the caller's sync() point) instead of stacking its own.
  virtual bool hasSyncToken() const { return false; }

  // RAII for the sync() token: clears it on scope exit so early
  // returns and exceptions in the caller can't leak the fence.
  class SyncScope {
   public:
    explicit SyncScope(Storage& storage) : storage_(storage) { storage_.sync(); }
    ~SyncScope() { storage_.clearSync(); }
    SyncScope(SyncScope const&) = delete;
    SyncScope& operator=(SyncScope const&) = delete;

   private:
    Storage& storage_;
  };

#include <unistd.h>  // For fsync()
  int GetFileDescriptor(std::filebuf& filebuf) {
    class MyFileBuf : public std::filebuf {
     public:
      int handle() { return _M_file.fd(); }
    };

    return static_cast<MyFileBuf&>(filebuf).handle();
  }

 private:
  /**
   * @brief move the pointer to the position in the file
   *
   * @param fileName
   * @param position
   */
  virtual void seek(std::ifstream& file, int position) { throw std::runtime_error("seek() is not implemented"); };
};

/**
 * @brief FileStorage class for storage
 *
 */
class FileStorage : public Storage {
 public:
  explicit FileStorage(std::string path) : Storage(path) {
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

  ~FileStorage() {
    for (auto& [key, value] : read_streams) {
      value->close();
      value.reset();
    }
    for (auto& [key, value] : write_streams) {
      value->close();
      value.reset();
    }
  };

  void createDirectory(std::string name);
  Status append(std::string const& fileName, unsigned char* const& data, int length);         // Method to append bytes to the file
  Status appendNoFlush(std::string const& fileName, unsigned char* const& data, int length);  // Method to append bytes to the file without flushing, this is only used for sstable writing
  Status appendInBatch(std::string const& fileName, unsigned char* const& data, int length) {
    return append(fileName, data, length);
  };
  Status flush(std::string const& fileName);                                                // Method to flush the file
  Status read(std::string const& fileName, unsigned char*& data, size_t& size);             // Method to read the all bytes from the file
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length);  // Read all bytes from the file position a, read length bytes.
  size_t size(std::string fileName);                                                        // Method to get the size of the file
  void seal(std::string fileName);                                                          // Seal the file
  bool isSealed(std::string fileName);                                                      // Check whether the file is sealed
  void remove(std::string fileName);                                                        // Delete the file
  bool exist(std::string fileName);                                                         // Check whether the file exists
 private:
  void seek(std::ifstream& file, int position);  // Move the pointer to the position in the file
  std::unordered_map<std::string, std::unique_ptr<std::ifstream>> read_streams;
  std::unordered_map<std::string, std::unique_ptr<std::ofstream>> write_streams;
  std::shared_mutex read_mtx;
  std::shared_mutex write_mtx;

  // Serializes writes to one file's ofstream.
  //
  // getWriteStream hands the SAME ofstream to every caller for a given
  // name, and one append is a write + flush + fsync. Two threads
  // appending concurrently interleave their bytes inside that stream, so
  // metadata.log ends up with two half-records spliced together and is
  // corrupt on disk, permanently.
  //
  // The lock belongs here, in the backend, not in MetadataLogHandler: the
  // contract is that the storage layer owns its own concurrency (which
  // CorfuDBStorage does), and every caller of append/appendNoFlush/flush
  // benefits, not just the metadata log.
  std::mutex append_map_mtx_;
  std::unordered_map<std::string, std::unique_ptr<std::mutex>> append_mtx_;
  std::mutex& appendMutexFor(std::string const& name);

 public:
  std::ifstream* getReadStream(std::string const& name);
  std::ofstream* getWriteStream(std::string const& name);
};
using namespace Azure::Storage::Blobs;
using namespace Azure::Identity;
class AzureBlobStorage : public Storage {
 public:
  std::shared_ptr<BlobServiceClient> blobServiceClient;
  std::shared_ptr<BlobContainerClient> containerClient;
  std::string containerName;
  std::mutex mtx;                                                           // Mutex for the cached_file
  std::unordered_map<std::string, std::vector<unsigned char>> cached_file;  // In-memory cache for sstable data
  std::chrono::_V2::system_clock::time_point last_commited_time_;
  int commit_count_ = 0;                      // Commit count
  int commit_interval_ = 10;                  // Commit interval in milliseconds
  bool sync_mode_ = false;                    // When true, appendInBatch blocks until batch is flushed to Azure
  std::condition_variable batch_flushed_cv_;  // Notifies blocked writers after batch flush
  void setSyncMode(bool sync) { sync_mode_ = sync; }

  // vp7eifiiqeHobq0nFpHv6MOI/J53UXgOKYxg0xIwOQj0NHe2cbOcVmdtgh6KE/9cu2UU9z3oPjvI+AStoe1A2Q==
  AzureBlobStorage(std::string const& connectionString, std::string const& containerName, std::string const& db_path)
      : Storage(db_path), containerName(containerName) {
    std::cout << "Creating AzureBlobStorage" << std::endl;
    blobServiceClient = std::make_shared<BlobServiceClient>(BlobServiceClient::CreateFromConnectionString(connectionString));
    containerClient = std::make_shared<BlobContainerClient>(blobServiceClient->GetBlobContainerClient(containerName));
    containerClient->CreateIfNotExists();
    last_commited_time_ = std::chrono::high_resolution_clock::now();
  }

  ~AzureBlobStorage() {
    for (auto& [key, value] : cached_file) {
      append(key, value.data(), value.size());
      std::filesystem::remove(key);
    }
  }

  BlobClient getBlobClient(std::string const& fileName);
  BlockBlobClient getBlockBlobClient(std::string const& fileName);
  AppendBlobClient getAppendBlobClient(std::string const& fileName);
  void createDirectory(std::string name);
  Status append(std::string const& fileName, unsigned char* const& data, int length);         // Method to append bytes to the file
  Status appendNoFlush(std::string const& fileName, unsigned char* const& data, int length);  // Method to append bytes to the file without flushing, this is only used for sstable writing
  Status appendInBatch(std::string const& fileName, unsigned char* const& data, int length);
  Status flush(std::string const& fileName);                                                // Method to flush the file
  Status read(std::string const& fileName, unsigned char*& data, size_t& size);             // Method to read the all bytes from the file
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length);  // Read all bytes from the file position a, read length bytes.
  size_t size(std::string fileName);                                                        // Method to get the size of the file
  void seal(std::string fileName);                                                          // Seal the file
  bool isSealed(std::string fileName);                                                      // Check whether the file is sealed
  void remove(std::string fileName);                                                        // Delete the file
  bool exist(std::string fileName);                                                         // Check whether the file exists
 private:
  void seek(std::ifstream& file, int position);  // Move the pointer to the position in the file
  void logBatch(std::string const& fileName, unsigned char* const& data, int length);
};
}  // namespace ozonedb
#endif  // STORAGE_H
