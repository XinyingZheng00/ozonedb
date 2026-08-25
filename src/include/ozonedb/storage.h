#ifndef STORAGE_H
#define STORAGE_H
#include "protobuf/record.pb.h"
#include <azure/identity/client_secret_credential.hpp>
#include <azure/storage/blobs.hpp>
#include <condition_variable>
#include <cstdint>
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

// One read-set entry of a transaction: the key and the version the
// caller observed for it through versionedLookup / DB::getVersioned
// (-1 = unwritten). Storage::appendTransaction validates the whole set
// at the commit record's log position.
struct ReadVersion {
  std::string key;
  int64_t version = -1;
};

enum class Status { kSuccess,
                    kFailure,
                    kSealed,
                    kNotFound,
                    // Conditional append lost: the key's version at the
                    // entry's log position differed from expected_version.
                    kCasConflict };

// Signalled by shared-log backends (e.g. Corfu) when a *remote* writer
// appends/removes a file entry. Lets the LogHandler maintain a key
// index that stays correct under multi-writer semantics without
// requiring every reader to fence against the shared log.
enum class RemoteOp { kAppend,
                      kRemove };
using RemoteAppendListener =
    std::function<void(std::string const& file_name,
                       unsigned char const* data, size_t len, RemoteOp op)>;

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
   * @brief Conditionally append one serialized Record to a data-log file.
   *
   * The append takes effect only if the record's key still has version
   * `expected_version` (the global log address of its last accepted
   * write; -1 = key must be unwritten) at the point the entry lands in
   * the shared log. Blocks until the outcome is decided. On kSuccess,
   * result_version is the key's new version. Only shared-log backends
   * can order the check; the default is unsupported.
   */
  virtual Status appendConditional(std::string const& /*fileName*/,
                                   unsigned char const* /*data*/, int /*length*/,
                                   int64_t /*expected_version*/,
                                   int64_t& /*result_version*/) {
    return Status::kFailure;
  }

  /**
   * @brief Move bytes that were appended to `from` but not yet sequenced
   * (a backend's write batch) onto `to`.
   *
   * Only backends with an unflushed write cache do anything here
   * (CorfuDBStorage with corfu_sync_mode=false). LogHandler::addRecord
   * calls it when the data-log tail moved under a batched writer: the
   * cached bytes are in no file yet, so moving them to the new tail
   * loses nothing and keeps them out of the old tail's invisible region
   * past its LOGCREATE-frozen size.
   *
   * @return true if bytes were moved (the caller's last record is among
   *         them and must not be re-issued), false if nothing was cached.
   */
  virtual bool migrateCached(std::string const& /*from*/, std::string const& /*to*/) {
    return false;
  }

  /**
   * @brief Append a transaction commit record: `data` is the write set
   * (zero or more serialized Records, appendInBatch's layout) and
   * `read_set` the {key, version} pairs it depends on.
   *
   * The record takes effect only if every read-set version equals the
   * key's version at the record's log position; then the whole payload
   * is applied atomically and every written key's version becomes the
   * record's address. Every replica evaluates the same rule at the
   * same position, so the verdict is identical everywhere. Keys in the
   * payload but not in the read set are blind writes (not validated).
   * An empty payload with a non-empty read set is a read-only
   * validation record (appends no bytes). Like appendConditional the
   * writer never self-applies; it waits for its own apply loop.
   *
   * @return kSuccess (result_version = the record's log address),
   *         kCasConflict (a read-set version changed), kSealed (the
   *         target is sealed or removed; retry on a new tail), kFailure.
   */
  virtual Status appendTransaction(std::string const& /*fileName*/,
                                   unsigned char const* /*data*/, int /*length*/,
                                   std::vector<ReadVersion> const& /*read_set*/,
                                   int64_t& /*result_version*/) {
    return Status::kFailure;
  }

  /**
   * @brief Look up a key in the log-ordered version map.
   *
   * Returns false when the backend doesn't track versions or the key has
   * never been written. On true: version is the log address of the last
   * accepted write; when has_value is set the map also carries the
   * record's value and value/deleted describe that record — an atomic
   * (value, version) pair for read-modify-write callers. The inline
   * value exists for every tracked write (blind or conditional) from
   * its log position until the log file holding the record is REMOVEd
   * (compacted): from then on the record is served by the normal read
   * path. See CorfuDBStorage::KeyVersion for why that window needs it.
   */
  virtual bool versionedLookup(std::string const& /*key*/, int64_t& /*version*/,
                               std::string& /*value*/, bool& /*has_value*/,
                               bool& /*deleted*/) {
    return false;
  }

  /**
   * @brief Block until every remote-append event the backend's tailer
   * has enqueued so far has been delivered to the listener.
   *
   * On Corfu the listener (LogHandler::onRemoteAppend) runs on a
   * dispatch thread that lags the tailer, so a fence alone (sync())
   * guarantees the *storage buffers* cover the target but not that the
   * listener-fed key index does. Callers that read through the index
   * after fencing (DB::getVersioned) call this to close that gap.
   * Default no-op: direct backends have no listener pipeline.
   */
  virtual void syncListeners() {}

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
