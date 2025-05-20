#include "storage.h"
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
  int commit_count_ = 0;      // Commit count
  int commit_interval_ = 10;  // Commit interval in milliseconds

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
