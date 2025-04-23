#include "gtest/gtest.h"
#include "protobuf/record.pb.h"
#include "shared_log_storage.h"
#include "storage.h"
#include "test_tool.h"
#include <thread>
using namespace ozonedb;
TEST(StorageTest, read_write_to_binary_file_test) {
  // append timestamp to avoid file name conflict
  Storage* storage = new FileStorage("/tank/test/storage/");
  std::string fileName = "read_write_to_binary_file_test" + std::to_string(time(0));
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);

  unsigned char* readData = nullptr;
  size_t size;
  status = storage->read(fileName, readData, size);
  EXPECT_EQ(Status::kSuccess, status);
  EXPECT_EQ(*data.data(), *readData);
  delete storage;
}

TEST(StorageTest, read_partial_file_back) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  std::string fileName = "read_write_to_binary_file_test" + std::to_string(time(0));
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  for (size_t i = 0; i < 10; i++) {
    Status status = storage->append(fileName, data.data(), data.size());
    EXPECT_EQ(Status::kSuccess, status);
  }

  int num = 5;
  int start = 5;
  unsigned char* readData = nullptr;
  Status status = storage->read(fileName, readData, data.size() * start, data.size() * num);
  EXPECT_EQ(Status::kSuccess, status);

  std::vector<uint8_t> expectedData;
  for (size_t i = 0; i < num; i++) {
    expectedData.insert(expectedData.end(), data.begin(), data.end());
  }
  EXPECT_EQ(*expectedData.data(), *readData);
  delete storage;
}

TEST(StorageTest, create_directory_test) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  std::string directory = "create_directory_test" + std::to_string(time(0));
  storage->createDirectory(directory);
  EXPECT_TRUE(std::filesystem::exists("/tank/test/storage/" + directory));
}

TEST(StorageTest, size_test) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  std::string fileName = "size_test" + std::to_string(time(0));
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);
  EXPECT_EQ(5, storage->size(fileName));
  delete storage;
}

TEST(StorageTest, seal_test) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  std::string fileName = "seal_test" + std::to_string(time(0));
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);
  storage->seal(fileName);
  std::filesystem::perms p = std::filesystem::status("/tank/test/storage/" + fileName).permissions();
  EXPECT_EQ(std::filesystem::perms::owner_read, p & std::filesystem::perms::owner_read);
  EXPECT_EQ(std::filesystem::perms::others_read, p & std::filesystem::perms::others_read);
  delete storage;
}

TEST(StorageTest, remove_test) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  std::string fileName = "remove_test" + std::to_string(time(0));
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);
  storage->remove(fileName);
  EXPECT_FALSE(std::filesystem::exists("/tank/test/storage/" + fileName));
  delete storage;
}

void testMultipleCreate(std::string fileName, int i) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  storage->getWriteStream(fileName);
  delete storage;
  _exit(0);
}

TEST(StorageTest, concurrent_create) {
  std::string fileName = "concurrent_create" + std::to_string(time(0));
  int numProcesses = 100;
  multipleProcessorTester(numProcesses, testMultipleCreate, fileName);
  // check only one file is created
  int count = 0;
  for (auto const& entry : std::filesystem::directory_iterator("/tank/test/storage/")) {
    if (entry.path().filename().string().compare(0, fileName.length(), fileName) == 0) {
      count++;
    }
  }
  EXPECT_EQ(1, count);
}

void testMultipleAppend(std::string fileName, int i) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  storage->getWriteStream(fileName);
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);
  delete storage;
  _exit(0);
}

TEST(StorageTest, concurrent_append) {
  std::string fileName = "concurrent_append" + std::to_string(time(0));
  (new FileStorage("/tank/test/storage/"))->getWriteStream(fileName);
  int numProcesses = 100;
  multipleProcessorTester(numProcesses, testMultipleAppend, fileName);

  FileStorage* storage = new FileStorage("/tank/test/storage/");
  EXPECT_EQ(numProcesses * 5, storage->size(fileName));
  unsigned char* readData = nullptr;
  size_t size;
  Status status = storage->read(fileName, readData, size);
  EXPECT_EQ(Status::kSuccess, status);
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  std::vector<uint8_t> expectedData;
  for (int i = 0; i < numProcesses; ++i) {
    expectedData.insert(expectedData.end(), data.begin(), data.end());
  }
  EXPECT_EQ(*expectedData.data(), *readData);
}

void testCocurrentReadWrite(std::string fileName, int i) {
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  storage->getWriteStream(fileName);
  if (i % 2 != 0) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    Status status = storage->append(fileName, data.data(), data.size());
    EXPECT_EQ(status, Status::kSuccess);
  } else {
    unsigned char* readData = nullptr;
    Status status = storage->read(fileName, readData, 0, 5);
    EXPECT_EQ(Status::kSuccess, status);
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    EXPECT_EQ(*data.data(), *readData);
  }
  delete storage;
  _exit(0);
}

TEST(StorageTest, concurrent_read_write) {
  std::string fileName = "concurrent_read_write" + std::to_string(time(0));
  FileStorage* storage = new FileStorage("/tank/test/storage/");
  storage->getWriteStream(fileName);
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);
  int numProcesses = 100;
  multipleProcessorTester(numProcesses, testCocurrentReadWrite, fileName);
  delete storage;
}

std::string connection_string = "DefaultEndpointsProtocol=https;AccountName=ozonedbstorage;AccountKey=vp7eifiiqeHobq0nFpHv6MOI/J53UXgOKYxg0xIwOQj0NHe2cbOcVmdtgh6KE/9cu2UU9z3oPjvI+AStoe1A2Q==;EndpointSuffix=core.windows.net";

TEST(CloudStorageTest, read_write_to_append_blob_test) {
  Storage* storage = new AzureBlobStorage(connection_string, "tank", "test/storage/");
  std::string fileName = "read-write-to-append-file";
  storage->remove(fileName);

  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  for (size_t i = 0; i < 10; i++) {
    Status status = storage->append(fileName, data.data(), data.size());
    EXPECT_EQ(Status::kSuccess, status);
  }
  unsigned char* readData = nullptr;
  size_t size;
  Status status = storage->read(fileName, readData, size);
  std::vector<uint8_t> expectedData;
  for (size_t i = 0; i < 10; i++) {
    expectedData.insert(expectedData.end(), data.begin(), data.end());
  }
  EXPECT_EQ(Status::kSuccess, status);
  EXPECT_EQ(*expectedData.data(), *readData);
  delete storage;
}

TEST(CloudStorageTest, read_write_to_block_blob_test) {
  Storage* storage = new AzureBlobStorage(connection_string, "tank", "test/storage/");
  std::string fileName = "read-write-to-blob-file";
  storage->remove(fileName);
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  for (size_t i = 0; i < 10; i++) {
    Status status = storage->appendNoFlush(fileName, data.data(), data.size());
    EXPECT_EQ(Status::kSuccess, status);
  }
  storage->flush(fileName);

  unsigned char* readData = nullptr;
  size_t size;
  Status status = storage->read(fileName, readData, size);
  std::vector<uint8_t> expectedData;
  for (size_t i = 0; i < 10; i++) {
    expectedData.insert(expectedData.end(), data.begin(), data.end());
  }
  EXPECT_EQ(Status::kSuccess, status);
  EXPECT_EQ(*expectedData.data(), *readData);
  delete storage;
}

TEST(CloudStorageTest, read_partial_file_back) {
  // run with previous test
  Storage* storage = new AzureBlobStorage(connection_string, "tank", "test/storage/");
  std::string fileNameBlob = "read-write-to-blob-file";
  std::string fileNameAppend = "read-write-to-append-file";

  int num = 5;
  int start = 5;
  unsigned char* readDataAppend = nullptr;
  unsigned char* readDataBlob = nullptr;
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->read(fileNameAppend, readDataAppend, data.size() * start, data.size() * num);
  status = storage->read(fileNameBlob, readDataBlob, data.size() * start, data.size() * num);
  EXPECT_EQ(Status::kSuccess, status);

  std::vector<uint8_t> expectedData;
  for (size_t i = 0; i < num; i++) {
    expectedData.insert(expectedData.end(), data.begin(), data.end());
  }
  EXPECT_EQ(*expectedData.data(), *readDataAppend);
  EXPECT_EQ(*expectedData.data(), *readDataBlob);
  delete storage;
}

TEST(CloudStorageTest, size_test) {
  Storage* storage = new AzureBlobStorage(connection_string, "tank", "test/storage/");
  std::string fileName = "size-test";
  storage->remove(fileName);
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);
  EXPECT_EQ(5, storage->size(fileName));
  delete storage;
}

TEST(CloudStorageTest, seal_test) {
  Storage* storage = new AzureBlobStorage(connection_string, "tank", "test/storage/");
  std::string fileName = "seal-test";
  storage->remove(fileName);
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};
  Status status = storage->append(fileName, data.data(), data.size());
  EXPECT_EQ(Status::kSuccess, status);
  storage->seal(fileName);
  EXPECT_TRUE(storage->isSealed(fileName));
  delete storage;
}

TEST(CloudStorageTest, write_file_to_append_blob_test) {
  Storage* storage = new AzureBlobStorage(connection_string, "tank", "test/storage/");
  std::string fileName = "read-write-to-append-file";
  storage->remove(fileName);
  // Service version 2022-11-02 and later: The maximum block size is 100 MB
  // Service versions earlier than 2022-11-02: The maximum block size is 4 MB

  int length = 100 * 1024 * 1024;  // 100MB file
  unsigned char* const& data = new unsigned char[length];
  // fill the data with random generated data
  for (int i = 0; i < length; i++) {
    data[i] = rand() % 256;
  }

  std::cout << "Start uploading file to Azure Blob Storage..." << std::endl;
  int sum = 0;
  for (int i = 0; i < 20; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    Status status = storage->append(fileName, data, length);
    auto end = std::chrono::high_resolution_clock::now();
    assert(status == Status::kSuccess);
    sum += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    storage->remove(fileName);
  }

  int duration = sum / 20;
  std::cout << "Average Time taken: " << duration << " millseconds." << std::endl;

  // calculate the upload speed in kBps
  double upload_speed_mbps = (length / 1024 / 1024) / (duration * 1.0 / 1000);

  std::cout << "File uploaded successfully!" << std::endl;
  std::cout << "Time taken: " << duration << " millseconds." << std::endl;
  std::cout << "Upload speed: " << upload_speed_mbps << " MB/s" << std::endl;

  delete storage;
}