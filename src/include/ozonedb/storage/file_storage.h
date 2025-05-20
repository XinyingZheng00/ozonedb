#ifndef FILE_STORAGE_H
#define FILE_STORAGE_H

#include "storage/storage.h"
#include <unistd.h>  // For fsync()

namespace ozonedb {
class FileStorage : public Storage {
 public:
  explicit FileStorage(std::string path);
  ~FileStorage() override;

  Status append(std::string const& file_name, google::protobuf::Message const& message, size_t& size) override;
  Status read(std::string const& file_name, size_t from, size_t to, std::function<google::protobuf::Message*()> const& messageFactory,
              std::vector<google::protobuf::Message*>& outMessages) override;
  Status read(std::string const& file_name, std::function<google::protobuf::Message*()> const& messageFactory,
              std::vector<google::protobuf::Message*>& outMessages) override;
  size_t size(std::string fileName) override;

  Status appendNoFlush(std::string const& file_name, google::protobuf::Message const& message, size_t& size);
  void createDirectory(std::string dirName);
  Status flush(std::string const& fileName);
  void seal(std::string fileName);
  void remove(std::string fileName);
  bool exist(std::string fileName);

 private:
  void seek(std::ifstream& file, int position);
  bool isSealed(std::string fileName);
  int GetFileDescriptor(std::filebuf& filebuf);
  Status append(std::string const& fileName, std::string const& data);
  Status appendNoFlush(std::string const& fileName, std::string const& data);
  Status read(std::string const& fileName, std::string& data);
  Status read(std::string const& fileName, std::string& data, size_t offset, size_t length);
  std::ifstream* getReadStream(std::string const& name);
  std::ofstream* getWriteStream(std::string const& name);

  std::unordered_map<std::string, std::unique_ptr<std::ifstream>> read_streams;
  std::unordered_map<std::string, std::unique_ptr<std::ofstream>> write_streams;
  std::shared_mutex read_mtx;
  std::shared_mutex write_mtx;
};
}  // namespace ozonedb
#endif  // FILE_STORAGE_H
