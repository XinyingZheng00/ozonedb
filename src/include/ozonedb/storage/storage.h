#ifndef STORAGE_H
#define STORAGE_H
#include "ozonedb_common.h"
#include "protobuf/record.pb.h"
#include "protobuf_serializer.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
namespace ozonedb {

class Storage {
 protected:
  std::string path;

 public:
  /**
   * for FileSystemStorage, it is the base directory where all the files are stored
   * for SharedStorage, it is the type of storage (e.g. DataLog, MetadataLog, TaskLog)
   */
  explicit Storage(std::string path) {
    this->path = path;
  };
  virtual ~Storage() = default;

  virtual Status append(std::string const& key, google::protobuf::Message const& message, size_t& size) = 0;

  /**
   * from and to are the offsets within the range
   */
  virtual Status read(std::string const& key, size_t from, size_t to, std::function<google::protobuf::Message*()> const& messageFactory,
                      std::vector<google::protobuf::Message*>& outMessages) = 0;

  virtual Status read(std::string const& key, std::function<google::protobuf::Message*()> const& messageFactory,
                      std::vector<google::protobuf::Message*>& outMessages) = 0;

  virtual size_t size(std::string key) = 0;
};

}  // namespace ozonedb
#endif  // STORAGE_H