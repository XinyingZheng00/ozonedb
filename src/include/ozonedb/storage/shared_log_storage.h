#ifndef SHARED_LOG_STORAGE_H
#define SHARED_LOG_STORAGE_H

#include "lazylog_cli.h"
#include "ozonedb_common.h"
#include "storage/storage.h"

namespace ozonedb {

class SharedLogStorage : public Storage {
 public:
  explicit SharedLogStorage(std::string path);
  ~SharedLogStorage() override;

  Status append(std::string const& offsetRange, google::protobuf::Message const& message, size_t& size) override;
  Status read(std::string const& offsetRange, size_t from, size_t to, std::function<google::protobuf::Message*()> const& messageFactory,
              std::vector<google::protobuf::Message*>& outMessages) override;
  Status read(std::string const& offsetRange, std::function<google::protobuf::Message*()> const& messageFactory,
              std::vector<google::protobuf::Message*>& outMessages) override;
  size_t size(std::string offsetRange) override;

 private:
  static std::atomic<int> global_client_id;

  std::unique_ptr<lazylog::LazyLogClient> shared_log;
  int local_client_id;
};
}  // namespace ozonedb
#endif