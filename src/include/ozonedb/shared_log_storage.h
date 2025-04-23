#ifndef SHARED_LOG_STORAGE_H
#define SHARED_LOG_STORAGE_H

#include "lazylog_cli.h"
#include "status.h"

namespace ozonedb {
class SharedLogStorage {
 private:
  std::unique_ptr<lazylog::LazyLogClient> shared_log;

 public:
  SharedLogStorage(int client_id = -1);
  ~SharedLogStorage();
  Status append(std::string const& data);
  Status read(std::vector<std::string>& entries, size_t from, size_t to);
  size_t size();
  bool exist();
};
}  // namespace ozonedb
#endif