#ifndef SHARED_LOG_STORAGE_H
#define SHARED_LOG_STORAGE_H

#include "lazylog_cli.h"
#include "status.h"

namespace ozonedb {
enum class Type {
  kDataLog,
  kMetadataLog,
  kTaskLog
};

class SharedLogStorage {
 private:
  static thread_local std::unique_ptr<lazylog::LazyLogClient> shared_log;
  static thread_local int local_client_id;
  static std::atomic<int> global_client_id;
  lazylog::Properties prop;
  void initSharedLogClient();

 public:
  SharedLogStorage(Type type = Type::kDataLog, int client_id = -1);
  ~SharedLogStorage();
  Status append(std::string const& data);
  Status read(std::vector<std::string>& entries, size_t from, size_t to);
  size_t size();
  bool exist();
};
}  // namespace ozonedb
#endif