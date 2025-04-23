#ifndef OZONEDB_LOG_HANDLER_SHARED_LOG_H
#define OZONEDB_LOG_HANDLER_SHARED_LOG_H
#include "log_handler_base.h"
#include "shared_log_storage.h"

namespace ozonedb {
class log_handler_shared_log : public LogHandlerBase {
 public:
  log_handler_shared_log(int client_id = -1);
  Status addRecord(Record const& record) override;
  Status readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset) override;

 private:
  std::unique_ptr<SharedLogStorage> shared_log_storage = nullptr;
};

}  // namespace ozonedb
#endif