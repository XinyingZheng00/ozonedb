#ifndef OZONEDB_LOG_HANDLER_BASE_H
#define OZONEDB_LOG_HANDLER_BASE_H

#include "protobuf/record.pb.h"
#include "status.h"

namespace ozonedb {

class LogHandlerBase {
 public:
  virtual Status addRecord(Record const& record) { throw std::runtime_error("addRecord() is not implemented"); };
  virtual Status readRecord(std::string const& key, Record*& record, std::string const& offset, std::string& latest_offset) { throw std::runtime_error("readRecord() is not implemented"); };
};
}  // namespace ozonedb
#endif