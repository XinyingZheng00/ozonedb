#ifndef YCSB_C_LAZY_KV_H_
#define YCSB_C_LAZY_KV_H_

#include "core/db.h"
#include "rpc.h"

namespace ycsbc {

const uint8_t KV_INSERT = 41;
const uint8_t KV_READ = 42;

class LazyKV : public DB {
  friend void rpc_cont_func(void *context, void *tag);

 public:
  LazyKV() : complete_(false) {}
  ~LazyKV() override {}
  void Init() override;
  void Cleanup() override;

  Status Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
              std::vector<Field> &result) override;

  Status Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
              std::vector<std::vector<Field>> &result) {
    throw "Scan: function not implemented!";
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return Insert(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) override;

  Status Delete(const std::string &table, const std::string &key) { throw "Delete: function not implemented!"; }

  Status ReadIdx(const uint64_t idx, std::string &data) override {}

 private:
  /**
   * key format:
   * | len | key |
   */
  static size_t SerializeKey(const std::string &key, char *data);

  /**
   * value format:
   * | field0_len | field0 | value0_len | value0 |...
   */
  static size_t SerializeRow(const std::vector<Field> &values, char *data);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  void pollForRpcComplete();
  void notifyRpcComplete();

 protected:
  static erpc::Nexus *nexus_;
  erpc::Rpc<erpc::CTransport> *rpc_;
  int wr_session_num_;
  int rd_session_num_;
  erpc::MsgBuffer req_;
  erpc::MsgBuffer resp_;
  static std::atomic<uint8_t> global_rpc_id_;

  bool complete_;
};

}  // namespace ycsbc

#endif