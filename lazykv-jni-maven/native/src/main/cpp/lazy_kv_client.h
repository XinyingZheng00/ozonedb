#ifndef YCSB_C_LAZY_KV_H_
#define YCSB_C_LAZY_KV_H_

#include "rpc.h"
#

const uint8_t KV_INSERT = 41;
const uint8_t KV_READ = 42;

class LazyKV {
  friend void rpc_cont_func(void* context, void* tag);

 public:
  LazyKV() : complete_(false) {}
  ~LazyKV() {}
  void Init(std::string client_uri, std::string wr_server_uri, std::string rd_server_uri, uint8_t phy_port, int msg_size);
  void Cleanup();

  // void Read(std::string const& key, std::vector<Field>& result);

  // void Scan(std::string const& key, int len, std::vector<std::string> const* fields,
  //             std::vector<std::vector<Field>>& result) {
  //   throw "Scan: function not implemented!";
  // }

  // void Update(std::string const& key, std::vector<Field>& values) {
  //   return Insert(key, values);
  // }

  // void Insert(std::string const& key, std::vector<Field>& values);

  void Delete(std::string const& key) { throw "Delete: function not implemented!"; }

  void ReadIdx(const uint64_t idx, std::string& data) { throw "ReadIdx: function not implemented!"; }

 private:
  /**
   * key format:
   * | len | key |
   */
  static size_t SerializeKey(std::string const& key, char* data);

  /**
   * value format:
   * | field0_len | field0 | value0_len | value0 |...
   */
  // static size_t SerializeRow(std::vector<Field> const& values, char* data);
  // static void DeserializeRow(std::vector<Field>& values, char const* p, char const* lim);
  void pollForRpcComplete();
  void notifyRpcComplete();

 protected:
  static erpc::Nexus* nexus_;
  erpc::Rpc<erpc::CTransport>* rpc_;
  int wr_session_num_;
  int rd_session_num_;
  erpc::MsgBuffer req_;
  erpc::MsgBuffer resp_;
  static std::atomic<uint8_t> global_rpc_id_;

  bool complete_;
};

#endif