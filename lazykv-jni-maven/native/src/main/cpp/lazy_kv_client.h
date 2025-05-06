#ifndef LAZY_KV_H_
#define LAZY_KV_H_

#include "client/lazylog_cli.h"
#include <absl/container/flat_hash_map.h>
#include <atomic>

namespace lazylog {

struct ClientWrapper {
  LazyLogClient* client;
  std::atomic<bool> is_busy;
};

class LazyKV {
 public:
  LazyKV() {}
  ~LazyKV() {}

  void Init(std::string be_path, std::string dl_client_path, std::string rdma_path);
  void Cleanup();
  void Read(std::string const& key, std::string const*& value);
  void Insert(std::string const& key, std::string value);
  void Update(std::string const& key, std::string value) { Insert(key, value); };
  void Delete(std::string const& key) { throw "Delete: function not implemented!"; }

 private:
  Properties prop;
  std::vector<std::shared_ptr<ClientWrapper>> ll_cli_w_pool_;
  std::mutex rw_mutex_;
  LazyLogClient* ll_cli_r = nullptr;

  std::thread* playlog_thread_ = nullptr;
  uint64_t next_idx_ = 0;

  bool running;
  absl::flat_hash_map<std::string, std::string> kv_store_;

  void playlog_func();
  std::string Serialize(std::string const& key, std::string const& value, uint8_t op);
  std::string DeserializeKey(const char* data);
  bool Deserialize(std::string const& buffer, std::string& key, std::string& value, uint8_t& op);
  std::shared_ptr<ClientWrapper> get_available_client();
  };
}  // namespace lazylog
#endif  // LAZY_KV_H_