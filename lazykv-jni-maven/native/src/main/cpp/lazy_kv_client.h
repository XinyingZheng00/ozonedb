#ifndef LAZY_KV_H_
#define LAZY_KV_H_

#include "client/lazylog_cli.h"
#include <absl/container/flat_hash_map.h>
#include <atomic>
#include <thread>
#include <unordered_map>

namespace lazylog {

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
  std::atomic<bool> running;
  LazyLogClient* ll_cli_r = nullptr;
  std::thread* playlog_thread_ = nullptr;
  uint64_t next_idx_ = 0;
  absl::flat_hash_map<std::string, std::string> kv_store_;

  // NEW: per-thread client
  static thread_local std::unique_ptr<LazyLogClient> thread_local_client_;
  static thread_local int thread_local_client_id_;
  static std::atomic<int> global_client_id_;

  void playlog_func();
  std::string Serialize(std::string const& key, std::string const& value, uint8_t op);
  std::string DeserializeKey(char const* data);
  bool Deserialize(std::string const& buffer, std::string& key, std::string& value, uint8_t& op);
};

}  // namespace lazylog

#endif  // LAZY_KV_H_
