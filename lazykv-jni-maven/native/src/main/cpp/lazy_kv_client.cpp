#include "lazy_kv_client.h"
#include "utils/properties.h"
#include <iostream>
#include <fstream>
#include <thread>

namespace lazylog {

thread_local std::unique_ptr<LazyLogClient> LazyKV::thread_local_client_ = nullptr;
thread_local int LazyKV::thread_local_client_id_ = -1;
std::atomic<int> LazyKV::global_client_id_{1}; 

void LazyKV::playlog_func() {
  ll_cli_r = new LazyLogClient();
  prop.SetProperty("client_id", std::to_string(0));
  ll_cli_r->Initialize(prop);

  while (running) {
    LOG(INFO) << "Playlog thread is running";
    int tail = std::get<0>(ll_cli_r->GetTail());
    for (auto i = next_idx_; i < tail; i++) {
      std::string data;
      ll_cli_r->ReadEntry(i, data);
      std::string key;
      std::string value;
      uint8_t op;
      if (!Deserialize(data, key, value, op)) {
        LOG(ERROR) << "Failed to deserialize data";
        continue;
      }
      if (op == KV_INSERT) {
        kv_store_[key] = value;
      } else {
        LOG(ERROR) << "Unknown operation " << op;
      }
    }
    // LOG_IF(INFO, next_idx_ != tail) << "Playlog thread read " << tail - next_idx_ << " entries";
    next_idx_ = tail;
  }
  ll_cli_r->Finalize();
  delete ll_cli_r;
}

void LazyKV::Init(std::string be_path, std::string dl_client_path, std::string rdma_path) {
  running.store(true);
  std::ifstream be(be_path);
  prop.Load(be);
  std::ifstream dl_client(dl_client_path);
  prop.Load(dl_client);
  std::ifstream rdma(rdma_path);
  prop.Load(rdma);

  playlog_thread_ = new std::thread(&LazyKV::playlog_func, this);
}

void LazyKV::Cleanup() {
  LOG(INFO) << "Cleaning up LazyKV in cpp";
  running.store(false);
  if (playlog_thread_ && playlog_thread_->joinable()) {
    playlog_thread_->join();
  }
  delete playlog_thread_;
  playlog_thread_ = nullptr;
  LOG(INFO) << "Deleted playlog thread";
}

void LazyKV::Read(std::string const& key, std::string const*& value) {
  auto it = kv_store_.find(key);
  if (it != kv_store_.end()) {
    value = &(it->second);
    LOG(INFO) << "Found value for key: " << key << " = " << *value;
  } else {
    LOG(INFO) << "Cannot find the value for key";
  }
}

void LazyKV::Insert(std::string const& key, std::string value) {
  if (thread_local_client_ == nullptr) {
    thread_local_client_ = std::make_unique<LazyLogClient>();
    int my_id = global_client_id_.fetch_add(1);
    thread_local_client_id_ = my_id;
    prop.SetProperty("client_id", std::to_string(my_id));
    thread_local_client_->Initialize(prop);
    LOG(INFO) << "Thread " << std::this_thread::get_id() << " initialized client_id " << my_id;
  }

  uint8_t op = KV_INSERT;
  std::string buffer = Serialize(key, value, op);
  thread_local_client_->AppendEntryAll(buffer);
  LOG(INFO) << "Finish Insert: key=" << key << " thread_id=" << std::this_thread::get_id() << " client_id=" << thread_local_client_id_;
}

std::string LazyKV::Serialize(std::string const& key, std::string const& value, uint8_t op) {
  std::string buffer;

  uint32_t key_size = key.size();
  uint32_t value_size = value.size();

  buffer.reserve(sizeof(key_size) + key_size + sizeof(value_size) + value_size + sizeof(op));
  buffer.append(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
  buffer.append(key.data(), key_size);
  buffer.append(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
  buffer.append(value.data(), value_size);
  buffer.push_back(static_cast<char>(op));
  return std::move(buffer);
}

bool LazyKV::Deserialize(std::string const& buffer, std::string& key, std::string& value, uint8_t& op) {
  size_t offset = 0;

  if (buffer.size() < sizeof(uint32_t)) return false;
  uint32_t key_size = *reinterpret_cast<const uint32_t*>(buffer.data() + offset);
  offset += sizeof(uint32_t);
  if (buffer.size() < offset + key_size + sizeof(uint32_t)) return false;
  key.assign(buffer.data() + offset, key_size);
  offset += key_size;
  uint32_t value_size = *reinterpret_cast<const uint32_t*>(buffer.data() + offset);
  offset += sizeof(uint32_t);
  if (buffer.size() < offset + value_size + sizeof(uint8_t)) return false;
  value.assign(buffer.data() + offset, value_size);
  offset += value_size;
  op = static_cast<uint8_t>(*(buffer.data() + offset));
  return true;
}

}  // namespace lazylog
