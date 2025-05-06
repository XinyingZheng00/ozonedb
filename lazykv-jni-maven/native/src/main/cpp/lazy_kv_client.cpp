#include "lazy_kv_client.h"
#include "utils/properties.h"
#include <iostream>
#include <thread>

namespace lazylog {

void LazyKV::playlog_func() {
  ll_cli_r = new LazyLogClient();
  ll_cli_r->Initialize(prop);
  while (running) {
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
        LOG(ERROR) << "Unknown operation" << op;
      }
    }
    // LOG_IF(INFO, next_idx_ != tail) << "Playlog thread read " << tail - next_idx_ << " entries";
    next_idx_ = tail;
  }
  ll_cli_r->Finalize();
  delete ll_cli_r;
}

std::shared_ptr<ClientWrapper> LazyKV::get_available_client() {
  std::lock_guard<std::mutex> lock(rw_mutex_);
  for (auto& wrapper : ll_cli_w_pool_) {
    bool expected = false;
    if (wrapper->is_busy.compare_exchange_strong(expected, true)) {
      return wrapper;
    }
  }
  return nullptr;
}

void LazyKV::Init(std::string be_path, std::string dl_client_path, std::string rdma_path) {
  running = true;
  std::ifstream be(be_path);
  prop.Load(be);
  std::ifstream dl_client(dl_client_path);
  prop.Load(dl_client);
  std::ifstream rdma(rdma_path);
  prop.Load(rdma);

  unsigned int num_threads = std::thread::hardware_concurrency();
  for (int i = 0; i < num_threads -1; i++) {
    auto wrapper = std::make_shared<ClientWrapper>();
    wrapper->client = new LazyLogClient();
    wrapper->is_busy = false;
    wrapper->client->Initialize(prop);
    ll_cli_w_pool_.push_back(wrapper);  
  }
  playlog_thread_ = new std::thread(&LazyKV::playlog_func, this);
}

void LazyKV::Cleanup() {
  running = false;
  playlog_thread_->join();
  delete playlog_thread_;

  for (auto& wrapper : ll_cli_w_pool_) {
    wrapper->client->Finalize();
    delete wrapper->client;
  }
  ll_cli_w_pool_.clear();
}

void LazyKV::Read(std::string const& key, std::string const*& value) {
  auto it = kv_store_.find(key);
  if (it != kv_store_.end()) {
    value = &(it->second);
    std::cout << "Found the value for key: " << key << " = " << *value << std::endl;
  } else {
    std::cout << "Cannot find the value for specific keys" << std::endl;
  }
}

void LazyKV::Insert(std::string const& key, std::string value) {
  uint8_t op = KV_INSERT;
  std::string buffer = Serialize(key, value, op);
  std::shared_ptr<ClientWrapper> wrapper = get_available_client();
  if (wrapper == nullptr) {
    throw std::runtime_error("No available RW client!");
  }
  wrapper->client->AppendEntryAll(buffer);
  wrapper->is_busy.store(false);
}

std::string LazyKV::Serialize(std::string const& key, std::string const& value, uint8_t op) {
  std::string buffer;

  uint32_t key_size = key.size();
  uint32_t value_size = value.size();

  buffer.reserve(sizeof(key_size) + key_size + sizeof(value_size) + value_size + sizeof(op));
  buffer.append(reinterpret_cast<char const*>(&key_size), sizeof(key_size));
  buffer.append(key.data(), key_size);
  buffer.append(reinterpret_cast<char const*>(&value_size), sizeof(value_size));
  buffer.append(value.data(), value_size);
  buffer.push_back(static_cast<char>(op));
  return buffer;
}

bool LazyKV::Deserialize(std::string const& buffer, std::string& key, std::string& value, uint8_t& op) {
  size_t offset = 0;

  if (buffer.size() < sizeof(uint32_t)) return false;
  uint32_t key_size = *reinterpret_cast<uint32_t const*>(buffer.data() + offset);
  offset += sizeof(uint32_t);
  if (buffer.size() < offset + key_size + sizeof(uint32_t)) return false;
  key.assign(buffer.data() + offset, key_size);
  offset += key_size;
  uint32_t value_size = *reinterpret_cast<uint32_t const*>(buffer.data() + offset);
  offset += sizeof(uint32_t);
  if (buffer.size() < offset + value_size + sizeof(uint8_t)) return false;
  value.assign(buffer.data() + offset, value_size);
  offset += value_size;
  op = static_cast<uint8_t>(*(buffer.data() + offset));
  return true;
}

}  // namespace lazylog