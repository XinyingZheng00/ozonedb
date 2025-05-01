#include "lazy_kv.h"

#include <iostream>


std::atomic<uint8_t> LazyKV::global_rpc_id_ = 0;
erpc::Nexus *LazyKV::nexus_ = nullptr;

const std::string PROP_LLKV_WR_SVR_URI = "kv_wr.server_uri";
const std::string PROP_LLKV_WR_SVR_URI_DEFAULT = "localhost:31870";

const std::string PROP_LLKV_RD_SVR_URI = "kv_rd.server_uri";
const std::string PROP_LLKV_RD_SVR_URI_DEFAULT = "localhost:31870";

const std::string PROP_LLKV_CLI_URI = "kv.client_uri";
const std::string PROP_LLKV_CLI_URI_DEFAULT = "localhost:31861";

const std::string PROP_MSG_SIZE = "msg.size";
const std::string PROP_MSG_SIZE_DEFAULT = "8192";

void cli_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}
void rpc_cont_func(void *context, void *_tag) { reinterpret_cast<LazyKV *>(context)->notifyRpcComplete(); }

void LazyKV::Init() {
  const utils::Properties &props = *props_;
  const std::string client_uri = props.GetProperty(PROP_LLKV_CLI_URI, PROP_LLKV_CLI_URI_DEFAULT);

  const std::string wr_server_uri = props.GetProperty(PROP_LLKV_WR_SVR_URI, PROP_LLKV_WR_SVR_URI_DEFAULT);
  const std::string rd_server_uri = props.GetProperty(PROP_LLKV_RD_SVR_URI, PROP_LLKV_RD_SVR_URI_DEFAULT);
  const uint8_t phy_port = std::stoi(props.GetProperty("erpc.phy_port", "0"));

  uint8_t rpc_id = global_rpc_id_.fetch_add(1);

  if (!nexus_) {
    nexus_ = new erpc::Nexus(client_uri);
  }

  rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, this, rpc_id, cli_sm_handler, phy_port);

  wr_session_num_ = rpc_->create_session(wr_server_uri, rpc_id + 1);
  rd_session_num_ = rpc_->create_session(rd_server_uri, rpc_id + 1);

  while (!rpc_->is_connected(wr_session_num_) || !rpc_->is_connected(rd_session_num_)) {
    // sleep(1);
    rpc_->run_event_loop_once();
  }
  std::cout << "eRPC client " << (int)rpc_id << " connected to write server " << wr_server_uri << " and read server "
            << rd_server_uri << std::endl;

  const int msg_size = std::stoull(props.GetProperty(PROP_MSG_SIZE, PROP_MSG_SIZE_DEFAULT));
  req_ = rpc_->alloc_msg_buffer_or_die(msg_size);
  resp_ = rpc_->alloc_msg_buffer_or_die(msg_size);
}

void LazyKV::Cleanup() {
  rpc_->free_msg_buffer(req_);
  rpc_->free_msg_buffer(resp_);
  delete rpc_;
  if (global_rpc_id_.fetch_sub(1) == 1) {  // todo: this assumes Cleanup() and Init() on different threads will
                                           // never overlap
    delete nexus_;                         // at this moment, all rpc objects must have been freed
    nexus_ = nullptr;
  }
}

DB::Status LazyKV::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                        std::vector<Field> &result) {
  size_t k_size = SerializeKey(key, reinterpret_cast<char *>(req_.buf_));

  rpc_->resize_msg_buffer(&req_, k_size);
  rpc_->enqueue_request(rd_session_num_, KV_READ, &req_, &resp_, rpc_cont_func, nullptr);
  pollForRpcComplete();
  if (resp_.get_data_size() < sizeof(DB::Status)) return DB::kError;
  DB::Status status = *reinterpret_cast<DB::Status *>(resp_.buf_);
  if (status == DB::kOK) {
    DeserializeRow(result, reinterpret_cast<const char *>(resp_.buf_ + sizeof(DB::Status)),
                   reinterpret_cast<const char *>((resp_.buf_ + resp_.get_data_size())));
  }
  return status;
}

void LazyKV::pollForRpcComplete() {
  while (!complete_) rpc_->run_event_loop_once();
  complete_ = false;
}

void LazyKV::notifyRpcComplete() { complete_ = true; }

DB::Status LazyKV::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  size_t k_size = SerializeKey(key, reinterpret_cast<char *>(req_.buf_));
  size_t v_size = SerializeRow(values, reinterpret_cast<char *>(req_.buf_ + k_size));

  rpc_->resize_msg_buffer(&req_, k_size + v_size);
  rpc_->enqueue_request(wr_session_num_, KV_INSERT, &req_, &resp_, rpc_cont_func, nullptr);
  pollForRpcComplete();
  assert(resp_.get_data_size() == sizeof(DB::Status));
  return *reinterpret_cast<DB::Status *>(resp_.buf_);
}

size_t LazyKV::SerializeKey(const std::string &key, char *data) {
  uint32_t len = key.size();
  memcpy(data, reinterpret_cast<char *>(&len), sizeof(uint32_t));
  memcpy(data + sizeof(uint32_t), key.data(), key.size());
  return sizeof(uint32_t) + key.size();
}

size_t LazyKV::SerializeRow(const std::vector<Field> &values, char *data) {
  size_t offset = 0;
  for (const Field &field : values) {
    uint32_t len = field.first.size();
    memcpy(data + offset, reinterpret_cast<char *>(&len), sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(data + offset, field.first.data(), field.first.size());
    offset += field.first.size();
    len = field.second.size();
    memcpy(data + offset, reinterpret_cast<char *>(&len), sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(data + offset, field.second.data(), field.second.size());
    offset += field.second.size();
  }
  return offset;
}

void LazyKV::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

DB *NewLazyKV() { return new LazyKV; }

const bool registered = DBFactory::RegisterDB("lazykv", NewLazyKV);

