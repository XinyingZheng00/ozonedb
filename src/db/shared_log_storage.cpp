#include "shared_log_storage.h"
#include "utils/properties.h"

namespace ozonedb {
/*
sudo GLOG_minloglevel=1 -P /sharedfs/LazyLog-Artifact/cfg/be.prop -P /sharedfs/LazyLog-Artifact/cfg/dl_client.prop -P /sharedfs/LazyLog-Artifact/cfg/rdma.prop
*/
thread_local std::unique_ptr<lazylog::LazyLogClient> SharedLogStorage::shared_log = nullptr;
thread_local int SharedLogStorage::local_client_id = -1;
std::atomic<int> SharedLogStorage::global_client_id{1};

void SharedLogStorage::initSharedLogClient() {
  shared_log = std::make_unique<lazylog::LazyLogClient>();
  shared_log->Initialize(prop);
}

SharedLogStorage::SharedLogStorage(Type type, int client_id) {
  auto loadProperties = [this](std::string const& base_path) {
    std::ifstream be(base_path + "/be.prop");
    this->prop.Load(be);
    std::ifstream dl_client(base_path + "/dl_client.prop");
    this->prop.Load(dl_client);
    std::ifstream rdma(base_path + "/rdma.prop");
    this->prop.Load(rdma);
  };
  switch (type) {
    case Type::kDataLog:
      loadProperties("/sharedfs/LazyLog-Artifact/cfg_datalog");
      break;
    case Type::kMetadataLog:
      loadProperties("/sharedfs/LazyLog-Artifact/cfg_metadatalog");
      break;
    case Type::kTaskLog:
      loadProperties("/sharedfs/LazyLog-Artifact/cfg_tasklog");
      break;
  }
  int my_id = global_client_id.fetch_add(1);
  local_client_id = my_id;
  prop.SetProperty("client_id", std::to_string(my_id));
}

SharedLogStorage::~SharedLogStorage() = default;

Status SharedLogStorage::append(std::string const& data) {
  if (shared_log == nullptr) {
    initSharedLogClient();
  }
  shared_log->AppendEntryAll(data);
  return Status::kSuccess;
}

Status SharedLogStorage::read(std::vector<std::string>& entries, size_t from, size_t to) {
  if (shared_log == nullptr) {
    initSharedLogClient();
  }
  for (size_t i = from; i < to; ++i) {
    std::string entry;
    if (!shared_log->ReadEntry(i, entry)) {
      std::cerr << "ReadEntry failed" << std::endl;
      return Status::kFailure;
    }
    entries.push_back(entry);
  }
  return Status::kSuccess;
}

size_t SharedLogStorage::size() {
  /*
     * @return 0. durable tail: index of the newest unordered entry + 1,
     *  1. ordered tail: index of the oldest unordered entry (i.e. index of the newest ordered entry + 1)
     *  2. curent view number
     * What you should fetch is within [ tail[1], tail[0] )
    std::tuple<uint64_t, uint64_t, uint16_t> GetTail();
  */
  if (shared_log == nullptr) {
    initSharedLogClient();
  }
  std::tuple<uint64_t, uint64_t, uint16_t> tail = shared_log->GetTail();
  uint64_t last_index = std::get<0>(tail);
  return last_index;
}

bool SharedLogStorage::exist() {
  return true;
}

}  // namespace ozonedb