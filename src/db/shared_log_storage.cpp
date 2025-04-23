#include "shared_log_storage.h"
#include "utils/properties.h"

namespace ozonedb {
/*
sudo GLOG_minloglevel=1 -P /sharedfs/LazyLog-Artifact/cfg/be.prop -P /sharedfs/LazyLog-Artifact/cfg/dl_client.prop -P /sharedfs/LazyLog-Artifact/cfg/rdma.prop
*/

SharedLogStorage::SharedLogStorage(int client_id) {
  lazylog::Properties prop;
  std::ifstream be("/sharedfs/LazyLog-Artifact/cfg/be.prop");
  prop.Load(be);
  std::ifstream dl_client("/sharedfs/LazyLog-Artifact/cfg/dl_client.prop");
  prop.Load(dl_client);
  std::ifstream rdma("/sharedfs/LazyLog-Artifact/cfg/rdma.prop");
  prop.Load(rdma);
  if (client_id != -1) {
    prop.SetProperty("dur_log.client_id", std::to_string(client_id));
  }
  shared_log = std::make_unique<lazylog::LazyLogClient>();
  shared_log->Initialize(prop);
}

SharedLogStorage::~SharedLogStorage() = default;

Status SharedLogStorage::append(std::string const& data) {
  shared_log->AppendEntryAll(data);
  return Status::kSuccess;
}

Status SharedLogStorage::read(std::vector<std::string>& entries, size_t from, size_t to) {
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
  std::tuple<uint64_t, uint64_t, uint16_t> tail = shared_log->GetTail();
  uint64_t last_index = std::get<0>(tail);
  return last_index;
}

bool SharedLogStorage::exist() {
  return true;
}

}  // namespace ozonedb