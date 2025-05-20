#include "storage/shared_log_storage.h"
#include "helper.h"
#include "utils/properties.h"

namespace ozonedb {
/*
sudo GLOG_minloglevel=1 -P /sharedfs/LazyLog-Artifact/cfg/be.prop -P /sharedfs/LazyLog-Artifact/cfg/dl_client.prop -P /sharedfs/LazyLog-Artifact/cfg/rdma.prop
*/

std::atomic<int> SharedLogStorage::global_client_id{1};

SharedLogStorage::SharedLogStorage(std::string path) : Storage(path) {
  lazylog::Properties prop;
  auto loadProperties = [&prop](std::string const& base_path) {
    std::ifstream be(base_path + "/be.prop");
    prop.Load(be);
    std::ifstream dl_client(base_path + "/dl_client.prop");
    prop.Load(dl_client);
    std::ifstream rdma(base_path + "/rdma.prop");
    prop.Load(rdma);
  };
  if (path == "datalog") {
    loadProperties("/sharedfs/LazyLog-Artifact/cfg_datalog");
  } else if (path == "metadatalog") {
    loadProperties("/sharedfs/LazyLog-Artifact/cfg_metadatalog");
  } else if (path == "tasklog") {
    loadProperties("/sharedfs/LazyLog-Artifact/cfg_tasklog");
  } else {
    std::cerr << "Invalid path for SharedLogStorage" << std::endl;
    throw std::invalid_argument("Invalid path for SharedLogStorage");
  }
  int my_id = global_client_id.fetch_add(1);
  local_client_id = my_id;
  prop.SetProperty("client_id", std::to_string(my_id));
  shared_log = std::make_unique<lazylog::LazyLogClient>();
  shared_log->Initialize(prop);
}

SharedLogStorage::~SharedLogStorage() {
  shared_log->Finalize();
};

Status SharedLogStorage::append(std::string const& offsetRange, google::protobuf::Message const& message, size_t& size) {
  std::string data = message.SerializeAsString();
  shared_log->AppendEntryAll(data);
  size = data.size();
  return Status::kSuccess;
}

Status SharedLogStorage::read(std::string const& offsetRange, size_t from, size_t to, std::function<google::protobuf::Message*()> const& messageFactory,
                              std::vector<google::protobuf::Message*>& outMessages) {
  int start_offset = getStartOffset(offsetRange);
  from += start_offset;
  to += start_offset;
  for (size_t i = from; i < to; ++i) {
    std::string entry;
    if (!shared_log->ReadEntry(i, entry)) {
      std::cerr << "ReadEntry failed" << std::endl;
      return Status::kFailure;
    }
    auto* record = messageFactory();
    record->ParseFromString(entry);
    outMessages.push_back(record);
  }
  return Status::kSuccess;
}

Status SharedLogStorage::read(std::string const& offsetRange, std::function<google::protobuf::Message*()> const& messageFactory,
                              std::vector<google::protobuf::Message*>& outMessages) {
  int start_offset = getStartOffset(offsetRange);
  int end_offset = getEndOffset(offsetRange);
  for (size_t i = start_offset; i < end_offset; ++i) {
    std::string entry;
    if (!shared_log->ReadEntry(i, entry)) {
      std::cerr << "ReadEntry failed" << std::endl;
      return Status::kFailure;
    }
    auto* record = messageFactory();
    record->ParseFromString(entry);
    outMessages.push_back(record);
  }
  return Status::kSuccess;
}

size_t SharedLogStorage::size(std::string offsetRange) {
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

}  // namespace ozonedb