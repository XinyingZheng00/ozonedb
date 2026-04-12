#ifndef METADATA_H
#define METADATA_H

#include "protobuf/record.pb.h"
#include "read_json.h"
#include <string>
#include <unordered_map>
#include <vector>
namespace ozonedb {

enum class CompactionPolicy {
  kHoAl,  // each client identify tasks and execute them rightway.
  kHeAl,  // Distribute by the throughput of each client, the larger the throughput, the more tasks it will work on.
  kHoSe   // Distribute by the number of put operations.
};

enum class BackendKind {
  kLocal,
  kAzure,
  kCorfu,
  kS3
};

class Metadata {
 public:
  /**
   * @brief Path info for all files
   *
   */
  std::string container_name; // only for Azure blob storage
  std::string DBpath;
  BackendKind backend_kind = BackendKind::kLocal;
  int is_cloud = false;  // legacy flag, still populated from "cloud" for backward compat
  // Corfu backend settings
  std::string corfu_endpoint;
  std::string corfu_jar_path;
  std::string corfu_jvm_opts;
  std::string corfu_stream_name = "ozonedb";

  // Optional separate backend for SSTable storage. When unset, SSTables
  // share the main backend (backward-compat with all existing configs).
  // The paper's §3.5 regular-storage tier for SSTables is the motivation:
  // SSTables are immutable, don't need atomic append, and are a bad fit
  // for the Corfu shared log.
  bool sstable_backend_set = false;
  BackendKind sstable_backend_kind = BackendKind::kLocal;
  std::string sstable_dir;             // key prefix inside the S3/Azure bucket
  std::string sstable_container_name;  // Azure-only when sstable_backend = azure
  // S3 / MinIO / R2 / Wasabi / SeaweedFS settings (used when
  // sstable_backend_kind == kS3). Endpoint empty == real AWS endpoint.
  std::string s3_endpoint;
  std::string s3_region = "us-east-1";
  std::string s3_bucket;
  std::string s3_access_key;
  std::string s3_secret_key;
  bool s3_use_path_style = true;  // MinIO default; real S3 users flip explicitly
  std::string metadata_log = "metadata.log";
  std::string task_log = "task.log";
  std::string log_prefix = "datalog";
  std::string sstable_level_prefix = "sstable";
  std::string task_prefix;
  int mode = -1;

  /**
   * @brief Limit info for all files
   *
   */
  uint64_t log_file_size_limit;
  int max_level;
  uint64_t base_file_number_limit;
  uint64_t last_file_number_limit = 4;
  std::vector<uint64_t> level_size;
  std::vector<uint64_t> level_file_size_limit;

  CompactionPolicy compaction_policy;

  /**
   * @brief Local Metadata, read from local config file
   *
   */
  // int64_t max_memory_size;
  int compaction_input_file_number = 2;  // todo: number of files to be compacted

  /**
   * @brief Construct a new Metadata object
   *
   * read config files and store all the information into a this metadata object
   *
   * @param shared_config_path
   */
  Metadata(std::string const& shared_config_path) {
    // read metadata from shared storage
    std::map<std::string, std::string> result = parseJSON(shared_config_path);
    DBpath = result["db_path"];
    task_prefix = result["task_prefix"];
    log_file_size_limit = std::stol(result["log_file_size_limit"]);
    mode = std::stoi(result["mode"]);

    auto backend_it = result.find("backend");
    if (backend_it != result.end()) {
      std::string const& b = backend_it->second;
      if (b == "local") {
        backend_kind = BackendKind::kLocal;
      } else if (b == "azure") {
        backend_kind = BackendKind::kAzure;
      } else if (b == "corfu") {
        backend_kind = BackendKind::kCorfu;
      } else {
        throw std::runtime_error("Unknown backend in config: " + b);
      }
    } else {
      is_cloud = std::stoi(result["cloud"]);
      backend_kind = is_cloud ? BackendKind::kAzure : BackendKind::kLocal;
    }
    is_cloud = (backend_kind == BackendKind::kAzure) ? 1 : 0;

    if (backend_kind == BackendKind::kAzure) {
      container_name = result["container_name"];
    } else if (backend_kind == BackendKind::kCorfu) {
      corfu_endpoint = result["corfu_endpoint"];
      corfu_jar_path = result["corfu_jar_path"];
      auto opts_it = result.find("corfu_jvm_opts");
      if (opts_it != result.end()) corfu_jvm_opts = opts_it->second;
      auto stream_it = result.find("corfu_stream_name");
      if (stream_it != result.end()) corfu_stream_name = stream_it->second;
    }

    // Optional SSTable-specific backend. See paper §3.5 — SSTables don't
    // need the atomic-append primitive and are usually better served by a
    // regular object store than by the log backend. Absent => share the
    // main backend and all existing configs keep working.
    auto sb_it = result.find("sstable_backend");
    if (sb_it != result.end()) {
      sstable_backend_set = true;
      std::string const& sb = sb_it->second;
      if (sb == "local") {
        sstable_backend_kind = BackendKind::kLocal;
      } else if (sb == "azure") {
        sstable_backend_kind = BackendKind::kAzure;
        sstable_container_name = result.count("sstable_container_name")
                                     ? result["sstable_container_name"]
                                     : container_name;
      } else if (sb == "s3") {
        sstable_backend_kind = BackendKind::kS3;
        s3_endpoint   = result.count("s3_endpoint")   ? result["s3_endpoint"]   : "";
        s3_region     = result.count("s3_region")     ? result["s3_region"]     : "us-east-1";
        s3_bucket     = result["s3_bucket"];
        s3_access_key = result.count("s3_access_key") ? result["s3_access_key"] : "";
        s3_secret_key = result.count("s3_secret_key") ? result["s3_secret_key"] : "";
        s3_use_path_style = result.count("s3_use_path_style")
                                ? result["s3_use_path_style"] == "true"
                                : true;
      } else {
        throw std::runtime_error("Unknown sstable_backend in config: " + sb);
      }
      sstable_dir = result.count("sstable_dir") ? result["sstable_dir"] : "";
    }

    std::string level_size_str = result["level_size"];
    std::vector<std::string> level_size_ = jsonArrayToVector(level_size_str);
    for (auto const& size : level_size_) {
      level_size.push_back(std::stol(size));
    }
    std::string level_file_size_limit_str = result["level_file_size_limit"];
    std::vector<std::string> level_file_size_limit_ = jsonArrayToVector(level_file_size_limit_str);
    for (auto const& size : level_file_size_limit_) {
      level_file_size_limit.push_back(std::stol(size));
    }

    max_level = std::stoi(result["max_level"]);
    // assert(level_size.size() == max_level - 1);
    // assert(level_file_size_limit.size() == max_level - 1);

    assert(max_level >= 2);  // otherwise the initoutput logic need to be modified
    base_file_number_limit = std::stoi(result["base_file_number_limit"]);
    compaction_policy = static_cast<CompactionPolicy>(std::stoi(result["compaction_policy"]));

    // std::cout<<shared_config_path<<std::endl;
    // std::cout << "DBpath: " << DBpath << std::endl;
    // std::cout << "max_log_file_size: " << max_log_file_size << std::endl;
    // std::cout << "base_file_number_limit: " << base_file_number_limit << std::endl;
    // std::cout << "max_level: " << max_level << std::endl;
    // std::cout << "policy: " << static_cast<int>(policy) << std::endl;
  };
};
}  // namespace ozonedb
#endif  // METADATA_H