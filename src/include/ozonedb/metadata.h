#ifndef METADATA_H
#define METADATA_H

#include "ozonedb_common.h"
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

class Metadata {
 public:
  /**
   * @brief Path info for all files
   *
   */
  std::string container_name;  // only for cloud storage
  std::string DBdir;
  StorageType storage_type;
  std::string metadata_log_path = "metadata.log";
  std::string task_log_path = "task.log";
  std::string data_log_prefix = "sharedlog";
  std::string sstable_level_prefix = "sstable";
  std::string task_prefix;

  /**
   * @brief Limit info for all files
   *
   */
  uint64_t log_file_size_limit;
  uint64_t log_file_number_limit;
  uint64_t last_file_number_limit = 4;
  uint64_t block_cache_capacity;
  std::vector<uint64_t> level_size;
  std::vector<uint64_t> level_file_size_limit;

  int max_level;
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
    DBdir = result["db_path"];
    task_prefix = result["task_prefix"];
    log_file_size_limit = std::stol(result["log_file_size_limit"]);
    storage_type = static_cast<StorageType>(std::stoi(result["storage_type"]));
    if (storage_type == StorageType::kAzureBlobStorage) {
      container_name = result["container_name"];
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
    log_file_number_limit = std::stoi(result["log_file_number_limit"]);
    block_cache_capacity = std::stol(result["block_cache_capacity"]);
    compaction_policy = static_cast<CompactionPolicy>(std::stoi(result["compaction_policy"]));

    // std::cout<<shared_config_path<<std::endl;
    // std::cout << "DBpath: " << DBpath << std::endl;
    // std::cout << "max_log_file_size: " << max_log_file_size << std::endl;
    // std::cout << "log_file_number_limit: " << log_file_number_limit << std::endl;
    // std::cout << "max_level: " << max_level << std::endl;
    // std::cout << "policy: " << static_cast<int>(policy) << std::endl;
  };
};
}  // namespace ozonedb
#endif  // METADATA_H