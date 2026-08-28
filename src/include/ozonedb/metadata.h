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
  // Ack a Corfu data append on the sequencer's answer (the fast path of
  // CorfuDBStorage::submitBatch). "false" / "0" makes every append wait
  // for the tailer instead; only for A/B measurement.
  bool corfu_fast_ack = true;

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

  // SSTable block cache capacity in bytes. Governs how much data the
  // LRU cache holds across all SSTables; when exceeded, least-recently-
  // used blocks are evicted and re-fetched on next read (an S3 GetObject
  // round-trip on the Corfu + object-store setup). Default 32 MB matches
  // the pre-config behavior; bump to hundreds of MB / GBs when the
  // working set is bigger than the default.
  uint64_t lru_cache_bytes = 33554432;
  // Largest ranged read that compaction issues for one SSTable input
  // (Table::getAll). The default equals level_file_size_limit, so one
  // input is one read; lower it to bound memory per in-flight compaction.
  uint64_t compaction_read_bytes = 67108864;

  // Compaction-aware block cache (bench/PLAN-compaction-cache.md, part B).
  // When a COMPACT is applied to this process's view, the outputs are
  // read once (compaction_read_bytes per ranged read) and published into
  // the block cache, so the peers of the compactor do not fetch them one
  // block per GET. Off by default; each rule below skips an output and
  // counts the skip in the `[lru_cache] levels` line.
  bool cache_warm_enabled = false;
  // Warm outputs whose level is at most this (L1 holds the newest
  // versions of the hot keys; the last level is the cold bulk).
  int cache_warm_max_level = 1;
  // Warm an output only if its size is at most this fraction of
  // lru_cache_bytes, so one file cannot flush the cache.
  double cache_warm_max_fraction = 0.25;
  // Warm only if this process held at least this many cached blocks of
  // the compaction's inputs: a process that never read the region does
  // not warm it, and a pure load warms nothing.
  uint64_t cache_warm_min_input_blocks = 1;

  // Disk-backed read-through tier for SSTables (bench/PLAN-disk-cache.md).
  // Empty dir = off. The dir gets a trailing '/'. Requires sstable_backend.
  std::string disk_cache_dir;
  uint64_t disk_cache_bytes = 0;
  uint64_t disk_cache_chunk_bytes = 67108864;
  bool disk_cache_drop_pages = true;
  uint64_t disk_cache_fill_queue = 256;

  // When true, log reads trust the in-memory key index (kept fresh by
  // local addRecord and, on Corfu, the tailer's remote-append listener)
  // and skip the synchronous log-file scan entirely. Removes the
  // fenced storage->size() round-trip on the active tail. Index miss
  // => log miss; the DB layer still consults SSTables as usual.
  bool trust_background_tail = false;

  // When true, every DB::get linearizes against the shared log: it
  // synchronously fences on the log's global tail and rolls the
  // metadata-log view forward before scanning, and the unfenced
  // key-index probe in LogHandler::readRecord is bypassed. This makes
  // reads strict (a get observes every put acked before it started, on
  // any writer process) at the cost of ONE fence per get: DB::get takes
  // a Storage::SyncScope (sequencer round-trip + tailer wait), and
  // every later storage call in the get reuses that thread-local fence
  // token, reading already-synced local state instead of re-fencing.
  // Mutually exclusive with trust_background_tail — the
  // constructor rejects configs that set both. The end-to-end guarantee
  // also assumes the write side keeps its sync defaults (every put is
  // sequenced in the log before it acks; see CorfuDBStorage::sync_mode_).
  bool linearizable_reads = false;

  // Log trimming (PLAN-trimming.md). When log_trim_enabled is true THIS
  // process runs a LogTrimmer: every log_trim_interval_ms it writes a
  // checkpoint of the live log state to sstable_storage under
  // <sstable_dir>/<checkpoint_dir>/ and trims the Corfu stream behind the
  // previous checkpoint. One process per cluster is enough. Every process,
  // trimmer or not, loads the newest checkpoint at open when one exists.
  // Requires backend = corfu and a separate sstable_backend: a checkpoint
  // written into the log being trimmed would be trimmed with it.
  bool log_trim_enabled = false;
  uint64_t log_trim_interval_ms = 30000;
  uint64_t log_trim_min_entries = 100000;
  int log_trim_keep_checkpoints = 2;
  std::string checkpoint_dir = "checkpoint";

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
      auto fast_it = result.find("corfu_fast_ack");
      if (fast_it != result.end()) {
        corfu_fast_ack = !(fast_it->second == "false" || fast_it->second == "0");
      }
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

    auto lru_it = result.find("lru_cache_bytes");
    if (lru_it != result.end()) {
      lru_cache_bytes = std::stoull(lru_it->second);
    }

    if (auto it = result.find("compaction_read_bytes"); it != result.end()) {
      compaction_read_bytes = std::stoull(it->second);
    }
    if (auto it = result.find("cache_warm_enabled"); it != result.end()) {
      cache_warm_enabled = (it->second == "true" || it->second == "1");
    }
    if (auto it = result.find("cache_warm_max_level"); it != result.end()) {
      cache_warm_max_level = std::stoi(it->second);
    }
    if (auto it = result.find("cache_warm_max_fraction"); it != result.end()) {
      cache_warm_max_fraction = std::stod(it->second);
    }
    if (auto it = result.find("cache_warm_min_input_blocks"); it != result.end()) {
      cache_warm_min_input_blocks = std::stoull(it->second);
    }

    if (auto it = result.find("disk_cache_dir"); it != result.end() && !it->second.empty()) {
      disk_cache_dir = it->second;
      if (disk_cache_dir.back() != '/') disk_cache_dir += '/';
    }
    if (auto it = result.find("disk_cache_bytes"); it != result.end()) {
      disk_cache_bytes = std::stoull(it->second);
    }
    if (auto it = result.find("disk_cache_chunk_bytes"); it != result.end()) {
      disk_cache_chunk_bytes = std::stoull(it->second);
    }
    if (auto it = result.find("disk_cache_drop_pages"); it != result.end()) {
      disk_cache_drop_pages = !(it->second == "false" || it->second == "0");
    }
    if (auto it = result.find("disk_cache_fill_queue"); it != result.end()) {
      disk_cache_fill_queue = std::stoull(it->second);
    }
    if (!disk_cache_dir.empty() && !sstable_backend_set) {
      throw std::runtime_error("disk_cache_dir requires sstable_backend: the tier fronts the object store that holds the SSTables");
    }
    if (!disk_cache_dir.empty() && disk_cache_bytes == 0) {
      throw std::runtime_error("disk_cache_dir requires disk_cache_bytes > 0");
    }

    auto tbt_it = result.find("trust_background_tail");
    if (tbt_it != result.end()) {
      trust_background_tail = (tbt_it->second == "true" || tbt_it->second == "1");
    }

    auto lin_it = result.find("linearizable_reads");
    if (lin_it != result.end()) {
      linearizable_reads = (lin_it->second == "true" || lin_it->second == "1");
    }
    if (linearizable_reads && trust_background_tail) {
      throw std::runtime_error(
          "config error: linearizable_reads and trust_background_tail are "
          "mutually exclusive (one demands a fence per get, the other skips "
          "it)");
    }

    auto lt_it = result.find("log_trim_enabled");
    if (lt_it != result.end()) {
      log_trim_enabled = (lt_it->second == "true" || lt_it->second == "1");
    }
    if (auto it = result.find("log_trim_interval_ms"); it != result.end()) {
      log_trim_interval_ms = std::stoull(it->second);
    }
    if (auto it = result.find("log_trim_min_entries"); it != result.end()) {
      log_trim_min_entries = std::stoull(it->second);
    }
    if (auto it = result.find("log_trim_keep_checkpoints"); it != result.end()) {
      log_trim_keep_checkpoints = std::stoi(it->second);
    }
    if (auto it = result.find("checkpoint_dir"); it != result.end() && !it->second.empty()) {
      checkpoint_dir = it->second;
    }
    if (log_trim_enabled &&
        (backend_kind != BackendKind::kCorfu || !sstable_backend_set)) {
      throw std::runtime_error(
          "config error: log_trim_enabled needs backend=corfu and a separate "
          "sstable_backend (the checkpoint must not live in the log it trims)");
    }
    if (log_trim_keep_checkpoints < 2) log_trim_keep_checkpoints = 2;

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