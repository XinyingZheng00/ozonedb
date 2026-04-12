#ifndef S3_STORAGE_H
#define S3_STORAGE_H
#ifdef OZONEDB_ENABLE_S3
#include "storage.h"
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ozonedb {

/**
 * @brief Storage backend backed by an S3-compatible object store.
 *
 * Designed to talk to any service that speaks the S3 API — real AWS S3,
 * MinIO, Cloudflare R2, Wasabi, Backblaze B2, SeaweedFS — by configuring
 * the endpoint override and path-style addressing. The paper's §3.5
 * regular-storage tier for SSTables is the intended use case: SSTables
 * are immutable once written, read by random byte range, and too bulky
 * for the atomic-append log layer.
 *
 * Write path: `appendInBatch` / `appendNoFlush` accumulate bytes into an
 * in-memory buffer keyed by filename. `flush` does one `PutObject` with
 * the entire buffered blob. `append` is backed by the same buffer +
 * immediate flush path (kept for API compatibility). S3 has no native
 * append primitive, which is fine — callers that need atomic append
 * (log / metadata / task logs) should use a different backend.
 *
 * Read path: `read(name, data, size)` is a full `GetObject`. The
 * ranged variant `read(name, data, offset, length)` uses `GetObject`
 * with a `Range` header. `size(name)` is `HeadObject` → Content-Length.
 *
 * `seal` / `isSealed` track state locally (S3 objects are immutable by
 * construction once PUT, so there is no server-side "seal" marker).
 */
class S3Storage : public Storage {
 public:
  S3Storage(std::string const& endpoint,
            std::string const& region,
            std::string const& bucket,
            std::string const& access_key,
            std::string const& secret_key,
            bool use_path_style,
            std::string const& db_path);
  ~S3Storage() override;

  void createDirectory(std::string name) override;
  Status append(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendNoFlush(std::string const& fileName, unsigned char* const& data, int length) override;
  Status appendInBatch(std::string const& fileName, unsigned char* const& data, int length) override;
  Status flush(std::string const& fileName) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t& size) override;
  Status read(std::string const& fileName, unsigned char*& data, size_t a, size_t length) override;
  size_t size(std::string fileName) override;
  void seal(std::string fileName) override;
  bool isSealed(std::string fileName) override;
  void remove(std::string fileName) override;
  bool exist(std::string fileName) override;

 private:
  // Form the full S3 key (storage_path is the db-path prefix inside the bucket).
  std::string keyFor(std::string const& fileName) const;

  // Flush the buffered bytes for this file as one PutObject. Caller must
  // not hold mtx_ — this function takes it.
  Status flushLocked(std::string const& fileName);

  std::unique_ptr<Aws::S3::S3Client> client_;
  std::string bucket_;
  Aws::SDKOptions aws_options_;
  bool owns_aws_sdk_ = false;

  std::mutex mtx_;
  // Same role as AzureBlobStorage::cached_file — per-file write buffer.
  std::unordered_map<std::string, std::vector<unsigned char>> cached_file_;
  // Sealed-locally set. S3 objects are immutable once PUT, so this is
  // really "we have called seal() on this name locally" and is used to
  // return kSealed from future append attempts per the Storage contract.
  std::unordered_set<std::string> sealed_files_;
};

}  // namespace ozonedb
#endif  // OZONEDB_ENABLE_S3
#endif  // S3_STORAGE_H
