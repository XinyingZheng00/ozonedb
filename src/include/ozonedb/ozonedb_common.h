#ifndef OZONEDB_COMMON_H
#define OZONEDB_COMMON_H

namespace ozonedb {
enum class Status { kSuccess,
                    kFailure,
                    kSealed,
                    kNotFound };

enum class StorageType {
  kFileStorage,
  kSharedLogStorage,
  kAzureBlobStorage,
};

}  // namespace ozonedb
#endif