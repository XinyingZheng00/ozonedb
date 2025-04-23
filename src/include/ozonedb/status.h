#ifndef OZONEDB_STATUS_H
#define OZONEDB_STATUS_H

namespace ozonedb {
enum class Status { kSuccess,
                    kFailure,
                    kSealed,
                    kNotFound };
}
#endif