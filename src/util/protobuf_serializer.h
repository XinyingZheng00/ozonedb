#ifndef OZONEDB_PROTOBUF_SERIALIZER_H
#define OZONEDB_PROTOBUF_SERIALIZER_H
#include "ozonedb_common.h"
#include "protobuf/record.pb.h"

namespace ozonedb {
namespace protobuf {
// unsigned char* serializeMessage(google::protobuf::Message const& message, int& buffer_size);

// Status deserializeMessages(unsigned char* buffer,
//                            size_t bufferSize,
//                            std::vector<google::protobuf::Message*>& messages,
//                            std::function<google::protobuf::Message*()> const& messageFactory);

std::string serializeMessage(google::protobuf::Message const& message);
Status deserializeMessages(std::string const& buffer,
                           std::vector<google::protobuf::Message*>& messages,
                           std::function<google::protobuf::Message*()> const& messageFactory);

}  // namespace protobuf
}  // namespace ozonedb
#endif  // OZONEDB_PROTOBUF_SERIALIZER_H