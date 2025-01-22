#ifndef OZONEDB_PROTOBUF_SERIALIZER_H
#define OZONEDB_PROTOBUF_SERIALIZER_H
#include "protobuf/record.pb.h"
#include "storage.h"
namespace ozonedb {

namespace protobuf

{
unsigned char* serializeMessage(google::protobuf::Message const& message, int& buffer_size);

Status deserializeMessages(unsigned char* buffer,
                           size_t bufferSize,
                           std::vector<google::protobuf::Message*>& messages,
                           std::function<google::protobuf::Message*()> const& messageFactory);

}  // namespace protobuf
}  // namespace ozonedb
#endif  // OZONEDB_PROTOBUF_SERIALIZER_H