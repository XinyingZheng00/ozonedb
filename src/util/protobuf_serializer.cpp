#include "protobuf_serializer.h"
namespace ozonedb {
namespace protobuf {

std::string serializeMessage(google::protobuf::Message const& message) {
  int const size = message.ByteSizeLong();
  int const prefix_size = google::protobuf::io::CodedOutputStream::VarintSize32(size);
  std::string buffer;
  buffer.resize(prefix_size + size);  // allocate exact size

  google::protobuf::io::ArrayOutputStream array_output(reinterpret_cast<void*>(buffer.data()), buffer.size());
  google::protobuf::io::CodedOutputStream coded_output(&array_output);

  coded_output.WriteVarint32(size);
  message.SerializeWithCachedSizesToArray(
      reinterpret_cast<google::protobuf::uint8*>(buffer.data()) + coded_output.ByteCount());

  return std::move(buffer);  // RVO or move
}

Status deserializeMessages(std::string const& buffer,
                           std::vector<google::protobuf::Message*>& messages,
                           std::function<google::protobuf::Message*()> const& messageFactory) {
  google::protobuf::io::ArrayInputStream array_input(buffer.data(), buffer.size());
  google::protobuf::io::CodedInputStream coded_input(&array_input);

  while (coded_input.CurrentPosition() < buffer.size()) {
    uint32_t size;
    if (!coded_input.ReadVarint32(&size)) {
      std::cerr << getpid() << ": Failed to read varint32 size\n";
      return Status::kFailure;
    }

    google::protobuf::io::CodedInputStream::Limit limit = coded_input.PushLimit(size);
    google::protobuf::Message* message = messageFactory();

    if (!message) {
      std::cerr << getpid() << ": Failed to create message: " << std::endl;
      return Status::kFailure;
    }

    if (!message->MergeFromCodedStream(&coded_input) || !coded_input.ConsumedEntireMessage()) {
      std::cerr << getpid() << ": Failed to parse message: " << buffer.size() << " "
                << message->GetDescriptor()->name() << std::endl;
      delete message;
      return Status::kFailure;
    }

    if (!message->IsInitialized()) {
      std::cerr << getpid() << ": Message missing required fields: "
                << message->InitializationErrorString() << std::endl;
      delete message;
      return Status::kFailure;
    }

    coded_input.PopLimit(limit);
    messages.push_back(message);
  }
  return Status::kSuccess;
}

}  // namespace protobuf
}  // namespace ozonedb