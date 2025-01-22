#include "protobuf_serializer.h"
namespace ozonedb {
namespace protobuf

{
unsigned char* serializeMessage(google::protobuf::Message const& message, int& buffer_length) {
  int size = message.ByteSizeLong();
  buffer_length = size + google::protobuf::io::CodedOutputStream::VarintSize32(size);
  auto* buffer = new unsigned char[buffer_length];
  google::protobuf::io::ArrayOutputStream array_output(buffer, buffer_length);
  google::protobuf::io::CodedOutputStream coded_output(&array_output);

  // Serialize the message
  coded_output.WriteVarint32(size);
  message.SerializeWithCachedSizesToArray(reinterpret_cast<google::protobuf::uint8*>(buffer) + coded_output.ByteCount());
  return buffer;
}

// Deserilize messages from storage
Status deserializeMessages(unsigned char* buffer,
                           size_t bufferSize,
                           std::vector<google::protobuf::Message*>& messages,
                           std::function<google::protobuf::Message*()> const& messageFactory) {
  google::protobuf::io::ArrayInputStream array_input(buffer, bufferSize);
  google::protobuf::io::CodedInputStream coded_input(&array_input);
  // Deserialize all messages
  while (coded_input.CurrentPosition() < bufferSize) {
    uint32_t size;
    coded_input.ReadVarint32(&size);

    google::protobuf::io::CodedInputStream::Limit limit = coded_input.PushLimit(size);
    google::protobuf::Message* message = messageFactory();
    if (!message->MergeFromCodedStream(&coded_input) || !coded_input.ConsumedEntireMessage()) {
      // print the name of messageFactory
      std::cerr << getpid() << ":Failed to parse message: " << bufferSize << " " << message->GetDescriptor()->name() << std::endl;
      delete message;
      message = nullptr;
      return Status::kFailure;
    }
    // Check if required fields are missing
    if (!message->IsInitialized()) {
      std::cerr << getpid() << ":Message missing required fields: "
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