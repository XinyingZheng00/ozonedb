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
  // Guard against callers that pass a null buffer with a positive size —
  // happens when a multi-writer race REMOVE's a log file between
  // checkReadMoreLog and the actual read, leaving `buffer == nullptr`
  // while the caller still believes the file has bytes. Without this
  // check the very first ReadVarint32 dereferences nullptr.
  if (buffer == nullptr) {
    return bufferSize == 0 ? Status::kSuccess : Status::kFailure;
  }
  google::protobuf::io::ArrayInputStream array_input(buffer, bufferSize);
  google::protobuf::io::CodedInputStream coded_input(&array_input);
  // Deserialize all messages
  while (static_cast<size_t>(coded_input.CurrentPosition()) < bufferSize) {
    uint32_t size = 0;
    // Bail on a malformed varint instead of using uninitialized `size`
    // (which would feed a garbage limit into the parser below).
    if (!coded_input.ReadVarint32(&size)) {
      std::cerr << getpid() << ":Failed to read varint prefix: bufferSize="
                << bufferSize << " pos=" << coded_input.CurrentPosition()
                << std::endl;
      return Status::kFailure;
    }
    // A length-prefixed message must fit in what's left of the buffer.
    // Without this, a corrupted prefix (e.g. from a partially-written
    // log) makes PushLimit accept a giant size and MergeFromCodedStream
    // can read past the buffer end before the parser notices.
    size_t remaining =
        bufferSize - static_cast<size_t>(coded_input.CurrentPosition());
    if (size > remaining) {
      std::cerr << getpid()
                << ":Varint prefix exceeds remaining bytes: size=" << size
                << " remaining=" << remaining << " bufferSize=" << bufferSize
                << std::endl;
      return Status::kFailure;
    }

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