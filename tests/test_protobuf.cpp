#include "gtest/gtest.h"
#include "protobuf/record.pb.h"
#include "protobuf_serializer.h"

using namespace ozonedb;

class ProtoBufSerializerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize 10 test records with varied data
    records.resize(10);

    for (int i = 0; i < 10; i++) {
      records[i].set_key("key" + std::to_string(i));
      records[i].set_value("value" + std::to_string(i * i));         // varied values
      records[i].set_type(i % 2 == 0 ? kTypeValue : kTypeDeletion);  // alternate types
    }
  }

  std::vector<Record> records;
};

// Test single record serialization
TEST_F(ProtoBufSerializerTest, SingleRecordSerialization) {
  std::string serialized = protobuf::serializeMessage(records[0]);
  std::vector<google::protobuf::Message*> messages;

  Status status = protobuf::deserializeMessages(
      serialized,
      messages,
      []() { return new Record(); });

  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), 1);

  Record* deserialized = dynamic_cast<Record*>(messages[0]);
  ASSERT_NE(deserialized, nullptr);
  EXPECT_EQ(deserialized->key(), records[0].key());
  EXPECT_EQ(deserialized->value(), records[0].value());
  EXPECT_EQ(deserialized->type(), records[0].type());

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

// Test multiple records serialization
TEST_F(ProtoBufSerializerTest, MultipleRecordsSerialization) {
  // Serialize multiple records into one buffer
  std::string combined_buffer;
  for (auto const& record : records) {
    combined_buffer += protobuf::serializeMessage(record);
  }

  std::vector<google::protobuf::Message*> messages;
  Status status = protobuf::deserializeMessages(
      combined_buffer,
      messages,
      []() { return new Record(); });

  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), records.size());

  for (size_t i = 0; i < messages.size(); i++) {
    Record* deserialized = dynamic_cast<Record*>(messages[i]);
    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->key(), records[i].key());
    EXPECT_EQ(deserialized->value(), records[i].value());
    EXPECT_EQ(deserialized->type(), records[i].type());
  }

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

// Test corrupted data deserialization
TEST_F(ProtoBufSerializerTest, CorruptedDataDeserialization) {
  std::string serialized = protobuf::serializeMessage(records[0]);
  // Corrupt the data by truncating
  serialized = serialized.substr(0, serialized.size() - 1);

  std::vector<google::protobuf::Message*> messages;
  Status status = protobuf::deserializeMessages(
      serialized,
      messages,
      []() { return new Record(); });

  EXPECT_EQ(status, Status::kFailure);
  EXPECT_TRUE(messages.empty());
}

// Test empty buffer deserialization
TEST_F(ProtoBufSerializerTest, EmptyBufferDeserialization) {
  std::string empty_buffer;
  std::vector<google::protobuf::Message*> messages;

  Status status = protobuf::deserializeMessages(
      empty_buffer,
      messages,
      []() { return new Record(); });

  EXPECT_EQ(status, Status::kSuccess);
  EXPECT_TRUE(messages.empty());
}

// Test large record serialization
TEST_F(ProtoBufSerializerTest, LargeRecordSerialization) {
  Record large_record;
  large_record.set_key("large_key");
  // Create a large value (1MB)
  std::string large_value(1024 * 1024, 'x');
  large_record.set_value(large_value);
  large_record.set_type(kTypeValue);

  std::string serialized = protobuf::serializeMessage(large_record);
  std::vector<google::protobuf::Message*> messages;

  Status status = protobuf::deserializeMessages(
      serialized,
      messages,
      []() { return new Record(); });

  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(messages.size(), 1);

  Record* deserialized = dynamic_cast<Record*>(messages[0]);
  ASSERT_NE(deserialized, nullptr);
  EXPECT_EQ(deserialized->key(), "large_key");
  EXPECT_EQ(deserialized->value(), large_value);
  EXPECT_EQ(deserialized->type(), kTypeValue);

  // Cleanup
  for (auto* msg : messages) {
    delete msg;
  }
}

// Test invalid message factory
TEST_F(ProtoBufSerializerTest, InvalidMessageFactory) {
  std::string serialized = protobuf::serializeMessage(records[0]);
  std::vector<google::protobuf::Message*> messages;

  Status status = protobuf::deserializeMessages(
      serialized,
      messages,
      []() -> google::protobuf::Message* { return nullptr; }  // Invalid factory
  );

  EXPECT_EQ(status, Status::kFailure);
  EXPECT_TRUE(messages.empty());
}
