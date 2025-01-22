#include "gtest/gtest.h"
#include "protobuf/record.pb.h"

TEST(ProtoBufTest, single_record_serialization) {
  Record* record = new Record();
  record->set_key("key");
  record->set_value("value");
  record->set_type(kTypeValue);
  std::string content;
  record->SerializeToString(&content);
  delete record;
  Record* record2 = new Record();
  record2->ParseFromString(content);
  EXPECT_EQ("key", record2->key());
  EXPECT_EQ("value", record2->value());
  EXPECT_EQ(kTypeValue, record2->type());
  delete record2;
}
