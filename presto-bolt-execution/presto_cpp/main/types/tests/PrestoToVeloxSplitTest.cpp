/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "presto_cpp/main/types/PrestoToBoltSplit.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/types/PrestoToBoltConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"

using namespace bytedance::bolt;
using namespace facebook::presto;

namespace {
protocol::ScheduledSplit makeHiveScheduledSplit() {
  protocol::ScheduledSplit scheduledSplit;
  scheduledSplit.sequenceId = 111;
  scheduledSplit.planNodeId = "planNodeId-0";

  protocol::Split split;
  split.connectorId = "split.connectorId-0";
  auto hiveTransactionHandle =
      std::make_shared<protocol::hive::HiveTransactionHandle>();
  hiveTransactionHandle->uuid = "split.transactionHandle.uuid-0";
  split.transactionHandle = hiveTransactionHandle;

  auto hiveSplit = std::make_shared<protocol::hive::HiveSplit>();
  hiveSplit->fileSplit.path = "/file/path";
  hiveSplit->storage.storageFormat.inputFormat =
      "com.facebook.hive.orc.OrcInputFormat";
  hiveSplit->fileSplit.start = 0;
  hiveSplit->fileSplit.length = 100;

  split.connectorSplit = hiveSplit;
  scheduledSplit.split = split;
  return scheduledSplit;
}
} // namespace

class PrestoToBoltSplitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registerPrestoToBoltConnector(
        std::make_unique<HivePrestoToBoltConnector>("hive"));
  }

  void TearDown() override {
    unregisterPrestoToBoltConnector("hive");
  }
};

TEST_F(PrestoToBoltSplitTest, nullPartitionKey) {
  auto scheduledSplit = makeHiveScheduledSplit();
  auto hiveSplit = std::dynamic_pointer_cast<protocol::hive::HiveSplit>(
      scheduledSplit.split.connectorSplit);
  protocol::hive::HivePartitionKey partitionKey{"nullPartitionKey", nullptr};
  hiveSplit->partitionKeys.push_back(partitionKey);
  auto boltSplit = toBoltSplit(scheduledSplit);
  std::shared_ptr<connector::hive::HiveConnectorSplit> boltHiveSplit;
  ASSERT_NO_THROW({
    boltHiveSplit =
        std::dynamic_pointer_cast<connector::hive::HiveConnectorSplit>(
            boltSplit.connectorSplit);
  });
  ASSERT_EQ(
      hiveSplit->partitionKeys.size(), boltHiveSplit->partitionKeys.size());
  ASSERT_FALSE(
      boltHiveSplit->partitionKeys.at("nullPartitionKey").has_value());
}

TEST_F(PrestoToBoltSplitTest, customSplitInfo) {
  auto scheduledSplit = makeHiveScheduledSplit();
  auto& hiveSplit = static_cast<protocol::hive::HiveSplit&>(
      *scheduledSplit.split.connectorSplit);
  hiveSplit.fileSplit.customSplitInfo["foo"] = "bar";
  auto boltSplit = toBoltSplit(scheduledSplit);
  auto* boltHiveSplit =
      dynamic_cast<const connector::hive::HiveConnectorSplit*>(
          boltSplit.connectorSplit.get());
  ASSERT_TRUE(boltHiveSplit);
  ASSERT_EQ(boltHiveSplit->customSplitInfo.size(), 1);
  ASSERT_EQ(boltHiveSplit->customSplitInfo.at("foo"), "bar");
}

TEST_F(PrestoToBoltSplitTest, extraFileInfo) {
  auto scheduledSplit = makeHiveScheduledSplit();
  auto& hiveSplit = static_cast<protocol::hive::HiveSplit&>(
      *scheduledSplit.split.connectorSplit);
  hiveSplit.fileSplit.extraFileInfo =
      std::make_shared<std::string>(encoding::Base64::encode("quux"));
  auto boltSplit = toBoltSplit(scheduledSplit);
  auto* boltHiveSplit =
      dynamic_cast<const connector::hive::HiveConnectorSplit*>(
          boltSplit.connectorSplit.get());
  ASSERT_TRUE(boltHiveSplit);
  ASSERT_TRUE(boltHiveSplit->extraFileInfo);
  ASSERT_EQ(*boltHiveSplit->extraFileInfo, "quux");
}

TEST_F(PrestoToBoltSplitTest, serdeParameters) {
  auto scheduledSplit = makeHiveScheduledSplit();
  auto& hiveSplit = dynamic_cast<protocol::hive::HiveSplit&>(
      *scheduledSplit.split.connectorSplit);
  hiveSplit.storage.serdeParameters[dwio::common::SerDeOptions::kFieldDelim] =
      "\t";
  hiveSplit.storage
      .serdeParameters[dwio::common::SerDeOptions::kCollectionDelim] = ",";
  hiveSplit.storage.serdeParameters[dwio::common::SerDeOptions::kMapKeyDelim] =
      "|";

  auto boltSplit = toBoltSplit(scheduledSplit);
  auto* boltHiveSplit =
      dynamic_cast<const connector::hive::HiveConnectorSplit*>(
          boltSplit.connectorSplit.get());
  ASSERT_TRUE(boltHiveSplit);
  ASSERT_EQ(boltHiveSplit->serdeParameters.size(), 3);
  ASSERT_EQ(
      boltHiveSplit->serdeParameters.at(
          dwio::common::SerDeOptions::kFieldDelim),
      "\t");
  ASSERT_EQ(
      boltHiveSplit->serdeParameters.at(
          dwio::common::SerDeOptions::kCollectionDelim),
      ",");
  ASSERT_EQ(
      boltHiveSplit->serdeParameters.at(
          dwio::common::SerDeOptions::kMapKeyDelim),
      "|");
}

TEST_F(PrestoToBoltSplitTest, bucketConversion) {
  auto scheduledSplit = makeHiveScheduledSplit();
  auto& hiveSplit = static_cast<protocol::hive::HiveSplit&>(
      *scheduledSplit.split.connectorSplit);
  hiveSplit.tableBucketNumber = std::make_shared<int>(42);
  hiveSplit.bucketConversion =
      std::make_shared<protocol::hive::BucketConversion>();
  hiveSplit.bucketConversion->tableBucketCount = 4096;
  hiveSplit.bucketConversion->partitionBucketCount = 512;
  auto& column = hiveSplit.bucketConversion->bucketColumnHandles.emplace_back();
  column.name = "c0";
  column.hiveType = "bigint";
  column.typeSignature = "bigint";
  column.columnType = protocol::hive::ColumnType::REGULAR;
  auto boltSplit = toBoltSplit(scheduledSplit);
  const auto& boltHiveSplit =
      static_cast<const connector::hive::HiveConnectorSplit&>(
          *boltSplit.connectorSplit);
  ASSERT_TRUE(boltHiveSplit.bucketConversion.has_value());
  ASSERT_EQ(boltHiveSplit.bucketConversion->tableBucketCount, 4096);
  ASSERT_EQ(boltHiveSplit.bucketConversion->partitionBucketCount, 512);
  ASSERT_EQ(boltHiveSplit.bucketConversion->bucketColumnHandles.size(), 1);
  ASSERT_EQ(boltHiveSplit.infoColumns.at("$path"), hiveSplit.fileSplit.path);
  ASSERT_EQ(boltHiveSplit.infoColumns.at("$bucket"), "42");
  auto& boltColumn = boltHiveSplit.bucketConversion->bucketColumnHandles[0];
  ASSERT_EQ(boltColumn->name(), "c0");
  ASSERT_EQ(*boltColumn->dataType(), *BIGINT());
  ASSERT_EQ(*boltColumn->hiveType(), *BIGINT());
  ASSERT_EQ(
      boltColumn->columnType(),
      connector::hive::HiveColumnHandle::ColumnType::kRegular);
}
