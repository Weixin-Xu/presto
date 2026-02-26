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
#include "presto_cpp/main/types/PrestoToBoltConnector.h"
#include <gtest/gtest.h>
#include "bolt/common/base/tests/GTestUtils.h"

using namespace facebook::presto;
using namespace bytedance::bolt;

class PrestoToBoltConnectorTest : public ::testing::Test {};

TEST_F(PrestoToBoltConnectorTest, registerVariousConnectors) {
  std::vector<std::pair<std::string, std::unique_ptr<PrestoToBoltConnector>>>
      connectorList;
  connectorList.emplace_back(
      std::pair("hive", std::make_unique<HivePrestoToBoltConnector>("hive")));
  connectorList.emplace_back(std::pair(
      "hive-hadoop2",

      std::make_unique<HivePrestoToBoltConnector>("hive-hadoop2")));
  connectorList.emplace_back(
      std::pair("tpch", std::make_unique<HivePrestoToBoltConnector>("tpch")));

  for (auto& [connectorName, connector] : connectorList) {
    registerPrestoToBoltConnector(std::move(connector));
    EXPECT_EQ(
        connectorName,
        getPrestoToBoltConnector(connectorName).connectorName());
    unregisterPrestoToBoltConnector(connectorName);
  }
}

TEST_F(PrestoToBoltConnectorTest, addDuplicates) {
  constexpr auto kConnectorName = "hive";
  registerPrestoToBoltConnector(
      std::make_unique<HivePrestoToBoltConnector>(kConnectorName));
  BOLT_ASSERT_THROW(
      registerPrestoToBoltConnector(
          std::make_unique<HivePrestoToBoltConnector>(kConnectorName)),
      fmt::format("Connector {} is already registered", kConnectorName));
}
