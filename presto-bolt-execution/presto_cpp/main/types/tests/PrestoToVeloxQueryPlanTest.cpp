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
#include <gtest/gtest.h>

#include "presto_cpp/main/types/PrestoToBoltQueryPlan.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/types/HyperLogLogType.h"
#include "bolt/functions/prestosql/types/IPAddressType.h"
#include "bolt/functions/prestosql/types/IPPrefixType.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/functions/prestosql/types/UuidType.h"

using namespace facebook::presto;
using namespace bytedance::bolt;

namespace {
inline void validateSqlFunctionHandleParsing(
    const std::shared_ptr<facebook::presto::protocol::FunctionHandle>&
        functionHandle,
    std::vector<TypePtr> expectedRawInputTypes) {
  std::vector<TypePtr> actualRawInputTypes;
  TypeParser typeParser;
  auto sqlFunctionHandle =
      std::static_pointer_cast<protocol::SqlFunctionHandle>(functionHandle);
  facebook::presto::parseSqlFunctionHandle(
      sqlFunctionHandle, actualRawInputTypes, typeParser);
  EXPECT_EQ(expectedRawInputTypes.size(), actualRawInputTypes.size());
  for (int i = 0; i < expectedRawInputTypes.size(); i++) {
    EXPECT_EQ(*expectedRawInputTypes[i], *actualRawInputTypes[i]);
  }
}
} // namespace

class PrestoToBoltQueryPlanTest : public ::testing::Test {
 public:
  PrestoToBoltQueryPlanTest() {
    registerHyperLogLogType();
    registerIPAddressType();
    registerIPPrefixType();
    registerJsonType();
    registerTimestampWithTimeZoneType();
    registerUuidType();
  }
};

TEST_F(PrestoToBoltQueryPlanTest, parseSqlFunctionHandleWithZeroParam) {
  std::string str = R"(
      {
        "@type": "json_file",
        "functionId": "json_file.test.count;",
        "version": "1"
      }
    )";

  json j = json::parse(str);
  std::shared_ptr<facebook::presto::protocol::FunctionHandle> functionHandle =
      j;
  ASSERT_NE(functionHandle, nullptr);
  validateSqlFunctionHandleParsing(functionHandle, {});
}

TEST_F(PrestoToBoltQueryPlanTest, parseSqlFunctionHandleWithOneParam) {
  std::string str = R"(
          {
            "@type": "json_file",
            "functionId": "json_file.test.sum;tinyint",
            "version": "1"
          }
    )";

  json j = json::parse(str);
  std::shared_ptr<facebook::presto::protocol::FunctionHandle> functionHandle =
      j;
  ASSERT_NE(functionHandle, nullptr);

  std::vector<TypePtr> expectedRawInputTypes{TINYINT()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToBoltQueryPlanTest, parseSqlFunctionHandleWithMultipleParam) {
  std::string str = R"(
        {
          "@type": "json_file",
          "functionId": "json_file.test.avg;array(decimal(15, 2));varchar",
          "version": "1"
        }
    )";

  json j = json::parse(str);
  std::shared_ptr<facebook::presto::protocol::FunctionHandle> functionHandle =
      j;
  ASSERT_NE(functionHandle, nullptr);

  std::vector<TypePtr> expectedRawInputTypes{ARRAY(DECIMAL(15, 2)), VARCHAR()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToBoltQueryPlanTest, parseSqlFunctionHandleAllComplexTypes) {
  std::string str = R"(
        {
          "@type": "json_file",
          "functionId": "json_file.test.all_complex_types;row(map(hugeint, ipaddress), ipprefix);row(array(varbinary), timestamp, date, json, hyperloglog, timestamp with time zone, interval year to month, interval day to second);function(double, boolean);uuid",
          "version": "1"
        }
    )";

  json j = json::parse(str);
  std::shared_ptr<facebook::presto::protocol::FunctionHandle> functionHandle =
      j;
  ASSERT_NE(functionHandle, nullptr);

  std::vector<TypePtr> expectedRawInputTypes{
      ROW({MAP(HUGEINT(), IPADDRESS()), IPPREFIX()}),
      ROW(
          {ARRAY(VARBINARY()),
           TIMESTAMP(),
           DATE(),
           JSON(),
           HYPERLOGLOG(),
           TIMESTAMP_WITH_TIME_ZONE(),
           INTERVAL_YEAR_MONTH(),
           INTERVAL_DAY_TIME()}),
      FUNCTION({DOUBLE()}, BOOLEAN()),
      UUID()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}
