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
#include "presto_cpp/main/PrestoTask.h"
#include <gtest/gtest.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/time/Timer.h"

DECLARE_bool(bolt_memory_leak_check_enabled);

using namespace bytedance::bolt;
using namespace facebook::presto;

using facebook::presto::PrestoTaskId;

class PrestoTaskTest : public testing::Test {
  void SetUp() override {
    FLAGS_bolt_memory_leak_check_enabled = true;
  }
};

TEST_F(PrestoTaskTest, basicTaskId) {
  const std::string taskIdStr("20201107_130540_00011_wrpkw.1.2.3.4");
  PrestoTaskId id(taskIdStr);
  ASSERT_EQ(id.queryId(), "20201107_130540_00011_wrpkw");
  ASSERT_EQ(id.stageId(), 1);
  ASSERT_EQ(id.stageExecutionId(), 2);
  ASSERT_EQ(id.id(), 3);
  ASSERT_EQ(id.attemptNumber(), 4);
  ASSERT_EQ(id.toString(), taskIdStr);
}

TEST_F(PrestoTaskTest, malformedTaskId) {
  BOLT_ASSERT_THROW(PrestoTaskId(""), "Malformed task ID: ");
  BOLT_ASSERT_THROW(
      PrestoTaskId("20201107_130540_00011_wrpkw."),
      "Malformed task ID: 20201107_130540_00011_wrpkw.");
  BOLT_ASSERT_THROW(PrestoTaskId("q.1.2"), "Malformed task ID: q.1.2");
}

TEST_F(PrestoTaskTest, runtimeMetricConversion) {
  RuntimeMetric boltMetric;
  boltMetric.unit = RuntimeCounter::Unit::kBytes;
  boltMetric.sum = 101;
  boltMetric.count = 17;
  boltMetric.min = 62;
  boltMetric.max = 79;

  const std::string metricName{"my_name"};
  const auto prestoMetric = toRuntimeMetric(metricName, boltMetric);
  EXPECT_EQ(metricName, prestoMetric.name);
  EXPECT_EQ(protocol::RuntimeUnit::BYTE, prestoMetric.unit);
  EXPECT_EQ(boltMetric.sum, prestoMetric.sum);
  EXPECT_EQ(boltMetric.count, prestoMetric.count);
  EXPECT_EQ(boltMetric.max, prestoMetric.max);
  EXPECT_EQ(boltMetric.min, prestoMetric.min);
}

TEST_F(PrestoTaskTest, basic) {
  PrestoTask task{"20201107_130540_00011_wrpkw.1.2.3.4", "node2", 0};

  // Test coordinator heartbeat.
  EXPECT_EQ(task.timeSinceLastCoordinatorHeartbeatMs(), 0);
  task.updateCoordinatorHeartbeat();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_GE(task.timeSinceLastCoordinatorHeartbeatMs(), 100);
}
