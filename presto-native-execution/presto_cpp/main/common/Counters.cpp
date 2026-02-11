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

#include "presto_cpp/main/common/Counters.h"
#include "bolt/common/base/StatsReporter.h"

namespace facebook::presto {

void registerPrestoMetrics() {
  DEFINE_METRIC(
      kCounterDriverCPUExecutorQueueSize, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterDriverCPUExecutorLatencyMs, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterSpillerExecutorQueueSize, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterSpillerExecutorLatencyMs, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterHTTPExecutorLatencyMs, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumHTTPRequest, facebook::bolt::StatType::COUNT);
  DEFINE_METRIC(kCounterNumHTTPRequestError, facebook::bolt::StatType::COUNT);
  DEFINE_METRIC(kCounterHTTPRequestLatencyMs, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterHttpClientNumConnectionsCreated, facebook::bolt::StatType::SUM);
  DEFINE_METRIC(kCounterNumQueryContexts, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasks, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksBytesProcessed, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksRunning, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksFinished, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksCancelled, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksAborted, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksFailed, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumZombieBoltTasks, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumZombiePrestoTasks, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumTasksWithStuckOperator, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumCancelledTasksByStuckDriver, facebook::bolt::StatType::COUNT);
  DEFINE_METRIC(kCounterNumTasksDeadlock, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumTaskManagerLockTimeOut, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumQueuedDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumOnThreadDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumSuspendedDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForConsumerDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForSplitDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForProducerDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForJoinBuildDrivers,
      facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForJoinProbeDrivers,
      facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForMergeJoinRightSideDrivers,
      facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForMemoryDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForConnectorDrivers,
      facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumBlockedYieldDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumStuckDrivers, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterTotalPartitionedOutputBuffer, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterPartitionedOutputBufferGetDataLatencyMs,
      facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsUserCpuTimeMicros, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsSystemCpuTimeMicros, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsNumSoftPageFaults, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsNumHardPageFaults, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterOsNumVoluntaryContextSwitches, facebook::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterOsNumForcedContextSwitches, facebook::bolt::StatType::AVG);
  DEFINE_HISTOGRAM_METRIC(
      kCounterExchangeSourcePeakQueuedBytes,
      1l * 1024 * 1024 * 1024,
      0,
      62l * 1024 * 1024 * 1024, // max bucket value: 62GB
      50,
      90,
      95,
      99,
      100);
  DEFINE_METRIC(kCounterMemoryPushbackCount, facebook::bolt::StatType::COUNT);
  DEFINE_HISTOGRAM_METRIC(
      kCounterMemoryPushbackLatencyMs, 10'000, 0, 100'000, 50, 90, 99, 100);
  DEFINE_HISTOGRAM_METRIC(
      kCounterMemoryPushbackReductionBytes,
      100l * 1024 * 1024, // 100MB
      0,
      15l * 1024 * 1024 * 1024, // 15GB
      50,
      90,
      99,
      100);
  DEFINE_HISTOGRAM_METRIC(
      kCounterMemoryPushbackExpectedReductionBytes,
      100l * 1024 * 1024, // 100MB
      0,
      15l * 1024 * 1024 * 1024, // 15GB
      50,
      90,
      99,
      100);

  // NOTE: Metrics type exporting for thread pool executor counters are in
  // PeriodicTaskManager because they have dynamic names and report configs. The
  // following counters have their type exported there:
  // [
  //  kCounterThreadPoolNumThreadsFormat,
  //  kCounterThreadPoolNumActiveThreadsFormat,
  //  kCounterThreadPoolNumPendingTasksFormat,
  //  kCounterThreadPoolNumTotalTasksFormat,
  //  kCounterThreadPoolMaxIdleTimeNsFormat
  // ]

  // NOTE: Metrics type exporting for file handle cache counters are in
  // PeriodicTaskManager because they have dynamic names. The following counters
  // have their type exported there:
  // [
  //  kCounterHiveFileHandleCacheNumElementsFormat,
  //  kCounterHiveFileHandleCachePinnedSizeFormat,
  //  kCounterHiveFileHandleCacheCurSizeFormat,
  //  kCounterHiveFileHandleCacheNumAccumulativeHitsFormat,
  //  kCounterHiveFileHandleCacheNumAccumulativeLookupsFormat
  // ]
}

} // namespace facebook::presto
