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
      kCounterDriverCPUExecutorQueueSize, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterDriverCPUExecutorLatencyMs, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterSpillerExecutorQueueSize, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterSpillerExecutorLatencyMs, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterHTTPExecutorLatencyMs, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumHTTPRequest, bytedance::bolt::StatType::COUNT);
  DEFINE_METRIC(kCounterNumHTTPRequestError, bytedance::bolt::StatType::COUNT);
  DEFINE_METRIC(kCounterHTTPRequestLatencyMs, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterHttpClientNumConnectionsCreated, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(kCounterNumQueryContexts, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasks, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksBytesProcessed, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksRunning, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksFinished, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksCancelled, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksAborted, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumTasksFailed, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumZombieBoltTasks, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumZombiePrestoTasks, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumTasksWithStuckOperator, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumCancelledTasksByStuckDriver, bytedance::bolt::StatType::COUNT);
  DEFINE_METRIC(kCounterNumTasksDeadlock, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumTaskManagerLockTimeOut, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumQueuedDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumOnThreadDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumSuspendedDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForConsumerDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForSplitDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForProducerDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForJoinBuildDrivers,
      bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForJoinProbeDrivers,
      bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForMergeJoinRightSideDrivers,
      bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForMemoryDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterNumBlockedWaitForConnectorDrivers,
      bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumBlockedYieldDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterNumStuckDrivers, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterTotalPartitionedOutputBuffer, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterPartitionedOutputBufferGetDataLatencyMs,
      bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsUserCpuTimeMicros, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsSystemCpuTimeMicros, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsNumSoftPageFaults, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(kCounterOsNumHardPageFaults, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterOsNumVoluntaryContextSwitches, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterOsNumForcedContextSwitches, bytedance::bolt::StatType::AVG);
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
  DEFINE_METRIC(kCounterMemoryPushbackCount, bytedance::bolt::StatType::COUNT);
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

  /// ================== Memory Arbitrator Counters =================

  DEFINE_METRIC(kCounterArbitratorNumRequests, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(kCounterArbitratorNumAborted, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(kCounterArbitratorNumFailures, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(kCounterArbitratorQueueTimeUs, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(
      kCounterArbitratorArbitrationTimeUs, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(
      kCounterArbitratorNumShrunkBytes, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(
      kCounterArbitratorNumReclaimedBytes, bytedance::bolt::StatType::SUM);
  DEFINE_METRIC(
      kCounterArbitratorFreeCapacityBytes, bytedance::bolt::StatType::AVG);
  DEFINE_METRIC(
      kCounterArbitratorNonReclaimableAttempts, bytedance::bolt::StatType::SUM);
}

} // namespace facebook::presto
