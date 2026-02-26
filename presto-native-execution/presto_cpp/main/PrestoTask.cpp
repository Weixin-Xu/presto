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
#include <sys/resource.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/common/Utils.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
#include "bolt/exec/Operator.h"

using namespace bytedance::bolt;

namespace facebook::presto {

namespace {

#define TASK_STATS_SUM(taskStats, statsName, taskStatusSum)      \
  do {                                                           \
    for (int i = 0; i < taskStats.pipelineStats.size(); ++i) {   \
      auto& pipeline = taskStats.pipelineStats[i];               \
      for (auto j = 0; j < pipeline.operatorStats.size(); ++j) { \
        auto& op = pipeline.operatorStats[j];                    \
        (taskStatusSum) += op.statsName;                         \
      }                                                          \
    }                                                            \
  } while (0)

protocol::TaskState toPrestoTaskState(exec::TaskState state) {
  switch (state) {
    case exec::TaskState::kRunning:
      return protocol::TaskState::RUNNING;
    case exec::TaskState::kFinished:
      return protocol::TaskState::FINISHED;
    case exec::TaskState::kCanceled:
      return protocol::TaskState::CANCELED;
    case exec::TaskState::kFailed:
      return protocol::TaskState::FAILED;
    case exec::TaskState::kAborted:
      [[fallthrough]];
    default:
      return protocol::TaskState::ABORTED;
  }
}

protocol::ExecutionFailureInfo toPrestoError(std::exception_ptr ex) {
  try {
    rethrow_exception(ex);
  } catch (const BoltException& e) {
    return BoltToPrestoExceptionTranslator::translate(e);
  } catch (const std::exception& e) {
    return BoltToPrestoExceptionTranslator::translate(e);
  }
}

protocol::RuntimeUnit toPrestoRuntimeUnit(RuntimeCounter::Unit unit) {
  switch (unit) {
    case RuntimeCounter::Unit::kNanos:
      return protocol::RuntimeUnit::NANO;
    case RuntimeCounter::Unit::kBytes:
      return protocol::RuntimeUnit::BYTE;
    case RuntimeCounter::Unit::kNone:
      return protocol::RuntimeUnit::NONE;
    default:
      return protocol::RuntimeUnit::NONE;
  }
}

// Presto operator's node id sometimes is not equivalent to bolt's.
// So when reporting task stats, we need to parse node id back to presto's.
// For example, bolt's partitionedOutput operator would have "root." prefix.
std::string toPrestoPlanNodeId(const protocol::PlanNodeId& id) {
  if (FOLLY_LIKELY(id.find("root.") == std::string::npos)) {
    return id;
  }
  return id.substr(5);
}

// Presto has certain query stats logic depending on the operator names.
// To leverage this logic we need to supply Presto's operator names.
std::string toPrestoOperatorType(const std::string& operatorType) {
  if (operatorType == "MergeExchange") {
    return "MergeOperator";
  }
  if (operatorType == "Exchange") {
    return "ExchangeOperator";
  }
  if (operatorType == "TableScan") {
    return "TableScanOperator";
  }
  if (operatorType == "TableWrite") {
    return "TableWriterOperator";
  }
  return operatorType;
}

void setTiming(
    const CpuWallTiming& timing,
    int64_t& count,
    protocol::Duration& wall,
    protocol::Duration& cpu) {
  count = timing.count;
  wall = protocol::Duration(timing.wallNanos, protocol::TimeUnit::NANOSECONDS);
  cpu = protocol::Duration(timing.cpuNanos, protocol::TimeUnit::NANOSECONDS);
}

// Creates a protocol runtime metric object from a raw value.
static protocol::RuntimeMetric createProtocolRuntimeMetric(
    const std::string& name,
    int64_t value,
    protocol::RuntimeUnit unit = protocol::RuntimeUnit::NONE) {
  return protocol::RuntimeMetric{name, unit, value, 1, value, value};
}

// Updates a Bolt runtime metric in the unordered map.
static void addRuntimeMetric(
    std::unordered_map<std::string, RuntimeMetric>& runtimeMetrics,
    const std::string& name,
    const RuntimeMetric& metric) {
  auto it = runtimeMetrics.find(name);
  if (it != runtimeMetrics.end()) {
    it->second.merge(metric);
  } else {
    runtimeMetrics.emplace(name, metric);
  }
}

// Updates a Bolt runtime metric in the unordered map if the value is not 0.
static void addRuntimeMetricIfNotZero(
    std::unordered_map<std::string, RuntimeMetric>& runtimeMetrics,
    const std::string& name,
    uint64_t value) {
  if (value > 0) {
    auto boltMetric = RuntimeMetric(value, RuntimeCounter::Unit::kNone);
    addRuntimeMetric(runtimeMetrics, name, boltMetric);
  }
}

RuntimeMetric fromMillis(int64_t ms) {
  return RuntimeMetric{ms * 1'000'000, bytedance::bolt::RuntimeCounter::Unit::kNanos};
}

RuntimeMetric fromNanos(int64_t nanos) {
  return RuntimeMetric{nanos, bytedance::bolt::RuntimeCounter::Unit::kNanos};
}

// Utility to generate presto runtime stat name when translating bolt runtime
// stats over to presto.
std::string generateRuntimeStatName(
    const exec::OperatorStats& boltOperatorStats,
    const std::string& statName) {
  return fmt::format(
      "{}.{}.{}",
      boltOperatorStats.operatorType,
      boltOperatorStats.planNodeId,
      statName);
}

// Helper to convert Bolt-specific generic operator stats into Presto runtime
// stats.
struct OperatorStatsCollector {
  const exec::OperatorStats& boltOperatorStats;
  protocol::RuntimeStats& prestoOperatorStats;
  protocol::RuntimeStats& prestoTaskStats;

  void addIfNotZero(
      const std::string& name,
      int64_t value,
      protocol::RuntimeUnit unit = protocol::RuntimeUnit::NONE) {
    if (value == 0) {
      return;
    }

    add(name, value, unit);
  }

  void add(
      const std::string& name,
      int64_t value,
      protocol::RuntimeUnit unit = protocol::RuntimeUnit::NONE) {
    const std::string statName =
        generateRuntimeStatName(boltOperatorStats, name);
    auto prestoMetric = createProtocolRuntimeMetric(statName, value, unit);
    prestoOperatorStats.emplace(statName, prestoMetric);
    prestoTaskStats.emplace(statName, prestoMetric);
  }
};

// Add 'spilling' metrics from Bolt operator stats to Presto operator stats.
void addSpillingOperatorMetrics(OperatorStatsCollector& collector) {
  auto& op = collector.boltOperatorStats;

  collector.add("spilledBytes", op.spilledBytes, protocol::RuntimeUnit::BYTE);
  collector.add("spilledRows", op.spilledRows);
  collector.add("spilledPartitions", op.spilledPartitions);
  collector.add("spilledFiles", op.spilledFiles);
}

// Updates the operator runtime stats in 'prestoTaskStats' based on the presto
// task state and system config. For example, if the task is running, then we
// might skip reporting operator runtime stats to control the communication data
// size with the coordinator.
void updateOperatorRuntimeStats(
    protocol::TaskState state,
    protocol::TaskStats& prestoTaskStats) {
  if (SystemConfig::instance()->skipRuntimeStatsInRunningTaskInfo() &&
      !isFinalState(state)) {
    for (auto& pipelineStats : prestoTaskStats.pipelines) {
      for (auto& opStats : pipelineStats.operatorSummaries) {
        opStats.runtimeStats.clear();
      }
    }
    return;
  }

  static const std::vector<std::string> prefixToExclude{"running", "blocked"};
  for (auto& pipelineStats : prestoTaskStats.pipelines) {
    for (auto& opStats : pipelineStats.operatorSummaries) {
      for (const auto& prefix : prefixToExclude) {
        for (auto it = opStats.runtimeStats.begin();
             it != opStats.runtimeStats.end();) {
          if (it->first.find(prefix) != std::string::npos) {
            it = opStats.runtimeStats.erase(it);
          } else {
            ++it;
          }
        }
      }
    }
  }
}

// Updates the task runtime stats in 'prestoTaskStats' based on the presto
// task state and system config. For example, if the task is running, then we
// might skip reporting task runtime stats to control the communication data
// size with the coordinator.
void updateTaskRuntimeStats(
    protocol::TaskState state,
    const std::unordered_map<std::string, RuntimeMetric>& taskRuntimeStats,
    bool tryToSkipIfRunning,
    protocol::TaskStats& prestoTaskStats) {
  if (!tryToSkipIfRunning ||
      !SystemConfig::instance()->skipRuntimeStatsInRunningTaskInfo() ||
      isFinalState(state)) {
    for (const auto& stats : taskRuntimeStats) {
      prestoTaskStats.runtimeStats[stats.first] =
          toRuntimeMetric(stats.first, stats.second);
    }
  } else {
    prestoTaskStats.runtimeStats.clear();
  }
}

presto::protocol::DynamicFilterStats toPrestoDynamicFilterStats(
    const bytedance::bolt::exec::OperatorStats& boltOpStats) {
  presto::protocol::DynamicFilterStats dynamicFilterStats;
  for (const auto& nodeId : boltOpStats.dynamicFilterStats.producerNodeIds) {
    dynamicFilterStats.producerNodeIds.emplace_back(nodeId);
  }
  return dynamicFilterStats;
}
} // namespace

PrestoTask::PrestoTask(
    const std::string& taskId,
    const std::string& nodeId,
    long _startProcessCpuTime)
    : id(taskId),
      startProcessCpuTime{
          _startProcessCpuTime > 0 ? _startProcessCpuTime
                                   : util::getProcessCpuTimeNs()} {
  info.taskId = taskId;
  info.nodeId = nodeId;
}

void PrestoTask::updateHeartbeatLocked() {
  lastHeartbeatMs = bytedance::bolt::getCurrentTimeMs();
  info.lastHeartbeat = util::toISOTimestamp(lastHeartbeatMs);
}

void PrestoTask::updateCoordinatorHeartbeat() {
  std::lock_guard<std::mutex> l(mutex);
  updateCoordinatorHeartbeatLocked();
}

void PrestoTask::updateCoordinatorHeartbeatLocked() {
  lastCoordinatorHeartbeatMs = bytedance::bolt::getCurrentTimeMs();
}

uint64_t PrestoTask::timeSinceLastHeartbeatMs() const {
  std::lock_guard<std::mutex> l(mutex);
  if (lastHeartbeatMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - lastHeartbeatMs;
}

uint64_t PrestoTask::timeSinceLastCoordinatorHeartbeatMs() const {
  std::lock_guard<std::mutex> l(mutex);
  if (lastCoordinatorHeartbeatMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - lastCoordinatorHeartbeatMs;
}

void PrestoTask::recordProcessCpuTime() {
  if (processCpuTime_ > 0) {
    return;
  }

  processCpuTime_ = util::getProcessCpuTimeNs() - startProcessCpuTime;
}

protocol::TaskStatus PrestoTask::updateStatusLocked() {
  if (!taskStarted && (error == nullptr)) {
    protocol::TaskStatus ret = info.taskStatus;
    if (ret.state != protocol::TaskState::ABORTED) {
      ret.state = protocol::TaskState::PLANNED;
    }
    return ret;
  }

  // Error occurs when creating task or even before task is created. Set error
  // and return immediately
  if (error != nullptr) {
    if (info.taskStatus.failures.empty()) {
      info.taskStatus.failures.emplace_back(toPrestoError(error));
    }
    info.taskStatus.state = protocol::TaskState::FAILED;
    recordProcessCpuTime();
    return info.taskStatus;
  }
  BOLT_CHECK_NOT_NULL(task, "task is null when updating status");

  const auto boltTaskStats = task->taskStats();

  info.taskStatus.state = toPrestoTaskState(task->state());

  // Presto has a Driver per split. When splits represent partitions
  // of data, there is a queue of them per Task. We represent
  // running/queued table scan splits as partitioned drivers for Presto.
  /*info.taskStatus.queuedPartitionedDrivers =
      boltTaskStats.numQueuedTableScanSplits;
  info.taskStatus.runningPartitionedDrivers =
      boltTaskStats.numRunningTableScanSplits;
  // Return weights if they were supplied in the table scan splits. Coordinator
  // uses these for split scheduling.
  info.taskStatus.queuedPartitionedSplitsWeight =
      boltTaskStats.queuedTableScanSplitWeights;
  info.taskStatus.runningPartitionedSplitsWeight =
      boltTaskStats.runningTableScanSplitWeights;*/

  info.taskStatus.completedDriverGroups.clear();
  info.taskStatus.completedDriverGroups.reserve(
      boltTaskStats.completedSplitGroups.size());
  for (auto splitGroupId : boltTaskStats.completedSplitGroups) {
    info.taskStatus.completedDriverGroups.push_back({true, splitGroupId});
  }

  const auto boltTaskMemStats = task->pool()->stats();
  info.taskStatus.memoryReservationInBytes = boltTaskMemStats.usedBytes;
  info.taskStatus.systemMemoryReservationInBytes = 0;
  // NOTE: a presto worker may run multiple tasks from the same query.
  // 'peakNodeTotalMemoryReservationInBytes' represents peak memory usage across
  // all these tasks.
  info.taskStatus.peakNodeTotalMemoryReservationInBytes =
      task->queryCtx()->pool()->peakBytes();

  TASK_STATS_SUM(
      boltTaskStats,
      physicalWrittenBytes,
      info.taskStatus.physicalWrittenDataSizeInBytes);

  info.taskStatus.outputBufferUtilization =
      boltTaskStats.outputBufferUtilization;
  info.taskStatus.outputBufferOverutilized =
      boltTaskStats.outputBufferOverutilized;

  if (task->error() && info.taskStatus.failures.empty()) {
    info.taskStatus.failures.emplace_back(toPrestoError(task->error()));
  }

  if (isFinalState(info.taskStatus.state)) {
    recordProcessCpuTime();
  }
  return info.taskStatus;
}

void PrestoTask::updateOutputBufferInfoLocked(
    const bytedance::bolt::exec::TaskStats& boltTaskStats,
    std::unordered_map<std::string, RuntimeMetric>& taskRuntimeStats) {
  if (!boltTaskStats.outputBufferStats.has_value()) {
    return;
  }
  const auto& outputBufferStats = boltTaskStats.outputBufferStats.value();
  auto& outputBufferInfo = info.outputBuffers;
  outputBufferInfo.type =
      bytedance::bolt::core::PartitionedOutputNode::kindString(outputBufferStats.kind);
  outputBufferInfo.canAddBuffers = !outputBufferStats.noMoreBuffers;
  outputBufferInfo.canAddPages = !outputBufferStats.noMoreData;
  outputBufferInfo.totalBufferedBytes = outputBufferStats.bufferedBytes;
  outputBufferInfo.totalBufferedPages = outputBufferStats.bufferedPages;
  outputBufferInfo.totalPagesSent = outputBufferStats.totalPagesSent;
  outputBufferInfo.totalRowsSent = outputBufferStats.totalRowsSent;
  // TODO: populate state and destination buffer stats in info.outputBuffers.

  taskRuntimeStats.insert(
      {"averageOutputBufferWallNanos",
       fromMillis(outputBufferStats.averageBufferTimeMs)});
  /*taskRuntimeStats["numTopOutputBuffers"].addValue(
      outputBufferStats.numTopBuffers);*/
}

protocol::TaskInfo PrestoTask::updateInfoLocked() {
  const protocol::TaskStatus prestoTaskStatus = updateStatusLocked();

  // Return limited info if there is no exec task.
  if (task == nullptr) {
    return info;
  }
  const bytedance::bolt::exec::TaskStats boltTaskStats = task->taskStats();
  const uint64_t currentTimeMs = bytedance::bolt::getCurrentTimeMs();
  // Set 'lastTaskStatsUpdateMs' to execution start time if it is 0.
  if (lastTaskStatsUpdateMs == 0) {
    lastTaskStatsUpdateMs = boltTaskStats.executionStartTimeMs;
  }

  std::unordered_map<std::string, RuntimeMetric> taskRuntimeStats;
  protocol::TaskStats& prestoTaskStats = info.stats;
  // Clear the old runtime metrics as not all of them would be overwritten by
  // the new ones.
  prestoTaskStats.runtimeStats.clear();

  updateOutputBufferInfoLocked(boltTaskStats, taskRuntimeStats);

  // Update time related info.
  updateTimeInfoLocked(boltTaskStats, currentTimeMs, taskRuntimeStats);

  // Update memory related info.
  updateMemoryInfoLocked(boltTaskStats, currentTimeMs, taskRuntimeStats);

  // Update execution related info.
  updateExecutionInfoLocked(boltTaskStats, prestoTaskStatus, taskRuntimeStats);

  // Task runtime metrics we want while the Task is not finalized.
  hasStuckOperator = false;
  if (!isFinalState(prestoTaskStatus.state)) {
    taskRuntimeStats.clear();

    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.total", boltTaskStats.numTotalDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.running", boltTaskStats.numRunningDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats,
        "drivers.completed",
        boltTaskStats.numCompletedDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats,
        "drivers.terminated",
        boltTaskStats.numTerminatedDrivers);
    for (const auto it : boltTaskStats.numBlockedDrivers) {
      addRuntimeMetricIfNotZero(
          taskRuntimeStats,
          fmt::format("drivers.{}", exec::blockingReasonToString(it.first)),
          it.second);
    }
    if (boltTaskStats.longestRunningOpCallMs != 0) {
      hasStuckOperator = true;
      addRuntimeMetricIfNotZero(
          taskRuntimeStats,
          "stuck_op." + boltTaskStats.longestRunningOpCall,
          boltTaskStats.numCompletedDrivers);
    }
    // These metrics we need when we are running, so do not try to skipp them.
    updateTaskRuntimeStats(
        prestoTaskStatus.state,
        taskRuntimeStats,
        /*tryToSkipIfRunning=*/false,
        prestoTaskStats);
  }

  lastTaskStatsUpdateMs = currentTimeMs;
  return info;
}

void PrestoTask::updateTimeInfoLocked(
    const bytedance::bolt::exec::TaskStats& boltTaskStats,
    uint64_t currentTimeMs,
    std::unordered_map<std::string, bytedance::bolt::RuntimeMetric>& taskRuntimeStats) {
  protocol::TaskStats& prestoTaskStats = info.stats;

  prestoTaskStats.totalScheduledTimeInNanos = {};
  prestoTaskStats.totalCpuTimeInNanos = {};
  prestoTaskStats.totalBlockedTimeInNanos = {};

  prestoTaskStats.createTime =
      util::toISOTimestamp(boltTaskStats.executionStartTimeMs);
  prestoTaskStats.firstStartTime =
      util::toISOTimestamp(boltTaskStats.firstSplitStartTimeMs);
  createTimeMs = boltTaskStats.executionStartTimeMs;
  firstSplitStartTimeMs = boltTaskStats.firstSplitStartTimeMs;
  prestoTaskStats.lastStartTime =
      util::toISOTimestamp(boltTaskStats.lastSplitStartTimeMs);
  prestoTaskStats.lastEndTime =
      util::toISOTimestamp(boltTaskStats.executionEndTimeMs);
  prestoTaskStats.endTime =
      util::toISOTimestamp(boltTaskStats.executionEndTimeMs);
  lastEndTimeMs = boltTaskStats.executionEndTimeMs;

  if (boltTaskStats.executionEndTimeMs > boltTaskStats.executionStartTimeMs) {
    prestoTaskStats.elapsedTimeInNanos = (boltTaskStats.executionEndTimeMs -
                                          boltTaskStats.executionStartTimeMs) *
        1'000'000;
  } else {
    prestoTaskStats.elapsedTimeInNanos =
        (currentTimeMs - boltTaskStats.executionStartTimeMs) * 1'000'000;
  }

  taskRuntimeStats["createTime"].addValue(boltTaskStats.executionStartTimeMs);
  if (boltTaskStats.endTimeMs >= boltTaskStats.executionEndTimeMs) {
    taskRuntimeStats.insert(
        {"outputConsumedDelayInNanos",
         fromMillis(
             boltTaskStats.endTimeMs - boltTaskStats.executionEndTimeMs)});
    taskRuntimeStats["endTime"].addValue(boltTaskStats.endTimeMs);
  }
  taskRuntimeStats.insert({"nativeProcessCpuTime", fromNanos(processCpuTime_)});
}

void PrestoTask::updateMemoryInfoLocked(
    const bytedance::bolt::exec::TaskStats& boltTaskStats,
    uint64_t currentTimeMs,
    std::unordered_map<std::string, bytedance::bolt::RuntimeMetric>& taskRuntimeStats) {
  protocol::TaskStats& prestoTaskStats = info.stats;

  const auto boltTaskMemStats = task->pool()->stats();
  const auto currentBytes = boltTaskMemStats.usedBytes;
  prestoTaskStats.userMemoryReservationInBytes = currentBytes;
  prestoTaskStats.systemMemoryReservationInBytes = 0;
  prestoTaskStats.peakUserMemoryInBytes = boltTaskMemStats.peakBytes;
  prestoTaskStats.peakTotalMemoryInBytes = boltTaskMemStats.peakBytes;

  // TODO(venkatra): Populate these memory stats as well.
  prestoTaskStats.revocableMemoryReservationInBytes = {};

  const int64_t averageMemoryForLastPeriod =
      (currentBytes + lastMemoryReservation) / 2;
  const double sinceLastPeriodMs = currentTimeMs - lastTaskStatsUpdateMs;

  prestoTaskStats.cumulativeUserMemory +=
      (averageMemoryForLastPeriod * sinceLastPeriodMs);
  // NOTE: bolt doesn't differentiate user and system memory usages.
  prestoTaskStats.cumulativeTotalMemory = prestoTaskStats.cumulativeUserMemory;
  prestoTaskStats.peakNodeTotalMemoryInBytes =
      task->queryCtx()->pool()->peakBytes();

  if (boltTaskStats.memoryReclaimCount > 0) {
    taskRuntimeStats["taskMemoryReclaimCount"].addValue(
        boltTaskStats.memoryReclaimCount);
    taskRuntimeStats.insert(
        {"taskMemoryReclaimWallNanos",
         fromMillis(boltTaskStats.memoryReclaimMs)});
  }
  lastMemoryReservation = currentBytes;
}

void PrestoTask::updateExecutionInfoLocked(
    const bytedance::bolt::exec::TaskStats& boltTaskStats,
    const protocol::TaskStatus& prestoTaskStatus,
    std::unordered_map<std::string, bytedance::bolt::RuntimeMetric>& taskRuntimeStats) {
  protocol::TaskStats& prestoTaskStats = info.stats;

  prestoTaskStats.rawInputPositions = 0;
  prestoTaskStats.rawInputDataSizeInBytes = 0;
  prestoTaskStats.processedInputPositions = 0;
  prestoTaskStats.processedInputDataSizeInBytes = 0;
  prestoTaskStats.outputPositions = 0;
  prestoTaskStats.outputDataSizeInBytes = 0;

  // Presto Java reports number of drivers to number of splits in Presto UI
  // because split and driver are 1 to 1 mapping relationship. This is not true
  // in Prestissimo where 1 driver handles many splits. In order to quickly
  // unblock developers from viewing the correct progress of splits in
  // Prestissimo's coordinator UI, we put number of splits in total, queued, and
  // finished to indicate the progress of the query. Number of running drivers
  // are passed as it is to have a proper running drivers count in UI.
  //
  // TODO: We should really extend the API (protocol::TaskStats and Presto
  // coordinator UI) to have splits information as a proper fix.
  prestoTaskStats.totalDrivers = boltTaskStats.numTotalSplits;
  prestoTaskStats.queuedDrivers = boltTaskStats.numQueuedSplits;
  prestoTaskStats.runningDrivers = boltTaskStats.numRunningDrivers;
  prestoTaskStats.completedDrivers = boltTaskStats.numFinishedSplits;

  prestoTaskStats.pipelines.resize(boltTaskStats.pipelineStats.size());
  for (int i = 0; i < boltTaskStats.pipelineStats.size(); ++i) {
    auto& prestoPipeline = info.stats.pipelines[i];
    auto& boltPipeline = boltTaskStats.pipelineStats[i];
    prestoPipeline.inputPipeline = boltPipeline.inputPipeline;
    prestoPipeline.outputPipeline = boltPipeline.outputPipeline;
    prestoPipeline.firstStartTime = prestoTaskStats.createTime;
    prestoPipeline.lastStartTime = prestoTaskStats.endTime;
    prestoPipeline.lastEndTime = prestoTaskStats.endTime;

    prestoPipeline.operatorSummaries.resize(boltPipeline.operatorStats.size());
    prestoPipeline.totalScheduledTimeInNanos = {};
    prestoPipeline.totalCpuTimeInNanos = {};
    prestoPipeline.totalBlockedTimeInNanos = {};
    prestoPipeline.userMemoryReservationInBytes = {};
    prestoPipeline.revocableMemoryReservationInBytes = {};
    prestoPipeline.systemMemoryReservationInBytes = {};

    // tasks may fail before any operators are created;
    // collect stats only when we have operators
    if (!boltPipeline.operatorStats.empty()) {
      const auto& firstBoltOpStats = boltPipeline.operatorStats[0];
      const auto& lastBoltOpStats = boltPipeline.operatorStats.back();

      prestoPipeline.pipelineId = firstBoltOpStats.pipelineId;
      prestoPipeline.totalDrivers = firstBoltOpStats.numDrivers;
      prestoPipeline.rawInputPositions = firstBoltOpStats.rawInputPositions;
      prestoPipeline.rawInputDataSizeInBytes = firstBoltOpStats.rawInputBytes;
      prestoPipeline.processedInputPositions = firstBoltOpStats.inputPositions;
      prestoPipeline.processedInputDataSizeInBytes =
          firstBoltOpStats.inputBytes;
      prestoPipeline.outputPositions = lastBoltOpStats.outputPositions;
      prestoPipeline.outputDataSizeInBytes = lastBoltOpStats.outputBytes;
    }

    if (prestoPipeline.inputPipeline) {
      prestoTaskStats.rawInputPositions += prestoPipeline.rawInputPositions;
      prestoTaskStats.rawInputDataSizeInBytes +=
          prestoPipeline.rawInputDataSizeInBytes;
      prestoTaskStats.processedInputPositions +=
          prestoPipeline.processedInputPositions;
      prestoTaskStats.processedInputDataSizeInBytes +=
          prestoPipeline.processedInputDataSizeInBytes;
    }
    if (prestoPipeline.outputPipeline) {
      prestoTaskStats.outputPositions += prestoPipeline.outputPositions;
      prestoTaskStats.outputDataSizeInBytes +=
          prestoPipeline.outputDataSizeInBytes;
    }

    /*for (const auto& driverStat : boltPipeline.driverStats) {
      for (const auto& [name, value] : driverStat.runtimeStats) {
        addRuntimeMetric(taskRuntimeStats, name, value);
      }
    }*/

    for (auto j = 0; j < boltPipeline.operatorStats.size(); ++j) {
      auto& prestoOp = prestoPipeline.operatorSummaries[j];
      auto& boltOp = boltPipeline.operatorStats[j];

      prestoOp.stageId = id.stageId();
      prestoOp.stageExecutionId = id.stageExecutionId();
      prestoOp.pipelineId = i;
      prestoOp.planNodeId = boltOp.planNodeId;
      prestoOp.planNodeId = toPrestoPlanNodeId(prestoOp.planNodeId);
      prestoOp.operatorId = boltOp.operatorId;
      prestoOp.operatorType = toPrestoOperatorType(boltOp.operatorType);

      prestoOp.totalDrivers = boltOp.numDrivers;
      prestoOp.inputPositions = boltOp.inputPositions;
      prestoOp.sumSquaredInputPositions =
          ((double)boltOp.inputPositions) * boltOp.inputPositions;
      prestoOp.inputDataSize =
          protocol::DataSize(boltOp.inputBytes, protocol::DataUnit::BYTE);
      prestoOp.rawInputPositions = boltOp.rawInputPositions;
      prestoOp.rawInputDataSize =
          protocol::DataSize(boltOp.rawInputBytes, protocol::DataUnit::BYTE);

      // Report raw input statistics on the Project node following TableScan, if
      // exists.
      if (j == 1 && boltOp.operatorType == "FilterProject" &&
          boltPipeline.operatorStats[0].operatorType == "TableScan") {
        const auto& scanOp = boltPipeline.operatorStats[0];
        prestoOp.rawInputPositions = scanOp.rawInputPositions;
        prestoOp.rawInputDataSize =
            protocol::DataSize(scanOp.rawInputBytes, protocol::DataUnit::BYTE);
      }

      prestoOp.outputPositions = boltOp.outputPositions;
      prestoOp.outputDataSize =
          protocol::DataSize(boltOp.outputBytes, protocol::DataUnit::BYTE);

      setTiming(
          boltOp.isBlockedTiming,
          prestoOp.isBlockedCalls,
          prestoOp.isBlockedWall,
          prestoOp.isBlockedCpu);
      setTiming(
          boltOp.addInputTiming,
          prestoOp.addInputCalls,
          prestoOp.addInputWall,
          prestoOp.addInputCpu);
      setTiming(
          boltOp.getOutputTiming,
          prestoOp.getOutputCalls,
          prestoOp.getOutputWall,
          prestoOp.getOutputCpu);
      CpuWallTiming finishAndBackgroundTiming;
      finishAndBackgroundTiming.add(boltOp.finishTiming);
      finishAndBackgroundTiming.add(boltOp.backgroundTiming);
      setTiming(
          finishAndBackgroundTiming,
          prestoOp.finishCalls,
          prestoOp.finishWall,
          prestoOp.finishCpu);

      prestoOp.blockedWall = protocol::Duration(
          boltOp.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);

      prestoOp.userMemoryReservation = protocol::DataSize(
          boltOp.memoryStats.userMemoryReservation, protocol::DataUnit::BYTE);
      prestoOp.revocableMemoryReservation = protocol::DataSize(
          boltOp.memoryStats.revocableMemoryReservation,
          protocol::DataUnit::BYTE);
      prestoOp.systemMemoryReservation = protocol::DataSize(
          boltOp.memoryStats.systemMemoryReservation,
          protocol::DataUnit::BYTE);
      prestoOp.peakUserMemoryReservation = protocol::DataSize(
          boltOp.memoryStats.peakUserMemoryReservation,
          protocol::DataUnit::BYTE);
      prestoOp.peakSystemMemoryReservation = protocol::DataSize(
          boltOp.memoryStats.peakSystemMemoryReservation,
          protocol::DataUnit::BYTE);
      prestoOp.peakTotalMemoryReservation = protocol::DataSize(
          boltOp.memoryStats.peakTotalMemoryReservation,
          protocol::DataUnit::BYTE);

      prestoOp.spilledDataSize =
          protocol::DataSize(boltOp.spilledBytes, protocol::DataUnit::BYTE);

      if (boltOp.operatorType == "HashBuild") {
        prestoOp.joinBuildKeyCount = boltOp.inputPositions;
        prestoOp.nullJoinBuildKeyCount = boltOp.numNullKeys;
      }
      if (boltOp.operatorType == "HashProbe") {
        prestoOp.joinProbeKeyCount = boltOp.inputPositions;
        prestoOp.nullJoinProbeKeyCount = boltOp.numNullKeys;
      }

      if (!boltOp.dynamicFilterStats.empty()) {
        prestoOp.dynamicFilterStats = toPrestoDynamicFilterStats(boltOp);
      }

      for (const auto& stat : boltOp.runtimeStats) {
        auto statName = generateRuntimeStatName(boltOp, stat.first);
        prestoOp.runtimeStats[statName] =
            toRuntimeMetric(statName, stat.second);
        addRuntimeMetric(taskRuntimeStats, statName, stat.second);
      }

      OperatorStatsCollector operatorStatsCollector{
          boltOp, prestoOp.runtimeStats, prestoTaskStats.runtimeStats};

      operatorStatsCollector.addIfNotZero("numSplits", boltOp.numSplits);
      operatorStatsCollector.addIfNotZero("inputBatches", boltOp.inputVectors);
      operatorStatsCollector.addIfNotZero(
          "outputBatches", boltOp.outputVectors);
      operatorStatsCollector.addIfNotZero(
          "numMemoryAllocations", boltOp.memoryStats.numMemoryAllocations);

      // If Bolt operator has spilling stats, then add them to the Presto
      // operator stats and the task stats as runtime stats.
      if (boltOp.spilledBytes > 0) {
        addSpillingOperatorMetrics(operatorStatsCollector);
      }

      auto wallNanos = boltOp.isBlockedTiming.wallNanos +
          boltOp.addInputTiming.wallNanos + boltOp.getOutputTiming.wallNanos +
          boltOp.finishTiming.wallNanos;
      auto cpuNanos = boltOp.isBlockedTiming.cpuNanos +
          boltOp.addInputTiming.cpuNanos + boltOp.getOutputTiming.cpuNanos +
          boltOp.finishTiming.cpuNanos;

      prestoPipeline.totalScheduledTimeInNanos += wallNanos;
      prestoPipeline.totalCpuTimeInNanos += cpuNanos;
      prestoPipeline.totalBlockedTimeInNanos += boltOp.blockedWallNanos;
      prestoPipeline.userMemoryReservationInBytes +=
          boltOp.memoryStats.userMemoryReservation;
      prestoPipeline.revocableMemoryReservationInBytes +=
          boltOp.memoryStats.revocableMemoryReservation;
      prestoPipeline.systemMemoryReservationInBytes +=
          boltOp.memoryStats.systemMemoryReservation;

      prestoTaskStats.totalScheduledTimeInNanos += wallNanos;
      prestoTaskStats.totalCpuTimeInNanos += cpuNanos;
      prestoTaskStats.totalBlockedTimeInNanos += boltOp.blockedWallNanos;
    } // bolt pipeline's operators loop
  } // bolt task's pipelines loop

  updateOperatorRuntimeStats(prestoTaskStatus.state, prestoTaskStats);
  updateTaskRuntimeStats(
      prestoTaskStatus.state,
      taskRuntimeStats,
      /*tryToSkipIfRunning=*/true,
      prestoTaskStats);
}

/*static*/ std::string PrestoTask::taskStatesToString(
    const std::array<size_t, 5>& taskStates) {
  // Names of five TaskState (enum defined in exec/Task.h).
  static constexpr std::array<folly::StringPiece, 5> taskStateNames{
      "Running",
      "Finished",
      "Canceled",
      "Aborted",
      "Failed",
  };

  std::string str;
  for (size_t i = 0; i < taskStates.size(); ++i) {
    if (taskStates[i] != 0) {
      folly::toAppend(
          fmt::format("{}={} ", taskStateNames[i], taskStates[i]), &str);
    }
  }
  return str;
}

folly::dynamic PrestoTask::toJson() const {
  std::lock_guard<std::mutex> l(mutex);
  folly::dynamic obj = folly::dynamic::object;
  obj["task"] = task ? task->toJson() : "null";
  obj["taskStarted"] = taskStarted;
  obj["lastHeartbeatMs"] = lastHeartbeatMs;
  obj["lastTaskStatsUpdateMs"] = lastTaskStatsUpdateMs;
  obj["lastMemoryReservation"] = lastMemoryReservation;
  obj["createTimeMs"] = createTimeMs;
  obj["firstSplitStartTimeMs"] = firstSplitStartTimeMs;
  obj["lastEndTimeMs"] = lastEndTimeMs;

  json j;
  to_json(j, info);
  obj["taskInfo"] = folly::parseJson(to_string(j));
  return obj;
}

protocol::RuntimeMetric toRuntimeMetric(
    const std::string& name,
    const RuntimeMetric& metric) {
  return protocol::RuntimeMetric{
      name,
      toPrestoRuntimeUnit(metric.unit),
      metric.sum,
      metric.count,
      metric.max,
      metric.min};
}

bool isFinalState(protocol::TaskState state) {
  switch (state) {
    case protocol::TaskState::FINISHED:
      [[fallthrough]];
    case protocol::TaskState::FAILED:
      [[fallthrough]];
    case protocol::TaskState::ABORTED:
      [[fallthrough]];
    case protocol::TaskState::CANCELED:
      return true;
    default:
      return false;
  }
}

} // namespace facebook::presto
