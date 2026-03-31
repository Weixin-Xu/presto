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
#pragma once

#include <functional>
#include <vector>

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::task {

template <typename PipelineStats>
inline void updateTaskInputOutputStats(
    const PipelineStats& pipelineStats,
    protocol::TaskStats& taskStats) {
  if (pipelineStats.operatorStats.empty()) {
    return;
  }

  if (pipelineStats.inputPipeline) {
    const auto& firstOperator = pipelineStats.operatorStats.front();
    taskStats.rawInputPositions += firstOperator.rawInputPositions;
    taskStats.rawInputDataSizeInBytes += firstOperator.rawInputBytes;
    taskStats.processedInputPositions += firstOperator.inputPositions;
    taskStats.processedInputDataSizeInBytes += firstOperator.inputBytes;
  }

  if (pipelineStats.outputPipeline) {
    const auto& lastOperator = pipelineStats.operatorStats.back();
    taskStats.outputPositions += lastOperator.outputPositions;
    taskStats.outputDataSizeInBytes += lastOperator.outputBytes;
  }
}

template <typename PipelineStats>
inline void updatePipelineInputOutputStats(
    const PipelineStats& pipelineStats,
    protocol::PipelineStats& prestoPipelineStats) {
  if (pipelineStats.operatorStats.empty()) {
    return;
  }

  const auto& firstOperator = pipelineStats.operatorStats.front();
  const auto& lastOperator = pipelineStats.operatorStats.back();

  prestoPipelineStats.pipelineId = firstOperator.pipelineId;
  prestoPipelineStats.totalDrivers = firstOperator.numDrivers;
  prestoPipelineStats.rawInputPositions = firstOperator.rawInputPositions;
  prestoPipelineStats.rawInputDataSizeInBytes = firstOperator.rawInputBytes;
  prestoPipelineStats.processedInputPositions = firstOperator.inputPositions;
  prestoPipelineStats.processedInputDataSizeInBytes = firstOperator.inputBytes;
  prestoPipelineStats.outputPositions = lastOperator.outputPositions;
  prestoPipelineStats.outputDataSizeInBytes = lastOperator.outputBytes;
}

template <typename OperatorStats>
inline void updatePipelineInputOutputStats(
    const std::vector<OperatorStats>& operatorStats,
    protocol::PipelineStats& prestoPipelineStats) {
  if (operatorStats.empty()) {
    return;
  }

  const auto& firstOperator = operatorStats.front();
  const auto& lastOperator = operatorStats.back();

  prestoPipelineStats.pipelineId = firstOperator.pipelineId;
  prestoPipelineStats.totalDrivers = firstOperator.numDrivers;
  prestoPipelineStats.rawInputPositions = firstOperator.rawInputPositions;
  prestoPipelineStats.rawInputDataSizeInBytes = firstOperator.rawInputBytes;
  prestoPipelineStats.processedInputPositions = firstOperator.inputPositions;
  prestoPipelineStats.processedInputDataSizeInBytes = firstOperator.inputBytes;
  prestoPipelineStats.outputPositions = lastOperator.outputPositions;
  prestoPipelineStats.outputDataSizeInBytes = lastOperator.outputBytes;
}

template <
    typename Backend,
    typename OperatorStats,
    typename ToPlanNodeId,
    typename ToOperatorType,
    typename SetTiming,
    typename ToDynamicFilterStats>
inline void populateOperatorSummaryStats(
    const OperatorStats& operatorStats,
    int stageId,
    int stageExecutionId,
    int pipelineId,
    protocol::OperatorStats& prestoOperatorStats,
    const ToPlanNodeId& toPlanNodeId,
    const ToOperatorType& toOperatorType,
    const SetTiming& setTiming,
    const ToDynamicFilterStats& toDynamicFilterStats) {
  prestoOperatorStats.stageId = stageId;
  prestoOperatorStats.stageExecutionId = stageExecutionId;
  prestoOperatorStats.pipelineId = pipelineId;
  prestoOperatorStats.planNodeId = toPlanNodeId(operatorStats.planNodeId);
  prestoOperatorStats.operatorId = operatorStats.operatorId;
  prestoOperatorStats.operatorType = toOperatorType(operatorStats.operatorType);

  prestoOperatorStats.totalDrivers = operatorStats.numDrivers;
  prestoOperatorStats.inputPositions = operatorStats.inputPositions;
  prestoOperatorStats.sumSquaredInputPositions =
      ((double)operatorStats.inputPositions) * operatorStats.inputPositions;
  Backend::setOperatorInputDataSize(prestoOperatorStats, operatorStats.inputBytes);
  prestoOperatorStats.rawInputPositions = operatorStats.rawInputPositions;
  Backend::setOperatorRawInputDataSize(
      prestoOperatorStats, operatorStats.rawInputBytes);
  prestoOperatorStats.outputPositions = operatorStats.outputPositions;
  Backend::setOperatorOutputDataSize(prestoOperatorStats, operatorStats.outputBytes);

  setTiming(
      operatorStats.isBlockedTiming,
      prestoOperatorStats.isBlockedCalls,
      prestoOperatorStats.isBlockedWall,
      prestoOperatorStats.isBlockedCpu);
  setTiming(
      operatorStats.addInputTiming,
      prestoOperatorStats.addInputCalls,
      prestoOperatorStats.addInputWall,
      prestoOperatorStats.addInputCpu);
  setTiming(
      operatorStats.getOutputTiming,
      prestoOperatorStats.getOutputCalls,
      prestoOperatorStats.getOutputWall,
      prestoOperatorStats.getOutputCpu);

  auto finishAndBackgroundTiming = operatorStats.finishTiming;
  finishAndBackgroundTiming.add(operatorStats.backgroundTiming);
  setTiming(
      finishAndBackgroundTiming,
      prestoOperatorStats.finishCalls,
      prestoOperatorStats.finishWall,
      prestoOperatorStats.finishCpu);

  prestoOperatorStats.blockedWall = protocol::Duration(
      operatorStats.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);

  Backend::setOperatorMemoryReservations(
      prestoOperatorStats,
      operatorStats.memoryStats.userMemoryReservation,
      operatorStats.memoryStats.revocableMemoryReservation,
      operatorStats.memoryStats.systemMemoryReservation,
      operatorStats.memoryStats.peakUserMemoryReservation,
      operatorStats.memoryStats.peakSystemMemoryReservation,
      operatorStats.memoryStats.peakTotalMemoryReservation);
  Backend::setOperatorSpilledDataSize(
      prestoOperatorStats, operatorStats.spilledBytes);

  if (operatorStats.operatorType == "HashBuild") {
    prestoOperatorStats.joinBuildKeyCount = operatorStats.inputPositions;
    prestoOperatorStats.nullJoinBuildKeyCount = operatorStats.numNullKeys;
  }
  if (operatorStats.operatorType == "HashProbe") {
    prestoOperatorStats.joinProbeKeyCount = operatorStats.inputPositions;
    prestoOperatorStats.nullJoinProbeKeyCount = operatorStats.numNullKeys;
  }
  if (!operatorStats.dynamicFilterStats.empty()) {
    prestoOperatorStats.dynamicFilterStats =
        toDynamicFilterStats(operatorStats);
  }
}

template <typename OperatorStats>
inline void updatePipelineRunningTotals(
    const OperatorStats& operatorStats,
    protocol::PipelineStats& prestoPipelineStats) {
  const auto wallNanos = operatorStats.isBlockedTiming.wallNanos +
      operatorStats.addInputTiming.wallNanos +
      operatorStats.getOutputTiming.wallNanos +
      operatorStats.finishTiming.wallNanos;
  const auto cpuNanos = operatorStats.isBlockedTiming.cpuNanos +
      operatorStats.addInputTiming.cpuNanos +
      operatorStats.getOutputTiming.cpuNanos + operatorStats.finishTiming.cpuNanos;

  prestoPipelineStats.totalScheduledTimeInNanos += wallNanos;
  prestoPipelineStats.totalCpuTimeInNanos += cpuNanos;
  prestoPipelineStats.totalBlockedTimeInNanos += operatorStats.blockedWallNanos;
  prestoPipelineStats.userMemoryReservationInBytes +=
      operatorStats.memoryStats.userMemoryReservation;
  prestoPipelineStats.revocableMemoryReservationInBytes +=
      operatorStats.memoryStats.revocableMemoryReservation;
  prestoPipelineStats.systemMemoryReservationInBytes +=
      operatorStats.memoryStats.systemMemoryReservation;
}

template <typename Backend, typename OperatorStats>
inline void maybeCopyRawInputFromTableScan(
    int operatorIndex,
    const OperatorStats& operatorStats,
    const std::vector<OperatorStats>& operatorStatsList,
    protocol::OperatorStats& prestoOperatorStats) {
  if (operatorIndex == 1 && operatorStats.operatorType == "FilterProject" &&
      operatorStatsList[0].operatorType == "TableScan") {
    const auto& scanOperator = operatorStatsList[0];
    prestoOperatorStats.rawInputPositions = scanOperator.rawInputPositions;
    Backend::setOperatorRawInputDataSize(
        prestoOperatorStats, scanOperator.rawInputBytes);
  }
}

} // namespace facebook::presto::task
