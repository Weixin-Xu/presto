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

#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto {

struct TaskBackend {
  static void setOperatorInputDataSize(
      protocol::OperatorStats& operatorStats,
      int64_t value) {
    operatorStats.inputDataSize = protocol::DataSize(value, protocol::DataUnit::BYTE);
  }

  static void setOperatorRawInputDataSize(
      protocol::OperatorStats& operatorStats,
      int64_t value) {
    operatorStats.rawInputDataSize =
        protocol::DataSize(value, protocol::DataUnit::BYTE);
  }

  static void setOperatorOutputDataSize(
      protocol::OperatorStats& operatorStats,
      int64_t value) {
    operatorStats.outputDataSize =
        protocol::DataSize(value, protocol::DataUnit::BYTE);
  }

  static void setOperatorMemoryReservations(
      protocol::OperatorStats& operatorStats,
      int64_t user,
      int64_t revocable,
      int64_t system,
      int64_t peakUser,
      int64_t peakSystem,
      int64_t peakTotal) {
    operatorStats.userMemoryReservation =
        protocol::DataSize(user, protocol::DataUnit::BYTE);
    operatorStats.revocableMemoryReservation =
        protocol::DataSize(revocable, protocol::DataUnit::BYTE);
    operatorStats.systemMemoryReservation =
        protocol::DataSize(system, protocol::DataUnit::BYTE);
    operatorStats.peakUserMemoryReservation =
        protocol::DataSize(peakUser, protocol::DataUnit::BYTE);
    operatorStats.peakSystemMemoryReservation =
        protocol::DataSize(peakSystem, protocol::DataUnit::BYTE);
    operatorStats.peakTotalMemoryReservation =
        protocol::DataSize(peakTotal, protocol::DataUnit::BYTE);
  }

  static void setOperatorSpilledDataSize(
      protocol::OperatorStats& operatorStats,
      int64_t value) {
    operatorStats.spilledDataSize =
        protocol::DataSize(value, protocol::DataUnit::BYTE);
  }

  static void setTaskCreateTime(protocol::TaskStats& taskStats, int64_t timeMs) {
    taskStats.createTime = util::toISOTimestamp(timeMs);
  }

  static void setTaskEndTime(protocol::TaskStats& taskStats, int64_t timeMs) {
    taskStats.endTime = util::toISOTimestamp(timeMs);
  }

  static void setTaskTimeInfo(
      protocol::TaskStats& taskStats,
      int64_t createTimeMs,
      int64_t firstStartTimeMs,
      int64_t lastStartTimeMs,
      int64_t lastEndTimeMs,
      int64_t endTimeMs) {
    taskStats.createTime = util::toISOTimestamp(createTimeMs);
    taskStats.firstStartTime = util::toISOTimestamp(firstStartTimeMs);
    taskStats.lastStartTime = util::toISOTimestamp(lastStartTimeMs);
    taskStats.lastEndTime = util::toISOTimestamp(lastEndTimeMs);
    taskStats.endTime = util::toISOTimestamp(endTimeMs);
  }

  static void setTaskInfoHeartbeat(protocol::TaskInfo& taskInfo, int64_t timeMs) {
    taskInfo.lastHeartbeat = util::toISOTimestamp(timeMs);
  }
};

} // namespace facebook::presto
