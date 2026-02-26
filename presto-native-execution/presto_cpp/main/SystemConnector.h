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

#include "presto_cpp/main/SystemSplit.h"
#include "presto_cpp/main/types/PrestoToBoltConnector.h"

#include "bolt/connectors/Connector.h"

namespace facebook::presto {

class TaskManager;

class SystemColumnHandle : public bytedance::bolt::connector::ColumnHandle {
 public:
  explicit SystemColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};

class SystemTableHandle
    : public bytedance::bolt::connector::ConnectorTableHandle {
 public:
  explicit SystemTableHandle(
      std::string connectorId,
      std::string schemaName,
      std::string tableName);

  std::string toString() const override;

  const std::string& schemaName() {
    return schemaName_;
  }

  const std::string& tableName() {
    return tableName_;
  }

  const bytedance::bolt::RowTypePtr taskSchema();

 private:
  const std::string schemaName_;
  const std::string tableName_;
};

class SystemDataSource : public bytedance::bolt::connector::DataSource {
 public:
  SystemDataSource(
      const bytedance::bolt::RowTypePtr& outputType,
      const std::shared_ptr<bytedance::bolt::connector::ConnectorTableHandle>&
          tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>&
          columnHandles,
      const TaskManager* taskManager,
      bytedance::bolt::memory::MemoryPool* pool);

  void addSplit(std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>
                    split) override;

  void addDynamicFilter(
      bytedance::bolt::column_index_t /*outputChannel*/,
      const std::shared_ptr<bytedance::bolt::common::Filter>& /*filter*/)
      override {
    BOLT_NYI("Dynamic filters not supported by SystemConnector.");
  }

  std::optional<bytedance::bolt::RowVectorPtr> next(
      uint64_t size,
      bytedance::bolt::ContinueFuture& future) override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  std::unordered_map<std::string, bytedance::bolt::RuntimeCounter>
  runtimeStats() override {
    return {};
  }

 private:
  enum class TaskColumnEnum {
    // Note: These values are in the same order as SystemTableHandle schema.
    kNodeId = 0,
    kTaskId,
    kStageExecutionId,
    kStageId,
    kQueryId,
    kState,
    kSplits,
    kQueuedSplits,
    kRunningSplits,
    kCompletedSplits,
    kSplitScheduledTimeMs,
    kSplitCpuTimeMs,
    kSplitBlockedTimeMs,
    kRawInputBytes,
    kRawInputRows,
    kProcessedInputBytes,
    kProcessedInputRows,
    kOutputBytes,
    kOutputRows,
    kPhysicalWrittenBytes,
    kCreated,
    kStart,
    kLastHeartBeat,
    kEnd,
  };

  bytedance::bolt::RowVectorPtr getTaskResults();

  // Mapping between output columns and their indices (column_index_t)
  // corresponding to the taskInfo fields for them.
  std::vector<bytedance::bolt::column_index_t> outputColumnMappings_;
  bytedance::bolt::RowTypePtr outputType_;

  const TaskManager* taskManager_;
  bytedance::bolt::memory::MemoryPool* pool_;

  std::shared_ptr<SystemSplit> currentSplit_;

  size_t completedRows_{0};
  size_t completedBytes_{0};
};

class SystemConnector : public bytedance::bolt::connector::Connector {
 public:
  SystemConnector(const std::string& id, const TaskManager* taskManager)
      : Connector(id), taskManager_(taskManager) {}

  std::unique_ptr<bytedance::bolt::connector::DataSource> createDataSource(
      const bytedance::bolt::RowTypePtr& outputType,
      const std::shared_ptr<bytedance::bolt::connector::ConnectorTableHandle>&
          tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>&
          columnHandles,
      std::shared_ptr<bytedance::bolt::connector::ConnectorQueryCtx> connectorQueryCtx,
      const bytedance::bolt::core::QueryConfig& /* queryConfig */) override final {
    BOLT_CHECK(taskManager_);
    return std::make_unique<SystemDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        taskManager_,
        connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<bytedance::bolt::connector::DataSink> createDataSink(
      bytedance::bolt::RowTypePtr /*inputType*/,
      std::shared_ptr<
          bytedance::bolt::connector::
              ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      bytedance::bolt::connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      bytedance::bolt::connector::CommitStrategy /*commitStrategy*/,
      const bytedance::bolt::core::QueryConfig& /*queryConfig*/)
      override final {
    BOLT_NYI("SystemConnector does not support data sink.");
  }

 private:
  const TaskManager* taskManager_;
};

class SystemPrestoToBoltConnector final : public PrestoToBoltConnector {
 public:
  explicit SystemPrestoToBoltConnector(std::string connectorId)
      : PrestoToBoltConnector(std::move(connectorId)) {}

  std::unique_ptr<bytedance::bolt::connector::ConnectorSplit> toBoltSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  std::unique_ptr<bytedance::bolt::connector::ColumnHandle> toBoltColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  std::unique_ptr<bytedance::bolt::connector::ConnectorTableHandle>
  toBoltTableHandle(
      const protocol::TableHandle& tableHandle,
      const BoltExprConverter& exprConverter,
      const TypeParser& typeParser,
      std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>&
          assignments) const final;

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;
};

} // namespace facebook::presto
