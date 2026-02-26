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

#include "PrestoToBoltExpr.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/presto_protocol/connector/hive/presto_protocol_hive.h"
#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/core/PlanNode.h"
#include "bolt/vector/ComplexVector.h"

namespace facebook::presto {

class PrestoToBoltConnector;

void registerPrestoToBoltConnector(
    std::unique_ptr<const PrestoToBoltConnector> connector);

void unregisterPrestoToBoltConnector(const std::string& connectorName);

const PrestoToBoltConnector& getPrestoToBoltConnector(
    const std::string& connectorName);

class PrestoToBoltConnector {
 public:
  virtual ~PrestoToBoltConnector() = default;

  [[nodiscard]] const std::string& connectorName() const {
    return connectorName_;
  }

  [[nodiscard]] virtual std::unique_ptr<bytedance::bolt::connector::ConnectorSplit>
  toBoltSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const = 0;

  [[nodiscard]] virtual std::unique_ptr<bytedance::bolt::connector::ColumnHandle>
  toBoltColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const = 0;

  [[nodiscard]] virtual std::unique_ptr<bytedance::bolt::connector::ConnectorTableHandle>
  toBoltTableHandle(
      const protocol::TableHandle& tableHandle,
      const BoltExprConverter& exprConverter,
      const TypeParser& typeParser,
      std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>& assignments)
      const = 0;

  [[nodiscard]] virtual std::unique_ptr<
      bytedance::bolt::connector::ConnectorInsertTableHandle>
  toBoltInsertTableHandle(
      const protocol::CreateHandle* createHandle,
      const TypeParser& typeParser) const {
    return {};
  }

  [[nodiscard]] virtual std::unique_ptr<
      bytedance::bolt::connector::ConnectorInsertTableHandle>
  toBoltInsertTableHandle(
      const protocol::InsertHandle* insertHandle,
      const TypeParser& typeParser) const {
    return {};
  }

  [[nodiscard]] std::unique_ptr<bytedance::bolt::core::PartitionFunctionSpec>
  createBoltPartitionFunctionSpec(
      const protocol::ConnectorPartitioningHandle* partitioningHandle,
      const std::vector<int>& bucketToPartition,
      const std::vector<bytedance::bolt::column_index_t>& channels,
      const std::vector<bytedance::bolt::VectorPtr>& constValues) const {
    bool ignored;
    return createBoltPartitionFunctionSpec(
        partitioningHandle, bucketToPartition, channels, constValues, ignored);
  }

  [[nodiscard]] virtual std::unique_ptr<bytedance::bolt::core::PartitionFunctionSpec>
  createBoltPartitionFunctionSpec(
      const protocol::ConnectorPartitioningHandle* partitioningHandle,
      const std::vector<int>& bucketToPartition,
      const std::vector<bytedance::bolt::column_index_t>& channels,
      const std::vector<bytedance::bolt::VectorPtr>& constValues,
      bool& effectivelyGather) const {
    return {};
  }

  [[nodiscard]] virtual std::unique_ptr<protocol::ConnectorProtocol>
  createConnectorProtocol() const = 0;

 protected:
  explicit PrestoToBoltConnector(std::string connectorName)
      : connectorName_(std::move(connectorName)) {}
  const std::string connectorName_;
};

class HivePrestoToBoltConnector final : public PrestoToBoltConnector {
 public:
  explicit HivePrestoToBoltConnector(std::string connectorName)
      : PrestoToBoltConnector(std::move(connectorName)) {}

  std::unique_ptr<bytedance::bolt::connector::ConnectorSplit> toBoltSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  std::unique_ptr<bytedance::bolt::connector::ColumnHandle> toBoltColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  std::unique_ptr<bytedance::bolt::connector::ConnectorTableHandle> toBoltTableHandle(
      const protocol::TableHandle& tableHandle,
      const BoltExprConverter& exprConverter,
      const TypeParser& typeParser,
      std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>& assignments)
      const final;

  std::unique_ptr<bytedance::bolt::connector::ConnectorInsertTableHandle>
  toBoltInsertTableHandle(
      const protocol::CreateHandle* createHandle,
      const TypeParser& typeParser) const final;

  std::unique_ptr<bytedance::bolt::connector::ConnectorInsertTableHandle>
  toBoltInsertTableHandle(
      const protocol::InsertHandle* insertHandle,
      const TypeParser& typeParser) const final;

  std::unique_ptr<bytedance::bolt::core::PartitionFunctionSpec>
  createBoltPartitionFunctionSpec(
      const protocol::ConnectorPartitioningHandle* partitioningHandle,
      const std::vector<int>& bucketToPartition,
      const std::vector<bytedance::bolt::column_index_t>& channels,
      const std::vector<bytedance::bolt::VectorPtr>& constValues,
      bool& effectivelyGather) const final;

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;

 private:
  std::vector<std::shared_ptr<const bytedance::bolt::connector::hive::HiveColumnHandle>>
  toHiveColumns(
      const protocol::List<protocol::hive::HiveColumnHandle>& inputColumns,
      const TypeParser& typeParser,
      bool& hasPartitionColumn) const;
};

class IcebergPrestoToBoltConnector final : public PrestoToBoltConnector {
 public:
  explicit IcebergPrestoToBoltConnector(std::string connectorName)
      : PrestoToBoltConnector(std::move(connectorName)) {}

  std::unique_ptr<bytedance::bolt::connector::ConnectorSplit> toBoltSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  std::unique_ptr<bytedance::bolt::connector::ColumnHandle> toBoltColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  std::unique_ptr<bytedance::bolt::connector::ConnectorTableHandle> toBoltTableHandle(
      const protocol::TableHandle& tableHandle,
      const BoltExprConverter& exprConverter,
      const TypeParser& typeParser,
      std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>& assignments)
      const final;

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;
};

class TpchPrestoToBoltConnector final : public PrestoToBoltConnector {
 public:
  explicit TpchPrestoToBoltConnector(std::string connectorName)
      : PrestoToBoltConnector(std::move(connectorName)) {}

  std::unique_ptr<bytedance::bolt::connector::ConnectorSplit> toBoltSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  std::unique_ptr<bytedance::bolt::connector::ColumnHandle> toBoltColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  std::unique_ptr<bytedance::bolt::connector::ConnectorTableHandle> toBoltTableHandle(
      const protocol::TableHandle& tableHandle,
      const BoltExprConverter& exprConverter,
      const TypeParser& typeParser,
      std::unordered_map<
          std::string,
          std::shared_ptr<bytedance::bolt::connector::ColumnHandle>>& assignments)
      const final;

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;
};
} // namespace facebook::presto
