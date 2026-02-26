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

#include "bolt/core/PlanNode.h"
#include "bolt/exec/Operator.h"

namespace facebook::presto::operators {

/// Partitions the input row based on partition function and serializes the
/// entire row using UnsafeRow format. The output contains 2 columns: partition
/// number (INTEGER) and serialized row (VARBINARY). If 'replicateNullsAndAny'
/// is true, the output includes a third boolean column which indicates whether
/// a row needs to be replicated to all partitions.
class PartitionAndSerializeNode : public bytedance::bolt::core::PlanNode {
 public:
  PartitionAndSerializeNode(
      const bytedance::bolt::core::PlanNodeId& id,
      std::vector<bytedance::bolt::core::TypedExprPtr> keys,
      uint32_t numPartitions,
      bytedance::bolt::RowTypePtr serializedRowType,
      bytedance::bolt::core::PlanNodePtr source,
      bool replicateNullsAndAny,
      bytedance::bolt::core::PartitionFunctionSpecPtr partitionFunctionFactory)
      : bytedance::bolt::core::PlanNode(id),
        keys_(std::move(keys)),
        numPartitions_(numPartitions),
        serializedRowType_{std::move(serializedRowType)},
        sources_({std::move(source)}),
        replicateNullsAndAny_(replicateNullsAndAny),
        partitionFunctionSpec_(std::move(partitionFunctionFactory)) {
    BOLT_USER_CHECK_NOT_NULL(
        partitionFunctionSpec_, "Partition function factory cannot be null.");
  }

  folly::dynamic serialize() const override;

  static bytedance::bolt::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const bytedance::bolt::RowTypePtr& outputType() const override {
    static const bytedance::bolt::RowTypePtr kOutputType{bytedance::bolt::ROW(
        {"partition", "data"}, {bytedance::bolt::INTEGER(), bytedance::bolt::VARBINARY()})};

    static const bytedance::bolt::RowTypePtr kReplicateNullsAndAnyOutputType{bytedance::bolt::ROW(
        {"partition", "data", "replicate"},
        {bytedance::bolt::INTEGER(), bytedance::bolt::VARBINARY(), bytedance::bolt::BOOLEAN()})};

    return replicateNullsAndAny_ ? kReplicateNullsAndAnyOutputType
                                 : kOutputType;
  }

  const std::vector<bytedance::bolt::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::vector<bytedance::bolt::core::TypedExprPtr>& keys() const {
    return keys_;
  }

  uint32_t numPartitions() const {
    return numPartitions_;
  }

  const bytedance::bolt::RowTypePtr& serializedRowType() const {
    return serializedRowType_;
  }

  /// Returns true if an arbitrary row and all rows with null keys must be
  /// replicated to all destinations. This is used to ensure correct results for
  /// anti-join which requires all nodes to know whether combined build side is
  /// empty and whether it has any entry with null join key.
  bool isReplicateNullsAndAny() const {
    return replicateNullsAndAny_;
  }

  const bytedance::bolt::core::PartitionFunctionSpecPtr& partitionFunctionFactory()
      const {
    return partitionFunctionSpec_;
  }

  std::string_view name() const override {
    return "PartitionAndSerialize";
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<bytedance::bolt::core::TypedExprPtr> keys_;
  const uint32_t numPartitions_;
  const bytedance::bolt::RowTypePtr serializedRowType_;
  const std::vector<bytedance::bolt::core::PlanNodePtr> sources_;
  const bool replicateNullsAndAny_;
  const bytedance::bolt::core::PartitionFunctionSpecPtr partitionFunctionSpec_;
};

class PartitionAndSerializeTranslator
    : public bytedance::bolt::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<bytedance::bolt::exec::Operator> toOperator(
      bytedance::bolt::exec::DriverCtx* ctx,
      int32_t id,
      const bytedance::bolt::core::PlanNodePtr& node) override;
};
} // namespace facebook::presto::operators