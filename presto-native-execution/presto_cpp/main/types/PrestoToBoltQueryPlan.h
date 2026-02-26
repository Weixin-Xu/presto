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

#include <stdexcept>
#include <vector>
#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "bolt/core/Expressions.h"
#include "bolt/core/PlanFragment.h"
#include "bolt/core/PlanNode.h"
#include "bolt/type/Variant.h"

#include "presto_cpp/main/types/PrestoTaskId.h"
#include "presto_cpp/main/types/PrestoToBoltExpr.h"
#include "presto_cpp/main/types/TypeParser.h"

namespace facebook::presto {

class BoltQueryPlanConverterBase {
 public:
  BoltQueryPlanConverterBase(
      bytedance::bolt::core::QueryCtx* queryCtx,
      bytedance::bolt::memory::MemoryPool* pool)
      : pool_(pool), queryCtx_{queryCtx}, exprConverter_(pool, &typeParser_) {}

  virtual ~BoltQueryPlanConverterBase() = default;

  virtual bytedance::bolt::core::PlanFragment toBoltQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  // visible for testing
  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::PlanNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

 protected:
  virtual bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) = 0;

  virtual bytedance::bolt::connector::CommitStrategy getCommitStrategy() const = 0;

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::OutputNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::ExchangeNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::FilterNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::ProjectNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::ProjectNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::ValuesNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::ValuesNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::TableScanNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::TableScanNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::AggregationNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::AggregationNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::GroupIdNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::GroupIdNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::DistinctLimitNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::JoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::SemiJoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::MarkDistinctNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::MergeJoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::TopNNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::TopNNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::LimitNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::LimitNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::OrderByNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::SortNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::TableWriteNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::TableWriterNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::TableWriteMergeNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::TableWriterMergeNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::UnnestNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::UnnestNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::EnforceSingleRowNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::EnforceSingleRowNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::AssignUniqueIdNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::AssignUniqueId>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::WindowNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::WindowNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::RowNumberNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::RowNumberNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const bytedance::bolt::core::PlanNode> toBoltQueryPlan(
      const std::shared_ptr<const protocol::TopNRowNumberNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::vector<bytedance::bolt::core::FieldAccessTypedExprPtr> toBoltExprs(
      const std::vector<protocol::VariableReferenceExpression>& variables);

  std::shared_ptr<const bytedance::bolt::core::ProjectNode> tryConvertOffsetLimit(
      const std::shared_ptr<const protocol::ProjectNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  bytedance::bolt::core::WindowNode::Function toBoltWindowFunction(
      const protocol::Function& func);

  bytedance::bolt::VectorPtr evaluateConstantExpression(
      const bytedance::bolt::core::TypedExprPtr& expression);

  std::shared_ptr<bytedance::bolt::core::AggregationNode> generateAggregationNode(
      const std::shared_ptr<protocol::StatisticAggregations>&
          statisticsAggregation,
      bytedance::bolt::core::AggregationNode::Step step,
      const protocol::PlanNodeId& id,
      const bytedance::bolt::core::PlanNodePtr& sourceBoltPlan,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::vector<protocol::VariableReferenceExpression> generateOutputVariables(
      const std::vector<protocol::VariableReferenceExpression>&
          nonStatisticsOutputVariables,
      const std::shared_ptr<protocol::StatisticAggregations>&
          statisticsAggregation);

  void toAggregations(
      const std::vector<protocol::VariableReferenceExpression>& outputVariables,
      const std::map<
          protocol::VariableReferenceExpression,
          protocol::Aggregation>& aggregationMap,
      std::vector<bytedance::bolt::core::AggregationNode::Aggregate>& aggregates,
      std::vector<std::string>& aggregateNames);

  bytedance::bolt::memory::MemoryPool* const pool_;
  bytedance::bolt::core::QueryCtx* const queryCtx_;
  BoltExprConverter exprConverter_;
  TypeParser typeParser_;
};

class BoltInteractiveQueryPlanConverter : public BoltQueryPlanConverterBase {
 public:
  using BoltQueryPlanConverterBase::toBoltQueryPlan;

  explicit BoltInteractiveQueryPlanConverter(
      bytedance::bolt::core::QueryCtx* queryCtx,
      bytedance::bolt::memory::MemoryPool* pool)
      : BoltQueryPlanConverterBase(queryCtx, pool) {}

 protected:
  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) override;

  bytedance::bolt::connector::CommitStrategy getCommitStrategy() const override;
};

class BoltBatchQueryPlanConverter : public BoltQueryPlanConverterBase {
 public:
  using BoltQueryPlanConverterBase::toBoltQueryPlan;

  BoltBatchQueryPlanConverter(
      const std::string& shuffleName,
      std::shared_ptr<std::string>&& serializedShuffleWriteInfo,
      std::shared_ptr<std::string>&& broadcastBasePath,
      bytedance::bolt::core::QueryCtx* queryCtx,
      bytedance::bolt::memory::MemoryPool* pool)
      : BoltQueryPlanConverterBase(queryCtx, pool),
        shuffleName_(shuffleName),
        serializedShuffleWriteInfo_(std::move(serializedShuffleWriteInfo)),
        broadcastBasePath_(std::move(broadcastBasePath)) {}

  bytedance::bolt::core::PlanFragment toBoltQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) override;

 protected:
  bytedance::bolt::core::PlanNodePtr toBoltQueryPlan(
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) override;

  bytedance::bolt::connector::CommitStrategy getCommitStrategy() const override;

 private:
  const std::string shuffleName_;
  const std::shared_ptr<std::string> serializedShuffleWriteInfo_;
  const std::shared_ptr<std::string> broadcastBasePath_;
};

void registerPrestoPlanNodeSerDe();

void parseSqlFunctionHandle(
    const std::shared_ptr<protocol::SqlFunctionHandle>& sqlFunction,
    std::vector<bytedance::bolt::TypePtr>& rawInputTypes,
    TypeParser& typeParser);
} // namespace facebook::presto
