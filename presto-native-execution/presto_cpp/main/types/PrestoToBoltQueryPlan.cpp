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

// clang-format off
#include "presto_cpp/main/types/PrestoToBoltConnector.h"
#include "presto_cpp/main/types/PrestoToBoltQueryPlan.h"
#include "bolt/type/Filter.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/HashPartitionFunction.h"
#include "bolt/exec/RoundRobinPartitionFunction.h"
#include "bolt/expression/Expr.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/core/Expressions.h"
// clang-format on

#include <folly/String.h>

#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/types/TypeParser.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace facebook::presto {

namespace {

TypePtr stringToType(
    const std::string& typeString,
    const TypeParser& typeParser) {
  return typeParser.parse(typeString);
}

std::vector<std::string> getNames(const protocol::Assignments& assignments) {
  std::vector<std::string> names;
  names.reserve(assignments.assignments.size());

  for (const auto& assignment : assignments.assignments) {
    names.emplace_back(assignment.first.name);
  }

  return names;
}

RowTypePtr toRowType(
    const std::vector<protocol::VariableReferenceExpression>& variables,
    const TypeParser& typeParser,
    const std::unordered_set<std::string>& excludeNames = {}) {
  std::vector<std::string> names;
  std::vector<bytedance::bolt::TypePtr> types;
  names.reserve(variables.size());
  types.reserve(variables.size());

  for (const auto& variable : variables) {
    if (excludeNames.count(variable.name)) {
      continue;
    }
    names.emplace_back(variable.name);
    types.emplace_back(stringToType(variable.type, typeParser));
  }

  return ROW(std::move(names), std::move(types));
}

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
}

std::shared_ptr<connector::ColumnHandle> toColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) {
  const auto& connector = getPrestoToBoltConnector(column->_type);
  return connector.toBoltColumnHandle(column, typeParser);
}

std::shared_ptr<connector::ConnectorTableHandle> toConnectorTableHandle(
    const protocol::TableHandle& tableHandle,
    const BoltExprConverter& exprConverter,
    const TypeParser& typeParser,
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>&
        assignments) {
  const auto& connector =
      getPrestoToBoltConnector(tableHandle.connectorHandle->_type);
  return connector.toBoltTableHandle(
      tableHandle, exprConverter, typeParser, assignments);
}

std::vector<core::TypedExprPtr> getProjections(
    const BoltExprConverter& exprConverter,
    const protocol::Assignments& assignments) {
  std::vector<core::TypedExprPtr> expressions;
  expressions.reserve(assignments.assignments.size());
  for (const auto& assignment : assignments.assignments) {
    expressions.emplace_back(exprConverter.toBoltExpr(assignment.second));
  }

  return expressions;
}

template <TypeKind KIND>
void setCellFromVariantByKind(
    const VectorPtr& column,
    vector_size_t row,
    const bytedance::bolt::variant& value) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = column->as<FlatVector<T>>();
  flatVector->set(row, value.value<T>());
}

template <>
void setCellFromVariantByKind<TypeKind::VARBINARY>(
    const VectorPtr& column,
    vector_size_t row,
    const bytedance::bolt::variant& value) {
  auto values = column->as<FlatVector<StringView>>();
  values->set(row, StringView(value.value<TypeKind::VARBINARY>()));
}

template <>
void setCellFromVariantByKind<TypeKind::VARCHAR>(
    const VectorPtr& column,
    vector_size_t row,
    const bytedance::bolt::variant& value) {
  auto values = column->as<FlatVector<StringView>>();
  values->set(row, StringView(value.value<TypeKind::VARCHAR>()));
}

void setCellFromVariant(
    const RowVectorPtr& data,
    vector_size_t row,
    vector_size_t column,
    const bytedance::bolt::variant& value) {
  auto columnVector = data->childAt(column);
  if (value.isNull()) {
    columnVector->setNull(row, true);
    return;
  }
  if (columnVector->typeKind() == TypeKind::HUGEINT) {
    setCellFromVariantByKind<TypeKind::HUGEINT>(columnVector, row, value);
    return;
  }
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setCellFromVariantByKind,
      columnVector->typeKind(),
      columnVector,
      row,
      value);
}

void setCellFromVariant(
    const VectorPtr& data,
    vector_size_t row,
    const bytedance::bolt::variant& value) {
  if (value.isNull()) {
    data->setNull(row, true);
    return;
  }
  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setCellFromVariantByKind, data->typeKind(), data, row, value);
}

core::SortOrder toBoltSortOrder(const protocol::SortOrder& sortOrder) {
  switch (sortOrder) {
    case protocol::SortOrder::ASC_NULLS_FIRST:
      return core::SortOrder(true, true);
    case protocol::SortOrder::ASC_NULLS_LAST:
      return core::SortOrder(true, false);
    case protocol::SortOrder::DESC_NULLS_FIRST:
      return core::SortOrder(false, true);
    case protocol::SortOrder::DESC_NULLS_LAST:
      return core::SortOrder(false, false);
    default:
      BOLT_UNSUPPORTED(
          "Unsupported sort order: {}.", fmt::underlying(sortOrder));
  }
}

bool isFixedPartition(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    protocol::SystemPartitionFunction partitionFunction) {
  if (node->type != protocol::ExchangeNodeType::REPARTITION) {
    return false;
  }

  auto connectorHandle =
      node->partitioningScheme.partitioning.handle.connectorHandle;
  auto handle = std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
      connectorHandle);
  if (!handle) {
    return false;
  }
  if (handle->partitioning != protocol::SystemPartitioning::FIXED) {
    return false;
  }
  if (handle->function != partitionFunction) {
    return false;
  }
  return true;
}

bool isHashPartition(
    const std::shared_ptr<const protocol::ExchangeNode>& node) {
  return isFixedPartition(node, protocol::SystemPartitionFunction::HASH);
}

bool isRoundRobinPartition(
    const std::shared_ptr<const protocol::ExchangeNode>& node) {
  return isFixedPartition(node, protocol::SystemPartitionFunction::ROUND_ROBIN);
}

std::vector<core::FieldAccessTypedExprPtr> toFieldExprs(
    const std::vector<std::shared_ptr<protocol::RowExpression>>& expressions,
    const BoltExprConverter& exprConverter) {
  std::vector<core::FieldAccessTypedExprPtr> fields;
  fields.reserve(expressions.size());
  for (const auto& expr : expressions) {
    auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
        exprConverter.toBoltExpr(expr));
    BOLT_CHECK_NOT_NULL(
        field,
        "Unexpected expression type: {}. Expected variable.",
        expr->_type);
    fields.emplace_back(std::move(field));
  }
  return fields;
}

std::vector<core::TypedExprPtr> toTypedExprs(
    const std::vector<std::shared_ptr<protocol::RowExpression>>& expressions,
    const BoltExprConverter& exprConverter) {
  std::vector<core::TypedExprPtr> typedExprs;
  typedExprs.reserve(expressions.size());
  for (const auto& expr : expressions) {
    auto typedExpr = exprConverter.toBoltExpr(expr);
    auto field =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
    if (field == nullptr) {
      auto constant =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(typedExpr);
      BOLT_CHECK_NOT_NULL(
          constant,
          "Unexpected expression type: {}. Expected variable or constant.",
          expr->_type);
    }
    typedExprs.emplace_back(std::move(typedExpr));
  }
  return typedExprs;
}

std::vector<column_index_t> toChannels(
    const RowTypePtr& type,
    const std::vector<core::FieldAccessTypedExprPtr>& fields) {
  std::vector<column_index_t> channels;
  channels.reserve(fields.size());
  for (const auto& field : fields) {
    auto channel = type->getChildIdx(field->name());
    channels.emplace_back(channel);
  }
  return channels;
}

column_index_t exprToChannel(
    const core::ITypedExpr* expr,
    const TypePtr& type) {
  if (auto field = dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
    return type->as<TypeKind::ROW>().getChildIdx(field->name());
  }
  if (dynamic_cast<const core::ConstantTypedExpr*>(expr)) {
    return kConstantChannel;
  }
  BOLT_CHECK(false, "Expression must be field access or constant");
  return 0; // not reached.
}

core::WindowNode::WindowType toBoltWindowType(
    protocol::WindowType windowType) {
  switch (windowType) {
    case protocol::WindowType::RANGE:
      return core::WindowNode::WindowType::kRange;
    case protocol::WindowType::ROWS:
      return core::WindowNode::WindowType::kRows;
    default:
      BOLT_UNSUPPORTED(
          "Unsupported window type: {}", fmt::underlying(windowType));
  }
}

core::WindowNode::BoundType toBoltBoundType(protocol::BoundType boundType) {
  switch (boundType) {
    case protocol::BoundType::CURRENT_ROW:
      return core::WindowNode::BoundType::kCurrentRow;
    case protocol::BoundType::PRECEDING:
      return core::WindowNode::BoundType::kPreceding;
    case protocol::BoundType::FOLLOWING:
      return core::WindowNode::BoundType::kFollowing;
    case protocol::BoundType::UNBOUNDED_PRECEDING:
      return core::WindowNode::BoundType::kUnboundedPreceding;
    case protocol::BoundType::UNBOUNDED_FOLLOWING:
      return core::WindowNode::BoundType::kUnboundedFollowing;
    default:
      BOLT_UNSUPPORTED(
          "Unsupported window bound type: {}", fmt::underlying(boundType));
  }
}

core::LocalPartitionNode::Type toLocalExchangeType(
    protocol::ExchangeNodeType type) {
  switch (type) {
    case protocol::ExchangeNodeType::GATHER:
      return core::LocalPartitionNode::Type::kGather;
    case protocol::ExchangeNodeType::REPARTITION:
      return core::LocalPartitionNode::Type::kRepartition;
    default:
      BOLT_UNSUPPORTED("Unsupported exchange type: {}", toJsonString(type));
  }
}

/* VectorSerde::Kind toBoltSerdeKind(protocol::ExchangeEncoding encoding) {
  switch (encoding) {
    case protocol::ExchangeEncoding::COLUMNAR:
      return VectorSerde::Kind::kPresto;
    case protocol::ExchangeEncoding::ROW_WISE:
      return VectorSerde::Kind::kCompactRow;
  }
  BOLT_UNSUPPORTED("Unsupported encoding: {}.", fmt::underlying(encoding));
}*/

std::shared_ptr<core::LocalPartitionNode> buildLocalSystemPartitionNode(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    core::LocalPartitionNode::Type type,
    bool scaleWriters,
    const RowTypePtr& outputType,
    std::vector<core::PlanNodePtr>&& sourceNodes,
    const BoltExprConverter& exprConverter) {
  if (isHashPartition(node)) {
    auto partitionKeys = toFieldExprs(
        node->partitioningScheme.partitioning.arguments, exprConverter);
    auto keyChannels = toChannels(outputType, partitionKeys);
    return std::make_shared<core::LocalPartitionNode>(
        node->id,
        type,
        //scaleWriters,
        std::make_shared<HashPartitionFunctionSpec>(outputType, keyChannels),
        std::move(sourceNodes));
  }

  if (isRoundRobinPartition(node)) {
    return std::make_shared<core::LocalPartitionNode>(
        node->id,
        type,
        //scaleWriters,
        std::make_shared<RoundRobinPartitionFunctionSpec>(),
        std::move(sourceNodes));
  }

  BOLT_UNSUPPORTED(
      "Unsupported flavor of local exchange with system partitioning handle: {}",
      toJsonString(node));
}
} // namespace

core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  BOLT_USER_CHECK(
      node->scope == protocol::ExchangeNodeScope::LOCAL,
      "Unsupported ExchangeNode scope");

  std::vector<core::PlanNodePtr> sourceNodes;
  sourceNodes.reserve(node->sources.size());
  for (const auto& source : node->sources) {
    sourceNodes.emplace_back(toBoltQueryPlan(source, tableWriteInfo, taskId));
  }

  if (node->orderingScheme) {
    std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
    std::vector<core::SortOrder> sortingOrders;
    sortingKeys.reserve(node->orderingScheme->orderBy.size());
    sortingOrders.reserve(node->orderingScheme->orderBy.size());
    for (const auto& orderBy : node->orderingScheme->orderBy) {
      sortingKeys.emplace_back(exprConverter_.toBoltExpr(orderBy.variable));
      sortingOrders.emplace_back(toBoltSortOrder(orderBy.sortOrder));
    }
    return std::make_shared<core::LocalMergeNode>(
        node->id, sortingKeys, sortingOrders, std::move(sourceNodes));
  }

  const auto type = toLocalExchangeType(node->type);

  const auto outputType =
      toRowType(node->partitioningScheme.outputLayout, typeParser_);

  // Different source nodes may have different output layouts.
  // Add ProjectNode on top of each source node to re-arrange the output columns
  // to match the output layout of the LocalExchangeNode.
  for (auto i = 0; i < sourceNodes.size(); ++i) {
    auto names = outputType->names();
    std::vector<core::TypedExprPtr> projections;
    projections.reserve(outputType->size());

    const auto desiredSourceOutput = toRowType(node->inputs[i], typeParser_);

    for (auto j = 0; j < outputType->size(); j++) {
      projections.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
          outputType->childAt(j), desiredSourceOutput->nameOf(j)));
    }

    sourceNodes[i] = std::make_shared<core::ProjectNode>(
        fmt::format("{}.{}", node->id, i),
        std::move(names),
        std::move(projections),
        sourceNodes[i]);
  }

  if (type == core::LocalPartitionNode::Type::kGather) {
    return core::LocalPartitionNode::gather(node->id, std::move(sourceNodes));
  }

  const auto& partitioningScheme = node->partitioningScheme;
  const auto& connectorHandle =
      partitioningScheme.partitioning.handle.connectorHandle;
  const bool scaleWriters = partitioningScheme.scaleWriters;
  if (std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
          connectorHandle) != nullptr) {
    return buildLocalSystemPartitionNode(
        node,
        type,
        scaleWriters,
        outputType,
        std::move(sourceNodes),
        exprConverter_);
  }

  auto partitionKeys = toFieldExprs(
      node->partitioningScheme.partitioning.arguments, exprConverter_);
  auto keyChannels = toChannels(outputType, partitionKeys);

  auto& connector = getPrestoToBoltConnector(connectorHandle->_type);
  bool effectivelyGather{false};
  auto spec = connector.createBoltPartitionFunctionSpec(
      connectorHandle.get(),
      {},
      keyChannels,
      std::vector<VectorPtr>{},
      effectivelyGather);
  if (effectivelyGather) {
    return core::LocalPartitionNode::gather(node->id, std::move(sourceNodes));
  }
  return std::make_shared<core::LocalPartitionNode>(
      node->id,
      type,
      //scaleWriters,
      std::shared_ptr(std::move(spec)),
      std::move(sourceNodes));
}

namespace {
bool equal(
    const std::shared_ptr<protocol::RowExpression>& actual,
    const protocol::VariableReferenceExpression& expected) {
  if (auto variableReference =
          std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
              actual)) {
    return (
        variableReference->name == expected.name &&
        variableReference->type == expected.type);
  }
  return false;
}

std::shared_ptr<protocol::CallExpression> isFunctionCall(
    const std::shared_ptr<protocol::RowExpression>& expression,
    const std::string_view& functionName) {
  if (auto call =
          std::dynamic_pointer_cast<protocol::CallExpression>(expression)) {
    if (auto builtin =
            std::dynamic_pointer_cast<protocol::BuiltInFunctionHandle>(
                call->functionHandle)) {
      if (builtin->signature.kind == protocol::FunctionKind::SCALAR &&
          builtin->signature.name == functionName) {
        return call;
      }
    }
  }
  return nullptr;
}

/// Check if input RowExpression is a 'NOT x' expression and returns it as
/// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isNot(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static const std::string_view kNot = "presto.default.not";
  return isFunctionCall(expression, kNot);
}

/// Check if input RowExpression is an 'a > b' expression and returns it as
/// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isGreaterThan(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static const std::string_view kGreaterThan =
      "presto.default.$operator$greater_than";
  return isFunctionCall(expression, kGreaterThan);
}

/// Checks if input PlanNode represents a local exchange with single source and
/// returns it as ExchangeNode. Returns nullptr if input node is something else.
std::shared_ptr<const protocol::ExchangeNode> isLocalSingleSourceExchange(
    const std::shared_ptr<const protocol::PlanNode>& node) {
  if (auto exchange =
          std::dynamic_pointer_cast<const protocol::ExchangeNode>(node)) {
    if (exchange->scope == protocol::ExchangeNodeScope::LOCAL &&
        exchange->sources.size() == 1) {
      return exchange;
    }
  }

  return nullptr;
}

/// Checks if input PlanNode represents an identity projection and returns it as
/// ProjectNode. Returns nullptr if input node is something else.
std::shared_ptr<const protocol::ProjectNode> isIdentityProjection(
    const std::shared_ptr<const protocol::PlanNode>& node) {
  if (auto project =
          std::dynamic_pointer_cast<const protocol::ProjectNode>(node)) {
    for (auto entry : project->assignments.assignments) {
      if (!equal(entry.second, entry.first)) {
        return nullptr;
      }
    }
    return project;
  }

  return nullptr;
}
} // namespace

core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::FilterNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // In Presto, semi and anti joins are implemented using two operators:
  // SemiJoin followed by Filter. SemiJoin operator returns all probe rows plus
  // an extra boolean column which indicates whether there is a match for a
  // given row. Then a filter on the boolean column is used to select a subset
  // of probe rows that match (semi join) or don't match (anti join).
  //
  // In Bolt, semi and anti joins are implemented using a single operator:
  // HashJoin which returns a subset of probe rows that match (semi) or don't
  // match (anti) the build side. Hence, we convert FilterNode over SemiJoinNode
  // into ProjectNode over HashJoinNode. Project node adds an extra boolean
  // column with constant value of 'true' for semi join and 'false' for anti
  // join.
  if (auto semiJoin = std::dynamic_pointer_cast<const protocol::SemiJoinNode>(
          node->source)) {
    std::optional<core::JoinType> joinType = std::nullopt;
    if (equal(node->predicate, semiJoin->semiJoinOutput)) {
      joinType = core::JoinType::kLeftSemiFilter;
    } else if (auto notCall = isNot(node->predicate)) {
      if (equal(notCall->arguments[0], semiJoin->semiJoinOutput)) {
        joinType = core::JoinType::kAnti;
      }
    }

    // No clear join type - fallback to the standard 'to bolt expr'.
    if (!joinType.has_value()) {
      return std::make_shared<core::FilterNode>(
          node->id,
          exprConverter_.toBoltExpr(node->predicate),
          toBoltQueryPlan(semiJoin, tableWriteInfo, taskId));
    }

    std::vector<core::FieldAccessTypedExprPtr> leftKeys = {
        exprConverter_.toBoltExpr(semiJoin->sourceJoinVariable)};
    std::vector<core::FieldAccessTypedExprPtr> rightKeys = {
        exprConverter_.toBoltExpr(semiJoin->filteringSourceJoinVariable)};

    auto left = toBoltQueryPlan(semiJoin->source, tableWriteInfo, taskId);
    auto right =
        toBoltQueryPlan(semiJoin->filteringSource, tableWriteInfo, taskId);

    const auto& leftNames = left->outputType()->names();
    const auto& leftTypes = left->outputType()->children();

    auto names = leftNames;
    names.push_back(semiJoin->semiJoinOutput.name);

    std::vector<core::TypedExprPtr> projections;
    projections.reserve(leftNames.size() + 1);
    for (auto i = 0; i < leftNames.size(); i++) {
      projections.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
          leftTypes[i], leftNames[i]));
    }
    const bool constantValue =
        joinType.value() == core::JoinType::kLeftSemiFilter;
    projections.emplace_back(
        std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), constantValue));

    return std::make_shared<core::ProjectNode>(
        node->id,
        std::move(names),
        std::move(projections),
        std::make_shared<core::HashJoinNode>(
            semiJoin->id,
            joinType.value(),
            joinType == core::JoinType::kAnti ? true : false,
            leftKeys,
            rightKeys,
            nullptr, // filter
            left,
            right,
            left->outputType()));
  }

  return std::make_shared<core::FilterNode>(
      node->id,
      exprConverter_.toBoltExpr(node->predicate),
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::ProjectNode>
BoltQueryPlanConverterBase::tryConvertOffsetLimit(
    const std::shared_ptr<const protocol::ProjectNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // Presto plans OFFSET n LIMIT m queries as
  // Project(drop row_number column)
  //  -> LocalExchange(1-to-N)
  //    -> Limit(m)
  //      -> LocalExchange(N-to-1)
  //        -> Filter(rowNumber > n)
  //          -> LocalExchange(1-to-N)
  //            -> RowNumberNode
  // Bolt supports OFFSET-LIMIT via a single LimitNode(n, m).
  //
  // Detect the pattern above and convert it to:
  // Project(as-is)
  //  -> Limit(n-m)

  // TODO Relax the check to only ensure that no expression is using row_number
  // column.
  auto identityProjections = isIdentityProjection(node);
  if (!identityProjections) {
    return nullptr;
  }

  auto exchangeBeforeProject = isLocalSingleSourceExchange(node->source);
  if (!exchangeBeforeProject || !isRoundRobinPartition(exchangeBeforeProject)) {
    return nullptr;
  }

  auto limit = std::dynamic_pointer_cast<protocol::LimitNode>(
      exchangeBeforeProject->sources[0]);
  if (!limit) {
    return nullptr;
  }

  auto exchangeBeforeLimit = isLocalSingleSourceExchange(limit->source);
  if (!exchangeBeforeLimit) {
    return nullptr;
  }

  auto filter = std::dynamic_pointer_cast<const protocol::FilterNode>(
      exchangeBeforeLimit->sources[0]);
  if (!filter) {
    return nullptr;
  }

  auto exchangeBeforeFilter = isLocalSingleSourceExchange(filter->source);
  if (!exchangeBeforeFilter) {
    return nullptr;
  }

  auto rowNumber = std::dynamic_pointer_cast<const protocol::RowNumberNode>(
      exchangeBeforeFilter->sources[0]);
  if (!rowNumber) {
    return nullptr;
  }

  auto rowNumberVariable = rowNumber->rowNumberVariable;

  auto gt = isGreaterThan(filter->predicate);
  if (gt && equal(gt->arguments[0], rowNumberVariable)) {
    auto offsetExpr = exprConverter_.toBoltExpr(gt->arguments[1]);
    if (auto offsetConstExpr =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                offsetExpr)) {
      if (!offsetConstExpr->type()->isBigint()) {
        return nullptr;
      }

      auto offset = offsetConstExpr->value().value<int64_t>();

      // Check that Project node drops row_number column.
      for (auto entry : node->assignments.assignments) {
        if (equal(entry.second, rowNumberVariable)) {
          return nullptr;
        }
      }

      return std::make_shared<core::ProjectNode>(
          node->id,
          getNames(node->assignments),
          getProjections(exprConverter_, node->assignments),
          std::make_shared<core::LimitNode>(
              limit->id,
              offset,
              limit->count,
              limit->step == protocol::LimitNodeStep::PARTIAL,
              toBoltQueryPlan(rowNumber->source, tableWriteInfo, taskId)));
    }
  }

  return nullptr;
}

std::shared_ptr<const core::ProjectNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::ProjectNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  if (auto limit = tryConvertOffsetLimit(node, tableWriteInfo, taskId)) {
    return limit;
  }

  return std::make_shared<core::ProjectNode>(
      node->id,
      getNames(node->assignments),
      getProjections(exprConverter_, node->assignments),
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

bytedance::bolt::VectorPtr BoltQueryPlanConverterBase::evaluateConstantExpression(
    const bytedance::bolt::core::TypedExprPtr& expression) {
  auto emptyRowVector = BaseVector::create<RowVector>(ROW({}), 1, pool_);
  core::ExecCtx execCtx{pool_, queryCtx_};
  exec::ExprSet exprSet{{expression}, &execCtx};
  exec::EvalCtx context(&execCtx, &exprSet, emptyRowVector.get());

  SelectivityVector rows{1};
  std::vector<VectorPtr> result(1);
  exprSet.eval(rows, context, result);
  return result[0];
}

std::shared_ptr<core::AggregationNode>
BoltQueryPlanConverterBase::generateAggregationNode(
    const std::shared_ptr<protocol::StatisticAggregations>&
        statisticsAggregation,
    core::AggregationNode::Step step,
    const protocol::PlanNodeId& id,
    const core::PlanNodePtr& sourceBoltPlan,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  if (statisticsAggregation == nullptr) {
    return nullptr;
  }
  auto outputVariables = statisticsAggregation->outputVariables;
  auto aggregationMap = statisticsAggregation->aggregations;
  auto groupingVariables = statisticsAggregation->groupingVariables;
  BOLT_CHECK_EQ(
      aggregationMap.size(),
      outputVariables.size(),
      "TableWriterNode's aggregations and outputVariables should be the same size");
  BOLT_CHECK(
      !outputVariables.empty(),
      "TableWriterNode's outputVariables shouldn't be empty");

  std::vector<std::string> aggregateNames;
  std::vector<core::AggregationNode::Aggregate> aggregates;
  toAggregations(outputVariables, aggregationMap, aggregates, aggregateNames);

  return std::make_shared<core::AggregationNode>(
      id,
      step,
      toBoltExprs(statisticsAggregation->groupingVariables),
      std::vector<core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      false, // ignoreNullKeys
      sourceBoltPlan);
}

std::vector<protocol::VariableReferenceExpression>
BoltQueryPlanConverterBase::generateOutputVariables(
    const std::vector<protocol::VariableReferenceExpression>&
        nonStatisticsOutputVariables,
    const std::shared_ptr<protocol::StatisticAggregations>&
        statisticsAggregation) {
  std::vector<protocol::VariableReferenceExpression> outputVariables;
  outputVariables.insert(
      outputVariables.end(),
      nonStatisticsOutputVariables.begin(),
      nonStatisticsOutputVariables.end());
  if (statisticsAggregation == nullptr) {
    return outputVariables;
  }
  auto statisticsOutputVariables = statisticsAggregation->outputVariables;
  auto statisticsGroupingVariables = statisticsAggregation->groupingVariables;
  outputVariables.insert(
      outputVariables.end(),
      statisticsGroupingVariables.begin(),
      statisticsGroupingVariables.end());
  for (auto const& variable : statisticsOutputVariables) {
    outputVariables.push_back(variable);
  }
  return outputVariables;
}

void BoltQueryPlanConverterBase::toAggregations(
    const std::vector<protocol::VariableReferenceExpression>& outputVariables,
    const std::map<
        protocol::VariableReferenceExpression,
        protocol::Aggregation>& aggregationMap,
    std::vector<bytedance::bolt::core::AggregationNode::Aggregate>& aggregates,
    std::vector<std::string>& aggregateNames) {
  aggregateNames.reserve(aggregates.size());
  aggregates.reserve(aggregates.size());
  for (const auto& entry : outputVariables) {
    aggregateNames.emplace_back(entry.name);

    const auto& prestoAggregation = aggregationMap.at(entry);

    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        exprConverter_.toBoltExpr(prestoAggregation.call));

    if (auto builtin =
            std::dynamic_pointer_cast<protocol::BuiltInFunctionHandle>(
                prestoAggregation.functionHandle)) {
      const auto& signature = builtin->signature;
      aggregate.rawInputTypes.reserve(signature.argumentTypes.size());
      for (const auto& argumentType : signature.argumentTypes) {
        aggregate.rawInputTypes.push_back(
            stringToType(argumentType, typeParser_));
      }
    } else if (
        auto sqlFunction =
            std::dynamic_pointer_cast<protocol::SqlFunctionHandle>(
                prestoAggregation.functionHandle)) {
      parseSqlFunctionHandle(sqlFunction, aggregate.rawInputTypes, typeParser_);
    } else {
      BOLT_USER_FAIL(
          "Unsupported aggregate function handle: {}",
          toJsonString(prestoAggregation.functionHandle));
    }

    aggregate.distinct = prestoAggregation.distinct;

    if (prestoAggregation.mask != nullptr) {
      aggregate.mask = exprConverter_.toBoltExpr(prestoAggregation.mask);
    }

    if (prestoAggregation.orderBy != nullptr) {
      for (const auto& orderBy : prestoAggregation.orderBy->orderBy) {
        aggregate.sortingKeys.emplace_back(
            exprConverter_.toBoltExpr(orderBy.variable));
        aggregate.sortingOrders.emplace_back(
            toBoltSortOrder(orderBy.sortOrder));
      }
    }

    aggregates.emplace_back(aggregate);
  }
}

std::shared_ptr<const core::ValuesNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::ValuesNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  vector_size_t numRows = node->rows.size();
  auto numColumns = rowType->size();
  std::vector<bytedance::bolt::VectorPtr> vectors;
  vectors.reserve(numColumns);

  for (int i = 0; i < numColumns; ++i) {
    auto base = bytedance::bolt::BaseVector::create(rowType->childAt(i), numRows, pool_);
    vectors.emplace_back(base);
  }

  auto rowVector = std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(), numRows, std::move(vectors), 0);

  for (int row = 0; row < numRows; ++row) {
    for (int column = 0; column < numColumns; ++column) {
      auto expr = exprConverter_.toBoltExpr(node->rows[row][column]);

      if (auto constantExpr =
              std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
        if (!constantExpr->hasValueVector()) {
          setCellFromVariant(rowVector, row, column, constantExpr->value());
        } else {
          auto& columnVector = rowVector->childAt(column);
          columnVector->copy(constantExpr->valueVector().get(), row, 0, 1);
        }
      } else {
        // Evaluate the expression.
        auto value = evaluateConstantExpression(expr);

        auto& columnVector = rowVector->childAt(column);
        columnVector->copy(value.get(), row, 0, 1);
      }
    }
  }

  return std::make_shared<core::ValuesNode>(
      node->id, std::vector<RowVectorPtr>{rowVector});
}

std::shared_ptr<const core::TableScanNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::TableScanNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (const auto& entry : node->assignments) {
    assignments.emplace(
        entry.first.name, toColumnHandle(entry.second.get(), typeParser_));
  }
  auto connectorTableHandle = toConnectorTableHandle(
      node->table, exprConverter_, typeParser_, assignments);
  return std::make_shared<core::TableScanNode>(
      node->id, rowType, connectorTableHandle, assignments);
}

std::vector<core::FieldAccessTypedExprPtr>
BoltQueryPlanConverterBase::toBoltExprs(
    const std::vector<protocol::VariableReferenceExpression>& variables) {
  std::vector<core::FieldAccessTypedExprPtr> fields;
  fields.reserve(variables.size());
  for (const auto& variable : variables) {
    fields.emplace_back(exprConverter_.toBoltExpr(variable));
  }
  return fields;
}

std::shared_ptr<const core::AggregationNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::AggregationNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<std::string> aggregateNames;
  std::vector<core::AggregationNode::Aggregate> aggregates;

  std::vector<protocol::VariableReferenceExpression> outputVariables;
  for (auto it = node->aggregations.begin(); it != node->aggregations.end();
       it++) {
    outputVariables.push_back(it->first);
  }
  toAggregations(
      outputVariables, node->aggregations, aggregates, aggregateNames);

  core::AggregationNode::Step step;
  switch (node->step) {
    case protocol::AggregationNodeStep::PARTIAL:
      step = core::AggregationNode::Step::kPartial;
      break;
    case protocol::AggregationNodeStep::FINAL:
      step = core::AggregationNode::Step::kFinal;
      break;
    case protocol::AggregationNodeStep::INTERMEDIATE:
      step = core::AggregationNode::Step::kIntermediate;
      break;
    case protocol::AggregationNodeStep::SINGLE:
      step = core::AggregationNode::Step::kSingle;
      break;
    default:
      BOLT_UNSUPPORTED("Unsupported aggregation step");
  }

  bool streamable = !node->preGroupedVariables.empty() &&
      node->groupingSets.groupingSetCount == 1 &&
      node->groupingSets.globalGroupingSets.empty();

  // groupIdField and globalGroupingSets are required for producing default
  // output rows for global grouping sets when there are no input rows.
  // Global grouping sets can be present without groupIdField in Final
  // aggregations. But the default output is generated only for Single and
  // Partial aggregations. Set both fields only when required for the
  // aggregation.
  std::optional<core::FieldAccessTypedExprPtr> groupIdField;
  std::vector<vector_size_t> globalGroupingSets;
  if (node->groupIdVariable && !node->groupingSets.globalGroupingSets.empty()) {
    groupIdField = toBoltExprs({*node->groupIdVariable.get()})[0];
    globalGroupingSets = node->groupingSets.globalGroupingSets;
  }

  return std::make_shared<core::AggregationNode>(
      node->id,
      step,
      toBoltExprs(node->groupingSets.groupingKeys),
      streamable ? toBoltExprs(node->preGroupedVariables)
                 : std::vector<core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      globalGroupingSets,
      groupIdField,
      false, // ignoreNullKeys
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::GroupIdNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::GroupIdNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // protocol::GroupIdNode.groupingSets uses output names for the grouping
  // keys. protocol::GroupIdNode.groupingColumns maps output name of a
  // grouping key to its input name.

  // Example:
  //  - GroupId[[orderstatus], [orderpriority]] =>
  //  [orderstatus$gid:varchar(1), orderpriority$gid:varchar(15),
  //  orderkey:bigint, groupid:bigint]
  //      orderstatus$gid := orderstatus (10:20)
  //      orderpriority$gid := orderpriority (10:35)
  //
  //  Here, groupingSets = [[orderstatus$gid], [orderpriority$gid]]
  //    and groupingColumns = [orderstatus$gid => orderstatus,
  //    orderpriority$gid
  //    => orderpriority]

  // core::GroupIdNode.groupingSets is defined using output field names.
  // core::GroupIdNode.groupingKeys maps output name of a
  // grouping key to the corresponding input field.

  std::vector<std::vector<std::string>> groupingSets;
  groupingSets.reserve(node->groupingSets.size());
  for (const auto& groupingSet : node->groupingSets) {
    std::vector<std::string> groupingKeys;
    groupingKeys.reserve(groupingSet.size());
    // Use the output key name in the GroupingSet as there could be
    // multiple output keys mapping to the same input column.
    for (const auto& groupingKey : groupingSet) {
      groupingKeys.emplace_back(groupingKey.name);
    }
    groupingSets.emplace_back(std::move(groupingKeys));
  }

  std::vector<core::GroupIdNode::GroupingKeyInfo> groupingKeys;
  groupingKeys.reserve(node->groupingColumns.size());
  for (const auto& [output, input] : node->groupingColumns) {
    groupingKeys.emplace_back(core::GroupIdNode::GroupingKeyInfo{
        output.name, exprConverter_.toBoltExpr(input)});
  }

  return std::make_shared<core::GroupIdNode>(
      node->id,
      std::move(groupingSets),
      std::move(groupingKeys),
      toBoltExprs(node->aggregationArguments),
      node->groupIdVariable.name,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::PlanNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::DistinctLimitNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // Convert to Limit(Aggregation)
  return std::make_shared<core::LimitNode>(
      // Make sure to use unique plan node IDs.
      fmt::format("{}.limit", node->id),
      0,
      node->limit,
      node->partial,
      std::make_shared<core::AggregationNode>(
          // Use the ID of the DistinctLimit plan node here to propagate the
          // stats.
          node->id,
          core::AggregationNode::Step::kSingle,
          toBoltExprs(node->distinctVariables),
          std::vector<core::FieldAccessTypedExprPtr>{},
          std::vector<std::string>{}, // aggregateNames
          std::vector<core::AggregationNode::Aggregate>{}, // aggregates
          false, // ignoreNullKeys
          toBoltQueryPlan(node->source, tableWriteInfo, taskId)));
}

namespace {
core::JoinType toJoinType(protocol::JoinType type) {
  switch (type) {
    case protocol::JoinType::INNER:
      return core::JoinType::kInner;
    case protocol::JoinType::LEFT:
      return core::JoinType::kLeft;
    case protocol::JoinType::RIGHT:
      return core::JoinType::kRight;
    case protocol::JoinType::FULL:
      return core::JoinType::kFull;
  }

  BOLT_UNSUPPORTED("Unknown join type");
}
} // namespace

core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::JoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto joinType = toJoinType(node->type);
  if (node->criteria.empty()) {
    const bool isNestedJoinType = core::isInnerJoin(joinType) ||
        core::isLeftJoin(joinType) || core::isRightJoin(joinType) ||
        core::isFullJoin(joinType);
    if (isNestedJoinType) {
      return std::make_shared<core::NestedLoopJoinNode>(
          node->id,
          joinType,
          node->filter ? exprConverter_.toBoltExpr(*node->filter) : nullptr,
          toBoltQueryPlan(node->left, tableWriteInfo, taskId),
          toBoltQueryPlan(node->right, tableWriteInfo, taskId),
          toRowType(node->outputVariables, typeParser_));
    }
    BOLT_UNSUPPORTED(
        "JoinNode has empty criteria that cannot be "
        "satisfied by NestedJoinNode or HashJoinNode");
  }

  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;

  leftKeys.reserve(node->criteria.size());
  rightKeys.reserve(node->criteria.size());
  for (const auto& clause : node->criteria) {
    leftKeys.emplace_back(exprConverter_.toBoltExpr(clause.left));
    rightKeys.emplace_back(exprConverter_.toBoltExpr(clause.right));
  }

  return std::make_shared<core::HashJoinNode>(
      node->id,
      joinType,
      false,
      leftKeys,
      rightKeys,
      node->filter ? exprConverter_.toBoltExpr(*node->filter) : nullptr,
      toBoltQueryPlan(node->left, tableWriteInfo, taskId),
      toBoltQueryPlan(node->right, tableWriteInfo, taskId),
      toRowType(node->outputVariables, typeParser_));
}

bytedance::bolt::core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::SemiJoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;

  leftKeys.push_back(exprConverter_.toBoltExpr(node->sourceJoinVariable));
  rightKeys.push_back(
      exprConverter_.toBoltExpr(node->filteringSourceJoinVariable));

  auto left = toBoltQueryPlan(node->source, tableWriteInfo, taskId);
  auto right = toBoltQueryPlan(node->filteringSource, tableWriteInfo, taskId);

  std::vector<std::string> outputNames = left->outputType()->names();
  outputNames.push_back(node->semiJoinOutput.name);
  std::vector<TypePtr> outputTypes = left->outputType()->children();
  outputTypes.push_back(BOOLEAN());

  return std::make_shared<core::HashJoinNode>(
      node->id,
      core::JoinType::kLeftSemiProject,
      true, // nullAware
      leftKeys,
      rightKeys,
      nullptr, // filter
      left,
      right,
      ROW(std::move(outputNames), std::move(outputTypes)));
}

core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::MarkDistinctNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<core::MarkDistinctNode>(
      node->id,
      node->markerVariable.name,
      toBoltExprs(node->distinctVariables),
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::MergeJoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto joinType = toJoinType(node->type);

  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;

  leftKeys.reserve(node->criteria.size());
  rightKeys.reserve(node->criteria.size());
  for (const auto& clause : node->criteria) {
    leftKeys.emplace_back(exprConverter_.toBoltExpr(clause.left));
    rightKeys.emplace_back(exprConverter_.toBoltExpr(clause.right));
  }

  return std::make_shared<core::MergeJoinNode>(
      node->id,
      joinType,
      leftKeys,
      rightKeys,
      node->filter ? exprConverter_.toBoltExpr(*node->filter) : nullptr,
      toBoltQueryPlan(node->left, tableWriteInfo, taskId),
      toBoltQueryPlan(node->right, tableWriteInfo, taskId),
      toRowType(node->outputVariables, typeParser_));
}

std::shared_ptr<const core::TopNNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::TopNNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  sortingKeys.reserve(node->orderingScheme.orderBy.size());
  sortingOrders.reserve(node->orderingScheme.orderBy.size());
  for (const auto& orderBy : node->orderingScheme.orderBy) {
    sortingKeys.emplace_back(exprConverter_.toBoltExpr(orderBy.variable));
    sortingOrders.emplace_back(toBoltSortOrder(orderBy.sortOrder));
  }
  return std::make_shared<core::TopNNode>(
      node->id,
      sortingKeys,
      sortingOrders,
      node->count,
      node->step == protocol::Step::PARTIAL,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::LimitNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::LimitNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<core::LimitNode>(
      node->id,
      0,
      node->count,
      node->step == protocol::LimitNodeStep::PARTIAL,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::OrderByNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::SortNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (const auto& orderBy : node->orderingScheme.orderBy) {
    sortingKeys.emplace_back(exprConverter_.toBoltExpr(orderBy.variable));
    sortingOrders.emplace_back(toBoltSortOrder(orderBy.sortOrder));
  }

  return std::make_shared<core::OrderByNode>(
      node->id,
      sortingKeys,
      sortingOrders,
      node->isPartial,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::TableWriteNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::TableWriterNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::string connectorId;
  std::shared_ptr<connector::ConnectorInsertTableHandle> connectorInsertHandle;
  if (auto createHandle = std::dynamic_pointer_cast<protocol::CreateHandle>(
          tableWriteInfo->writerTarget)) {
    connectorId = createHandle->handle.connectorId;
    auto& connector =
        getPrestoToBoltConnector(createHandle->handle.connectorHandle->_type);
    auto boltHandle =
        connector.toBoltInsertTableHandle(createHandle.get(), typeParser_);
    connectorInsertHandle = std::shared_ptr(std::move(boltHandle));
  } else if (
      auto insertHandle = std::dynamic_pointer_cast<protocol::InsertHandle>(
          tableWriteInfo->writerTarget)) {
    connectorId = insertHandle->handle.connectorId;
    auto& connector =
        getPrestoToBoltConnector(insertHandle->handle.connectorHandle->_type);
    auto boltHandle =
        connector.toBoltInsertTableHandle(insertHandle.get(), typeParser_);
    connectorInsertHandle = std::shared_ptr(std::move(boltHandle));
  }

  if (!connectorInsertHandle) {
    BOLT_UNSUPPORTED(
        "Unsupported table writer handle: {}",
        toJsonString(tableWriteInfo->writerTarget));
  }

  auto insertTableHandle = std::make_shared<core::InsertTableHandle>(
      connectorId, connectorInsertHandle);

  const auto outputType = toRowType(
      generateOutputVariables(
          {node->rowCountVariable,
           node->fragmentVariable,
           node->tableCommitContextVariable},
          node->statisticsAggregation),
      typeParser_);
  const auto sourceBoltPlan =
      toBoltQueryPlan(node->source, tableWriteInfo, taskId);
  std::shared_ptr<core::AggregationNode> aggregationNode =
      generateAggregationNode(
          node->statisticsAggregation,
          core::AggregationNode::Step::kPartial,
          node->id,
          sourceBoltPlan,
          tableWriteInfo,
          taskId);
  return std::make_shared<core::TableWriteNode>(
      node->id,
      toRowType(node->columns, typeParser_),
      node->columnNames,
      std::move(aggregationNode),
      std::move(insertTableHandle),
      node->partitioningScheme != nullptr,
      outputType,
      getCommitStrategy(),
      sourceBoltPlan);
}

std::shared_ptr<const core::TableWriteMergeNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::TableWriterMergeNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  const auto outputType = toRowType(
      generateOutputVariables(
          {node->rowCountVariable,
           node->fragmentVariable,
           node->tableCommitContextVariable},
          node->statisticsAggregation),
      typeParser_);
  const auto sourceBoltPlan =
      toBoltQueryPlan(node->source, tableWriteInfo, taskId);
  std::shared_ptr<core::AggregationNode> aggregationNode =
      generateAggregationNode(
          node->statisticsAggregation,
          core::AggregationNode::Step::kIntermediate,
          node->id,
          sourceBoltPlan,
          tableWriteInfo,
          taskId);

  return std::make_shared<core::TableWriteMergeNode>(
      node->id, outputType, aggregationNode, sourceBoltPlan);
}

std::shared_ptr<const core::UnnestNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::UnnestNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> unnestFields;
  unnestFields.reserve(node->unnestVariables.size());
  std::vector<std::string> unnestNames;
  for (const auto& entry : node->unnestVariables) {
    unnestFields.emplace_back(exprConverter_.toBoltExpr(entry.first));
    for (const auto& output : entry.second) {
      unnestNames.emplace_back(output.name);
    }
  }

  return std::make_shared<core::UnnestNode>(
      node->id,
      toBoltExprs(node->replicateVariables),
      unnestFields,
      unnestNames,
      node->ordinalityVariable ? std::optional{node->ordinalityVariable->name}
                               : std::nullopt,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::EnforceSingleRowNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::EnforceSingleRowNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<core::EnforceSingleRowNode>(
      node->id, toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::AssignUniqueIdNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::AssignUniqueId>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto prestoTaskId = PrestoTaskId(taskId);
  // `taskUniqueId` is an integer to uniquely identify the generated id
  // across all the nodes executing the same query stage in a distributed
  // query execution.
  //
  // 10 bit for stageId && 14-bit for taskId should be sufficient
  // given the max stage per query is 100 by default.

  // taskUniqueId = last 10 bit of stageId | last 14 bits of taskId
  int32_t taskUniqueId = (prestoTaskId.stageId() & ((1 << 10) - 1)) << 14 |
      (prestoTaskId.id() & ((1 << 14) - 1));
  return std::make_shared<core::AssignUniqueIdNode>(
      node->id,
      node->idVariable.name,
      taskUniqueId,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

core::WindowNode::Function BoltQueryPlanConverterBase::toBoltWindowFunction(
    const protocol::Function& func) {
  core::WindowNode::Function windowFunc;
  windowFunc.functionCall =
      std::dynamic_pointer_cast<const core::CallTypedExpr>(
          exprConverter_.toBoltExpr(func.functionCall));
  windowFunc.ignoreNulls = func.ignoreNulls;

  windowFunc.frame.type = toBoltWindowType(func.frame.type);
  windowFunc.frame.startType = toBoltBoundType(func.frame.startType);
  windowFunc.frame.startValue = func.frame.startValue
      ? exprConverter_.toBoltExpr(func.frame.startValue)
      : nullptr;

  windowFunc.frame.endType = toBoltBoundType(func.frame.endType);
  windowFunc.frame.endValue = func.frame.endValue
      ? exprConverter_.toBoltExpr(func.frame.endValue)
      : nullptr;

  return windowFunc;
}

namespace {

std::pair<
    std::vector<core::FieldAccessTypedExprPtr>,
    std::vector<core::SortOrder>>
toSortFieldsAndOrders(
    const protocol::OrderingScheme* orderingScheme,
    BoltExprConverter& exprConverter,
    const std::unordered_set<std::string>& partitionKeys) {
  std::vector<core::FieldAccessTypedExprPtr> sortFields;
  std::vector<core::SortOrder> sortOrders;
  if (orderingScheme != nullptr) {
    auto nodeSpecOrdering = orderingScheme->orderBy;
    sortFields.reserve(nodeSpecOrdering.size());
    sortOrders.reserve(nodeSpecOrdering.size());
    for (const auto& spec : nodeSpecOrdering) {
      // Drop sorting keys that are present in partitioning keys.
      if (partitionKeys.count(spec.variable.name) == 0) {
        sortFields.emplace_back(exprConverter.toBoltExpr(spec.variable));
        sortOrders.emplace_back(toBoltSortOrder(spec.sortOrder));
      }
    }
  }
  return {sortFields, sortOrders};
}
} // namespace

std::shared_ptr<const bytedance::bolt::core::WindowNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::WindowNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> partitionFields;
  partitionFields.reserve(node->specification.partitionBy.size());
  std::unordered_set<std::string> partitionFieldNames;
  for (const auto& entry : node->specification.partitionBy) {
    partitionFields.emplace_back(exprConverter_.toBoltExpr(entry));
    partitionFieldNames.emplace(partitionFields.back()->name());
  }

  auto [sortFields, sortOrders] = toSortFieldsAndOrders(
      node->specification.orderingScheme.get(),
      exprConverter_,
      partitionFieldNames);

  std::vector<std::string> windowNames;
  std::vector<core::WindowNode::Function> windowFunctions;
  windowNames.reserve(node->windowFunctions.size());
  windowFunctions.reserve(node->windowFunctions.size());
  for (const auto& func : node->windowFunctions) {
    windowNames.emplace_back(func.first.name);
    windowFunctions.emplace_back(toBoltWindowFunction(func.second));
  }

  // TODO(spershin): Supply proper 'inputsSorted' argument to WindowNode
  // constructor instead of 'false'.
  return std::make_shared<bytedance::bolt::core::WindowNode>(
      node->id,
      partitionFields,
      sortFields,
      sortOrders,
      windowNames,
      windowFunctions,
      false,
      0, // bolt window with limit order by 
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

namespace {

core::WindowNode::Function makeRowNumberFunction(
    const protocol::VariableReferenceExpression& rowNumberVariable,
    const TypeParser& typeParser) {
  core::WindowNode::Function function;
  function.functionCall = std::make_shared<core::CallTypedExpr>(
      stringToType(rowNumberVariable.type, typeParser),
      std::vector<core::TypedExprPtr>{},
      "presto.default.row_number");

  function.frame.type = core::WindowNode::WindowType::kRows;
  function.frame.startType = core::WindowNode::BoundType::kUnboundedPreceding;
  function.frame.endType = core::WindowNode::BoundType::kCurrentRow;

  return function;
}
} // namespace

std::shared_ptr<const bytedance::bolt::core::RowNumberNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::RowNumberNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> partitionFields;
  partitionFields.reserve(node->partitionBy.size());
  for (const auto& entry : node->partitionBy) {
    partitionFields.emplace_back(exprConverter_.toBoltExpr(entry));
  }

  std::optional<int32_t> limit;
  if (node->maxRowCountPerPartition) {
    limit = *node->maxRowCountPerPartition;
  }

  std::optional<std::string> rowNumberColumnName;
  if (!node->partial) {
    rowNumberColumnName = node->rowNumberVariable.name;
  }

  return std::make_shared<core::RowNumberNode>(
      node->id,
      partitionFields,
      rowNumberColumnName,
      limit,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const bytedance::bolt::core::PlanNode>
BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::TopNRowNumberNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> partitionFields;
  partitionFields.reserve(node->specification.partitionBy.size());
  std::unordered_set<std::string> partitionFieldNames;
  for (const auto& entry : node->specification.partitionBy) {
    partitionFields.emplace_back(exprConverter_.toBoltExpr(entry));
    partitionFieldNames.emplace(partitionFields.back()->name());
  }

  auto [sortFields, sortOrders] = toSortFieldsAndOrders(
      node->specification.orderingScheme.get(),
      exprConverter_,
      partitionFieldNames);

  std::optional<std::string> rowNumberColumnName;
  if (!node->partial) {
    rowNumberColumnName = node->rowNumberVariable.name;
  }

  if (sortFields.empty()) {
    // May happen if all sorting keys are also used as partition keys.

    return std::make_shared<core::RowNumberNode>(
        node->id,
        partitionFields,
        rowNumberColumnName,
        node->maxRowCountPerPartition,
        toBoltQueryPlan(node->source, tableWriteInfo, taskId));
  }

  return std::make_shared<core::TopNRowNumberNode>(
      node->id,
      partitionFields,
      sortFields,
      sortOrders,
      rowNumberColumnName,
      node->maxRowCountPerPartition,
      toBoltQueryPlan(node->source, tableWriteInfo, taskId));
}

core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::PlanNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  if (auto exchange =
          std::dynamic_pointer_cast<const protocol::ExchangeNode>(node)) {
    return toBoltQueryPlan(exchange, tableWriteInfo, taskId);
  }
  if (auto filter =
          std::dynamic_pointer_cast<const protocol::FilterNode>(node)) {
    return toBoltQueryPlan(filter, tableWriteInfo, taskId);
  }
  if (auto project =
          std::dynamic_pointer_cast<const protocol::ProjectNode>(node)) {
    return toBoltQueryPlan(project, tableWriteInfo, taskId);
  }
  if (auto values =
          std::dynamic_pointer_cast<const protocol::ValuesNode>(node)) {
    return toBoltQueryPlan(values, tableWriteInfo, taskId);
  }
  if (auto tableScan =
          std::dynamic_pointer_cast<const protocol::TableScanNode>(node)) {
    return toBoltQueryPlan(tableScan, tableWriteInfo, taskId);
  }
  if (auto aggregation =
          std::dynamic_pointer_cast<const protocol::AggregationNode>(node)) {
    return toBoltQueryPlan(aggregation, tableWriteInfo, taskId);
  }
  if (auto groupId =
          std::dynamic_pointer_cast<const protocol::GroupIdNode>(node)) {
    return toBoltQueryPlan(groupId, tableWriteInfo, taskId);
  }
  if (auto distinctLimit =
          std::dynamic_pointer_cast<const protocol::DistinctLimitNode>(node)) {
    return toBoltQueryPlan(distinctLimit, tableWriteInfo, taskId);
  }
  if (auto join = std::dynamic_pointer_cast<const protocol::JoinNode>(node)) {
    return toBoltQueryPlan(join, tableWriteInfo, taskId);
  }
  if (auto join =
          std::dynamic_pointer_cast<const protocol::SemiJoinNode>(node)) {
    return toBoltQueryPlan(join, tableWriteInfo, taskId);
  }
  if (auto join =
          std::dynamic_pointer_cast<const protocol::MergeJoinNode>(node)) {
    return toBoltQueryPlan(join, tableWriteInfo, taskId);
  }
  if (auto remoteSource =
          std::dynamic_pointer_cast<const protocol::RemoteSourceNode>(node)) {
    return toBoltQueryPlan(remoteSource, tableWriteInfo, taskId);
  }
  if (auto topN = std::dynamic_pointer_cast<const protocol::TopNNode>(node)) {
    return toBoltQueryPlan(topN, tableWriteInfo, taskId);
  }
  if (auto limit = std::dynamic_pointer_cast<const protocol::LimitNode>(node)) {
    return toBoltQueryPlan(limit, tableWriteInfo, taskId);
  }
  if (auto sort = std::dynamic_pointer_cast<const protocol::SortNode>(node)) {
    return toBoltQueryPlan(sort, tableWriteInfo, taskId);
  }
  if (auto unnest =
          std::dynamic_pointer_cast<const protocol::UnnestNode>(node)) {
    return toBoltQueryPlan(unnest, tableWriteInfo, taskId);
  }
  if (auto enforceSingleRow =
          std::dynamic_pointer_cast<const protocol::EnforceSingleRowNode>(
              node)) {
    return toBoltQueryPlan(enforceSingleRow, tableWriteInfo, taskId);
  }
  if (auto tableWriter =
          std::dynamic_pointer_cast<const protocol::TableWriterNode>(node)) {
    return toBoltQueryPlan(tableWriter, tableWriteInfo, taskId);
  }
  if (auto tableWriteMerger =
          std::dynamic_pointer_cast<const protocol::TableWriterMergeNode>(
              node)) {
    return toBoltQueryPlan(tableWriteMerger, tableWriteInfo, taskId);
  }
  if (auto assignUniqueId =
          std::dynamic_pointer_cast<const protocol::AssignUniqueId>(node)) {
    return toBoltQueryPlan(assignUniqueId, tableWriteInfo, taskId);
  }
  if (auto window =
          std::dynamic_pointer_cast<const protocol::WindowNode>(node)) {
    return toBoltQueryPlan(window, tableWriteInfo, taskId);
  }
  if (auto rowNumber =
          std::dynamic_pointer_cast<const protocol::RowNumberNode>(node)) {
    return toBoltQueryPlan(rowNumber, tableWriteInfo, taskId);
  }
  if (auto topNRowNumber =
          std::dynamic_pointer_cast<const protocol::TopNRowNumberNode>(node)) {
    return toBoltQueryPlan(topNRowNumber, tableWriteInfo, taskId);
  }
  if (auto markDistinct =
          std::dynamic_pointer_cast<const protocol::MarkDistinctNode>(node)) {
    return toBoltQueryPlan(markDistinct, tableWriteInfo, taskId);
  }
  if (auto sampleNode =
          std::dynamic_pointer_cast<const protocol::SampleNode>(node)) {
    // SampleNode (used for System TABLESAMPLE) is a no-op at this layer.
    // SYSTEM Tablesample is implemented by sampling when generating splits
    // since it skips in units of logical segments of data.
    // The sampled splits are correctly passed to the TableScanNode which
    // is the source of this SampleNode.
    // BERNOULLI sampling is implemented as a filter on the TableScan
    // directly, and does not have the intermediate SampleNode.
    return toBoltQueryPlan(sampleNode->source, tableWriteInfo, taskId);
  }
  BOLT_UNSUPPORTED("Unknown plan node type {}", node->_type);
}

namespace {
core::ExecutionStrategy toStrategy(protocol::StageExecutionStrategy strategy) {
  switch (strategy) {
    case protocol::StageExecutionStrategy::UNGROUPED_EXECUTION:
      return core::ExecutionStrategy::kUngrouped;

    case protocol::StageExecutionStrategy::
        FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
    case protocol::StageExecutionStrategy::
        DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
      return core::ExecutionStrategy::kGrouped;

    case protocol::StageExecutionStrategy::RECOVERABLE_GROUPED_EXECUTION:
      BOLT_UNSUPPORTED(
          "RECOVERABLE_GROUPED_EXECUTION "
          "Stage Execution Strategy is not supported");
  }
  BOLT_UNSUPPORTED("Unknown Stage Execution Strategy type {}", (int)strategy);
}

// Presto doesn't have PartitionedOutputNode and assigns its source node's plan
// node id to PartitionedOutputOperator.
// However, Bolt has PartitionedOutputNode and doesn't allow duplicate node
// ids. Hence, we use "root." + source node's plan id as
// PartitionedOutputNode's.
// For example, if source node plan id is "10", then the associated
// partitioned output node id is "root.10".
protocol::PlanNodeId toPartitionedOutputNodeId(const protocol::PlanNodeId& id) {
  return "root." + id;
}

} // namespace

core::PlanFragment BoltQueryPlanConverterBase::toBoltQueryPlan(
    const protocol::PlanFragment& fragment,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  core::PlanFragment planFragment;

  // Convert the fragment info first.
  const auto& descriptor = fragment.stageExecutionDescriptor;
  planFragment.executionStrategy =
      toStrategy(fragment.stageExecutionDescriptor.stageExecutionStrategy);
  planFragment.numSplitGroups = descriptor.totalLifespans;
  for (const auto& planNodeId : descriptor.groupedExecutionScanNodes) {
    planFragment.groupedExecutionLeafNodeIds.emplace(planNodeId);
  }
  if (planFragment.executionStrategy == core::ExecutionStrategy::kGrouped) {
    BOLT_CHECK(
        !planFragment.groupedExecutionLeafNodeIds.empty(),
        "groupedExecutionScanNodes cannot be empty if stage execution strategy "
        "is grouped execution");
  }

  if (auto output = std::dynamic_pointer_cast<const protocol::OutputNode>(
          fragment.root)) {
    planFragment.planNode = toBoltQueryPlan(output, tableWriteInfo, taskId);
    return planFragment;
  }

  auto partitioningScheme = fragment.partitioningScheme;
  auto partitioningHandle =
      partitioningScheme.partitioning.handle.connectorHandle;

  auto partitioningKeys =
      toTypedExprs(partitioningScheme.partitioning.arguments, exprConverter_);

  auto sourceNode = toBoltQueryPlan(fragment.root, tableWriteInfo, taskId);
  auto inputType = sourceNode->outputType();

  std::vector<column_index_t> keyChannels;
  std::vector<VectorPtr> constValues;
  keyChannels.reserve(partitioningKeys.size());
  for (const auto& expr : partitioningKeys) {
    auto channel = exprToChannel(expr.get(), inputType);
    keyChannels.push_back(channel);
    // For constant channels create a base vector, add single value to it from
    // our variant and add it to the list of constant expressions.
    if (channel == kConstantChannel) {
      auto constExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr);
      if (constExpr->hasValueVector()) {
        constValues.emplace_back(constExpr->valueVector());
      } else {
        constValues.emplace_back(
            bytedance::bolt::BaseVector::create(expr->type(), 1, pool_));
        setCellFromVariant(constValues.back(), 0, constExpr->value());
      }
    }
  }
  auto outputType = toRowType(partitioningScheme.outputLayout, typeParser_);
  const auto partitionedOutputNodeId =
      toPartitionedOutputNodeId(fragment.root->id);

  if (auto systemPartitioningHandle =
          std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
              partitioningHandle)) {
    switch (systemPartitioningHandle->partitioning) {
      case protocol::SystemPartitioning::SINGLE:
        BOLT_CHECK(
            systemPartitioningHandle->function ==
                protocol::SystemPartitionFunction::SINGLE,
            "Unsupported partitioning function: {}",
            toJsonString(systemPartitioningHandle->function));
        planFragment.planNode = core::PartitionedOutputNode::single(
            partitionedOutputNodeId,
            outputType,
            //toBoltSerdeKind((partitioningScheme.encoding)),
            sourceNode);
        return planFragment;
      case protocol::SystemPartitioning::FIXED: {
        switch (systemPartitioningHandle->function) {
          case protocol::SystemPartitionFunction::ROUND_ROBIN: {
            auto numPartitions = partitioningScheme.bucketToPartition
                ? partitioningScheme.bucketToPartition->size()
                : 1;

            if (numPartitions == 1) {
              planFragment.planNode = core::PartitionedOutputNode::single(
                  partitionedOutputNodeId,
                  outputType,
                  //toBoltSerdeKind((partitioningScheme.encoding)),
                  sourceNode);
              return planFragment;
            }
            planFragment.planNode =
                std::make_shared<core::PartitionedOutputNode>(
                    partitionedOutputNodeId,
                    core::PartitionedOutputNode::Kind::kPartitioned,
                    partitioningKeys,
                    numPartitions,
                    partitioningScheme.replicateNullsAndAny,
                    std::make_shared<RoundRobinPartitionFunctionSpec>(),
                    outputType,
                    //toBoltSerdeKind((partitioningScheme.encoding)),
                    sourceNode);
            return planFragment;
          }
          case protocol::SystemPartitionFunction::HASH: {
            auto numPartitions = partitioningScheme.bucketToPartition
                ? partitioningScheme.bucketToPartition->size()
                : 1;

            if (numPartitions == 1) {
              planFragment.planNode = core::PartitionedOutputNode::single(
                  partitionedOutputNodeId,
                  outputType,
                  //toBoltSerdeKind((partitioningScheme.encoding)),
                  sourceNode);
              return planFragment;
            }
            planFragment.planNode =
                std::make_shared<core::PartitionedOutputNode>(
                    partitionedOutputNodeId,
                    core::PartitionedOutputNode::Kind::kPartitioned,
                    partitioningKeys,
                    numPartitions,
                    partitioningScheme.replicateNullsAndAny,
                    std::make_shared<HashPartitionFunctionSpec>(
                        inputType, keyChannels, constValues),
                    outputType,
                    //toBoltSerdeKind((partitioningScheme.encoding)),
                    sourceNode);
            return planFragment;
          }
          case protocol::SystemPartitionFunction::BROADCAST: {
            planFragment.planNode = core::PartitionedOutputNode::broadcast(
                partitionedOutputNodeId,
                1,
                outputType,
                //toBoltSerdeKind((partitioningScheme.encoding)),
                sourceNode);
            return planFragment;
          }
          default:
            BOLT_UNSUPPORTED(
                "Unsupported partitioning function: {}",
                toJsonString(systemPartitioningHandle->function));
        }
      }
      case protocol::SystemPartitioning::SCALED: {
        BOLT_CHECK(
            systemPartitioningHandle->function ==
                protocol::SystemPartitionFunction::ROUND_ROBIN,
            "Unsupported partitioning function: {}",
            toJsonString(systemPartitioningHandle->function));
        planFragment.planNode = core::PartitionedOutputNode::arbitrary(
            partitionedOutputNodeId,
            std::move(outputType),
            //toBoltSerdeKind((partitioningScheme.encoding)),
            std::move(sourceNode));
        return planFragment;
      }
      default:
        BOLT_FAIL(
            "Unsupported kind of SystemPartitioning: {}",
            toJsonString(systemPartitioningHandle->partitioning));
    }
  }

  const auto& bucketToPartition = *partitioningScheme.bucketToPartition;
  auto numPartitions =
      *std::max_element(bucketToPartition.begin(), bucketToPartition.end()) + 1;

  if (numPartitions == 1) {
    planFragment.planNode = core::PartitionedOutputNode::single(
        partitionedOutputNodeId,
        outputType,
        //toBoltSerdeKind((partitioningScheme.encoding)),
        sourceNode);
    return planFragment;
  }

  auto& connector = getPrestoToBoltConnector(partitioningHandle->_type);
  auto spec = connector.createBoltPartitionFunctionSpec(
      partitioningHandle.get(), bucketToPartition, keyChannels, constValues);
  planFragment.planNode = std::make_shared<core::PartitionedOutputNode>(
      partitionedOutputNodeId,
      core::PartitionedOutputNode::Kind::kPartitioned,
      partitioningKeys,
      numPartitions,
      partitioningScheme.replicateNullsAndAny,
      std::shared_ptr(std::move(spec)),
      toRowType(partitioningScheme.outputLayout, typeParser_),
      //toBoltSerdeKind((partitioningScheme.encoding)),
      sourceNode);
  return planFragment;
}

core::PlanNodePtr BoltQueryPlanConverterBase::toBoltQueryPlan(
    const std::shared_ptr<const protocol::OutputNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return core::PartitionedOutputNode::single(
      node->id,
      toRowType(node->outputVariables, typeParser_),
      //bytedance::bolt::VectorSerde::Kind::kPresto,
      BoltQueryPlanConverterBase::toBoltQueryPlan(
          node->source, tableWriteInfo, taskId));
}

bytedance::bolt::core::PlanNodePtr BoltInteractiveQueryPlanConverter::toBoltQueryPlan(
    const std::shared_ptr<const protocol::RemoteSourceNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  if (node->orderingScheme) {
    std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
    std::vector<core::SortOrder> sortingOrders;
    sortingKeys.reserve(node->orderingScheme->orderBy.size());
    sortingOrders.reserve(node->orderingScheme->orderBy.size());

    for (const auto& orderBy : node->orderingScheme->orderBy) {
      sortingKeys.emplace_back(exprConverter_.toBoltExpr(orderBy.variable));
      sortingOrders.emplace_back(toBoltSortOrder(orderBy.sortOrder));
    }
    return std::make_shared<core::MergeExchangeNode>(
        node->id,
        rowType,
        sortingKeys,
        sortingOrders/*,
        toBoltSerdeKind(node->encoding)*/);
  }
  return std::make_shared<core::ExchangeNode>(
      node->id, rowType/*, toBoltSerdeKind(node->encoding)*/);
}

bytedance::bolt::connector::CommitStrategy
BoltInteractiveQueryPlanConverter::getCommitStrategy() const {
  return bytedance::bolt::connector::CommitStrategy::kNoCommit;
}

bytedance::bolt::core::PlanFragment BoltBatchQueryPlanConverter::toBoltQueryPlan(
    const protocol::PlanFragment& fragment,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto planFragment = BoltQueryPlanConverterBase::toBoltQueryPlan(
      fragment, tableWriteInfo, taskId);

  auto partitionedOutputNode =
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
          planFragment.planNode);

  BOLT_USER_CHECK_NOT_NULL(
      partitionedOutputNode, "PartitionedOutputNode is required");

  if (partitionedOutputNode->isBroadcast()) {
    BOLT_USER_CHECK_NOT_NULL(
        broadcastBasePath_, "broadcastBasePath is required");
    // TODO - Use original plan node with root node and aggregate operator
    // stats for additional nodes.
    auto broadcastWriteNode = std::make_shared<operators::BroadcastWriteNode>(
        fmt::format("{}.bw", partitionedOutputNode->id()),
        *broadcastBasePath_,
        partitionedOutputNode->outputType(),
        core::LocalPartitionNode::gather(
            "broadcast-write-gather",
            std::vector<core::PlanNodePtr>{partitionedOutputNode->sources()}));

    planFragment.planNode = core::PartitionedOutputNode::broadcast(
        partitionedOutputNode->id(),
        1,
        broadcastWriteNode->outputType(),
        //bytedance::bolt::VectorSerde::Kind::kPresto,
        {broadcastWriteNode});
    return planFragment;
  }

  // If the serializedShuffleWriteInfo is not nullptr, it means this fragment
  // ends with a shuffle stage. We convert the PartitionedOutputNode to a
  // chain of following nodes:
  // (1) A PartitionAndSerializeNode.
  // (2) A "gather" LocalPartitionNode that gathers results from multiple
  //     threads to one thread.
  // (3) A ShuffleWriteNode.
  // To be noted, whether the last node of the plan is PartitionedOutputNode
  // can't guarantee the query has shuffle stage, for example a plan with
  // TableWriteNode can also have PartitionedOutputNode to distribute the
  // metadata to coordinator.
  if (serializedShuffleWriteInfo_ == nullptr) {
    BOLT_USER_CHECK_EQ(1, partitionedOutputNode->numPartitions());
    return planFragment;
  }

  auto partitionAndSerializeNode =
      std::make_shared<operators::PartitionAndSerializeNode>(
          fmt::format("{}.ps", partitionedOutputNode->id()),
          partitionedOutputNode->keys(),
          partitionedOutputNode->numPartitions(),
          partitionedOutputNode->outputType(),
          partitionedOutputNode->sources()[0],
          partitionedOutputNode->isReplicateNullsAndAny(),
          partitionedOutputNode->partitionFunctionSpecPtr());

  planFragment.planNode = std::make_shared<operators::ShuffleWriteNode>(
      fmt::format("{}.sw", partitionedOutputNode->id()),
      partitionedOutputNode->numPartitions(),
      shuffleName_,
      std::move(*serializedShuffleWriteInfo_),
      core::LocalPartitionNode::gather(
          fmt::format("{}.g", partitionedOutputNode->id()),
          std::vector<core::PlanNodePtr>{partitionAndSerializeNode}));
  return planFragment;
}

bytedance::bolt::core::PlanNodePtr BoltBatchQueryPlanConverter::toBoltQueryPlan(
    const std::shared_ptr<const protocol::RemoteSourceNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  // Broadcast exchange source.
  if (node->exchangeType == protocol::ExchangeNodeType::REPLICATE) {
    return std::make_shared<core::ExchangeNode>(
        node->id, rowType/*, bytedance::bolt::VectorSerde::Kind::kPresto*/);
  }
  // Partitioned shuffle exchange source.
  return std::make_shared<operators::ShuffleReadNode>(node->id, rowType);
}

bytedance::bolt::connector::CommitStrategy
BoltBatchQueryPlanConverter::getCommitStrategy() const {
  return bytedance::bolt::connector::CommitStrategy::kTaskCommit;
}

void registerPrestoPlanNodeSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register(
      "PartitionAndSerializeNode",
      presto::operators::PartitionAndSerializeNode::create);
  registry.Register(
      "ShuffleReadNode", presto::operators::ShuffleReadNode::create);
  registry.Register(
      "ShuffleWriteNode", presto::operators::ShuffleWriteNode::create);
  registry.Register(
      "BroadcastWriteNode", presto::operators::BroadcastWriteNode::create);
}

void parseSqlFunctionHandle(
    const std::shared_ptr<protocol::SqlFunctionHandle>& sqlFunction,
    std::vector<bytedance::bolt::TypePtr>& rawInputTypes,
    TypeParser& typeParser) {
  const auto& functionId = sqlFunction->functionId;
  // functionId format is function-name;arg-type1;arg-type2;...
  // For example: foo;INTEGER;VARCHAR.
  auto start = functionId.find(";");
  if (start != std::string::npos) {
    for (;;) {
      auto pos = functionId.find(";", start + 1);
      if (pos == std::string::npos) {
        auto argumentType = functionId.substr(start + 1);
        if (!argumentType.empty()) {
          rawInputTypes.push_back(stringToType(argumentType, typeParser));
        }
        break;
      }
      auto argumentType = functionId.substr(start + 1, pos - start - 1);
      BOLT_CHECK(!argumentType.empty());
      rawInputTypes.push_back(stringToType(argumentType, typeParser));
      start = pos;
    }
  }
}
} // namespace facebook::presto
