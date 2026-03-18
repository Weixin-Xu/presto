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

/// BroadcastWriteNode represents node which broadcasts using file system.
class BroadcastWriteNode : public bytedance::bolt::core::PlanNode {
 public:
  /// @param serdeRowType Type of the serialized data. This can be different
  /// from the input type. Input columns may appear in different order, some
  /// columns may be missing, some columns may appear multiple types. May
  /// contain no columns at all if only row count needs to be broadcasted.
  BroadcastWriteNode(
      const bytedance::bolt::core::PlanNodeId& id,
      const std::string& basePath,
      bytedance::bolt::RowTypePtr serdeRowType,
      bytedance::bolt::core::PlanNodePtr source)
      : bytedance::bolt::core::PlanNode(id),
        basePath_{basePath},
        serdeRowType_{serdeRowType},
        sources_{std::move(source)} {}

  folly::dynamic serialize() const override;

  static bytedance::bolt::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const bytedance::bolt::RowTypePtr& outputType() const override {
    static const auto outputType = bytedance::bolt::ROW({bytedance::bolt::VARCHAR()});
    return outputType;
  }

  const std::vector<bytedance::bolt::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const bytedance::bolt::RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::string& basePath() const {
    return basePath_;
  }

  /// The desired schema of the serialized data. May include a subset of input
  /// columns, some columns may be duplicated, some columns may be missing,
  /// columns may appear in different order.
  const bytedance::bolt::RowTypePtr& serdeRowType() const {
    return serdeRowType_;
  }

  std::string_view name() const override {
    return "BroadcastWrite";
  }

 private:
  void addDetails(std::stringstream& stream) const override {}

  const std::string basePath_;
  const bytedance::bolt::RowTypePtr serdeRowType_;
  const std::vector<bytedance::bolt::core::PlanNodePtr> sources_;
};

class BroadcastWriteTranslator
    : public bytedance::bolt::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<bytedance::bolt::exec::Operator> toOperator(
      bytedance::bolt::exec::DriverCtx* ctx,
      int32_t id,
      const bytedance::bolt::core::PlanNodePtr& node) override;
};
} // namespace facebook::presto::operators
