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
class ShuffleReadNode : public bytedance::bolt::core::PlanNode {
 public:
  ShuffleReadNode(const bytedance::bolt::core::PlanNodeId& id, bytedance::bolt::RowTypePtr type)
      : PlanNode(id), outputType_(type) {}

  folly::dynamic serialize() const override;

  static bytedance::bolt::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const bytedance::bolt::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<bytedance::bolt::core::PlanNodePtr>& sources() const override {
    static const std::vector<bytedance::bolt::core::PlanNodePtr> kEmptySources;
    return kEmptySources;
  }

  bool requiresExchangeClient() const override {
    return true;
  }

  bool requiresSplits() const override {
    return true;
  }

  std::string_view name() const override {
    return "ShuffleRead";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    // Nothing to add
  }

  const bytedance::bolt::RowTypePtr outputType_;
};

class ShuffleReadTranslator : public bytedance::bolt::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<bytedance::bolt::exec::Operator> toOperator(
      bytedance::bolt::exec::DriverCtx* ctx,
      int32_t id,
      const bytedance::bolt::core::PlanNodePtr& node,
      std::shared_ptr<bytedance::bolt::exec::ExchangeClient> exchangeClient) override;
};
} // namespace facebook::presto::operators
