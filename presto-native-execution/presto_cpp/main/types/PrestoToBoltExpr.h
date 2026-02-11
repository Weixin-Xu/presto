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

#include <stdexcept>
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "bolt/core/Expressions.h"

namespace facebook::presto {

class BoltExprConverter {
 public:
  BoltExprConverter(bolt::memory::MemoryPool* pool, TypeParser* typeParser)
      : pool_(pool), typeParser_(typeParser) {}

  std::shared_ptr<const bolt::core::ConstantTypedExpr> toBoltExpr(
      std::shared_ptr<protocol::ConstantExpression> pexpr) const;

  bolt::core::TypedExprPtr toBoltExpr(
      std::shared_ptr<protocol::SpecialFormExpression> pexpr) const;

  bolt::core::FieldAccessTypedExprPtr toBoltExpr(
      std::shared_ptr<protocol::VariableReferenceExpression> pexpr) const;

  std::shared_ptr<const bolt::core::LambdaTypedExpr> toBoltExpr(
      std::shared_ptr<protocol::LambdaDefinitionExpression> pexpr) const;

  // TODO Remove when protocols are updated to use shared_ptr
  std::shared_ptr<const bolt::core::FieldAccessTypedExpr> toBoltExpr(
      const protocol::VariableReferenceExpression& pexpr) const;

  bolt::core::TypedExprPtr toBoltExpr(
      const protocol::CallExpression& pexpr) const;

  bolt::core::TypedExprPtr toBoltExpr(
      std::shared_ptr<protocol::RowExpression> pexpr) const;

  // Deserializes Presto Block of a scalar type into a variant.
  bolt::variant getConstantValue(
      const bolt::TypePtr& type,
      const protocol::Block& block) const;

 private:
  std::vector<bolt::core::TypedExprPtr> toBoltExpr(
      std::vector<std::shared_ptr<protocol::RowExpression>> pexpr) const;

  std::optional<bolt::core::TypedExprPtr> tryConvertLike(
      const protocol::CallExpression& pexpr) const;

  std::optional<bolt::core::TypedExprPtr> tryConvertDate(
      const protocol::CallExpression& pexpr) const;

  bolt::memory::MemoryPool* const pool_;
  TypeParser* const typeParser_;
};

} // namespace facebook::presto
