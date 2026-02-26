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

#include <unordered_map>
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "bolt/common/base/BoltException.h"

namespace std {
class exception;
}

namespace facebook::presto {
namespace protocol {
struct ExecutionFailureInfo;
struct ErrorCode;
} // namespace protocol

class BoltToPrestoExceptionTranslator {
 public:
  // Translates to Presto error from Bolt exceptions
  static protocol::ExecutionFailureInfo translate(
      const bytedance::bolt::BoltException& e);

  // Translates to Presto error from std::exceptions
  static protocol::ExecutionFailureInfo translate(const std::exception& e);

 private:
  static const std::unordered_map<
      std::string,
      std::unordered_map<std::string, protocol::ErrorCode>>&
  translateMap() {
    static const std::unordered_map<
        std::string,
        std::unordered_map<std::string, protocol::ErrorCode>>
        kTranslateMap = {
            {bytedance::bolt::error_source::kErrorSourceRuntime,
             {{bytedance::bolt::error_code::kMemCapExceeded,
               {0x00020007,
                "EXCEEDED_LOCAL_MEMORY_LIMIT",
                protocol::ErrorType::INSUFFICIENT_RESOURCES}},
              {bytedance::bolt::error_code::kMemAborted,
               {0x00020000,
                "GENERIC_INSUFFICIENT_RESOURCES",
                protocol::ErrorType::INSUFFICIENT_RESOURCES}},
              {bytedance::bolt::error_code::kSpillLimitExceeded,
               {0x00020006,
                "EXCEEDED_SPILL_LIMIT",
                protocol::ErrorType::INSUFFICIENT_RESOURCES}},
              {bytedance::bolt::error_code::kInvalidState,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}},
              {bytedance::bolt::error_code::kUnreachableCode,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}},
              {bytedance::bolt::error_code::kNotImplemented,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}},
              {bytedance::bolt::error_code::kUnknown,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}}}},
            {bytedance::bolt::error_source::kErrorSourceUser,
             {{bytedance::bolt::error_code::kInvalidArgument,
               {0x00000000,
                "GENERIC_USER_ERROR",
                protocol::ErrorType::USER_ERROR}},
              {bytedance::bolt::error_code::kUnsupported,
               {0x0000000D, "NOT_SUPPORTED", protocol::ErrorType::USER_ERROR}},
              {bytedance::bolt::error_code::kArithmeticError,
               {0x00000000,
                "GENERIC_USER_ERROR",
                protocol::ErrorType::USER_ERROR}}}},
            {bytedance::bolt::error_source::kErrorSourceSystem, {}}};
    return kTranslateMap;
  }
};
} // namespace facebook::presto
