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
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/common/Utils.h"
#include "bolt/common/base/Exceptions.h"

using namespace bytedance::bolt;
using namespace facebook::presto;

TEST(BoltToPrestoExceptionTranslatorTest, exceptionTranslation) {
  FLAGS_bolt_exception_user_stacktrace_enabled = true;
  for (const bool withContext : {false, true}) {
    for (const bool withAdditionalContext : {false, true}) {
      SCOPED_TRACE(fmt::format("withContext: {}", withContext));
      // Setup context based on 'withContext' flag.
      auto contextMessageFunction = [](BoltException::Type type, auto* arg) {
        return std::string(static_cast<char*>(arg));
      };
      std::string additonalMessage = "additional context message";
      bytedance::bolt::ExceptionContextSetter additionalContextSetter(
          withAdditionalContext
              ? ExceptionContext{contextMessageFunction, additonalMessage.data(), true}
              : ExceptionContext{});

      std::string contextMessage = "context message";
      bytedance::bolt::ExceptionContextSetter contextSetter(
          withContext
              ? ExceptionContext{contextMessageFunction, contextMessage.data()}
              : ExceptionContext{});
      BoltUserError userException(
          "file_name",
          1,
          "function_name()",
          "operator()",
          "test message",
          "",
          error_code::kArithmeticError,
          false);

      EXPECT_THROW({ throw userException; }, BoltException);
      try {
        throw userException;
      } catch (const BoltException& e) {
        EXPECT_EQ(e.exceptionName(), "BoltUserError");
        EXPECT_EQ(e.errorSource(), error_source::kErrorSourceUser);
        EXPECT_EQ(e.errorCode(), error_code::kArithmeticError);

        auto failureInfo = BoltToPrestoExceptionTranslator::translate(e);
        EXPECT_EQ(failureInfo.type, e.exceptionName());
        EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
        EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_USER_ERROR");
        EXPECT_EQ(failureInfo.errorCode.code, 0x00000000);
        std::string expectedMessage = "operator() test message";
        if (withContext) {
          expectedMessage += " " + contextMessage;
        }
        if (withAdditionalContext) {
          expectedMessage += " " + additonalMessage;
        }
        EXPECT_EQ(failureInfo.message, expectedMessage);
        EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::USER_ERROR);
      }
    }
  }

  BoltRuntimeError runtimeException(
      "file_name",
      1,
      "function_name()",
      "operator()",
      "test message",
      "",
      error_code::kInvalidState,
      false);

  EXPECT_THROW({ throw runtimeException; }, BoltException);
  try {
    throw runtimeException;
  } catch (const BoltException& e) {
    EXPECT_EQ(e.exceptionName(), "BoltRuntimeError");
    EXPECT_EQ(e.errorSource(), error_source::kErrorSourceRuntime);
    EXPECT_EQ(e.errorCode(), error_code::kInvalidState);

    auto failureInfo = BoltToPrestoExceptionTranslator::translate(e);
    EXPECT_EQ(failureInfo.type, e.exceptionName());
    EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
    EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_INTERNAL_ERROR");
    EXPECT_EQ(failureInfo.errorCode.code, 0x00010000);
    EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
  }

  EXPECT_THROW(([]() { BOLT_USER_CHECK_EQ(1, 2); })(), BoltException);
  try {
    BOLT_USER_CHECK_EQ(1, 2, "test user error message");
  } catch (const BoltException& e) {
    EXPECT_EQ(e.exceptionName(), "BoltUserError");
    EXPECT_EQ(e.errorSource(), error_source::kErrorSourceUser);
    EXPECT_EQ(e.errorCode(), error_code::kInvalidArgument);

    auto failureInfo = BoltToPrestoExceptionTranslator::translate(e);
    EXPECT_EQ(failureInfo.type, e.exceptionName());
    EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
    EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_USER_ERROR");
    EXPECT_EQ(failureInfo.errorCode.code, 0x00000000);
    EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::USER_ERROR);
    EXPECT_EQ(failureInfo.message, "1 == 2 (1 vs. 2) test user error message");
    EXPECT_NE(failureInfo.stack.size(), 0);
  }

  std::runtime_error stdRuntimeError("Test error message");
  auto failureInfo =
      BoltToPrestoExceptionTranslator::translate((stdRuntimeError));
  EXPECT_EQ(failureInfo.type, "std::exception");
  EXPECT_EQ(failureInfo.errorLocation.lineNumber, 1);
  EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_INTERNAL_ERROR");
  EXPECT_EQ(failureInfo.errorCode.code, 0x00010000);
  EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
}

TEST(UtilsTest, general) {
  EXPECT_EQ("2021-05-20T19:18:27.001Z", util::toISOTimestamp(1621538307001l));
  EXPECT_EQ("2021-05-20T19:18:27.000Z", util::toISOTimestamp(1621538307000l));
}
