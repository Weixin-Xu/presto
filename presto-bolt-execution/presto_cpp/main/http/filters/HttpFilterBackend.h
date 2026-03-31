#pragma once

#include <string>

#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/time/Timer.h"

namespace facebook::presto::http::filters::detail {

using HttpFilterMicrosecondTimer = bytedance::bolt::MicrosecondTimer;

inline std::string succinctMicrosString(uint64_t micros) {
  return bytedance::bolt::succinctMicros(micros);
}

} // namespace facebook::presto::http::filters::detail
