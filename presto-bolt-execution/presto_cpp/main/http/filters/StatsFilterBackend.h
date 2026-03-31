#pragma once

#include "presto_cpp/main/common/Counters.h"
#include "bolt/common/base/StatsReporter.h"

namespace facebook::presto::http::filters::detail {

inline void recordHttpRequestCount() {
  RECORD_METRIC_VALUE(kCounterNumHTTPRequest, 1);
}

inline void recordHttpRequestLatencyMs(int64_t latencyMs) {
  RECORD_METRIC_VALUE(kCounterHTTPRequestLatencyMs, latencyMs);
}

inline void recordHttpRequestError() {
  RECORD_METRIC_VALUE(kCounterNumHTTPRequestError, 1);
}

inline void recordHttpRequestBodySize(size_t /* requestBodySize */) {}

inline bool deleteStatsFilterOnTerminal() {
  return true;
}

} // namespace facebook::presto::http::filters::detail
