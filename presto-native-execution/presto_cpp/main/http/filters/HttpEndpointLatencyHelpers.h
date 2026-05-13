#pragma once

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

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <proxygen/lib/http/HTTPMessage.h>
#include <re2/re2.h>

namespace facebook::presto::http {
class EndPoint;
}

namespace facebook::presto::http::filters::detail {

template <typename EndPointMetrics>
inline void updateEndpointLatency(
    std::unordered_map<std::string, EndPointMetrics>& map,
    const std::string& endpoint,
    uint64_t latencyUs) {
  auto itr = map.find(endpoint);
  if (itr != map.end()) {
    auto& metrics = itr->second;
    metrics.maxLatencyUs = std::max(metrics.maxLatencyUs, latencyUs);
    metrics.avgLatencyUs =
        (metrics.avgLatencyUs * metrics.count + latencyUs) / (metrics.count + 1);
    ++metrics.count;
    return;
  }
  map.emplace(endpoint, EndPointMetrics{endpoint, latencyUs, latencyUs, 1});
}

template <typename EndPointMetrics>
inline std::vector<EndPointMetrics> retrieveEndpointLatencies(
    std::unordered_map<std::string, EndPointMetrics>& map) {
  std::vector<EndPointMetrics> result;
  result.reserve(map.size());
  for (const auto& pair : map) {
    result.push_back(pair.second);
  }
  map.clear();
  std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.maxLatencyUs > rhs.maxLatencyUs;
  });
  return result;
}

inline std::string matchHttpEndpoint(
    const std::string& method,
    const std::string& path,
    const std::vector<std::unique_ptr<facebook::presto::http::EndPoint>>&
        endpoints) {
  std::vector<std::string> matches(4);
  std::vector<RE2::Arg> args(4);
  std::vector<RE2::Arg*> argPtrs(4);

  for (const auto& endpoint : endpoints) {
    if (endpoint->check(path, matches, args, argPtrs)) {
      return method + " " + endpoint->pattern();
    }
  }
  return "";
}

} // namespace facebook::presto::http::filters::detail
