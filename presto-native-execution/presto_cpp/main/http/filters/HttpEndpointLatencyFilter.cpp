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

#include "presto_cpp/main/http/filters/HttpEndpointLatencyFilter.h"

#include "presto_cpp/main/http/filters/HttpEndpointLatencyHelpers.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::http::filters {

HttpEndpointLatencyFilter::HttpEndpointLatencyFilter(
    proxygen::RequestHandler* upstream,
    const std::shared_ptr<std::unordered_map<
        proxygen::HTTPMethod,
        std::vector<std::unique_ptr<EndPoint>>>>& endpoints)
    : Filter(upstream), endpoints_(endpoints) {}

// static
void HttpEndpointLatencyFilter::updateLatency(
    const std::string& endpoint,
    uint64_t latencyUs) {
  metricMap().withWLock(
      [&](std::unordered_map<std::string, EndPointMetrics>& map) {
        detail::updateEndpointLatency(map, endpoint, latencyUs);
      });
}

// static
std::vector<HttpEndpointLatencyFilter::EndPointMetrics>
HttpEndpointLatencyFilter::retrieveLatencies() {
  std::vector<HttpEndpointLatencyFilter::EndPointMetrics> result;
  metricMap().withWLock([&](std::unordered_map<
                            std::string,
                            HttpEndpointLatencyFilter::EndPointMetrics>& map) {
    result = detail::retrieveEndpointLatencies(map);
  });
  return result;
}

void HttpEndpointLatencyFilter::onRequest(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  auto path = msg->getPath();
  const auto method = msg->getMethod().value();
  const auto& endpoints = endpoints_->at(method);
  requestEndpoint_ =
      detail::matchHttpEndpoint(msg->getMethodString(), path, endpoints);
  VELOX_CHECK(!requestEndpoint_.empty());

  // Starts the timer.
  timer_ = std::make_unique<velox::MicrosecondTimer>(&timeUs_);
  proxygen::Filter::onRequest(std::move(msg));
}

void HttpEndpointLatencyFilter::requestComplete() noexcept {
  timer_.reset();
  updateLatency(requestEndpoint_, timeUs_);
  proxygen::Filter::requestComplete();
}

void HttpEndpointLatencyFilter::onError(proxygen::ProxygenError err) noexcept {
  timer_.reset();
  updateLatency(requestEndpoint_, timeUs_);
  proxygen::Filter::onError(err);
}

} // namespace facebook::presto::http::filters
