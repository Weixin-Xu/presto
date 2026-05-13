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

#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/http/filters/HttpFilterBackend.h"

namespace facebook::presto::http::filters {

class HttpEndpointLatencyFilter : public proxygen::Filter {
 public:
  struct EndPointMetrics {
    std::string endpoint;
    uint64_t maxLatencyUs;
    uint64_t avgLatencyUs;
    uint64_t count;

    std::string toString() const {
      std::stringstream oss;
      oss << "{'" << endpoint << "' : "
          << detail::succinctMicrosString(maxLatencyUs) << "(max) "
          << detail::succinctMicrosString(avgLatencyUs) << "(avg) " << count
          << "(count)}";
      return oss.str();
    }
  };

  HttpEndpointLatencyFilter(
      proxygen::RequestHandler* upstream,
      const std::shared_ptr<std::unordered_map<
          proxygen::HTTPMethod,
          std::vector<std::unique_ptr<EndPoint>>>>& endpoints);

  static std::vector<EndPointMetrics> retrieveLatencies();

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  static void updateLatency(const std::string& endpoint, uint64_t latencyMs);

  static folly::Synchronized<std::unordered_map<std::string, EndPointMetrics>>&
  metricMap() {
    static folly::Synchronized<std::unordered_map<std::string, EndPointMetrics>>
        metricMap_;
    return metricMap_;
  }

  const std::shared_ptr<std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>>
      endpoints_;
  std::string requestEndpoint_;
  std::unique_ptr<detail::HttpFilterMicrosecondTimer> timer_;
  uint64_t timeUs_{0};
};

class HttpEndpointLatencyFilterFactory
    : public proxygen::RequestHandlerFactory {
 public:
  explicit HttpEndpointLatencyFilterFactory(http::HttpServer* httpServer)
      : endpoints_(std::make_shared<std::unordered_map<
                       proxygen::HTTPMethod,
                       std::vector<std::unique_ptr<EndPoint>>>>(
            httpServer->endpoints())) {}

  void onServerStart(folly::EventBase* /*evb*/) noexcept override {}

  void onServerStop() noexcept override {}

  proxygen::RequestHandler* onRequest(
      proxygen::RequestHandler* handler,
      proxygen::HTTPMessage*) noexcept override {
    return new HttpEndpointLatencyFilter(handler, endpoints_);
  }

 private:
  const std::shared_ptr<std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>>
      endpoints_;
};

} // namespace facebook::presto::http::filters
