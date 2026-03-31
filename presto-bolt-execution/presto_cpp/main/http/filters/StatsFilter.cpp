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

#include "presto_cpp/main/http/filters/StatsFilter.h"

#include "presto_cpp/main/http/filters/StatsFilterBackend.h"

namespace facebook::presto::http::filters {

StatsFilter::StatsFilter(proxygen::RequestHandler* upstream)
    : Filter(upstream) {}

void StatsFilter::onRequest(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  startTime_ = std::chrono::steady_clock::now();
  detail::recordHttpRequestCount();
  Filter::onRequest(std::move(msg));
}

void StatsFilter::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body) {
    requestBodySize_ += body->computeChainDataLength();
  }
  Filter::onBody(std::move(body));
}

void StatsFilter::requestComplete() noexcept {
  detail::recordHttpRequestLatencyMs(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - startTime_)
          .count());
  detail::recordHttpRequestBodySize(requestBodySize_);
  if (detail::deleteStatsFilterOnTerminal()) {
    delete this;
    return;
  }
  Filter::requestComplete();
}

void StatsFilter::onError(proxygen::ProxygenError err) noexcept {
  detail::recordHttpRequestError();
  if (detail::deleteStatsFilterOnTerminal()) {
    delete this;
    return;
  }
  Filter::onError(err);
}

} // namespace facebook::presto::http::filters
