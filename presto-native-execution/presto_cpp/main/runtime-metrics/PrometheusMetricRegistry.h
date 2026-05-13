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
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>
#include <prometheus/text_serializer.h>

namespace facebook::presto::prometheus::detail {

inline constexpr std::string_view kSummarySuffix{"_summary"};

class PrometheusMetricRegistry {
 public:
  explicit PrometheusMetricRegistry(
      const std::map<std::string, std::string>& labels)
      : registry_(std::make_shared<::prometheus::Registry>()),
        labels_(labels.begin(), labels.end()) {}

  const ::prometheus::Labels& labels() const {
    return labels_;
  }

  ::prometheus::Registry& registry() const {
    return *registry_;
  }

  std::string fetchMetrics() const {
    ::prometheus::TextSerializer serializer;
    return serializer.Serialize(registry_->Collect());
  }

  static std::string sanitizeMetricKey(std::string_view key) {
    std::string sanitizedMetricKey(key);
    std::replace(
        sanitizedMetricKey.begin(), sanitizedMetricKey.end(), '.', '_');
    return sanitizedMetricKey;
  }

  static ::prometheus::Histogram::BucketBoundaries createBucketBoundaries(
      int64_t bucketWidth,
      int64_t min,
      int64_t max) {
    ::prometheus::Histogram::BucketBoundaries bucketBoundaries;
    auto numBuckets = (max - min) / bucketWidth;
    auto bound = min + bucketWidth;
    while (numBuckets-- > 0) {
      bucketBoundaries.push_back(bound);
      bound += bucketWidth;
    }
    return bucketBoundaries;
  }

  static ::prometheus::Summary::Quantiles createQuantiles(
      const std::vector<int32_t>& pcts) {
    ::prometheus::Summary::Quantiles quantiles;
    quantiles.reserve(pcts.size());
    for (auto pct : pcts) {
      quantiles.push_back(
          ::prometheus::detail::CKMSQuantiles::Quantile(
              pct / static_cast<double>(100), 0));
    }
    return quantiles;
  }

 private:
  std::shared_ptr<::prometheus::Registry> registry_;
  ::prometheus::Labels labels_;
};

} // namespace facebook::presto::prometheus::detail
