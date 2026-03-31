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

#include <ctime>
#include <string>

#include <fmt/format.h>
#include <proxygen/lib/utils/Time.h>

namespace facebook::presto::http::filters::detail {

inline std::string formatAccessLogLine(
    proxygen::TimePoint startTime,
    const std::string& method,
    const std::string& url,
    const std::string& version,
    const std::string& remoteAddr,
    uint16_t statusCode,
    size_t bytesSent,
    const std::string& httpReferer,
    const std::string& httpUserAgent) {
  std::time_t requestStartTime = proxygen::toTimeT(proxygen::getCurrentTime());
  static struct tm formattedStartTime;
  localtime_r(&requestStartTime, &formattedStartTime);
  char timeBuf[64];
  std::strftime(timeBuf, 64, "%F %T", &formattedStartTime);
  const auto latency = proxygen::millisecondsSince(startTime);

  return fmt::format(
      "{} - - [{}] \"{} {} {}\" {:d} {:d} {} {} {:d}",
      remoteAddr.c_str(),
      timeBuf,
      method.c_str(),
      url.c_str(),
      version.c_str(),
      statusCode,
      bytesSent,
      httpReferer.c_str(),
      httpUserAgent.c_str(),
      latency.count());
}

} // namespace facebook::presto::http::filters::detail
