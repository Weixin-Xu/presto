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

#include <proxygen/httpserver/ResponseBuilder.h>

namespace facebook::presto::http::filters::detail {

inline void sendRejectedResponse(
    proxygen::RequestHandler*& upstream,
    proxygen::ResponseHandler* downstream,
    proxygen::ProxygenError error,
    uint16_t status,
    const char* reason) {
  upstream->onError(error);
  upstream = nullptr;
  proxygen::ResponseBuilder(downstream).status(status, reason).sendWithEOM();
}

} // namespace facebook::presto::http::filters::detail
