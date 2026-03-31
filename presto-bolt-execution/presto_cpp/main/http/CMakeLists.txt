# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_library(presto_http HttpClient.cpp HttpServer.cpp)
target_include_directories(presto_http PUBLIC ${CMAKE_SOURCE_DIR}/..)

if(PRESTO_ENABLE_JWT)
  add_compile_definitions(JWT_DISABLE_PICOJSON)
  target_include_directories(
    presto_http PRIVATE ${CMAKE_SOURCE_DIR}/presto_cpp/external/json)
endif()

add_subdirectory(filters)

target_link_libraries(
  presto_http
  http_filters
  presto_common
  bolt::bolt
  proxygen::proxygenhttpserver
  libsodium::libsodium 
  ${OPENSSL_SSL_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  Folly::folly
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  ${FMT}
  re2::re2)

set_property(TARGET presto_http PROPERTY JOB_POOL_LINK presto_link_job_pool)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
