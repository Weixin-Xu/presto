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

set(PRESTO_NATIVE_CPP_DIR ${CMAKE_SOURCE_DIR}/../presto-native-execution/presto_cpp)
set(PRESTO_NATIVE_PROTOCOL_DIR ${PRESTO_NATIVE_CPP_DIR}/presto_protocol)
set(PRESTO_NATIVE_THRIFT_DIR ${PRESTO_NATIVE_CPP_DIR}/main/thrift)
set(PRESTO_NATIVE_EXTERNAL_JSON_DIR ${PRESTO_NATIVE_CPP_DIR}/external/json)
set(PRESTO_NATIVE_INCLUDE_ROOT ${CMAKE_CURRENT_BINARY_DIR}/include)

file(MAKE_DIRECTORY ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp)
file(MAKE_DIRECTORY ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/external)
file(MAKE_DIRECTORY ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/main)
file(CREATE_LINK
     ${PRESTO_NATIVE_PROTOCOL_DIR}
     ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/presto_protocol
     SYMBOLIC COPY_ON_ERROR)
file(CREATE_LINK
     ${PRESTO_NATIVE_EXTERNAL_JSON_DIR}
     ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/external/json
     SYMBOLIC COPY_ON_ERROR)
file(CREATE_LINK
     ${PRESTO_NATIVE_THRIFT_DIR}
     ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/main/thrift
     SYMBOLIC COPY_ON_ERROR)

set(PRESTO_NATIVE_INCLUDE_ROOT ${PRESTO_NATIVE_INCLUDE_ROOT} PARENT_SCOPE)

add_library(
  presto_protocol OBJECT
  ${PRESTO_NATIVE_PROTOCOL_DIR}/presto_protocol.cpp
  ${PRESTO_NATIVE_PROTOCOL_DIR}/Base64Util.cpp
  ${PRESTO_NATIVE_PROTOCOL_DIR}/core/DataSize.cpp
  ${PRESTO_NATIVE_PROTOCOL_DIR}/core/Duration.cpp
  ${PRESTO_NATIVE_PROTOCOL_DIR}/core/ConnectorProtocol.cpp)

target_link_libraries(presto_protocol bolt::bolt ${RE2})
target_include_directories(presto_protocol BEFORE PRIVATE ${PRESTO_NATIVE_INCLUDE_ROOT})

find_program(THRIFT1 thrift1)
find_library(THRIFTCPP2 thriftcpp2)
find_library(THRIFT_CORE thrift-core)
find_library(THRIFT_PROTOCOL thriftprotocol)
find_library(THRIFT_METADATA thriftmetadata)
find_library(THRIFT_TRANSPORT transport)
find_library(THRIFT_TYPEREP thrifttyperep)
get_filename_component(THRIFT_BIN_DIR ${THRIFT1} DIRECTORY)
get_filename_component(THRIFT_PACKAGE_DIR ${THRIFT_BIN_DIR}/../.. ABSOLUTE)
set(FBTHRIFT_THRIFT_INCLUDE_DIR ${THRIFT_PACKAGE_DIR}/b)
if(NOT EXISTS ${FBTHRIFT_THRIFT_INCLUDE_DIR}/thrift/annotation/cpp.thrift)
  message(FATAL_ERROR "Unable to locate fbthrift thrift includes under ${FBTHRIFT_THRIFT_INCLUDE_DIR}")
endif()

set(THRIFT_INCLUDES ${fbthrift_INCLUDE_DIRS})
include(${PRESTO_NATIVE_THRIFT_DIR}/ThriftLibrary.cmake)

find_package(fbthrift CONFIG REQUIRED)
find_package(wangle CONFIG REQUIRED)

thrift_library(
  presto_thrift
  PrestoThrift
  cpp2
  ""
  ${PRESTO_NATIVE_THRIFT_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ".."
  THRIFT_INCLUDE_DIRECTORIES
  ${FBTHRIFT_THRIFT_INCLUDE_DIR})

target_link_libraries(presto_thrift-cpp2
                      ${THRIFT_PROTOCOL}
                      ${THRIFT_METADATA}
                      ${THRIFT_CORE}
                      ${THRIFT_TRANSPORT}
                      FBThrift::fbthrift
                      ${THRIFT_TYPEREP}
                      wangle::wangle
                      Folly::folly)
set(presto_thrift_INCLUDES ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(presto_thrift-cpp2 PUBLIC ${presto_thrift_INCLUDES})
target_include_directories(presto_thrift-cpp2-obj PUBLIC
  ${PRESTO_NATIVE_INCLUDE_ROOT}
  ${THRIFT_INCLUDES}
  ${wangle_INCLUDE_DIRS}
  ${jemalloc_INCLUDE_DIRS})

add_library(presto_thrift_extra ${CMAKE_SOURCE_DIR}/presto_cpp/main/ProtocolToThrift.cpp)

target_include_directories(presto_thrift_extra PUBLIC
  ${PRESTO_NATIVE_INCLUDE_ROOT}
  ${presto_thrift_INCLUDES}
  ${THRIFT_INCLUDES}
  ${CMAKE_BINARY_DIR})

find_package(nlohmann_json CONFIG REQUIRED)
target_link_libraries(presto_thrift_extra
  nlohmann_json::nlohmann_json
  wangle::wangle
  Folly::folly
  re2::re2)
target_include_directories(presto_thrift_extra PUBLIC ${presto_thrift_INCLUDES})
add_dependencies(presto_thrift_extra presto_thrift-cpp2)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(${PRESTO_NATIVE_PROTOCOL_DIR}/tests
                   ${CMAKE_CURRENT_BINARY_DIR}/presto_protocol_tests)
endif()
