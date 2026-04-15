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
set(PRESTO_NATIVE_THRIFT_DIR ${PRESTO_NATIVE_CPP_DIR}/main/thrift)
set(PRESTO_NATIVE_THRIFT_FILE ${PRESTO_NATIVE_THRIFT_DIR}/presto_thrift.thrift)
set(PRESTO_NATIVE_INCLUDE_ROOT ${CMAKE_CURRENT_BINARY_DIR}/include)
set(PRESTO_NATIVE_THRIFT_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/thrift)
set(PRESTO_NATIVE_THRIFT_PREPROCESSED ${PRESTO_NATIVE_THRIFT_BUILD_DIR}/presto_thrift.thrift)

file(MAKE_DIRECTORY ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp)
file(MAKE_DIRECTORY ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/main)
file(MAKE_DIRECTORY ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/main/thrift)
file(REMOVE_RECURSE ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/presto_protocol)
file(REMOVE_RECURSE ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/external)
file(MAKE_DIRECTORY ${PRESTO_NATIVE_THRIFT_BUILD_DIR})
file(READ ${PRESTO_NATIVE_THRIFT_FILE} PRESTO_NATIVE_THRIFT_CONTENT)
string(REPLACE
  "  @thrift.DeprecatedUnvalidatedAnnotations{items = {\"drift.recursive_reference\": \"true\"}}\n"
  ""
  PRESTO_NATIVE_THRIFT_CONTENT
  "${PRESTO_NATIVE_THRIFT_CONTENT}")
file(WRITE ${PRESTO_NATIVE_THRIFT_PREPROCESSED} "${PRESTO_NATIVE_THRIFT_CONTENT}")
file(CREATE_LINK
     ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2
     ${PRESTO_NATIVE_INCLUDE_ROOT}/presto_cpp/main/thrift/gen-cpp2
     SYMBOLIC COPY_ON_ERROR)

set(PRESTO_NATIVE_INCLUDE_ROOT ${PRESTO_NATIVE_INCLUDE_ROOT} PARENT_SCOPE)

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
  ${PRESTO_NATIVE_THRIFT_BUILD_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ".."
  THRIFT_INCLUDE_DIRECTORIES
  ${FBTHRIFT_THRIFT_INCLUDE_DIR})

get_target_property(PRESTO_THRIFT_OBJ_SOURCES presto_thrift-cpp2-obj SOURCES)
set(PRESTO_THRIFT_OBJ_SOURCES
  ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/presto_thrift_constants.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/presto_thrift_data.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/presto_thrift_metadata.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/presto_thrift_types.cpp)
set_property(TARGET presto_thrift-cpp2-obj PROPERTY SOURCES "${PRESTO_THRIFT_OBJ_SOURCES}")

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
  $<TARGET_PROPERTY:Folly::folly,INTERFACE_INCLUDE_DIRECTORIES>
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
