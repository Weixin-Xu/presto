cmake_minimum_required(VERSION 3.10)

# set the project name
project(PrestoCpp CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
set(TREAT_WARNINGS_AS_ERRORS 1)
if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

# Known warnings that are benign can be disabled.
set(DISABLED_WARNINGS
    "-Wno-nullability-completeness -Wno-deprecated-declarations")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  add_compile_options(-fdiagnostics-color=always)
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  add_compile_options(-fcolor-diagnostics)
endif()

# Important warnings that must be explicitly enabled.
set(ENABLE_WARNINGS "-Wreorder")

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} ${DISABLED_WARNINGS} ${ENABLE_WARNINGS}")

# Add all Presto options below.
option(PRESTO_ENABLE_S3 "Build S3 support" OFF)
option(PRESTO_ENABLE_HDFS "Build HDFS support" OFF)
option(PRESTO_ENABLE_PARQUET "Enable Parquet support" OFF)
option(PRESTO_ENABLE_TESTING "Enable tests" ON)
option(PRESTO_ENABLE_JEMALLOC "Enable Jemalloc Allocator" OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# find_package: third parties
find_package(bolt CONFIG REQUIRED)

find_package(gflags COMPONENTS shared)

find_library(GLOG glog)

find_library(FMT fmt)

find_library(EVENT event)

find_library(DOUBLE_CONVERSION double-conversion)

find_library(LZ4 lz4)
find_library(LZO lzo2)
find_library(ZSTD zstd)
find_package(ZLIB)
find_library(SNAPPY snappy)

find_package(folly CONFIG REQUIRED)

include_directories(SYSTEM ${FOLLY_INCLUDE_DIRS})

find_package(OpenSSL REQUIRED)

find_package(libsodium REQUIRED)
find_package(proxygen CONFIG REQUIRED)
find_library(PROXYGEN proxygen)
find_library(PROXYGEN_HTTP_SERVER proxygenhttpserver)
find_library(FIZZ fizz)
find_library(WANGLE wangle)

find_library(RE2 re2 REQUIRED)

find_package(fizz CONFIG)
find_package(wangle CONFIG)
find_package(fbthrift CONFIG REQUIRED)
include_directories(SYSTEM ${FBTHRIFT_INCLUDE_DIR})

find_package(nlohmann_json REQUIRED CONFIG)
include_directories(SYSTEM ${nlohmann_json_INCLUDE_DIR})

include_directories(.)
include_directories(${bolt_INCLUDE_DIR})

if(PRESTO_ENABLE_TESTING)
  find_package(GTest CONFIG REQUIRED)
  include_directories(${GTest_INCLUDE_DIR})
endif()

if("${MAX_LINK_JOBS}")
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS "presto_link_job_pool=${MAX_LINK_JOBS}")
else()
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS "presto_link_job_pool=8")
endif()
set(CMAKE_JOB_POOL_LINK presto_link_job_pool)

add_subdirectory(presto_cpp)