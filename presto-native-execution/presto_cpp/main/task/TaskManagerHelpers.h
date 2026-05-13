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

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <fmt/core.h>

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::task {

template <typename Backend, typename PrestoTaskPtr>
inline void initializeNewTask(
    const std::string& baseUri,
    int64_t createTimeInMillis,
    const protocol::TaskId& taskId,
    PrestoTaskPtr& prestoTask) {
  Backend::setTaskCreateTime(prestoTask->info.stats, createTimeInMillis);
  prestoTask->info.needsPlan = true;

  struct UuidSplit {
    int64_t lo;
    int64_t hi;
  };

  union UuidParse {
    boost::uuids::uuid uuid;
    UuidSplit split;
  };

  UuidParse uuid = {boost::uuids::random_generator()()};

  prestoTask->info.taskStatus.taskInstanceIdLeastSignificantBits = uuid.split.lo;
  prestoTask->info.taskStatus.taskInstanceIdMostSignificantBits = uuid.split.hi;
  prestoTask->info.taskStatus.state = protocol::TaskState::RUNNING;
  prestoTask->info.taskStatus.self = fmt::format("{}/v1/task/{}", baseUri, taskId);
}

} // namespace facebook::presto::task
