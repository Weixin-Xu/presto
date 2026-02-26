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
#include "presto_cpp/main/types/PrestoToBoltSplit.h"
#include "presto_cpp/main/types/PrestoToBoltConnector.h"
#include "bolt/exec/Exchange.h"

using namespace bytedance::bolt;

namespace facebook::presto {

bytedance::bolt::exec::Split toBoltSplit(
    const presto::protocol::ScheduledSplit& scheduledSplit) {
  const auto& connectorSplit = scheduledSplit.split.connectorSplit;
  const auto splitGroupId = scheduledSplit.split.lifespan.isgroup
      ? scheduledSplit.split.lifespan.groupid
      : -1;
  if (auto remoteSplit = std::dynamic_pointer_cast<const protocol::RemoteSplit>(
          connectorSplit)) {
    return bytedance::bolt::exec::Split(
        std::make_shared<exec::RemoteConnectorSplit>(
            remoteSplit->location.location),
        splitGroupId);
  }

  if (std::dynamic_pointer_cast<const protocol::EmptySplit>(connectorSplit)) {
    // We return NULL for empty splits to signal to do nothing.
    return bytedance::bolt::exec::Split(nullptr, splitGroupId);
  }

  auto& connector = getPrestoToBoltConnector(connectorSplit->_type);
  auto boltSplit = connector.toBoltSplit(
      scheduledSplit.split.connectorId,
      connectorSplit.get(),
      &scheduledSplit.split.splitContext);
  return bytedance::bolt::exec::Split(std::move(boltSplit), splitGroupId);
}

} // namespace facebook::presto
