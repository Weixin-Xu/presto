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

namespace facebook::presto::protocol::hive {

HiveMetadataUpdateHandle::HiveMetadataUpdateHandle() noexcept {
  _type = "hive";
}

void to_json(json& j, const HiveMetadataUpdateHandle& p) {
  j = json::object();
  j["@type"] = "hive";
  to_json_key(
      j,
      "requestId",
      p.requestId,
      "HiveMetadataUpdateHandle",
      "UUID",
      "requestId");
  to_json_key(
      j,
      "schemaTableName",
      p.schemaTableName,
      "HiveMetadataUpdateHandle",
      "SchemaTableName",
      "schemaTableName");
  to_json_key(
      j,
      "partitionName",
      p.partitionName,
      "HiveMetadataUpdateHandle",
      "String",
      "partitionName");
  to_json_key(
      j,
      "fileName",
      p.fileName,
      "HiveMetadataUpdateHandle",
      "String",
      "fileName");
}

void from_json(const json& j, HiveMetadataUpdateHandle& p) {
  p._type = j["@type"];
  from_json_key(
      j,
      "requestId",
      p.requestId,
      "HiveMetadataUpdateHandle",
      "UUID",
      "requestId");
  from_json_key(
      j,
      "schemaTableName",
      p.schemaTableName,
      "HiveMetadataUpdateHandle",
      "SchemaTableName",
      "schemaTableName");
  from_json_key(
      j,
      "partitionName",
      p.partitionName,
      "HiveMetadataUpdateHandle",
      "String",
      "partitionName");
  from_json_key(
      j,
      "fileName",
      p.fileName,
      "HiveMetadataUpdateHandle",
      "String",
      "fileName");
}

} // namespace facebook::presto::protocol::hive
