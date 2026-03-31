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

void toThrift(const protocol::Lifespan& proto, thrift::Lifespan& thrift) {
  thrift.grouped_ref() = proto.isgroup;
  thrift.groupId_ref() = proto.groupid;
}

void fromThrift(const thrift::Lifespan& thrift, protocol::Lifespan& proto) {
  proto.isgroup = *thrift.grouped_ref();
  proto.groupid = *thrift.groupId_ref();
}
