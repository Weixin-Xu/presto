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

add_library(presto_type_converter OBJECT TypeParser.cpp)

target_link_libraries(presto_type_converter bolt::bolt)

add_library(
  presto_types OBJECT
  PrestoToBoltQueryPlan.cpp PrestoToBoltExpr.cpp BoltPlanValidator.cpp
  PrestoToBoltSplit.cpp PrestoToBoltConnector.cpp)
add_dependencies(presto_types presto_operators presto_type_converter )

target_link_libraries(presto_types presto_type_converter bolt::bolt)

set_property(TARGET presto_types PROPERTY JOB_POOL_LINK presto_link_job_pool)

add_library(presto_function_metadata OBJECT FunctionMetadata.cpp)

target_link_libraries(presto_function_metadata bolt::bolt)

add_library(presto_bolt_conversion OBJECT BoltPlanConversion.cpp)

target_link_libraries(presto_bolt_conversion bolt::bolt)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
