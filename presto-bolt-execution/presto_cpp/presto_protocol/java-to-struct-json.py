#!/usr/bin/env python3
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

import importlib.util
import sys
from pathlib import Path

_NATIVE_COMMON = (
    Path(__file__).resolve().parents[2]
    / ".."
    / "presto-native-execution"
    / "presto_cpp"
    / "presto_protocol"
    / "java_to_struct_common.py"
).resolve()
_SPEC = importlib.util.spec_from_file_location(
    "presto_native_java_to_struct_common", _NATIVE_COMMON
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

LANGUAGE = {
    "cpp": {
        "TypeMap": {
            r"([ ,<])(ColumnHandle|PlanNode|RowExpression|ConnectorMetadataUpdateHandle|ConnectorDeleteTableHandle|ConnectorDistributedProcedureHandle|ConnectorMergeTableHandle)([ ,>])": r"\1std::shared_ptr<\2>\3",
            r"Optional<int\[\]>": "Optional<List<int>>",
            r"Optional<byte\[\]>": "Optional<List<byte>>",
            r"int\[\]": "List<int>",
            r"byte\[\]": "List<byte>",
            "OptionalInt": "Optional<int>",
            "boolean": "bool",
            "long": "int64_t",
            "List<byte>": "String",
            r"Set<(.*)>": r"List<\1>",
            r"Optional<(.*)>": {"replace": r"\1", "flag": {"optional": True}},
            r"ExchangeNode.Type": "ExchangeNodeType",
            r"com\.facebook\.presto\.spi\.MergeHandle": "MergeHandle",
            r"com\.facebook\.presto\.spi\.connector\.RowChangeParadigm": "RowChangeParadigm",
            r"com\.facebook\.presto\.spi\.plan\.DataOrganizationSpecification": "DataOrganizationSpecification",
            r"com\.facebook\.presto\.sidecar\.NativeSidecarFailureInfo": "NativeSidecarFailureInfo",
            r"com\.facebook\.presto\.metadata\.BuiltInFunctionKind": "BuiltInFunctionKind",
        }
    },
    "pb": {
        "TypeMap": {
            r"Optional<int\[\]>": "Optional<List<int>>",
            r"Optional<byte\[\]>": "Optional<List<byte>>",
            r"int\[\]": "List<int>",
            r"byte\[\]": "List<byte>",
            "OptionalInt": "Optional<int>",
            "boolean": "bool",
            "int": "int32",
            "long": "int64",
            "String": "string",
            "List<byte>": "bytes",
            r"Set<(.*)>": r"List<\1>",
            r"Optional<(.*)>": {"replace": r"\1", "flag": {"optional": True}},
            r"List<(.*)>": {"replace": r"repeated \1", "flag": {"repeated": True}},
            r"Map<(.*)>": {"replace": r"map<\1>", "flag": {"repeated": True}},
        }
    },
}

if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[2]
    sys.exit(
        _MODULE.main(
            LANGUAGE,
            str((script_dir / "../../license.header").resolve()),
            presto_home=str(repo_root),
        )
    )
