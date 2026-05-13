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
    Path(__file__).resolve().parents[4]
    / "presto-native-execution"
    / "presto_cpp"
    / "main"
    / "thrift"
    / "presto_protocol_to_thrift_common.py"
).resolve()
_SPEC = importlib.util.spec_from_file_location(
    "presto_native_thrift_protocol_to_thrift_common", _NATIVE_COMMON
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

if __name__ == "__main__":
    sys.exit(
        _MODULE.main(
            skip_structs=False,
            wrapper_structs=False,
            connector_structs=False,
            struct_in_protocol_core=False,
            support_union=False,
            require_protocol_match_for_special=True,
            license_script="../../../scripts/license-header.py --header ../../../license.header",
        )
    )
