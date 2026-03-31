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

import argparse
import os
import sys
from pathlib import Path

_PROTO_DIR = (Path(__file__).resolve().parents[2] / "presto_protocol").resolve()
if str(_PROTO_DIR) not in sys.path:
    sys.path.insert(0, str(_PROTO_DIR))

import util


def eprint(text):
    print(text, file=sys.stderr)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate Presto protocol to/from Presto Thrift JSON data"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="presto_protocol-to-thrift-json.yml",
        help="config file",
    )
    parser.add_argument("thrift", help="Thrift spec as JSON")
    parser.add_argument("protocol", help="Presto protocol spec as JSON")

    return parser.parse_args()


def load_special_file(filename, thrift_item, key, license_script):
    if os.path.isfile(filename):
        (_, stdout, _) = util.run(f"{license_script} --remove " + filename)
        thrift_item[key] = stdout
        return True
    return False


def verify(thrift_item, protocol_item):
    thrift_field_set = {t.proto_name for t in thrift_item.fields}
    protocol_field_set = {p.field_name for p in protocol_item.fields}
    valid_fields = thrift_field_set.intersection(protocol_field_set)

    for field in thrift_item.fields:
        if field.field_name in valid_fields:
            field["convert"] = True

    if len((thrift_field_set - protocol_field_set)) != 0:
        eprint(
            "Missing protocol fields: "
            + thrift_item.class_name
            + " "
            + str(thrift_field_set - protocol_field_set)
        )

    if len((protocol_field_set - thrift_field_set)) != 0:
        eprint(
            "Missing thrift fields: "
            + thrift_item.class_name
            + " "
            + str(protocol_field_set - thrift_field_set)
        )


def process_fields(thrift_item, config_item):
    for field in thrift_item.fields:
        if (
            config_item is not None
            and "fields" in config_item
            and field.field_name in config_item.fields
        ):
            field["proto_name"] = config_item.fields[field.field_name].field_name
        else:
            field["proto_name"] = field.field_name


def main(
    *,
    skip_structs,
    wrapper_structs,
    connector_structs,
    struct_in_protocol_core,
    support_union,
    require_protocol_match_for_special,
    license_script,
):
    args = parse_args()
    config = util.load_yaml(args.config)
    thrift = util.load_yaml(args.thrift)
    protocol = util.load_yaml(args.protocol)
    skip_struct = set(config.get("SkipStruct", []))
    struct_in_protocol_core = set(config.get("StructInProtocolCore", []))
    wrapper_struct = set(config.get("WrapperStruct", []))
    connector_struct = set(config.get("ConnectorStruct", []))
    special = set(config.get("Special", []))
    struct_map = config.get("StructMap", {})

    pmap = {}
    for item in protocol:
        if "class_name" in item:
            pmap[item.class_name] = item

    comment = "// This file is generated DO NOT EDIT @" + "generated"
    result = [{"comment": comment}]

    if skip_structs:
        thrift = [
            item
            for item in thrift
            if "class_name" in item and item.class_name not in skip_struct
        ]

    for thrift_item in thrift:
        if "class_name" not in thrift_item:
            continue

        if struct_in_protocol_core and thrift_item.class_name in struct_in_protocol_core:
            thrift_item["core"] = "true"

        if support_union and "union" in thrift_item and thrift_item.class_name not in special:
            thrift_item["proto_name"] = thrift_item.class_name.removesuffix("Union")
            if (
                thrift_item.class_name in struct_map
                and "fields" in struct_map[thrift_item.class_name]
            ):
                config_item = struct_map[thrift_item.class_name].fields
                for field in thrift_item.fields:
                    if field.field_name in config_item:
                        field["proto_field_type"] = config_item[
                            field.field_name
                        ].field_type
            continue

        if wrapper_structs and thrift_item.class_name in wrapper_struct:
            thrift_item["wrapper"] = "true"
            del thrift_item["struct"]
            continue

        if connector_structs and thrift_item.class_name in connector_struct:
            thrift_item["connector"] = "true"
            del thrift_item["struct"]

        if thrift_item.class_name in special:
            hfile = "./special/" + thrift_item.class_name + ".hpp.inc"
            load_special_file(hfile, thrift_item, "hinc", license_script)

            cfile = "./special/" + thrift_item.class_name + ".cpp.inc"
            load_special_file(cfile, thrift_item, "cinc", license_script)
            continue

        if (thrift_item.class_name in pmap) or (
            thrift_item.class_name in struct_map
            and struct_map[thrift_item.class_name].class_name in pmap
        ):
            protocol_item = (
                pmap[thrift_item.class_name]
                if thrift_item.class_name in pmap
                else pmap[struct_map[thrift_item.class_name].class_name]
            )
            thrift_item["proto_name"] = protocol_item.class_name

            config_item = None
            if "struct" in thrift_item:
                if thrift_item.class_name in struct_map:
                    config_item = struct_map[thrift_item.class_name]
                    thrift_item["proto_name"] = config_item.class_name

                process_fields(thrift_item, config_item)

                if "struct" in protocol_item:
                    verify(thrift_item, protocol_item)
                elif require_protocol_match_for_special:
                    eprint(
                        "Thrift struct missing from presto_protocol: "
                        + thrift_item.class_name
                    )
        else:
            eprint(
                "Thrift item missing from presto_protocol: " + thrift_item.class_name
            )

    result.extend(thrift)
    print(util.to_json(result))
    return 0
