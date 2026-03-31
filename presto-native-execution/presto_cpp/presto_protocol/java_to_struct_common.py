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
import re
import sys
from collections import defaultdict
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

import util
from topological import topological

IGNORED_TYPES = {
    "byte",
    "Integer",
    "Map",
    "int",
    "double",
    "String",
    "List",
    "boolean",
    "long",
    "Optional",
    "Set",
}


def preprocess_file(filename):
    text = util.file_read(filename)

    text = re.sub(r"//.*$", "", text)
    text = re.sub(r"@JsonProperty", "\n@JsonProperty", text)
    text = re.sub(r"{", "\n{", text)

    return text.rstrip().splitlines()


def add_field(
    current_class, filename, field_name, field_type, config, lang, classes, depends
):
    if (
        "EnumMap" in config
        and filename in config.EnumMap
        and field_type in config.EnumMap[filename]
    ):
        field_type = config.EnumMap[filename][field_type]

    field_flag = {}
    field_text = field_type

    field_local = True
    if "super_class" in classes[current_class]:
        super = classes[classes[current_class].super_class]
        if "fields" in super:
            if (
                len(list(filter(lambda x: x.field_name == field_name, super.fields)))
                != 0
            ):
                field_local = False

    for key, value in lang.items():
        if isinstance(value, str):
            field_text = re.sub(key, value, field_text)
        else:
            field_text, n = re.subn(key, value["replace"], field_text)
            if n > 0 and value["flag"]:
                field_flag.update(value["flag"])

    classes[current_class].fields.append(
        util.attrdict(
            field_type=field_type, field_name=field_name, field_text=field_text
        )
    )
    classes[current_class].fields[-1].update(field_flag)
    classes[current_class].fields[-1]._N = len(classes[current_class].fields)
    if current_class == field_type:
        classes[current_class].fields[-1].optional = True
    classes[current_class].fields[-1].field_local = field_local

    if (
        field_type in classes
        and "abstract" in classes[field_type]
        and classes[field_type].abstract
    ):
        classes[current_class].fields[-1].optional = True

    types = set(re.sub("[^a-zA-Z0-9_]+", " ", field_type).split())
    types.discard(current_class)
    depends[current_class].update(types)


def add_extra(class_name, fileroot, config, lang, classes, depends):
    if "ExtraFields" in config and class_name in config.ExtraFields:
        for type, name in config.ExtraFields[class_name].items():
            add_field(class_name, fileroot, name, type, config, lang, classes, depends)
        classes[class_name].fields[-1].last = True
    if "UpdateFields" in config and class_name in config.UpdateFields:
        for type, name in config.UpdateFields[class_name].items():
            update_field(class_name, name, type, classes)


def update_field(class_name, name, type, classes):
    for field in classes[class_name].fields:
        if field.field_name == name:
            field.field_text = type
            break


def member_name(name):
    return name[0].lower() + name[1:]


def special(filepath, current_class, key, classes, depends, license_header_path):
    classes[current_class].class_name = current_class
    (_, stdout, _) = classes[current_class][key] = util.run(
        f"../../scripts/license-header.py --header {license_header_path} --remove "
        + filepath
    )
    classes[current_class][key] = stdout

    for line in classes[current_class][key].rstrip().splitlines():
        match = re.match(r"^.*// dependency[ ]*$", line)
        if match:
            other = line.split()[0]
            depends[current_class].update([other])

        match = re.match(r"^[ ]*// dependency.*$", line)
        if match:
            other = line.split()[2]
            depends[current_class].update([other])


def process_file(
    filepath, config, lang, subclasses, classes, depends, license_header_path
):
    filename = util.get_filename(filepath)

    if filepath.endswith(".hpp.inc"):
        special(
            filepath,
            filename[: -len(".hpp.inc")],
            "hinc",
            classes,
            depends,
            license_header_path,
        )
        return

    if filepath.endswith(".cpp.inc"):
        special(
            filepath,
            filename[: -len(".cpp.inc")],
            "cinc",
            classes,
            depends,
            license_header_path,
        )
        return

    fileroot = os.path.splitext(filename)[0]
    current_enum = ""
    current_class = ""
    json_mode = False

    for line in preprocess_file(filepath):
        fields = line.split()

        if re.match(r"^ *$", line):
            continue

        match = re.match(r" *(public|private) enum .*", line)
        if match:
            current_enum = fields[2]

            if (
                "EnumMap" in config
                and fileroot in config.EnumMap
                and current_enum in config.EnumMap[fileroot]
            ):
                current_enum = config.EnumMap[fileroot][current_enum]

            classes[current_enum].class_name = current_enum
            classes[current_enum].enum = True
            classes[current_enum].elements = []

        if current_enum != "":
            line = re.sub(r"\([^)]+\)", " ", line)
            fields = line.split()

        match = re.match(r"^[A-Z0-9_]+[,;]?$", fields[0])
        if current_enum != "" and match:
            line = re.sub(r"//.*$", "", line)
            line = re.sub(r"[{;]", "", line)

            names = line.split(",")
            for name in names:
                name = name.strip()
                if name == "":
                    continue

                name = name.split()[0]

                classes[current_enum].elements.append(util.attrdict(element=name))
                classes[current_enum].elements[-1]._N = len(
                    classes[current_enum].elements
                )

        match = re.match(r".*;$|^}$|^};$", line)
        if current_enum != "" and match:
            classes[current_enum].elements[-1]._last = True
            current_enum = ""

        match = re.match(r".*@JsonCreator.*", line)
        if match:
            json_mode = True

        line = re.sub(r"[()]", " ", line)
        fields = line.split()

        match = re.match(r" *public.*", line)
        if json_mode and match:
            current_class = fields[1] if fields[1] != "static" else fields[2]
            classes[current_class].class_name = current_class
            classes[current_class].struct = True
            classes[current_class].fields = []

            if current_class in subclasses:
                classes[current_class].subclass = True
                classes[current_class].super_class = subclasses[current_class].super
                classes[current_class].json_key = subclasses[current_class].key

        match = re.match(r" *@JsonProperty.*", line)
        if json_mode and match and len(fields) >= 3:
            line = re.sub(r"^[^@]*", "", line)
            line = re.sub(r"@Nullable", "", line)
            fields = line.split()
            fields[-1] = re.sub(r",", "", fields[-1])

            if fields[1][0] == '"':
                type = " ".join(fields[2:-1])
                name = re.sub('"', "", fields[1])
            else:
                type = " ".join(fields[1:-1])
                name = fields[-1]

            add_field(
                current_class, fileroot, name, type, config, lang, classes, depends
            )

        match = re.match(r" *{ *", line)
        if json_mode and match:
            add_extra(current_class, fileroot, config, lang, classes, depends)

            if len(classes[current_class].fields) == 0:
                classes.pop(current_class)
                json_mode = False
                continue

            json_mode = False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract Presto protocol data struture from Java annotations"
    )
    parser.add_argument(
        "-c", "--config", default="presto_protocol.yml", help="config file"
    )
    parser.add_argument("-l", "--lang", default="cpp", help="output language type map")
    parser.add_argument(
        "-j",
        "--json",
        default=False,
        action="store_true",
        help="output language type map",
    )
    parser.add_argument("files", metavar="FILES", nargs="*", help="files to process")
    return parser.parse_args()


def build_abstract_classes(config, classes, subclasses):
    for abstract_name, abstract_value in config.AbstractClasses.items():
        classes[abstract_name].class_name = abstract_name
        classes[abstract_name].field_name = member_name(abstract_name)
        classes[abstract_name].abstract = True
        classes[abstract_name].super_class = abstract_value.super
        if "comparable" in abstract_value:
            classes[abstract_name].comparable = True
        classes[abstract_name].subclasses = []

        if abstract_value.subclasses:
            for subclass in abstract_value.subclasses:
                subclasses[subclass.name] = util.attrdict(
                    super=abstract_name, key=subclass.key
                )

                classes[abstract_name].subclasses.append(
                    util.attrdict(
                        type=subclass.name,
                        name=member_name(subclass.name),
                        key=subclass.key,
                    )
                )
                classes[abstract_name].subclasses[-1]._N = len(
                    classes[abstract_name].subclasses
                )

        if classes[abstract_name].subclasses:
            classes[abstract_name].subclasses[-1]._last = True


def main(language, license_header_path, presto_home=None):
    args = parse_args()
    config = util.load_yaml(args.config)
    files = util.input_files(args.files)
    lang = language.get(args.lang, None)["TypeMap"]
    presto_home = Path(
        presto_home
        or os.environ.get("PRESTO_HOME")
        or os.environ.get("HOME") + "/presto"
    ).resolve()

    files.extend([str(presto_home / file) for file in config.JavaClasses])

    classes = defaultdict(util.attrdict)
    depends = defaultdict(set)
    subclasses = {}

    build_abstract_classes(config, classes, subclasses)

    for abstract_name, abstract_value in config.AbstractClasses.items():
        if "source" in abstract_value:
            file = abstract_value.source
            process_file(
                str(presto_home / file),
                config,
                lang,
                subclasses,
                classes,
                depends,
                license_header_path,
            )
        else:
            classes[abstract_name].fields = []
            add_extra(abstract_name, abstract_name, config, lang, classes, depends)

    for file in files:
        process_file(
            file, config, lang, subclasses, classes, depends, license_header_path
        )

    depends = list(topological({k: list(v) for k, v in depends.items()}))[::-1]

    comment = "// This file is generated DO NOT EDIT @" + "generated"
    result = [{"comment": comment}]
    result += [classes[name] for name in depends if name in classes]
    if "AddToOutput" in config:
        result += [classes[name] for name in config.AddToOutput]

    if args.json:
        print(util.to_json(result))

    return 0
