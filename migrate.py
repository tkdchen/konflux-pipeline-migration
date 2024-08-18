import argparse
import io
import json
import logging
import os
import re
import subprocess

from typing import Callable, Final
from ruamel.yaml import YAML
from fn import append, apply, delete_if, delete_key, with_path, if_matches, nth

logging.basicConfig(
    level=logging.DEBUG, format="%(levelname)s:%(name)s:%(asctime)s:%(message)s"
)
logger = logging.getLogger("migration")

OP_ADDED: Final = "added"
OP_REMOVED: Final = "removed"
FIELD_TYPE_LIST: Final = "list"
FIELD_TYPE_MAP: Final = "map"


def count_leading_spaces(s: str) -> int:
    n = 0
    for i in s:
        if i == " ":
            n += 1
        else:
            break
    return n


def convert_difference(difference: str):
    yaml_lines = []
    read_buf = io.StringIO(difference)
    while True:
        line = read_buf.readline()
        if not line:
            break
        s = line.rstrip()
        if not s:
            continue
        spaces_n = count_leading_spaces(s)
        match spaces_n:
            case 0:
                yaml_lines.append(f'"{s}":')
            case 2:
                yaml_lines.append(f'  "{s[2:]}": |')
            case _:
                yaml_lines.append(" " * spaces_n + s[spaces_n:])

    yaml_content = "\n".join(yaml_lines)
    yaml = YAML(typ="safe")
    return yaml.load(yaml_content)


def compare_pipeline_definitions(from_: str, to: str):
    compare_cmd = [
        "dyff",
        "between",
        "--omit-header",
        "--no-table-style",
        "--detect-kubernetes",
        "--set-exit-code",
        from_,
        to,
    ]
    proc = subprocess.run(compare_cmd, capture_output=True, text=True)
    if proc.returncode == 0:
        return {}
    if proc.returncode == 1:
        return convert_difference(proc.stdout)
    raise RuntimeError(f"Difference comparison error: {proc.stderr}")


def load_list_details(s: str):
    yaml = YAML(typ="safe")
    piece = yaml.load("list:\n" + s)
    return piece["list"]


def load_map_details(s: str):
    return YAML(typ="safe").load(s)


def json_compact_dumps(o) -> str:
    return json.dumps(o, separators=(", ", ": "))


LIST_MAP_ACTIONS_RE: Final = re.compile(
    r"^. (?P<count>[a-z]+) (?P<type>list|map) (entry|entries) (?P<operation>added|removed):$"
)

TK_LIST_FIELDS: Final = ["params", "tasks", "workspaces"]


def is_tk_list_fields(name: str) -> bool:
    return name in TK_LIST_FIELDS


def generate_yq_commands(differences: dict[str, dict[str, str]]) -> list[str]:
    """Generate yq commands

    spec.workspaces             -> filter pipe
      + one list entry added:   -> operator: del(), +=, =
        - name: netrc
          optional: true

    :param differences: a mapping from path to the detail.
    :type differences: dict[str, dict[str, str]]
    """
    exprs: list[str] = []  # yq expressions

    path_pattern = r"^spec\.tasks\.(?P<task_name>[\w-]+)(\.params)?$"

    for path in differences:
        if not re.match(path_pattern, path):
            continue

        path_filters: list[str] = []
        parts = path.split(".")

        for i, part in enumerate(parts):
            if i > 0 and is_tk_list_fields(parts[i - 1]):
                path_filters[-1] += "[]"
                path_filters.append(f'select(.name == "{part}")')
            else:
                path_filters.append("." + part)

        for action in differences[path]:
            path_filters_pipe = " | ".join(path_filters)
            detail = differences[path][action]
            if (m := LIST_MAP_ACTIONS_RE.match(action)) is not None:
                op = m.group("operation")
                type_ = m.group("type")
                if type_ == "list":
                    for detail_item in load_list_details(detail):
                        if op == OP_ADDED:
                            exprs.append(f"({path_filters_pipe}) += {json_compact_dumps(detail_item)}")
                        elif op == OP_REMOVED:
                            name = detail_item["name"]
                            value = detail_item["value"]
                            e = f'del({path_filters_pipe}[] | select(.name == "{name}" and .value == "{value}"))'
                            exprs.append(e)
                elif type_ == "map":
                    maps = load_map_details(detail)
                    if op == OP_ADDED:
                        exprs.append(f"({path_filters_pipe}) += {json_compact_dumps(maps)}")
                    elif op == OP_REMOVED:
                        exprs.extend(f"del({path_filters_pipe} | .{key})" for key in maps)

    return exprs


def match_task(name: str) -> Callable:
    def _match(task) -> bool:
        return task["name"] == name

    return _match


def match_name_value(name: str, value: str) -> Callable:
    def _match(obj) -> bool:
        return obj["name"] == name and obj["value"] == value

    return _match


def generate_dsl(differences):
    """Generate DSL"""
    # Each callable object represents the DSL operations for a specific path.
    applies: list[Callable] = []

    for path in differences:
        # NOTE: only handle this path temporarily
        path_pattern = r"^spec\.tasks\.(?P<task_name>[\w-]+)\.params$"
        if not re.match(path_pattern, path):
            continue

        fns = []
        parts = path.split(".")

        for i, part in enumerate(parts):
            if i > 0 and is_tk_list_fields(parts[i - 1]):
                fns.append(if_matches(match_task(part)))
                fns.append(nth(0))
            else:
                fns.append(with_path(part))

        for action, detail in differences[path].items():
            m = LIST_MAP_ACTIONS_RE.match(action)
            if m:
                op = m.group("operation")
                type_ = m.group("type")
                if type_ == FIELD_TYPE_LIST:
                    for detail_item in load_list_details(detail):
                        if op == OP_ADDED:
                            fns.append(append(detail_item))
                        elif op == OP_REMOVED:
                            fns.append(delete_if(match_name_value(**detail_item)))
                elif type_ == FIELD_TYPE_MAP:
                    maps = load_map_details(detail)
                    if op == OP_ADDED:
                        fns.append(append(maps))
                    elif op == OP_REMOVED:
                        for key in maps:
                            fns.append(delete_key(key))
                else:
                    raise ValueError(f"Unknown operation in: {action}")

        applies.append(apply(*fns))

    return applies


def migrate_with_dsl(migrations: list[Callable], pipeline_file: str) -> None:
    """Apply migrations to given pipeline

    :param migrations: list of migration to be applied to pipeline. Each of
        them is for a single difference path and includes all the necessary
        migration steps.
    :type migrations: list[Callable]
    :param pipeline_file: path to a pipeline to apply the migrations
    :type pipeline_file: str
    """

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 8192

    with open(pipeline_file, "r", encoding="utf-8") as f:
        origin_pipeline = yaml.load(f)

    pipeline = {"spec": origin_pipeline["spec"]["pipelineSpec"]}

    for migration in migrations:
        logger.debug("applying migration: %r", migration)
        migration(pipeline)

    with open(pipeline_file + ".modified", "w", encoding="utf-8") as f:
        yaml.dump(origin_pipeline, f)


def migrate_with_yq(
    exprs: list[str], pipeline_file: str, dry_run: bool = False
) -> None:
    if dry_run:
        exprs = "\n".join([f"yq e '{expr}' {pipeline_file}" for expr in exprs])
        logger.info("dry run:\n%s", exprs)
        return
    for expr in exprs:
        yq_cmd = f"yq e '{expr}' {pipeline_file}"
        if dry_run:
            logger.info("dry run: %s", yq_cmd)
        else:
            subprocess.run(yq_cmd, shell=True, check=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", metavar="PATH", dest="from_pipeline", required=True)
    parser.add_argument("--to", metavar="PATH", dest="to_pipeline", required=True)
    parser.add_argument(
        "--generate",
        choices=("dsl", "yq"),
        metavar="TYPE",
        dest="generate_target",
        required=True,
    )
    parser.add_argument("--modify-pipeline", metavar="PATH")
    parser.add_argument("--show-diff", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.modify_pipeline and not os.path.exists(args.modify_pipeline):
        raise ValueError(f"pipeline does not exist: {args.modify_pipeline}")

    diff = compare_pipeline_definitions(args.from_pipeline, args.to_pipeline)

    if args.show_diff:
        j = json.dumps(dict(diff), indent=2)
        logger.debug("pipeline differences:\n%s\n", j)

    match args.generate_target:
        case "yq":
            r = generate_yq_commands(diff)
            if args.modify_pipeline:
                migrate_with_yq(r, args.modify_pipeline, dry_run=args.dry_run)
        case "dsl":
            r = generate_dsl(diff)
            if args.modify_pipeline:
                migrate_with_dsl(r, args.modify_pipeline)
        case _:
            print("Unknown generate target:", args.generate_target)
            return


if __name__ == "__main__":
    main()
