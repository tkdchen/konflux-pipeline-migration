import argparse
import io
import json
import logging
import os
import re
import subprocess

from typing import Callable, Final
from ruamel.yaml import YAML
from fn import append, apply, delete_if, with_path, if_matches, nth

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(asctime)s:%(message)s")
logger = logging.getLogger("migration")


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
        "dyff", "between", "--omit-header", "--no-table-style",
        "--detect-kubernetes", "--set-exit-code",
        from_, to
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
    exprs = []  # yq expressions

    path_pattern = r"^spec\.tasks\.(?P<task_name>[\w-]+)\.params$"

    for path in differences:
        if not re.match(path_pattern, path):
            continue

        filters: list[str] = []
        parts = path.split(".")

        for i, part in enumerate(parts):
            if i > 0 and is_tk_list_fields(parts[i-1]):
                filters[-1] += "[]"
                filters.append(f'select(.name == "{part}")')
            else:
                filters.append("." + part)

        for action in differences[path]:
            detail = differences[path][action]
            if (m := LIST_MAP_ACTIONS_RE.match(action)) is not None:
                for detail_item in load_list_details(detail):
                    op = m.group("operation")
                    if op == "added":
                        add_this = json.dumps(param, separators=(", ", ": "))
                        filters_pipe = " | ".join(filters)
                        exprs.append(f"({filters_pipe}) += {add_this}")
                    elif op == "removed":
                        if is_tk_list_fields(filters[-1][1:]):
                            filters[-1] += "[]"
                        name = detail_item["name"]
                        value = detail_item["value"]
                        filters.append(f'select(.name == "{name}" and .value == "{value}")')
                        filters_pipe = " | ".join(filters)
                        exprs.append(f"del({filters_pipe})")

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
    """Generate DSL

    spec.tasks.deprecated-base-image-check.params
      - two list entries removed:
        - name: BASE_IMAGES_DIGESTS
          value: $(tasks.build-container.results.BASE_IMAGES_DIGESTS)
        - name: IMAGE_URL
          value: $(tasks.build-container.results.IMAGE_URL)

      + one list entry added:
        - name: IMAGE_NAME
          value: $(tasks.build-container.results.IMAGE_URL)
    """
    # Each callable object represents the DSL operations for a specific path.
    applies: list[Callable] = []
    # action_re: Final = r". (?P<count>[a-z]+) (?P<type>list|map) (entry|entries) (?P<operation>added|removed):$"

    for path in differences:
        # NOTE: only handle this path temporarily
        path_pattern = r"^spec\.tasks\.(?P<task_name>[\w-]+)\.params$"
        if not re.match(path_pattern, path):
            continue

        fns = []
        add_fn = fns.append
        parts = path.split(".")
        for i, part in enumerate(parts):
            if i > 0 and parts[i-1] in ("tasks", "workspaces"):
                fns.append(if_matches(match_task(part)))
                fns.append(nth(0))
            else:
                fns.append(with_path(part))
        for action in differences[path]:
            detail = differences[path][action]
            # map is not handled yet
            if (m := LIST_MAP_ACTIONS_RE.match(action)) is not None:
                for detail_item in load_list_details(detail):
                    op = m.group("operation")
                    if op == "added":
                        fns.append(append(detail_item))
                    elif op == "removed":
                        fns.append(delete_if(match_name_value(detail_item["name"], detail_item["value"])))
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

    with open(pipeline_file, 'r', encoding='utf-8') as f:
        origin_pipeline = yaml.load(f)

    pipeline = {"spec": origin_pipeline["spec"]["pipelineSpec"]}

    for migration in migrations:
        logger.debug("applying migration: %r", migration)
        migration(pipeline)

    with open(pipeline_file + ".modified", 'w', encoding='utf-8') as f:
        yaml.dump(origin_pipeline, f)


def migrate_with_yq(exprs: list[str], pipeline_file: str, dry_run: bool = False) -> None:
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
    parser.add_argument("--generate", choices=("dsl", "yq"), metavar="TYPE", dest="generate_target", required=True)
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
