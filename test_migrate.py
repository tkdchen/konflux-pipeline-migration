
import os
import sys

import fn
import migrate
from migrate import main, generate_dsl
from pathlib import Path
from typing import Final


from_pipeline: Final = """\
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  creationTimestamp: null
  labels:
    pipelines.openshift.io/runtime: generic
    pipelines.openshift.io/strategy: docker
    pipelines.openshift.io/used-by: build-cloud
  name: docker-build
spec:
  tasks:
  - name: step-0
"""

to_pipeline: Final = """\
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  creationTimestamp: null
  labels:
    pipelines.openshift.io/runtime: generic
    pipelines.openshift.io/strategy: docker
    pipelines.openshift.io/used-by: build-cloud
  name: docker-build
spec:
  tasks:
  - name: step-0
    script: echo hello adventure
  - name: init
    params:
    - name: image-url
      value: $(params.output-image)
    - name: rebuild
      value: $(params.rebuild)
    - name: skip-checks
      value: $(params.skip-checks)
    taskRef:
      params:
      - name: name
        value: init
      - name: bundle
        value: quay.io/myorg/init:0.1@sha256:61f1202
      - name: kind
        value: task
      resolver: bundles
"""

pipeline_run: Final = """\
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  annotations:
    revision: {{revision}}
  labels:
    hello: world
  name: pr-build
  namespace: my
spec:
  params: []
  pipelineSpec:
    tasks:
    - name: step-0
"""


def write_sample_from_pipeline(tmpdir, spec_content: str = "") -> str:
    filename = os.path.join(tmpdir, "from_pipeline.yaml")
    with open(filename, "w") as f:
        f.write(from_pipeline)
        if spec_content:
            f.write(spec_content)
    return filename


def write_sample_to_pipeline(tmpdir, spec_content: str = "") -> str:
    filename = os.path.join(tmpdir, "to_pipeline.yaml")
    with open(filename, "w") as f:
        f.write(to_pipeline)
        if spec_content:
            f.write(spec_content)
    return filename


def write_sample_pipeline_run(tmpdir) -> str:
    filename = os.path.join(tmpdir, "pipelinerun.yaml")
    with open(filename, "w") as f:
        f.write(pipeline_run)
    return filename


def test_migrate_with_dsl(tmpdir, monkeypatch):
    from_pipeline_file = write_sample_from_pipeline(tmpdir)
    to_pipeline_file = write_sample_to_pipeline(tmpdir)
    pipeline_run_file = write_sample_pipeline_run(tmpdir)

    cmd = [
        "migrate", "--show-diff",
        "--from", from_pipeline_file,
        "--to", to_pipeline_file,
        "--generate", "dsl",
        "--modify-pipeline", pipeline_run_file,
    ]
    monkeypatch.setattr(sys, "argv", cmd)
    main()

    with open(pipeline_run_file + ".modified", "r", encoding="utf-8") as f:
        modified_plr = migrate.create_yaml_obj().load(f)

    task_step_0 = fn.apply(
        fn.with_path("spec", "pipelineSpec", "tasks"),
        fn.if_matches(migrate.match_task("step-0")),
        fn.nth(0),
    )(modified_plr)

    to_pl = migrate.create_yaml_obj().load(to_pipeline)
    base_task_step_0 = fn.apply(
        fn.with_path("spec", "tasks"),
        fn.if_matches(migrate.match_task("step-0")),
        fn.nth(0),
    )(to_pl)

    assert task_step_0 == base_task_step_0

    task_init = fn.apply(
        fn.with_path("spec", "pipelineSpec", "tasks"),
        fn.if_matches(migrate.match_task("init")),
        fn.nth(0),
    )(modified_plr)

    base_task_init = fn.apply(
        fn.with_path("spec", "tasks"),
        fn.if_matches(migrate.match_task("init")),
        fn.nth(0),
    )(to_pl)

    assert task_init == base_task_init


# def test_generate_dsl():
#     generate_dsl()
