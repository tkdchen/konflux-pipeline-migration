# konflux-pipeline-migration

A POC of generating pipeline updates migrations.

The migrations can be in a series of yq commands or in a programmatic way by
applying sets of build-migrations DSL functions to the pipeline definitions.

It works like the following pipes:

```
Generate differences | generate migrations | apply to pipeline definition
                       |
                       |-- yq commands
                       |
                       +-- DSL calls
```

yq commands generatation:

```bash
python3 migrate.py --from pipeline --to pipeline --generate yq --dry-run
```

To apply migration steps:

```bash
python3 migrate.py --from pipeline --to pipeline --generate dsl --modify-pipeline pipelinerun-file
```

Note that, not all cases of pipeline updates are covered.

