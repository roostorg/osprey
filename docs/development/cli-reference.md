# CLI Reference

Osprey ships three command-line tools, each installed as a console script by its package's `pyproject.toml`. Run any of them with `uv run <command> --help` from the repo root, or `<command> --help` inside an environment where the package is installed.

## osprey-cli

Installed by `osprey_worker` (`osprey.worker.lib.cli:cli`). General-purpose worker administration: pushing rules, opening a debug shell, and applying labels manually.

### push_rules

```bash
osprey-cli push_rules RULES_PATH [--dry-run/--no-dry-run] [--suppress-warnings]
```

Validates the rules at `RULES_PATH` and pushes them. Use `--dry-run` to validate without pushing. Exits non-zero if validation fails.

### compute_and_upload_dependencies_mapping

```bash
osprey-cli compute_and_upload_dependencies_mapping RULES_PATH [--suppress-warnings]
```

Computes the dependency graph for the rules at `RULES_PATH` and uploads it; this is what powers the [Rules Visualizer](../user/manage.md#rules-visualizer).

### shell

```bash
osprey-cli shell [-i / --auto-import / --no-auto-import]
```

Opens an interactive shell (IPython if installed, otherwise a fallback `code.InteractiveConsole`) with `labels`, `access_audit_log`, and `stored_execution_result` storage modules pre-imported, plus `EntityT`, `EntityLabelMutation`, and `LabelStatus`. With `--auto-import` (the default), it also imports every model class it can find under `osprey_lib`. Useful for interactively inspecting stored data.

### apply_label

```bash
osprey-cli apply_label ENTITY_TYPE ENTITY_ID LABEL_NAME LABEL_STATUS \
  [--reason REASON] [--description DESCRIPTION] [--expire-instantly]
```

Manually applies a label to a single entity. Mainly intended for debugging or importing individual labels from an external source. `--reason` defaults to `CliLabelMutationWithoutEffects`; `--description` defaults to `"Manually changed from the command line for debugging."`. `--expire-instantly` makes the label expire almost immediately rather than persisting.

This requires a labels provider to be configured for the Osprey instance; it fails with an assertion error otherwise.

### bulk_apply_label

```bash
osprey-cli bulk_apply_label ENTITY_TYPE ENTITY_IDS_FILE_PATH LABEL_NAME LABEL_STATUS \
  [--reason REASON] [--description DESCRIPTION] [--expire-instantly]
```

Same as `apply_label`, but reads entity IDs (one per line) from `ENTITY_IDS_FILE_PATH` and applies the label to all of them, printing progress as it goes. This is the CLI path for importing bulk label lists from external sources; see also the UI-driven [Bulk Actions](../user/operate.md#bulk-actions) workflow.

## osprey-stress

Installed by `osprey_worker` (`osprey.worker.stress.cli:main`). Runs an end-to-end stress test against a live Osprey worker: produces synthetic events, consumes the resulting execution results, and reports drop rate and latency. Useful for validating dependency bumps, measuring throughput regressions, and gating CI on pipeline health.

### run

```bash
osprey-stress run \
  --events 10000 --rate 1000 \
  --threshold-drop-rate 0.01 --threshold-p95-ms 500 \
  --report json
```

Produces `--events` synthetic events (default `1000`) at `--rate` events/second (default `100.0`) to the input topic, then waits up to `--drain-seconds` (default `30.0`) for the worker to finish processing before reporting.

Common flags:
- `--report {human,json}`: output format (default `human`)
- `--verbose`: emit periodic progress lines to stderr, with `--verbose-interval-seconds` (default `2.0`) controlling frequency
- `--bootstrap-servers` (default `localhost:9092`), `--input-topic` (default `osprey.actions_input`), `--output-topic` (default `osprey.execution_results`)
- `--threshold-drop-rate` and `--threshold-p95-ms`: if set, the command exits non-zero when the observed drop rate or p95 latency breaches the threshold, so it can gate a CI job

### measure

```bash
osprey-stress measure [--duration SECONDS] [--report {human,json}]
```

Reserved for open-loop measurement against externally-produced events, once the jetstream input stream plugin (#236) lands. Today it prints a stub message and exits non-zero; use `run` for closed-loop synthetic testing in the meantime.

## osprey-async-cli

Installed by `osprey_async_worker` (`osprey.async_worker.cli.main:cli`). **Experimental**: the asyncio-native worker prototype (no gevent, no monkey-patching), for validating whether an asyncio-based executor can replace the gevent one. Not intended for production use yet.

### run

```bash
osprey-async-cli run --rules-path PATH [--input-file PATH] [--max-concurrent 12] \
  [--with-plugins] [--input-source {file,kafka}] \
  [--kafka-topic TOPIC] [--kafka-bootstrap-servers SERVERS] \
  [--kafka-group-id GROUP] [--kafka-offset-reset {latest,earliest}]
```

Runs the async worker against a static rules directory (`--rules-path`, required). By default it uses stdlib UDFs only and prints results to stdout; `--with-plugins` loads the full async plugin system (UDFs, validators, and output sinks registered via the `osprey_async_plugin` entry-point group), which requires external services to be available.

Input source is controlled by `--input-source`:
- `file` (default): reads JSONL actions from `--input-file`, or runs with no input if omitted (useful for just validating the worker boots)
- `kafka`: consumes from `--kafka-topic` (default `osprey.actions_input`) via `--kafka-bootstrap-servers` (default `localhost:9092`)

### benchmark

```bash
osprey-async-cli benchmark --rules-path PATH --input-file PATH \
  [--max-concurrent 12] [--iterations 1000] [--warmup 50]
```

Benchmarks the async executor against the gevent executor (if `gevent` is importable) using the same rules and input data, running `--warmup` iterations first, then `--iterations` timed iterations, and prints a throughput/latency comparison.

> [!NOTE]
> Verify the exact `--help` output for each command against a running dev environment (e.g. via `docker compose` or `uv run <command> --help`) before relying on it. The flags documented here come directly from the current source, but are worth a final sanity check since these are actively evolving tools.
