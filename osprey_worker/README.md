# osprey-worker

The main Osprey Python package. It contains:

- `src/osprey/engine/`: the rules engine: the SML parser and AST (`ast/`), validators (`ast_validator/`), the execution graph and executor (`executor/`), and the standard library of UDFs (`stdlib/`).
- `src/osprey/worker/`: the services built on the engine:
  - `sinks/` and `cli/sinks.py`: the worker, which reads events from an input stream, runs the rules on them and sends the results to output sinks (`run-rules-sink`);
  - `ui_api/`: the Flask API behind the Osprey UI;
  - `adaptor/`: the pluggy plugin system (`register_udfs`, `register_output_sinks`, and the other hooks);
  - `lib/`: shared code, including the `osprey-cli` command;
  - `stress/`: the `osprey-stress` load-testing command.

It installs the `osprey-cli` and `osprey-stress` commands. The Docker image runs the worker with `entrypoint.sh osprey-worker` and the UI API with `entrypoint.sh osprey-ui-api`.

See the [Osprey documentation](https://docs.roost.tools/osprey/latest/) for running Osprey ([Local Development](https://docs.roost.tools/osprey/latest/development/local.html)), writing rules, and writing plugins, and the [CLI Reference](https://docs.roost.tools/osprey/latest/development/cli-reference.html) for the commands.
