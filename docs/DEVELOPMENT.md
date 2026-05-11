# Setup Guide

This guide provides comprehensive instructions for setting up a development environment for Osprey.

## Prerequisites

- **Operating System**: macOS, Linux, or Windows (with WSL recommended)
- **[Python](https://www.python.org/) 3.11 or higher** (check with `python --version`)
- **[Git](https://git-scm.com/)** for version control
- **[uv](https://docs.astral.sh/uv/)** for Python package management
- **[npm](https://nodejs.org/en/download)**

## Project Setup

### 1. Clone the Repository

```bash
git clone git@github.com:roostorg/osprey.git
cd osprey
```

### 2. Install Dependencies

```bash
# Install all dependencies including development tools
uv sync
```

This command will:

- Create a virtual environment automatically
- Install all production dependencies
- Install development dependencies (ruff, mypy, pre-commit) automatically
- Use the locked versions from `uv.lock` for reproducible builds

**Note**: `uv sync` includes development dependencies by default. Use `uv sync --no-dev` if you only want production dependencies.

### 3. Set Up Pre-commit Hooks

```bash
uv run pre-commit install
```

This installs git hooks that automatically run code quality checks before each commit.

### 4. Verify Setup

Run these commands to ensure everything is working correctly:

```bash
# Check linting configuration
uv run ruff check

# Check formatting
uv run ruff format --diff

# Run type checking
uv run mypy .

# Test pre-commit hooks
uv run pre-commit run --all-files
```

**Expected Results:**

- Ruff should report "All checks passed!" or show specific issues to fix
- MyPy should run without errors
- Pre-commit should run all hooks successfully

For the common development workflows below, you can also run `make help` to see
the supported shortcuts.

Compose projects used by the repo:

- `osprey`: default development stack
- `osprey-clickhouse`: ClickHouse development stack
- `osprey-test`: default Docker test stack
- `osprey-druid-test`: Druid integration Docker test stack
- `osprey-clickhouse-test`: ClickHouse Docker test stack

When you need a clean repo-scoped Docker environment, especially on a machine
with limited disk space, use:

```bash
make docker-clean
```

That removes all Osprey compose projects, their named volumes, and their Docker
images, including stale profile-specific services such as the optional Bigtable
emulator and seed-data producer containers.

### 5. Getting Started

```bash
make up
```

or directly with Docker Compose:

```bash
docker compose up -d
```

This starts up many services, including:
- **Osprey Worker**: The main engine that processes input events given the rules and UDFs
  - **Test Data Producer**: Optional with `--profile test_data`
- **Osprey UI**: Frontend service that hosts the react code for the web interface and communicates to the UI API
- **Osprey UI API**: Backend service that provides data and functionality to the web interface
- **Kafka** (KRaft mode): Message streaming for user generated events
- **Postgres**: A database that the Worker, UI API, and Druid use for various reasons, such as the Postgres-backed Labels Service (in the example plugins)
- **Druid**: A database that consumes Osprey Worker outputs to power the UI API for real-time querying
- **ClickHouse**: Optional reference backend for UI event queries. It can consume the same
  `osprey.execution_results` Kafka topic with
  `make clickhouse-up`.
  The ClickHouse overlay configures the UI API for ClickHouse, keeps Druid behind the
  optional `druid` profile, and keeps Bigtable behind the optional `bigtable` profile
  for that run. It uses separate container names and alternate host ports so it can
  run alongside the default `osprey` stack. For a measured local Docker footprint
  comparison against the default Druid stack, see
  [ClickHouse Local Footprint](development/clickhouse_local_footprint.md).
- **Bigtable**: Optional emulator profile for Bigtable-backed local development and tests.
  It is not required for the default dev stack because the repo compose files explicitly
  configure MinIO as the local execution-result store.

On Apple Silicon macOS hosts, the upstream Druid images run under `linux/amd64`
emulation in Docker Desktop. That noticeably increases startup time and CPU use
compared with native `arm64` images. The `etcd` image used in the Docker test
stacks is also `amd64`-only today.

To stop the dev stacks:

```bash
make down
make clickhouse-down
```

To start the optional Bigtable emulator on top of the default dev stack:

```bash
make bigtable-up
```

Alternatively, you can start Osprey with `osprey-coordinator`, refer to the [Coordinator README](../example_docker_compose/run_osprey_with_coordinator/README.md) for more information

### 6. (Optional) Open ports for the UI/UI API

By default, the `docker-compose.yaml` binds running services to `127.0.0.1`. If you are running the docker compose on a headless machine, you may need to modify this configuration and/or make changes to your firewall, specifically for ports `5002` and `5004`.

For example, if you use Tailscale to access your Osprey instance, you may change `127.0.0.1:5002:5002` to `<Tailscale IP>:5002:5002`. Alternatively, if you wish for your instance to be accessible from the public internet, you may set it simply to `5002:5002` to bind to `0.0.0.0`.

Be aware that some firewalls like iptables/UFW do _not_ prevent access to ports being used by Docker networking. Not explicitly setting a bind address with only UFW as a firewall will not prevent access from the public internet unless [properly configured](https://github.com/chaifeng/ufw-docker).

### 7. Access the Application

The UI will automatically connect to the backend services running in Docker containers.

- Osprey UI: [127.0.0.1:5002](http://127.0.0.1:5002)
- Backend API: [127.0.0.1:5004](http://127.0.0.1:5004)
- Worker Service: [127.0.0.1:5001](http://127.0.0.1:5001)

If you run the ClickHouse dev stack at the same time, it uses separate host ports:

- Osprey ClickHouse UI: [127.0.0.1:6002](http://127.0.0.1:6002)
- Osprey ClickHouse API: [127.0.0.1:6004](http://127.0.0.1:6004)
- Osprey ClickHouse Worker: [127.0.0.1:6001](http://127.0.0.1:6001)
- ClickHouse HTTP: [127.0.0.1:9123](http://127.0.0.1:9123)

## Plugins

In Osprey, UDFs and output sinks are designed to be easily portable. This is done through a plugin system based on pluggy. An example plugin package has been provided for reference, see `example_plugins/register_plugins.py`:

```python
@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    # Register custom user-defined functions

@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    # Define output destinations
    # By default it prints the execution results to the console

@hookimpl_osprey
def register_ast_validators() -> None:
    # Register AST validators
```

## Rules

Rules are written in SML, some examples are provided in `example_rules/` with YAML config, the rules are mounted to the worker processes when the containers start via environment variables. ex:

```bash
OSPREY_RULES=./example_rules uv run python3.11 osprey_worker/src/osprey/worker/cli/sinks.py run-rules-sink
```

[More about rules →](rules.md)

## Test Data

Generate sample JSON actions:
```bash
make seed-data
```

For the ClickHouse dev stack, use the overlay and both profiles:

```bash
make clickhouse-seed-data
```

The generator writes `create_post` actions to `osprey.actions_input`. The mounted
example rules in `example_rules/` detect posts containing `"hello"`, emit a
`BanUser` effect, and add the `meow` label from `example_rules/config/labels.yaml`.

Stop the test-data producers with:

```bash
make seed-data-down
make clickhouse-seed-data-down
```

## Prefetching Images And Builds

If you want to separate image fetch/build time from container startup time, use
the fetch targets before `make up`, `make clickhouse-up`, or the test targets:

```bash
time make fetch
time make clickhouse-fetch
time make test-fetch
time make druid-test-fetch
time make clickhouse-test-fetch
```

These targets pull remote images and build the local Dockerfiles needed by the
selected stack without starting the stack itself.

For the test stacks, the fetch targets intentionally skip optional emulator
images such as the Bigtable Cloud SDK image. Those images will still be pulled
later on demand if the actual test run needs them.

For measured cold-fetch and query-ready timing numbers comparing the default
Druid stack with the ClickHouse overlay, see
[ClickHouse Local Footprint](development/clickhouse_local_footprint.md).

## Test Stacks

Use the default Docker test stack for the fast path:

```bash
make test
```

This stack does not start Druid or ClickHouse. It is intended for the main test
suite and backend-neutral regressions.

Run the Druid integration tests separately:

```bash
make druid-test
```

This uses a separate compose project named `osprey-druid-test` and starts the
full Druid dependency chain. On Apple Silicon macOS, that includes multiple
`amd64` Druid containers running under emulation, so it is substantially slower
than `make test`.

Run the ClickHouse backend suite separately:

```bash
make clickhouse-test
```

This uses a separate compose project named `osprey-clickhouse-test`, so it can
run without interfering with the local dev stack.

Re-measured on the same Apple Silicon macOS host:

| Command | Scope | Current observation |
| --- | --- | --- |
| `make druid-test PYTEST_ARGS='osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_druid_integration.py'` | full isolated Druid integration file | one successful cold run completed in about `178s`; a later cold rerun on the same host flaked and failed after `303s` |
| `make clickhouse-test PYTEST_ARGS='osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_clickhouse_integration.py'` | full isolated ClickHouse integration file | cold run passed in `49.20s` end-to-end (`13 passed` in `5.19s` of pytest time) |

The backend suites should stay separate from `make test`. ClickHouse is
meaningfully faster and more stable in this local flow, while Druid still pays
for a heavier cluster startup and can be flaky on this Apple Silicon setup.

These isolated integration suites now cover:

- scan and event-detail hydration
- boolean query filters
- action ACL filters
- composed boolean query filter plus action ACL filter
- aggregate endpoints (`timeseries`, `groupby/approximate-count`, `topn`)
- CSV export for `topn` with composed boolean query filter plus action ACL filter

To pass custom pytest args:

```bash
make test PYTEST_ARGS='osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events.py -k scan'
make druid-test PYTEST_ARGS='osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_druid_integration.py -k aggregate'
make clickhouse-test PYTEST_ARGS='osprey_worker/src/osprey/worker/ui_api/osprey/views/tests/test_events_clickhouse_integration.py -k aggregate'
```

Tear it down when finished:

```bash
make test-down
make druid-test-down
make clickhouse-test-down
```

For a cold reset, remove the stack volumes too:

```bash
make test-reset
make druid-test-reset
make clickhouse-test-reset
```

If you want the test stack to be cleaned before and after the run, use:

```bash
make test-fresh
make druid-test-fresh
make clickhouse-test-fresh
```

These targets:

- remove existing Osprey Docker stacks and volumes before the run
- run the selected test suite
- tear down the corresponding test stack again on exit
