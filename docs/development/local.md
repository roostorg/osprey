# Local Development

Set up a full local development environment for Osprey. To just get up and running quickly with sample data, see [Getting Started](./) instead.

## Prerequisites

- **Operating System**: macOS, Linux, or Windows (with WSL recommended)
- **[Python](https://www.python.org/) 3.11 or higher** (check with `python --version`)
- **[Git](https://git-scm.com/)** for version control
- **[uv](https://docs.astral.sh/uv/)** for Python package management
- **[Node.js](https://nodejs.org/en/download/) 22+** for the UI (Corepack ships with Node and auto-resolves pnpm from `osprey_ui/package.json`'s `packageManager` field; no separate pnpm install needed)

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

This creates a virtual environment and installs production and development dependencies (ruff, mypy, pre-commit) at the locked versions from `uv.lock`. Use `uv sync --no-dev` if you only want production dependencies.

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

Ruff reports "All checks passed!" (or the specific issues to fix), and mypy and the pre-commit hooks run without errors.

### 5. Start the Services

```bash
docker compose --profile test_data up -d
```

or using the wrapper script

```bash
./start.sh --profile test_data up -d
```

The `test_data` profile includes a producer that generates sample events; without it the stack still runs, but the UI shows no data until you send events yourself. [Getting Started](./) covers the one-command demo that wraps all of this.

This starts up many services, including:
- **Osprey Worker**: The main engine that processes input events given the rules and UDFs
  - **Test Data Producer**: The `--profile test_data` sample event generator
- **Osprey UI**: Frontend service that hosts the React code for the web interface and communicates with the UI API
- **Osprey UI API**: Backend service that provides data and functionality to the web interface
- **Kafka** (KRaft mode): Message streaming for user-generated events
- **Postgres**: A database that the Worker, UI API, and Druid use for various reasons, such as the Postgres-backed Labels Service (in the example plugins)
- **Druid**: A database that consumes Osprey Worker outputs to power the UI API for real-time querying
- **MinIO**: S3-compatible object storage; the default execution result store in this stack (`OSPREY_EXECUTION_RESULT_STORAGE_BACKEND=minio`)

Alternatively, you can start Osprey with `osprey-coordinator`; see the [Coordinator README](https://github.com/roostorg/osprey/tree/main/example_docker_compose/run_osprey_with_coordinator) for more information.

### 6. (Optional) Open ports for the UI/UI API

By default, the `docker-compose.yaml` binds running services to `127.0.0.1`. If you are running the docker compose on a headless machine, you may need to modify this configuration and/or make changes to your firewall, specifically for ports `5002` and `5004`.

For example, if you use Tailscale to access your Osprey instance, you may change `127.0.0.1:5002:5002` to `<Tailscale IP>:5002:5002`. Alternatively, if you wish for your instance to be accessible from the public internet, you may set it simply to `5002:5002` to bind to `0.0.0.0`.

Be aware that some firewalls like iptables/UFW do _not_ prevent access to ports being used by Docker networking. Not explicitly setting a bind address with only UFW as a firewall will not prevent access from the public internet unless [properly configured](https://github.com/chaifeng/ufw-docker).

### 7. Access the Application

The UI will automatically connect to the backend services running in Docker containers.

- Osprey UI: [localhost:5002](http://localhost:5002)
- Backend API: [localhost:5004](http://localhost:5004)
- Worker Service: [localhost:5001](http://localhost:5001)

## Plugins

In Osprey, UDFs and output sinks are designed to be easily portable through a plugin system based on pluggy. An example plugin package is provided for reference; see `example_plugins/src/register_plugins.py`. For a full walkthrough with examples (UDFs, sinks, hash-based lookups, ML models, labels service), see [Integrations & Plugins](../integration/integrations.md).

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

### Available hooks

Implement any subset of these in your plugin's `register_plugins.py`:

| Hook | Returns | Notes |
| --- | --- | --- |
| `register_udfs` | `Sequence[Type[UDFBase]]` | Custom user-defined functions. |
| `register_output_sinks` | `Sequence[BaseOutputSink]` | Where execution results go. |
| `register_ast_validators` | `Sequence[Type[BaseValidator]]` | Extra SML validators. |
| `register_action_proto_deserializer` | `ActionProtoDeserializer \| None` | Custom action proto → JSON. |
| `register_input_stream` | `BaseInputStream` | Single-provider (`firstresult`). |
| `register_execution_result_store` | `ExecutionResultStore` | Single-provider (`firstresult`). |
| `register_labels_service_or_provider` | `LabelsServiceBase \| LabelsProvider` | Single-provider (`firstresult`). |
| `register_validation_exporter` | `BaseValidationResultExporter \| None` | Optional; publishes experiment/bucket metadata after validation. Single-provider (`firstresult`). |
| `register_label_output_sink` | `BaseOutputSink \| None` | Optional; custom label-mutation sink, replacing the default `LabelOutputSink`. Single-provider (`firstresult`). |

## Rules

Rules are written in SML; examples with YAML config are provided in `example_rules/`. Rules are mounted into the worker processes when the containers start, controlled via environment variables; for example:

```bash
OSPREY_RULES_PATH=./example_rules uv run python osprey_worker/src/osprey/worker/cli/sinks.py run-rules-sink
```

For more about rules, see [Writing Rules](../rules/).

## Test Data

Generate sample JSON actions:
```bash
docker compose --profile test_data up osprey-kafka-test-data-producer -d
```

This produces synthetic post-creation events with timestamps, user IDs, and IP addresses to the `osprey.actions_input` topic; it's the same producer the `test_data` profile starts.
