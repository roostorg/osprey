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

### 5. Getting Started

```bash
docker compose up -d
```

or using the wrapper script

```bash
./start.sh
```

This starts up many services, including:
- **Osprey Worker**: The main engine that processes input events given the rules and UDFs
  - **Test Data Producer**: Optional with `--profile test_data`
- **Osprey UI**: Frontend service that hosts the react code for the web interface and communicates to the UI API
- **Osprey UI API**: Backend service that provides data and functionality to the web interface
- **Kafka** (KRaft mode): Message streaming for user generated events
- **Postgres**: A database that the Worker, UI API, and Druid use for various reasons, such as the Postgres-backed Labels Service (in the example plugins)
- **Druid**: A database that consumes Osprey Worker outputs to power the UI API for real-time querying

Alternatively, you can start Osprey with `osprey-coordinator`, refer to the [Coordinator README](../example_docker_compose/run_osprey_with_coordinator/README.md) for more information

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
| `register_llm_provider` | `BaseLLMProvider` | Single-provider (`firstresult`). LLM API access for AI-assisted features. |

The `register_llm_provider` hook and the vendor-neutral tool-calling helpers
(`@tool`, `ToolRegistry`, `run_tool_loop`) have their own page:
[LLM provider & tool calling](llm.md).

## Rules

Rules are written in SML, some examples are provided in `example_rules/` with YAML config, the rules are mounted to the worker processes when the containers start via environment variables. ex:

```bash
OSPREY_RULES=./example_rules uv run python3.11 osprey_worker/src/osprey/worker/cli/sinks.py run-rules-sink
```

[More about rules →](rules.md)

## Test Data

Generate sample JSON actions:
```bash
docker compose --profile test_data up osprey-kafka-test-data-producer -d
```

Produces user login events with timestamps, user IDs, and IP addresses to `osprey.actions_input` topic.

## Troubleshooting

### Druid schema not updating

If Druid is not ingesting data or is stuck on a stale schema after changing rule output structure, reset the Kafka supervisor and resubmit the ingestion spec:

```bash
curl -X POST http://localhost:8888/druid/indexer/v1/supervisor/osprey.execution_results/terminate
docker compose restart druid-spec-submitter
```

To wipe all Druid and MinIO state and start fresh (Postgres data is preserved):

```bash
docker compose down
docker volume rm osprey_middle_var osprey_historical_var osprey_broker_var osprey_coordinator_var osprey_router_var osprey_druid_shared osprey_minio_data
docker compose up -d
```

To wipe everything including Postgres:

```bash
docker compose down -v && docker compose up -d
```

### Test data not appearing in the UI

The UI defaults to querying the last 24 hours. If the selected time range is too large (weeks or months), Druid scans across many segments and results can be slow or appear empty.

- Narrow the time range to 1–4 hours centered on when you generated test data
- Click the edit icon next to the displayed time range to switch to a custom date/time picker
- Note that Druid's Kafka consumer uses `auto.offset.reset: latest` — it only picks up events produced after `docker compose up` first ran, so events from before that point will not appear regardless of the time range

### Kafka topic disk growth

Topics are created with a 48-hour / 8 GB-per-partition retention limit. If you deployed before this was set, apply the config to your existing topics:

```bash
for topic in osprey.actions_input osprey.execution_results; do
  kafka-configs --bootstrap-server localhost:9092 \
    --entity-type topics --entity-name $topic --alter \
    --add-config retention.ms=172800000,retention.bytes=8589934592,segment.bytes=1073741824
done
```
