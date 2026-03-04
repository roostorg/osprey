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

Alternatively, you can start Osprey with `osprey-coordinator`, refer to the [Coordinator README](./example_docker_compose/run_osprey_with_coordinator/README.md) for more information

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

## Rules

Rules are written in SML, some examples are provided in `example_rules/` with YAML config, the rules are mounted to the worker processes when the containers start via environment variables. ex:

```bash
OSPREY_RULES=./example_rules uv run python3.11 osprey_worker/src/osprey/worker/cli/sinks.py run-rules-sink
```

[More about rules →](rules.md)

## Release Process

Osprey uses [Semantic Versioning](https://semver.org/) (SemVer) with a 1.x.y series. This is a lightweight, bootstrap release process so downstream users can depend on version tags instead of commit hashes. The process may evolve as project usage grows.

### Patch releases (1.x.y)

Patch releases are backward-compatible fixes or small improvements already merged to `main`.

Cut a patch release when:

- Downstream users need a stable version tag, or
- Meaningful fixes have accumulated and CI is green

There is no fixed cadence; releases are event-driven.

### Patch release checklist

Before cutting a release:

- [ ] Code quality CI passing on `main` (see [Code Quality Checks](https://github.com/roostorg/osprey/actions))
- [ ] No breaking changes
- [ ] [CHANGELOG.md](../CHANGELOG.md) updated (if applicable)

### How to cut a release

1. Ensure the checklist above is satisfied.
2. In GitHub: **Releases** → **Draft a new release**.
3. Choose or create a tag `X.Y.Z` (e.g. `1.0.1`) from `main`.
4. Publish the release.

Publishing the release triggers existing automation:

- **osprey-rpc**: build and attach sdist (and zip) to the release ([release-osprey-rpc](https://github.com/roostorg/osprey/blob/main/.github/workflows/release-osprey-rpc.yml)).
- **Osprey Coordinator**: build and push Docker image to GHCR with version tags ([publish-coordinator-image](https://github.com/roostorg/osprey/blob/main/.github/workflows/publish-coordinator-image.yml)).

Downstreams can depend on version tags (e.g. `1.0.1`) instead of commit SHAs.

## Test Data

Generate sample JSON actions:
```bash
docker compose --profile test_data up osprey-kafka-test-data-producer -d
```

Produces user login events with timestamps, user IDs, and IP addresses to `osprey.actions_input` topic.
