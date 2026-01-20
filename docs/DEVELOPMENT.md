# Development

Welcome to the development guide for Osprey. This document will help you get started with contributing to the project.

## Reporting a Bug or Issue

Found a bug or have a feature request? We'd love to hear from you! When opening an issue, please use our templates:

* [Bug Report](https://github.com/roostorg/osprey/issues/new?template=bug_report.md)
* [Feature Request](https://github.com/roostorg/osprey/issues/new?template=feature_request.md)
* [Submit an Egg (new tool idea) to ROOST!](https://github.com/roostorg/osprey/issues/new?template=documentation.md)

# Setup Guide

This guide provides comprehensive instructions for setting up a development environment for Osprey.

## Prerequisites

### System Requirements

- **Python 3.11 or higher** - Check with `python --version`
- **Git** - Version control system
- **[uv](https://docs.astral.sh/uv/)** (Python package manager) or python package manager of choice
- **Operating System**: macOS, Linux, or Windows (with WSL recommended)

#### Install UV

**macOS/Linux:**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**

```bash
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Alternative (via pip):**

```bash
pip install uv
```

**Verify installation:**

```bash
uv --version
```

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

alternatively, you can start Osprey with `osprey-coordinator`, refer to the [Coordinator README](./example_docker_compose/run_osprey_with_coordinator/README.md) for more information

### 6. (Optional) Port Forward the UI/UI API

If you are running the docker compose on a headless machine, you will need to port forward the UI and UI API.
Namely, ports `5002` (UI) and `5004` (UI API). Then, you can connect via http://localhost:5002/ :D

### 7. Access the Application

The UI will automatically connect to the backend services running in Docker containers.

- Osprey UI: http://localhost:5002
- Backend API: http://localhost:5004
- Worker Service: http://localhost:5001

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

## Test Data

Generate sample JSON actions:
```bash
docker compose --profile test_data up kafka_test_data_producer -d
```

Produces user login events with timestamps, user IDs, and IP addresses to `osprey.actions_input` topic.

# Development Workflow

## Branch Management

- **Branch naming convention**: Use `github_username/description` format (e.g., `caidanw/feature-auth`, `caidanw/fix-database-timeout`)
- **Base branch**: Always branch from `main`
- **Create new branch**: `git checkout -b username/feature-name`

## Code Quality Standards

### Automated Checks

Every commit automatically runs:

1. **Trailing whitespace removal**
2. **End-of-file fixing**
3. **YAML/JSON/TOML validation**
4. **Ruff linting and formatting**

### Manual Checks

Before pushing, run:

```bash
# Comprehensive linting check
uv run ruff check

# Format all code
uv run ruff format

# Type checking (on specific files/modules)
uv run mypy osprey_worker/src/osprey_worker/lib
# Or you can type check every module (this will happen in CI)
uv run mypy .

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

## Commit Standards

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
feat: add user authentication system
fix: resolve database connection timeout
docs: update API documentation
refactor: simplify rule evaluation logic
```

**Examples:**

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `chore:` - Maintenance tasks

## Making Changes

1. **Create a new branch:**

   ```bash
   git checkout -b username/feature-name
   ```

2. **Make your changes**

3. **Run quality checks:**

   ```bash
   uv run ruff check --fix
   uv run ruff format
   ```

4. **Test your changes** (if tests exist)

5. **Commit your changes:**

   ```bash
   git add .
   git commit -m "feat: descriptive commit message"
   ```

   Pre-commit hooks will run automatically and may fix formatting issues.

6. **Push your branch:**

   ```bash
   git push origin username/feature-name
   ```

## Development Tools Overview

### Ruff - Linting and Formatting

**Purpose**: Replaces Black, isort, Flake8, and other tools

**Configuration**: Located in `pyproject.toml` under `[tool.ruff]`

**Key Rules Enabled**:

- `E` - pycodestyle errors
- `F` - pyflakes
- `I` - isort (import sorting)
- `B006` - flake8-bugbear (mutable default arguments)

**Commands**:

```bash
# Check for issues
uv run ruff check

# Fix auto-fixable issues
uv run ruff check --fix

# Format code
uv run ruff format

# Check specific files
uv run ruff check path/to/file.py
```

### MyPy - Type Checking

**Purpose**: Static type checking for Python

**Configuration**: Located in `pyproject.toml` under `[tool.mypy]`

**Key Features**:

- Pydantic plugin support
- SQLAlchemy plugin support
- Relaxed strict mode (matching legacy codebase)
- Ignores protobuf generated files

**Commands**:

```bash
# Type check entire project
uv run mypy .

# Type check specific files
uv run mypy path/to/file.py

# Type check entire module
uv run mypy osprey_worker/

# Check with verbose output
uv run mypy --show-traceback path/to/file.py
```

### Pre-commit - Git Hooks

**Purpose**: Automated quality checks before commits

**Configuration**: Located in `.pre-commit-config.yaml`

**Commands**:

```bash
# Run all hooks on staged files
uv run pre-commit run

# Run all hooks on all files
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run ruff

# Update hook versions
uv run pre-commit autoupgrade

# Bypass hooks (emergency only)
git commit --no-verify
```

### UV - Package Management

**Purpose**: Fast Python package manager and environment management

**Key Commands**:

```bash
# Install dependencies
uv sync

# Add new dependency
uv add package-name

# Add development dependency
uv add --group dev package-name

# Remove dependency
uv remove package-name

# Run command in environment
uv run command-name

# Update dependencies
uv lock --upgrade
```

## Troubleshooting

### Common Issues

#### "uv: command not found"

**Solution**: Install uv using the installation script or pip:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# Then restart your terminal
```

#### Pre-commit hooks failing

**Solution**: Run hooks manually to see detailed errors:

```bash
uv run pre-commit run --all-files
```

#### MyPy errors on protobuf files

**Solution**: Protobuf generated files are excluded in configuration. If you see errors, check that files match the exclusion patterns in `pyproject.toml`.

#### Import errors during type checking

**Solution**: Ensure all dependencies are installed:

```bash
uv sync
```

### Getting Help

1. **Check this documentation** for common setup issues
2. **Review error messages** carefully - they often contain solutions
3. **Run commands with verbose flags** for more detailed output
4. **Check configuration files** (`pyproject.toml`, `.pre-commit-config.yaml`)

## Next Steps

- Check the [contributing guidelines](https://github.com/roostorg/.github/blob/main/CONTRIBUTING.md) for project-specific rules
- Explore the codebase structure in `osprey_worker/`, `osprey_common/`, and `osprey_rpc/`

## IDE Setup Recommendations

### VS Code

Install these extensions for the best development experience:

- **Python** (Microsoft)
- **Ruff** (Astral Software)
- **MyPy Type Checker** (Microsoft)

**Settings** (add to `.vscode/settings.json`):

```json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "ruff.enable": true,
  "ruff.organizeImports": true,
  "python.analysis.typeCheckingMode": "basic"
}
```

This completes the development setup! You're now ready to contribute to Osprey.
