# Development Tools Overview

## Ruff - Linting and Formatting

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

## MyPy - Type Checking

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

## Pre-commit - Git Hooks

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

## UV - Package Management

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
