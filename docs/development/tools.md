# Development Tools

Familiarize yourself with these development tools to better understand how to work on and debug Osprey.

## Ruff

Linting and formatting. Replaces Black, isort, Flake8, and other tools. Configuration lives in `pyproject.toml` under `[tool.ruff]`; the enabled rules are pycodestyle errors (`E`), pyflakes (`F`), import sorting (`I`), and flake8-bugbear's mutable-default-arguments check (`B006`).

Commands:

```sh
# Check for issues
uv run ruff check

# Fix auto-fixable issues
uv run ruff check --fix

# Format code
uv run ruff format

# Check specific files
uv run ruff check path/to/file.py
```

## MyPy

Static type checking for Python. Configuration in `pyproject.toml` under `[tool.mypy]` loads the Pydantic and SQLAlchemy plugins, relaxes strict mode to match the older parts of the codebase, and skips generated protobuf files.

Commands:

```sh
# Type check entire project
uv run mypy .

# Type check specific files
uv run mypy path/to/file.py

# Type check entire module
uv run mypy osprey_worker/

# Check with verbose output
uv run mypy --show-traceback path/to/file.py
```

## Pre-commit

Git hooks for automated quality checks before committing code. Configuration in `.pre-commit-config.yaml`.

Commands:

```sh
# Run all hooks on staged files
uv run pre-commit run

# Run all hooks on all files
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run ruff

# Update hook versions
uv run pre-commit autoupdate

# Bypass hooks (emergency only)
git commit --no-verify
```

## UV

Python package and environment management. Key commands:

```sh
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
