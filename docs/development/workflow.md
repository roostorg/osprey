# Development Workflow

Branch from `main` and name your branch `github_username/short-description` (e.g. `caidanw/fix-database-timeout`).

## What runs on every commit

Pre-commit hooks run automatically when you commit; the config is `.pre-commit-config.yaml` at the repo root. The hooks check for filename case conflicts, leftover merge-conflict markers, and forgotten Python debugger calls; validate JSON and TOML; normalize line endings to LF; lint and format Python with Ruff; and type-check with mypy. Changes under `osprey_ui/` also go through Prettier and ESLint.

Hooks stop at the first failure, and several fix files in place—if a hook modifies a file, re-stage it and commit again.

## Checking your work before you push

CI runs the same hooks against the whole repo, so you can catch failures ahead of time with:

```bash
uv run pre-commit run --all-files
```

Or run the individual tools directly:

```bash
# Lint and format
uv run ruff check
uv run ruff format

# Type check a specific module, or everything with `uv run mypy .`
uv run mypy osprey_worker/src/osprey/worker/lib
```

## Commit messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) format. Real examples from this repo's history:

```
fix(ui): show the event-stream timezone once, not twice
build(deps): remove unused Discord-era Python dependencies
```
