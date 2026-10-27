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

## Test coverage

CI reports test coverage for every PR to [Codecov](https://app.codecov.io/gh/roostorg/osprey), which comments on the PR with the coverage of the lines you changed. The numbers are informational for now; nothing fails on coverage. Each suite reports under its own flag:

| Flag | Suite | CI job |
| --- | --- | --- |
| `worker` | `osprey_worker` integration tests, run in Docker by `./run-tests.sh` | Integration Tests |
| `async-worker` | `osprey_async_worker` unit tests | Code Quality → python-quality |
| `ui` | `osprey_ui` rstest tests | Code Quality → ui-quality |

To see coverage locally, run the same commands CI does:

```bash
# worker: the test container writes into /tmp/test-results, which must already exist
mkdir -p /tmp/test-results
./run-tests.sh --cov=osprey_worker --cov-report=xml:/tmp/test-results/coverage-worker.xml --cov-report=term

# async-worker: run on its own; a single pytest session over both workers fails with conftest conflicts
uv run pytest osprey_async_worker --cov=osprey_async_worker --cov-report=term --cov-report=html

# ui: writes an lcov report and an HTML report to osprey_ui/coverage/
cd osprey_ui && pnpm run test:coverage
```

Codecov's settings (flags, components, ignored paths) live in `codecov.yml` at the repo root, and coverage.py's in `[tool.coverage.run]` in `pyproject.toml`.

## Commit messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) format. Real examples from this repo's history:

```
fix(ui): show the event-stream timezone once, not twice
build(deps): remove unused Discord-era Python dependencies
```
