# CI Quality Gates Design

## Goal

Make Osprey's existing Python quality checks reproducible and add CI coverage for the native asyncio worker without duplicating the service-backed integration suite.

## Current State

- `python-quality` already runs pre-commit across the repository. Its hooks include Ruff lint, Ruff formatting, and mypy; the workflow separately runs FawltyDeps.
- `integration-tests` runs the service-backed Docker suite. Pytest's configured `testpaths` cover `osprey_worker` and `example_plugins`, but not `osprey_async_worker`.
- The async-worker tests are intentionally gevent-free and cannot run inside the existing test runner, whose entrypoint monkey-patches gevent before invoking pytest.
- The async worker imports `hash_ring` at runtime, but its package metadata does not declare that dependency.
- `main` has no branch protection or repository ruleset, so all workflow checks are currently advisory.

## Design

### Reproducible Python quality job

Keep the existing `python-quality` status and pre-commit configuration as the single source of truth for Ruff, Ruff-format, mypy, and repository hygiene. Harden the job by:

- granting only `contents: read` to the workflow token;
- checking out with `persist-credentials: false`;
- pinning uv to `0.11.10`, matching the current Discord `access` workflow and local toolchain;
- installing with `uv sync --dev --locked`;
- verifying `uv lock --check`;
- applying a 15-minute job timeout.

FawltyDeps remains in the same job. Splitting each tool into its own job is intentionally deferred because it would repeat environment setup without adding coverage.

### Async unit-test job

Add a separate `async-unit-tests` job to `code-quality.yml`. It uses the same Python and uv setup as `python-quality`, runs without gevent monkey-patching or service containers, and executes:

```bash
uv run pytest -q \
  --junitxml=/tmp/test-results/junit-async.xml \
  osprey_async_worker
```

The job always uploads the JUnit file, has a 10-minute timeout, and is triggered alongside the existing quality jobs on pull requests and pushes to `main`.

### Async dependency and test baseline

Declare `hash-ring` as a runtime dependency of `osprey-async-worker`, sourced from the public `discord/hash_ring` repository at commit `b4b56bc93053881b68b829ee9d1a4871b4aee592`, the same revision used by Discord's monorepo. Regenerate `uv.lock` and remove the now-obsolete mypy missing-import exception for `hash_ring`.

Repair the three stale coordinator input-stream tests so construction happens inside an event loop and test doubles contain the `_channel_pool` lifecycle state that production `stop()` closes. Production coordinator behavior does not change.

### Existing integration coverage

Keep `integration-tests` as the authoritative service-backed sync-worker suite. It continues to provision Kafka, Bigtable, MinIO, Postgres, Snowflake, and etcd and runs through the gevent-patched entrypoint. Introducing `unit` and `integration` pytest markers for the sync suite is a separate refactor because those tests are not currently classified cleanly.

## Discord Conventions Applied

- PR and default-branch triggers with cancellation of superseded runs.
- SHA-pinned GitHub Actions.
- Exact uv version and locked dependency installation.
- Ruff lint, Ruff formatting, type checking, lockfile validation, and pytest.
- Explicit timeouts and least-privilege workflow permissions.
- Separate fast unit and service-backed integration statuses.

Buildkite-specific Clyde commands, internal runner queues, and changed-file caching are not copied into this standalone GitHub Actions repository.

## Non-Goals

- Creating or changing branch protection/rulesets.
- Splitting every Python quality tool into a separate status.
- Reclassifying the sync-worker tests with pytest markers.
- Adding UI tests where no test files currently exist.
- Changing Rust advisory checks.

## Verification

- `uv lock --check`
- `uv sync --dev --locked`
- `uv run ruff check --no-fix .`
- `uv run ruff format --check .`
- `uv run mypy .`
- `uv tool run fawltydeps --check-unused --pyenv .venv`
- `uv run pytest -q osprey_async_worker`
- Existing Docker integration suite through `./run-tests.sh`
- GitHub Actions workflow syntax and actionlint, if available

After all new jobs are green on a pull request, merge enforcement can be added separately through a repository ruleset with explicit authorization.
