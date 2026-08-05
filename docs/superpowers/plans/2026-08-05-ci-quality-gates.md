# CI Quality Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** Make Osprey's Python quality checks reproducible and add a required-quality candidate job that runs the native asyncio worker's unit tests.

**Architecture:** Keep the existing pre-commit-driven Python quality job and Docker integration workflow intact, while hardening dependency installation and adding a separate gevent-free async test job. First establish a correctly declared async-worker runtime dependency and a green local async test baseline; only then make that command a CI status.

**Tech Stack:** GitHub Actions, uv, pytest/pytest-asyncio, Ruff, mypy, FawltyDeps, Python 3.11.

## Global Constraints

- Do not change production coordinator behavior.
- Do not change the existing Docker integration test entrypoint or service matrix.
- Do not create or modify repository rulesets or branch protection.
- Keep `python-quality` as the single CI status for Ruff, Ruff-format, mypy, and FawltyDeps.
- Preserve SHA pins for third-party GitHub Actions.

### Task 1: Declare the async worker's hash-ring dependency

**Files:**

- Modify: `osprey_async_worker/pyproject.toml`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Step 1: Confirm the missing dependency fails from the locked environment**

Run:

```bash
uv sync --dev --locked
uv run pytest -q osprey_async_worker
```

Expected: collection fails because `hash_ring` is imported by the async discovery directory but is not declared or installed.

**Step 2: Add the smallest dependency declaration**

- Add `hash-ring` to `osprey-async-worker` runtime dependencies.
- Add a root uv source for `https://github.com/discord/hash_ring.git` pinned to revision `b4b56bc93053881b68b829ee9d1a4871b4aee592`.
- Remove the obsolete `hash_ring` missing-import exception from the root mypy configuration.

**Step 3: Regenerate and install the lockfile**

Run:

```bash
uv lock
uv sync --dev --locked
uv lock --check
uv run python -c 'from hash_ring import HashRing, HashRingNode'
```

Expected: all commands exit 0 and the dependency resolves from the pinned revision.

**Step 4: Expose the remaining test failures**

Run:

```bash
uv run pytest -q osprey_async_worker
```

Expected: collection succeeds and exactly the three known stale coordinator input-stream tests fail.

**Step 5: Commit**

```bash
git add osprey_async_worker/pyproject.toml pyproject.toml uv.lock
git commit -m "fix(async-worker): declare hash ring dependency"
```

### Task 2: Repair the async coordinator lifecycle tests

**Files:**

- Modify: `osprey_async_worker/src/osprey/async_worker/tests/test_coordinator_input_stream.py`

**Step 1: Make channel construction and cleanup event-loop safe**

- Convert `test_discovery_pool_creates_channels` to an asyncio pytest test.
- Construct the pool inside the running event loop.
- Close the pool in `finally` so its gRPC channels do not leak.

**Step 2: Model the production stop invariant in test doubles**

- Give both `__new__`-constructed streams a mocked `_channel_pool` with an `AsyncMock.close` method.
- Assert `stop()` awaits `close()` once, as well as preserving the existing shutdown-event assertions.

**Step 3: Run focused tests**

Run:

```bash
uv run pytest -q \
  osprey_async_worker/src/osprey/async_worker/tests/test_coordinator_input_stream.py
```

Expected: all coordinator input-stream tests pass.

**Step 4: Run the complete async suite**

Run:

```bash
uv run pytest -q osprey_async_worker
```

Expected: 111 tests pass with no failures.

**Step 5: Commit**

```bash
git add osprey_async_worker/src/osprey/async_worker/tests/test_coordinator_input_stream.py
git commit -m "test(async-worker): repair coordinator lifecycle coverage"
```

### Task 3: Harden Python CI and add async unit coverage

**Files:**

- Modify: `.github/workflows/code-quality.yml`

**Step 1: Harden the workflow and existing Python job**

- Add workflow-level `permissions: contents: read`.
- Set `persist-credentials: false` on checkouts.
- Pin setup-uv's configured uv version to `0.11.10`.
- Add a 15-minute timeout to `python-quality`.
- Change installation to `uv sync --dev --locked`.
- Add `uv lock --check` before pre-commit.

**Step 2: Add the gevent-free async unit job**

Add `async-unit-tests` with:

- Ubuntu 24.04, Python 3.11, and the same SHA-pinned checkout/setup actions.
- A 10-minute timeout.
- `uv sync --dev --locked`.
- Direct pytest execution against `osprey_async_worker`, writing JUnit XML under `/tmp/test-results`.
- An `if: always()` upload step using the repository's existing pinned upload-artifact action.

**Step 3: Validate workflow structure**

Run actionlint when available. Also parse the YAML and inspect the diff to confirm the existing UI and Rust jobs retain their commands and trigger behavior.

**Step 4: Re-run the commands represented by the workflow**

Run:

```bash
uv lock --check
uv sync --dev --locked
uv run pre-commit run --all-files
uv tool run fawltydeps --check-unused --pyenv .venv
uv run pytest -q --junitxml=/tmp/test-results/junit-async.xml osprey_async_worker
```

Expected: all commands exit 0 and the JUnit file exists.

**Step 5: Commit**

```bash
git add .github/workflows/code-quality.yml
git commit -m "ci: add async worker unit test gate"
```

### Task 4: Verify the branch, review independently, and open the PR

**Files:**

- Review: all changes from `origin/main...HEAD`

**Step 1: Run full local verification**

Run the complete Python quality commands, async suite, and existing Docker integration suite from the design document. Validate workflow syntax with actionlint if available.

**Step 2: Request an independent subagent review**

Have a separate subagent inspect the complete diff for correctness, dependency integrity, CI behavior, test quality, and unintended scope. Address confirmed findings and re-run affected verification.

**Step 3: Inspect final repository state**

Run:

```bash
git status --short
git diff --check origin/main...HEAD
git log --oneline origin/main..HEAD
```

Expected: clean worktree, no whitespace errors, and only the planned commits.

**Step 4: Push and create the PR**

Push `codex/ci-unit-quality-gates`, open a PR against `main`, and report the local verification evidence plus any checks that must finish remotely.
