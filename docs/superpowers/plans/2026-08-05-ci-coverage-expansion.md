# CI Coverage Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand advisory CI coverage across documentation, Rust, UI, and both Python runtimes while publishing trustworthy report-only coverage artifacts.

**Architecture:** Keep each existing language job as the owner of its checks. Convert mdBook to a validation-only workflow, make Rust tests affect their existing job while preserving Clippy as visible baseline debt, add the existing UI production build, and configure separate gevent-aware and gevent-free coverage collection for the sync and async Python suites.

**Tech Stack:** GitHub Actions, mdBook 0.5.2, Rust/Cargo, npm/React Scripts, uv, pytest, pytest-cov, coverage.py, Docker Compose.

## Global Constraints

- Do not create or modify branch protection, repository rulesets, or required status checks.
- Do not deploy documentation or change the repository homepage/edit links.
- Do not introduce a coverage threshold or external coverage service.
- Do not add UI tests or ESLint enforcement in this PR.
- Keep sync coverage gevent-aware and async coverage gevent-free.
- Preserve all production Osprey, Smite, coordinator, and UI behavior.

---

### Task 1: Convert mdBook deployment into documentation validation

**Files:**

- Modify: `.github/workflows/mdbook.yml`
- Modify: `docs/docs.md:1-5`

**Interfaces:**

- Consumes: `book.toml`, `docs/SUMMARY.md`, and mdBook `0.5.2`.
- Produces: an advisory `Documentation Check / build-docs` status on pull requests and pushes to `main`.

- [ ] **Step 1: Record the current failing deployment baseline**

Inspect the latest `main` workflow failure and confirm it stops in `actions/configure-pages` because Pages is not enabled:

```bash
RUN_ID=$(gh run list --workflow mdbook.yml --branch main --limit 1 --json databaseId --jq '.[0].databaseId')
gh run view "$RUN_ID" --log-failed
```

Expected: `Get Pages site failed` / `Not Found` before `mdbook build` runs.

- [ ] **Step 2: Replace the workflow with a validation-only job**

Set `.github/workflows/mdbook.yml` to the following shape, retaining the exact action SHAs already used elsewhere in the repository:

```yaml
name: Documentation Check

on:
  pull_request:
    branches: [main]
    types: [opened, synchronize, reopened, ready_for_review]
  push:
    branches: [main]

permissions:
  contents: read

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  build-docs:
    runs-on: ubuntu-24.04
    timeout-minutes: 15
    env:
      MDBOOK_VERSION: 0.5.2
    steps:
      - name: Checkout code
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd  # v6.0.2
        with:
          persist-credentials: false

      - name: Set up Rust
        uses: dtolnay/rust-toolchain@3c5f7ea28cd621ae0bf5283f0e981fb97b8a7af9  # master
        with:
          toolchain: stable

      - name: Install mdBook
        run: cargo install --locked --version "$MDBOOK_VERSION" mdbook

      - name: Build documentation
        run: mdbook build
```

- [ ] **Step 3: Correct the contributor documentation**

Replace its opening deployment paragraph with:

```markdown
This documentation is built using [mdBook](https://rust-lang.github.io/mdBook/). Pull requests and changes merged into `main` are automatically compiled to catch broken documentation, but this Discord repository does not deploy a separate GitHub Pages site. The canonical published documentation remains the [upstream ROOST Osprey site](https://roostorg.github.io/osprey/).
```

- [ ] **Step 4: Validate workflow syntax and the book**

Run:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/mdbook.yml
cargo install --locked --version 0.5.2 mdbook
mdbook build
test -s book/index.html
```

Expected: actionlint exits 0 and `book/index.html` is non-empty.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/mdbook.yml docs/docs.md
git commit -m "ci: validate mdBook documentation"
```

### Task 2: Expand Rust and UI job coverage

**Files:**

- Modify: `.github/workflows/code-quality.yml:98-167`

**Interfaces:**

- Consumes: existing `ui-quality` and `rust-quality` jobs and package scripts.
- Produces: UI production-build validation, blocking Rust tests within the advisory job, and a Clippy run that reaches the known lint baseline.

- [ ] **Step 1: Confirm the proposed commands are green before changing CI**

Run:

```bash
cd osprey_ui
npm ci --ignore-scripts
npm run format:check
npm run build
cd ../osprey_coordinator
cargo fmt --check
cargo clippy -- -D warnings
cargo test --verbose
```

Expected: UI formatting/build, Rust formatting, and all 52 Rust tests exit 0. Clippy reaches linting and reports the known 229-error baseline instead of failing early on missing `protoc`.

- [ ] **Step 2: Add UI production-build validation**

In `ui-quality`:

- add `timeout-minutes: 15`;
- add a step after Prettier named `Build UI` that runs `npm run build` in `osprey_ui`.

- [ ] **Step 3: Make Rust checks affect their job safely**

In `rust-quality`:

- add an `Install protobuf compiler` step before Clippy;
- move the existing apt update/install commands out of `Build Rust project`, leaving only `cargo build --verbose` there;
- keep `continue-on-error: true` on Clippy while its baseline is handled separately;
- remove `continue-on-error: true` from Rust tests.

- [ ] **Step 4: Validate the workflow**

Run:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/code-quality.yml
git diff --check
```

Expected: both commands exit 0.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/code-quality.yml
git commit -m "ci: enforce Rust checks and build UI"
```

### Task 3: Add runtime-specific Python coverage and async reporting

**Files:**

- Create: `osprey_worker/.coveragerc-sync`
- Create: `osprey_async_worker/.coveragerc-async`
- Modify: `pyproject.toml:104-121,180-218`
- Modify: `uv.lock`
- Modify: `.github/workflows/code-quality.yml:61-96`

**Interfaces:**

- Consumes: pytest-cov and the existing native asyncio pytest process.
- Produces: gevent-free branch coverage XML at `/tmp/test-results/coverage-async.xml` and validated JUnit XML.

- [ ] **Step 1: Verify coverage arguments fail before the dependency exists**

Run:

```bash
uv sync --dev --locked
uv run pytest -q \
  osprey_async_worker/src/osprey/async_worker/tests/test_no_gevent_imports.py \
  --cov=osprey.async_worker
```

Expected: pytest exits non-zero with `unrecognized arguments: --cov=osprey.async_worker`.

- [ ] **Step 2: Declare and lock pytest-cov**

Add `pytest-cov` to the root `dev` dependency group. Add `pytest-cov` beside the other pytest plugins in `[tool.fawltydeps].ignore_unused`, with the existing plugin/CLI comment. Then run:

```bash
uv lock
uv sync --dev --locked
uv lock --check
```

Expected: the lock contains pytest-cov and coverage.py, and all commands exit 0.

- [ ] **Step 3: Create separate coverage configurations**

Create `osprey_worker/.coveragerc-sync`:

```ini
[run]
branch = True
concurrency =
    gevent
    thread

[report]
skip_empty = True
omit =
    */tests/*
    */test/*
    */conftest.py
    */__test__.py
    *_pb2*.py
```

Create `osprey_async_worker/.coveragerc-async` with the same report omissions and:

```ini
[run]
branch = True

[report]
skip_empty = True
omit =
    */tests/*
    */test/*
    */conftest.py
    */__test__.py
    *_pb2*.py
```

- [ ] **Step 4: Prove async coverage stays gevent-free**

Run:

```bash
mkdir -p /tmp/test-results
uv run pytest -q \
  osprey_async_worker/src/osprey/async_worker/tests/test_no_gevent_imports.py \
  --cov-config=osprey_async_worker/.coveragerc-async \
  --cov=osprey.async_worker \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:/tmp/test-results/coverage-async.xml
test -s /tmp/test-results/coverage-async.xml
uv run python -c 'from xml.etree import ElementTree; ElementTree.parse("/tmp/test-results/coverage-async.xml")'
```

Expected: the no-gevent test passes and coverage XML parses.

- [ ] **Step 5: Update the async CI job**

Extend its pytest command with the async configuration, branch coverage, `term-missing`, and XML report arguments. After pytest, validate both XML files with `test -s` and `xml.etree.ElementTree.parse`. Upload both files and set `if-no-files-found: error`.

- [ ] **Step 6: Run the complete async suite with CI-equivalent outputs**

Run:

```bash
uv run pytest -q \
  --junitxml=/tmp/test-results/junit-async.xml \
  --cov-config=osprey_async_worker/.coveragerc-async \
  --cov=osprey.async_worker \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:/tmp/test-results/coverage-async.xml \
  osprey_async_worker
test -s /tmp/test-results/junit-async.xml
test -s /tmp/test-results/coverage-async.xml
uv run python -c 'from xml.etree import ElementTree; ElementTree.parse("/tmp/test-results/junit-async.xml"); ElementTree.parse("/tmp/test-results/coverage-async.xml")'
```

Expected: all 111 async tests pass, both XML files parse, and coverage XML reports branch data for `osprey.async_worker`.

- [ ] **Step 7: Verify dependency hygiene and commit**

Run:

```bash
uv tool run fawltydeps --check-unused --pyenv .venv
git diff --check
```

Then commit:

```bash
git add pyproject.toml uv.lock osprey_worker/.coveragerc-sync osprey_async_worker/.coveragerc-async .github/workflows/code-quality.yml
git commit -m "ci: publish async Python coverage"
```

### Task 4: Publish gevent-aware sync coverage

**Files:**

- Modify: `.github/workflows/integration-tests.yml:34-43`

**Interfaces:**

- Consumes: `osprey_worker/.coveragerc-sync` through the existing `/osprey/osprey_worker` bind mount and pytest-cov installed in the locked image.
- Produces: branch coverage XML at `/tmp/test-results/coverage-sync.xml` alongside sync JUnit XML.

- [ ] **Step 1: Extend the Docker test invocation**

Pass these arguments through `./run-tests.sh`:

```text
--junitxml=/tmp/test-results/junit-pytest.xml
--cov-config=osprey_worker/.coveragerc-sync
--cov=osprey.engine
--cov=osprey.worker
--cov=example_plugins/src
--cov-branch
--cov-report=term-missing
--cov-report=xml:/tmp/test-results/coverage-sync.xml
```

- [ ] **Step 2: Validate and upload both sync artifacts**

After the Docker test step, add non-empty and XML parsing assertions for JUnit and sync coverage. Add coverage XML to the upload path and set `if-no-files-found: error`.

- [ ] **Step 3: Run the Docker suite end to end**

Run the exact workflow command through `./run-tests.sh`. If Clyde's supervisor owns host port 9000, use a temporary, uncommitted Compose override that changes only MinIO host bindings; keep container address `minio:9000` unchanged.

Expected: the full sync suite passes, both XML files exist on the host, and `coverage-sync.xml` contains branch data for engine/worker/plugin source files.

- [ ] **Step 4: Validate workflow syntax and commit**

Run:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/integration-tests.yml
git diff --check
```

Then commit:

```bash
git add .github/workflows/integration-tests.yml
git commit -m "ci: publish sync Python coverage"
```

### Task 5: Update CI guidance and ship

**Files:**

- Modify: `AGENTS.md:77-90,123-152,176-180`
- Modify: `.github/copilot-instructions.md:5-25`
- Review: `origin/main...HEAD`

**Interfaces:**

- Consumes: final workflow commands and behavior from Tasks 1-4.
- Produces: accurate contributor/reviewer documentation and a reviewed pull request.

- [ ] **Step 1: Update contributor and review guidance**

Make the guidance state these exact contracts:

```markdown
- UI CI runs both `npm run format:check` and `npm run build`.
- Rust CI runs `cargo fmt --check`, advisory `cargo clippy -- -D warnings`, `cargo build --verbose`, and blocking `cargo test --verbose`.
- `async-unit-tests` runs native asyncio pytest with `.coveragerc-async` and uploads JUnit plus report-only coverage XML.
- `integration-tests` runs gevent-patched pytest in Docker with `.coveragerc-sync` and uploads JUnit plus report-only coverage XML.
- `Documentation Check` compiles mdBook on pull requests and pushes; it does not deploy GitHub Pages.
- Python CI runs `uv lock --check` and `uv sync --dev --locked` before quality checks.
```

Replace stale command examples with the exact commands implemented in the three workflows; document that Clippy alone remains advisory because of its existing baseline, and remove every reference to Rust tests being advisory or mdBook being a release/deploy workflow.

- [ ] **Step 2: Run complete local verification**

Run:

```bash
uv lock --check
uv sync --dev --locked
uv run ruff check --no-fix .
uv run ruff format --check .
uv run mypy .
uv tool run fawltydeps --check-unused --pyenv .venv
SKIP=prettier-osprey-ui uv run pre-commit run --all-files
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/code-quality.yml .github/workflows/integration-tests.yml .github/workflows/mdbook.yml
```

Also rerun the CI-equivalent mdBook, UI, Rust tests, async coverage, and Docker coverage commands from Tasks 1-4. Confirm separately that Clippy reaches linting after `protoc` installation and fails only on its documented baseline.

- [ ] **Step 3: Inspect final state**

Run:

```bash
git status --short
git diff --check origin/main...HEAD
git log --oneline origin/main..HEAD
```

Expected: clean worktree, no whitespace errors, and only the planned commits.

- [ ] **Step 4: Request independent review**

Have a separate read-only subagent review the complete diff for workflow semantics, gevent/async coverage correctness, artifact validation, documentation accuracy, and unintended production behavior. Fix confirmed findings and rerun affected checks.

- [ ] **Step 5: Push and create the pull request**

Push `codex/ci-coverage-expansion`, open a PR against `main`, and include the exact verification counts and initial coverage percentages. Do not modify repository rulesets or required checks.
