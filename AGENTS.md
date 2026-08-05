# AGENTS.md

Instructions for AI coding agents working on Osprey. `README.md` is for humans; this file is for machines. The nearest `AGENTS.md` to the edited file wins; explicit user prompts override everything.

## Architecture

Top-level modules:

- `osprey_worker/` — main Python engine. Consumes events from Kafka, evaluates SML rules, emits verdicts and effects to output sinks. New worker/engine code belongs here (`osprey_worker/src/osprey/worker/`).
- `osprey_rpc/` — generated protobuf/gRPC bindings under `osprey_rpc/src/osprey/rpc/`. Do not edit generated files (`*_pb2*.py`, `*_pb2*.pyi`) by hand; regenerate via `./gen-protos.sh` after editing the `.proto` files.
- `osprey_ui/` — React + TypeScript frontend (Ant Design, Highcharts; versions in `osprey_ui/package.json`). UI code belongs here.
- `osprey_coordinator/` — Rust gRPC coordinator (tokio, tonic, etcd, rdkafka). Rust code belongs here.
- `proto/osprey/rpc/` — protobuf source of truth for `osprey_rpc` and `osprey_coordinator` types.
- `example_plugins/` — reference plugins (UDFs, output sinks, labels service) using the pluggy-based plugin system. Do not add production code here.
- `example_rules/` — sample SML rules and YAML config.

Reference files: `docs/DEVELOPMENT.md` (setup), `example_plugins/src/register_plugins.py` (plugin patterns), `example_plugins/src/services/labels_service.py` (labels service example).

## Design

- API: gRPC between `osprey_coordinator` and workers; HTTP/Flask for `osprey-ui-api` (port 5004); protobuf definitions under `proto/osprey/rpc/` are authoritative.
- Rules: SML (Osprey's rule language) with user-defined functions registered via pluggy hooks (`@hookimpl_osprey`): `register_udfs`, `register_output_sinks`, `register_labels_service_or_provider`.
- Data model conventions: Pydantic for models, SQLAlchemy for persistence (versions pinned in `pyproject.toml`).

## Build and run

Prerequisites: Python (version in `.python-version`), [uv](https://docs.astral.sh/uv/), Docker + Docker Compose v2, Node.js (version in `.github/workflows/code-quality.yml`, UI only), Rust stable + `protoc` (coordinator only).

```bash
# Install Python deps (creates .venv, uses uv.lock)
uv sync --dev

# Install git hooks
uv run pre-commit install --install-hooks

# Start full stack (Kafka, Postgres, Druid, MinIO, Bigtable emulator, worker, UI, UI API)
docker compose up -d
# or
./start.sh
# with coordinator:
./start.sh --with-coordinator

# UI dev server
cd osprey_ui && npm ci && npm start

# Regenerate protobuf bindings after editing proto/osprey/rpc/**/*.proto
./gen-protos.sh
```

UI: http://localhost:5002 · UI API: http://localhost:5004 · Worker (port 5001)

## Testing

Run the full integration suite (spins up all services via docker compose; ~8 GB RAM):

```bash
./run-tests.sh
```

Pass pytest args through:

```bash
./run-tests.sh path/to/test_file.py::test_name
./run-tests.sh -k some_keyword
./run-tests.sh --junitxml=/tmp/test-results/junit-pytest.xml
```

Python lint / format / type-check (no Docker needed):

```bash
uv run ruff check
uv run ruff format --diff
uv run mypy .
uv run pre-commit run --all-files
```

UI checks (in `osprey_ui/`):

```bash
npm run format:check
npm run build
```

Rust checks (in `osprey_coordinator/`; requires `protoc`). CI gates on `fmt`, `build`, and `test`. Clippy remains advisory while the existing lint baseline is cleaned up:

```bash
cargo fmt --check
cargo clippy -- -D warnings   # advisory
cargo build --verbose
cargo test --verbose
```

## Browser MCP (UI verification)

A project-scoped MCP server (`.mcp.json` at repo root) registers `@playwright/mcp@0.0.73` via `npx`. When Claude Code launches in this repo it gets `browser_navigate`, `browser_snapshot`, `browser_evaluate`, `browser_take_screenshot`, and the rest of the `browser_*` tool surface — useful for verifying visual UI changes against the running dev server without a full automated test suite. There is intentionally no `playwright.config.ts` / `@playwright/test` integration and no devDep in `osprey_ui/package.json`; the MCP is for ad-hoc verification, not CI.

All commands in this section — including any `--dry-run` previews — are **operator-run only**. Agents must never execute them. If a prereq is missing, the agent surfaces *what the operator should run* and waits.

The MCP needs Playwright's bundled Chromium binary plus its system shared libs. Setup is platform-specific — Playwright's docs cover it across macOS / Windows / WSL / Linux: <https://playwright.dev/docs/browsers#install-browsers>.

**Operator runs the binary install** (no sudo, user-cache only). Pinned via `@playwright/mcp@0.0.73`'s bundled `playwright-core`, so the downloaded Chromium build matches what the MCP server will launch:

```bash
npx -y @playwright/mcp@0.0.73 install-browser chromium
```

System libs:

- **macOS, recent Windows / WSL**: nothing extra to install.
- **Linux**: distro-specific. **The operator** runs the dry-run to preview the package list `install-deps` would apt-install:

  ```bash
  npx -y playwright install-deps chromium --dry-run
  ```

  …and then **the operator** runs the printed `apt-get install` line themselves. The agent does neither step.

  `install-deps` only auto-supports recent Ubuntu / Debian. Fedora, Arch, Alpine, and NixOS need manual lib installation — Playwright's troubleshooting docs cover their package names.

Before calling `browser_navigate("http://localhost:5002")`, **the operator** starts the dev server (`cd osprey_ui && npm start`, listens on `:5002`).

If the MCP approval prompt doesn't fire on a fresh `claude` launch, **the operator** runs `claude mcp reset-project-choices` and re-launches.

## CI

CI runs entirely via GitHub Actions on `pull_request` and `push` to `main`. The commands below mirror the workflow steps in order; subshells model each job's working directory. Run them without error suppression so each step's exit code remains visible:

```bash
# code-quality.yml → python-quality
uv lock --check
uv sync --dev --locked
uv run pre-commit install --install-hooks
SKIP=prettier-osprey-ui uv run pre-commit run --show-diff-on-failure --color=always --all-files
uv tool run fawltydeps --check-unused --pyenv .venv

# code-quality.yml → async-unit-tests
uv sync --dev --locked
mkdir -p /tmp/test-results
uv run pytest -q \
  --junitxml=/tmp/test-results/junit-async.xml \
  --cov-config=osprey_async_worker/.coveragerc-async \
  --cov=osprey_async_worker/src/osprey/async_worker \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:/tmp/test-results/coverage-async.xml \
  osprey_async_worker
test -s /tmp/test-results/junit-async.xml
test -s /tmp/test-results/coverage-async.xml
uv run python -c 'from xml.etree import ElementTree; ElementTree.parse("/tmp/test-results/junit-async.xml"); ElementTree.parse("/tmp/test-results/coverage-async.xml")'

# code-quality.yml → ui-quality (CI `working-directory: osprey_ui`)
( cd osprey_ui
  npm ci --ignore-scripts
  npm run format:check
  npm run build )

# code-quality.yml → rust-quality (CI `working-directory: osprey_coordinator`)
# Only Clippy is advisory (`continue-on-error: true`) because of its existing baseline.
sudo apt-get update
sudo apt-get install -y protobuf-compiler
( cd osprey_coordinator
  cargo fmt --check
  cargo clippy -- -D warnings
  cargo build --verbose
  cargo test --verbose )

# integration-tests.yml
mkdir -p /tmp/test-results
docker compose -f docker-compose.yaml -f docker-compose.test.yaml --profile test pull
docker compose -f docker-compose.yaml -f docker-compose.test.yaml --profile test build
./run-tests.sh \
  --junitxml=/tmp/test-results/junit-pytest.xml \
  --cov-config=osprey_worker/.coveragerc-sync \
  --cov=osprey_worker/src/osprey/engine \
  --cov=osprey_worker/src/osprey/worker \
  --cov=example_plugins/src \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:/tmp/test-results/coverage-sync.xml
test -s /tmp/test-results/junit-pytest.xml
test -s /tmp/test-results/coverage-sync.xml
python3 -c 'from xml.etree import ElementTree; ElementTree.parse("/tmp/test-results/junit-pytest.xml"); ElementTree.parse("/tmp/test-results/coverage-sync.xml")'

# mdbook.yml → build-docs
cargo install --locked --version 0.5.2 mdbook
mdbook build
```

The async and Docker pytest jobs upload their validated JUnit and report-only coverage XML files; neither enforces a coverage threshold. `mdbook.yml` validates documentation on pull requests and pushes to `main`; it does not deploy GitHub Pages. Do not modify `mdbook.yml` or the release/deploy workflows without human approval (see "Human-approval-required actions" below).

## Security

- No secrets in code or committed files. Use environment variables via `docker-compose.yaml`.
- Do not disable lint or type rules to silence errors. Fix the underlying issue, or use a narrowly-scoped `# noqa: <code>` / `# type: ignore[<code>]` with a comment explaining why.
- Before adding a new dependency, check it for known CVEs and confirm the license is compatible with `LICENSE.md`.
- Do not commit generated protobuf files from an untrusted toolchain; always regenerate via `./gen-protos.sh`.
- Default Docker bindings are `127.0.0.1`; do not change bind addresses without explicit instruction (see `docs/DEVELOPMENT.md` §6).

## Code review

- Keep diffs small and focused; split unrelated changes into separate PRs.
- PR titles are descriptive and imperative ("Add X", "Fix Y").
- New behavior requires a test. Bug fixes require a regression test.
- All blocking CI checks above must pass before requesting review. Review advisory Clippy output separately against its existing baseline.

## Code style

- Python: version in `.python-version`. Lint + format with `ruff`, type-check with `mypy` (versions and config in `pyproject.toml` under `[tool.ruff]` and `[tool.mypy]`).
- TypeScript / React in `osprey_ui/` (versions in `osprey_ui/package.json`). Formatter is Prettier (`npm run format:check`); config in `osprey_ui/.prettierrc`. Node version in `.github/workflows/code-quality.yml`.
- Rust stable in `osprey_coordinator/` (edition and toolchain in `osprey_coordinator/Cargo.toml`). Formatter `cargo fmt`; linter `cargo clippy -- -D warnings`.
- Protobuf generated files (`*_pb2*.py`, `*_pb2*.pyi`) are excluded from ruff and mypy — do not edit.

## CD

- Releases are cut by publishing a GitHub Release; the tag triggers `.github/workflows/release-osprey-rpc.yml` to build and attach the `osprey_rpc` sdist. Tags follow semver (`vMAJOR.MINOR.PATCH`).
- Coordinator image publishes to `ghcr.io` via `.github/workflows/publish-coordinator-image.yml` on push to `main` and on release.
- `.github/workflows/mdbook.yml` only validates that mdBook compiles. The canonical published documentation remains on the upstream ROOST site.
- Release/deploy workflows, production Dockerfiles, and signing/tagging are restricted — see "Human-approval-required actions" below.

## Dependencies

- Python deps are pinned in `pyproject.toml` and locked in `uv.lock`. Add with `uv add <pkg>` (runtime) or `uv add --dev <pkg>` (dev); commit the updated `uv.lock`.
- Node deps live in `osprey_ui/package.json`; add with `npm install --save <pkg>` and commit the updated `osprey_ui/package-lock.json`.
- Rust deps live in `osprey_coordinator/Cargo.toml`. Note: `Cargo.lock` is currently in `.gitignore` — do not commit it without first un-ignoring it.
- Every new or upgraded package including transitive dependencies requires human approval. Confirm the license is compatible with `LICENSE.md` and that there are no known CVEs.
- `fawltydeps` enforces that every declared Python dep is used; add intentional exceptions to `[tool.fawltydeps].ignore_unused` in `pyproject.toml` with a comment.

## ROOST guiding principles

- **Commands over prose.** Prefer `./run-tests.sh path/to/test_file.py::test_name` over descriptive paragraphs.
- **Same review bar.** PRs authored with agent assistance are held to the same standards as any other PR.
- **Boundaries with alternatives.** When stating a restriction, provide the alternative path (e.g. don't edit `*_pb2*.py` — regenerate via `./gen-protos.sh`).
- **Iterate over time.** Start minimal. When you give an agent the same instruction twice, add it to this file.
- **Contributors update `AGENTS.md`.** When you find a gap, update this file as part of your PR.

## Human-approval-required actions

Stop and get explicit human approval before:

- Changing license headers, copyright notices, or any legal text (including `LICENSE.md`).
- Modifying release, signing, or deploy workflows (`.github/workflows/publish-coordinator-image.yml`, `.github/workflows/release-osprey-rpc.yml`), the documentation workflow (`.github/workflows/mdbook.yml`), production Dockerfiles (`osprey_coordinator/Dockerfile`, `osprey_worker/Dockerfile`, `osprey_ui/Dockerfile`), `docker-compose.yaml`, `start.sh`, or `entrypoint.sh`.
- Adding, removing, or upgrading any library or package (including transitive dependencies in `uv.lock` or `osprey_ui/package-lock.json`) — confirm licenses are compatible.
- Editing generated code under `osprey_rpc/src/osprey/rpc/` by hand instead of regenerating via `./gen-protos.sh`.
