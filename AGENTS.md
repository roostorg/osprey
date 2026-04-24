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
```

Rust checks (in `osprey_coordinator/`; requires `protoc`). CI only gates on `fmt` and `build`; `clippy` and `test` are advisory (`continue-on-error: true`):

```bash
cargo fmt --check
cargo build --verbose
cargo clippy -- -D warnings   # advisory
cargo test --verbose          # advisory
```

## CI

CI runs entirely via GitHub Actions on `pull_request` and `push` to `main`. Each line below is one literal CI `run:` step, in workflow order. Run them in your shell (paste-as-is — no `&&` chaining, no error suppression — so each step's exit code matches the corresponding CI step's exit code):

```bash
# code-quality.yml → python-quality
uv sync --dev
uv run pre-commit install --install-hooks
SKIP=prettier-osprey-ui uv run pre-commit run --show-diff-on-failure --color=always --all-files
uv tool run fawltydeps --check-unused --pyenv .venv

# code-quality.yml → ui-quality (CI `working-directory: osprey_ui`)
( cd osprey_ui
  npm ci
  npm run format:check )

# code-quality.yml → rust-quality (CI `working-directory: osprey_coordinator`)
# Note: in CI the `cargo clippy` and `cargo test` steps are marked `continue-on-error: true`,
# so they currently print failures but do not fail the job. Locally, expect the same output.
( cd osprey_coordinator
  cargo fmt --check
  cargo clippy -- -D warnings
  cargo build --verbose
  cargo test --verbose )

# integration-tests.yml
./run-tests.sh
```

`mdbook.yml`, `publish-coordinator-image.yml`, and `release-osprey-rpc.yml` are release/deploy workflows — do not modify without human approval (see "Human-approval-required actions" below).

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
- All CI checks (above) must pass before requesting review.

## Code style

- Python: version in `.python-version`. Lint + format with `ruff`, type-check with `mypy` (versions and config in `pyproject.toml` under `[tool.ruff]` and `[tool.mypy]`).
- TypeScript / React in `osprey_ui/` (versions in `osprey_ui/package.json`). Formatter is Prettier (`npm run format:check`); config in `osprey_ui/.prettierrc`. Node version in `.github/workflows/code-quality.yml`.
- Rust stable in `osprey_coordinator/` (edition and toolchain in `osprey_coordinator/Cargo.toml`). Formatter `cargo fmt`; linter `cargo clippy -- -D warnings`.
- Protobuf generated files (`*_pb2*.py`, `*_pb2*.pyi`) are excluded from ruff and mypy — do not edit.

## CD

- Releases are cut by publishing a GitHub Release; the tag triggers `.github/workflows/release-osprey-rpc.yml` to build and attach the `osprey_rpc` sdist. Tags follow semver (`vMAJOR.MINOR.PATCH`).
- Coordinator image publishes to `ghcr.io` via `.github/workflows/publish-coordinator-image.yml` on push to `main` and on release.
- mdBook docs deploy via `.github/workflows/mdbook.yml` to GitHub Pages on push to `main`.
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
- Modifying release, signing, or deploy workflows: `.github/workflows/publish-coordinator-image.yml`, `.github/workflows/release-osprey-rpc.yml`, `.github/workflows/mdbook.yml`, production Dockerfiles (`osprey_coordinator/Dockerfile`, `osprey_worker/Dockerfile`, `osprey_ui/Dockerfile`), `docker-compose.yaml`, `start.sh`, or `entrypoint.sh`.
- Adding, removing, or upgrading any library or package (including transitive dependencies in `uv.lock` or `osprey_ui/package-lock.json`) — confirm licenses are compatible.
- Editing generated code under `osprey_rpc/src/osprey/rpc/` by hand instead of regenerating via `./gen-protos.sh`.
