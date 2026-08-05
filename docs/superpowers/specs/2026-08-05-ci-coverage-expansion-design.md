# CI Coverage Expansion Design

## Goal

Expand Osprey's advisory CI coverage across documentation, Rust, UI, and Python without adding branch protection, required status checks, or arbitrary coverage thresholds.

## Current State

- The mdBook workflow deploys only on pushes to `main`, but Discord's repository does not have GitHub Pages enabled. The workflow fails in `actions/configure-pages`; the repository homepage and book metadata still point to the upstream ROOST site.
- Rust formatting and compilation are enforced. All 52 Rust tests pass but use `continue-on-error`. Clippy also uses `continue-on-error`; it previously stopped at missing `protoc`, and after that prerequisite is installed it exposes 229 existing lint errors.
- UI CI installs the locked npm dependencies and checks Prettier. The existing production build succeeds locally but is not run in CI. There are no UI test files, and the current ESLint configuration does not define a meaningful rule baseline.
- The sync Docker suite and native asyncio suite publish JUnit XML but do not collect coverage.

## Design

### Documentation build check

Convert `.github/workflows/mdbook.yml` from a Pages deployment into a validation-only workflow:

- rename the workflow and comments to describe documentation validation rather than deployment;
- trigger on pull requests targeting `main` and pushes to `main`;
- grant only `contents: read`;
- use credential-safe checkout;
- replace the deployment-wide concurrency group with workflow/ref grouping and cancellation of superseded runs;
- install the existing pinned mdBook version (`0.5.2`) with the repository's SHA-pinned Rust toolchain action and `cargo install --locked`;
- run `mdbook build` with a 15-minute job timeout;
- remove Pages setup, artifact deployment, deployment permissions, and the deployment job.

The generated book remains an ephemeral build output. This repository will not publish a Discord-hosted Pages site or change its homepage/edit links.

### Rust test enforcement and Clippy visibility

Install `protobuf-compiler` before Clippy so a cold runner can execute the coordinator's `build.rs`. Keep Clippy advisory while its 229-error baseline is cleaned up separately, but remove `continue-on-error` from `cargo test` so behavioral regressions make `rust-quality` visibly red.

Keep the existing Rust format, build, cache, toolchain, and Clippy command otherwise unchanged, and remove the now-duplicate package installation from the build step. Existing compiler and Clippy warnings are not part of this cleanup.

### UI production-build coverage

Add `npm run build` after Prettier in `ui-quality` and give the job a 15-minute timeout. The build exercises strict TypeScript checking, production bundling, imports, and static assets using the existing locked dependencies and build script.

Do not add `npm test` while the repository has no UI tests, and do not add ESLint until its rule configuration has a deliberate baseline.

### Report-only Python coverage

Add `pytest-cov` to the root development dependency group, add it to FawltyDeps' plugin/CLI-only unused-dependency exemptions, and regenerate `uv.lock`.

Use separate coverage configuration files for the two runtimes:

- `osprey_worker/.coveragerc-sync` is available through the test runner's existing worker bind mount and enables branch coverage with `gevent` and `thread` concurrency;
- `osprey_async_worker/.coveragerc-async` enables branch coverage without importing gevent.

Both configurations omit `*/tests/*`, `*/test/*`, `*/conftest.py`, `*/__test__.py`, and `*_pb2*.py`. Keeping them separate preserves the async worker's gevent-free import invariant.

Collect branch coverage separately for the two execution models:

- the native asyncio job measures the `osprey_async_worker/src/osprey/async_worker` source tree and emits `/tmp/test-results/coverage-async.xml` using the async coverage configuration;
- the Docker/gevent suite measures `osprey.engine`, `osprey.worker`, and the `example_plugins/src` source tree and emits `/tmp/test-results/coverage-sync.xml` using the sync configuration.

Both pytest invocations explicitly request `term-missing` and XML coverage reports. After pytest, each workflow asserts that its JUnit and coverage XML files are non-empty and parse as XML. Artifact uploads use `if-no-files-found: error` and contain both validated files.

There is no `fail_under` value and no external coverage service in this PR. The initial reports establish a trustworthy baseline; a later change can introduce a regression budget after the numbers are reviewed.

### CI documentation cleanup

Update `AGENTS.md`, `.github/copilot-instructions.md`, and `docs/docs.md` so their CI and documentation descriptions match the validation-only mdBook workflow, UI production build, enforced Rust tests/advisory Clippy, and report-only Python coverage. Remove stale wording that promises Pages deployment, says CI only checks formatting, or omits tests.

## Failure Behavior

- A documentation compile error fails the mdBook job; no deployment is attempted.
- A Rust unit-test failure fails `rust-quality`; existing Clippy debt remains visible but advisory.
- A TypeScript or production-bundle failure fails `ui-quality`.
- Test failures, missing/malformed coverage XML, and missing result artifacts fail their existing Python jobs, but low coverage percentages do not.
- None of these statuses block merging because repository protection settings remain unchanged.

## Scope Boundaries

This PR does not include:

- branch protection or required checks;
- UI test creation or ESLint rule design;
- Python undeclared-dependency or wheel-install checks;
- actionlint, zizmor, CodeQL, or dependency-review checks;
- Dependabot alert remediation;
- a Discord-hosted documentation site;
- production Osprey, Smite, coordinator, or UI behavior changes.

The excluded Python-integrity and workflow-security work will be implemented in the next two independent PRs.

## Verification

- `mdbook build` using mdBook `0.5.2` installed with `--locked`
- `npm ci --ignore-scripts && npm run format:check && npm run build`
- `cargo fmt --check`
- `cargo clippy -- -D warnings` reaches linting after `protoc` installation and reports the known baseline without blocking the job
- `cargo build --verbose`
- `cargo test --verbose`
- locked Python sync and the existing Ruff, formatting, mypy, and FawltyDeps checks
- native asyncio suite with JUnit and coverage XML
- Docker-backed integration suite with JUnit and coverage XML
- inspection of both coverage artifacts to confirm non-empty measured files and branch data
- current CI guidance in `AGENTS.md`, `.github/copilot-instructions.md`, and `docs/docs.md` updated for the new documentation, UI build, Rust, and coverage behavior
- independent read-only review of the complete diff
