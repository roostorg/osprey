# Code review instructions

These instructions apply to AI tools when they review pull requests in this repository, and when they answer questions about this codebase. They are guidance, not a checklist. Use judgment, prefer fewer high-signal comments over many low-signal ones, and skip points that don't apply to the diff in front of you. For human contributor guidance see [`README.md`](../README.md); for general agent rules see [`AGENTS.md`](../AGENTS.md).

## Repository at a glance

- Multi-language monorepo: Python (worker + UI API), TypeScript (UI), Rust (coordinator). Top-level modules are documented in [`AGENTS.md`](../AGENTS.md) § Architecture — `osprey_worker/`, `osprey_rpc/`, `osprey_ui/`, `osprey_coordinator/`, `proto/osprey/rpc/`, `example_plugins/`, `example_rules/`.
- Python version pinned in `.python-version`, managed with [`uv`](https://docs.astral.sh/uv/). Linted and type-checked in CI by `ruff` + `mypy` via `pre-commit`. Unused-dep enforcement via `fawltydeps`.
- TypeScript in `osprey_ui/` (React + Ant Design + Highcharts; Node version in `.github/workflows/code-quality.yml`). Formatted in CI by `prettier` (`npm run format:check`).
- Rust in `osprey_coordinator/` (stable toolchain, `tokio` + `tonic` + `etcd` + `rdkafka`). `cargo fmt` and `cargo build` gate CI; `cargo clippy -- -D warnings` and `cargo test` run advisory (`continue-on-error: true`).
- **Generated code**: `osprey_rpc/src/osprey/rpc/**/*_pb2*.py`, `*_pb2*.pyi`, and `osprey_coordinator/src/proto/` are produced by `./gen-protos.sh` from `proto/osprey/rpc/**/*.proto`. Hand-edits drift from the schema — regenerate instead. Generated files are excluded from `ruff` and `mypy` for a reason.
- Plugin system: `pluggy` with `@hookimpl_osprey` hooks (`register_udfs`, `register_output_sinks`, `register_labels_service_or_provider`). Reference patterns live in `example_plugins/src/register_plugins.py`.
- Data model conventions: Pydantic for models, SQLAlchemy for persistence (versions pinned in `pyproject.toml`).

## Scope of review — focus on quality and security

Lint and formatting are enforced in CI, so please skip:

- formatting, whitespace, indentation, quote style, or import ordering
- `ruff` / `mypy` / `prettier` / `cargo fmt` rule violations
- typos in comments or doc grammar nits
- missing docstrings on internal helpers
- subjective style preferences not codified in a project rule

If a finding would be caught by `uv run ruff check`, `uv run mypy .`, `npm run format:check` (in `osprey_ui/`), or `cargo fmt --check` (in `osprey_coordinator/`), it's redundant.

## Security (cross-cutting)

Security findings are the highest-value comments you can leave. When you spot one, name the risk concretely and suggest a fix. Areas to watch across the whole codebase:

- **Hard-coded secrets.** API keys, tokens, passwords, OAuth secrets, JWT signing keys, DB connection strings (Postgres, Druid, Kafka SASL, MinIO, Bigtable), or webhook secrets in source or committed config. `docker-compose.yaml` uses environment variables intentionally — flag any secret that leaks into committed files.
- **Injection.** String-built SQL, shell commands, file paths, HTML, or LLM prompts are usually a smell. Look for SQLAlchemy bound parameters (or `text()` with `bindparams`), `subprocess` arg arrays (no `shell=True`), context-aware HTML encoding, and a clear separation between trusted system prompts and untrusted user content.
- **Sensitive logging.** Secrets, JWTs, full `Authorization` headers, full request/response bodies, raw Kafka payloads, or PII in logs, traces, metric labels, or error responses are risky. Error responses from the UI API (port 5004) shouldn't leak Python stack traces.
- **Weak crypto.** MD5 or SHA-1 used for security, ECB mode, reused IVs, `random.random()` / `random.choice()` for tokens or IDs (prefer `secrets` in Python, `OsRng` / `getrandom` in Rust), or hand-rolled crypto are worth questioning. JWTs should reject the `none` algorithm, use strong secrets, and have short access-token expirations.
- **Unsafe deserialization or evaluation.** `eval`, `exec`, `pickle.loads`, `yaml.load` without `SafeLoader`, and `marshal.loads` on untrusted input are risky. Same for `eval` / the `Function` constructor / `setTimeout` with string arguments in TypeScript.
- **Removing security controls.** If a diff disables authentication, authorization, CSRF, CORS, rate limiting, or the default `127.0.0.1` bindings in `docker-compose.yaml` (see `docs/DEVELOPMENT.md` §6), ask whether it's intentional and justified.
- **Untrusted protobuf toolchain.** Per `AGENTS.md` § Security, only regenerate bindings via `./gen-protos.sh`. Flag generated-file diffs that don't match a corresponding `.proto` change.

Path-specific concerns (UI API views, output sinks, plugin adaptor, Rust coordinator, raw `.proto` changes, dependency manifests, restricted infra) are scoped in [`.coderabbit.yaml`](../.coderabbit.yaml) under `reviews.path_instructions`. When reviewing those areas, apply the same general principles — authorization checks on UI-API-supplied IDs, parameterized queries, XSS care on the client, panic-safety in Rust, license/CVE attention on dependency bumps.

## Code quality (cross-cutting)

Use judgment — these patterns tend to cause bugs or maintenance pain regardless of where they appear:

- **Generated files.** `*_pb2*.py`, `*_pb2*.pyi`, and `osprey_coordinator/src/proto/` are produced by `./gen-protos.sh`; hand-edits drift from the `.proto` schema.
- **Error handling.** Silently swallowed exceptions (`except Exception: pass` with no log or rethrow), Rust `let _ = result;` discarding an error, unhandled promise rejections in TS, and missing `await` on a coroutine whose result matters tend to cause production surprises.
- **Async correctness (Python).** Calling sync I/O inside an `async def` blocks the event loop. `asyncio.gather(...)` without `return_exceptions=True` or explicit handling can mask failures.
- **Async correctness (Rust).** Holding a `std::sync::Mutex` guard across `.await` deadlocks the tokio runtime; long-lived `tokio::sync` guards can starve other tasks.
- **Type safety.** New `# type: ignore` / `Any` / `cast(...)` in Python, `any` / `as unknown as` / non-null `!` / `@ts-ignore` in TypeScript, or `unwrap()` / `expect()` / `unsafe` on user-driven paths in Rust are worth questioning. Narrowly-scoped `# type: ignore[<code>]` and `@ts-expect-error` with a justifying comment are preferred when an escape hatch is genuinely needed (matches `AGENTS.md` § Security).
- **Tests.** New behavior generally warrants a test; bug fixes generally warrant a regression test (`AGENTS.md` § "Code review"). Integration tests run via `./run-tests.sh`.
- **Duplicated logic.** If a helper already exists in the same package, prefer it over a parallel implementation.
- **Dead code.** Commented-out blocks and TODOs without an issue link are worth a nudge.

## What not to flag

These categories of comments tend to add noise without surfacing real risk — please skip them:

- "Consider adding a `None` check" on a value already typed as non-`None` by mypy or non-`null`/`undefined` by TypeScript.
- "Consider adding error handling" on a wrapper that already propagates errors via `async`/`await` or `?` in Rust.
- "This could be a constant" on a string literal used in a single place.
- "Add a docstring" on an internal helper.
- Rhetorical questions like "have you considered…" without a concrete risk attached.
- Defensive-coding suggestions on values whose types already prevent the failure mode.
- "Add a test" on a config-only, doc-only, or comment-only change.
- Suggestions to rename a symbol "for clarity" without a concrete ambiguity.
- Style nits in generated protobuf files (`*_pb2*.py`, `*_pb2*.pyi`, `osprey_coordinator/src/proto/`) — they're excluded from `ruff` and `mypy` for a reason.

## Tone

Be specific and concise. When you flag something, name the concrete risk ("possible SQL injection via string-built `text()`", "missing authorization check — possible IDOR", "secret logged at info level", "`unwrap()` on RPC input — panics will crash the worker") and, where helpful, show the fix in code. Skip nits, and stay quiet when nothing in this list applies.
