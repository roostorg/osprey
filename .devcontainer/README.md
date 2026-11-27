# Osprey demo in this codespace

This codespace runs the whole Osprey stack, with synthetic demo data flowing through
it. You don't need to type any commands.

## What happens

1. **Preparing** (first launch only, a few minutes, skipped when a prebuild exists):
   the codespace downloads and builds Osprey's services.
2. **Starting** (every launch, 1–3 minutes): a terminal named *postStartCommand*
   shows each service coming up.
3. **Ready**: the terminal prints **Osprey is ready** with the UI's address, and the
   Osprey UI opens in a new browser tab. If your browser blocked the tab, open the
   **Ports** tab at the bottom of this window and click the globe icon next to
   **Osprey UI (5002)**.

## What to try

The Query page opens empty: click **Submit Query** to load the last day of events.

Synthetic posts arrive about once a second. The demo rule, `ContainsHello`, bans and
labels any user whose post contains "hello".

- Watch posts arrive in the **event stream** on the right of the Query page.
- Query `RegexMatch(target=PostText, pattern="hello")` to see only the posts the rule matched.
- Open the **Rules Visualizer**, search for `meow`, and turn on **Show Upstream Nodes**.

## Stopping and coming back

- A codespace stops by itself after 30 idle minutes (your account may set a different
  timeout). You can also stop it from <https://github.com/codespaces> → **⋯** → **Stop codespace**.
- Reopen it from the same page. The demo restarts on its own, keeping the data it had.
- When you're finished for good, **Delete** it there, so it stops using your storage allowance.

Full guide, costs and troubleshooting: [docs/development/codespaces.md](../docs/development/codespaces.md).

## For engineers

How [devcontainer.json](devcontainer.json) is put together:

- **Image**: a plain Ubuntu devcontainer with the Docker-in-Docker feature. Osprey runs
  as Docker Compose services inside it, exactly as on a laptop.
- **`hostRequirements`**: 4 cores / 16 GB. The running stack uses about 6 GiB, plus
  image builds; the 2-core / 8 GB machine runs out of memory.
- **`containerEnv`**: points `docker compose` at the demo stack from any directory.
- **Lifecycle**: `updateContentCommand` ([prepare.sh](scripts/prepare.sh)) pulls and
  builds every image. It's the slow part, and a prebuild runs it ahead of time.
  `postStartCommand` ([start.sh](scripts/start.sh)) runs on every start or resume:
  `docker compose up -d`, then waits for the API, the UI and the first ingested event.
  `postAttachCommand` ([status.sh](scripts/status.sh)) tells each new connection
  whether the demo is ready and where.
- **Ports**: only the UI (5002) is forwarded, and it opens in the browser. Everything
  else is silent; the browser never needs another port.

- `bash .devcontainer/scripts/start.sh` brings the stack up again and waits for it;
  `bash .devcontainer/scripts/status.sh` prints whether it's ready.
- `docker compose` in the terminal already targets the demo stack (`COMPOSE_FILE` and
  `COMPOSE_PROFILES` are set), e.g. `docker compose ps`, `docker compose logs -f osprey-worker`.
- [docker-compose.codespaces.yaml](docker-compose.codespaces.yaml) layers the demo's changes
  over [docker-compose.yaml](../docker-compose.yaml): the UI calls the API through its own
  dev server at `/api`, and the Druid web console is off (`docker compose --profile
  druid-console up -d druid-router` turns it on, at port 8888).
- The demo container doesn't include Python, uv, Node or Rust; it's built for running the
  demo. For development, use the other configuration below.

## Development configuration

[dev/devcontainer.json](dev/devcontainer.json) is a second configuration for working on
Osprey's code. It has the toolchains and installed dependencies, but starts nothing, so it's
ready sooner and stays light until you need the stack.

- **Open it**: in Codespaces, choose **New with options…** and pick **Osprey development**
  under *Dev container configuration*, or use
  <https://codespaces.new/roostorg/osprey?devcontainer_path=.devcontainer/dev/devcontainer.json>.
  VS Code's Dev Containers extension asks which configuration to open; with the CLI, run
  `npx @devcontainers/cli up --workspace-folder . --config .devcontainer/dev/devcontainer.json`.
- **Tools**: Python (from `.python-version`, installed by uv), uv, Node 22.22 with Corepack
  and pnpm, Rust stable with `protoc`, and Docker. [install-tools.sh](dev/install-tools.sh)
  installs what the devcontainer features don't.
- **Dependencies**: [install-deps.sh](dev/install-deps.sh) runs `uv sync --dev`,
  `pnpm install --frozen-lockfile` and `pre-commit install`, at creation, in prebuilds and when
  the codespace's content updates.
- **Running things**: `bash .devcontainer/scripts/start.sh` starts the same stack as the demo,
  with test data, and the UI on port 5002; UI code is mounted into its container, so edits
  reload in the browser. `./run-tests.sh` runs the integration tests. `cd osprey_ui && pnpm
  start` also works, once the demo's `osprey-ui` container is stopped (`docker compose stop
  osprey-ui`), because both use port 5002; the container sets `REACT_APP_API_BASE_URL=/api` so
  it works through Codespaces' forwarded port.
