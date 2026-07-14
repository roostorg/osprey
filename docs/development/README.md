# Getting Started

Quickly see Osprey working with sample data and a real rule using the demo script. It needs no code or configuration, and everything cleans up with a single command afterward. For a full development environment—running services individually, making changes, and debugging—see [Local Development](local.md).

To run the demo:

1. **Ensure you have prerequisites** installed: Docker with the Compose v2 plugin, and the Docker daemon running. The script checks these before doing anything.

   ```sh
   docker --version && docker compose version
   ```

   The stack also needs a number of free ports; the script checks them up front and names any that are taken. The ones you'll actually visit are `5002` (Osprey UI) and `8888` (Druid console); the worker and UI API use `5001` and `5004`.

2. **Run the script** from a clone of the repo:

   ```sh
   ./demo.sh
   ```

   Or without cloning anything first—this clones the repo into `./osprey-demo` for you:

   ```sh
   curl -sSL https://raw.githubusercontent.com/roostorg/osprey/main/demo.sh | bash
   ```

   The script starts the whole stack (Kafka, Druid, PostgreSQL, MinIO, and Osprey's own worker, API, and UI—more than a dozen containers in all) plus a test-data producer that sends one synthetic post event per second. If it finds services from a previous run, it asks before stopping them; volumes from earlier demo runs are removed either way, so each demo starts from clean state. Expect the first run to spend a few minutes pulling and building images.

3. **Wait for the "Demo Ready!" banner.** The script waits for every service to report healthy, then for the first events to flow through, so this takes a couple of minutes. When it's done, it opens the UI in your browser (or prints the URL to open yourself), pre-filled with a query for the last day of events.

## What to try

The demo ruleset has one rule, `ContainsHello`: any post containing the word "hello" gets its author banned and labeled `meow`. The producer builds five-word posts from a small word pool, so roughly a third of the generated posts trigger it.

- **Watch the event stream.** The right panel shows events as they're processed. Click one that matched `ContainsHello` and look at its extracted features and effects.
- **Filter with a query.** Enter `ContainsHello == True` in the query bar to see only the posts that fired the rule; see [Query Syntax](../user/investigate/query-syntax.md) for what else you can express.
- **Group with Top N.** The pre-filled query groups by `UserId`, showing which synthetic users have been banned the most.
- **Open the Rules Visualizer** from the navigation bar to see the dependency graph between the demo's features and rule.

The [User Guide](../user/) covers the full investigation workflow, and [Writing Rules](../rules.md) explains the SML behind `ContainsHello` and how to write your own rules.

## Stopping the demo

From the repo folder (or `osprey-demo/` if the script cloned for you):

```sh
docker compose --profile test_data down -v
```

This stops every container and deletes the volumes, including all generated demo data.

## If something goes wrong

The script is a convenience wrapper; you can run the same stack directly and watch the logs:

```sh
docker compose --profile test_data up -d
docker compose logs --follow osprey-worker
```

Then open <http://localhost:5002> once things settle. [Troubleshooting](troubleshooting.md) covers common failure modes, and [Local Development](local.md) documents each service the demo starts.
