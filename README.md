<img width="200" height="64" alt="ROOST logo" src="images/ROOST-Horizontal-Yellow.png" />

# Osprey

**Automate the obvious and investigate the ambiguous.** High-performance safety rules engine for real-time event processing at scale. 

- take action based on user behavior
- combine actions with human written rules
- let operators query human actions and past decisions
- perform investigations or write new rules based on decisions

Osprey is an event stream decisions engine and analysis UI designed to investigate and take automatic action on events and their properties as they happen in real-time. Originally developed internally at [Discord](https://discord.com/) to combat spam, abuse, botting, and scripting across its platform, Osprey has been open-sourced to help other platforms facing similar challenges.

![Osprey UI sample](docs/images/query-and-charts.png)

Osprey is a library for processing actions through human-written rules and outputting verdicts & custom effects back to configurable output sinks. It evaluates events using structured rule logic (SML) that is extendable via user-defined functions (UDFs). Osprey can also track state across events by labelling entities if implementers provide a labels service backend (see [labels_service.py](./example_plugins/src/services/labels_service.py) for a Postgres-backed labels service example).

Osprey is built for engineers and Trust & Safety teams who want to explore, test, and integrate its core capabilities into their platform for incident response and Trust & Safety investigation. [Read more about user research and personas](docs/user_personas.md).

## Adopters

Osprey is used by:

[![Bluesky](docs/images/adopters/bluesky.png)](https://bsky.social) | [![Discord](docs/images/adopters/discord.png)](https://discord.com) | [![Matrix.org](docs/images/adopters/matrix.png)](https://matrix.org)
--- | --- | ---

Using Osprey and want to add your project/organization to this list? [Open a pull request!](https://github.com/roostorg/osprey/edit/main/README.md)

## Development

- See [DEVELOPMENT.md](./docs/DEVELOPMENT.md) for comprehensive development setup and workflow documentation
- All code changes should pass linting (Ruff) and type checking (MyPy)
- Pre-commit hooks automatically run on each commit to maintain code quality

## Join Us

Writing code is not the only way to help the project. Reviewing pull requests, answering questions to help others on mailing lists or issues, providing feedback from a domain expert perspective, organizing and teaching tutorials, working on the website, improving the documentation, are all priceless contributions.

- Join us in [our Discord server](https://discord.gg/5Csqnw2FSQ)
- Join our [newsletter](https://roost.tools/#get-started) for more announcements and information
- Follow us on [Bluesky](https://bsky.app/profile/roost.tools) or [LinkedIn](https://www.linkedin.com/company/roost-tools/)

_[ROOST](https://roost.tools) (Robust Open Online Safety Tools) is a non-profit organization that brings together expertise, resources, and investments from major technology companies and philanthropies to build scalable, interoperable safety infrastructure for the AI era._

### Feedback Wanted

This is a working system, not a prototype. Try it locally, connect your data, write some rules, and tell us what's missing for your use case. We're particularly interested in:

- Integration challenges with your existing platform infrastructure
- Performance characteristics with your event volumes and rule complexity
- Missing detection capabilities or response actions you need
- API improvements that would make adoption easier for your team

Your experimentation feedback will directly shape future priorities and help us build the most useful Trust & Safety tooling for the community.

## Quickstart Troubleshooting

### Druid schema not updating

If Druid is not ingesting data or is stuck on a stale schema after changing rule output structure, reset the Kafka supervisor and resubmit the ingestion spec:

```bash
curl -X POST http://localhost:8888/druid/indexer/v1/supervisor/osprey.execution_results/terminate
docker compose restart druid-spec-submitter
```

To wipe all Druid and MinIO state and start fresh (Postgres data is preserved):

```bash
docker compose down
docker volume rm osprey_middle_var osprey_historical_var osprey_broker_var osprey_coordinator_var osprey_router_var osprey_druid_shared osprey_minio_data
docker compose up -d
```

To wipe everything including Postgres:

```bash
docker compose down -v && docker compose up -d
```

### Test data not appearing in the UI

The UI defaults to querying the last 24 hours. If the selected time range is too large (weeks or months), Druid scans across many segments and results can be slow or appear empty.

- narrow the time range to 1–4 hours centered on when you generated test data
- click the edit icon next to the displayed time range to switch to a custom date/time picker
- note that Druid's Kafka consumer uses `auto.offset.reset: latest` — it only picks up events produced after `docker compose up` first ran, so events from before that point will not appear regardless of the time range

## Recognition

Discord uses Osprey to quickly detect and remove new types of harm that put users at risk. Rather than leaving other platforms to build similar tools from scratch, ROOST and Discord have open-sourced this powerful rule engine in collaboration with [internet.dev](https://internet.dev/) to make it available for anyone who needs it.
