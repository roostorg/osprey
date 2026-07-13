# Changelog

All notable changes to Osprey will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For more information about each release including git tags and artifacts, see [Releases](https://github.com/roostorg/osprey/releases).

## [Unreleased]

### Added

- Postgres execution result store backend ([#171](https://github.com/roostorg/osprey/pull/171) by [@serendipty01](https://github.com/serendipty01))
- MinIO execution-result backend wired to the UI API ([#332](https://github.com/roostorg/osprey/pull/332) by [@julietshen](https://github.com/julietshen))
- Persistent sidebar navigation ([#240](https://github.com/roostorg/osprey/pull/240) by [@haileyok](https://github.com/haileyok))
- Dark mode ([#246](https://github.com/roostorg/osprey/pull/246) by [@haileyok](https://github.com/haileyok))
- Dark mode follows system preferences ([#360](https://github.com/roostorg/osprey/pull/360) by [@BinaryFiddler](https://github.com/BinaryFiddler))
- Features Registry page at `/features` ([#276](https://github.com/roostorg/osprey/pull/276) by [@haileyok](https://github.com/haileyok))
- Rules Registry page at `/rules` ([#277](https://github.com/roostorg/osprey/pull/277) by [@haileyok](https://github.com/haileyok))
- `GetActionId()` stdlib UDF for retrieving the current action ID from execution context ([#327](https://github.com/roostorg/osprey/pull/327) by [@julietshen](https://github.com/julietshen))
- `ParseInt` UDF for converting numeric strings to integers ([#190](https://github.com/roostorg/osprey/pull/190) by [@bealsbe](https://github.com/bealsbe))
- `StringSlice` UDF for extracting substrings by index range ([#189](https://github.com/roostorg/osprey/pull/189) by [@bealsbe](https://github.com/bealsbe))
- `InExperiment` UDF for checking experiment membership ([#203](https://github.com/roostorg/osprey/pull/203) by [@bealsbe](https://github.com/bealsbe))
- Per-action health metrics in the executor ([#191](https://github.com/roostorg/osprey/pull/191) by [@cmttt](https://github.com/cmttt))
- Option to suppress cached errors to reduce metric bloat ([#180](https://github.com/roostorg/osprey/pull/180) by [@lithium-powered](https://github.com/lithium-powered))
- Experimental asyncio-native worker with metrics and engine/coordinator improvements ([#341](https://github.com/roostorg/osprey/pull/341) by [@cmttt](https://github.com/cmttt))

### Changed

- Event stream defaults to selecting all event types on first load ([#194](https://github.com/roostorg/osprey/pull/194) by [@chimosky](https://github.com/chimosky))
- Event stream shows sensible defaults so first-load isn't empty ([#297](https://github.com/roostorg/osprey/pull/297) by [@haileyok](https://github.com/haileyok))
- Replace `react-scripts` with `rsbuild`/`rspack` for UI builds ([#235](https://github.com/roostorg/osprey/pull/235) by [@chimosky](https://github.com/chimosky))
- Migrate from npm to pnpm via Corepack ([#252](https://github.com/roostorg/osprey/pull/252) by [@haileyok](https://github.com/haileyok))

### Fixed

- Escape literal braces when parsing f-strings in the engine ([#347](https://github.com/roostorg/osprey/pull/347) by [@haileyok](https://github.com/haileyok))
- Tolerate malformed URI escapes in `EntityWithPopover` UI component ([#377](https://github.com/roostorg/osprey/pull/377) by [@julietshen](https://github.com/julietshen))
- Add retention limits to Kafka topics to prevent unbounded disk growth ([#249](https://github.com/roostorg/osprey/pull/249) by [@VINODvoid](https://github.com/VINODvoid))
- Fix failed UDF query ([#233](https://github.com/roostorg/osprey/pull/233) by [@chimosky](https://github.com/chimosky))
- Show the event-stream timezone once instead of twice; `zz` rendered a doubled abbreviation (e.g. `EDTEDT`) after the moment-to-dayjs migration ([#419](https://github.com/roostorg/osprey/pull/419) by [@julietshen](https://github.com/julietshen))

## [1.0.1] - 2026-02-27

1.0.1 is a small patch release follow-up to the [1.0 release](https://github.com/roostorg/osprey/releases/tag/1.0). It mostly consists of bug fixes, small improvements for flexibility, and updates to documentation and project/release management.

### Bug fixes

- Fix label expiry checks on reasons ([#149](https://github.com/roostorg/osprey/pull/149) by [@jeffhalmich](https://github.com/jeffhalmich))
- Add `safeparse` to string UDFs ([#125](https://github.com/roostorg/osprey/pull/125) by [@cmttt](https://github.com/cmttt))

### Enhancements

- Configurable execution result store ([#29](https://github.com/roostorg/osprey/pull/29) by [@haileyok](https://github.com/haileyok))
- Add `error_on_empty` parameter to `HasLabel` UDF ([#128](https://github.com/roostorg/osprey/pull/128) by [@cmttt](https://github.com/cmttt))
- Support `Optional` types in comparison operators (`<`, `>`, `<=`, `>=`) ([#146](https://github.com/roostorg/osprey/pull/146) by [@alphonsoc](https://github.com/alphonsoc))

### New Contributors

- [@alphonsoc](https://github.com/alphonsoc) in [#146](https://github.com/roostorg/osprey/pull/146)
- [@jeffhalmich](https://github.com/jeffhalmich) in [#149](https://github.com/roostorg/osprey/pull/149)
- [@juanmrad](https://github.com/juanmrad) in [#152](https://github.com/roostorg/osprey/pull/152)

## [1.0] - 2026-01-28

Osprey is an open-source rules engine for trust and safety operations, currently running in production at Discord and Bluesky, processing 45M-61M events daily at Bluesky alone. v1 expands the open-source release to include more of the infrastructure that powers those deployments, so teams adopting Osprey can run it in production without rebuilding these pieces themselves.

This release adds three major features: a flexible labeling service, a Rust-based coordinator for horizontal scaling, and a real-time event stream for investigations. As part of making it easier to get started, this release also includes a searchable documentation website: <https://roostorg.github.io/osprey/>

### Labeling Service

Added a labeling service to Osprey: a pluggable subsystem for attaching human and automated labels to any entity (users, IPs, emails, etc.). Labels feed into the rules engine and UI to improve detection and workflow.

- Manual labeling in the UI (positive/negative/neutral) for safety teams to annotate entities
- Programmatic label mutations produced by rule executions are persisted by the labels service
- Pluggable backend: implement `LabelsServiceBase` or provide a `LabelsProvider` to support different storage backends (Postgres plugin included)
- Atomic read-modify-write context for safe concurrent updates and retries
- Batch reads supported for performance during rule execution
- Conflict resolution: simultaneous or contradictory mutations are resolved by status priority; dropped mutations are reported back
- Rich label model: statuses (automatic vs manual), reasons (descriptions, features, created/expires), previous states, and label connotation (positive/negative/neutral)

### Coordinator

Introduced the Osprey Coordinator: a Rust-based action distribution and load-balancing service that centralizes action ingestion and routes work to Osprey workers for reliable horizontal scaling.

- Central action hub: accepts actions from Kafka/PubSub or directly via gRPC (bidirectional streaming and synchronous API)
- Load balancing and prioritization: maintains a priority queue (sync vs async) and routes actions to workers based on priority and hashring/discovery logic
- Reliable delivery: tracks outstanding actions, acknowledgements, and retries to ensure dependable processing
- Service discovery: integrates with etcd so workers and coordinators can discover each other for scalable deployments
- Metrics and observability: emits coordinator metrics to monitor throughput, latencies, and health
- Pluggable deployment: runs as a separate service (Rust binary and Dockerfile); included docker-compose example and startup flags to enable the coordinator
- Works with existing worker model: workers connect to the coordinator using long-lived gRPC streams to receive assigned actions

### Event Stream

Added the Event Stream: a real-time, searchable investigation feed in the Osprey UI that surfaces events, rule verdicts, and extracted features to speed investigations and monitoring.

- Live feed for real-time event monitoring and operator investigations
- Searchable historical events using SML queries for ad-hoc investigations and pattern discovery
- Built-in visibility into rule execution results and extracted features beside each event
- Configurable presentation: card vs list views and user-selectable summary features
- Improved integration with analytics and sinks so event metadata and classification data populate the stream
- UI components and APIs included to support deep dives (event detail pages and feature location endpoints)

### Contributors

Thanks to [@EXBreder](https://github.com/EXBreder), [@BinaryFiddler](https://github.com/BinaryFiddler), [@elijaharita](https://github.com/elijaharita), [@caidanw](https://github.com/caidanw), [@jaredmiller13](https://github.com/jaredmiller13), [@haileyok](https://github.com/haileyok), [@cmttt](https://github.com/cmttt), [@ryanprior](https://github.com/ryanprior), [@ThisIsMissEm](https://github.com/ThisIsMissEm), and [@julietshen](https://github.com/julietshen) for their continued contributions, and welcome to our new contributors:

- [@annebdh](https://github.com/annebdh) in [#66](https://github.com/roostorg/osprey/pull/66)
- [@bmuenzenmeyer](https://github.com/bmuenzenmeyer) in [#93](https://github.com/roostorg/osprey/pull/93)
- [@cassidyjames](https://github.com/cassidyjames) in [#97](https://github.com/roostorg/osprey/pull/97)
- [@rashmiraghunandan](https://github.com/rashmiraghunandan) in [#101](https://github.com/roostorg/osprey/pull/101)
- [@vinaysrao1](https://github.com/vinaysrao1) in [#120](https://github.com/roostorg/osprey/pull/120)

## [0.2] - 2026-01-12

This release includes an asset for the `osprey_rpc` library.

## [0.1] - 2025-11-14

Release pre-coordinator integration. Effectively the same feature-wise as the 1.0 release, but without the coordinator integration and other upstreamed improvements from the reintegration of Osprey into internal Discord tools.

See the [1.0 release notes](#10---2026-01-28) for a full description of the core Osprey feature set.

[Unreleased]: https://github.com/roostorg/osprey/compare/1.0.1...HEAD
[1.0.1]: https://github.com/roostorg/osprey/compare/1.0...1.0.1
[1.0]: https://github.com/roostorg/osprey/compare/0.2...1.0
[0.2]: https://github.com/roostorg/osprey/compare/0.1...0.2
[0.1]: https://github.com/roostorg/osprey/releases/tag/0.1
