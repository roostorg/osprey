# Changelog

All notable changes to Osprey will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 📚 3rd party library updates
- Upgrade `protobuf` from 4.25.8 to 5.29.6 ([#317](https://github.com/roostorg/osprey/issues/317), [#XXX](https://github.com/roostorg/osprey/pull/XXX)); as a downstream consequence, also upgrade 
  - `grpcio` from 1.49.1/1.53.x to 1.74.0
  - `grpcio-tools` from 1.49.1/1.53.x to 1.71.2
  - `google-cloud-logging` from 3.4.0 to 3.12.0
  - `google-cloud-pubsub` from 2.15.2 to 2.38.0
  - `tink` from 1.9.0 to 1.15.0
  - relax `grpcio-health-checking`, `grpcio-reflection`, and `grpcio-status` from exact pins to floor constraints
  - regenerate all gRPC bindings
- Upgrade `simplejson` from 3.19.3 to 3.20.2 ([#174](https://github.com/roostorg/osprey/pull/174) by [@dependabot](https://github.com/dependabot))
- Upgrade `requests-mock` from 1.9.3 to 1.12.1 ([#176](https://github.com/roostorg/osprey/pull/176) by [@dependabot](https://github.com/dependabot))
- Upgrade `antd` and `@ant-design/icons` to v5 (Ant Design 5 migration) ([#238](https://github.com/roostorg/osprey/pull/238) by [@haileyok](https://github.com/haileyok))
- Replace `moment` with `dayjs` ([#238](https://github.com/roostorg/osprey/pull/238) by [@haileyok](https://github.com/haileyok))
- Add project-scoped Playwright MCP registration (`@playwright/mcp@0.0.73` via `npx`) so Claude Code can drive a browser against `osprey_ui` for ad-hoc UI verification ([#243](https://github.com/roostorg/osprey/pull/243) by [@haileyok](https://github.com/haileyok))
- Replace react-scripts with rsbuild/rspack ([#235](https://github.com/roostorg/osprey/pull/235) by [@chimosky](https://github.com/chimosky))

### 🛠 Breaking changes

### 🎉 New features
- Add Postgres execution result store ([#171](https://github.com/roostorg/osprey/pull/171) by [@serendipty01](https://github.com/serendipty01))
- Add `ParseInt` UDF — converts a numeric string to an integer ([#190](https://github.com/roostorg/osprey/pull/190) by [@bealsbe](https://github.com/bealsbe))
- Add `StringSlice` UDF which extracts a substring by index range ([#189](https://github.com/roostorg/osprey/pull/189) by [@bealsbe](https://github.com/bealsbe))
- Add `InExperiment` UDF which checks if an entity is in an experiment ([#203](https://github.com/roostorg/osprey/pull/203) by [@bealsbe](https://github.com/bealsbe))
- Add Rules Registry page at `/rules` ([#277](https://github.com/roostorg/osprey/pull/277) by [@haileyok](https://github.com/haileyok))

### 🐛 Bug fixes
- Default to selecting all for event stream ([#194](https://github.com/roostorg/osprey/pull/194) by [@chimosky](https://github.com/chimosky))
- Fix failed UDF query ([#233](https://github.com/roostorg/osprey/pull/233) by [@chimosky](https://github.com/chimosky))
