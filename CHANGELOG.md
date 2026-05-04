# Changelog

All notable changes to Osprey will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 📚 3rd party library updates
- Upgrade `simplejson` from 3.19.3 to 3.20.2 ([#174](https://github.com/roostorg/osprey/pull/174) by [@dependabot](https://github.com/dependabot))
- Upgrade `requests-mock` from 1.9.3 to 1.12.1 ([#176](https://github.com/roostorg/osprey/pull/176) by [@dependabot](https://github.com/dependabot))
- Upgrade `antd` and `@ant-design/icons` to v5 (Ant Design 5 migration) ([#238](https://github.com/roostorg/osprey/pull/238) by [@haileyok](https://github.com/haileyok))
- Replace `moment` with `dayjs` ([#238](https://github.com/roostorg/osprey/pull/238) by [@haileyok](https://github.com/haileyok))
- Add `@playwright/mcp` devDep + project-scoped MCP registration so Claude Code can drive a browser against `osprey_ui` for ad-hoc UI verification ([#243](https://github.com/roostorg/osprey/pull/243) by [@haileyok](https://github.com/haileyok))

### 🛠 Breaking changes

### 🎉 New features
- Add Postgres execution result store ([#171](https://github.com/roostorg/osprey/pull/171) by [@serendipty01](https://github.com/serendipty01))
- Add `ParseInt` UDF — converts a numeric string to an integer ([#190](https://github.com/roostorg/osprey/pull/190) by [@bealsbe](https://github.com/bealsbe))
- Add `StringSlice` UDF which extracts a substring by index range ([#189](https://github.com/roostorg/osprey/pull/189) by [@bealsbe](https://github.com/bealsbe))
- Add `InExperiment` UDF which checks if an entity is in an experiment ([#203](https://github.com/roostorg/osprey/pull/203) by [@bealsbe](https://github.com/bealsbe))

### 🐛 Bug fixes
- Default to selecting all for event stream ([#194](https://github.com/roostorg/osprey/pull/194) by [@chimosky](https://github.com/chimosky))
