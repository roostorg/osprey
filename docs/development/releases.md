# Release Process

Osprey uses [Semantic Versioning](https://semver.org/) (SemVer) with a 1.x.y series. This is a lightweight, bootstrap release process so downstream users can depend on version tags instead of commit hashes. The process may evolve as project usage grows.

## Patch releases (1.x.y)

Patch releases are backward-compatible fixes or small improvements already merged to `main`.

Cut a patch release when:

- Downstream users need a stable version tag, or
- Meaningful fixes have accumulated and CI is green

There is no fixed cadence; releases are event-driven.

## Patch release checklist

Before cutting a release:

- [ ] Code quality CI passing on `main` (see [Code Quality Checks](https://github.com/roostorg/osprey/actions))
- [ ] No breaking changes
- [ ] [CHANGELOG.md](https://github.com/roostorg/osprey/blob/main/CHANGELOG.md) updated (if applicable)

## How to cut a release

1. Ensure the checklist above is satisfied.
2. In GitHub: **Releases** → **Draft a new release**.
3. Choose or create a tag `X.Y.Z` (e.g. `1.0.1`) from `main`.
4. Publish the release.

Publishing the release triggers existing automation:

- **osprey-rpc**: build and attach sdist (and zip) to the release ([release-osprey-rpc](https://github.com/roostorg/osprey/blob/main/.github/workflows/release-osprey-rpc.yml)).
- **Osprey Coordinator**: build and push Docker image to GHCR with version tags ([publish-coordinator-image](https://github.com/roostorg/osprey/blob/main/.github/workflows/publish-coordinator-image.yml)).

Downstreams can depend on version tags (e.g. `1.0.1`) instead of commit SHAs.
