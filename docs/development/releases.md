# Releases

Osprey follows a lightweight, bootstrap release process so downstream users can depend on version tags (e.g. `1.0.1`) instead of commit hashes. The process may evolve as project usage grows. There is currently no fixed release cadence; releases are made when:

- Downstream users need an updated stable version tag, or
- Meaningful changes have accumulated and CI is green

## Versioning

Osprey uses [Semantic Versioning (SemVer)](https://semver.org/) following the MAJOR.MINOR.PATCH version format. In brief:

- **Patch releases** (x.y.**Z**): backward-compatible fixes or small improvements
- **Minor releases** (x.**Y**.z): new functionality or substantial improvements; changes to public API are backward-compatible
- **Major releases** (**X**.y.z): backward-incompatible changes to public API, or major feature or user interface overhauls

## Creating a release

Before cutting a release, ensure:

- [ ] **Code quality CI is passing** for the `main` branch
- [ ] **You understand the correct version** according to SemVer
- [ ] **[CHANGELOG.md](https://github.com/roostorg/osprey/blob/main/CHANGELOG.md) is up-to-date**

Then:

1. In GitHub: **Releases** → **Draft a new release**
2. **Create a tag** in SemVer format `x.y.z` from the `main` branch
3. **Draft the release notes**, starting from [CHANGELOG.md](https://github.com/roostorg/osprey/blob/main/CHANGELOG.md)
4. Check **Create a discussion for this release** for at least major and minor releases
5. **Publish** the release

Publishing a release triggers automations:

- **osprey-rpc**: builds and attaches sdist (and zip) to the release
- **Osprey Coordinator**: builds and pushes Docker image to GHCR with version tags
- **Docs**: adds a folder for the release to the [index](https://roostorg.github.io/osprey/) with a snapshot of the docs  
