# Releases

Osprey follows a lightweight release process so downstream users can depend on version tags (e.g. `1.0.1`) instead of commit hashes. The process may evolve as project usage grows.

There is currently no fixed release cadence; generally, releases are made when:

- Downstream users need an updated stable version tag,
- Meaningful changes have accumulated and CI is green, and/or
- A [security advisory](https://github.com/roostorg/osprey/security/) is addressed

We do not currently backport changes to or release patches for versions other than the latest.

## Versioning

Osprey uses [Semantic Versioning (SemVer)](https://semver.org/) following the MAJOR.MINOR.PATCH version format. In brief:

- **Patch releases** (x.y.**Z**): backward-compatible fixes or small improvements
- **Minor releases** (x.**Y**.z): new functionality or substantial improvements; changes to public API are backward-compatible
- **Major releases** (**X**.y.z): backward-incompatible changes to public API, or major feature or user interface overhauls

## Milestones

Osprey may use [milestones](https://github.com/roostorg/osprey/milestones) to help plan work. All resolved issues for the release should be attached to the milestone. If a pull request does not have an associated issue that it resolves, the PR itself should be included on the milestone as well. Dependabot PRs may be omitted.

Release milestones should not have any open issues or unmerged PRs attached once the release is made.

## Preparing a release

Before cutting a release, ensure:

- [ ] **CI is passing** for the `main` branch
- [ ] **You understand the correct version** according to SemVer
- [ ] **The milestone is up-to-date** with no remaining open issues or unmerged PRs
- [ ] **[CHANGELOG.md](https://github.com/roostorg/osprey/blob/main/CHANGELOG.md) is up-to-date** with notable changes under `[Unreleased]`

Then open a release-prep pull request to:

1. Add a release heading to CHANGELOG.md including the version number and date, leaving the “Unreleased” section empty above it

2. Update the compare links at the bottom of CHANGELOG.md: ensure the “Unreleased” link points to the comparison link between this release and main, and there's a new link for this release

**Merge this PR _before_ creating the tag** so the tagged commit includes the release's changelog.

This is a good time to draft the release notes, starting from CHANGELOG.md. See [Writing release notes](https://community.roost.tools/software-development-practices/releases.html) from the ROOST community site for recommendations on how to structure them.

## Creating a release

Once the release-prep pull request is merged (and CI is confirmed to still be passing), head to the GitHub repo:

1. **Releases** → **Draft a new release** (or edit your existing draft)
2. **Create a tag** in SemVer format `x.y.z` from the `main` branch
3. **Title the release** with the project name and version, e.g. "Osprey 1.2.0"
4. **Paste the drafted release notes**
5. Check **Create a discussion for this release** for at least major and minor releases
6. **Publish** the release
7. **Close the milestone**

Publishing a release triggers automations:

- **osprey-rpc**: builds and attaches sdist (and zip) to the release
- **Osprey Coordinator**: builds and pushes Docker image to GHCR with version tags
- **Docs**: adds a folder for the release to the [index](https://roostorg.github.io/osprey/) with a snapshot of the docs

For larger releases, consider coordinating a non-technical announcement on the [ROOST blog](https://roost.tools/blog/) to link to from the release notes. If a blog post is published after the release, you can edit the release notes to point to it later.
