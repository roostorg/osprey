# Security releases

A security release is a release that ships a fix for a vulnerability in Osprey or in one of its dependencies. It follows [Creating a release](releases.md#creating-a-release), with the extra steps on this page for keeping details private, publishing an advisory and notifying adopters. ROOST's overall process is on the [Security](https://community.roost.tools/software-development-practices/security) page of the community repo.

We do not currently backport changes to or release patches for versions other than the latest.

The scope tiers, response targets and communication rules below come from [RFC: Security vulnerability discovery and response process](https://github.com/roostorg/community/issues/119). They are proposals, not commitments, until that RFC is accepted.

| Tier | What's in it | Response |
| --- | --- | --- |
| 1. Shipped runtime | Packages and OS libraries in the `osprey-coordinator` image, and the `osprey-rpc` sdist attached to each release | Fix, then a security release |
| 2. Pinned dependencies | Third-party images and versions pinned in `docker-compose.yaml` and its overlays (e.g. Postgres, Kafka, Druid) | Bump the pin to a fixed version and release |
| 3. Adopter-managed services | Services adopters run themselves, where Osprey only documents supported versions | Raise the documented minimum version |
| 4. Build and CI | Dev dependencies, build tools, GitHub Actions | Fix in a normal release; review for supply-chain risk |

Osprey publishes only the coordinator image. There is no published worker or UI image: adopters build those from the repository, so a fix in the worker or UI reaches them through the release tag and their own rebuild.

## Before you start

- [ ] Every vulnerability going into the release has been triaged: the reported one, and every upstream advisory fixed by a dependency update in the release (see [Finding a release's security fixes](#finding-a-releases-security-fixes)). For each, record which released versions ship it, whether it is reachable from Osprey's code, whether it is exploitable in the documented default configuration, and whether it is [known exploited](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) or has a high EPSS score. Reproduce it against Osprey where feasible.
- [ ] Each reachable vulnerability has a draft [security advisory](https://github.com/roostorg/osprey/security/advisories/new), scored for how Osprey uses the affected code rather than copied from the upstream score. So does every vulnerability in Osprey's own code. Upstream advisories that are not reachable get no Osprey advisory; the release notes list them as precautionary updates.
- [ ] You know which path in step 1 applies.

Proposed response targets:

| Situation | Triage | Fix or mitigate |
| --- | --- | --- |
| Critical/high, tier 1 or 2, reachable or known exploited | 1 business day | 72 hours |
| Critical/high, tier 1 or 2, reachability unclear | 1 business day | 7 days |
| Critical/high, tier 3 | 2 business days | Update the documented minimum within 7 days |
| Moderate, or tier 4 | Weekly | Next scheduled release |
| Low, or not reachable | Weekly | Dismissed with a reason |

Reachability and exploitability details stay in the draft advisory or the private triage queue until the fix is released. Never put them in a public issue, PR, commit message or changelog entry.

The steps run in this order: choose the release base (step 2) before merging anything from a private fork; then merge the fix, publish the release, publish the advisories, and announce.

### Finding a release's security fixes

<!-- TODO(roostorg/coop#1520 item 1): replace with the `security` label query once Osprey has an equivalent labeling workflow. -->

List the PRs merged since the last release and pick out the security fixes and dependency updates:

```bash
gh pr list --state merged --base main --search "merged:>YYYY-MM-DD" --json number,title,labels
```

For each dependency update, read the release notes or advisories of every package version it changes.

## 1. Prepare the fix

### Undisclosed vulnerability: private fork

Use this path for a vulnerability in Osprey's own code, or any issue not yet public.

1. Choose the release base (step 2). On option B, create `release/x.y` first if it does not exist.
2. In the draft advisory, select **Start a temporary private fork**.
3. Clone the fork and open a PR against it with the fix, a regression test, and the version and changelog changes from step 3, so that a single merge makes everything public. Target `main` on option A, or `release/x.y` on option B.
4. GitHub Actions does not run in temporary private forks. Run the [CI checks](https://github.com/roostorg/osprey/blob/main/AGENTS.md#ci) locally (at least `uv run pre-commit run --all-files` and `./run-tests.sh`) and record the results in the PR.
5. Once the PR is reviewed, select **Merge pull request** in the advisory. The fix is public from this point, so publish the release (step 4) straight after.

### Public upstream CVE: normal PR

Use this path when the vulnerability is already public, e.g. a CVE in a dependency or base image. Open a normal PR against `main` that bumps the dependency or pin, and add a `### Security` changelog entry for it under `## [Unreleased]`. The PR can name the upstream CVE, but not Osprey-specific reachability or exploitability details. [#541](https://github.com/roostorg/osprey/pull/541) (`certifi`, released in 1.2.1) is an example.

If Dependabot has already opened the bump, use its PR. If its PR groups the security update with other updates, release only the security update (on option B, a separate PR to `release/x.y` that applies the same specifier and version as `main`).

## 2. Choose the release base

If `## [Unreleased]` in `CHANGELOG.md` contains only the fix, release from `main` as usual and skip to step 3.

If `main` has unreleased changes, choose one of the options below. **Pending decision ([roostorg/coop#1520](https://github.com/roostorg/coop/issues/1520) item 11):** option B as a standing model is a proposal; until it is decided, agree the base with a maintainer for each release.

**A. Release from `main`.** Ship the fix together with everything already on `main`. This is how 1.2.1 was released.

- Uses the existing release process and workflows unchanged.
- The release contains changes that were not planned for release yet. If `## [Unreleased]` has entries under `### Added`, or any backward-incompatible change, the next version is a minor or major release, not a patch, under [Versioning](releases.md#versioning).
- Unreleased behavior and configuration changes reach adopters who only wanted the security fix.

**B. Release from `release/x.y`.** Ship only the fixes as `x.y.(z+1)`, from a release branch kept for the latest minor version.

- Create the branch once per minor version, from its first release tag: `git switch -c release/x.y x.y.0`, then push it to `origin`.
- Fixes land on `main` first, then are cherry-picked to `release/x.y` with `git cherry-pick -x`, one PR per cherry-picked commit. The exception is a private-fork fix, which merges into `release/x.y` directly and is forward-ported to `main` in step 8.
- The `Code Quality Checks` and `Integration Tests` workflows only run on PRs against `main`, so they don't run on PRs against `release/x.y`: run the [CI checks](https://github.com/roostorg/osprey/blob/main/AGENTS.md#ci) locally. `main`'s branch protection rules don't cover the branch either.
- A release runs the workflow files on the release branch, not on `main`. Cherry-pick any workflow fixes the release needs, e.g. [#545](https://github.com/roostorg/osprey/pull/545) for `release-osprey-rpc.yml` on `release/1.2`.
- A `release/x.y` branch cut before [#551](https://github.com/roostorg/osprey/pull/551) merged has no `.github/workflows/e2e.yml` (or `e2e-smoke.sh`), so the smoke test won't run on it until they are cherry-picked.

### Cherry-picking deterministically

Dependency updates usually conflict in `uv.lock` (or `osprey_ui/pnpm-lock.yaml`). Resolve them so the branch keeps its own versions of everything except the packages being fixed:

1. Take the branch's lockfile, `git checkout --ours uv.lock`, and its `pyproject.toml` specifiers for anything outside the fix.
2. Set the fixed packages to the same specifier as `main`, then run `uv lock --upgrade-package <package>` to update the lockfile for them alone.
3. Run `uv lock --check` to confirm the lockfile is up to date, and `uv sync --locked --dev`.
4. Check that only the intended packages changed versions, e.g. `git diff origin/release/x.y -- uv.lock`.

For `osprey_ui/pnpm-lock.yaml`, the same steps use `pnpm install` to update the fixed packages, then `pnpm install --frozen-lockfile`, and confirm that `pnpm install` leaves the lockfile unchanged (`git diff --exit-code pnpm-lock.yaml`).

If a fix adds or bumps a dependency, the backport takes the same specifier and version as `main` as part of the fix, even when that dependency change is not itself a security update; approving the fix approves it, which is the human approval [`AGENTS.md`](https://github.com/roostorg/osprey/blob/main/AGENTS.md#dependencies) requires for new or upgraded packages. Check its license and known CVEs as usual, and name the dependency change in the backport PR. In the steps above, these count as packages being fixed.

<!-- TODO(roostorg/coop#1520 item 5): reference an Osprey equivalent of the lockfile check script once it lands. -->

## 3. Bump the version and changelog

Follow [Preparing a release](releases.md#preparing-a-release) with the patch version `x.y.z`, in a PR against the release base from step 2 (on the private fork path, in the fork PR). Each fix goes under `### Security` in the new `## [x.y.z]` section as a single line, in the format from [`AGENTS.md`](https://github.com/roostorg/osprey/blob/main/AGENTS.md#changelog):

```markdown
### Security

- Prevent <impact> ([GHSA-xxxx-xxxx-xxxx](https://github.com/roostorg/osprey/security/advisories/GHSA-xxxx-xxxx-xxxx), [#123](https://github.com/roostorg/osprey/pull/123) by [@user](https://github.com/user))
- Update <package> to <x.y.z> to fix <issue> ([CVE-…](https://github.com/advisories/GHSA-…), [#123](https://github.com/roostorg/osprey/pull/123) by [@user](https://github.com/user))
```

List an Osprey advisory before the PR link, and the upstream CVE when there is one. A fix from a private fork has no PR in this repository, so link only the advisory.

Add the fix to the release's [milestone](releases.md#milestones), creating it if needed. A private-fork fix can't be added until it is public.

## 4. Tag and publish the release

On option A, follow [Creating a release](releases.md#creating-a-release). The tag is SemVer with no `v` prefix, e.g. `1.2.2`.

On option B, create the release from the branch:

```bash
gh release create x.y.z --target release/x.y --title "Osprey x.y.z" --notes-file notes.md
```

CI must be green on the release commit (on option B, the local runs from step 2). Once the release end-to-end smoke test ([#551](https://github.com/roostorg/osprey/pull/551)) has merged, it gates every release, including patch and security releases: confirm the **E2E Smoke Test** run is green on the release commit. It runs on every push to `main` and `release/**`. If the run is missing or flaked, use **Run workflow** on the branch.

Actions does not run in temporary private forks, so on the private fork path the smoke test first runs on the public merge commit that **Merge pull request** pushes to `main` or `release/x.y`. Wait for that run to go green before publishing the release. If it fails, the fix is already public, so fix forward quickly.

Publishing the release triggers, from the workflow files at the tag:

- `.github/workflows/release-osprey-rpc.yml` on the `release` event, which builds the `osprey-rpc` sdist and attaches it to the release
- `.github/workflows/publish-coordinator-image.yml` on the `release` event, which pushes the `x.y.z`, `x.y` and `x` tags of `ghcr.io/roostorg/osprey/osprey-coordinator`. It does not move `latest`, which follows `main`.
- `.github/workflows/mdbook.yml` on the tag push, which adds the docs snapshot for the release

### Release notes template

```markdown
> [!IMPORTANT]
> **This release addresses <a reported security advisory | N reported security advisories>**: [GHSA-xxxx-xxxx-xxxx](https://github.com/roostorg/osprey/security/advisories/GHSA-xxxx-xxxx-xxxx) (<Severity>, CVSS <n.n>). <Upgrade wording>. For security announcements, we encourage adopters to subscribe to the [security-announce@roost.tools mailing list](https://groups.google.com/a/roost.tools/g/security-announce).

<One paragraph per advisory: the affected component, the upstream CVE if any, the affected versions, and the fixed version.>

<The risk, in one or two sentences, without exploitation details.>

Thanks to @<reporter> for reporting <GHSA-xxxx-xxxx-xxxx>.

## Other security updates

- **<package>** <x.y.z> fixes [CVE-…](https://github.com/advisories/GHSA-…) (<Severity>) ([#123](https://github.com/roostorg/osprey/pull/123))

### Upstream advisories that don't affect Osprey

The updates in this release also fix these upstream advisories. They're in code that Osprey doesn't use, so they don't affect Osprey:

- **<package>** <x.y.z> fixes [CVE-…](https://github.com/advisories/GHSA-…) (<Severity>) ([#123](https://github.com/roostorg/osprey/pull/123))

## Upgrade notes

- Coordinator image: `ghcr.io/roostorg/osprey/osprey-coordinator:x.y.z`.
- Worker and UI: rebuild your images from the `x.y.z` tag.
- `osprey-rpc`: the sdist attached to this release.
```

Leave out sections that do not apply. For upgrade wording, see [Upgrade wording](#upgrade-wording).

## 5. Confirm artifacts published

- [ ] The **Release osprey-rpc** run for the tag succeeded, and the release has the `osprey_rpc-*.tar.gz` and `.zip` assets: `gh release view x.y.z --json assets --jq '.assets[].name'`. This workflow failed on every release up to and including 1.2.1 and was fixed in [#545](https://github.com/roostorg/osprey/pull/545). It has no manual trigger; if it fails, build the sdist with `uv build --sdist --out-dir dist` in `osprey_rpc/` and attach it with `gh release upload x.y.z ...`.
- [ ] The **Build and Publish Osprey Coordinator Docker Image** run for the tag succeeded, and the image exists, e.g. `docker buildx imagetools inspect ghcr.io/roostorg/osprey/osprey-coordinator:x.y.z`. If it failed, re-run it, or run the workflow manually from the tag.
- [ ] The **Update docs with mdbook** run for the tag succeeded, and the release's docs are on the [docs site](https://roostorg.github.io/osprey/).
- [ ] If a tier 1 dependency or base image was fixed, the new coordinator image or sdist no longer contains the vulnerable version, using the image scan or SBOM attached to the release where available.

## 6. Publish the advisory

Publish each Osprey advisory after the release, so that its patched version exists. Tier 1 vulnerabilities that are not exploitable get only the `### Security` changelog entry, not an advisory.

### Advisory template

- **Title:** `<Impact> via <vector>`, with the upstream package in parentheses for a dependency, e.g. `Denial of service via malformed request bodies (<package>)`.
- **Affected product:** ecosystem left empty, package `osprey`, affected versions `< x.y.z` (narrower if older versions are not affected), patched versions `x.y.z`.
- **Severity:** CVSS scored for how Osprey uses the affected code.
- **Description**, with these sections:
  - `### Summary`: one or two sentences.
  - `### Details`: the affected component and why Osprey is affected.
  - `### Proof of concept`: the steps in prose. No exploit code.
  - `### Impact`: who can exploit it and what they get.
  - `### Patches`: the fixed version and the fixing change.
  - `### Workarounds`: mitigations for deployments that cannot upgrade yet, or "None".
  - `### References`: the upstream advisory and CVE, the release, and the fix.

### Before publishing

- [ ] The description is rewritten in the template's form; a reporter's draft is source material, not the published text.
- [ ] Affected and patched versions match the release.
- [ ] Credits are listed and every credited person has accepted.
- [ ] For a vulnerability in Osprey's own code, a CVE is requested from the advisory.
- [ ] The release and changelog link the advisory.

## 7. Notify

### Hosted vendors early notice

Vendors who host Osprey so that their users cannot self-patch can receive early notice (see [Security releases](https://community.roost.tools/software-development-practices/security#security-releases)).

- For an undisclosed vulnerability, tell them the release date and what to update before the release, under embargo.
- For an upstream CVE that is already public, tell them as soon as impact on Osprey is confirmed, without waiting for the release.

### security-announce@roost.tools

Post to [security-announce@roost.tools](https://groups.google.com/a/roost.tools/g/security-announce) once the release and its advisories are published. The email lists only Osprey advisories; upstream advisories that do not affect Osprey stay in the release notes.

| Situation | Advisory | security-announce | Changelog |
| --- | --- | --- | --- |
| Tier 1, exploitable | Yes | Yes | `Security` |
| Tier 1, not exploitable | No | No | `Security` |
| Tier 2 | No | Yes: pull the new `docker-compose.yaml`, or update the image tag in your own deployment | `Security` |
| Tier 3 | No | Yes: the new minimum version and upgrade guidance | `Security` |
| Tier 4 | No | No | If notable |

### Upgrade wording

**Pending decision ([roostorg/coop#1520](https://github.com/roostorg/coop/issues/1520) item 18):**

- An advisory rated High or Critical: "All deployments should upgrade."
- Otherwise: "We recommend you upgrade."

Don't qualify the wording by exposure (e.g. "if your UI is public").

### Email templates

One advisory:

```text
Subject: Osprey x.y.z security release

Osprey x.y.z fixes a security vulnerability:

- <Advisory title>: https://github.com/roostorg/osprey/security/advisories/GHSA-xxxx-xxxx-xxxx (<Severity>, CVSS <n.n>)

<Upgrade wording>

Release notes and artifacts: https://github.com/roostorg/osprey/releases/tag/x.y.z
```

More than one advisory:

```text
Subject: Osprey x.y.z security release

Osprey x.y.z fixes N security vulnerabilities:

- <Advisory title>: https://github.com/roostorg/osprey/security/advisories/GHSA-xxxx-xxxx-xxxx (<Severity>, CVSS <n.n>)
- <Advisory title>: https://github.com/roostorg/osprey/security/advisories/GHSA-yyyy-yyyy-yyyy (<Severity>, CVSS <n.n>)

<Upgrade wording, for the highest severity listed>

Release notes and artifacts: https://github.com/roostorg/osprey/releases/tag/x.y.z
```

No advisory (tier 2 or 3):

```text
Subject: Osprey x.y.z: <component> security update

Osprey x.y.z updates <component> to <version>, which fixes <CVE-…>. <What adopters need to do, e.g. pull the new docker-compose.yaml, or the new minimum version>.

Release notes: https://github.com/roostorg/osprey/releases/tag/x.y.z
```

## 8. Forward-port to `main`

On option B, open a PR to `main` that brings over the release:

- Add the `## [x.y.z]` section and its compare link to `CHANGELOG.md` below `## [Unreleased]`, matching the release notes, and remove from `## [Unreleased]` the entries that shipped in it.
- Cherry-pick any fix that merged into `release/x.y` from a private fork and is not yet on `main`.

## Checklist

- [ ] Every vulnerability in the release triaged privately; reachable ones have draft advisories
- [ ] Release base agreed (`main` or `release/x.y`) before any private-fork merge
- [ ] Fix merged (private fork or normal PR), with a regression test where possible; cherry-picks change only the fixed packages in `uv.lock` / `pnpm-lock.yaml`
- [ ] Changelog `### Security` entries and version bump merged
- [ ] CI and the E2E smoke test (once #551 merges) green on the release commit; on the private fork path, on the public merge commit
- [ ] Release `x.y.z` published from the agreed base, with a no-`v` SemVer tag
- [ ] `osprey-rpc` sdist attached, coordinator image and docs snapshot confirmed published
- [ ] Advisories published after the pre-publish checks
- [ ] Hosted vendors notified
- [ ] security-announce posted, if required
- [ ] If released from `release/x.y`: release forward-ported to `main`
