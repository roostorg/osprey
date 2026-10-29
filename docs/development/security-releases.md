# Security releases

This runbook covers shipping a fix for a vulnerability that affects Osprey, either in Osprey's own code or in a dependency, and telling adopters about it. It extends the normal [release process](releases.md); where a step is the same as a normal release, it links there instead of repeating it.

It follows the dependency vulnerability process proposed in [roostorg/community#119](https://github.com/roostorg/community/issues/119) and the [ROOST security process](https://community.roost.tools/software-development-practices/security). The scope tiers, response targets and communication rules below are **proposals** from that RFC, not commitments, and may change as it is discussed.

| Tier | What's in it, for Osprey | What we do |
| --- | --- | --- |
| 1. Shipped runtime | Packages and OS libraries in what we publish: the `osprey-rpc` sdist and the `osprey-coordinator` image | Fix, then a security release |
| 2. Pinned dependencies | Third-party images and versions we pin in our compose files and examples (e.g. Postgres, Kafka, Druid) | Bump the pin to a fixed version and release; tell adopters to pull it |
| 3. Adopter-managed services | Services adopters run themselves, where we only document supported versions | Raise the documented minimum version and tell adopters which versions to upgrade |
| 4. Build and CI | Dev dependencies, build tools, GitHub Actions | Fix in a normal release; review for supply-chain risk |

Osprey publishes only the coordinator image. There is no published worker or UI image: adopters build those from the repository (or from the compose files), so a fix in the worker or UI reaches them through the release tag and their own rebuild, not through a new image.

## Before you start

- [ ] **Triage is done.** The on-duty maintainer has answered the four triage questions from the RFC (do we ship the vulnerable version, and in which releases; is it reachable; is it exploitable in the default configuration; is it known exploited or likely to be) and the outcome is "fix now".
- [ ] **You know which path you're on:**
  - **Undisclosed vulnerability** (reported to us privately, or found by us, and not yet public): use the [private fork](#undisclosed-vulnerability-private-fork) path.
  - **Public upstream CVE** (already published by the upstream project): use a [normal PR](#public-upstream-cve-normal-pr). There is no embargo to protect.

Proposed response targets, from the RFC:

| Situation | Triage | Fix or mitigate | Communication |
| --- | --- | --- | --- |
| Critical/high, tier 1 or 2, reachable or known exploited | 1 business day | 72 hours | Security release, GHSA, security-announce |
| Critical/high, tier 1 or 2, reachability unclear | 1 business day | 7 days | Security release; GHSA if impact confirmed |
| Critical/high, tier 3 | 2 business days | Update the documented minimum within 7 days | security-announce note with upgrade guidance |
| Moderate, or tier 4 | Weekly | Next scheduled release | Changelog |
| Low, or not reachable | Weekly | Dismissed with a reason | None |

Moderate, low and tier 4 items don't need this runbook: they go out in the next normal release with a changelog entry.

Reachability and exploitability details stay in the draft advisory or the private triage queue until the fix is released. Never put them in a public issue, PR, commit message or changelog entry.

## 1. Prepare the fix

### Undisclosed vulnerability: private fork

1. In **Security** → **Advisories**, open a **draft security advisory** (or use the one created from the private report).
2. In the draft advisory, **Start a temporary private fork**, and push the fix there on a branch.
3. Open a pull request from the private fork's branch, and get it reviewed there.
4. GitHub Actions don't run in temporary private forks, so run the relevant checks locally before merging (see [CI](https://github.com/roostorg/osprey/blob/main/AGENTS.md#ci) in `AGENTS.md`), at least:

   ```bash
   uv run pre-commit run --all-files
   ./run-tests.sh
   ```

5. Make the [version and changelog changes](#3-bump-the-version-and-changelog) in the private fork PR too, instead of a separate public release-prep PR, so nothing about the fix is public before the release.
6. **Merge pull request(s)** from the advisory page only when you are ready to release: merging makes the fix public on `main`. Tag and publish the release (step 4) right after.

If the vulnerability isn't public yet, request a CVE from the draft advisory (see [step 6](#6-publish-the-advisory)).

### Public upstream CVE: normal PR

Open an ordinary pull request against `main` that bumps the dependency (often this is a Dependabot PR) and let CI run. Describe the change as a dependency bump with the upstream advisory link; leave our own reachability analysis out of the PR. See [#541](https://github.com/roostorg/osprey/pull/541) (`certifi`, released in 1.2.1) for an example.

## 2. Choose the release base

If `main` has no unreleased changes since the latest tag, release from `main` as usual and skip to step 3.

If `main` has unreleased changes (check the `## [Unreleased]` section of [CHANGELOG.md](https://github.com/roostorg/osprey/blob/main/CHANGELOG.md), or `git log <latest-tag>..origin/main`), there are two options. **Which one Osprey uses is a maintainer decision that hasn't been made yet**; pick per release until it is.

**A. Release from `main`.** Merge the fix to `main` and cut the release from `main`, carrying the unreleased changes with it. This is the current documented process ("we do not currently backport changes"), and how 1.2.1 was released.

- Simplest: one branch, every CI workflow runs as usual.
- The release includes unrelated changes, so the version may have to be a minor (or major) release rather than a patch, and adopters who only want the fix must take the rest too. Check the `## [Unreleased]` entries against SemVer before choosing the version.

**B. Branch from the latest tag.** Create a branch from the latest release tag (e.g. `git switch -c release-1.2 1.2.1`), apply the fix there (cherry-pick it from `main`, or apply the same patch), and release a patch from that branch.

- Adopters get only the fix, as a patch release.
- The `Code Quality Checks` and `Integration Tests` workflows only run on pull requests to `main` and pushes to `main`, so they won't run on the release branch: run them locally (see [CI](https://github.com/roostorg/osprey/blob/main/AGENTS.md#ci) in `AGENTS.md`) before tagging.
- After the release, open a follow-up PR to `main` that cherry-picks the fix and adds the release's CHANGELOG section, so the next release's changelog stays complete.
- This is a backport, which `releases.md` currently says we don't do; adopting this option changes that policy.

## 3. Bump the version and changelog

Use the release-prep pull request from [Preparing a release](releases.md#preparing-a-release) (against the release base from step 2); for an undisclosed vulnerability, make these changes in the private fork PR instead. Either way, with these additions:

- **Version:** a patch release (`x.y.Z`) under option B, or under option A when the unreleased changes allow it; otherwise the next minor.
- **Changelog:** add the fix under `### Security` in the new release's section (adding the heading if it's missing). Keep it to one line naming the dependency, the versions, and the advisory, for example:

  ```markdown
  ### Security

  - Upgrade `certifi` from 2026.5.20 to 2026.10.19 to drop the compromised emSign root CA ([GHSA-6q48-6c7w-9gw2](https://github.com/advisories/GHSA-6q48-6c7w-9gw2), [#541](https://github.com/roostorg/osprey/pull/541) by [@reitblatt](https://github.com/reitblatt))
  ```

  For an undisclosed vulnerability, link our own advisory instead. Under the proposed communication rules, a tier 1 fix that isn't exploitable gets this changelog entry and no advisory.

- **Milestone:** create a milestone for the release (e.g. `1.2.2`) if there isn't one, and attach the fix and the release-prep PR, as in [Milestones](releases.md#milestones). For an undisclosed vulnerability, the private fork PR can't be attached; attach the merged commit's PR once it is public, or leave the milestone empty.

## 4. Tag and publish the release

Follow [Creating a release](releases.md#creating-a-release), with these differences:

- **Gate:** CI must be green on the commit you tag. Under option B that means the local runs from step 2. Once the release end-to-end smoke test ([#551](https://github.com/roostorg/osprey/pull/551)) has merged, it is a gate for security releases as well: confirm it is green on the release commit.
- **Tag:** create the tag in SemVer format with no `v` prefix (e.g. `1.2.2`), targeting `main` under option A or the release branch under option B.
- **Release notes:** start with a callout saying it is a security release and who should upgrade, and link the advisory. The [1.2.1 release](https://github.com/roostorg/osprey/releases/tag/1.2.1) is an example. Point readers at [security-announce@roost.tools](https://groups.google.com/a/roost.tools/g/security-announce).
- **Discussion:** optional for a patch release.

## 5. Confirm artifacts published

Publishing the release triggers three workflows. Check each one finished, from **Actions** or with `gh run list --event release --limit 5`:

- [ ] **`osprey-rpc` sdist** ([`release-osprey-rpc.yml`](https://github.com/roostorg/osprey/blob/main/.github/workflows/release-osprey-rpc.yml)): the `osprey_rpc-*.tar.gz` and `.zip` are attached to the release. Check with `gh release view <tag> --json assets --jq '.assets[].name'`. This workflow failed on every release up to and including 1.2.1 and was fixed in [#545](https://github.com/roostorg/osprey/pull/545), so check the assets rather than assuming. It has no manual trigger: if it fails, fix the workflow and build and attach the sdist by hand (`uv build --sdist --out-dir dist` in `osprey_rpc/`, then `gh release upload <tag> ...`).
- [ ] **Coordinator image** ([`publish-coordinator-image.yml`](https://github.com/roostorg/osprey/blob/main/.github/workflows/publish-coordinator-image.yml)): `ghcr.io/roostorg/osprey/osprey-coordinator` has the `x.y.z`, `x.y` and `x` tags for the release. The `latest` tag follows `main`, not releases. If the run failed, re-run it, or run the workflow manually from the release tag.
- [ ] **Docs snapshot** ([`mdbook.yml`](https://github.com/roostorg/osprey/blob/main/.github/workflows/mdbook.yml)): the tag push builds a docs folder for the release on the [docs site](https://roostorg.github.io/osprey/).

If image scanning and SBOM generation have been added to the publish workflows, also confirm the release's SBOM is attached and the image scan no longer reports the vulnerability.

## 6. Publish the advisory

Under the proposed communication rules, publish a GitHub Security Advisory (GHSA) for tier 1 vulnerabilities that are exploitable in Osprey. A non-exploitable tier 1 fix, and tier 2–4 fixes, get no advisory.

In **Security** → **Advisories**, create (or finish) the advisory for Osprey:

- **Affected versions:** the Osprey releases that shipped the vulnerable dependency or code (e.g. `<= 1.2.1`), from triage question 1.
- **Patched versions:** the release from step 4.
- **Description:** what the vulnerability is, who is affected, and what to do (upgrade; for adopters who build their own worker or UI images, rebuild from the new tag). Now that the fix is released, include the reachability and exploitability summary.
- **References:** for an upstream issue, the upstream CVE and advisory; it is not our CVE to request. For a vulnerability in Osprey itself, request a CVE from the draft advisory before publishing.

Then **Publish advisory**.

## 7. Notify

### Hosted vendors early notice

Vendors offering Osprey as a hosted service that users can't patch themselves are eligible for early notice of security releases (see the [ROOST security process](https://community.roost.tools/software-development-practices/security); vendors can email hello@roost.tools for details).

- **Undisclosed vulnerability:** tell eligible vendors the planned release date before you merge and release.
- **Public upstream CVE:** there is no embargo, so tell them as soon as impact on Osprey is confirmed, without waiting for the release.

### security-announce@roost.tools

Post to [security-announce@roost.tools](https://groups.google.com/a/roost.tools/g/security-announce) after the release is published, linking the release and the advisory (if any), and saying who needs to act and how.

What to send, by tier (proposed in the RFC):

| Tier | Advisory | security-announce |
| --- | --- | --- |
| 1, exploitable | GHSA for the affected Osprey versions, referencing the upstream CVE | Yes: link the release and the GHSA |
| 1, not exploitable | None; `### Security` changelog entry only | No |
| 2 | None | Yes: tell adopters to pull the new compose files, or update the image tag in their own deployment |
| 3 | None | Yes: the new minimum version and upgrade guidance |
| 4 | None | No; fixed in a normal release |

## Checklist

- [ ] Triage done; private or public path chosen
- [ ] Fix prepared (private fork or normal PR), checks pass
- [ ] Release base chosen (from `main` or from the latest tag)
- [ ] Release-prep PR: version, `### Security` changelog entry, milestone
- [ ] Hosted vendors notified (before release for undisclosed; on confirmed impact for public CVEs)
- [ ] Fix merged; CI (and the E2E smoke test, once merged) green on the release commit
- [ ] Release published with a no-`v` SemVer tag
- [ ] `osprey-rpc` sdist attached, coordinator image tags on GHCR, docs snapshot built
- [ ] Advisory published (tier 1, exploitable)
- [ ] security-announce sent (per the tier table)
- [ ] Milestone closed; fix and changelog on `main` (option B)
