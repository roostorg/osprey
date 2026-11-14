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

- [ ] Every vulnerability going into the release has been triaged: the reported one, and every upstream advisory fixed by a dependency update in the release (see [Finding a release's security fixes](#finding-a-releases-security-fixes)), including Dependabot alerts that closed when their fix merged. For each, record which released versions ship it, whether it is reachable from Osprey's code, whether it is exploitable in the documented default configuration, and whether it is [known exploited](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) or has a high EPSS score. Reproduce it against Osprey where feasible.
- [ ] Each reachable vulnerability has a draft [security advisory](https://github.com/roostorg/osprey/security/advisories/new), scored for how Osprey uses the affected code rather than copied from the upstream score. So does every vulnerability in Osprey's own code. Upstream advisories that are not reachable get no Osprey advisory; the release notes list them under **Other security updates**, without saying whether they affect Osprey.
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

<!-- TODO(roostorg/coop#1520 item 1): replace with a `security-update` label query once Osprey has an equivalent labeling workflow and label. -->

Only fixes already merged to `main`, or merged from an advisory fork, are eligible for a patch release; open PRs wait for the next release. Osprey has no label for security fixes yet, so list every PR merged to `main` since the last release `x.y.z` of the line being patched, and pick out the security fixes and dependency updates:

```bash
LAST=$(gh release view x.y.z --repo roostorg/osprey --json publishedAt --jq .publishedAt)
gh pr list --repo roostorg/osprey --state merged --base main --search "merged:>=${LAST%%T*}" --limit 200 --json number,title,labels
```

Without a tag, `gh release view` returns the repository's **Latest** release, which may belong to another line.

On option B, the date alone is not enough: a fix merged to `main` before `x.y.z` may not have shipped in it. Compare the fixes with what `release/x.y` already has, by the commits cherry-picked to it and by its lockfiles:

```bash
git log origin/release/x.y --grep 'cherry picked from commit' --format='%h %s'
git log origin/release/x.y --format='%h %s' | grep -i backport
git diff origin/release/x.y origin/main -- uv.lock osprey_ui/pnpm-lock.yaml
```

A backport squashed without its cherry-pick line has none; the second command finds it by its title, and its PR description names the source PR.

For each dependency update, read the release notes or advisories of every package version it changes. To find which updates are tier 1, check what depends on each package:

```bash
uv tree --invert --package <package>
```

A Python package counts as tier 1 only through `osprey_rpc`, whose sdist is attached to each release; packages used only by the worker, UI API or examples reach adopters through their own builds from the tag, and packages reached only through dev dependencies are tier 4. The coordinator's Rust dependencies are resolved when the image is built, because `Cargo.lock` is not committed; check them with the image scan or SBOM where available.

Dependabot closes an alert when its fix merges, so it no longer shows as open. List the alerts fixed since the last release and check that each was triaged:

```bash
gh api --paginate "repos/roostorg/osprey/dependabot/alerts?state=fixed&per_page=100" --jq ".[] | select(.fixed_at >= \"$LAST\") | [.number, .security_advisory.ghsa_id, .dependency.package.name] | @tsv"
```

## 1. Prepare the fix

### Undisclosed vulnerability: private fork

Use this path for a vulnerability in Osprey's own code, or any issue not yet public.

1. Choose the release base (step 2). On option B, create `release/x.y` first if it does not exist.
2. In the draft advisory, select **Start a temporary private fork**. Fine-grained personal access tokens cannot reach the fork, even with it added to the token. Use a short-lived classic token with the `repo` and `workflow` scopes, and revoke it when the release is done.
3. Bring the fork's target branch up to date. Its branches are a snapshot from when the fork was created, and **Sync fork** does not work on advisory forks, so push the upstream commit to it as a fast-forward, without force:

   ```bash
   git push https://github.com/roostorg/<fork>.git <upstream sha>:refs/heads/main
   ```

   Do the same for `release/x.y` on option B. The push is rejected without the `workflow` scope if the new commits change `.github/workflows/`.

4. Clone the fork and open a PR against it with the fix, a regression test, and the version and changelog changes from step 3, so that a single merge makes everything public. Target `main` on option A, or `release/x.y` on option B. If on option B you also want the `main` version reviewed in the fork, open it as a second PR and keep both in sync: add every commit to both before review. Only one of them can be merged from the advisory, so before merging, export the other with `git format-patch` for the forward-port in step 8.
5. GitHub Actions does not run in temporary private forks. Run the [CI checks](https://github.com/roostorg/osprey/blob/main/AGENTS.md#ci) locally (at least `uv run pre-commit run --all-files` and `./run-tests.sh`) and record the results in the PR. `main`'s required checks (`integration-tests`, `python-quality`, `rust-quality`) never report for the fork's merge, so merging into `main` needs a ruleset bypass. **Pending decision:** a narrow bypass for security releases, or a private mirror repository where Actions runs.
6. Before merging, update the fork's target branch again as in step 3, rebase the fix onto it, and rerun the local CI checks. The fork checks mergeability against its own stale branch, so a PR it shows as mergeable can still conflict upstream. Force-pushing the rebase does not dismiss existing approvals, so ask for a new review.
7. Copy anything you need from the fork PR, such as review comments and CI results, into the draft advisory. GitHub deletes the fork when the PR is merged.
8. Once the PR is reviewed, select **Merge pull request** in the advisory. The fix is public from this point, so publish the release (step 4) straight after.
9. The merge lands as one squashed commit titled "Merge commit from fork", with the PR's title only in the commit body, so `git log --oneline` does not show it as a security fix. Whoever merges it tells the release owner the commit SHA.

If a fork fix for option B merged into `main` instead of `release/x.y`, the fix is public from that merge, and the response clock runs from it. Cherry-pick it to `release/x.y` with `git cherry-pick -x` in a public backport PR:

- Title it `fix(<scope>): backport <sha> to release/x.y`, with the one-line description from step 2, nothing about the vulnerability.
- Add its `### Security` entry in the release-prep PR (step 3), linking only the advisory.
- Skip step 8's cherry-pick; the fix is already on `main`.

### Public upstream CVE: normal PR

Use this path when the vulnerability is already public, e.g. a CVE in a dependency or base image. Open a normal PR against `main` that bumps the dependency or pin, with a `fix` title (e.g. `fix(deps): update <package> to <x.y.z>`), and add a `### Security` changelog entry for it under `## [Unreleased]`, in the format from step 3. The PR can name the upstream CVE, but not Osprey-specific reachability or exploitability details. [#541](https://github.com/roostorg/osprey/pull/541) (`certifi`, released in 1.2.1) is an example.

If Dependabot has already opened the bump, use its PR. If its PR groups the security update with other updates, release only the security update (on option B, a separate PR to `release/x.y` that applies the same specifier and version as `main`).

## 2. Choose the release base

If `## [Unreleased]` in `CHANGELOG.md` contains only the fix, and nothing else has merged to `main` since the last release, release from `main` as usual and skip to step 3.

If `main` has unreleased changes, choose one of the options below. **Pending decision ([roostorg/coop#1520](https://github.com/roostorg/coop/issues/1520) item 11):** option B as a standing model is a proposal; until it is decided, agree the base with a maintainer for each release.

**A. Release from `main`.** Ship the fix together with everything already on `main`. This is how 1.2.1 was released.

- Uses the existing release process and workflows unchanged.
- The release contains changes that were not planned for release yet. If `## [Unreleased]` has entries under `### Added`, or any backward-incompatible change, the next version is a minor or major release, not a patch, under [Versioning](releases.md#versioning).
- Unreleased behavior and configuration changes reach adopters who only wanted the security fix.

**B. Release from `release/x.y`.** Ship only the fixes as `x.y.(z+1)`, from a release branch kept for the latest minor version.

- Create the branch once per minor version, from its first release tag: `git switch -c release/x.y x.y.0`, then push it to `origin`.
- Fixes land on `main` first, then are cherry-picked to `release/x.y` with `git cherry-pick -x`. The exception is a private-fork fix, which merges into `release/x.y` directly and is forward-ported to `main` in step 8.
- Clean `-x` cherry-picks of exact `main` commits were already reviewed on `main`, so batch them into one backport PR per release. Open a separate PR for each backport that does not map exactly to a `main` commit: one update split out of a Dependabot group, a lockfile regenerated with `uv lock --upgrade-package`, or a cherry-pick with conflicts resolved by hand.
- If a security fix adds or bumps a dependency, the backport takes that change as part of the fix, even when the dependency change is not itself a security update. It uses the same specifier and version as `main`, counts among the packages being fixed in the lockfile steps below, and approving the fix approves it without a separate dependency approval; that approval is the human approval [`AGENTS.md`](https://github.com/roostorg/osprey/blob/main/AGENTS.md#dependencies) requires for new or upgraded packages. Check its license and known CVEs as usual, and list the change under **Upgrade notes** if deployers need to know about it.
- **Pending decision:** a ruleset on `release/*` like Coop's, which requires a PR for every change, allows only rebase merges, requires the CI and E2E checks, blocks force pushes and branch deletion, and lets only its bypass actors create or update the branch. Osprey has no such ruleset yet, and `main`'s ruleset doesn't cover `release/x.y`.
- Open each backport PR with `gh pr create --base release/x.y`; `gh` defaults to `main`. Its description is one line, `Backporting PR #<n> to <x.y> security release branch` (or `Backporting <sha> …` for a fix with no PR), with no template sections; an agent may write this line.
- Merge backport PRs with **Rebase and merge**, so each commit keeps its `(cherry picked from commit …)` line. **Pending decision:** this repository currently allows only squash merges, with a blank commit body, so a squash merge drops that line. Until rebase merges are enabled (`main`'s ruleset would still allow only squash on `main`), paste each commit's `(cherry picked from commit …)` line into the squash commit's body by hand.
- The `Code Quality Checks` and `Integration Tests` workflows only run on PRs against `main` (until [#578](https://github.com/roostorg/osprey/pull/578) is on the branch), so they don't run on PRs against `release/x.y`: run the [CI checks](https://github.com/roostorg/osprey/blob/main/AGENTS.md#ci) locally and keep the results in the release log, to share with the reviewer on request.
- A release runs the workflow files at its tag, and PR checks run the workflow files on the PR's base branch, so fixes to `.github/workflows/` on `main` must also be cherry-picked to `release/x.y` before they apply to it, e.g. [#545](https://github.com/roostorg/osprey/pull/545) for `release-osprey-rpc.yml` on `release/1.2`.
- A `release/x.y` branch cut before [#551](https://github.com/roostorg/osprey/pull/551) merged has no `.github/workflows/e2e.yml` (or `e2e-smoke.sh`), so the smoke test won't run on it until they are cherry-picked.

### Cherry-picking deterministically

Dependency updates usually conflict in `uv.lock` (or `osprey_ui/pnpm-lock.yaml`). Resolve them so the branch keeps its own versions of everything except the packages being fixed:

1. Take the branch's lockfile, `git checkout --ours uv.lock`, and its `pyproject.toml` specifiers for anything outside the fix.
2. Set the fixed packages to the same specifier as `main`, then run `uv lock --upgrade-package <package>==<main's version>` to update the lockfile for them alone, at exactly `main`'s version.
3. Run `uv lock --check` to confirm the lockfile is up to date, and `uv sync --locked --dev`.
4. Check that only the intended packages changed versions, and that none is newer than `main`'s, e.g. `git diff origin/release/x.y -- uv.lock` and `git diff origin/main -- uv.lock`.

For `osprey_ui/pnpm-lock.yaml`, the same steps use `pnpm install` to update the fixed packages, then `pnpm install --frozen-lockfile`, and confirm that `pnpm install` leaves the lockfile unchanged (`git diff --exit-code pnpm-lock.yaml`).

<!-- TODO(roostorg/coop#1520 item 5): reference an Osprey equivalent of Coop's lockfile check script once it lands. -->

For a fix that only changes `uv.lock`, regenerating on the branch is simpler than resolving the cherry-pick: run `uv lock --upgrade-package <package>==<main's version>` for each fixed package, then steps 3 and 4 above, and end the commit message with `(cherry picked from commit <sha on main>)` by hand, as `git cherry-pick -x` would.

## 3. Bump the version and changelog

Follow [Preparing a release](releases.md#preparing-a-release) with the patch version `x.y.z`, in a PR against the release base from step 2 (on the private fork path, in the fork PR). Each fix goes under `### Security` in the new `## [x.y.z]` section as a single line, in the format from [`AGENTS.md`](https://github.com/roostorg/osprey/blob/main/AGENTS.md#changelog):

```markdown
### Security

- Prevent <impact> ([GHSA-xxxx-xxxx-xxxx](https://github.com/roostorg/osprey/security/advisories/GHSA-xxxx-xxxx-xxxx), [#123](https://github.com/roostorg/osprey/pull/123) by [@user](https://github.com/user))
- Update <package> to <x.y.z> to fix <issue> ([CVE-…](https://github.com/advisories/GHSA-…), [#123](https://github.com/roostorg/osprey/pull/123) by [@user](https://github.com/user))
```

List an Osprey advisory before the PR link, and the upstream CVE when there is one. A fix from a private fork has no PR in this repository, so link only the advisory.

Add the fix to the release's [milestone](releases.md#milestones), creating it if needed. A private-fork fix can't be added until it is public.

The release notes for step 4 are written from the [release notes template](#release-notes-template), using these entries as source material.

## 4. Tag and publish the release

CI must be green on the release commit (on option B, the local runs from step 2). Once the release end-to-end smoke test ([#551](https://github.com/roostorg/osprey/pull/551)) has merged, it gates every release, including patch and security releases: confirm the **E2E Smoke Test** run is green on the release commit. It runs on every push to `main` and `release/**`. If the run is missing or flaked, use **Run workflow** on the branch.

Actions does not run in temporary private forks, so on the private fork path the smoke test first runs on the public merge commit that **Merge pull request** pushes to `main` or `release/x.y`. Wait for that run to go green before publishing the release. If it fails, the fix is already public, so fix forward quickly.

On option A, follow [Creating a release](releases.md#creating-a-release), with the notes from step 3. The tag is SemVer with no `v` prefix, e.g. `1.2.2`.

On option B, first check that `release/x.y` holds exactly the reviewed backports, whatever merge method was used: its tree must match the reviewed PR heads merged together. For two PRs, compare these; for a single PR, use `git rev-parse <pr head>^{tree}` instead of `git merge-tree`:

```bash
git merge-tree --write-tree <first pr head> <second pr head>
git rev-parse origin/release/x.y^{tree}
```

Then check that no other draft release uses the tag or title `x.y.z`, and create the release from the branch; the web UI's target defaults to `main`:

```bash
gh release list --repo roostorg/osprey --json tagName,name,isDraft
gh release create x.y.z --target release/x.y --title "Osprey x.y.z" --notes-file notes.md
```

`--notes-file` publishes only the file's text, so check that it ends with the **Full Changelog** compare link from the template.

If a newer minor version has already been released, add `--latest=false` so that GitHub's **Latest** release does not move back to `x.y.z`.

If `gh release create` fails, for example with HTTP 500, check that it created neither a release nor a tag before retrying; otherwise a retry can leave a duplicate release or an orphaned tag:

```bash
gh release list --repo roostorg/osprey --limit 3 --json tagName,isDraft
git ls-remote --tags origin x.y.z
```

Publishing the release, from `main` or from `release/x.y`, triggers the following at once from the workflow files at the tag, so the notes must be final before you publish:

- `.github/workflows/release-osprey-rpc.yml` on the `release` event, which builds the `osprey-rpc` sdist and attaches it to the release.
- `.github/workflows/publish-coordinator-image.yml` on the `release` event, which pushes the `x.y.z`, `x.y`, `x` and commit SHA tags of `ghcr.io/roostorg/osprey/osprey-coordinator`. It does not move `latest`, which follows `main`. If a newer minor version has already been released, the `x` tag moves back to `x.y.z`.
- `.github/workflows/mdbook.yml` on the tag push, which adds the docs snapshot for the release.

### Release notes template

```markdown
> [!IMPORTANT]
> **This release addresses <a reported security advisory | N reported security advisories>**: [GHSA-xxxx-xxxx-xxxx](https://github.com/roostorg/osprey/security/advisories/GHSA-xxxx-xxxx-xxxx) (<Severity>, CVSS <n.n>). <Upgrade wording>. For security announcements, we encourage adopters to subscribe to the [security-announce@roost.tools mailing list](https://groups.google.com/a/roost.tools/g/security-announce).

<One paragraph per advisory: the affected component, the upstream CVE if any, the affected versions, and the fixed version.>

<The risk, in one or two sentences, without exploitation details.>

Thanks to @<reporter> for reporting <GHSA-xxxx-xxxx-xxxx>.

## Other security updates

- **<package>** <x.y.z> fixes [CVE-…](https://github.com/advisories/GHSA-…) (<Severity>) ([#123](https://github.com/roostorg/osprey/pull/123))

## Upgrade notes

- Coordinator image: `ghcr.io/roostorg/osprey/osprey-coordinator:x.y.z`.
- Worker and UI: rebuild your images from the `x.y.z` tag.
- `osprey-rpc`: the sdist attached to this release.

**Full Changelog**: https://github.com/roostorg/osprey/compare/x.y.(z-1)...x.y.z
```

For a release with no Osprey advisory, replace everything above **Other security updates** with:

```markdown
> [!IMPORTANT]
> **This is a security release.** It updates dependencies to versions that fix published upstream advisories, listed below. <Upgrade wording>. For security announcements, we encourage adopters to subscribe to the [security-announce@roost.tools mailing list](https://groups.google.com/a/roost.tools/g/security-announce).
```

List every upstream advisory fixed by a dependency update that has no Osprey advisory under **Other security updates**, without saying whether it affects Osprey: reachability stays private (see [Before you start](#before-you-start)). A backport resolved by hand can change behavior beyond the fix; list any such change under **Upgrade notes**. Leave out sections that do not apply. For upgrade wording, see [Upgrade wording](#upgrade-wording).

## 5. Confirm artifacts published

- [ ] The **Release osprey-rpc** run for the tag succeeded, and the release has the `osprey_rpc-*.tar.gz` and `.zip` assets: `gh release view x.y.z --json assets --jq '.assets[].name'`. This workflow failed on every release up to and including 1.2.1 and was fixed in [#545](https://github.com/roostorg/osprey/pull/545). It has no manual trigger; if it fails, build the sdist with `uv build --sdist --out-dir dist` in `osprey_rpc/` and attach it with `gh release upload x.y.z ...`.
- [ ] The **Build and Publish Osprey Coordinator Docker Image** run for the tag succeeded, and the image exists, e.g. `docker buildx imagetools inspect ghcr.io/roostorg/osprey/osprey-coordinator:x.y.z`. If it failed, re-run it, or run the workflow manually from the tag.
- [ ] The **Update docs with mdbook** run for the tag succeeded, and the release's docs are on the [docs site](https://roostorg.github.io/osprey/).
- [ ] The fix is in the release at `x.y.z`, checked against the previous release `x.y.(z-1)` as a control, which should still show the old code or version. For a Python dependency, read its locked version at each tag; for a code fix, search the tag for a string the fix removed (no matches at `x.y.z`):

  ```bash
  git show x.y.z:uv.lock | grep -A1 '^name = "<package>"$'
  git grep -c "<removed string>" x.y.z -- osprey_worker osprey_coordinator osprey_rpc
  ```

- [ ] If a tier 1 dependency or base image was fixed, the new coordinator image or sdist no longer contains the vulnerable version, using the image scan or SBOM attached to the release where available.

## 6. Publish the advisory

Publish each Osprey advisory after the release, so that its patched version exists. Tier 1 vulnerabilities that are not exploitable get only the `### Security` changelog entry, not an advisory.

### Advisory template

- **Title:** `<Impact> via <vector>`, with the upstream package in parentheses for a dependency, e.g. `Denial of service via malformed request bodies (<package>)`.
- **Affected product:** ecosystem left empty, package `osprey`, affected versions `< x.y.z` (narrower if older versions are not affected), patched versions `x.y.z`. Set these and the severity in the draft before step 4; a draft can be saved with them blank.
- **Severity:** CVSS scored for how Osprey uses the affected code.
- **Description**: don't describe related components the release does not fix; if one needs its own fix, give it its own advisory. Use these sections:
  - `### Summary`: one or two sentences.
  - `### Details`: the affected component and why Osprey is affected.
  - `### Proof of concept`: the steps in prose. No exploit code.
  - `### Impact`: who can exploit it and what they get.
  - `### Patches`: the fixed version and the fixing change.
  - `### Workarounds`: mitigations for deployments that cannot upgrade yet, or "None".
  - `### References`: the upstream advisory and CVE, the release, and the fix.

### CVEs

- Request a CVE only for a vulnerability in Osprey's own code. For an upstream dependency issue that already has a CVE, reference the upstream CVE instead of requesting a new one.
- Requesting a CVE sends the advisory's content to GitHub for review, so request it from the draft advisory only once the draft passes the content checks in [Before publishing](#before-publishing), and before publishing it. GitHub usually reviews requests within 72 hours. Requesting does not make the advisory public, and GitHub publishes the CVE when the advisory is published. GitHub does not document requesting a CVE after publication, so request it while the advisory is a draft.
- Don't hold the release, the advisory or the announcement for the CVE. If it is assigned after publication, edit the GitHub release notes to add it next to the advisory. The email links the advisory page, which shows the CVE, so it need not be resent.

### Before publishing

- [ ] The description is rewritten in the template's form, including References; a reporter's draft is source material, not the published text.
- [ ] Affected and patched versions match the release.
- [ ] Severity and CVSS score are consistent with the Impact section, and the CWE is set.
- [ ] Credits are listed and every credited person has accepted.
- [ ] Then, for a vulnerability in Osprey's own code, a CVE is requested from the draft (see [CVEs](#cves)).
- [ ] The release notes and changelog link the advisory.

## 7. Notify

### Hosted vendors early notice

Vendors who host Osprey so that their users cannot self-patch can receive early notice (see [Security releases](https://community.roost.tools/software-development-practices/security#security-releases)).

- For an undisclosed vulnerability, tell them the release date and what to update before the release, under embargo.
- For an upstream CVE that is already public, tell them as soon as impact on Osprey is confirmed, without waiting for the release.

### security-announce@roost.tools

Post to [security-announce@roost.tools](https://groups.google.com/a/roost.tools/g/security-announce) once the release and its advisories are published. The email lists only Osprey advisories; upstream advisories without an Osprey advisory are listed only in the release notes. A sent email cannot be corrected, so keep it to the facts in the templates below. Workarounds, impact details and upgrade caveats go in the release notes and the advisory, which can be edited.

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

With one or more advisories:

```text
Subject: Security advisory for Osprey: User action required

This is a security advisory for vulnerabilities discovered in the Osprey project. Patches have been applied in the latest Osprey release, version x.y.z. We recommend users upgrade.

The following vulnerabilities affect versions earlier than x.y.z:

* [GHSA-xxxx-xxxx-xxxx](https://github.com/roostorg/osprey/security/advisories/GHSA-xxxx-xxxx-xxxx) (<Severity>, CVSS <n.n>)

Release notes: https://github.com/roostorg/osprey/releases/tag/x.y.z
```

Keep the email to these lines, with one bullet per advisory: no workarounds, impact or remediation details.

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
- [ ] Fix merged (private fork or normal PR), with a regression test where possible; cherry-picks change only the fixed packages in `uv.lock` / `pnpm-lock.yaml`, at `main`'s versions
- [ ] Changelog `### Security` entries and version bump merged
- [ ] CI and the E2E smoke test (once #551 merges) green on the release commit; on the private fork path, on the public merge commit
- [ ] On option B, the branch's tree matches the reviewed backports
- [ ] Release `x.y.z` published from the agreed base, with a no-`v` SemVer tag and the Full Changelog link
- [ ] `osprey-rpc` sdist attached, coordinator image and docs snapshot confirmed published, fix confirmed in the release
- [ ] Advisories published after the pre-publish checks
- [ ] Hosted vendors notified
- [ ] security-announce posted, if required
- [ ] If released from `release/x.y`: release forward-ported to `main`
