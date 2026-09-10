#!/usr/bin/env bash
#
# Supply-chain analysis of this repository's CI pipelines (GitHub Actions)
# using poutine <https://github.com/boostsecurityio/poutine>.
#
# Read-only by design: the repo is bind-mounted into a pinned container as
# ro, the container has no network access, and the only file written is the
# SARIF report (default: poutine.sarif, gitignored / uploaded as an artifact).
#
# Usage:
#   ./poutine-scan.sh                 # pretty report + SARIF, fail on warning/error
#   POUTINE_FAIL_LEVEL=none ./poutine-scan.sh
#   POUTINE_FAIL_LEVEL=note ./poutine-scan.sh
#   POUTINE_SARIF=/tmp/out.sarif ./poutine-scan.sh
#
# Env:
#   POUTINE_IMAGE       pinned poutine image (digest-pinned; bump deliberately)
#   POUTINE_FAIL_LEVEL  none | note | warning | error   (default: warning)
#   POUTINE_SARIF       SARIF output path                (default: poutine.sarif)
#
set -euo pipefail

POUTINE_IMAGE="${POUTINE_IMAGE:-ghcr.io/boostsecurityio/poutine:1.1.6@sha256:722a8e0999b583c1540fe2974e691032b2d9d21b9256a17965132b6bfd0081b0}"
POUTINE_FAIL_LEVEL="${POUTINE_FAIL_LEVEL:-warning}"
POUTINE_SARIF="${POUTINE_SARIF:-poutine.sarif}"

repo_root="${GITHUB_WORKSPACE:-$(git rev-parse --show-toplevel)}"

for bin in docker jq; do
  command -v "$bin" >/dev/null || { echo "poutine-scan: '$bin' is required" >&2; exit 2; }
done

poutine() {
  docker run --rm --network none \
    -v "${repo_root}:/src:ro" -w /src \
    "$POUTINE_IMAGE" analyze_local /src \
    --disable-version-check --quiet "$@"
}

# Human-readable report (stdout) + machine-readable report (SARIF).
poutine
poutine --format sarif >"$POUTINE_SARIF"

case "$POUTINE_FAIL_LEVEL" in
  none)    echo "poutine-scan: report written to ${POUTINE_SARIF} (not gating)"; exit 0 ;;
  note)    levels='["note","warning","error"]' ;;
  warning) levels='["warning","error"]' ;;
  error)   levels='["error"]' ;;
  *) echo "poutine-scan: invalid POUTINE_FAIL_LEVEL='${POUTINE_FAIL_LEVEL}'" >&2; exit 2 ;;
esac

blocking="$(jq --argjson levels "$levels" \
  '[.runs[0].results[]? | select(((.level // "warning")) as $l | $levels | index($l))] | length' \
  "$POUTINE_SARIF")"

if [ "$blocking" -gt 0 ]; then
  echo "poutine-scan: ${blocking} finding(s) at level >= ${POUTINE_FAIL_LEVEL} (see ${POUTINE_SARIF})" >&2
  exit 1
fi

echo "poutine-scan: no findings at level >= ${POUTINE_FAIL_LEVEL} (report: ${POUTINE_SARIF})"
