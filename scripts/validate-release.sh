#!/usr/bin/env bash
# Check the v* release path without publishing: script self-tests plus the
# workflow invariants that keep attestations and publishing tag-only.
# `make validate` (CI) runs it on every pull request.
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
  printf 'validate-release: %s\n' "$1" >&2
  exit 1
}

python3 scripts/release-manifest.py --self-test
python3 scripts/release-sbom.py --self-test
bash scripts/verify-release-tag.sh self-test

workflow=.github/workflows/release.yml
# The trigger block must name only v* tag pushes.
trigger="$(sed -n '/^on:/,/^permissions:/p' "$workflow" | grep -vE '^\s*(#|$)')"
[[ "$trigger" == $'on:\n  push:\n    tags: ["v*"]\npermissions:' ]] || fail "${workflow} must trigger only on v* tag pushes"
# shellcheck disable=SC2016 # literal workflow text
for needle in \
  'verify-release-tag.sh verify' \
  'needs: verify-tag' \
  'draft: true' \
  'changelog: CHANGELOG.md' \
  'release-sbom.py' \
  'release-manifest.py' \
  'actions/attest-sbom@' \
  'Refuse to replace a published release' \
  'gh release edit "$TAG" --draft=false'; do
  grep -F -- "$needle" "$workflow" >/dev/null || fail "${workflow} is missing: ${needle}"
done
sed -n '1,/^jobs:/p' "$workflow" | grep -E 'attestations: write|id-token: write|packages: write|contents: write' >/dev/null &&
  fail "${workflow} must not grant write scopes at workflow level"
grep -E ':latest\b|--tag[= ]latest' "$workflow" >/dev/null && fail "${workflow} must not produce a latest tag"

printf 'validate-release passed\n'
