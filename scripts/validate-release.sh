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

python3 scripts/changelog-notes.py --self-test
python3 scripts/release-manifest.py --self-test
python3 scripts/release-sbom.py --self-test
bash scripts/verify-release-tag.sh self-test

workflow=.github/workflows/release.yml
dispatcher=.github/workflows/release-dispatch.yml

# The dispatcher only turns a v* tag push into a main run of release.yml: no
# checkout, no actions, no other trigger, no write access but actions.
# shellcheck disable=SC2016 # literal workflow text
trigger="$(sed -n '/^on:/,/^permissions:/p' "$dispatcher" | grep -vE '^\s*(#|$)')"
[[ "$trigger" == $'on:\n  push:\n    tags: ["v*"]\npermissions:' ]] || fail "${dispatcher} must trigger only on v* tag pushes"
grep -F 'actions: write' "$dispatcher" >/dev/null || fail "${dispatcher} must be able to dispatch workflows"
grep -E '^\s*(-\s+)?uses:|contents: write' "$dispatcher" >/dev/null && fail "${dispatcher} must only dispatch"
# shellcheck disable=SC2016 # literal workflow text
[[ "$(grep -E '^\s*(-\s+)?run:' "$dispatcher" | sed -E 's/^[[:space:]]*(-[[:space:]]+)?//')" == 'run: gh workflow run release.yml --ref main -f tag="$TAG"' ]] ||
  fail "${dispatcher} must run only: gh workflow run release.yml --ref main -f tag=\"\$TAG\""

# release.yml runs only from a dispatch: main for a tag, any ref for a dry run.
trigger="$(sed -n '/^on:/,/^concurrency:/p' "$workflow" | grep -vE '^\s*(#|$)' | head -3)"
[[ "$trigger" == $'on:\n  workflow_dispatch:\n    inputs:' ]] || fail "${workflow} must trigger only on workflow_dispatch"
grep -E '^  (push|pull_request|release):' "$workflow" >/dev/null && fail "${workflow} must not run on push; release-dispatch.yml starts it"
# shellcheck disable=SC2016 # literal workflow text
for needle in \
  'group: release-${{ inputs.tag || github.ref }}' \
  '[[ "$WORKFLOW_REF" == refs/heads/main ]]' \
  "ref: \${{ inputs.tag && format('refs/tags/{0}', inputs.tag) || '' }}" \
  'git fetch --force origin "refs/tags/${TAG}:refs/tags/${TAG}"' \
  'sha="$(git rev-parse "${TAG}^{commit}")"' \
  'RELEASE_TRUST_REF=origin/main bash "${MAIN_TREE}/scripts/verify-release-tag.sh" verify "$TAG" "$sha"' \
  'git merge-base --is-ancestor "$sha" refs/remotes/origin/main' \
  'needs: verify-tag' \
  "if: needs.verify-tag.outputs.tag != ''" \
  'gh release create "$TAG" --draft --verify-tag "${kind[@]}"' \
  'scripts/changelog-notes.py "${TAG#v}"' \
  '[[ "$TAG" == *-* ]]' \
  'kind=(--prerelease --latest=false)' \
  'gh release edit "$TAG" --draft=false --prerelease --latest=false' \
  'ref: refs/tags/${{ needs.verify-tag.outputs.tag }}' \
  'release-sbom.py' \
  'release-manifest.py' \
  'actions/attest-sbom@' \
  'Refuse to replace a published release' \
  'gh release edit "$TAG" --draft=false'; do
  grep -F -- "$needle" "$workflow" >/dev/null || fail "${workflow} is missing: ${needle}"
done
# Every job after verify-tag checks out the release commit and checks HEAD.
checkouts="$(grep -c 'uses: actions/checkout@' "$workflow")"
# shellcheck disable=SC2016 # literal workflow text
sha_checkouts="$(grep -c 'ref: ${{ needs.verify-tag.outputs.sha }}' "$workflow")"
# shellcheck disable=SC2016 # literal workflow text
head_checks="$(grep -c 'run: test "$(git rev-parse HEAD)" = "$SHA"' "$workflow")"
((checkouts == sha_checkouts + 2)) || fail "${workflow} must check out the release commit in every job after verify-tag"
((head_checks == sha_checkouts)) || fail "${workflow} must check HEAD after every release-commit checkout"
grep -E 'github\.sha|github\.ref_name|GITHUB_SHA|GITHUB_REF' "$workflow" >/dev/null &&
  fail "${workflow} must use the tagged commit and tag input, not the event SHA or ref name"
grep -E -- '--latest( |$|")' "$workflow" >/dev/null && fail "${workflow} must never force a release to latest"
grep -F 'create-gh-release-action' "$workflow" >/dev/null && fail "${workflow} must resume a draft, not delete and recreate it"
(($(grep -c 'is already published; cut a new tag instead of re-running' "$workflow") >= 3)) ||
  fail "${workflow} must refuse a published release early, when drafting, and before publishing"
sed -n '1,/^jobs:/p' "$workflow" | grep -E 'attestations: write|id-token: write|packages: write|contents: write' >/dev/null &&
  fail "${workflow} must not grant write scopes at workflow level"
grep -E ':latest\b|--tag[= ]latest' "$workflow" >/dev/null && fail "${workflow} must not produce a latest tag"

printf 'validate-release passed\n'
