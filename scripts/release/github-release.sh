#!/usr/bin/env bash
# The only script that changes a GitHub Release. Every command first refuses a
# published release, so "Re-run failed jobs" can resume a draft but never
# rewrite what was published. A tag containing "-" (vX.Y.Z-rc.N) is a
# pre-release and is never marked latest. Needs GH_TOKEN and GH_REPO.
#
# Usage:
#   bash scripts/release/github-release.sh guard <tag>
#       Exit 0 when the release is a draft or does not exist yet.
#   bash scripts/release/github-release.sh draft <tag> <notes>
#       Create the draft, or resume it: drop the earlier run's assets and
#       reset title, notes, and pre-release state.
#   bash scripts/release/github-release.sh upload <tag> <asset>...
#       Upload assets to the draft, replacing ones of the same name.
#   bash scripts/release/github-release.sh publish <tag>
#       Publish the draft.
set -euo pipefail

fail() {
  printf 'github-release: %s\n' "$1" >&2
  exit 1
}

# Prints "draft" or "missing"; fails on a published release or a lookup error.
# A token that cannot see drafts reports a draft as not found.
release_state() {
  local tag="$1" state
  if state="$(gh release view "$tag" --json isDraft --jq .isDraft 2>&1)"; then
    [[ "$state" == true ]] || fail "release ${tag} is already published; cut a new tag instead of re-running"
    echo draft
  elif [[ "$state" == *"release not found"* ]]; then
    echo missing
  else
    fail "cannot read release ${tag}: ${state}"
  fi
}

require_draft() {
  [[ "$(release_state "$1")" == draft ]] || fail "release $1 has no draft"
}

cmd="${1:-}"
[[ $# -ge 1 ]] && shift
tag="${1:-}"
kind=()
if [[ "$tag" == *-* ]]; then
  kind=(--prerelease --latest=false)
fi
case "$cmd" in
  guard)
    [[ $# -eq 1 ]] || fail "usage: $0 guard <tag>"
    release_state "$tag" >/dev/null
    ;;
  draft)
    [[ $# -eq 2 ]] || fail "usage: $0 draft <tag> <notes>"
    notes="$2"
    state="$(release_state "$tag")"
    if [[ "$state" == draft ]]; then
      # Every run rebuilds every asset; drop the earlier run's so a stale or
      # renamed asset is never attested or published.
      names="$(gh release view "$tag" --json assets --jq '.assets[].name')"
      while IFS= read -r name; do
        [[ -n "$name" ]] && gh release delete-asset "$tag" "$name" --yes
      done <<<"$names"
      gh release edit "$tag" --title "$tag" --notes-file "$notes" --draft ${kind[@]+"${kind[@]}"}
    else
      gh release create "$tag" --draft --verify-tag ${kind[@]+"${kind[@]}"} \
        --title "$tag" --notes-file "$notes"
    fi
    ;;
  upload)
    [[ $# -ge 2 ]] || fail "usage: $0 upload <tag> <asset>..."
    shift
    require_draft "$tag"
    gh release upload "$tag" "$@" --clobber
    ;;
  publish)
    [[ $# -eq 1 ]] || fail "usage: $0 publish <tag>"
    require_draft "$tag"
    gh release edit "$tag" --draft=false ${kind[@]+"${kind[@]}"}
    echo "Published GitHub Release ${tag}"
    ;;
  *)
    fail "usage: $0 {guard|draft|upload|publish} <tag> ..."
    ;;
esac
