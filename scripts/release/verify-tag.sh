#!/usr/bin/env bash
# Refuse a v* tag that is lightweight, unsigned, signed by a key outside
# security/release-tag-fingerprints, on a commit outside the trusted branch
# (or a release/v* branch), or whose version is not the workspace version in
# Cargo.toml. The key and fingerprints are read from the trusted ref, never
# from the tagged commit, so a tag cannot bring its own signer. The v* tag
# ruleset keeps the workflow itself reviewed. The release workflow runs
# `verify` before packaging.
#
# RELEASE_TRUST_REF names the trusted ref (default origin/main).
#
# Usage:
#   bash scripts/release/verify-tag.sh verify <tag> [commit-sha]
set -euo pipefail

TRUST_REF="${RELEASE_TRUST_REF:-origin/main}"
TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)\.[0-9]+)?$'

fail() {
  printf 'verify-tag: %s\n' "$1" >&2
  exit 1
}

normalize_fpr() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]'
}

# Reads Cargo.toml on stdin and prints the [workspace.package] version, which
# every crate inherits.
workspace_version() {
  sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' | head -1
}

verify_release_tag() {
  local tag="$1" expected_commit="${2:-}" objtype peeled raw signer primary line fingerprints allowed=()
  [[ "$tag" =~ $TAG_RE ]] || fail "invalid release tag: ${tag}"

  fingerprints="$(git show "${TRUST_REF}:security/release-tag-fingerprints" 2>/dev/null)" ||
    fail "missing security/release-tag-fingerprints on ${TRUST_REF}"
  while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*(#|$) ]] && continue
    allowed+=("$(normalize_fpr "$line")")
  done <<<"$fingerprints"
  [[ ${#allowed[@]} -gt 0 ]] || fail "no fingerprints on ${TRUST_REF}"
  git show "${TRUST_REF}:security/release-tag-key.asc" 2>/dev/null | gpg --batch --import >/dev/null 2>&1 ||
    fail "could not import security/release-tag-key.asc from ${TRUST_REF}"

  objtype="$(git cat-file -t "refs/tags/${tag}" 2>/dev/null)" || fail "tag ${tag} was not found"
  [[ "$objtype" == "tag" ]] || fail "tag ${tag} is lightweight; release tags must be annotated"

  peeled="$(git rev-parse "${tag}^{commit}")"
  if [[ -n "$expected_commit" && "$peeled" != "$expected_commit" ]]; then
    fail "tag ${tag} resolves to ${peeled}, expected ${expected_commit}"
  fi
  local branch on_branch=false
  for branch in "$TRUST_REF" $(git for-each-ref --format='%(refname)' "refs/remotes/${TRUST_REF%%/*}/release/v*"); do
    git merge-base --is-ancestor "$peeled" "$branch" 2>/dev/null && on_branch=true
  done
  [[ "$on_branch" == true ]] || fail "tag ${tag} is not on ${TRUST_REF} or a release/v* branch"

  raw="$(git verify-tag --raw "$tag" 2>&1)" || true
  # gpg still reports VALIDSIG for an expired or revoked key.
  if grep -qE '^\[GNUPG:\] (EXPKEYSIG|REVKEYSIG|EXPSIG) ' <<<"$raw"; then
    fail "tag ${tag} is signed by an expired or revoked key"
  fi
  [[ "$raw" == *GOODSIG* && "$raw" == *VALIDSIG* ]] || fail "tag ${tag} is unsigned or the signature is not valid"
  # VALIDSIG carries the signing (sub)key in field 3 and the primary key in field 12.
  signer="$(normalize_fpr "$(awk '/^\[GNUPG:\] VALIDSIG / { print $3; exit }' <<<"$raw")")"
  primary="$(normalize_fpr "$(awk '/^\[GNUPG:\] VALIDSIG / { print $12; exit }' <<<"$raw")")"
  local fpr ok=false
  for fpr in "${allowed[@]}"; do
    [[ "$signer" == "$fpr" || "$primary" == "$fpr" ]] && ok=true
  done
  [[ "$ok" == true ]] || fail "tag ${tag} is signed by an unknown key"

  local expected
  expected="$(git show "${peeled}:Cargo.toml" | workspace_version)"
  [[ "${tag#v}" == "$expected" ]] || fail "tag ${tag} does not match workspace version ${expected}"
  printf 'Release tag %s is annotated, signed, and matches workspace version %s\n' "$tag" "$expected"
}

case "${1:-}" in
  verify)
    shift
    [[ $# -ge 1 && $# -le 2 ]] || fail "usage: $0 verify <tag> [commit-sha]"
    verify_release_tag "$@"
    ;;
  *)
    fail "usage: $0 verify <tag> [commit-sha]"
    ;;
esac
