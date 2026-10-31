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
#   bash scripts/verify-release-tag.sh verify <tag> [commit-sha]
#   bash scripts/verify-release-tag.sh self-test
set -euo pipefail

SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")"
TRUST_REF="${RELEASE_TRUST_REF:-origin/main}"
TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)\.[0-9]+)?$'

fail() {
  printf 'verify-release-tag: %s\n' "$1" >&2
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

expect_failure() {
  local message="$1" output
  shift
  if output="$(bash "$SCRIPT_PATH" verify "$@" 2>&1)"; then
    fail "expected verification failure containing: ${message}"
  fi
  [[ "$output" == *"$message"* ]] || fail "expected '${message}', got: ${output}"
}

# Builds a throwaway repo and keyring under a unique temp dir, so concurrent
# runs never share state.
self_test() {
  local tmp work fpr first
  # /tmp, not $TMPDIR: gpg-agent's socket path must stay short on macOS.
  tmp="$(mktemp -d "/tmp/shikigami-tag.$$.XXXXXX")"
  # shellcheck disable=SC2064 # expand $tmp now; it is local.
  trap "gpgconf --kill gpg-agent >/dev/null 2>&1 || true; rm -rf '$tmp'" EXIT
  work="${tmp}/work"
  export GNUPGHOME="${tmp}/gpg"
  mkdir -m 700 "$GNUPGHOME"
  gpg --batch --pinentry-mode loopback --passphrase '' \
    --quick-generate-key "Release self-test <release-self-test@example.invalid>" ed25519 default never >/dev/null 2>&1
  fpr="$(gpg --batch --list-secret-keys --with-colons | awk -F: '/^fpr:/{print $10; exit}')"
  [[ -n "$fpr" ]] || fail "could not generate a self-test signing key"
  export RELEASE_TRUST_REF=main

  git init -q -b main "$work"
  git -C "$work" config user.name "Release tag self-test"
  git -C "$work" config user.email "release-self-test@example.invalid"
  git -C "$work" config commit.gpgsign false
  git -C "$work" config tag.gpgSign false
  git -C "$work" config user.signingkey "$fpr"
  git -C "$work" config gpg.program "$(command -v gpg)"
  mkdir "${work}/security"
  gpg --batch --armor --export "$fpr" >"${work}/security/release-tag-key.asc"
  printf '# self-test\n%s\n' "$fpr" >"${work}/security/release-tag-fingerprints"
  printf '[workspace]\nmembers = []\n\n[workspace.package]\nversion = "0.1.0"\n\n[workspace.dependencies]\nversion = "9.9.9"\n' >"${work}/Cargo.toml"
  git -C "$work" add Cargo.toml security
  git -C "$work" commit -q -m first
  first="$(git -C "$work" rev-parse HEAD)"

  git -C "$work" tag v0.1.0 "$first"
  (cd "$work" && expect_failure "is lightweight" v0.1.0 "$first")
  git -C "$work" tag -d v0.1.0 >/dev/null

  git -C "$work" tag -a v0.1.0 -m unsigned "$first"
  (cd "$work" && expect_failure "unsigned" v0.1.0 "$first")
  git -C "$work" tag -d v0.1.0 >/dev/null

  git -C "$work" tag -s v0.1.0 -m signed "$first"
  (cd "$work" && bash "$SCRIPT_PATH" verify v0.1.0 "$first" >/dev/null)
  (cd "$work" && expect_failure "expected 0000" v0.1.0 "$(printf '0%.0s' {1..40})")

  # A tag off the trusted branch fails even if its commit lists its own key.
  git -C "$work" checkout -q -b side
  git -C "$work" commit -q --allow-empty -m side
  git -C "$work" tag -s v0.1.0-rc.1 -m side
  (cd "$work" && expect_failure "is not on main" v0.1.0-rc.1)
  git -C "$work" checkout -q main

  # The allowlist on the trusted ref decides, not the key in the keyring.
  printf 'AEDA618886BF0331105B9CFCE4DC1F4A579505FB\n' >"${work}/security/release-tag-fingerprints"
  git -C "$work" commit -q -am "drop self-test key"
  (cd "$work" && expect_failure "unknown key" v0.1.0 "$first")
  git -C "$work" reset -q --hard "$first"

  git -C "$work" tag -s v0.2.0 -m "wrong version" "$first"
  (cd "$work" && expect_failure "does not match workspace version" v0.2.0 "$first")

  (cd "$work" && expect_failure "invalid release tag" v0.1 "$first")

  # Revoking the key on the trusted ref rejects tags it signed earlier.
  # gpg prefixes the stored certificate's armor with ':' against accidental use.
  sed 's/^:-----BEGIN/-----BEGIN/' "${GNUPGHOME}/openpgp-revocs.d/${fpr}.rev" | gpg --batch --import >/dev/null 2>&1
  gpg --batch --armor --export "$fpr" >"${work}/security/release-tag-key.asc"
  git -C "$work" commit -q -am "revoke self-test key"
  (cd "$work" && expect_failure "expired or revoked" v0.1.0)
  printf 'self-test passed\n'
}

case "${1:-}" in
  verify)
    shift
    [[ $# -ge 1 && $# -le 2 ]] || fail "usage: $0 verify <tag> [commit-sha]"
    verify_release_tag "$@"
    ;;
  self-test)
    self_test
    ;;
  *)
    fail "usage: $0 {verify <tag> [commit-sha]|self-test}"
    ;;
esac
