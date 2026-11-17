#!/usr/bin/env bash
# Lint GitHub workflow files (actionlint) and audit them for security issues
# (zizmor). `make validate` runs this locally and in CI, so both use the same
# pinned releases. Each binary is downloaded once per version into
# ${SHIKIGAMI_TOOLS_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/shikigami/tools} and
# verified against the sha256 pinned below before it runs.
#
# Bump a tool: change its version and every platform's digest together (the
# release page lists the digests).

set -o errexit
set -o nounset
set -o pipefail

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
TOOLS_DIR="${SHIKIGAMI_TOOLS_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/shikigami/tools}"

ACTIONLINT_VERSION=1.7.12
ZIZMOR_VERSION=1.32.0

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)
        actionlint_asset="actionlint_${ACTIONLINT_VERSION}_linux_amd64.tar.gz"
        actionlint_sha=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
        zizmor_asset=zizmor-x86_64-unknown-linux-gnu.tar.gz
        zizmor_sha=bb110cc765eca9cd7efb2e284a956be5974c60027b0970f16397d86208a9957b
        ;;
    Linux-aarch64)
        actionlint_asset="actionlint_${ACTIONLINT_VERSION}_linux_arm64.tar.gz"
        actionlint_sha=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6
        zizmor_asset=zizmor-aarch64-unknown-linux-gnu.tar.gz
        zizmor_sha=cb152b207f815116d7fa1c1ee4614f61578803351395052908095e79adfd9fd0
        ;;
    Darwin-arm64)
        actionlint_asset="actionlint_${ACTIONLINT_VERSION}_darwin_arm64.tar.gz"
        actionlint_sha=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f
        zizmor_asset=zizmor-aarch64-apple-darwin.tar.gz
        zizmor_sha=6efed18c4539a663fd3c851a2be9533fd8456c4e646d9416206da059154a0d3c
        ;;
    Darwin-x86_64)
        actionlint_asset="actionlint_${ACTIONLINT_VERSION}_darwin_amd64.tar.gz"
        actionlint_sha=5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644
        zizmor_asset=zizmor-x86_64-apple-darwin.tar.gz
        zizmor_sha=6983143c719ada6845a69cc4dd2bacc215f971662fecd8bbd2678388e5ce5969
        ;;
    *)
        echo "validate-workflows: no pinned actionlint/zizmor for $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

sha256() {
    if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}

# fetch <name> <version> <url> <sha256>: prints the path of the verified binary.
# Extracts into a private temp dir and renames it into place, so concurrent
# runs never see a partial install.
fetch() {
    local name="$1" version="$2" url="$3" want="$4"
    local dest="${TOOLS_DIR}/${name}-${version}-${want:0:12}"
    if [ ! -x "${dest}/${name}" ]; then
        mkdir -p "$TOOLS_DIR"
        local tmp
        tmp="$(mktemp -d "${TOOLS_DIR}/.${name}.$$.XXXXXX")"
        curl --fail --silent --show-error --location --output "${tmp}/archive.tar.gz" "$url"
        local got
        got="$(sha256 "${tmp}/archive.tar.gz")"
        if [ "$got" != "$want" ]; then
            rm -rf "$tmp"
            echo "validate-workflows: ${url} has sha256 ${got}, expected ${want}" >&2
            exit 1
        fi
        tar -xzf "${tmp}/archive.tar.gz" -C "$tmp" "$name"
        rm "${tmp}/archive.tar.gz"
        mv "$tmp" "$dest" 2>/dev/null || rm -rf "$tmp"
    fi
    printf '%s/%s\n' "$dest" "$name"
}

actionlint="$(fetch actionlint "$ACTIONLINT_VERSION" \
    "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/${actionlint_asset}" \
    "$actionlint_sha")"
zizmor="$(fetch zizmor "$ZIZMOR_VERSION" \
    "https://github.com/zizmorcore/zizmor/releases/download/v${ZIZMOR_VERSION}/${zizmor_asset}" \
    "$zizmor_sha")"

cd "$ROOT_PATH"
# Shellcheck and pyflakes integrations stay off so a local run and CI report
# the same findings whatever happens to be installed.
"$actionlint" -shellcheck= -pyflakes=
# Offline: no GitHub token, and the result depends only on this checkout.
"$zizmor" --offline --no-progress .github
