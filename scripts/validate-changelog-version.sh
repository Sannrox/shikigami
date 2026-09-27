#!/usr/bin/env bash
# Crate version, README status stamp, and the first dated CHANGELOG heading
# must be the same SemVer. Unreleased must not still list bullets that already
# shipped under that heading.

set -o errexit
set -o nounset
set -o pipefail

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${ROOT_PATH}"

version="$(sed -nE 's/^version = "([0-9]+\.[0-9]+\.[0-9]+)"/\1/p' Cargo.toml | head -n1)"
if [[ -z "${version}" ]]; then
    echo "validate-changelog-version: could not read Cargo.toml version" >&2
    exit 1
fi

heading="$(awk '/^## \[[0-9]+\.[0-9]+\.[0-9]+\]/ {
    gsub(/[][]/, "", $2)
    print $2
    exit
}' CHANGELOG.md)"
if [[ "${heading}" != "${version}" ]]; then
    echo "validate-changelog-version: first dated CHANGELOG heading [${heading}] != crate version ${version}" >&2
    exit 1
fi

if ! grep -Fq "**Status:** \`v${version}\`" README.md; then
    echo "validate-changelog-version: README.md Status stamp is not v${version}" >&2
    exit 1
fi

python3 - "${version}" <<'PY'
import pathlib
import sys

version = sys.argv[1]
text = pathlib.Path("CHANGELOG.md").read_text(encoding="utf-8")
start = text.find("## Unreleased")
dated = text.find(f"## [{version}]")
if start < 0 or dated < 0 or dated <= start:
    print("validate-changelog-version: Unreleased or version heading missing", file=sys.stderr)
    sys.exit(1)
unreleased = text[start:dated]
shipped = text[dated:]
next_heading = shipped.find("\n## [", 1)
if next_heading > 0:
    shipped = shipped[:next_heading]


def bullets(block: str) -> set[str]:
    items: set[str] = set()
    buf: list[str] = []
    for line in block.splitlines():
        if line.startswith("- "):
            if buf:
                items.add(" ".join(buf).strip())
            buf = [line[2:].strip()]
        elif buf and (line.startswith("  ") or line == ""):
            if line.strip():
                buf.append(line.strip())
        elif buf:
            items.add(" ".join(buf).strip())
            buf = []
    if buf:
        items.add(" ".join(buf).strip())
    return items


dupes = bullets(unreleased) & bullets(shipped)
if dupes:
    sample = next(iter(dupes))
    print(
        f"validate-changelog-version: Unreleased still lists a bullet shipped in [{version}]: {sample[:120]}",
        file=sys.stderr,
    )
    sys.exit(1)
PY
