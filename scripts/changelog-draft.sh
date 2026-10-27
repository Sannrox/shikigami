#!/usr/bin/env bash
# Print a Keep a Changelog section for the pull requests squash-merged to main
# since the last v* tag. Release prep pastes it into CHANGELOG.md, edits it,
# and commits it with the version bump; pull requests never edit CHANGELOG.md.
#
#   scripts/changelog-draft.sh <version> [since-ref]
#
# Squash subjects are Conventional Commits: feat -> Added, fix -> Fixed,
# refactor/perf -> Changed; other types are omitted. A commit marked breaking
# (`type!:`, a `BREAKING CHANGE` line, or a `Breaking`/`Upgrade` heading in
# the pull request body) is listed under "Breaking changes and upgrade"
# whatever its type.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: scripts/changelog-draft.sh <version> [since-ref]" >&2
    exit 2
fi
version="${1#v}"

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${ROOT_PATH}"

git fetch --quiet --tags origin main
since="${2:-$(git describe --tags --abbrev=0 --match 'v*' origin/main)}"

added=() fixed=() changed=() breaking=()
while read -r sha; do
    subject="$(git log -1 --format=%s "${sha}")"
    body="$(git log -1 --format=%b "${sha}")"
    header_re='^([a-z]+)(\([^)]*\))?(!?): (.*)$'
    if [[ "${subject}" =~ ${header_re} ]]; then
        type="${BASH_REMATCH[1]}" bang="${BASH_REMATCH[3]}" text="${BASH_REMATCH[4]}"
    else
        type="" bang="" text="${subject}"
    fi
    line="- ${text}"
    if [[ -n "${bang}" ]] || grep -qE '^(BREAKING[ -]CHANGE|#+ *(Breaking|Upgrade))' <<<"${body}"; then
        breaking+=("${line}")
        continue
    fi
    case "${type}" in
        feat) added+=("${line}") ;;
        fix) fixed+=("${line}") ;;
        refactor | perf) changed+=("${line}") ;;
    esac
done < <(git log --first-parent --reverse --format=%H "${since}..origin/main")

# section <title> <lines...> prints a ### block when it has entries.
section() {
    local title="$1"
    shift
    [[ $# -eq 0 ]] && return
    printf '\n### %s\n\n' "${title}"
    printf '%s\n' "$@"
}

echo "## [${version}] — $(date -u +%F)"
section "Breaking changes and upgrade" ${breaking[@]+"${breaking[@]}"}
section "Added" ${added[@]+"${added[@]}"}
section "Changed" ${changed[@]+"${changed[@]}"}
section "Fixed" ${fixed[@]+"${fixed[@]}"}
