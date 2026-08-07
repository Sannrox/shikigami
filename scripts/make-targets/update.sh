#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

update_count=0
while IFS= read -r script; do
    update_count=$((update_count + 1))
    echo "Updating $(basename "${script}")"
    "${script}"
done < <(
    find "${ROOT_PATH}/scripts" -maxdepth 1 -type f -name 'update-*.sh' -print \
        | LC_ALL=C sort
)

if ((update_count == 0)); then
    echo "No update scripts found under ${ROOT_PATH}/scripts" >&2
    exit 1
fi
