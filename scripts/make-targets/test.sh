#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "${ROOT_PATH}"

"${CARGO:-cargo}" test --locked --lib "$@"
