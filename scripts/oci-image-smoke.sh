#!/usr/bin/env bash
set -euo pipefail

image_ref=${1:?usage: oci-image-smoke.sh IMAGE [EXPECTED_VERSION] [EXPECTED_REVISION]}
expected_version=${2:-}
expected_revision=${3:-}

label() {
  local name=$1
  docker image inspect --format="{{index .Config.Labels \"${name}\"}}" "$image_ref"
}

assert_equal() {
  local name=$1
  local expected=$2
  local actual=$3
  if [[ "$actual" != "$expected" ]]; then
    printf 'OCI smoke: %s mismatch: expected %q, got %q\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
}

assert_nonempty() {
  local name=$1
  local value=$2
  if [[ -z "$value" || "$value" == "<no value>" ]]; then
    printf 'OCI smoke: %s is empty\n' "$name" >&2
    exit 1
  fi
}

source_label=$(label org.opencontainers.image.source)
version_label=$(label org.opencontainers.image.version)
revision_label=$(label org.opencontainers.image.revision)
worker_lifecycle_label=$(label io.sannrox.shikigami.worker-lifecycle)
assert_equal "source label" "https://github.com/Sannrox/shikigami" "$source_label"
assert_nonempty "version label" "$version_label"
assert_nonempty "revision label" "$revision_label"
assert_equal "worker lifecycle label" "shikigami.worker_lifecycle/v1" "$worker_lifecycle_label"

if [[ -n "$expected_version" ]]; then
  assert_equal "version label" "$expected_version" "$version_label"
  version_output=$(docker run --platform linux/amd64 --rm --pull=never --read-only "$image_ref" version)
  assert_equal "version command" "shikigami $expected_version" "$version_output"
  assert_equal "default command" "shikigami $expected_version" \
    "$(docker run --platform linux/amd64 --rm --pull=never --read-only "$image_ref")"
fi
if [[ -n "$expected_revision" ]]; then
  assert_equal "revision label" "$expected_revision" "$revision_label"
fi

assert_equal "architecture" "amd64" \
  "$(docker image inspect --format='{{.Architecture}}' "$image_ref")"
assert_equal "user" "65532:65532" \
  "$(docker image inspect --format='{{.Config.User}}' "$image_ref")"
assert_equal "working directory" "/workspace" \
  "$(docker image inspect --format='{{.Config.WorkingDir}}' "$image_ref")"
assert_equal "entrypoint" "[/usr/local/bin/shikigami]" \
  "$(docker image inspect --format='{{.Config.Entrypoint}}' "$image_ref")"

environment=$(docker image inspect --format='{{json .Config.Env}}' "$image_ref")
if grep -Eiq '(^|[^A-Za-z0-9_])(OPENAI_API_KEY|SEKAI_TOKEN|SHIKIGAMI_CONTROL_PLANE|TOKEN|API_KEY)(=|[^A-Za-z0-9_]|$)' <<<"$environment"; then
  printf 'OCI smoke: image config contains a credential or control-plane environment variable\n' >&2
  exit 1
fi
assert_equal "state environment" "true" \
  "$(grep -Eq 'SHIKIGAMI_STATE=/var/lib/shikigami' <<<"$environment" && echo true || echo false)"

# Exporting the image config is deterministic and avoids relying on a shell in
# the distroless runtime. Source files, Cargo metadata, and repository control
# data must not be present in the final rootfs.
container_id=$(docker create --platform linux/amd64 "$image_ref" version)
rootfs_tar=$(mktemp)
cleanup() {
  docker rm "$container_id" >/dev/null 2>&1 || true
  rm -f "$rootfs_tar"
}
trap cleanup EXIT
docker export "$container_id" >"$rootfs_tar"
if tar -tf "$rootfs_tar" | grep -E '^((\.git|Cargo\.toml|Cargo\.lock)(/|$)|src/|proto/)' >/dev/null; then
  printf 'OCI smoke: final rootfs contains repository data\n' >&2
  exit 1
fi

printf 'OCI smoke: PASS (%s)\n' "$image_ref"
