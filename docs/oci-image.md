# Shikigami OCI image

Shikigami publishes a linux/amd64 OCI image for supported stable `1.0.5` and
newer releases.
The release workflow pushes it to the repository-scoped GitHub Container
Registry path:

```text
ghcr.io/sannrox/shikigami@sha256:<digest>
```

Version tags are convenience labels only. Consumers must use the immutable
digest recorded in the `shikigami-<version>-oci.json` GitHub Release asset.
The image is not published as a `latest` tag.

## Boundary and invocation

The image is a build input for the managed Aldunis Code image. It targets the
local-subprocess contract currently specified in #142: the host invokes the
`shikigami` executable with CLI arguments such as `version`, `doctor`, and
`run --task-file`. It is not a Shikigami network service and does not define a
new protocol.

This consumption boundary is preferred by
[aldunis-platform#142](https://github.com/Sannrox/aldunis-platform/issues/142),
but platform integration remains blocked until that design issue records the
accepted boundary. This repository does not change the Aldunis Code image or
Compose configuration.

The platform-side deployment guardrails remain owned by
[aldunis-platform#141](https://github.com/Sannrox/aldunis-platform/issues/141):
the Code image or Compose host must pin the recorded digest, use the approved
registry and pull-auth path, run read-only with dropped capabilities and
resource limits, and provide no host paths, Docker socket, or platform
database credentials. This image publication does not decide or weaken those
settings.

The image contract is:

- `ENTRYPOINT` is `/usr/local/bin/shikigami` and the default command is `version`;
- the supported command surface is the stable CLI contract documented in
  [embedding.md](embedding.md), including `version`, `doctor`, `run`, and
  `serve`;
- the process runs as UID/GID `65532:65532` with working directory `/workspace`;
- state is explicitly rooted at `/var/lib/shikigami`; callers should mount
  `/var/lib/shikigami` and `/workspace` when retaining state or workspaces;
- no provider keys, plane tokens, repository checkout, host path, or runtime
  service credentials are copied into the image.
- the OCI label
  `io.sannrox.shikigami.worker-lifecycle=shikigami.worker_lifecycle/v1`
  advertises the versioned plane-worker lifecycle contract used by Tenkai and
  other fleet hosts.

Governed operation still requires the configured plane and token. Missing or
unhealthy governance remains fail-closed; the image does not add credentials or
weaken that policy.

## Reproducibility and evidence

The builder and runtime base images are pinned to linux/amd64 manifest digests.
The build context is allow-listed in `.dockerignore`; the final stage copies
only the release executable and empty state/workspace directories. OCI labels
record the source repository, release version, source revision, and the
supported plane-worker lifecycle contract. The release
workflow also enables BuildKit max-mode provenance and SBOM attestations and
uploads the resulting immutable image reference and digest as release metadata.

Run the deterministic local smoke check with a loaded image:

Run this from a checkout whose Cargo package version matches the `VERSION`
argument; the release workflow additionally rejects versions below `1.0.5`.

```bash
docker buildx build --platform linux/amd64 --load --tag shikigami:smoke \
  --build-arg VERSION=1.0.5 \
  --build-arg VCS_REF=$(git rev-parse HEAD) .
bash scripts/oci-image-smoke.sh shikigami:smoke 1.0.5 $(git rev-parse HEAD)
```

The same check runs in CI and before release publication. It verifies startup,
version, metadata, architecture, non-root execution, explicit paths, absence
of credential environment variables, and absence of source/repository data in
the final rootfs.
