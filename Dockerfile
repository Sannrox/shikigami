# syntax=docker/dockerfile:1.7

# The builder and runtime references are pinned to the linux/amd64 manifests
# used by the release workflow. Do not replace them with floating tags.
FROM --platform=linux/amd64 docker.io/library/rust:1.97.1-bookworm@sha256:408fe88047cef61a2087653b0c5255fa51c0f2d6d94ddedd7a2562a9b91a46f6 AS builder

WORKDIR /src

# Keep the build context limited to files needed for compilation. The final
# stage copies only the executable and empty operator-owned directories.
COPY Cargo.toml Cargo.lock README.md LICENSE ./
COPY src ./src
COPY crates ./crates

ENV CARGO_INCREMENTAL=0 \
    RUSTFLAGS=--remap-path-prefix=/src=/usr/src/shikigami \
    SOURCE_DATE_EPOCH=0

RUN cargo build --locked --release --bin shikigami \
    && mkdir -p /image-root/var/lib/shikigami /image-root/workspace \
    && touch /image-root/var/lib/shikigami/.keep /image-root/workspace/.keep

FROM --platform=linux/amd64 gcr.io/distroless/cc-debian12:nonroot@sha256:471dbca9cad607b9a32c10e9c31fb09ffaeb2d460e0afbff86c27abbc80b1b98 AS runtime

ARG VERSION=unknown
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="shikigami" \
      org.opencontainers.image.description="Open-source local-first headless agent harness" \
      org.opencontainers.image.source="https://github.com/Sannrox/shikigami" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      io.sannrox.shikigami.worker-lifecycle="shikigami.worker_lifecycle/v1"

COPY --from=builder --chown=65532:65532 /src/target/release/shikigami /usr/local/bin/shikigami
COPY --from=builder --chown=65532:65532 /image-root/var/lib/shikigami /var/lib/shikigami
COPY --from=builder --chown=65532:65532 /image-root/workspace /workspace

ENV SHIKIGAMI_STATE=/var/lib/shikigami

WORKDIR /workspace
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/shikigami"]
CMD ["version"]
