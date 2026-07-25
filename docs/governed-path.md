# Governed path (sekai-chisei)

This document is the smoke recipe for the production governance adapter.

## Prerequisites

1. A running [sekai-chisei](https://github.com/Sannrox/sekai-chisei) control plane
   reachable over gRPC (default `http://127.0.0.1:50051`).
2. Optional plane token in an environment variable (see
   `governance.token_env` in settings).
3. Providers configured on the plane for model execution (required for full
   `run`, not for `doctor` connectivity probe).

## Configuration

Use [`examples/governed-sekai-chisei.toml`](../examples/governed-sekai-chisei.toml)
or set:

```bash
export SHIKIGAMI_PROFILE=governed
export SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051
# export SEKAI_TOKEN=...   # if token_env = "SEKAI_TOKEN"
```

## Doctor

```bash
cargo run --bin shikigami -- --config examples/governed-sekai-chisei.toml doctor
# or JSON:
cargo run --bin shikigami -- --config examples/governed-sekai-chisei.toml doctor --json
```

Expected when the plane is up: `status: ok` and a `plane: reachable at ...`
line. Missing endpoint or fail-closed probe failure yields `status: fail`
(`ok: false` in JSON).

## Live tests (ignored by default)

```bash
export SEKAI_LIVE=1
export SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051
cargo test --test plane_live -- --ignored --nocapture
```

Offline `cargo test` never requires a plane.

## Run (requires plane + model providers)

```bash
cargo run --bin shikigami -- --config examples/governed-sekai-chisei.toml \
  run "say hello via tools" --keep-workspace --timeout-secs 120
```

If the plane is down under `fail_closed`, doctor and run refuse to start.
