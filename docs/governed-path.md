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

## Mid-run tool authorization (external-action)

When governance is `sekai-chisei`, each **consequential tool invocation** is
authorized through the plane’s host-executed external-action API
(`AuthorizeExternalAction`) **before** the host runs the tool.

| Tool | External-action? | `risk_class` |
| --- | --- | --- |
| `bash` | yes | `destructive` |
| `write_file` | yes | `write` |
| `edit` | yes | `write` |
| `read_file` | yes | `read` |
| `report` | no | — (harness-internal completion signal) |

### Decision handling (headless)

| Decision | Host behavior |
| --- | --- |
| `permit` | Execute the tool |
| `deny` | Do **not** execute; surface denial on the tool result / events |
| `require_approval` | Do **not** execute (headless path cannot wait for interactive approval) |
| missing / unknown | Fail closed as denial |
| plane unavailable + `fail_closed` | Fail closed (tool not executed) |

Offline adapters (`none`, `local`) do **not** call external-action; they only
enforce the local tool allow-list.

Action type is `shikigami.tool.<name>`. Arguments are summarized as a SHA-256
digest (`canonical_arguments_digest`); full args stay on the host.

### Tests

- Unit tests cover decision interpretation and tool/risk mapping
  (`src/governance/sekai_chisei.rs` tests).
- Live plane coverage remains under `tests/plane_live.rs` (ignored unless
  `SEKAI_LIVE=1`).
