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
# Include concrete models authorized by the plane plus the `auto` route.
cargo run --bin shikigami -- --config examples/governed-sekai-chisei.toml doctor --models
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

### Nightly workflow (optional)

Repository workflow [nightly-live.yml](../.github/workflows/nightly-live.yml)
runs the live doctor probe on a schedule when secrets are configured:

| Secret | Required | Purpose |
| --- | --- | --- |
| `SHIKIGAMI_CONTROL_PLANE` | yes | gRPC endpoint URL for the plane |
| `SEKAI_TOKEN` | no | Bearer token if the plane requires auth |

If `SHIKIGAMI_CONTROL_PLANE` is missing (typical on forks), the job **succeeds
as a no-op** and does not fail CI. Maintainers can also run the workflow via
**Actions → Nightly Live Plane → Run workflow**.

## Run (requires plane + model providers)

```bash
cargo run --bin shikigami -- --config examples/governed-sekai-chisei.toml \
  run "say hello via tools" --keep-workspace --timeout-secs 120
```

A plane `require_approval` parks the attempt and returns. Resume with
`shikigami run --resume <run_id>` (no `--answer`). The host re-queries the
same authorization; it does not approve locally.

If the plane is down under `fail_closed`, doctor and run refuse to start
unless a current signed local-model fallback grant is already checkpointed
and `[model.fallback].enabled` is true
([ADR 0007](decisions/0007-governed-local-fallback.md)). Fallback never
replaces the governance adapter and never reports governed success from
local scratch.

## Replay evidence

Content-bound replay uses a new isolated attempt and never treats its local
manifest, evidence bundle, comparison result, or checkpoint as a plane receipt.
A profile that requires governance must verify the bound policy and receipt
evidence before the first model call.

The current sekai-chisei Rust facade does not expose replay-evidence
verification. `Harness::replay` therefore fails closed for required governed
profiles instead of selecting a local or `none` adapter. Deterministic offline
replay and all denial tests remain available without a live service. See
[replay.md](replay.md).

## Harvest (plane-visible run records)

Governed runs emit operation events so outcomes are reconstructable without
local-only state. Mapping table and attribute contract:
**[harvest.md](harvest.md)**.

## Mid-run tool authorization (external-action)

When governance is `sekai-chisei`, each **consequential tool invocation** is
authorized through the plane’s host-executed external-action API
(`AuthorizeExternalAction`) **before** the host runs the tool.

| Tool | External-action? | `risk_class` |
| --- | --- | --- |
| `bash` / `bash_background` / `bash_job_status` / `bash_job_logs` | yes | `destructive` |
| `write_file` / `edit` / `multi_edit` / `apply_patch` | yes | `write` |
| `read_file` / `glob` / `grep` / `web_fetch` | yes | `read` |
| unknown / MCP names | yes | `write` (default) |
| `report` / `escalate` / `todo_write` | no | — (harness-internal terminate, park, or checklist) |

### Decision handling (headless)

| Decision | Host behavior |
| --- | --- |
| `permit` | Redeem the signed permit, then execute only after redemption succeeds |
| `deny` | Do **not** execute; surface denial on the tool result / events |
| `require_approval` | Park at the effect boundary with the plane `approval_id`. Resume re-validates once; execute only under a current permit. Deny, expiry, cancel, and revoke resume with no effect. |
| missing / unknown | Fail closed as denial |
| plane unavailable / transport / build / redeem error | Fail closed (tool not executed) |

The sekai-chisei adapter never fail-opens mid-run tool authorization: plane
connect/RPC/redeem failures deny the tool even when `governance.fail_closed`
is false. `fail_closed` / profile `governed` still gate doctor and run start.

### Approval-park observation (live behavior, not the `approval_state` poll)

[Discussion #291](https://github.com/Sannrox/shikigami/discussions/291) designed
observation as a stable `GovernancePort::approval_state(approval_id)` poll.
`sekai-chisei` does not implement that trait method (it stays on the default,
unsupported), and nothing in the resume path calls it. Resume instead
re-sends the original `AuthorizeExternalAction` request (same `deadline_ms`,
same `request_digest`) and remaps the response:

| Plane `decision` | Harness `ApprovalState` |
| --- | --- |
| `permit` | `Approved` |
| `require_approval` | `Pending` |
| `deny` with `cancelled_at_ms > 0` | `Cancelled` |
| `deny`, reason contains `expir` (case-insensitive) | `Expired` |
| `deny`, reason contains `revok` | `Revoked` |
| `deny`, reason contains `cancel` | `Cancelled` |
| `deny`, none of the above | `Denied { reason }` |
| anything else | `Denied { reason: "unexpected decision ..." }` |

`ExternalActionDecision` carries no structured approval-status field, so the
`Expired` / `Revoked` / `Cancelled` classes above depend on English substrings
in the plane's free-text `reason`. A revoke whose reason does not contain
`revok` (or a differently-worded expiry or cancellation) is classified as a
plain `Denied` instead. The host effect is identical either way — the tool
never executes — but the reported reason class and any telemetry built on it
can be wrong. Resolving this precisely needs either a plane-exposed,
approval-stable read or a structured status field on the decision; until
then, this table is the adapter's actual behavior, not the Discussion #291
design.

Offline adapters (`none`, `local`) do **not** call external-action; they only
enforce the local tool allow-list.

Action type is `shikigami.tool.<name>.<risk_class>/v1`, with the risk class
derived from the tool (`read`, `write`, or `destructive`). Arguments are
summarized as a SHA-256 digest (`canonical_arguments_digest`); full args stay
on the host. A permitted action also binds the executor, harness, argument
digest, project-scoped target selector, host capability, and idempotency key;
the host redeems that signed permit through `RedeemExternalActionPermit`
before running the tool.

### Tests

- Unit tests cover decision interpretation and tool/risk mapping
  (`src/governance/sekai_chisei.rs` tests).
- Live plane coverage remains under `tests/plane_live.rs` (ignored unless
  `SEKAI_LIVE=1`).
