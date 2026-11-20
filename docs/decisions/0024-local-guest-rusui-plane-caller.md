# ADR 0024: Shikigami is a local guest; rusui is the plane caller

- Status: Accepted
- Date: 2026-11-20
- Supersedes: ADR 0001 clause 2 (first-party `sekai-chisei` adapter) and
  ADR 0004's governed-path freeze row
- Amends: [0001](0001-ports-and-settings.md), [0002](0002-run-identity.md),
  [0004](0004-v1-contract.md), [0005](0005-governed-run-replay.md),
  [0007](0007-governed-local-fallback.md)
- Source: [Discussion #674](https://github.com/Sannrox/shikigami/discussions/674)
  (accepted); Issue [#675](https://github.com/Sannrox/shikigami/issues/675)

## Context

ADR 0001 made `sekai-chisei` a first-party governance adapter so the turn loop
could fail closed on a plane. `GovernancePort` then owned admission, model
turns (`PlanExecution`), per-tool `AuthorizeExternalAction`, harvest, and
fallback. Rusui already governs guests from outside: it spawns
`shikigami acp`, injects a model-proxy URL and per-turn grant, and maps
`session/request_permission` to policy. Two plane clients existed. The in-tree
one is the extra product.

## Decision

1. **Shikigami is a local-first guest harness.** It materializes a workspace,
   drives a model turn loop, executes jailed tools, parks on ask, and emits
   progress. It does not call a governance plane.
2. **Rusui is the plane caller.** Mid-run policy, model proxying, receipts, and
   any sekai-chisei RPCs live in rusui. The intercept rusui already has is ACP
   plus the model proxy.
3. **The engine always calls `ModelPort`.** `GovernancePort::plan_turn` is
   removed. A plane-routed model is an HTTP endpoint the host injects.
4. **Session `ask=park` stays the mutating-tool gate** for ACP/TUI.
   `none` / `local` remain in-process allow-list adapters. `http-callback` /
   `host-authz` stays for non-ACP process hosts.
5. **Removed from this repository:** crate `shikigami-governance-sekai`, crate
   `shikigami-plane-intake`, feature `governance-sekai-chisei`,
   `serve --intake plane`, agent-turn intake, profile `governed`, and the
   site-agent plane-worker family (`site-agent`, `security-triage`).
6. **Ports + settings remain** for model, workspace, events, sandbox, and the
   remaining governance adapters. Delivery (tenkai) remains not a runtime port.

## Consequences

- `doctor` and `run` never probe a control plane.
- Settings that name `adapter = "sekai-chisei"` or profile `governed` /
  `site-agent` / `security-triage` fail at resolve with an upgrade error.
- Offline `cargo test` is the whole default suite. There is no `sekai-client`
  pin.
- Local jail, sandbox, allow-list, hooks, and ACP `session/request_permission`
  are unchanged.
- Filesystem `serve` (ADR 0003) stays. Plane-claim intake does not.
- Follow-up plane RPCs, if any, are rusui work.

## Rejected alternatives

1. **Keep the in-tree `sekai-chisei` adapter.** Two plane clients; this crate
   stays a control-plane worker.
2. **Move the adapter out of tree and keep `GovernancePort.plan_turn`.** The
   pin leaves; the inverted loop stays. The engine would still treat the plane
   as the model.
3. **Always-approve ACP so rusui need not answer permission.** Rejected by
   ADR 0014 and rusui ADR 0002.
