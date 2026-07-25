# ADR 0001: Ports and settings (sekai-chisei first-party)

- Status: Accepted
- Date: 2026-07-25
- Supersedes: —

## Context

Shikigami is an open-source headless agent harness. The primary production path
uses sekai-chisei for governance, but the product must remain usable and
testable without that plane, and other use cases must be selectable without
forking the turn loop.

Hard-wiring sekai-chisei into the core would make OSS adoption and CI painful.
Treating a delivery system (e.g. tenkai) as a runtime peer would confuse
install/upgrade with governance.

## Decision

1. **Ports + settings.** The harness core owns run lifecycle, workspace
   materialization, tools, and the turn loop. External capabilities are reached
   through versioned ports selected by settings.
2. **First-party adapters in-tree:** governance `none`, `local`, and
   `sekai-chisei` (best-supported production path). Workspace, model, and event
   adapters follow the same pattern.
3. **Settings-driven use cases.** Profiles (`local`, `governed`) and explicit
   adapter ids resolve from defaults → file → env → CLI flags. Users change
   behavior by configuration, not by patching the loop.
4. **Fail closed when required.** A `governed` profile (or explicit
   `fail_closed`) requires a healthy governance adapter; absence is an error.
   A `local` profile may run with `none` or `local` governance.
5. **Delivery is not a runtime port.** Installers such as tenkai never appear
   in harness process settings. Packaging is documented separately.

## Consequences

- Unit and integration tests run without a plane (`local` / `none`).
- The sekai-chisei adapter is feature-gated (`governance-sekai-chisei`); the
  port boundary is mandatory either way.
- Out-of-tree adapters can implement the same traits without forking the turn
  loop (dynamic plugins remain out of scope for v0).
- Doctor reports effective profile and adapter ids so operators can verify
  offline vs governed wiring.

## Rejected alternative

Hard-wire sekai-chisei with compile flags only: faster for one stack, hostile
to open-source and alternate use cases.
