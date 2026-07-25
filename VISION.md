# Vision

## Purpose

`shikigami` is a headless agent harness: a local-first runtime that executes
**runs** of agent work without requiring a desktop UI.

It is open-source and adapter-based. The primary production integration is
[sekai-chisei](https://github.com/Sannrox/sekai-chisei) for governance and
durable operational records. Offline and alternative adapters exist so the
project is useful without that plane. The binary can be delivered by tools
such as [tenkai](https://github.com/Sannrox/tenkai); delivery is outside the
runtime.

## Problem

Agent execution is fragmented:

- Chat apps and IDE plugins own the loop, so headless and fleet use is awkward.
- Governance is missing, optional, or bolted on after model calls already ran.
- CI and unattended hosts reimplement tools and workspaces ad hoc.
- Every UI reinvents execution instead of sharing a testable core.

## Product promise

An operator, CI job, or embedding host can start a **run**. Shikigami
materializes a workspace, drives a model turn loop, executes jailed tools,
emits progress, and returns a structured outcome.

When governance is required, the harness fails closed if the configured plane
is missing or unhealthy. When it is not required, the same binary runs offline
with local adapters.

## Principles

1. **Headless by default.** UI is a client, not the runtime.
2. **Library-first.** CLI and embedders share one core (`Harness`).
3. **Ports + settings.** Use cases change by configuration, not forks.
4. **Fail closed when required.** Governed profiles do not silently degrade.
5. **Runs are the unit of work.** The product name is not the instance name.
6. **Local scratch, external truth.** Harness state holds workspaces and
   logs; durable operational facts belong to the governance plane when used.
7. **Safe tool defaults.** Bash and high-risk tools are opt-in via settings.

## Non-goals

- Replacing a governance control plane (e.g. sekai-chisei)
- Replacing a delivery control plane (e.g. tenkai)
- Multi-tenant SaaS control plane in v0
- Shipping a desktop shell in this repository
- Dynamic plugin marketplaces in v0 (in-tree + Cargo features first)

## Success signals

| Audience | Signal |
| --- | --- |
| OSS contributor | Clone, `cargo test`, offline `run` succeeds with no plane |
| Operator | `doctor` explains effective adapters; governed path fails closed without a plane |
| Integrator | Embeds `Harness` without forking the turn loop |
| Production (stack) | Runs constrained and recorded through sekai-chisei; binary deliverable via tenkai |

## Status

Early pre-1.0. Boundaries in [DESIGN.md](DESIGN.md) and
[ADR 0001](docs/decisions/0001-ports-and-settings.md) are the architectural
source of truth while APIs stabilize.
