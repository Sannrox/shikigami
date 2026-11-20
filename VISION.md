# Vision

## Purpose

`shikigami` is a headless agent harness: a local-first runtime that executes
**runs** of agent work without requiring a desktop UI.

It is open-source and adapter-based. The product is a local guest: workspace,
turn loop, jailed tools, and ACP. When a control plane is required, the caller
is [rusui](https://github.com/Sannrox/rusui); [sekai-chisei](https://github.com/Sannrox/sekai-chisei)
stays rusui's optional plane. The binary can be delivered by tools
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

The same binary runs offline with local adapters. Session hosts ask before
mutating tools. Org policy, when required, sits on the ACP permission channel
in rusui.

## Principles

1. **Headless by default.** Desktop UI is a client, not the runtime. A
   terminal process host is in-tree and thin
   ([ADR 0014](docs/decisions/0014-usable-guest-hosts.md)).
2. **Shared core.** CLI, `serve`, MCP, ACP, TUI, and advanced embedders
   share one library core (`Harness`); process hosts are the common
   product entry points.
3. **Ports + settings.** Use cases change by configuration, not forks.
4. **Fail closed on trust.** Jail, sandbox, and ask do not silently degrade.
   A retired plane adapter does not become a local run.
5. **Runs are the unit of work.** The product name is not the instance name.

## Path to 1.0

The **1.0 must-have / won't-have** contract and ordered post-1.0 themes live in
[docs/decisions/0004-v1-contract.md](docs/decisions/0004-v1-contract.md)
(medium 1.0: library + serve + park/resume + harvest; not a SaaS control plane).
Freeze-core surfaces follow semver from crate `1.0.0`.
6. **Local scratch, caller truth.** Harness state holds workspaces and
   logs; durable operational facts belong to the caller (rusui) when a plane
   is used.
7. **Safe tool defaults.** Bash and high-risk tools are opt-in via settings.

## Non-goals

- Replacing a governance control plane (e.g. sekai-chisei)
- Replacing a delivery control plane (e.g. tenkai)
- Multi-tenant SaaS control plane (not a 1.x product goal)
- Shipping a desktop shell in this repository
- Dynamic plugin marketplaces (not a 1.x product goal; in-tree + Cargo features first)

## Success signals

| Audience | Signal |
| --- | --- |
| OSS contributor | Clone, `cargo test`, offline `run` succeeds with no plane |
| Operator | `doctor` explains effective adapters; a named plane adapter fails at resolve |
| Integrator | Uses CLI/MCP/serve by default; ACP or TUI when a session guest is required; embeds `Harness` when in-process control is required |
| Production (stack) | Rusui spawns this guest over ACP; binary deliverable via tenkai |

## Status

**1.0** (medium contract), amended by
[ADR 0024](docs/decisions/0024-local-guest-rusui-plane-caller.md).
Boundaries in [DESIGN.md](DESIGN.md) and
[ADR 0001](docs/decisions/0001-ports-and-settings.md) (ports + settings)
remain; freeze-core vs evolving surfaces are listed in
[docs/embedding.md](docs/embedding.md) and ADR 0004.
