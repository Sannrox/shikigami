# shikigami — headless agent harness on sekai-chisei + tenkai

> Founding design document, v0.1 (2026-07-25). Product name: **shikigami**
> (式神) — the headless force that executes agent work. Sibling to sekai
> (world), chisei (intelligence), and tenkai (deployment / unfolding).

## Purpose

`shikigami` is the **headless agent harness** for the stack:

- start and fence **runs** of agent work;
- materialize isolated workspaces;
- invoke tools under chisei authorization;
- stream progress and harvest evidence into the control plane;
- remain operable as a single local binary without a UI.

It does **not** own policy, budgets, eval judgment, durable operational graph
truth, release catalogs, or human chat UX.

## Why a separate product

Operator shells (onmyoji, bugyo/kiro) need a harness, but a harness that only
exists inside a desktop app cannot be:

- deployed and versioned by tenkai onto fleets and CI;
- tested headlessly as the system of record for execution behavior;
- run on remote or unattended hosts without a GUI.

Shikigami is that extractable execution plane. UIs may embed or drive it; they
must not redefine governance or delivery.

## Non-goals (v0)

- Desktop / chat UX
- Being a second chisei (no local policy brain that bypasses the control plane)
- Being a CI system or tenkai replacement
- Multi-tenant SaaS control plane
- Wrapping third-party agent CLIs as the long-term core (native run loop first)

## Core concepts

| Concept | Meaning |
| --- | --- |
| **Harness** | This product: process that executes runs |
| **Run** | One countable unit of agent work (workspace + attempts + harvest) |
| **Workspace** | Isolated tree (worktree or equivalent) for a run |
| **Control plane** | sekai-chisei: facts, policy, approvals, eval, audit |
| **Delivery** | tenkai: how this binary and its config land on a host |
| **Host** | CLI (`shikigamictl`), future daemon, or UI-embedded adapter |

## Architecture (target)

```
 operator / CI / UI
        │
        ▼
 ┌──────────────────┐     gRPC / local      ┌─────────────────┐
 │  shikigami core   │─────────────────────▶│  sekai-chisei    │
 │  runs, workspace, │     authz, harvest   │  graph, policy,  │
 │  tools, evidence  │                      │  budget, eval    │
 └────────┬─────────┘                      └─────────────────┘
          │
          │ installed & updated by
          ▼
     tenkai product
```

v0 ships an **embedded CLI host** only (`shikigamictl`). Library code stays
host-agnostic so a daemon or UI adapter can share the same core later.

## State ownership

| State | Owner |
| --- | --- |
| Operations, attempts, harvests, evidence, outcomes | sekai-chisei |
| Policy, budget, routing, approval, eval verdicts | sekai-chisei (chisei) |
| Releases / channels for the harness binary | tenkai |
| Local install config, run scratch, workspace paths | shikigami (`.shikigami-state`) |

Harness-local state is never a substitute for graph truth. If the control plane
is required for a run and unavailable, the run fails closed.

## First vertical slice

1. Product identity, local state, `version` / `init` / `doctor` — **done in scaffold**
2. Run record model (local + control-plane registration contract)
3. Workspace materialization (git worktree or directory sandbox)
4. Minimal tool loop with chisei external-action / capability path
5. Harvest of attempt outcome into sekai
6. tenkai product manifest so the harness installs as a release

## Open questions

- Exact gRPC surface for run registration vs reusing PlanExecution / ExecutePlan
- Whether remote daemon mode is a second binary or a `shikigamictl serve` mode
- How much of onmyoji-core’s worker loop is donated vs rewritten
- Default trust profile when running unattended under tenkai

## Naming rule

- **Shikigami** = this product (harness)
- **Run** (preferred) / worker / session = one unit of work
- Do not use “a shikigami” for an individual agent attempt
