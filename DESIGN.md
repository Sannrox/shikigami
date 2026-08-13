# Design

Founding architecture for **shikigami** (式神), a local-first headless agent
harness. Companion documents: [VISION.md](VISION.md),
[docs/settings.md](docs/settings.md),
[ADR 0001](docs/decisions/0001-ports-and-settings.md).

## Purpose

Shikigami executes agent **runs**:

1. Materialize an isolated workspace.
2. Drive a model turn loop (locally or through a governance plane).
3. Execute jailed tools.
4. Emit harness-local progress events.
5. Complete with a structured outcome (and optional plane reporting).

It does **not** own:

- durable operational graph truth, policy, budgets, or eval judgment
  (governance plane, when used);
- release catalogs or environment convergence (delivery tools such as tenkai);
- human chat UX (operator shells and IDEs).

## Why a separate product

A harness that only exists inside a desktop app cannot be:

- tested headlessly as the system of record for execution behavior;
- run unattended in CI or on fleet hosts without a GUI;
- versioned and installed as a delivery product independent of a UI.

Shikigami is that extractable execution plane. UIs may embed or drive it; they
must not redefine governance or the turn loop.

## Architecture

Accepted in [ADR 0001](docs/decisions/0001-ports-and-settings.md): **ports +
settings**. The core never hard-wires a control plane; settings select
adapters. `sekai-chisei` is the first-party production governance adapter.

```text
  operator / CI / embedder
            │
            ▼
   ┌──────────────────────┐
   │  shikigami core       │
   │  Harness · Engine     │
   │  run · tools · prompt │
   └──────────┬───────────┘
              │ ports (selected by settings)
     ┌────────┼────────┬──────────┐
     ▼        ▼        ▼          ▼
 governance    model  workspace   events
 none/local    scripted directory stderr
 http-callback http    inplace   jsonl
 sekai-chisei  plane   git-worktree none
```

When governance is `sekai-chisei`, model turns use the plane
(`PlanExecution` / `ExecutePlanStream`). Direct model adapters apply to
ungoverned profiles only.

**Tenkai** (or any installer) may ship the binary. It is not a runtime port and
must not appear in harness process settings.

### Process shapes

| Shape | Role |
| --- | --- |
| Library (`Harness`) | Embeddable API for hosts |
| CLI (`shikigami`) | Thin embedded host over the library |
| Daemon (`shikigami serve`) | Thin long-running host over `Harness`; accepts filesystem-queue or plane-claim intake |

## Core concepts

| Concept | Meaning |
| --- | --- |
| **Harness** | This product: process that executes runs |
| **Run** | One countable unit of work (workspace + turns + outcome) |
| **Workspace** | Run working tree selected by the host (`directory`, `inplace`, or `git-worktree`) |
| **Port** | Versioned boundary (governance, model, workspace, events) |
| **Adapter** | Implementation of a port selected by settings |
| **Governance plane** | Optional external system (e.g. sekai-chisei) for policy and governed model execution |
| **Host** | CLI, embedder, MCP server, or `serve` daemon |

## State ownership

| State | Owner |
| --- | --- |
| Operations, harvests, evidence, outcomes (when governed) | Governance plane |
| Policy, budget, routing, approvals, eval | Governance plane |
| Release/channel identity of the binary | Delivery system (e.g. tenkai) |
| Host config, run scratch, workspace paths, local event logs | Shikigami (`.shikigami-state` / configured paths) |

Harness-local state is never a substitute for plane truth. If governance is
required and unavailable, the run fails closed.

## Run lifecycle

```text
create run id
  → materialize workspace
  → governance.begin_run
  → loop until terminal | limit:
        governance.plan_turn (plane or local model)
        authorize + execute tools (workspace jail)
        governance.report_tool (best-effort / fail-closed)
  → governance.complete_run
  → emit local events / exit
```

Default tools (when allow-list empty): `read_file`, `write_file`, `edit`,
`report`. **`bash` is opt-in** via settings for safety.

## Module map

| Path | Responsibility |
| --- | --- |
| `src/harness.rs`, `src/harness/diagnosis.rs` | Public wiring: config → ports → doctor/run; diagnosis delegates to one private deep module |
| `src/run/` | Thin `Engine` interface over deep run admission and supervision, host-local Run preparation, the Run artifact lifecycle, the durable run transaction, durable model turns and tool batches, resume validation, and `RunSession` checkpoints |
| `src/serve.rs`, `src/serve/queue.rs`, `src/serve/control.rs`, `src/serve/serve_loop.rs` | Thin local-queue host over the private deep filesystem serve loop, filesystem queue lifecycle, and Run Control protocol |
| `src/plane_intake.rs`, `src/plane_intake/` | Claimed-work mapping plus thin `run_plane_serve` over private deep plane serve loop and claimed-run transaction modules |
| `src/governance/` | `none`, `local`, `http-callback` (`host-authz` alias), `sekai-chisei`; the production adapter delegates plane session, governed Run admission, governed model turns, run completion, tool authorization, harvest durability and event reporting, and plane claim acquisition plus lease RPCs to private deep modules |
| `src/tools/`, `src/mcp/`, `src/mcp_server/` | Run-scoped `ToolRegistry` interface over private builtin execution (catalog authority, jailed dispatch, shared bash spawn), private deep MCP tool attachment and background Run lifecycle modules, and shared bounded framing behind the stdio adapter seams |
| `src/workspace.rs` | Directory, in-place, and git-worktree materialization |
| `src/model.rs` | Scripted / HTTP (ungoverned) |
| `src/events.rs` | stderr / jsonl / none |
| `src/config.rs`, `src/config/resolution.rs` | Versioned settings over the private deep effective settings resolution protocol |
| `src/bin/shikigami.rs` | CLI host |
| `sekai-client` dependency | Versioned Rust facade over canonical sekai-chisei gRPC contracts |

## Cargo features

| Feature | Default | Purpose |
| --- | --- | --- |
| `governance-sekai-chisei` | on | SDK-backed sekai-chisei governance adapter |
| `model-http` | on | OpenAI-compatible HTTP model |

## Security posture (summary)

- No secrets in config files; use env references (`token_env`, `api_key_env`).
- Workspace path jail: no absolute or parent-traversing paths.
- Bash disabled by default tool allow-list.
- Fail-closed doctor/run when `governed` / `fail_closed` and plane unhealthy.
- Do not commit `.shikigami-state/`, credentials, or plane tokens.

Full reporting process: [SECURITY.md](SECURITY.md).

## Roadmap

Shipped in the **1.0** tree (medium contract; see ADR 0004):

- Settings + ports + doctor
- Local scripted/HTTP runs
- sekai-chisei PlanExecution path + external-action tool authz + harvest
- Directory, in-place, and git-worktree workspaces
- Embeddable `Harness` API + in-repo/external host proofs
- Park/escalate resume, serve FS queue, metrics, MCP host/client (host-adjacent)

Post-1.0 themes (not freeze-core):

- Richer serve intake beyond the shipped filesystem queue and plane claim path
- Deeper governance-native harvest objects
- Delivery fleets and adapter ecosystem
- Eval / quality-loop harnesses

## Naming rule

- **Shikigami** = product / harness
- **Run** = unit of work
- Do not use “a shikigami” for an individual agent attempt
