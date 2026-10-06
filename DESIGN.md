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
5. Optionally export identity-only run/turn/tool spans when `[tracing]` is enabled.
6. Park on a plane `require_approval` decision and resume after re-validating
   current authority (no host-side self-approval).
7. Complete with a structured outcome (and optional plane reporting).

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

Sandbox backends (`none` / `rlimit` / `linux_native`) are settings-selected
isolation for spawned children, not a `*Port` trait. Plane intake is a
host-side claim port (`PlaneIntakePort`), not a turn-loop port.

When governance is `sekai-chisei`, model turns use the plane
(`PlanExecution` / `ExecutePlanStream`). Direct model adapters apply to
ungoverned profiles only. An opt-in, default-deny local-model fallback may
use one authorized digest only after fail-closed grant verification
([ADR 0007](docs/decisions/0007-governed-local-fallback.md)); the configured
governance adapter is not replaced.

**Tenkai** (or any installer) may ship the binary. It is not a runtime port and
must not appear in harness process settings.

### Process shapes

| Shape | Role |
| --- | --- |
| Library (`Harness`) | Embeddable API for hosts |
| CLI (`shikigami`) | Thin embedded host over the library (`doctor` / `run` / …) |
| Daemon (`shikigami serve`) | Thin long-running host over `Harness`; accepts filesystem-queue or plane-claim intake |
| ACP (`shikigami acp`) | Thin session guest over `Harness` (newline JSON-RPC); evolving, [ADR 0014](docs/decisions/0014-usable-guest-hosts.md) |
| TUI (`shikigami tui`) | Thin interactive host; ACP client of the in-process session; evolving, ADR 0014 |

## Core concepts

| Concept | Meaning |
| --- | --- |
| **Harness** | This product: process that executes runs |
| **Run** | One countable unit of work (workspace + turns + outcome) |
| **Workspace** | Run working tree selected by the host (`directory`, `inplace`, or `git-worktree`) |
| **Port** | Versioned boundary (governance, model, workspace, events; sandbox is settings-selected isolation, not a trait) |
| **Adapter** | Implementation of a port selected by settings |
| **Governance plane** | Optional external system (e.g. sekai-chisei) for policy and governed model execution |
| **Host** | CLI, embedder, MCP server, `serve` daemon, ACP guest, or TUI |

## State ownership

| State | Owner |
| --- | --- |
| Operations, harvests, evidence, outcomes (when governed) | Governance plane |
| Policy, budget, routing, approvals, eval | Governance plane |
| Release/channel identity of the binary | Delivery system (e.g. tenkai) |
| Host config, run scratch, workspace paths, local event logs | Shikigami (`.shikigami-state` / configured paths) |

Harness-local state is never a substitute for plane truth. If governance is
required and unavailable, the run fails closed.

Content-bound replay is a new isolated run attempt with local comparative
evidence. It is not checkpoint resume, and its manifest, result, and checkpoint
never replace a governed receipt. See [ADR 0005](docs/decisions/0005-governed-run-replay.md).

## Run lifecycle

```text
create run id
  → materialize workspace
  → governance.begin_run
  → loop until terminal | limit:
        governance.plan_turn (plane or local model)
        authorize + execute tools (workspace jail for in-process FS;
        sandbox backend for spawned children)
        governance.report_tool (best-effort / fail-closed)
  → governance.complete_run
  → emit local events / optional identity-only spans / exit
```

Default tools (when allow-list empty): `read_file`, `write_file`, `edit`,
`multi_edit`, `apply_patch`, `glob`, `grep`, `todo_write`, `report`,
`escalate`. **`bash` is opt-in** via settings for safety.

## Module map

| Path | Responsibility |
| --- | --- |
| `src/harness.rs`, `src/harness/diagnosis.rs`, `src/harness/recovery.rs` | Public wiring: config → ports → doctor/run; diagnosis delegates to one private deep recovery module |
| `crates/shikigami-types` (`identity`, `digest`, `atomic`) | Product identity, shared SHA-256 hex/prefixed digests and Unix-ms clocks, atomic file replace |
| `crates/shikigami-engine` | Turn loop (`Engine`), ports, settings, workspace jail, sandbox, ungoverned scripted/plane model and events adapters, in-process `none`/`local` governance, tools, MCP client, replay, content, fallback, evidence queue, checkpoints, registry, metrics, tracing, transcripts, worker lifecycle |
| `crates/shikigami-http` | OpenAI-compatible HTTP `ModelPort` adapter selected by `Harness::from_config` |
| `crates/shikigami-governance-sekai` | Production `sekai-chisei` `GovernancePort` plus `PlaneIntakePort`; consumes versioned `sekai-client` |
| `src/serve.rs`, `src/serve/queue.rs`, `src/serve/control.rs`, `src/serve/serve_loop.rs` | Thin local-queue host over the private deep filesystem serve loop, filesystem queue lifecycle, and Run Control protocol |
| `crates/shikigami-plane-intake` | `PlaneIntakePort` and claim values shared by the sekai adapter and plane serve |
| `src/plane_intake.rs`, `src/plane_intake/` | Claimed-work mapping plus thin `run_plane_serve` over private deep plane serve loop and claimed-run transaction (including one lease-safe fenced RPC retry protocol) |
| `src/governance/` | Host adapter `http-callback` (`host-authz` alias) plus `from_config` wiring for `sekai-chisei` |
| `src/mcp_server/`, `src/acp.rs`, `crates/shikigami-tui` | MCP server host, ACP newline JSON-RPC session host, and the thin TUI ACP client |
| `src/eval.rs` | Offline golden-fixture harness (`shikigami eval`) |
| `crates/shikigami-cli` | CLI host (`shikigami` binary) |
| `src/plane_host.rs` | Optional in-process plane host bootstrap for embedders |
| `sekai-client` dependency | Versioned Rust facade over canonical sekai-chisei gRPC contracts; consumed by `shikigami-governance-sekai` |

## Cargo features

| Feature | Default | Purpose |
| --- | --- | --- |
| `governance-sekai-chisei` | on | SDK-backed sekai-chisei governance adapter |
| `model-http` | on | OpenAI-compatible HTTP model |

Workspace members: `shikigami` (library), `shikigami-cli` (binary),
`shikigami-tui` (interactive host), `shikigami-types` (identity/digest/atomic),
`shikigami-plane-intake` (claim port and values), `shikigami-engine` (turn loop
and ports), `shikigami-http` (HTTP model adapter),
`shikigami-governance-sekai` (sekai-chisei adapter).
CLI and TUI features forward onto the library. `cargo run --bin shikigami`
and `cargo test --workspace` stay the contributor commands.

## Security posture (summary)

- No secrets in config files; use env references (`token_env`, `api_key_env`).
- Workspace path jail: no absolute or parent-traversing paths.
- Bash disabled by default tool allow-list.
- Fail-closed doctor/run when profile `governed` or `governance.fail_closed` and plane unhealthy.
- Remote plaintext `http://` governance requires `governance.allow_insecure_remote` (default false).
- Governed / fail-closed Bash requires `sandbox.backend` other than `none` and `network.egress` other than `unrestricted`.
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
- Offline eval golden-fixture harness (`shikigami eval`)

Post-1.0 themes (not freeze-core):

- ACP and TUI process hosts, session wait, ask=park, plan write-jail
  ([ADR 0014](docs/decisions/0014-usable-guest-hosts.md)); nested child
  runs ([ADR 0015](docs/decisions/0015-nested-child-runs.md))
- Richer serve intake beyond the shipped filesystem queue, plane claim path,
  and filesystem `POST /runs` control surface
- Deeper governance-native harvest objects
- Delivery fleets and adapter ecosystem

## Naming rule

- **Shikigami** = product / harness
- **Run** = unit of work
- Do not use “a shikigami” for an individual agent attempt
