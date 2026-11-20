# Design

Founding architecture for **shikigami** (式神), a local-first headless agent
harness. Companion documents: [VISION.md](VISION.md),
[docs/settings.md](docs/settings.md),
[ADR 0001](docs/decisions/0001-ports-and-settings.md).

## Purpose

Shikigami executes agent **runs**:

1. Materialize an isolated workspace.
2. Drive a model turn loop through a local `ModelPort`.
3. Execute jailed tools.
4. Emit harness-local progress events.
5. Optionally export identity-only run/turn/tool spans when `[tracing]` is enabled.
6. Park on session `ask` (ACP `session/request_permission`) and resume from the
   host's answer.
7. Complete with a structured outcome.

It does **not** own:

- durable operational graph truth, policy, budgets, or eval judgment
  (the caller, typically rusui, and sekai-chisei when that caller uses a plane);
- release catalogs or environment convergence (delivery tools such as tenkai);
- human chat UX (operator shells and IDEs).

## Why a separate product

A harness that only exists inside a desktop app cannot be:

- tested headlessly as the system of record for execution behavior;
- run unattended in CI or on fleet hosts without a GUI;
- versioned and installed as a delivery product independent of a UI.

Shikigami is that extractable execution plane. UIs and rusui may embed or
drive it over ACP; they own policy. They must not fork the turn loop.

## Architecture

Accepted in [ADR 0001](docs/decisions/0001-ports-and-settings.md): **ports +
settings**, amended by
[ADR 0024](docs/decisions/0024-local-guest-rusui-plane-caller.md). The core
never calls a control plane; settings select local adapters. Rusui is the
plane caller.

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
               git-worktree none
```

Sandbox backends (`none` / `rlimit` / `linux_native`) are settings-selected
isolation for spawned children, not a `*Port` trait.

The engine always calls `ModelPort`. Session `ask=park` and `http-callback`
authorize tools. Org policy and harvest live in rusui.

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
| **Caller** | Process that starts the guest (rusui over ACP, CLI, TUI, embedder) |
| **Host** | CLI, embedder, MCP server, `serve` daemon, ACP guest, or TUI |

## State ownership

| State | Owner |
| --- | --- |
| Operations, harvests, evidence, outcomes (when a plane is used) | Caller (rusui) |
| Policy, budget, routing, approvals, eval | Caller (rusui / sekai-chisei) |
| Release/channel identity of the binary | Delivery system (e.g. tenkai) |
| Host config, run scratch, workspace paths, local event logs | Shikigami (`~/.shikigami/projects/<encoded-cwd>`, or `--state`) |

Harness-local state is scratch for execution. Receipts belong to the caller.

Content-bound replay is a new isolated run attempt with local comparative
evidence. It is not checkpoint resume. See [ADR 0005](docs/decisions/0005-governed-run-replay.md).

## Run lifecycle

```text
create run id
  → materialize workspace
  → governance.begin_run (local handle)
  → loop until terminal | limit:
        ModelPort.next_turn
        authorize + execute tools (workspace jail for in-process FS;
        sandbox backend for spawned children; session ask or host-authz)
  → governance.complete_run
  → emit local events / optional identity-only spans / exit
```

Default tools (when allow-list empty): `read_file`, `write_file`, `edit`,
`multi_edit`, `apply_patch`, `glob`, `grep`, `todo_write`, `handoff`, `report`,
`escalate`. **`bash` is opt-in** via settings for safety.

## Module map

| Path | Responsibility |
| --- | --- |
| `src/harness.rs`, `src/harness/diagnosis.rs`, `src/harness/recovery.rs` | Public wiring: config → ports → doctor/run; diagnosis delegates to one private deep recovery module |
| `crates/shikigami-types` (`identity`, `digest`, `atomic`) | Product identity, shared SHA-256 hex/prefixed digests and Unix-ms clocks, atomic file replace |
| `crates/shikigami-engine` | Turn loop (`Engine`), ports, settings, workspace jail, sandbox (settings mapped onto pinned kekkai), scripted model and events adapters, in-process `none`/`local` governance, tools, MCP client, replay, content, checkpoints, registry, metrics, tracing, transcripts |
| `crates/shikigami-http` | OpenAI-compatible HTTP `ModelPort` adapter selected by `Harness::from_config` |
| `src/serve.rs`, `src/serve/queue.rs`, `src/serve/control.rs`, `src/serve/serve_loop.rs` | Thin local-queue host over the private deep filesystem serve loop, filesystem queue lifecycle, and Run Control protocol |
| `src/governance/` | Host adapter `http-callback` (`host-authz` alias) plus `from_config` wiring for `none`/`local` |
| `src/mcp_server/`, `src/acp.rs`, `crates/shikigami-tui` | MCP server host, ACP newline JSON-RPC session host, and the thin TUI ACP client |
| `crates/shikigami-cli` | CLI host (`shikigami` binary) |

## Cargo features

| Feature | Default | Purpose |
| --- | --- | --- |
| `model-http` | on | OpenAI-compatible HTTP model |

Workspace members: `shikigami` (library), `shikigami-cli` (binary),
`shikigami-tui` (interactive host), `shikigami-types` (identity/digest/atomic),
`shikigami-engine` (turn loop and ports), `shikigami-http` (HTTP model adapter).
CLI and TUI features forward onto the library. `cargo run --bin shikigami`
and `cargo test --workspace` stay the contributor commands.

## Security posture (summary)

- No secrets in config files; use env references (`token_env`, `api_key_env`).
- Workspace path jail: no absolute or parent-traversing paths.
- Bash disabled by default tool allow-list.
- `http-callback` / `host-authz` denies on transport or unexpected decisions.
- Bash under `fail_closed` requires `sandbox.backend` other than `none` and `network.egress` other than `unrestricted`.
- Retired `sekai-chisei` / `governed` / site-agent settings fail at resolve.
- Do not commit `.shikigami-state/` or credentials.

Full reporting process: [SECURITY.md](SECURITY.md).

## Roadmap

Shipped in the **1.0** tree (medium contract; see ADR 0004):

- Settings + ports + doctor
- Local scripted/HTTP runs
- Directory, in-place, and git-worktree workspaces
- Embeddable `Harness` API + in-repo/external host proofs
- Park/escalate resume, serve FS queue, metrics, MCP host/client (host-adjacent)
- Local guest cut ([ADR 0024](docs/decisions/0024-local-guest-rusui-plane-caller.md)):
  rusui is the plane caller

Post-1.0 themes (not freeze-core):

- ACP and TUI process hosts, session wait, ask=park, plan write-jail
  ([ADR 0014](docs/decisions/0014-usable-guest-hosts.md)); nested child
  runs ([ADR 0015](docs/decisions/0015-nested-child-runs.md))
- Richer filesystem serve intake and `POST /runs` control surface
- Delivery fleets and adapter ecosystem

## Naming rule

- **Shikigami** = product / harness
- **Run** = unit of work
- Do not use “a shikigami” for an individual agent attempt
