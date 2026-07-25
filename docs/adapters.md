# Adapters

Shikigami selects backends through **ports**. Built-in adapters are listed
below. Out-of-tree adapters can implement the same traits (dynamic plugins are
out of scope for v0; use a library dependency or fork of the adapter module).

Architectural decision: [decisions/0001-ports-and-settings.md](decisions/0001-ports-and-settings.md).

## Governance (`GovernancePort`)

| Id | Status | Role |
| --- | --- | --- |
| `none` | stable for v0 | No external plane; local model path; tool allow-list only |
| `local` | stable for v0 | In-process tool allow-list for deterministic tests |
| `sekai-chisei` | primary production path | gRPC: probe, `PlanExecution`, `ExecutePlanStream`, operation events |

Feature flag: `governance-sekai-chisei` (default **on**) compiles the client from
vendored protos in [`../proto/`](../proto/).

### Fail-closed behavior

When `fail_closed` or profile `governed` is set:

- missing endpoint → doctor fail, run fail
- unreachable plane (async probe) → doctor fail; run fails at begin/plan

When not fail-closed, some reporting steps may best-effort skip if the plane is
down (see implementation of `complete_run` / `report_tool`).

### Credentials

Do not put tokens in TOML. Set `governance.token_env` to an environment
variable name (for example `SEKAI_TOKEN`) that holds a raw token or
`Bearer …` value.

## Model (`ModelPort`)

| Id | Status | Role |
| --- | --- | --- |
| `scripted` | stable for v0 | Deterministic multi-turn JSON script (default offline) |
| `http` | stable for v0 | OpenAI-compatible Chat Completions (`model-http` feature) |
| `plane` | with sekai-chisei | Placeholder id; actual turns are owned by governance |

When governance is `sekai-chisei`, the engine uses the plane for planning even if
a local model adapter is configured for other profiles.

## Workspace (`WorkspacePort`)

| Id | Status | Role |
| --- | --- | --- |
| `directory` | stable for v0 | Sandbox directory under state runs or configured root |
| `git-worktree` | stable for v0 | `git worktree add` + branch; cleaned after successful runs |

Requires `git` on `PATH` for `git-worktree`.

## Events (`EventSink`)

| Id | Status | Role |
| --- | --- | --- |
| `stderr` | stable for v0 | JSON lines on stderr |
| `jsonl` | stable for v0 | Append-only file under the state runs directory |
| `none` | stable for v0 | Discard events |

Harness events are **not** a substitute for plane audit records.

## Tools (`ToolRegistry`)

Tools are not selected by a free-form adapter id. The run loop uses a
**registry** bootstrapped with **builtins** filtered by
`[tools].enabled` (see [settings.md](settings.md)).

| Builtin | Role |
| --- | --- |
| `read_file` / `write_file` / `edit` / `multi_edit` | Workspace-jailed file ops |
| `glob` / `grep` | Workspace-jailed search (capped matches / output) |
| `bash` | Opt-in shell in workspace (timeout-bounded) |
| `report` / `escalate` | Finish or park; exclusive batch |

API: `ToolRegistry::with_builtins` → `definitions()` + `execute()`.
Dynamic native plugins remain out of scope; future MCP/skill tools register
into the same registry without changing the turn loop.

Governed runs still call `authorize_tool` before `execute` for consequential
tools.

## Not an adapter

| Name | Why |
| --- | --- |
| **tenkai** | Installs/upgrades the `shikigami` binary. Not loaded by the process. |
| Third-party agent CLIs | Not the long-term core; native loop first. |

## Implementing a custom governance adapter

1. Implement `shikigami::governance::GovernancePort`.
2. Wire it in a host binary or extend `governance::from_config` in a fork/PR.
3. Keep operational mutations and policy decisions in your plane, not in the
   turn loop.
4. Honor fail-closed semantics when the host profile requires governance.

See [embedding.md](embedding.md) for host integration.
