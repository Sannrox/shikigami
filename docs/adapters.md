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
| `http-callback` (alias `host-authz`) | stable for host brokers | POSTs tool authz to a host URL; allow/deny with timeout |
| `sekai-chisei` | primary production path | gRPC: probe, `PlanExecution`, `ExecutePlanStream`, operation events |

### HTTP host callback (`http-callback`)

For interactive hosts (for example Aldunis Code PermissionBroker) that cannot
embed the harness library:

```toml
[governance]
adapter = "http-callback"
endpoint = "http://127.0.0.1:9/host-authz"   # host-supplied loopback URL
token_env = "ALDUNIS_PROVIDER_RUN_TOKEN"    # optional Bearer token env
fail_closed = true
```

On each mutating tool authorization the harness POSTs JSON:

```json
{
  "version": "host-authz.request/v1",
  "run_id": "…",
  "operation_id": "…",
  "tool": "write_file",
  "args_json": "{…redacted summary…}"
}
```

Expected response:

```json
{ "decision": "allow" }
```

or `{ "decision": "deny", "message": "…" }` (`behavior` is accepted as an alias
for `decision`). Timeout, HTTP failure, or unexpected decisions always deny
(http-callback never fail-opens mid-run tool authorization).
Read/search/report/todo tools skip the host callback after the local allow-list
check. Requires the `model-http` feature (default) for the HTTP client.

Feature flag: `governance-sekai-chisei` (default **on**) enables the pinned
`sekai-client` Rust facade. It owns typed core-loop helpers, streaming,
operation events, and receipts; the adapter uses its bounded raw escape hatch
for supported RPCs without a typed helper yet. A private plane-session module
applies connection setup, authentication metadata, deadlines, and SDK error
mapping. The plane claim client reuses one connected Channel for
list/claim/lease RPCs and reconnects only after a transport error. The facade
consumes the canonical upstream `sekai-proto`
crate, so Shikigami does not carry a second protocol snapshot. The supported
boundary is `sekai-client` 0.1.x with `sekai-proto` 1.x, pinned in
`Cargo.toml`/`Cargo.lock`; the SDK permits plain HTTP only for loopback
development endpoints, so use HTTPS for non-loopback production planes.

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
| `inplace` (alias `directory-inplace`) | stable for host-selected workspaces | Uses `workspace.root` directly; no per-run directory or automatic cleanup |
| `git-worktree` | stable for v0 | `git worktree add` + branch; cleaned after successful runs |

`inplace` requires an existing directory, does not support snapshots, and
requires the harness state root to be outside the workspace. Hosts must
serialize concurrent runs against the same in-place root.

`git-worktree` requires `git` on `PATH`.

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

API: `ToolRegistry::from_config` → `definitions()` + `execute()`.
The registry is the only execution interface: it owns resolved tool authority
(including bash helpers that share `bash` authority), network policy, ignore
behavior, protected child-process environment names, sandbox policy, and
jailed builtin dispatch. Lower-level `with_builtins*` constructors remain
available for focused adapters and tests. Dynamic native plugins remain out of
scope; future MCP/skill tools register into the same registry without changing
the turn loop.

Governed runs still call `authorize_tool` before `execute` for consequential
tools.

MCP: optional `tools.mcp_servers` — see [mcp.md](mcp.md).

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
