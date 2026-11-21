# Adapters

Shikigami selects backends through **ports**. Built-in adapters are listed
below. Out-of-tree adapters can implement the same traits (dynamic plugins are
out of scope for 1.x; use a library dependency or fork of the adapter module).

Architectural decision: [decisions/0001-ports-and-settings.md](decisions/0001-ports-and-settings.md).
The engine always calls `ModelPort`. Rusui is the plane caller
([ADR 0024](decisions/0024-local-guest-rusui-plane-caller.md)).

## Governance (`GovernancePort`)

| Id | Status | Role |
| --- | --- | --- |
| `none` | stable | No external broker; local model path; tool allow-list only |
| `local` | stable | In-process tool allow-list for deterministic tests |
| `http-callback` (alias `host-authz`) | stable for host brokers | POSTs tool authz to a host URL; allow/deny with timeout |

Adapter `sekai-chisei` and profile `governed` were removed. Settings that still
name them fail at resolve.

### HTTP host callback (`http-callback`)

For interactive hosts that cannot embed the harness library:

```toml
[governance]
adapter = "http-callback"
endpoint = "http://127.0.0.1:9/host-authz"   # host-supplied loopback URL
token_env = "HOST_RUN_TOKEN"                 # optional Bearer token env
namespace = "default"                        # required for remote adapters
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

ACP session hosts use `session/request_permission` instead of this callback
([acp.md](acp.md)).

### Fail-closed behavior

When `fail_closed` is set, or a remote adapter (`http-callback` / `host-authz`)
is selected:

- missing endpoint → doctor fail, run fail
- unreachable `http-callback` (async probe) → doctor fail; run fails at
  authorization rather than silently allowing

`doctor` never probes a control plane. Delayed-evidence queueing is opt-in and
never replaces local receipts: disabled `complete_run` preserves `Unavailable`.

### Credentials

Do not put secrets in TOML. Set `governance.token_env` to an environment
variable name that holds the host Bearer token for `http-callback`.

## Model (`ModelPort`)

| Id | Status | Role |
| --- | --- | --- |
| `scripted` | stable | Deterministic multi-turn JSON script (default offline) |
| `http` | stable | OpenAI-compatible Chat Completions (`model-http` feature). Any Chat Completions gateway works, including a local CLIProxyAPI listen; see [`examples/cliproxy-http.toml`](../examples/cliproxy-http.toml). |

Adapter `plane` was removed. Settings that still name it fail at resolve.
A plane-routed model is an HTTP endpoint the host injects.

## Workspace (`WorkspacePort`)

| Id | Status | Role |
| --- | --- | --- |
| `directory` | stable | Sandbox directory under state runs or configured root |
| `inplace` (alias `directory-inplace`) | stable for host-selected workspaces | Uses `workspace.root` directly; no per-run directory or automatic cleanup |
| `git-worktree` | stable | `git worktree add` + branch; cleaned after successful runs |

`inplace` requires an existing directory, does not support snapshots, and
requires the harness state root to be outside the workspace. The CLI default
state root is already outside cwd. Hosts must serialize concurrent runs against
the same in-place root.

`git-worktree` requires `git` on `PATH`.

## Events (`EventSink`)

| Id | Status | Role |
| --- | --- | --- |
| `stderr` | stable | JSON lines on stderr |
| `jsonl` | stable | Append-only file under the state runs directory |
| `none` | stable | Discard events |

Harness events are local progress. Durable operational truth, when a plane is
used, lives in the caller.

## Tools (`ToolRegistry`)

Tools are not selected by a free-form adapter id. The run loop uses a
**registry** bootstrapped with **builtins** filtered by
`[tools].enabled` (see [settings.md](settings.md)).

| Builtin | Role |
| --- | --- |
| `read_file` / `write_file` / `delete_file` / `edit` / `multi_edit` / `apply_patch` | Workspace-jailed file ops. `read_file` takes optional 1-indexed `start_line`/`end_line`; an unranged read of a large file returns a 400-line / 32 KiB slice plus the total line count. `delete_file` unlinks one regular file (directories, missing paths, and jail escapes fail) |
| `glob` / `grep` | Workspace-jailed search (capped matches / output) |
| `todo_write` | Run-scoped checklist |
| `handoff` | Brief event for a fresh session (`task` required); does not start a session or child run |
| `web_fetch` | Opt-in HTTP(S) GET; not in the coding default |
| `web_search` | Opt-in HTTP POST search; not registered until `[tools.web_search].url` is set |
| `bash` | Opt-in shell in workspace (timeout-bounded). Combined stdout/stderr is truncated to `[tools] model_output_max_bytes` (default 64 KiB) with `…[truncated]` before it enters the prompt; 2 MiB remains a memory bound |
| `bash_background` / `bash_job_status` / `bash_job_logs` | Implicit when `bash` is enabled. `bash_job_status` polls once, or waits with optional `timeout_ms` (same cap as foreground bash) |
| `report` / `escalate` | Finish or park; exclusive batch. `report` takes `summary`, `success`, and an optional `data` JSON object for a structured answer |

API: `ToolRegistry::from_config` → `definitions()` + `execute()`.
The registry is the only execution interface: it owns resolved tool authority
(including bash helpers that share `bash` authority), network policy, ignore
behavior, protected child-process environment names, sandbox policy, and
jailed builtin dispatch. Lower-level `with_builtins*` constructors remain
available for focused adapters and tests. Dynamic native plugins remain out of
scope. Configured MCP servers register as `mcp.<name>.<tool>` into the same
registry without changing the turn loop; skills load as prompt context.
MCP tool text is truncated to `[tools] model_output_max_bytes` the same way as bash.

Runs still call `authorize_tool` before `execute` for consequential tools.
Session hosts park on ask (`ask=park`) for mutating tools
([acp.md](acp.md)).

MCP: optional `tools.mcp_servers` — see [mcp.md](mcp.md).

## Not an adapter

| Name | Why |
| --- | --- |
| **rusui** | Plane caller. Spawns this guest over ACP. Not loaded by the process. |
| **tenkai** | Installs/upgrades the `shikigami` binary. Not loaded by the process. |
| **metrics / tracing export** | Settings-gated process telemetry, not ports. Counters stay local; optional OTLP spans are identity-only ([tracing.md](tracing.md)). |
| Third-party agent CLIs | Not the long-term core; native loop first. |

## Implementing a custom governance adapter

1. Implement `shikigami::governance::GovernancePort`.
2. Wire it in a host binary or extend `governance::from_config` in a fork/PR.
3. Keep operational mutations and policy decisions in the caller, not in the
   turn loop.
4. Honor fail-closed semantics when the host profile requires governance.

See [embedding.md](embedding.md) for host integration.
