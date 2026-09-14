# MCP

Shikigami speaks MCP in two directions:

| Role | Purpose | Entry |
| --- | --- | --- |
| **Client** | Attach remote MCP tools into the run-loop registry | `[[tools.mcp_servers]]` settings |
| **Server** | Expose harness tools to MCP-native hosts | `shikigami mcp` (stdio): `doctor`, `run`, `run_start`, `run_status`, `run_wait` |

Library embed (`Harness`) is the advanced in-process path and retains its CI
contract proof ([embedding.md](embedding.md), `examples/embed_smoke.rs`). The
MCP server is the process-host path for stdio clients — **not** a multi-tenant
control plane, and **not** part of the 1.0 library freeze surface. Tenkai
delivers the binary only.

## Server (`shikigami mcp`)

Starts a JSON-RPC 2.0 server with `Content-Length` framing on **stdio only**.
No network bind in v1 — do not pipe this to an open TCP port without your own
authenticated boundary.

```bash
# Optional: same --state / --config as other subcommands
shikigami --state ./state mcp
```

Hosts should connect via MCP stdio (for example Cursor / Claude Desktop style
config pointing at the `shikigami` binary with args `mcp`). See
[examples/mcp-host.example.json](../examples/mcp-host.example.json).

### Tools

| Tool | Arguments | Result |
| --- | --- | --- |
| `doctor` | (none) | Pretty-printed doctor JSON (`schema_version` = 1). Secrets redacted as in the CLI. `isError` when `ok` is false. |
| `run` | `task` (string, required unless resume), `keep_workspace` (bool), `timeout_secs` (u64), `resume_run_id`, `resume_answer` | **Blocking** run summary. Prefer async tools for long work. |
| `run_start` | same as `run` | Starts a **single-flight** background run (non-blocking). |
| `run_status` | (none) | `phase` (`idle`/`running`/`finished`), recent event lines, `result` when finished. |
| `run_wait` | optional `timeout_secs` | Blocks until finished (or timeout while still `running`). |

Cancellation: process-level only in v1 (kill the MCP server). No multi-tenant job queue.

Default local profile needs **no** governance plane. Serve-queue administration
is intentionally out of scope for v1 MCP tools.

### Example client session (conceptual)

1. `initialize` → server capabilities + `serverInfo` (instructions mention async poll tools)
2. `notifications/initialized`
3. `tools/list` → `doctor`, `run`, `run_start`, `run_status`, `run_wait`
4. `tools/call` name=`doctor`
5. Short work: `tools/call` name=`run` arguments=`{"task":"…","keep_workspace":true}`
6. Long work (preferred):
   - `tools/call` name=`run_start` arguments=`{"task":"…","keep_workspace":true}`
   - poll `run_status` until `phase` is `finished` (or call `run_wait`)
7. Only one background run at a time (**single-flight**); a second `run_start`
   while `running` fails until the first finishes

Example host config (Cursor / Claude Desktop style): 
[examples/mcp-host.example.json](../examples/mcp-host.example.json).

### Security

- Stdio transport only; exposure risk is whoever can launch or attach to the process.
- Doctor uses the same redaction rules as `shikigami doctor --json`.
- Runs honor the same config, authorization, and egress policy as the CLI `run` path.
- Do not treat this as a remote multi-tenant API.

## Client (tool servers)

Attach MCP stdio servers so their tools join the run-loop registry as
`mcp.<server>.<tool>`.

### Settings

```toml
[[tools.mcp_servers]]
name = "demo"
command = "mock"          # offline mock: registers mcp.demo.echo
args = []
```

For a real stdio server (the MCP stdio specification frames one JSON-RPC
message per line, so set `framing = "newline"`; the default `content-length`
keeps compatibility with LSP-style hosts such as `shikigami mcp`):

```toml
[[tools.mcp_servers]]
name = "filesystem"
transport = "stdio"       # default
framing = "newline"       # MCP stdio spec; default "content-length"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/path"]
timeout_secs = 30         # per tools/list + tools/call deadline
```

Responses are accepted in either framing regardless of the setting.

HTTP JSON-RPC (POST) transport:

```toml
[[tools.mcp_servers]]
name = "remote"
transport = "http"
url = "https://mcp.example.com/mcp"
token_env = "MCP_TOKEN"   # optional Bearer
```

HTTP URLs are checked with `[network]` egress (`deny` / `allowlist` / `unrestricted`).
`timeout_secs` (default 30) bounds every request on both transports. Full SSE
streaming is not required for v1 list/call.

### Result and failure projection

| Server response | Harness outcome |
| --- | --- |
| `result.content[].text` | Joined text is the tool result (`ok = true`) |
| `result.isError = true` | Failed tool call; the projected error body is the failure detail (`ok = false`) |
| JSON-RPC `error` | Failed tool call |
| No response within `timeout_secs` | Failed tool call `deadline_exceeded`; the remote side may still have completed — reconcile through the server's receipt surface before retrying |
| Deadline expires while the request is still being written (server stopped reading stdin) | That call fails `deadline_exceeded`; every later call on the attachment fails `stdin desynchronized` and the server is killed, so a partial frame is never completed by the next request |

Denials, conflicts, and deadlines therefore reach governance reports and
`ToolEnd` events as failures, never as a successful call whose text happens to
describe an error.

### Client security

- Tools execute through the same `authorize_tool` path when governed.
- Prefer `tools.mode` without bash when combining MCP and least privilege.
- Stdio MCP children use the same reconstructed environment as Bash: harness
  `governance.token_env`, HTTP `model.api_key_env`, and MCP `token_env` names
  are removed before spawn (children do not inherit plane/model secrets).
- HTTP MCP transports consult `[network]` egress policy.
- Stdio responses are decoded by a dedicated reader so a deadline cannot leave
  the stream mid-frame; a late response is discarded by request id. The reader
  holds at most 64 decoded frames ahead of the next call, after which the pipe
  applies backpressure to the server instead of growing harness memory.

### Offline tests

`command = "mock"` registers a deterministic echo tool without spawning MCP.

## Governed ontology reads and Action calls (`sekai-mcp`)

sekai-chisei ships `sekai-mcp`, a stdio projection host over three native
RPCs: `sekai.objects.get` (`GetObject`), `sekai.actions.submit`
(`SubmitActionInstance`), and `chisei.receipt.read` (`GetOperationReceipt`).
Attaching it lets a run consume the same object and Action contracts as an
ordinary SDK client. Shikigami adds no ontology or policy model of its own:
the plane owns objects, admission decisions, and receipts; the harness owns
the run, its tool authorization, and run/tool correlation.

Two authorization layers stay in force and either one alone prevents the
effect:

| Layer | Owner | Denial surface |
| --- | --- | --- |
| Harness tool authorization (`tools.enabled`, governance `authorize_tool`) | Shikigami | Tool never reaches the server; `ToolEnd.ok = false`, detail `... denies tool ...` |
| Native admission (namespace access, policy, budget) | sekai-chisei | `sekai.actions.submit` returns `instance.status = "denied"` with `deny_reason`, or a `permission_denied` tool error |

Every call carries the plane operation identity the run was started with:

```json
{"operation_id": "<logical_operation_id>", "input": {"id": "widget-1"}}
{"operation_id": "<logical_operation_id>", "input": {"type_id": "review.intake", "version": "1.0.0", "parameters_json": "{\"summary\":\"approve\"}", "idempotency_key": "<logical_operation_id>/review.intake"}}
{"operation_id": "<logical_operation_id>", "input": {"operation_id": "<logical_operation_id>"}}
```

The plane binds `request_id` to that operation, so the instance's
`operation_id`, the receipt spine, and the run record's
`logical_operation_id` all name the same operation, while `ToolStart` /
`ToolEnd` events keep `run_id` and `call_id` next to it. Reserved session
metadata (`authorization`, `principal`, `x-sekai-*`, `x-chisei-*`) in tool
arguments is rejected by the server; credentials stay in its process
environment and are never part of a tool call.

Duplicate-effect protection is the plane's idempotency contract, and the
harness must not paper over it:

| Situation | Plane behavior | Required harness behavior |
| --- | --- | --- |
| Same idempotency key, same parameters | `replay: true`, same `instance_id` | Treat as the earlier effect, not a new one |
| Same key, changed parameters | `already_exists` tool error | Report failure; do not invent a new key |
| Access revoked | `tools/list` or the call fails `permission_denied` | Fail closed; nothing to reconcile |
| Deadline or cancellation mid-call | Effect may or may not have been admitted | Read `chisei.receipt.read` for the operation before claiming success or retrying |
| Attempt killed, replacement started | Instance persists under the operation | Replacement reads the receipt, then re-submits with the same key and observes `replay: true` |

### Offline proof

`tests/mcp_governed_action.rs` (`cargo test --test mcp_governed_action`)
runs this matrix without a plane. The test binary re-executes itself as a
stdio server shaped like `sekai-mcp` — newline framing, the three tools,
`operation_id` + `input` arguments, session binding on every dispatch,
reserved-metadata rejection, idempotent replay, digest conflicts, policy
denials, receipts, a durable effect ledger, and a journal of every native RPC
(the reference SDK view). Scripted runs cover the happy path, harness denial,
plane denial, revoked discovery, revoked admission, replay, changed-parameter
conflict, deadline reconciliation, and a SIGKILLed attempt followed by a
replacement under the same operation. Each case asserts the effect ledger
count, compares harness-side transcripts and events with the journal
(object identity, selected Action version, `instance_id`, `request_digest`,
`operation_id`), and checks run/tool correlation.

### Local integration recipe (live plane)

Not run in CI. Requires a local sekai-chisei with a namespace the principal
can read and one enabled governed Action type.

1. Build `sekai-mcp` from the sekai-chisei checkout
   (`cargo build --bin sekai-mcp`) and seed the plane: a namespace, a typed
   object, and a governed Action type (`review.intake@1.0.0` with a
   `summary` parameter in the upstream synthetic example). Keep the bearer
   token in `SEKAI_CREDENTIAL` only.
2. Configure the harness ([examples/governed-mcp-action.toml](../examples/governed-mcp-action.toml)):
   `governance.adapter = "local"` for a harness-only proof or
   `sekai-chisei` for the fully governed path, `framing = "newline"`,
   `timeout_secs = 10`, and `tools.enabled` listing exactly the three
   `mcp.sekai.*` tools plus `report`.
3. Run the scripted read → submit → receipt sequence. The CLI `run` derives
   the operation from the run id; an embedding host sets
   `RunRequest.logical_operation_id`, and plane-admitted work receives it
   through `serve --intake plane`. Use the same value as the `operation_id`
   argument of every MCP call. Keep the workspace and JSONL events.
4. Compare with the reference SDK calls through the plane's admin client or
   CLI: `GetObject(id)` must return the same `id`, `kind`, and `namespace`;
   `GetActionInstance` must show the same `instance_id`, `version`,
   `status`, and `request_digest`; `GetOperationReceipt(operation_id)` must
   attribute the effect to that instance.
5. Repeat the submit with the same key (expect `replay: true`), with changed
   parameters (expect `already_exists`), after revoking the principal's
   namespace grant (expect `permission_denied` and no effect), and once with
   `timeout_secs = 1` against a paused plane (expect `deadline_exceeded`
   followed by a receipt read that settles the outcome).

Record the run id, operation id, and receipt in the change that relies on the
proof; the plane, not the harness transcript, remains the system of record.

## Non-goals (v1)

- Replacing `Harness` embed for in-process hosts
- Full serve-queue administration over MCP
- Governance plane APIs re-exported as MCP
- Multi-tenant / authenticated network MCP server
