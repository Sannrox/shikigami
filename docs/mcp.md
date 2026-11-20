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
delivers the binary only. Skills plus MCP are the extension surface; there is
no plugin loader ([ADR 0018](decisions/0018-skills-mcp-extension.md)).

## Server (`shikigami mcp`)

Starts a JSON-RPC 2.0 server with `Content-Length` framing on **stdio only**.
No network bind in v1 — do not pipe this to an open TCP port without your own
authenticated boundary.

```bash
# Optional: same --state / --config as other subcommands
shikigami mcp
```

Hosts should connect via MCP stdio (for example Cursor / Claude Desktop style
config pointing at the `shikigami` binary with args `mcp`). See
[examples/mcp-host.example.json](../examples/mcp-host.example.json).

### Tools

| Tool | Arguments | Result |
| --- | --- | --- |
| `doctor` | (none) | Pretty-printed doctor JSON (`schema_version` = 1). Secrets redacted as in the CLI. `isError` when `ok` is false. |
| `run` | `task` (string, required unless resume), `keep_workspace` (bool), `timeout_secs` (u64), `resume_run_id`, `resume_answer` | **Blocking** run summary, with the report `data` when the run gave one. Prefer async tools for long work. |
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

The TUI `/mcp` command (ACP `session/mcp`) lists attached servers for the current session and can disable one until the host exits. The overlay is not persisted; `--continue` attaches from settings again. The MCP gateway cannot be disabled from the TUI.

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

Streamable HTTP transport (one POST per message):

```toml
[[tools.mcp_servers]]
name = "remote"
transport = "http"
url = "https://mcp.example.com/mcp"
token_env = "MCP_TOKEN"   # optional Bearer
```

HTTP URLs are checked with `[network]` egress (`deny` / `allowlist` / `unrestricted`).
`timeout_secs` (default 30) bounds every request on both transports, including
a re-initialize after a lost session.

The client accepts either reply shape the server picks per request: a JSON body
or an event stream. It reads an event stream until the reply to its request
arrives. When `initialize` returns an `Mcp-Session-Id`, every later request
carries it, along with the negotiated `MCP-Protocol-Version`. If the server answers 404 for that session, the client
re-initializes once and retries, without a session if the server no longer
issues one; a second loss fails the call.

The run's tool catalog is fixed when the server attaches. After a
`notifications/tools/list_changed` arrives with a reply, the next call re-lists,
and a call to a tool the server stopped offering fails without being sent.
Each such call re-lists first, so a tool the server offers again works again. An
event stream is bounded by the same 1 MiB limit as a stdio frame. The client does
not follow redirects, so a session id never reaches a host the egress check did
not approve. Tools the server adds
mid-run are not offered until the next run. Servers that only answer with JSON
need no setting change.

### Gateway mode

`[tools.mcp_gateway]` (off when unset) routes every remote MCP tool call
through one governed gateway:

```toml
[tools.mcp_gateway]
url = "https://tools-gateway.example/mcp"
timeout_secs = 30   # optional
```

- The gateway's tools register as `mcp.gateway.<tool>`.
- Every request carries the run's live job credential as the bearer. A run
  without one, or whose grant has expired, fails closed instead of calling.
- An HTTP entry in `tools.mcp_servers` is a configuration error, and the name
  `gateway` is reserved. Local stdio servers are unchanged.
- Third-party tool credentials stay in the gateway; the run never reads or
  stores them.

### Result and failure projection

| Server response | Harness outcome |
| --- | --- |
| `result.content[].text` | Joined text is the tool result (`ok = true`) |
| `result.content[]` of type `resource_link` | Its `uri` is recorded as a run artifact with the tool and call id ([runs.md](runs.md#artifacts-a-run-produced)); the text result is unchanged |
| `result.isError = true` | Failed tool call; the projected error body is the failure detail (`ok = false`) |
| JSON-RPC `error` | Failed tool call |
| No response within `timeout_secs` | Failed tool call `deadline_exceeded`; the remote side may still have completed — reconcile through the server's receipt surface before retrying |
| Deadline expires while the request is still being written (server stopped reading stdin) | That call fails `deadline_exceeded`; every later call on the attachment fails `stdin desynchronized` and the server is killed, so a partial frame is never completed by the next request |

Denials, conflicts, and deadlines therefore reach governance reports and
`ToolEnd` events as failures, never as a successful call whose text happens to
describe an error.

### Client security

- Tools execute through the same `authorize_tool` path.
- Prefer `tools.mode` without bash when combining MCP and least privilege.
- Stdio MCP children use the same reconstructed environment as Bash: harness
  `governance.token_env`, HTTP `model.api_key_env`, and MCP `token_env` names
  are removed before spawn (children do not inherit host/model secrets).
- HTTP MCP transports consult `[network]` egress policy.
- Stdio responses are decoded by a dedicated reader so a deadline cannot leave
  the stream mid-frame; a late response is discarded by request id. The reader
  holds at most 64 decoded frames ahead of the next call, after which the pipe
  applies backpressure to the server instead of growing harness memory.

### Offline tests

`command = "mock"` registers a deterministic echo tool without spawning MCP.
An optional `link` argument comes back as a `resource_link`.

## Plane MCP servers (retired)

Attaching a plane projection host from this crate is not a supported governed
path. Rusui is the plane caller
([ADR 0024](decisions/0024-local-guest-rusui-plane-caller.md)). Generic MCP
stdio/HTTP servers still attach through `[[tools.mcp_servers]]`.

## Non-goals (v1)

- Replacing `Harness` embed for in-process hosts
- Full serve-queue administration over MCP
- Governance plane APIs re-exported as MCP
- Multi-tenant / authenticated network MCP server
