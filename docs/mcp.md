# MCP

Shikigami speaks MCP in two directions:

| Role | Purpose | Entry |
| --- | --- | --- |
| **Client** | Attach remote MCP tools into the run-loop registry | `[[tools.mcp_servers]]` settings |
| **Server** | Expose `doctor` + `run` to MCP-native hosts | `shikigami mcp` (stdio) |

Library embed (`Harness`) remains the preferred in-process host path
([ADR 0001](decisions/0001-ports-and-settings.md)). The MCP server is a thin
CLI host, **not** a multi-tenant control plane. Tenkai delivers the binary only.

## Server (`shikigami mcp`)

Starts a JSON-RPC 2.0 server with `Content-Length` framing on **stdio only**.
No network bind in v1 — do not pipe this to an open TCP port without your own
authenticated boundary.

```bash
# Optional: same --state / --config as other subcommands
shikigami --state ./state mcp
```

Hosts should connect via MCP stdio (for example Cursor / Claude Desktop style
config pointing at the `shikigami` binary with args `mcp`).

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

1. `initialize` → server capabilities + `serverInfo`
2. `notifications/initialized`
3. `tools/list` → `doctor`, `run`
4. `tools/call` name=`doctor`
5. `tools/call` name=`run` arguments=`{"task":"…","keep_workspace":true}`

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

For a real server:

```toml
[[tools.mcp_servers]]
name = "filesystem"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/path"]
```

### Client security

- Tools execute through the same `authorize_tool` path when governed.
- Prefer `tools.mode` without bash when combining MCP and least privilege.
- HTTP MCP transports may later consult `[network]` egress policy.

### Offline tests

`command = "mock"` registers a deterministic echo tool without spawning MCP.

## Non-goals (v1)

- Replacing `Harness` embed for in-process hosts
- Full serve-queue administration over MCP
- Governance plane APIs re-exported as MCP
- Multi-tenant / authenticated network MCP server
