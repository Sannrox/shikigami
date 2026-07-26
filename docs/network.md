# Network egress policy

Harness-level controls for **HTTP(S) clients owned by shikigami**:

- `http` model adapter
- optional `web_fetch` tool (opt-in via `tools.enabled` / mode allow-list)

Path jail does not constrain the network.

## Settings (`[network]`)

| Field | Default | Description |
| --- | --- | --- |
| `egress` | `unrestricted` | `unrestricted` \| `deny` \| `allowlist` |
| `allow_hosts` | `[]` | Exact hostnames when `egress = allowlist` |

## `web_fetch` tool

Opt-in builtin (not in the default coding tool set). Enable with e.g.:

```toml
[tools]
enabled = ["read_file", "write_file", "edit", "glob", "grep", "todo_write", "web_fetch", "report", "escalate"]
```

or `mode = "custom"` with that list. `web_fetch` always:

- Uses HTTP(S) GET only (no browser automation)
- Enforces `[network]` egress
- Blocks private / link-local / loopback hosts **even when** `egress = unrestricted` (SSRF baseline)
- Caps response size and time; limits redirects

This is not an OS sandbox and does not replace container/seccomp isolation.

## Residual risk (bash)

`bash` is **not** interposing network syscalls. A process with bash enabled can
still open sockets unless an **OS sandbox** (containers, seccomp, network NS)
is applied outside the harness. Prefer `tools.mode = "workspace"` (no bash)
when you need lower network risk without OS isolation.

## Future

MCP HTTP/SSE transports should call the same `NetworkSettings::check_http_url`.
