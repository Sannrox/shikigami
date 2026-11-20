# Network egress policy

Harness-level controls for **HTTP(S) clients owned by shikigami**:

- `http` model adapter
- MCP HTTP transports
- optional `web_fetch` tool (opt-in via `tools.enabled` / mode allow-list)
- optional `web_search` tool (opt-in via `tools.enabled` plus `[tools.web_search].url`)
- optional OTLP/HTTP tracing export when `[tracing]` is enabled

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

## `web_search` tool

Opt-in builtin (not in the default coding tool set or `read` mode). Enable
with `[tools.web_search].url` **and** `web_search` in `tools.enabled`. The
tool always:

- POSTs JSON `{"query":"..."}` to the configured URL (no browser automation)
- Enforces `[network]` egress on that URL before contacting it
- Caps results (5), field length, response bytes, and time; does not follow redirects
- Sends `token_env` as a Bearer token when set, and never copies it into the tool message

Result URLs are not fetched by search; a later `web_fetch` still applies SSRF.

## Residual risk (bash)

`bash` is **not** interposing network syscalls unless `sandbox.backend =
linux_native` is selected on Linux or `sandbox.backend = seatbelt` on macOS.
Those backends deny child sockets; `[network]` still governs only the
harness-owned HTTP clients above. Prefer `tools.mode = "workspace"` (no bash)
when you need lower network risk without OS isolation, or on hosts that cannot
provide those backends.

MCP HTTP transports already call `NetworkSettings::check_http_url`. See
[mcp.md](mcp.md).

`linux_native` and `seatbelt` deny child sockets unless the job sets
`sandbox.tool_sockets = "host"` (ADR 0013). That grant is host-enforced and
unverified by the harness. An address-level egress allowlist for tool children
remains a separate decision.
