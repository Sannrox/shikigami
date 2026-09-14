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

`bash` is **not** interposing network syscalls unless `sandbox.backend =
linux_native` is selected on Linux. That backend denies `socket()` in the
child; `[network]` still governs only the harness-owned HTTP clients above.
Prefer `tools.mode = "workspace"` (no bash) when you need lower network risk
without OS isolation, or on hosts that cannot provide `linux_native`.

## Future

MCP HTTP/SSE transports should call the same `NetworkSettings::check_http_url`.

An address-level egress allowlist for tool children is a separate decision
([ADR 0013](decisions/0013-os-sandbox-adapter.md)); today `linux_native`
either denies every socket or (later) can delegate sockets to the host.
