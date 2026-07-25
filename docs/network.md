# Network egress policy

Harness-level controls for **HTTP(S) clients owned by shikigami** (today: the
`http` model adapter). Path jail does not constrain the network.

## Settings (`[network]`)

| Field | Default | Description |
| --- | --- | --- |
| `egress` | `unrestricted` | `unrestricted` \| `deny` \| `allowlist` |
| `allow_hosts` | `[]` | Exact hostnames when `egress = allowlist` |

## Residual risk (bash)

`bash` is **not** interposing network syscalls. A process with bash enabled can
still open sockets unless an **OS sandbox** (containers, seccomp, network NS)
is applied outside the harness. Prefer `tools.mode = "workspace"` (no bash)
when you need lower network risk without OS isolation.

## Future

MCP client tools (#57) should call the same `NetworkSettings::check_http_url`
for HTTP transports.
