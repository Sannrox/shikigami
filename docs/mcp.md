# MCP client (tool servers)

Shikigami can attach **MCP stdio servers** so their tools join the run-loop
registry as `mcp.<server>.<tool>`.

## Settings

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

## Security

- Tools execute through the same `authorize_tool` path when governed.
- Prefer `tools.mode` without bash when combining MCP and least privilege.
- HTTP MCP transports may later consult `[network]` egress policy.

## Offline tests

`command = "mock"` registers a deterministic echo tool without spawning MCP.
