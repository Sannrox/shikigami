# Examples

Sample settings and packaging manifests. Copy and edit; do not commit secrets.

## Examples by purpose

| Purpose | Artifact | Role |
| --- | --- | --- |
| Library contract proof | [`embed_smoke.rs`](embed_smoke.rs) | Advanced in-process host (`Harness` + events + export). Gated on PR/`main` CI. |
| Operator CLI | [`local-run.toml`](local-run.toml) | Offline profile for `doctor` / `run` demos |
| Local HTTP model | [`cliproxy-http.toml`](cliproxy-http.toml) | Ungoverned `http` adapter through a loopback OpenAI-compatible gateway (CLIProxyAPI) |
| Optional MCP host | [`mcp-host.example.json`](mcp-host.example.json) | Cursor/Claude Desktop-style stdio config for `shikigami mcp` |
| Governed wiring | [`governed-sekai-chisei.toml`](governed-sekai-chisei.toml) | Plane profile (needs reachable sekai-chisei) |
| Governed ontology read → Action → receipt over MCP | [`governed-mcp-action.toml`](governed-mcp-action.toml) | Scripted run against `sekai-mcp`; local recipe in [docs/mcp.md](../docs/mcp.md) |
| Linux OS sandbox | [`linux-native-sandbox.toml`](linux-native-sandbox.toml) | `linux_native` (Landlock + seccomp) on Bash children; Linux only |
| Delivery only | [`tenkai-product.toml`](tenkai-product.toml) | Packaging manifest; **not** loaded by the harness |
| Plane worker fleet sketch | [`k8s-worker-lifecycle.yaml`](k8s-worker-lifecycle.yaml) | Readiness/liveness/SIGTERM drain for managed plane workers |

See [docs/embedding.md](../docs/embedding.md) (host selection + freeze list) and
[docs/mcp.md](../docs/mcp.md) (MCP tools including `run_start` / `run_status` / `run_wait`).

## Offline demo

```bash
cargo run --bin shikigami -- --config examples/local-run.toml doctor
cargo run --bin shikigami -- --config examples/local-run.toml run "demo" --keep-workspace
cargo run --locked --example embed_smoke
```

## Governed doctor

```bash
export SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051
cargo run --bin shikigami -- --config examples/governed-sekai-chisei.toml doctor
```

`run` on the governed profile requires a healthy plane and whatever model
providers that plane is configured to use.

## Local HTTP gateway (CLIProxyAPI)

The `http` model adapter speaks OpenAI Chat Completions. Point `base_url` at a
loopback gateway and put the **gateway** access key in an env var named by
`api_key_env`. This is not a new adapter.

```bash
export CLIPROXY_API_KEY="…"   # CLIProxyAPI access.api-keys, not an xAI token
cargo run --bin shikigami -- --config examples/cliproxy-http.toml doctor
cargo run --bin shikigami -- --config examples/cliproxy-http.toml tui
```

`model` must exist on `GET {base_url}/models`. Override with `--model` /
`SHIKIGAMI_MODEL`. Direct HTTP `auto` still maps to `gpt-4.1-mini`.

The example allowlists `127.0.0.1` so the harness HTTP client can reach the
gateway and nothing else. Widen `network.allow_hosts` if you add `web_fetch`.
For TUI over a project tree, set `workspace.adapter = "inplace"` and put
`--state` / `SHIKIGAMI_STATE` outside that tree.

## Tenkai note

`tenkai-product.toml` is **not** loaded by the harness. It is an example of how
an operator might publish the `shikigami` binary as a product. See
[tenkai](https://github.com/Sannrox/tenkai).
