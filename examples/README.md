# Examples

Sample settings and packaging manifests. Copy and edit; do not commit secrets.

## Host surfaces

| Priority | Artifact | Role |
| --- | --- | --- |
| **Primary CI proof** | [`embed_smoke.rs`](embed_smoke.rs) | Offline library host (`Harness` + events + export). Gated on PR/`main` CI. |
| Operator CLI | [`local-run.toml`](local-run.toml) | Offline profile for `doctor` / `run` demos |
| Optional MCP host | [`mcp-host.example.json`](mcp-host.example.json) | Cursor/Claude Desktop-style stdio config for `shikigami mcp` |
| Governed wiring | [`governed-sekai-chisei.toml`](governed-sekai-chisei.toml) | Plane profile (needs reachable sekai-chisei) |
| Delivery only | [`tenkai-product.toml`](tenkai-product.toml) | Packaging manifest; **not** loaded by the harness |

See [docs/embedding.md](../docs/embedding.md) (host ranking + freeze list) and
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

## Tenkai note

`tenkai-product.toml` is **not** loaded by the harness. It is an example of how
an operator might publish the `shikigami` binary as a product. See
[tenkai](https://github.com/Sannrox/tenkai).
