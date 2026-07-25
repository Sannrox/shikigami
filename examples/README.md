# Examples

Sample settings and packaging manifests. Copy and edit; do not commit secrets.

| File | Purpose |
| --- | --- |
| [`local-run.toml`](local-run.toml) | Offline profile: local governance + scripted model |
| [`governed-sekai-chisei.toml`](governed-sekai-chisei.toml) | Production-style plane wiring (needs a reachable sekai-chisei) |
| [`tenkai-product.toml`](tenkai-product.toml) | Example **delivery** manifest for installing the binary via tenkai |

## Offline demo

```bash
cargo run --bin shikigami -- --config examples/local-run.toml doctor
cargo run --bin shikigami -- --config examples/local-run.toml run "demo" --keep-workspace
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
