# Delivering shikigami with tenkai

**Tenkai installs/upgrades the binary only.** It is not a runtime dependency of
the harness and must never inject process settings. Runtime config stays in
`SHIKIGAMI_*` env vars and TOML (see [settings.md](settings.md)).

## Release assets

GitHub Releases publish archives named:

```text
shikigami-vX.Y.Z-<target>.tar.gz
shikigami-vX.Y.Z-<target>.sha256
```

Targets (`v1.0.0` and later use the same matrix):

| Target | Typical host |
| --- | --- |
| `aarch64-apple-darwin` | Apple Silicon macOS |
| `x86_64-apple-darwin` | Intel macOS |
| `x86_64-unknown-linux-gnu` | Linux x86_64 |
| `aarch64-unknown-linux-gnu` | Linux aarch64 |

Example download:

```bash
VERSION=v1.0.0
TARGET=x86_64-unknown-linux-gnu
BASE=https://github.com/Sannrox/shikigami/releases/download/${VERSION}
curl -fsSL -O "$BASE/shikigami-${VERSION}-${TARGET}.tar.gz"
curl -fsSL -O "$BASE/shikigami-${VERSION}-${TARGET}.sha256"
shasum -a 256 -c "shikigami-${VERSION}-${TARGET}.sha256"
tar -xzf "shikigami-${VERSION}-${TARGET}.tar.gz"
# produces ./shikigami
```

## Example product manifest

[`examples/tenkai-product.toml`](../examples/tenkai-product.toml) is the
operator-facing product definition:

- `install` — place binary under `/usr/local/bin/shikigami` (adjust for fleet)
- `health` — `shikigami version` (must exit 0 after install)
- `inputs` — expects a local `shikigami` binary next to the manifest at publish time

### Publish / converge flow (operator checklist)

1. **Build or download** the binary for the target OS/arch (Release asset or
   `cargo build --release`).
2. **Stage** the binary as `./shikigami` beside the product manifest (or set
   `inputs` to your packaging layout).
3. **Publish** with tenkai (from a tenkai checkout / operator host):

   ```bash
   tenkaictl publish examples/tenkai-product.toml --allow-unsigned-development
   ```

4. **Subscribe / promote / apply** using your site’s tenkai product channels
   (exact subcommands depend on tenkai version — treat this repo’s example as
   the product definition, not a tenkai tutorial).
5. **Verify** on the node:

   ```bash
   shikigami version
   shikigami doctor
   ```

## Explicit non-goals

- Embedding tenkai inside the harness process
- Using tenkai to push governance/settings (use plane + env instead)
- Supporting every OS target in one manifest (publish per target)

## Related

- [README prebuilt binaries](../README.md#prebuilt-binaries)
- [serve.md](serve.md) for long-running hosts after install
