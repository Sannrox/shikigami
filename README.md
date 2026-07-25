# shikigami

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**shikigami** (式神) is an open-source, local-first **headless agent harness**.

It runs autonomous agent work as countable **runs**: materialize a workspace,
call a model, execute jailed tools, emit progress, and finish with a structured
result — without a desktop UI.

Use it offline for demos and CI, or wire it to a governance control plane for
production. The loop is fixed; **settings select adapters** so different use
cases do not require forking the core.

| Path | What you get |
| --- | --- |
| **Local / OSS** | No external plane. Scripted or HTTP models. Deterministic tests. |
| **Governed** | First-party adapter for [sekai-chisei](https://github.com/Sannrox/sekai-chisei): policy, budget, PlanExecution, audit-oriented events. |
| **Delivery** | Optional packaging via [tenkai](https://github.com/Sannrox/tenkai). Delivery is not a runtime dependency. |

> **Status:** early (`v0.1.0`). Public APIs and settings may change before 1.0.
> Offline `cargo test` is the supported baseline; live plane tests are ignored
> by default.

## Why

Most coding agents stop at a chat window or a one-off CLI:

- Governance is missing or bolted on after the fact.
- The same execution core cannot run unattended in CI or on a fleet host.
- Desktop shells reimplement the loop instead of sharing a testable library.

Shikigami is the **execution plane**: library-first, headless by default,
fail-closed when governance is required, and pluggable when it is not.

## Requirements

- [Rust](https://rustup.rs/) toolchain with **Rust 2024** edition support
- macOS or Linux (primary targets today)
- Optional: a running [sekai-chisei](https://github.com/Sannrox/sekai-chisei) for the governed path
- Optional: OpenAI-compatible HTTP endpoint for ungoverned `http` model turns

`protoc` is supplied by a vendored build dependency when the
`governance-sekai-chisei` feature is enabled (default).

## Quickstart (offline)

No control plane, no API keys — uses the built-in **scripted** model:

```bash
git clone https://github.com/Sannrox/shikigami.git
cd shikigami
cargo build --release

./target/release/shikigami doctor
./target/release/shikigami --config examples/local-run.toml run "demo" --keep-workspace
```

Expect a successful run that writes `SHIKIGAMI_OK.txt` under the run workspace
and prints `success=true`.

### Prebuilt binaries

Tagged releases publish multi-arch archives from GitHub Actions
([Releases](https://github.com/Sannrox/shikigami/releases)):

| Archive suffix | Target |
| --- | --- |
| `aarch64-apple-darwin` | Apple Silicon macOS |
| `x86_64-apple-darwin` | Intel macOS |
| `x86_64-unknown-linux-gnu` | Linux x86_64 |
| `aarch64-unknown-linux-gnu` | Linux aarch64 |

Each archive includes a `sha256` checksum. Prefer building from source when
you need a custom feature set.

## CLI

```text
shikigami [--state DIR] [--config FILE] <COMMAND>
```

| Command | Purpose |
| --- | --- |
| `version [--json]` | Product identity |
| `doctor [--json]` | Effective profile, adapters, and health (probes the plane when configured) |
| `run <task> [--keep-workspace]` | Execute one run |

| Flag / env | Purpose |
| --- | --- |
| `--state` / `SHIKIGAMI_STATE` | State root (default: `./.shikigami-state`) |
| `--config` / `SHIKIGAMI_CONFIG` | Settings file path |
| `run --keep-workspace` | Keep the workspace after a successful run |

There is **no** `init` command. Config is optional; disk state is created when a
run needs it.

## Configuration

Settings are versioned TOML. Use cases change by profile and adapter ids, not
by patching the turn loop.

| Profile | Intent |
| --- | --- |
| `local` (default) | Offline-friendly. Governance `none` or `local`. Model `scripted` or `http`. |
| `governed` | Production path. Governance `sekai-chisei`, fail-closed, model turns via the plane. |

```bash
# Inspect effective wiring
./target/release/shikigami --config examples/local-run.toml doctor

# Governed example (requires a reachable plane)
export SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051
./target/release/shikigami --config examples/governed-sekai-chisei.toml doctor
```

Full schema, environment variables, and resolution order:
**[docs/settings.md](docs/settings.md)**.

Examples:

- [`examples/local-run.toml`](examples/local-run.toml) — offline
- [`examples/governed-sekai-chisei.toml`](examples/governed-sekai-chisei.toml) — plane-backed
- [`examples/tenkai-product.toml`](examples/tenkai-product.toml) — binary delivery only

## Architecture (short)

```text
  operator / CI / embedder
            │
            ▼
   ┌─────────────────┐     governance port      ┌────────────────┐
   │  shikigami core  │────────────────────────▶│ none / local / │
   │  run · tools ·   │                         │ sekai-chisei   │
   │  workspace       │                         └────────────────┘
   └─────────────────┘
            │
            │  (optional) install/upgrade binary
            ▼
         tenkai
```

- **Core owns** run lifecycle, workspace materialization, tool jail, event
  emission.
- **Adapters own** governance, model source (when not plane-owned), workspace
  kind, and event sinks.
- **sekai-chisei** (when selected) owns policy, budget, governed model
  execution, and durable operational truth.
- **tenkai** (when used) owns shipping the binary — never process config.

Details: [DESIGN.md](DESIGN.md), [ADR 0001](docs/decisions/0001-ports-and-settings.md),
[docs/adapters.md](docs/adapters.md).

## Library embedding

The CLI is a thin host. Prefer the library when you need structured results:

```rust
use shikigami::{Config, Harness, RunRequest, StateRoot};

async fn example() -> Result<(), shikigami::HarnessError> {
    let state = StateRoot::default_in(".");
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();

    let harness = Harness::from_config(config, state)?;
    let mut request = RunRequest::new("do work");
    request.keep_workspace = true;
    let result = harness.run(request).await?;
    assert!(result.success);
    Ok(())
}
```

See [docs/embedding.md](docs/embedding.md).

## Development

```bash
cargo fmt
cargo test
cargo build --all-targets
```

CI on `main` and pull requests runs **Build & Test**, **Rustfmt**, and **Clippy**
(see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)). Those checks are
required for merges to `main`.

Offline tests must pass with **no** control plane. Live plane probe:

```bash
SEKAI_LIVE=1 SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051 \
  cargo test --test plane_live -- --ignored --nocapture
```

Cargo features (defaults on):

| Feature | Purpose |
| --- | --- |
| `governance-sekai-chisei` | gRPC client + vendored protos |
| `model-http` | OpenAI-compatible HTTP model adapter |

Contributor guide: [CONTRIBUTING.md](CONTRIBUTING.md). Agent/repo operating rules:
[AGENTS.md](AGENTS.md).

## Documentation map

| Document | Audience |
| --- | --- |
| [VISION.md](VISION.md) | Why this product exists |
| [DESIGN.md](DESIGN.md) | Architecture and boundaries |
| [docs/README.md](docs/README.md) | Full documentation index |
| [docs/settings.md](docs/settings.md) | Configuration reference |
| [docs/adapters.md](docs/adapters.md) | Ports and built-in adapters |
| [docs/embedding.md](docs/embedding.md) | Library integration |
| [docs/decisions/](docs/decisions/) | Accepted ADRs |
| [CHANGELOG.md](CHANGELOG.md) | Notable changes |
| [SECURITY.md](SECURITY.md) | Vulnerability reporting |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | Community norms |

## Naming

- **Shikigami** — this product (the harness)
- **Run** — one unit of agent work
- Do not call an individual agent attempt “a shikigami”

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
