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

> **Status:** `v1.0.0`. Freeze-core library, settings, run, doctor JSON, and
> offline OSS paths follow semver under
> [ADR 0004](docs/decisions/0004-v1-contract.md). Additive evolution remains
> allowed on documented evolving/host-only surfaces (e.g. MCP). Offline
> `cargo test` is the supported baseline; live plane tests are ignored by default.

## Why

Most coding agents stop at a chat window or a one-off CLI:

- Governance is missing or bolted on after the fact.
- The same execution core cannot run unattended in CI or on a fleet host.
- Desktop shells reimplement the loop instead of sharing a testable library.

Shikigami is the **execution plane**: built on one shared library core,
headless by default, fail-closed when governance is required, and pluggable
when it is not.

## Requirements

- [Rust](https://rustup.rs/) toolchain with **Rust 2024** edition support
- macOS or Linux (primary targets today)
- Optional: a running [sekai-chisei](https://github.com/Sannrox/sekai-chisei) for the governed path
- Optional: OpenAI-compatible HTTP endpoint for ungoverned `http` model turns

The governed adapter uses the pinned upstream `sekai-client` Rust facade and
canonical `sekai-proto` dependency; no local `protoc` installation is needed.

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

### Library embed smoke (contract proof)

This offline check proves that an out-of-process host is not required to drive
`Harness`. PR and `main` CI run the same command after `cargo test`:

```bash
cargo run --locked --example embed_smoke
```

Expect `embed_smoke: PASS` (doctor, scripted run with live events, transcript export).

**External** host proof (out-of-tree consumer for ADR 0004):
[`Sannrox/shikigami-embed-smoke`](https://github.com/Sannrox/shikigami-embed-smoke)
depends on git tag `v1.0.0` and runs the same offline doctor + scripted run +
export pattern under its own CI.

Choose a process host for the common integration paths:

- CLI: one-shot operator and CI use (`doctor` / `run`)
- `serve`: long-running filesystem or plane-claim intake
- MCP stdio: IDE and tool clients — [docs/mcp.md](docs/mcp.md)
- Library: advanced in-process integrations that need direct results,
  cancellation, events, or metrics — [docs/embedding.md](docs/embedding.md)

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
| `doctor [--json] [--models]` | Effective profile, adapters, health, and optionally available models |
| `run <task> [--keep-workspace] [--resume ID] [--answer TEXT]` | Execute or resume a run (parked runs need `--answer`) |
| `serve [--intake filesystem\|plane] [--poll-ms N] [--max-jobs N]` | Filesystem-queue or plane-claim daemon host ([docs/serve.md](docs/serve.md)) |
| `mcp` | MCP stdio server: `doctor`, `run`, `run_start`/`run_status`/`run_wait` ([docs/mcp.md](docs/mcp.md)) |
| `export <run_id> [-o FILE]` | Offline JSONL transcript from checkpoint ([docs/embedding.md](docs/embedding.md)) |

| Flag / env | Purpose |
| --- | --- |
| `--state` / `SHIKIGAMI_STATE` | State root (default: `./.shikigami-state`) |
| `--config` / `SHIKIGAMI_CONFIG` | Settings file path |
| `--model` / `SHIKIGAMI_MODEL` | Final model override; `auto` delegates routing to sekai-chisei |
| `run --keep-workspace` | Keep the workspace after a successful run |

There is **no** `init` command. Config is optional; disk state is created when a
run needs it.

## Configuration

Settings are versioned TOML. Use cases change through adapters and explicit
policy settings, not by patching the turn loop. Version-1 profiles remain
compatible, but new configurations should specify adapters and
`governance.fail_closed` explicitly.

| Profile | Intent |
| --- | --- |
| `local` (default) | Offline-friendly. Governance `none` or `local`. Model `scripted` or `http`. |
| `governed` | Production path. Governance `sekai-chisei`, fail-closed, model turns via the plane. |

The examples below include those explicit fields; the profile names remain for
version-1 compatibility. See [docs/settings.md](docs/settings.md#profiles) for
the preset and environment-resolution details.

```bash
# Inspect effective wiring
./target/release/shikigami --config examples/local-run.toml doctor

# Governed example (requires a reachable plane)
export SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051
./target/release/shikigami --config examples/governed-sekai-chisei.toml doctor
# Include the plane-authorized model catalog (`auto` is the routing option)
./target/release/shikigami --config examples/governed-sekai-chisei.toml doctor --models
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
   ┌─────────────────┐     governance port      ┌───────────────────┐
   │  shikigami core  │────────────────────────▶│ none / local /    │
   │  run · tools ·   │                         │ http-callback /   │
   │  workspace       │                         │ sekai-chisei      │
   └─────────────────┘                         └───────────────────┘
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

External library embedding is an advanced integration surface. Use it when a
process boundary through the CLI, `serve`, or MCP would lose required
in-process behavior such as typed results, cancellation, events, or metrics:

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
make update
make validate
make test
make test-integration
```

The deterministic project gate is documented in
[docs/project-verification.md](docs/project-verification.md). CI on `main` and
pull requests calls the same Make targets (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)).
Those checks are required for merges to `main`.

Offline tests must pass with **no** control plane. Live plane probe:

```bash
SEKAI_LIVE=1 SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051 \
  cargo test --test plane_live -- --ignored --nocapture
```

Cargo features (defaults on):

| Feature | Purpose |
| --- | --- |
| `governance-sekai-chisei` | Versioned `sekai-client` Rust facade |
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
