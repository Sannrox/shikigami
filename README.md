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

> **Status:** `v1.1.1`. Freeze-core library, settings, run, doctor JSON, and
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

- [Rust](https://rustup.rs/) toolchain pinned in [rust-toolchain.toml](rust-toolchain.toml) (Rust 2024)
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

## Next steps

Choose a process host for the common integration paths:

- CLI: one-shot operator and CI use (`doctor` / `run`) — [CLI reference](docs/cli.md)
- `serve`: long-running filesystem or plane-claim intake — [serve guide](docs/serve.md)
- MCP stdio: IDE and tool clients — [MCP guide](docs/mcp.md)
- ACP / TUI: session guest and interactive terminal host — [ACP](docs/acp.md), [TUI](docs/tui.md)
- Library: advanced in-process integrations that need direct results,
  cancellation, events, or metrics — [embedding guide](docs/embedding.md)

Settings are versioned TOML. Select adapters and explicit policy settings for
your use case; see the [configuration reference](docs/settings.md) for the
schema, environment variables, resolution order, and compatible version-1
profiles. New configurations should specify adapters and
`governance.fail_closed` explicitly.

Examples:

- [`examples/local-run.toml`](examples/local-run.toml) — offline
- [`examples/governed-sekai-chisei.toml`](examples/governed-sekai-chisei.toml) — plane-backed; follow the [governed guide](docs/governed-path.md)
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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and
[project verification](docs/project-verification.md) for the deterministic gates.
Agent/repo operating rules: [AGENTS.md](AGENTS.md).

## Documentation map

| Document | Audience |
| --- | --- |
| [VISION.md](VISION.md) | Why this product exists |
| [DESIGN.md](DESIGN.md) | Architecture and boundaries |
| [docs/README.md](docs/README.md) | Full documentation index |
| [docs/cli.md](docs/cli.md) | CLI command and option reference |
| [docs/governed-path.md](docs/governed-path.md) | Governed setup and doctor checks |
| [CONTEXT.md](CONTEXT.md) | Domain glossary and naming |
| [docs/settings.md](docs/settings.md) | Configuration reference |
| [docs/adapters.md](docs/adapters.md) | Ports and built-in adapters |
| [docs/embedding.md](docs/embedding.md) | Library integration |
| [docs/decisions/](docs/decisions/) | Accepted ADRs |
| [CHANGELOG.md](CHANGELOG.md) | Notable changes |
| [SECURITY.md](SECURITY.md) | Vulnerability reporting |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | Community norms |

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
