# Changelog

## Unreleased

### Added

- Provide `Engine::new` as the preferred low-level construction interface while
  preserving 1.x-compatible public fields for existing embedders.

### Changed

- Centralize run-scoped tool bootstrap behind `ToolRegistry::from_config` so
  the registry owns coordinated authority, network, environment, ignore, and
  sandbox settings instead of the turn loop.

### Fixed

- Reject path-like workspace snapshot identifiers before mutation, and restore
  the workspace before artifact-baseline capture or project-context loading.
- Centralize parked-checkpoint validation and digest calculation so plane
  continuation intake binds its digest to the exact bytes it validates.
- Return the `auto` routing choice from `Harness::available_models` itself so
  every governed host receives the same effective model catalog.

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Durable local run registry and redacted event journals with runs, cancel,
  logs, and cleanup CLI controls; retained artifact manifests and bounded git
  patch export.
- Bounded filesystem worker queue semantics: priority, concurrency,
  backpressure, local retry limits, and authenticated HTTP run/control routes.
- Opt-in Unix rlimit child process groups for CPU, memory, PID, file-size, and
  descriptor limits.
- Durable aggregate metrics with CLI/HTTP Prometheus export and an offline
  scripted golden-fixture evaluator (shikigami eval).
- Deterministic project verification through thin Make targets and sorted
  validation/update scripts, plus a pinned Rust toolchain for local agents and
  CI.
- `shikigami doctor --models` to print the effective model catalog, including
  the `auto` sekai-chisei routing option; new settings default the preferred
  model to `auto` while preserving `gpt-4.1-mini` for direct HTTP fallback.
- Global `--model` / `SHIKIGAMI_MODEL` selection for direct CLI model overrides.
- Versioned plane-worker lifecycle contract (`shikigami.worker_lifecycle`
  schema_version 1): `$SHIKIGAMI_STATE/worker/lifecycle.json`, optional
  `--lifecycle-listen` probes (`/readyz`, `/livez`; full `/lifecycle` only on
  loopback), SIGTERM drain that stops new claims without force-acking, and a
  minimal Kubernetes host example.

### Changed

- Migrated the governed adapter and vendored Sekai/Chisei schemas to the 1.0
  control-plane contract. Model discovery now uses
  `GetEffectivePolicySummary`; governed harvest uses canonical receipt events
  with authenticated, causally linked reporting; and the removed LLM protocol
  and reporter-preflight RPC are no longer compiled. External-action tools
  build the versioned risk/project request, require the signed permit, and
  redeem it before host execution.
- Migrated the governed adapter to the pinned `sekai-client` Rust facade
  (`0.1.1` at the license-compatible upstream SDK revision). Core-loop connection,
  authentication metadata, deadlines, typed errors, streaming, operation
  events, and receipt lookup are SDK-owned; discovery, external-action, and
  plane-intake calls use its bounded raw escape hatch. The local protocol
  snapshot and build-time code generation were removed. The SDK transport
  guard now permits plaintext gRPC only for loopback development endpoints;
  non-loopback planes must use HTTPS.

### Fixed

- Workspace snapshots now skip symbolic links instead of following them outside
  the workspace.

## [1.0.5] — 2026-08-01

### Security

- Foreground and background Bash now clear ambient child-process inheritance,
  reconstruct the compatible parent environment, and always remove configured
  harness credentials and shell-startup control variables.

### Added

- Digest-pinned linux/amd64 OCI image publication for supported stable `1.0.5`
  and newer releases, with non-root startup smoke coverage, OCI metadata,
  provenance, and SBOM attestations.

## [1.0.4] — 2026-07-30

### Security

- Hardened opt-in `web_fetch` against DNS and redirect SSRF and bounded response
  bodies while streaming.
- Bound MCP stdio headers and frames before allocation.
- Bound checkpoint resume identifiers and stored workspaces to the configured
  run and workspace roots.

### Changed

- Architecture, settings, adapter, and serve documentation now consistently
  describes the shipped plane-claim host, HTTP callback governance, and
  in-place workspace surfaces.
- External Rust embedding remains supported under the 1.x compatibility
  contract but is positioned as an advanced integration surface; CLI, `serve`,
  and MCP are the common product entry points.
- Behavioral profiles remain compatible in settings version 1 but are
  deprecated for new configuration authoring in favor of explicit adapters and
  `governance.fail_closed`; a future-schema proposal is tracked in Discussion
  #146.

### Added

- Library mapping from already-claimed plane `runtime_dispatch` work to a
  correlation-safe, host-capped `RunRequest`.
- Explicit `serve --intake plane` mode for fenced claim → run/harvest →
  heartbeat/ack execution; filesystem intake remains the default.
- Plane intake now acknowledges intentional parks, consumes governed
  continuations, verifies optional local checkpoint handles and digests,
  reports fenced resume/replacement events, and cancels active work fail closed
  when claim authority is lost.

## [1.0.3] — 2026-07-26

### Added

- Governance adapter `http-callback` (alias `host-authz`): POST tool authorization
  to a host URL for interactive PermissionBroker-style gates.

## [1.0.2] — 2026-07-26

### Added

- `shikigami run --task-file PATH` so hosts can pass task text without putting
  prompts on the process argv.

## [1.0.1] — 2026-07-26

### Added

- Workspace adapter `inplace` / `directory-inplace`: use `workspace.root` as the
  run workspace without creating nested `shikigami-runs/<id>` directories
  (for host-selected worktrees such as Aldunis Code).

## [1.0.0] — 2026-07-26

First **stable** release under the [ADR 0004](docs/decisions/0004-v1-contract.md)
**medium 1.0** contract. Freeze-core surfaces follow semver; additive evolution
remains allowed where documented.

### Stability (freeze core)

| Area | Contract |
| --- | --- |
| Architecture | Ports + settings (ADR 0001); tenkai delivery-only |
| Library | `Harness::{from_config, resolve, doctor, doctor_async, run, run_with_events}` |
| Settings | `version = 1`, deny unknown keys |
| Run | `RunRequest` / `RunResult` / `RunTermination` including park + resume |
| Identity | ADR 0002 |
| Events | `HarnessEvent` additive; channel sink |
| CLI | `version`, `doctor`, `run`, `serve` (flags may grow) |
| Offline OSS | `cargo test` without plane |
| Governed path | PlanExecution + external-action tool authz + harvest |
| Doctor JSON | `schema_version = 1` |

See [docs/embedding.md](docs/embedding.md) for freeze vs evolving/host-only
surfaces (MCP, hooks, TUI remain non-core).

### Host proof

- In-repo: `cargo run --locked --example embed_smoke` **CI-gated** on PR/`main`
- External: [Sannrox/shikigami-embed-smoke](https://github.com/Sannrox/shikigami-embed-smoke)
  (out-of-tree consumer; offline doctor + scripted run + transcript export)

### Added (since 0.2.0)

- Host proof CI gate for `examples/embed_smoke` on PR/`main` Build & Test.
- Host-proof docs alignment (embed ranking, MCP poll tools, freeze candidates).
- [docs/1.0-freeze-audit.md](docs/1.0-freeze-audit.md) research closeout (#109)
  and external proof links (#113).

### Fixed (since 0.2.0)

- MCP HTTP client compiles with `--no-default-features` (`list_tools` always
  present; feature-gated body).

### Changed (since 0.2.0)

- `doctor` now explains configured, preset, excluded, implicit, effective, and
  model-visible builtin tool authority without changing permission semantics.
- Agent closeout: `AGENTS.md` / `deliver-ready-issue` require shared
  `autoreview` (not vendored) before ship; CI is not a substitute.
- Product status is **1.0 stable** for freeze-core surfaces (no longer “0.x may
  break freely”).

### Release artifacts

Tag `v1.0.0` builds archives via `.github/workflows/release.yml` for documented
multi-arch targets (same matrix as 0.2.0).

## [0.2.0] — 2026-07-26

Coding-agent parity and host surfaces on the headless harness core.

### Added

- **Tool registry & coding tools:** `ToolRegistry`, workspace-jailed `glob` /
  `grep`, `multi_edit`, `apply_patch`, `todo_write`, permission modes
  (`tools.mode`), concurrent parallel-safe tool batches (`run.tool_concurrency`).
- **Context:** project rules (`AGENTS.md`), skill packs, optional compaction,
  ignore support for search (`tools.respect_ignore`, default true).
- **MCP:** client (stdio + **HTTP** transport), server host
  (`shikigami mcp`: `doctor`, `run`, `run_start` / `run_status` / `run_wait`).
- **Network:** egress policy for HTTP model and MCP HTTP / `web_fetch` (opt-in;
  private/link-local blocked for fetch).
- **Ops:** transcript export (`shikigami export`), lifecycle hooks (`[[hooks]]`),
  background bash jobs, optional cost estimate on `RunResult.cost`, metrics,
  live embed event stream, serve daemon, workspace snapshots.
- **Governed path:** external-action authz, richer harvest, run identity ADR,
  park/escalate resume, versioned prompts, credential hygiene.
- **Quality:** property tests, cargo-deny CI, ADR 0004 v1 contract notes.
- **Host proof:** `examples/embed_smoke.rs` offline embed + export smoke.
- **Deps:** `sha2` 0.11, `toml` 1.x.

### Changed

- Default search tools honor ignore patterns (set `tools.respect_ignore = false`
  to restore 0.1-style unfiltered walks).
- CLI surface grows: `serve`, `mcp`, `export` (still headless-default).

### Compatibility

- Settings `version = 1` with `deny_unknown_fields` unchanged.
- `0.x` may still break; see ADRs for 1.0 freeze intent.

## [0.1.0] — 2026-07-25

First tagged public release.

### Added

- Headless harness core with ports + settings (ADR 0001).
- CLI: `version`, `doctor`, `run` (no `init`).
- Governance adapters: `none`, `local`, `sekai-chisei` (PlanExecution path).
- Model adapters: `scripted`, `http` (feature), plane-owned turns when governed.
- Workspace adapters: `directory`, `git-worktree`.
- Event sinks: `stderr`, `jsonl`, `none`.
- Workspace-jailed tools: `read_file`, `write_file`, `edit`, `bash` (opt-in), `report`.
- Embeddable `Harness` library API.
- Run cancellation and deadline timeouts.
- Local checkpoint and resume under `.shikigami-state/runs/<id>/`.
- Stable `doctor --json` contract (`schema_version = 1`) for automation.
- Examples for local, governed, and tenkai delivery packaging.
- CI (Build & Test, Rustfmt, Clippy), CodeQL, cargo-audit, and tag-driven multi-arch release workflow.
- Project documentation (settings, adapters, embedding, governed path), security policy with tool-jail threat model, code of conduct, and Dependabot triage notes.
- Agent skills retargeted for this repository.

### Changed

- Settings tables reject unknown keys (`deny_unknown_fields`); see [docs/settings.md](docs/settings.md) compatibility policy.

### Release artifacts

Tag `v0.1.0` builds archives via `.github/workflows/release.yml` for:

| Archive suffix | Target |
| --- | --- |
| `aarch64-apple-darwin` | Apple Silicon macOS |
| `x86_64-apple-darwin` | Intel macOS |
| `x86_64-unknown-linux-gnu` | Linux x86_64 |
| `aarch64-unknown-linux-gnu` | Linux aarch64 |
