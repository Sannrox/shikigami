# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once 1.0.0 is released. Until then, `0.x` releases may include breaking changes.

## [Unreleased]

### Added

- Governed mid-run tool authorization via sekai-chisei `AuthorizeExternalAction`
  (bash / write_file / edit / read_file; `report` remains harness-internal).
- Richer governed harvest: `shikigami.run.begin` / tool / complete events with
  turns, termination, and evidence references ([docs/harvest.md](docs/harvest.md)).
- Documented run identity model (ADR 0002); `RunRequest.logical_operation_id`
  for host/plane correlation ([docs/identity.md](docs/identity.md)).
- Headless `escalate` tool parks runs with structured payload; resume with
  `--answer` / `resume_answer` continues from checkpoint.
- Versioned prompt assets (`src/prompts/`) with digest ids on events, harvest,
  and `RunResult` ([docs/prompts.md](docs/prompts.md)).
- Credential helper docs; doctor reports env presence only and redacts secret
  values ([docs/credentials.md](docs/credentials.md)).
- Optional nightly live plane workflow (no-op without secrets).
- Property tests for path jail and settings parse/validate invariants.
- Supply-chain CI: `cargo deny` with committed `deny.toml` (licenses + advisories).
- `shikigami serve` local-queue daemon with health file and graceful shutdown
  (ADR 0003).
- Tenkai delivery docs and release-aligned product manifest example.
- `Harness::run_with_events` + `ChannelSink` / `FanoutSink` for embedder live streams.
- Process metrics counters with JSON + Prometheus text export (`docs/metrics.md`).

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

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
