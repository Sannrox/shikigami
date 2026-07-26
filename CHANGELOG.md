# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once 1.0.0 is released. Until then, `0.x` releases may include breaking changes.

## [Unreleased]

### Changed

- Agent closeout: `AGENTS.md` / `deliver-ready-issue` require shared
  `autoreview` (not vendored) before ship; CI is not a substitute.

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

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
