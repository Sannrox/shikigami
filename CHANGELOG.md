# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once 1.0.0 is released. Until then, `0.x` releases may include breaking changes.

## [Unreleased]

### Added

- Headless harness core with ports + settings (ADR 0001).
- CLI: `version`, `doctor`, `run` (no `init`).
- Governance adapters: `none`, `local`, `sekai-chisei` (PlanExecution path).
- Model adapters: `scripted`, `http` (feature), plane-owned turns when governed.
- Workspace adapters: `directory`, `git-worktree`.
- Event sinks: `stderr`, `jsonl`, `none`.
- Workspace-jailed tools: `read_file`, `write_file`, `edit`, `bash` (opt-in), `report`.
- Embeddable `Harness` library API.
- Examples for local, governed, and tenkai delivery packaging.
- Project documentation, security policy, and code of conduct.

## [0.1.0] — 2026-07-25

Initial public tree under active development.
