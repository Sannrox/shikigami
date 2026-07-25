# Repository Guidelines

## Project Structure & Module Organization

`shikigami` is a Rust 2024 crate for a local-first headless agent harness.
Source code lives in `src/`: `src/lib.rs` exports the public API,
`src/bin/shikigami.rs` is a thin CLI host, `src/harness.rs` wires settings to
ports, `src/run.rs` owns the turn loop, `src/governance/` holds governance
adapters (`none`, `local`, `sekai-chisei`), `src/tools.rs` implements
workspace-jailed tools, `src/workspace.rs` materializes sandboxes,
`src/model.rs` supplies ungoverned model turns, and `src/events.rs` sinks
harness-local progress. Protocol definitions for the sekai-chisei adapter live
in `proto/`. Integration tests live in `tests/`. Optional host state defaults
under `.shikigami-state/`; do not commit local state, run workspaces, or
generated runtime artifacts.

Architecture is **ports + settings**
([ADR 0001](docs/decisions/0001-ports-and-settings.md)): the turn loop depends
on ports; adapters are selected by configuration. Production governance is
sekai-chisei; offline paths use `none`/`local` plus scripted or HTTP models.
Delivery tools (e.g. tenkai) may install the binary; they are not runtime ports.

Human documentation index: [docs/README.md](docs/README.md).

## Build, Test, and Development Commands

- `cargo fmt` formats Rust code before review.
- `cargo test` runs the normal unit and integration test suite (no plane required).
- `cargo run --bin shikigami -- doctor` prints effective settings and adapter health.
- `cargo run --bin shikigami -- --config examples/local-run.toml run "demo" --keep-workspace`
  exercises an offline scripted run.
- `cargo build --release` builds an optimized binary.
- `SEKAI_LIVE=1 SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051 cargo test --test plane_live -- --ignored`
  runs the ignored live plane probe when a local sekai-chisei is available.

Settings resolve from defaults → optional `shikigami.toml` → environment → CLI.
Important variables include `SHIKIGAMI_STATE`, `SHIKIGAMI_CONFIG`,
`SHIKIGAMI_PROFILE`, `SHIKIGAMI_GOVERNANCE_ADAPTER`, `SHIKIGAMI_CONTROL_PLANE`,
and `SHIKIGAMI_MODEL_ADAPTER`. See [docs/settings.md](docs/settings.md) and
[examples/](examples/).

GitHub Issues are the planning source of truth. Project-specific Skills live
under `.agents/skills/`. Read `DESIGN.md`, `VISION.md`, and accepted ADRs under
`docs/decisions/` before changing system boundaries.

## Ontology Policy

For work involving portable ontology definitions, classes, relations,
provenance, validation, import, export, or structural queries, always use the
project-local `sekai-ontology` Skill in `.agents/skills/sekai-ontology/`.

Select the ontology database explicitly with `--db <path>` or `SEKAI_DB`, then
run `sekai --db <path> --json validate` before relying on its contents. Treat
successful ontology output as structured repository evidence, preserve its
provenance in answers, and state when validation fails or the requested fact is
absent rather than inferring it. Do not use a harness state directory or a
control-plane database as a portable ontology database.

## Coding Style & Naming Conventions

Follow standard Rust formatting with `cargo fmt` and keep modules aligned with
the existing domain boundaries. Use `snake_case` for files, modules, functions,
and variables; use `PascalCase` for types and traits; use
`SCREAMING_SNAKE_CASE` for constants. Keep governance, workspace, model, and
event behavior behind port traits and in-tree adapters. Prefer explicit
fail-closed governance when required over silent degradation. Individual units
of work are **runs**, not “a shikigami.” Keep the CLI thin; put logic in the
library so hosts can embed `Harness` without shelling out.

## Testing Guidelines

Add focused tests for changes touching the turn loop, tools, workspace
materialization, settings resolution, or governance adapters. Prefer
deterministic tests that do not require external services. Mark plane- or
provider-dependent tests ignored, following `tests/plane_live.rs`, and document
required local services in the test or related docs. Offline `cargo test` must
stay green without sekai-chisei.

## Commit & Pull Request Guidelines

Use short imperative subjects, often Conventional Commit style:
`feat(run): jail bash to workspace`, `docs: document settings resolution`,
`fix(governance): fail closed without plane endpoint`. Keep commits narrow and
describe the affected subsystem when useful. Pull requests should include a
concise behavior summary, tests run, linked issue or context, and any
configuration or security implications. Update [CHANGELOG.md](CHANGELOG.md) for
user-visible changes. When merging PRs, prefer GitHub rebase merges so reviewed
commits remain individually visible while `main` stays linear. Use
`gh pr merge --rebase --delete-branch` unless the user explicitly asks for a
merge commit or squash merge. Do not rewrite protected `main` after merging
unless the user explicitly approves; if protection is temporarily relaxed,
restore force-push and status-check settings immediately after the correction.

## Security & Configuration Tips

Never commit secrets, tokens, provider credentials, logs, or local harness
state (`.shikigami-state/`). Do not treat delivery systems as runtime control
dependencies. When profile `governed` or `fail_closed` is set, missing or
unhealthy governance must fail doctor and run. Report vulnerabilities through
[SECURITY.md](SECURITY.md). Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) in
all project spaces.
