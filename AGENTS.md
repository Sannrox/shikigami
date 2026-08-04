# Repository Guidelines

## Project Structure & Module Organization

`shikigami` is a Rust 2024 crate for a local-first headless agent harness.
Source code lives in `src/`: `src/lib.rs` exports the public API,
`src/bin/shikigami.rs` is a thin CLI host, `src/harness.rs` wires settings to
ports, `src/run.rs` owns the turn loop, `src/governance/` holds governance
adapters (`none`, `local`, `sekai-chisei`), `src/tools.rs` implements
workspace-jailed tools, `src/workspace.rs` materializes sandboxes,
`src/model.rs` supplies ungoverned model turns, and `src/events.rs` sinks
harness-local progress. The sekai-chisei adapter consumes the versioned
upstream `sekai-client` Rust facade and its canonical `sekai-proto` dependency;
Shikigami does not carry a second protocol snapshot. Integration tests live in
`tests/`. Optional host state defaults under `.shikigami-state/`; do not commit
local state, run workspaces, or generated runtime artifacts.

Architecture is **ports + settings**
([ADR 0001](docs/decisions/0001-ports-and-settings.md)): the turn loop depends
on ports; adapters are selected by configuration. Production governance is
sekai-chisei; offline paths use `none`/`local` plus scripted or HTTP models.
Delivery tools (e.g. tenkai) may install the binary; they are not runtime ports.

Human documentation index: [docs/README.md](docs/README.md).

## Build, Test, and Development Commands

- `cargo fmt` formats Rust code before review.
- `cargo test` runs the normal unit and integration test suite (no plane required).
- `cargo clippy --all-targets -- -D warnings` is required for ship-level local
  gates (matches CI).
- `cargo run --bin shikigami -- doctor` prints effective settings and adapter health.
- `cargo run --bin shikigami -- --config examples/local-run.toml run "demo" --keep-workspace`
  exercises an offline scripted run.
- `cargo run --locked --example embed_smoke` is the offline library host proof
  (also gated on PR/`main` CI Build & Test).
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

## Agent delivery closeout (required)

`cargo test` / Clippy / green CI are **not** a substitute for structured code
review. For non-trivial implementation work (any behavior, security, settings,
or public-API change) that will be **committed, pushed, or opened as a PR**:

1. Run focused checks via the `verify-change` Skill (or equivalent `cargo fmt`,
   `cargo test`, `cargo clippy --all-targets -- -D warnings`).
2. Run **`autoreview`** before the ship commit (or before push if the commit
   already exists). **Do not vendor** the autoreview skill into this repo;
   resolve the helper from the shared skill install (first match wins):

   ```bash
   # Preferred: shared agent-skills checkout or global skills home
   if [ -x "${AUTOREVIEW:-}" ]; then :; \
   elif [ -x "$HOME/Projects/agent-skills/skills/autoreview/scripts/autoreview" ]; then
     export AUTOREVIEW="$HOME/Projects/agent-skills/skills/autoreview/scripts/autoreview"
   elif [ -x "${AGENTS_HOME:-$HOME/.agents}/skills/autoreview/scripts/autoreview" ]; then
     export AUTOREVIEW="${AGENTS_HOME:-$HOME/.agents}/skills/autoreview/scripts/autoreview"
   elif [ -x "$HOME/Projects/sekai-chisei/.agents/skills/autoreview/scripts/autoreview" ]; then
     export AUTOREVIEW="$HOME/Projects/sekai-chisei/.agents/skills/autoreview/scripts/autoreview"
   else
     echo "autoreview helper not found; install shared agent-skills or set AUTOREVIEW" >&2
     exit 1
   fi
   # Dirty uncommitted work:
   "$AUTOREVIEW" --mode local
   # Topic branch / open PR (preferred after commit):
   "$AUTOREVIEW" --mode branch --base origin/main
   # Already committed on a clean tree:
   "$AUTOREVIEW" --mode commit --commit HEAD
   ```

3. Treat helper output as advisory: verify each accepted finding in the real
   code, fix actionable ones, rerun tests and autoreview until the helper exits
   0 with no accepted/actionable findings (or document a maintainer judgment
   blocker in the PR).
4. Report in the PR or handoff: commands run, tests, autoreview result
   (clean / findings fixed / consciously rejected).

**Do not skip autoreview** because CI is green, the change is “small,” or the
session is optimizing for throughput unless the user explicitly waived it.
Docs-only typo fixes and pure formatting may skip structured review; state the
waiver. The `deliver-ready-issue` Skill requires this same closeout before
publish/land.

Default review engine is Codex (`gpt-5.5` via the helper). Read the shared
`autoreview` skill’s `SKILL.md` next to the resolved helper for engines and
findings policy.

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
concise behavior summary, **tests run**, **autoreview result** (for non-trivial
code), linked issue or context, and any configuration or security implications.
Update [CHANGELOG.md](CHANGELOG.md) for user-visible changes.

### Verified commits on GitHub

Prefer publishing PR branch tips with GitHub-signed commits so GitHub shows
**Verified**:

1. Implement and commit locally as usual (`commit.gpgsign` may still apply).
2. Publish the branch tip with `scripts/gh-verified-push.sh` instead of a plain
   `git push` when you want the hosted commit Verified (OpenClaw-style GraphQL
   `createCommitOnBranch`). That path creates one server-side commit with the
   local `HEAD` tree; committer is typically **GitHub**.
3. New branch:
   `scripts/gh-verified-push.sh --create-branch-from origin/main --branch <topic> --sync-local`
4. Existing PR branch:
   `scripts/gh-verified-push.sh --branch <topic> --sync-local`
   (uses the current remote tip as `expectedHeadOid`).
5. Never pass `--no-gpg-sign` for local commits; if GPG fails, stop and fix it.
6. After publish, confirm `verification.verified=true` (the script prints this).

When merging PRs, prefer **squash** (`gh pr merge --squash --delete-branch`) so
the land commit on `main` is also GitHub-signed/Verified and history stays
linear. Use `gh pr merge --merge` only when multi-commit history must be kept
(original SHAs preserved). Avoid GitHub **rebase** merges when Verified history
matters: rebase-merge rewrites commits and drops signatures. Do not rewrite
protected `main` after merging unless the user explicitly approves; if
protection is temporarily relaxed, restore force-push and status-check settings
immediately after the correction.

## Security & Configuration Tips

Never commit secrets, tokens, provider credentials, logs, or local harness
state (`.shikigami-state/`). Do not treat delivery systems as runtime control
dependencies. When profile `governed` or `fail_closed` is set, missing or
unhealthy governance must fail doctor and run. Report vulnerabilities through
[SECURITY.md](SECURITY.md). Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) in
all project spaces.
