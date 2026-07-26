# Settings reference

Shikigami has **no install / `init` step**. Behavior is selected by versioned
settings so open-source and production use cases share one binary.

## Resolution order

Later sources override earlier ones where documented:

1. Built-in defaults (`profile = local`, governance `none`, model `scripted`)
2. Config file (first match):
   - `--config` / `SHIKIGAMI_CONFIG`
   - `$SHIKIGAMI_STATE/shikigami.toml` (default state root: `./.shikigami-state`)
   - `./shikigami.toml` in the current working directory
3. Environment variables
4. CLI flags (where provided)

Invalid adapter ids fail at resolve/validate time. Missing optional files are
not errors.

## Profiles

| Name | Effect |
| --- | --- |
| `local` | Default. Offline-friendly. Does not force a plane. |
| `governed` | Sets governance to `sekai-chisei` if still `none`, forces `fail_closed = true`, prefers model adapter `plane`. |

Custom profile names are allowed as labels; only `governed` applies the preset
above today.

## Schema (`version = 1`)

Top-level `version` is required and must be `1`.

### `[profile]`

| Field | Default | Description |
| --- | --- | --- |
| `name` | `"local"` | Profile label / preset |

### `[governance]`

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"none"` | `none` \| `local` \| `sekai-chisei` |
| `endpoint` | unset | Control plane base URL (required for fail-closed `sekai-chisei`) |
| `principal` | `"shikigami"` | Identity presented to the plane (not a secret) |
| `namespace` | `"default"` | Plane namespace |
| `fail_closed` | `false` | Fail doctor/run when governance is unhealthy |
| `token_env` | unset | Env var name holding a Bearer token for the plane |

### `[model]`

Used for ungoverned planning (`none` / `local` governance). When governance is
`sekai-chisei`, turns use the plane regardless of local HTTP settings.

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"scripted"` | `scripted` \| `http` \| `plane` |
| `script_json` | built-in demo script | JSON array of turns for `scripted` |
| `base_url` | OpenAI-compatible default | Base URL for `http` |
| `model` | `"gpt-4.1-mini"` | Model id for `http` / plane preferred model |
| `api_key_env` | `"OPENAI_API_KEY"` | Env var for HTTP API key |
| `input_usd_micros_per_mtok` | unset | Optional cost rate: USD microdollars per million **input** tokens (1_000_000 = $1/MTok). Both rates required for `RunResult.cost`. |
| `output_usd_micros_per_mtok` | unset | Optional cost rate: USD microdollars per million **output** tokens |

When either cost rate is unset, `RunResult.cost` is **absent** (not zero). Never invents provider prices.

#### Scripted turn JSON

```json
[
  {
    "content": "optional assistant text",
    "tool_calls": [
      {
        "id": "optional",
        "name": "write_file",
        "args_json": "{\"path\":\"a.txt\",\"content\":\"hi\"}"
      }
    ]
  }
]
```

### `[workspace]`

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"directory"` | `directory` \| `inplace` (`directory-inplace`) \| `git-worktree` |
| `root` | `"."` | Parent/repo root for materialization; for `inplace`, the workspace path itself |
| `snapshot` | `false` | After materialize, copy workspace to `state/runs/<id>/snapshots/initial` (not supported with `inplace`) |

For `inplace`, place the harness **state** root (`--state` / `SHIKIGAMI_STATE`)
**outside** `workspace.root`. Hosts must serialize concurrent runs against the
same inplace root.
| `branch_prefix` | `"shikigami/"` | Branch prefix for git-worktree |

### `[run]` (tool concurrency)

| Field | Default | Description |
| --- | --- | --- |
| `tool_concurrency` | `4` | Max concurrent tools when a turn’s batch is **all parallel-safe** (`read_file`, `glob`, `grep`, `web_fetch`). `1` forces sequential. Any write/bash/`todo_write`/`report`/`escalate` batch runs **serially**. Tool messages are applied in original call order. |

### `[tools]`

| Field | Default | Description |
| --- | --- | --- |
| `mode` | `custom` | `custom` \| `read` \| `workspace` \| `workspace_exec` |
| `enabled` | `[]` | Allow-list; with non-`custom` mode, **intersects** the mode set |
| `bash_timeout_secs` | `60` | Default bash timeout (capped at 120s) |
| `respect_ignore` | `true` | `glob`/`grep` honor built-in defaults + `.gitignore` / `.shikigamiignore` |

| Mode | Effective tools (before optional `enabled` intersect) |
| --- | --- |
| `custom` | `enabled` if set, else coding default (writes/search, **no** bash) |
| `read` | `read_file`, `glob`, `grep`, `todo_write`, `report`, `escalate` |
| `workspace` | coding default (no bash) |
| `workspace_exec` | coding default + `bash` (also exposes `bash_background` / `bash_job_status` / `bash_job_logs`) |

Background jobs (when bash is enabled): start with `bash_background`, poll with
`bash_job_status` / `bash_job_logs`. Jobs are killed when the run finishes
(success, failure, or park). Max 4 concurrent jobs; logs capped at 256KiB.

Coding default includes `todo_write` (run-scoped checklist; max 32 items).
It is **not** a plane work-unit API and does not replace `escalate`/park.

Coding default also includes `apply_patch` (structured multi-hunk patches with
optional context; atomic across files). Prefer `edit` / `multi_edit` for exact
single-site or multi-site replacements; use `apply_patch` when surrounding
context is needed to disambiguate. Caps: 16 files, 32 hunks, 64KiB JSON payload.

`web_fetch` is **opt-in only** (not in coding default or mode presets). Add it via
`tools.enabled` (custom mode or intersect). See [network.md](network.md).

Modes are host policy, not an OS sandbox.

When `respect_ignore = true` (default), search tools skip heavy dirs (`node_modules`,
`target`, `.git`, …) and patterns from workspace `.shikigamiignore` / `.gitignore`
(no negation/`!` in v1; pure matcher, no git binary). **`read_file` of an explicit
path is never blocked by ignore** — ignore is convenience filtering, not a secret vault.

### `[context]`

| Field | Default | Description |
| --- | --- | --- |
| `load_project_rules` | `true` | Load first matching rules file from the **workspace** root |
| `rules_filenames` | `["AGENTS.md","shikigami.rules.md"]` | Tried in order; flat names only |
| `max_rules_bytes` | `32768` | Truncate with a marker when larger |
| `skills_root` | unset → `.shikigami/skills` under workspace | Root for skill packs |
| `skills` | `[]` | Skill directory names (`<root>/<id>/SKILL.md`) |
| `max_skill_bytes` | `32768` | Per-skill size cap |

Rules and skills are **untrusted text** injected into the system prompt (not executed). Disable rules with `load_project_rules = false`. Leave `skills` empty for no packs.

### `[run]`

| Field | Default | Description |
| --- | --- | --- |
| `max_turns` | `50` | Hard stop for the turn loop |
| `compact_after_messages` | unset | Compact middle history when message count exceeds N (off by default) |
| `compact_keep_tail` | `8` | Messages kept after the first task message when compacting |
| `timeout_secs` | unset | Optional overall wall-clock limit (checked at turn boundaries) |

CLI / env override: `shikigami run --timeout-secs N` or `SHIKIGAMI_RUN_TIMEOUT_SECS`.
Embedders may also pass `RunRequest.timeout` and a cooperative
`RunRequest.cancel` (`tokio::sync::watch::Receiver<bool>`). Cancel and timeout
surface as errors (`RunError::Cancelled` / `TimedOut`), never as silent success.

### Resume

Local checkpoints are written under `$SHIKIGAMI_STATE/runs/<run_id>/checkpoint.json`
after each turn (version `1`). Resume:

```bash
shikigami run --resume <run-id>
# or library: RunRequest { resume_run_id: Some(id), resume_answer: Some(...), .. }

### Park / escalate (headless)

When the model calls the `escalate` tool, the run terminates with
`termination=parked` (library `RunTermination::Parked`, CLI non-zero exit).
Workspace and checkpoint are retained. Resume:

```bash
shikigami run --resume <run_id> --answer "operator decision"
# or --answer-file path
```

Resume without an answer errors (no silent success/deny).
```

Checkpoints are harness scratch only — not plane truth. Prompt id must match
the current system prompt or resume fails.

### `[events]`

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"stderr"` | `stderr` \| `jsonl` \| `none` |

`jsonl` appends under the state runs directory (`events.jsonl`).

## Environment variables

| Variable | Purpose |
| --- | --- |
| `SHIKIGAMI_STATE` | State root directory |
| `SHIKIGAMI_CONFIG` | Path to settings file |
| `SHIKIGAMI_PROFILE` | Profile name |
| `SHIKIGAMI_GOVERNANCE_ADAPTER` | Governance adapter id |
| `SHIKIGAMI_CONTROL_PLANE` | sekai-chisei endpoint |
| `SHIKIGAMI_MODEL_ADAPTER` | Model adapter id |
| `SHIKIGAMI_MODEL_SCRIPT` | Scripted JSON (inline) |
| `OPENAI_API_KEY` | Default HTTP model key |
| *(value of `token_env`)* | Plane bearer token when configured |

Credential ergonomics, anti-patterns, and doctor redaction:
**[credentials.md](credentials.md)**.

## Property tests

Default `cargo test` includes proptest coverage for:

- path jail rejection (`is_unsafe_relative_path` — absolute / `..` components)
- settings validate/parse invariants (unknown adapters, unknown TOML keys)

To run only those cases:

```bash
cargo test property_
```

No separate fuzz job is required for v0.x.

There are **no** tenkai environment variables for the harness process. Tenkai
only installs or upgrades the binary; see
[../examples/tenkai-product.toml](../examples/tenkai-product.toml).

## Fail-closed rules

Doctor and `run` fail when:

- `fail_closed` is true or profile is `governed`, **and**
- governance is unhealthy (e.g. `sekai-chisei` without endpoint, or live probe
  failure for required plane connectivity).

Offline defaults never require a plane.

## Examples

- [../examples/local-run.toml](../examples/local-run.toml)
- [../examples/governed-sekai-chisei.toml](../examples/governed-sekai-chisei.toml)

Adapter semantics: [adapters.md](adapters.md).

## Doctor JSON (`schema_version` = 1)

`shikigami doctor --json` emits a `DoctorReport` object:

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | number | Doctor JSON schema (currently `1`) |
| `ok` | bool | All required checks passed |
| `profile` | string | Effective profile name |
| `governance` | string | Governance adapter id |
| `governance_detail` | string | Adapter health detail |
| `workspace` | string | Workspace adapter id |
| `workspace_detail` | string | Workspace health detail |
| `events` | string | Events adapter id |
| `events_detail` | string | Events health detail |
| `model` | string | Model adapter id |
| `lines` | string[] | Human diagnostic lines |

Breaking renames/removals of these fields require a `schema_version` bump and
CHANGELOG entry. Additional fields may appear without a bump.

## Compatibility policy (1.0)

1. **`version` is required** and must equal the crate's supported schema
   (`1` today). Unsupported versions fail at load time.
2. **Unknown keys are rejected** at every table (`deny_unknown_fields`). Fix
   typos rather than silent ignore.
3. **Breaking changes** (rename/remove freeze-core fields, change defaults that
   alter security posture, change settings schema `version`) require a settings
   `version` bump and/or crate major version, plus a CHANGELOG entry.
4. **Additive fields** with `#[serde(default)]` may ship in the same
   settings `version` when they do not change existing behavior.
5. From **crate 1.0**, freeze-core public API and doctor JSON follow semver;
   the settings `version` field remains the config compatibility signal.
