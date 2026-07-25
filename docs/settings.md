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
| `adapter` | `"directory"` | `directory` \| `git-worktree` |
| `root` | `"."` | Parent/repo root for materialization |
| `branch_prefix` | `"shikigami/"` | Branch prefix for git-worktree |

### `[tools]`

| Field | Default | Description |
| --- | --- | --- |
| `mode` | `custom` | `custom` \| `read` \| `workspace` \| `workspace_exec` |
| `enabled` | `[]` | Allow-list; with non-`custom` mode, **intersects** the mode set |
| `bash_timeout_secs` | `60` | Default bash timeout (capped at 120s) |

| Mode | Effective tools (before optional `enabled` intersect) |
| --- | --- |
| `custom` | `enabled` if set, else coding default (writes/search, **no** bash) |
| `read` | `read_file`, `glob`, `grep`, `report`, `escalate` |
| `workspace` | coding default (no bash) |
| `workspace_exec` | coding default + `bash` |

Modes are host policy, not an OS sandbox.

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

## Compatibility policy (v0.2)

1. **`version` is required** and must equal the crate's supported schema
   (`1` today). Unsupported versions fail at load time.
2. **Unknown keys are rejected** at every table (`deny_unknown_fields`). Fix
   typos rather than silent ignore.
3. **Breaking changes** (rename/remove fields, change defaults that alter
   security posture, change version) require a `version` bump and a
   CHANGELOG entry under a new version heading.
4. **Additive fields** with `#[serde(default)]` may ship in the same
   `version` when they do not change existing behavior.
5. Pre-1.0 (`0.x`) may still break between minor crates versions; the
   settings `version` field is the config compatibility signal.
