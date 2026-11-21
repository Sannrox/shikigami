# Settings reference

Shikigami has **no install / `init` step**. Behavior is selected by versioned
settings so open-source and production use cases share one binary.

## Resolution order

Later sources override earlier ones where documented:

1. Built-in defaults (`profile = local`, governance `none`, model `scripted`)
2. Config file (first match):
   - `--config` / `SHIKIGAMI_CONFIG`
   - `$SHIKIGAMI_STATE/shikigami.toml` (default state root: `~/.shikigami/projects/<encoded-cwd>`; see [cli.md](cli.md))
3. Environment variables
4. CLI flags (where provided)
5. A preset (`--preset` / `SHIKIGAMI_PRESET`), which only narrows tools and
   adds a prompt section; see [presets.md](presets.md)

A `shikigami.toml` in the current working directory is never loaded
implicitly. It is repository content that can add hooks and MCP servers or
relax governance, sandbox, and egress, so a cloned repository must not apply it
without consent. The CLI prints a note when one is present; opt in with
`--config shikigami.toml`.

Invalid adapter ids fail at resolve/validate time. Missing optional files are
not errors.

## Host state root

`--state` / `SHIKIGAMI_STATE` select the harness scratch root (runs, journals,
serve queue, metrics). When unset, the process host uses a root per current
working directory under the shikigami home:

```text
~/.shikigami/                  SHIKIGAMI_HOME replaces it (absolute path)
  projects/<encoded-cwd>/      state root for one working directory
```

On Windows the home is `%USERPROFILE%\.shikigami`. A relative `SHIKIGAMI_HOME`,
or no absolute `HOME` / `USERPROFILE` when it is unset, fails closed.

`<encoded-cwd>` is `--<canonical-cwd>--<fingerprint>`: `/`, `\`, and `:` become
`-`, then a 16-character SHA-256 prefix of the canonical OS path bytes so
`/tmp/a-b` and `/tmp/a/b` do not share a root. The readable prefix is truncated
to keep the directory name within 255 bytes. That root sits outside an inplace
workspace, so `tui` and `acp` do not need `--state`. Project-local scratch is
`--state ./.shikigami-state`. Embedders that want a cwd-local root call
`StateRoot::default_in`; CLI-like hosts call `StateRoot::user_default` or
`StateRoot::resolve_host`. New state directories are created mode `0700` on
Unix.

Earlier defaults keep working:

- **3.x location.** 3.0 kept roots under the platform state directory
  (`$XDG_STATE_HOME/shikigami/`, else macOS `~/Library/Application Support/shikigami/`,
  Windows `%LOCALAPPDATA%\shikigami\`, other Unix `~/.local/state/shikigami/`).
  While `projects/<encoded-cwd>` does not exist and that root does, the CLI
  keeps using it and prints a note with the command that moves it. Nothing is
  moved automatically, because an older binary still writing there would split
  the state. Run the move when no shikigami process uses the directory.
- **2.x location.** Without `--state`, an existing `./.shikigami-state` stops the
  CLI with an error that names the move command and the `--state` alternative,
  so earlier runs, parked runs, and its config do not silently disappear. It is
  never adopted automatically because a directory inside a repository may be
  repository content.

## Profiles

Behavioral profiles are labels for settings version 1. Prefer explicit
governance/model adapters and `governance.fail_closed`, as shown in the
repository examples. A future schema direction is proposed in
[Discussion #146](https://github.com/Sannrox/shikigami/discussions/146).

| Name | Effect |
| --- | --- |
| `local` | Default. Offline-friendly. |
| `security-finder` | Specialised read-only reviewer roles (`[security_finder] role`) that emit typed findings matching `pkg:security.finding-exposure/v1`. Does not force a governance or model adapter. See [security-finder.md](security-finder.md). |
| `governed` / `site-agent` / `security-triage` | Retired. Fail at resolve with an ADR 0024 upgrade error. |

Custom profile names are allowed as labels. `security-finder` applies the
read-only role checks. Version 1 expands a file profile before applying
environment overrides.

| File settings | Environment | Effective result |
| --- | --- | --- |
| none | none | profile `local`; governance `none`; model `scripted`; fail-open |

Always inspect the complete effective wiring with `doctor`.

## Schema (`version = 1`)

Top-level `version` is required and must be `1`.

### `[profile]`

| Field | Default | Description |
| --- | --- | --- |
| `name` | `"local"` | Profile label / preset |

### `[security_finder]`

Used by profile `security-finder`. Empty on other profiles.

| Field | Default | Description |
| --- | --- | --- |
| `role` | unset | `dependency-advisory`, `source-change`, `deployment-manifest`, or `authorization-boundary`. Required when the profile is `security-finder`. |

### `[governance]`

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"none"` | `none` \| `local` \| `http-callback` (`host-authz`). `sekai-chisei` fails at resolve (ADR 0024). |
| `endpoint` | unset | Authorization URL for `http-callback`; required for callback use |
| `principal` | `"shikigami"` | Identity presented to a host broker (not a secret) |
| `namespace` | unset | Governance namespace. Required for `http-callback` and `host-authz`; there is no implicit default |
| `fail_closed` | `false` | Fail doctor/run when governance is unhealthy. Always on for `http-callback` and `host-authz` |
| `token_env` | unset | `http-callback` / `host-authz`: env var name holding the host Bearer token |

Dropped fields such as `client_secret_env` and `allow_insecure_remote` fail
parse (`deny_unknown_fields`).

#### `[governance.delayed_evidence]`

Bounded spool for signed already-executed outcomes. Disabled by default.
Unresolved entries are never silently discarded. Queueing copies lease, fence,
and policy identities from a current grant; Shikigami never mints those fields
or an `accepted` disposition. When `enabled` is false, `Unavailable` on
`complete_run` is preserved and nothing is written.

| Field | Default | Description |
| --- | --- | --- |
| `enabled` | `false` | Persist and reconcile delayed envelopes |
| `max_entries` | `32` | Fail closed when unresolved capacity is exhausted |
| `retention_ms` | 7 days | Mark older unresolved entries expired; do not evict them |
| `allow_test_signatures` | `false` | Accept fixture `test-hmac-sha256` envelopes |

### `[model]`

The engine always calls `ModelPort`. Adapter `plane` fails at resolve
([ADR 0024](decisions/0024-local-guest-rusui-plane-caller.md)).

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"scripted"` | `scripted` \| `http` |
| `script_json` | built-in demo script | JSON array of turns for `scripted`. An entry `{"summary": "..."}` is not a turn; it answers compaction summaries. A turn with `requires_context: ["..."]` fails unless each text appears in a message it sees. |
| `base_url` | OpenAI-compatible default | Base URL for `http` (Chat Completions). A local gateway such as CLIProxyAPI is typically `http://127.0.0.1:8317/v1`. |
| `model` | `"auto"` | Direct HTTP treats `auto` as `gpt-4.1-mini`. Set an explicit id for a gateway catalog (see [`examples/cliproxy-http.toml`](../examples/cliproxy-http.toml)). |
| `api_key_env` | `"OPENAI_API_KEY"` | Env var for the HTTP API key. For a local gateway this is the gateway access key (for CLIProxyAPI, `CLIPROXY_API_KEY`), not an upstream provider token. |
| `input_usd_micros_per_mtok` | unset | Optional cost rate: USD microdollars per million **input** tokens (1_000_000 = $1/MTok). Both rates required for `RunResult.cost`. |
| `output_usd_micros_per_mtok` | unset | Optional cost rate: USD microdollars per million **output** tokens |
| `effort` | unset | Optional reasoning effort; adapters that do not speak it ignore it |
| `alternates` | `[]` | More model ids a session may switch to with ACP `session/set_model` or TUI `/model`, on the same adapter, endpoint, and key. Entries must be distinct and non-empty. |

When either cost rate is unset, `RunResult.cost` is **absent** (not zero). Never invents provider prices.

The `http` adapter retries a transient provider failure up to 2 times per turn:
a timeout, a refused connection, or a 408, 429, or 5xx response. It waits with
jittered exponential backoff from 0.5s, or for the provider's `Retry-After`
seconds, capped at 30s. Each retry emits a `status` event such as
`model retry 1/2: model provider returned 503 Service Unavailable`. Any other
failure, such as 401 or a malformed body, fails the turn at once. Responses are
not streamed, so a retry never joins two partial answers.

#### `[model.fallback]`

Opt-in governed local-model fallback ([ADR 0007](decisions/0007-governed-local-fallback.md)).
Denied by default. Presence of a model artifact is not authorization.

| Field | Default | Description |
| --- | --- | --- |
| `enabled` | `false` | Permit attempting fallback when a current signed grant exists |
| `adapter` | unset | Local adapter after admission: `scripted` \| `http` |
| `artifact_path` | unset | Extra bytes bound into the local digest; required for `http` fallback |
| `script_json` | unset | Optional scripted turns for the fallback adapter |
| `allow_test_signatures` | `false` | Accept fixture `test-hmac-sha256` envelopes; production unknown algorithms stay unverifiable |

No delivery-system keys. Unknown fields deny at parse time. Production
enablement still requires a verifiable host-issued envelope whose model digest
matches. This crate does not call a plane to mint a grant.

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

### `[session.modes.<name>]`

ACP session catalog ([ADR 0016](decisions/0016-session-mode.md)). Names are
`low`, `medium`, `high`, and `ultra`. Extra names fail closed. Empty mapping
inherits `[model]` and `[tools]`. Omit the table and omitted ACP `mode` keeps
today's spawn.

| Field | Default | Description |
| --- | --- | --- |
| `model` | inherit `[model].model` | Model id for this mode |
| `effort` | unset | Optional reasoning effort; adapters that do not speak it ignore it |
| `prompt` | unset | Extra system-prompt section |
| `tools` | inherit | Optional further restrict the host tool set. Empty list fails closed. |

### `[workspace]`

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"directory"` | `directory` \| `inplace` (`directory-inplace`) \| `git-worktree` |
| `root` | `"."` | Parent/repo root for materialization; for `inplace`, the workspace path itself |
| `branch_prefix` | `"shikigami/"` | Branch prefix for git-worktree |
| `snapshot` | `false` | After first materialize, copy workspace to `state/runs/<id>/snapshots/initial` and keep that copy across resume. Resume never creates the directory. Directory walks and file copies do not follow symbolic links. Not supported with `inplace`. This is the only original-input proof `replay-export` accepts. |

For `inplace`, the harness **state** root must sit **outside** `workspace.root`.
The process-host default (`~/.shikigami/projects/<encoded-cwd>`) already
does. `--state` / `SHIKIGAMI_STATE` still win; a root inside the workspace fails
closed. Hosts must serialize concurrent runs against the same inplace root.

### [sandbox]

| Field | Default | Description |
| --- | --- | --- |
| backend | "none" | `none` (no isolation), Unix-only `rlimit` (limits + process group), Linux-only `linux_native` (Landlock + seccomp via pinned kekkai), or macOS-only `seatbelt` (Seatbelt via kekkai `macos`; ADR 0013, ADR 0020) |
| cpu_time_secs | unset | CPU seconds for Bash children |
| memory_mb | unset | Address-space limit for Bash children |
| user_processes | unset | Per-real-user process ceiling (RLIMIT_NPROC); not isolated per run |
| file_size_mb | unset | Maximum child-created file size |
| open_files | unset | Maximum open descriptors |
| read_only_paths | `[]` | Extra absolute paths granted read/execute under `linux_native` and `seatbelt` |
| tool_sockets | `deny` | `deny` (default): `linux_native` and `seatbelt` children cannot open sockets. `host`: explicit job/operator grant that lifts the socket gate; doctor reports host-enforced, unverified by the harness (ADR 0013) |

`none` is the historical default and produces a doctor warning. `rlimit` is
opt-in limits-only (doctor label: `limits`); it is not OS isolation. Non-Unix
hosts reject `rlimit`; non-Linux hosts reject `linux_native`; non-macOS hosts
reject `seatbelt`. Selecting a backend the host cannot provide fails `doctor`
and `run` under every profile.

`linux_native` applies in the child's `pre_exec`: Landlock ABI 3+ (kernel 6.2)
is required so `truncate` is mediated. ABI 6 (kernel 6.12) additionally scopes
signals to the sandbox; below that, doctor reports `signal_scoping=none`
(same-user `kill` of the harness remains a residual). The workspace and a
run-owned `TMPDIR`
are the writable trees; `/proc`, `$HOME`, and host secrets are denied; `socket()`
fails for every address family (`socketpair` is limited to `AF_UNIX` stream
pairs for pipelines). Landlock does not mediate `chmod`/`chown`/`setxattr` on
inaccessible paths; that is a kernel residual, not an allowlist. Tool-child sockets
stay denied even when `[network]` is an allowlist unless `sandbox.tool_sockets
= "host"`. `[network]` still governs only harness-owned HTTP clients.

`seatbelt` is the macOS development profile (never a hosted tier). It maps
onto kekkai `Backend::Macos` and keeps the settings name `seatbelt`. Doctor
reports `backend=seatbelt`. The profile is deny-default reads with a system
allowlist (including the root directory inode, not the whole tree), canonical
workspace plus a run-owned `TMPDIR` as the writable set, and network denied
unless `tool_sockets = "host"`. Bash children exec `/bin/bash` so the
interpreter is inside that allowlist. Toolchains outside the allowlist go in
`read_only_paths`. See
[network.md](network.md),
[ADR 0013](decisions/0013-os-sandbox-adapter.md), and
[ADR 0020](decisions/0020-kekkai-sandbox-pin.md).

Under `governance.fail_closed = true` or a remote governance adapter
(`http-callback`, `host-authz`), enabling Bash (explicitly or via
`tools.mode = workspace_exec`) **refuses** configuration when
`sandbox.backend = none` or `network.egress = unrestricted`. Operators must set
`sandbox.backend = rlimit`, `linux_native`, or `seatbelt`, and
`network.egress = deny` or `allowlist`.

### `[network]`

Harness-owned HTTP clients only (`http` model, MCP HTTP, `web_fetch`,
`web_search`, and OTLP/HTTP tracing export). Does not interpose bash sockets; see
[network.md](network.md).

| Field | Default | Description |
| --- | --- | --- |
| `egress` | `unrestricted` | `unrestricted` \| `deny` \| `allowlist` |
| `allow_hosts` | `[]` | Exact hostnames when `egress = allowlist` (case-insensitive) |

### `[run]` (tool concurrency)

| Field | Default | Description |
| --- | --- | --- |
| `tool_concurrency` | `4` | Max concurrent tools when a turn’s batch is **all parallel-safe** (`read_file`, `glob`, `grep`, `web_fetch`, `web_search`). `1` forces sequential. Any write/bash/`todo_write`/`handoff`/`report`/`escalate` batch runs **serially**. Isolated `wait=false` `child_run`s are not globally parallel-safe; they share a nested start-wait under `nested_max_children` when the batch has no workspace-mutating tools. `wait=true` stays serial. Tool messages are applied in original call order. |

### `[tools]`

| Field | Default | Description |
| --- | --- | --- |
| `mode` | `custom` | `custom` \| `read` \| `workspace` \| `workspace_exec` |
| `enabled` | `[]` | Allow-list; with non-`custom` mode, **intersects** the mode set |
| `bash_timeout_secs` | `60` | Default bash timeout (capped at 120s) |
| `model_output_max_bytes` | `65536` | Cap for bash combined stdout/stderr and MCP tool text before they become model tool messages. Oversize results keep a prefix plus `…[truncated]` and stay `ok`. Clamped to 2 MiB (the bash memory bound). `web_fetch` keeps its own 256 KiB cap. |
| `respect_ignore` | `true` | `glob`/`grep` honor built-in defaults + `.gitignore` / `.shikigamiignore` |
| `mcp_lazy_schemas` | `false` | When true, attached MCP tools stay out of the model tool list until `load_mcp_tool` offers them. The system prompt lists each attached `mcp.<server>.<tool>` with a one-line description (200-char cap). Names in a preset's `mcp_tools` (or a site-agent allow-list) stay fully offered. Off by default: every attached schema is sent each turn. |

| Mode | Effective tools (before optional `enabled` intersect) |
| --- | --- |
| `custom` | `enabled` if set, else coding default (writes/delete/search, `load_skill`, **no** bash) |
| `read` | `read_file`, `glob`, `grep`, `load_skill`, `todo_write`, `handoff`, `report`, `escalate` |
| `workspace` | coding default (no bash) |
| `workspace_exec` | coding default + `bash` (also exposes `bash_background` / `bash_job_status` / `bash_job_logs`) |

Background jobs (when bash is enabled): start with `bash_background`, wait or
poll with `bash_job_status` (optional `timeout_ms`, same cap as foreground
bash) / `bash_job_logs`. Jobs are killed when the run finishes
(success, failure, or park). ACP sessions keep them across parks until the
run ends or the host exits ([acp.md](acp.md)). Max 4 concurrent jobs; logs
capped at 256KiB and stored in host scratch, not the workspace.

Foreground and background Bash share one explicitly reconstructed child
environment. For 1.x compatibility, ordinary parent variables remain
available, while active `governance.token_env`, HTTP `model.api_key_env`, MCP
bearer-token env names, and shell-startup controls are removed. Bash startup
files are disabled so they cannot repopulate the child environment. MCP stdio
server children use the same reconstructed environment (credentials are not
inherited from the harness process).

Managed hosts must launch Shikigami with an allowlisted parent environment when
additional ambient variables are untrusted or sensitive. A configurable child
environment policy would change the public Rust settings shape and is deferred
to a future major-version API/schema decision. Environment filtering does not
provide filesystem, PID, or network isolation.

Coding default includes `delete_file` (unlink one regular workspace file;
directories, missing paths, and jail escapes fail). It is the pair of
`write_file` and is **not** in `read` mode.

Coding default includes `todo_write` (run-scoped checklist; max 32 items).
It is **not** a plane work-unit API and does not replace `escalate`/park.

Coding default includes `handoff` (brief event: required `task`, optional
`decisions` / `files` / `ignore`). It does **not** start a session, child
run, or plane session; the host that sees `handoff_brief` decides what to
do next. Skills cannot omit `task`.

Coding default also includes `apply_patch` (structured multi-hunk patches with
optional context; atomic across files). Prefer `edit` / `multi_edit` for unique
single-site or multi-site replacements (exact first, then the fixed normalization
fallback only when there is no exact match); use `apply_patch` when surrounding
context is needed to disambiguate. Each batch hunk matches the file as read;
merge nearby changes into one hunk if their spans (including patch context) overlap. Caps: 16 files, 32 hunks, 64KiB JSON payload.
All three accept LF fragments for consistently CRLF files while preserving
CRLF output. See [edit outcomes](runs.md#edit-tools-and-outcomes) for journal
records and per-model failure aggregation.

`web_fetch` is **opt-in only** (not in coding default or mode presets). Add it via
`tools.enabled` (custom mode or intersect). See [network.md](network.md).

`web_search` is the same opt-in class. It is not registered until
`[tools.web_search].url` is set **and** `web_search` is in `tools.enabled`.
`tools.mode = read` does not include it. See [network.md](network.md).

Modes are host policy, not an OS sandbox.

`shikigami doctor` explains this composition without changing it:

| Doctor field | Meaning |
| --- | --- |
| `tools.mode` | Configured permission mode |
| `tools.configured` | Explicit `tools.enabled` input, or `(none)` |
| `tools.preset` | Complete tool set supplied by the selected mode |
| `tools.excluded` | Preset tools removed by a non-`custom` intersection |
| `tools.implicit` | Model-visible helpers sharing parent authority (currently background helpers when `bash` is effective) |
| `tools.effective` | Final configured enforcement allow-list before implicit expansion |
| `tools.visible` | Model-visible builtin tools after implicit expansion |
| `tools.external` | Configured MCP servers whose definitions are resolved at run start |
| `tools.environment` | Fixed v1 child-environment rule and protected variable names; never values |

These diagnostics are compatibility-only visibility: empty `custom`, explicit
custom lists, named presets, and named-preset intersections retain their
existing behavior. No configuration migration is required, and doctor does not
grant tools. Governed external-action authorization and any OS isolation apply
in addition to this host policy.

When `respect_ignore = true` (default), search tools skip heavy dirs (`node_modules`,
`target`, `.git`, …) and patterns from workspace `.shikigamiignore` / `.gitignore`
(no negation/`!` in v1; pure matcher, no git binary). **`read_file` of an explicit
path is never blocked by ignore** — ignore is convenience filtering, not a secret vault.

### `[[tools.mcp_servers]]`

Each entry attaches one MCP server whose tools join the registry as
`mcp.<name>.<tool>`; list those names in `tools.enabled` to authorize them.

| Field | Default | Description |
| --- | --- | --- |
| `name` | (required) | Registry prefix |
| `command` / `args` | `""` / `[]` | Stdio server executable; `mock` registers offline `echo` and `wide` tools |
| `transport` | `stdio` | `stdio` or `http` |
| `url` / `token_env` | unset | HTTP endpoint and optional bearer-token env name |
| `framing` | `content-length` | Stdio request framing: `content-length` (LSP-style headers) or `newline` (MCP stdio specification; `sekai-mcp`, reference servers). Responses are accepted in either framing. |
| `timeout_secs` | `30` | Per-request deadline for `tools/list` and `tools/call`; must be > 0. On expiry the call fails closed with `deadline_exceeded` and the remote outcome must be reconciled through the server's receipt surface. |

A `tools/call` result flagged `isError` is a failed tool call (`ok = false` in
governance reports and events). See [mcp.md](mcp.md).

### `[tools.web_search]`

Off when `url` is unset. The builtin is not in the coding default or mode
presets. POST JSON `{"query":"..."}` to `url`; the endpoint must answer
`{"results":[{"title":"...","url":"...","snippet":"..."}]}`. At most 5
results, each title/snippet clipped, and the tool message capped. Result URLs
are not fetched here; later `web_fetch` still applies SSRF and egress.

| Field | Default | Description |
| --- | --- | --- |
| `url` | unset | Search POST endpoint, checked against `[network]` egress |
| `token_env` | unset | Env var holding a Bearer token; never written to events |

### `[tools.mcp_gateway]`

Off when unset. When set, the gateway is the only remote MCP server: its tools
join as `mcp.gateway.<tool>`, each request carries the run's live job
credential, and HTTP `[[tools.mcp_servers]]` entries fail validation. See
[mcp.md](mcp.md#gateway-mode).

| Field | Default | Description |
| --- | --- | --- |
| `url` | (required) | Gateway MCP endpoint, checked against `[network]` egress |
| `timeout_secs` | `30` | Per-request deadline; must be > 0 |

### `[context]`

| Field | Default | Description |
| --- | --- | --- |
| `load_project_rules` | `true` | Load listed rules files from the workspace and each ancestor up to the git root (directory containing `.git`, file or directory). Without a git root, only the workspace directory. Files above the git root are not loaded. |
| `rules_filenames` | `["AGENTS.md","shikigami.rules.md"]` | Every existing name is attached in list order at each directory, git-root first then nearer directories, each under its own heading. Headings name the path relative to the git root. Missing names are skipped. Flat names only (`..`, `/`, and `\\` are ignored). |
| `max_rules_bytes` | `32768` | Shared cap across all loaded files (every directory). The file that would cross the cap is truncated with a marker; later files are omitted. |
| `skills_root` | unset → `.shikigami/skills` under workspace | Root for skill packs. The TUI offers each pack as `/name` (or `/skill:name`; built-ins win a collision) and also searches `.agents/skills`. |
| `skills` | `[]` | Skill directory names (`<root>/<id>/SKILL.md`). The prompt lists them; the agent loads a body with `load_skill`. Without that tool, bodies go into the system prompt. |
| `eager_skills` | `[]` | Ids from `skills` whose bodies go into the system prompt at run start |
| `max_skill_bytes` | `32768` | Per-skill size cap |
| `standing_max_percent` | `25` | When `run.context_window_tokens` is set, refuse to start a run whose standing prompt (project rules + eager skill bodies + lazy catalog lines + JSON of offered tool schemas) exceeds this percent of the window. This gate counts 4 characters ≈ 1 token (a conservative over-estimate). Unset window: `doctor` still reports bytes and does not refuse. `doctor` always reports the breakdown; overflow is `standing.over` on doctor and a start error on `run`. |

Rules and skills are **untrusted text** given to the model (not executed). Ancestor rules files are prompt-only: tools stay jailed to the workspace. Disable rules with `load_project_rules = false`. Leave `skills` empty for no packs. `doctor` prints the standing breakdown (rules, eager skills, tool-schema bytes, sum). Operators who set a small window and large eager skills see a start error that names what overflowed.

### `[run]`

| Field | Default | Description |
| --- | --- | --- |
| `max_turns` | `50` | Hard stop for the turn loop. Unattended `run` applies it to the run lifetime. Session hosts (`session_wait`) apply it per prompt, including ask/escalate resumes of that prompt. |
| `compact_after_messages` | unset | Compact middle history when message count exceeds N (off by default). With `local` or `none` governance the model summarizes the compacted span and the summary replaces it (`context_summarized`). Brokered governance, a failed summary, or three failures in a row drop the span behind a marker instead (`context_compacted`). The run checkpoint keeps every message either way. |
| `compact_on_context_length` | `true` | After the model refuses the prompt as too large for its context window, compact with the same cut as `compact_after_messages` and retry that turn once. A second context-length refusal, or a history that cannot shrink, fails the run with a context-length error. Off disables the retry; count-based compact stays off by default. |
| `compact_keep_tail` | `8` | Messages the host *tries* to keep after the first task message when compacting |
| `compact_keep_tail_bytes` | `32768` | Byte cap on that verbatim tail, applied with `compact_keep_tail`. The tail is whole messages and a complete tool exchange. The last exchange is kept even when it exceeds the cap. |
| `context_window_tokens` | unset | Operator-declared context window. When set, compact with the same cut as `compact_after_messages` before the next model call if the last completed turn reported `input_tokens` at or above `compact_at_usage_percent` of this window. Unset leaves fill-based compact off. Missing usage on a turn skips this trigger. `doctor` names the window when set. Must be greater than zero. |
| `compact_at_usage_percent` | `80` | Percent of `context_window_tokens` that last-turn `input_tokens` must reach to compact (1–100). Consulted only when the window is set. |
| `timeout_secs` | unset | Optional overall wall-clock limit (checked at turn boundaries) |
| `plan_jail` | `false` | Restrict mutating tools to `.shikigami/plan.md` until a parked plan is accepted. `report` parks `ParkKind::Plan` with the plan digest. Resume with `RunRequest.resume_plan` / `--plan-accept` / `--plan-reject`. Only Accept clears the jail; Reject completes failed and keeps it. `doctor` names the mode when selected. |
| `nested` | `false` | Enable `child_run` / `child_status`. A child is a first-class Run with a typed profile (`explore` read-only, `plan` write-jail, `full` parent authority). Default shares the parent workspace (ACP/TUI inplace included); `worktree=true` isolates with `git-worktree` outside the parent jail (harvest is the child summary; successful isolated trees are cleaned up with `git worktree remove`, including after `--resume` of a parked plan child, unless the resume sets `keep_workspace`). Plan-jail parents may start shared-workspace children; `worktree=true` is denied because materialize mutates the parent git checkout. `full`/`plan` children inherit the parent jail (writes stay on `.shikigami/plan.md`); `explore` is read-only. Explore and plan-jailed children do not attach parent MCP servers. Children run unattended to a summary (they do not inherit `session_wait`). Session hosts ask=park before `full`/`plan` `child_run` and before any `worktree=true`. Git worktree add/remove runs with hooks disabled. Children do not inherit nested tools, including on resume, so only one nesting level is implemented (`nested_max_depth` > 1 is reserved). A parked parent restores nested tools on `--resume`. Unattended park cancels `wait=false` children and waits until their registry rows are terminal (in-flight tools drop on cancel). Session hosts leave them running until the parent completes. Parent complete keeps a shared workspace while a recorded child is still parked or running. `local`/`none` authorize the tool names; `http-callback` and `sekai-chisei` keep host/plane allow-lists. `doctor` names the mode when selected. |
| `nested_max_depth` | `1` | Maximum nested depth (root is 0). Must be greater than zero. |
| `max_stop_repairs` | `3` | Refused completions a `stop` hook allows before the run ends failed ([hooks.md](hooks.md#stop-verify-before-success)) |
| `nested_max_children` | `4` | Maximum children one parent may start. Must be greater than zero. Depth and fan-out caps fail closed. Isolated `wait=false` `child_run`s in one turn start concurrently up to this cap. |

CLI / env override: `shikigami run --timeout-secs N` or `SHIKIGAMI_RUN_TIMEOUT_SECS`.
`shikigami run --plan-jail` or `SHIKIGAMI_RUN_PLAN_JAIL`.
`shikigami run --nested` or `SHIKIGAMI_RUN_NESTED`.
Embedders may also pass `RunRequest.timeout` and a cooperative
`RunRequest.cancel` (`tokio::sync::watch::Receiver<bool>`). Cancel and timeout
surface as errors (`RunError::Cancelled` / `TimedOut`), never as silent success.

### Resume

Local checkpoints are written under `$SHIKIGAMI_STATE/runs/<run_id>/checkpoint.json`
after each turn (version `1`). Resume:

```bash
shikigami run --resume <run-id>
# or library: RunRequest { resume_run_id: Some(id), resume_answer: Some(...), .. }
```

### Park / escalate (headless)

When the model calls the `escalate` tool, the run terminates with
`termination=parked` (library `RunTermination::Parked`, CLI non-zero exit).
Workspace and checkpoint are retained. Resume:

```bash
shikigami run --resume <run_id> --answer "operator decision"
# or --answer-file path
```

Resume without an answer errors (no silent success/deny).

Checkpoints are harness scratch only — not plane truth. Prompt id must match
the current system prompt or resume fails.

### `[events]`

| Field | Default | Description |
| --- | --- | --- |
| `adapter` | `"stderr"` | `stderr` \| `jsonl` \| `none` |

`jsonl` appends under the state runs directory (`events.jsonl`).

### `[tracing]`

Optional OpenTelemetry span export. **Off by default.** See
[tracing.md](tracing.md).

| Field | Default | Description |
| --- | --- | --- |
| `enabled` | `false` | Create and export spans |
| `exporter` | `"none"` | `none` \| `otlp` |
| `endpoint` | unset | File path, `file://` path, or OTLP/HTTP collector URL |

`enabled = true` requires `exporter = "otlp"` and `endpoint`. HTTP endpoints
obey `[network]` egress. Disabled export leaves offline behavior unchanged.
Attributes are an identity allowlist; prompts and tool payloads are never
exported.

### `[[hooks]]`

Optional operator-trusted subprocess hooks. Empty list (the default) disables
hooks. See [hooks.md](hooks.md).

| Field | Default | Description |
| --- | --- | --- |
| `event` | required | `pre_run` \| `post_run` \| `pre_tool` \| `post_tool` \| `on_park` \| `pre_prompt` \| `stop` ([hooks.md](hooks.md)) |
| `command` | required | Executable path or name on `PATH` |
| `args` | `[]` | Extra argv |
| `timeout_ms` | `5000` | Kill after this duration (capped at 120s; `stop` at 1h) |
| `fail_closed` | `false` | Fail the tool/run on hook failure or timeout |

### `[tui]`

Key bindings, colors, and a status line command for `shikigami tui`
([ADR 0023](decisions/0023-tui-customization.md)). Like every setting, they
come only from the settings file you chose. A `shikigami.toml` in the project
is never loaded implicitly, so a cloned repository cannot rebind keys or run a
status line command. An
unknown action or role, a key that is not `ctrl+<letter>` or `alt+<letter>`,
an invalid color, or two actions on one key stops launch with the entry named.

```toml
[tui.keys]
toggle_plan = "alt+p"
interrupt = "ctrl+g"

[tui.colors]
muted = "#8a8a8a"
selected = "cyan"
```

`[tui.keys]` maps an action to a key. Unlisted actions keep their default.
`ctrl+m`, `ctrl+i`, and `ctrl+h` are refused because terminals send them as
Enter, Tab, and Backspace.

| Action | Default | Does |
| --- | --- | --- |
| `interrupt` | `ctrl+c` | Cancel the running prompt, or quit when idle |
| `quit` | `ctrl+d` | Quit when idle |
| `toggle_plan` | `ctrl+p` | Show or hide the plan |
| `toggle_tool` | `ctrl+o` | Expand or collapse the selected tool line |
| `copy` | `ctrl+y` | Copy the last answer (OSC 52) |
| `scroll_line_up` | `ctrl+k` | Scroll the transcript one row up |
| `scroll_line_down` | `ctrl+j` | Scroll the transcript one row down |
| `line_start` | `ctrl+a` | Move the cursor to the start of the draft |
| `line_end` | `ctrl+e` | Move the cursor to the end of the draft |
| `clear_input` | `ctrl+u` | Clear the draft |

`[tui.colors]` sets a foreground color per role: a name (`red`, `lightblue`,
`gray`, ...), an ANSI index, or `#rrggbb`. With `NO_COLOR` set, the names are
still checked but colors are not applied.

| Role | Default | Covers |
| --- | --- | --- |
| `text` | terminal default | Answers and the composer |
| `muted` | dim | Prompts, tool and system lines, rules, lists, footer |
| `selected` | reversed | The highlighted row in a list or ask |

`[tui.statusline]` runs an operator-trusted command, like a
[lifecycle hook](hooks.md), and shows the first line it prints in the footer
in place of the model and turn, cut to 48 columns. It runs out of process,
off the draw path, whenever the session state below changes. A command that
prints nothing, exits non-zero, or runs past `timeout_ms` leaves the default
footer. The first non-zero exit or timeout also adds one line to the
transcript; later ones stay quiet.

```toml
[tui.statusline]
command = "/usr/local/bin/my-status"
timeout_ms = 200
```

| Field | Default | Description |
| --- | --- | --- |
| `command` | required | Executable path or name on `PATH` |
| `timeout_ms` | `200` | Kill after this duration (capped at 5s) |

The command gets one JSON line on stdin. `schema_version` changes only when a
field changes meaning or goes away; new fields may appear in the same version.

```json
{"schema_version":1,"session_id":"…","cwd":"/work/repo","model":"claude-sonnet-5-5","status":"ready","turn":2,"run_id":"…"}
```

| Field | Meaning |
| --- | --- |
| `session_id` | ACP session shown in this window |
| `cwd` | Working directory the session belongs to |
| `model` | Model name, or `demo` / `scripted` |
| `status` | Footer status word: `ready`, `running`, `compacting`, `ask`, `error`, ... |
| `turn` | Prompts sent in this session |
| `run_id` | The session's run, or `null` until the first prompt finishes |

## Environment variables

| Variable | Purpose |
| --- | --- |
| `SHIKIGAMI_HOME` | Absolute path that replaces `~/.shikigami` as the home of per-project state roots |
| `SHIKIGAMI_STATE` | State root directory (overrides the `SHIKIGAMI_HOME` default) |
| `SHIKIGAMI_CONFIG` | Path to settings file |
| `SHIKIGAMI_PROFILE` | Profile name |
| `SHIKIGAMI_GOVERNANCE_ADAPTER` | Governance adapter id |
| `SHIKIGAMI_CONTROL_PLANE` | Maps to `governance.endpoint` (http-callback URL) |
| `SHIKIGAMI_GOVERNANCE_NAMESPACE` | Override `governance.namespace` |
| `SHIKIGAMI_MODEL_ADAPTER` | Model adapter id |
| `SHIKIGAMI_MODEL` | Override the configured model name; HTTP `auto` maps to `gpt-4.1-mini` |
| `SHIKIGAMI_MODEL_SCRIPT` | Scripted JSON (inline) |
| `SHIKIGAMI_PRESET` | Preset name or file; see [presets.md](presets.md) |
| `SHIKIGAMI_RUN_TIMEOUT_SECS` | Overall run wall-clock limit |
| `SHIKIGAMI_SERVE_LISTEN` | Filesystem-serve HTTP control bind |
| `SHIKIGAMI_SERVE_AUTH_TOKEN_ENV` | Env var name holding the serve HTTP bearer token |
| `OPENAI_API_KEY` | Default HTTP model key |
| *(value of `token_env`)* | Callback host bearer token when configured |

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

No separate fuzz job is required for 1.x.

There are **no** tenkai environment variables for the harness process. Tenkai
only installs or upgrades the binary; see
[../examples/tenkai-product.toml](../examples/tenkai-product.toml).

## Fail-closed rules

Doctor and `run` fail when:

- `fail_closed` is true or a remote governance adapter (`http-callback`,
  `host-authz`) is selected, **and**
- governance is unhealthy (e.g. `http-callback` without endpoint, or live
  probe failure for the host broker).

Settings that name adapter `sekai-chisei`, model `plane`, or profile
`governed` / `site-agent` / `security-triage` fail at resolve
([ADR 0024](decisions/0024-local-guest-rusui-plane-caller.md)).

Offline defaults never require a host broker.

### Upgrading remote-governance configs

Remote governance adapters fail closed and need an explicit namespace.
For each host that selects `http-callback` or `host-authz`:

1. Set `governance.namespace` (or `SHIKIGAMI_GOVERNANCE_NAMESPACE`). Configs
   that relied on the old implicit value use `namespace = "default"`.
   Without it, settings resolution fails at startup.
2. Expect doctor, `run`, and `serve` to refuse when the host broker is
   unreachable or refuses, even without `fail_closed = true`. `fail_closed =
   false` no longer relaxes a remote adapter.
3. If Bash is enabled, set `sandbox.backend = rlimit`, `linux_native`, or
   `seatbelt` and `network.egress = deny` or `allowlist`; the fail-closed
   Bash rule now applies.
4. Run `shikigami doctor` with the new settings before starting the host.

A config that still sets `governance.adapter = "sekai-chisei"` or
`client_secret_env` fails at resolve or parse.

## Examples

- [../examples/local-run.toml](../examples/local-run.toml)
- [../examples/cliproxy-http.toml](../examples/cliproxy-http.toml)
- [../examples/seatbelt-sandbox.toml](../examples/seatbelt-sandbox.toml)

Adapter semantics: [adapters.md](adapters.md).

## Doctor JSON (`schema_version` = 1)

`shikigami doctor --json` emits a `DoctorReport` object. Add `--models` to
include `available_models` and `default_model` from the local model adapter.
Use the global `--model MODEL` flag (or `SHIKIGAMI_MODEL`) for a final
operator override; it is applied before adapter construction.

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
| `standing_bytes` | number | Additive. Sum of standing prompt bytes (rules + eager skills + catalog lines + offered tool-schema JSON). |
| `standing` | object | Additive. Breakdown: `bytes`, `rules` (`filename`, `bytes`), `eager` (`id`, `bytes`), `catalog` (`id`, `bytes`), `tool_schema_bytes`. MCP schemas are resolved at run start, so doctor counts builtins (plus `load_skill` / `load_mcp_tool` when those tools would be offered). |

Breaking renames/removals of these fields require a `schema_version` bump and
a breaking release note. Additional fields may appear without a bump.

## Compatibility policy (1.0)

1. **`version` is required** and must equal the crate's supported schema
   (`1` today). Unsupported versions fail at load time.
2. **Unknown keys are rejected** at every table (`deny_unknown_fields`). Fix
   typos rather than silent ignore.
3. **Breaking changes** (rename/remove freeze-core fields, change defaults that
   alter security posture, change settings schema `version`) require a settings
   `version` bump and/or crate major version, plus a breaking release note.
4. **Additive fields** with `#[serde(default)]` may ship in the same
   settings `version` when they do not change existing behavior.
5. From **crate 1.0**, freeze-core public API and doctor JSON follow semver;
   the settings `version` field remains the config compatibility signal.
