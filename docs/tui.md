# TUI

`shikigami tui` is a thin interactive process host over the in-process ACP
session. Same protocol rusui speaks; not a second turn loop. Evolving, same
rank as [`mcp.md`](mcp.md) and [`acp.md`](acp.md). Not part of the ADR 0004
freeze-core CLI (`version` / `doctor` / `run` / `serve`). Bare `shikigami`
stays usage/help.

```bash
shikigami tui              # new session
shikigami tui --continue   # latest session in this folder
```

The session is inplace on the current directory, except for the built-in demo
(below). The process-host default puts
`--state` at `~/.shikigami/projects/<encoded-cwd>`, which sits outside
the workspace. `--state` / `SHIKIGAMI_STATE` still override; a root inside the
workspace fails closed at launch: the TUI exits non-zero with the refusal before
it writes anything, so the project stays unchanged.

Credentials come from the environment, same as CLI. There is no TUI login.
A missing key variable stops launch with the variable to set. With no
`[model]` configured, the built-in demo script answers: launch says so in one
system line and names how to configure a real model. The demo's write goes to
`demo-workspace` under the state root, never into the project, and the launch
line names that directory. With an HTTP model,
launch also checks in the background that the model address answers (a plain
request without the key, so no model request is spent); when it does not, one
system line says so and names the next step. Typing works meanwhile.
A prompt that fails because the model did not answer leads with the likely
cause and next step, then the upstream detail: the scripted model has no more
answers (configure a real model), the model cannot be reached (check the
address), the provider rejected the key (check the key variable), or the
provider refused for billing, quota, or rate (check the account). Any other
model failure points to `shikigami doctor`. The next prompt tries again.
Ungoverned HTTP through a local OpenAI-compatible gateway uses the same
`--config` as `doctor` / `run`; see
[`examples/cliproxy-http.toml`](../examples/cliproxy-http.toml).

## Behavior

| Action | Mapping |
| --- | --- |
| Start | `initialize`, then `session/new`. With `--continue`, `session/list` for this cwd then `session/load` of the session that last did work (its run checkpoint was written most recently), followed by one system line: `Continued your session from <age> (<n> turns).` When no session with a run exists or the load fails, `--continue` starts `session/new` and says so in a system line. The replay shows the outcomes seen live: a tool keeps its path and outcome (`cancelled`, `failed`), and a prompt that failed or was cancelled before any answer shows `This prompt got no answer.` |
| Prompt | Enter sends `session/prompt`. Framed composer (`>` between two `─` rules); short drafts stay one row; Shift+Enter / Alt+Enter insert a newline and the composer grows in place (capped like ask/plan). Bracketed paste inserts as typed text, including newlines. `>` on the first composer line only. Left/Right/Home/End move the cursor; Up/Down move inside a multiline draft and walk sent prompts from the first or last row. Draft stays visible while a turn runs. While busy, a static dim `running` row sits above the composer (same slot as the slash list, above the top `─`); idle has no extra row unless nested children are in flight. Nested `wait=false` children occupy that slot instead: one dim row per child with run id, profile (`explore` / `plan` / `full`), and status (`running` / `parked`). The block clears when none remain. Slash list and ask/plan docks take the slot first. No extra keys; `/run` is still the parent. Enter while a turn is running queues one follow-up (the current draft) and shows dim `queued  …` in the footer; a second Enter replaces it; Esc drops it unless a slash list, resume list, or plan is open (those dismiss first). A slash-command draft is not queued: it stays in the composer and a system line says it runs when the turn ends; Enter then runs it. `//text` and a path-like first word still queue as prompts. After the in-flight turn finishes, the host sends the queued `session/prompt` the same as a normal Enter. Ctrl-C cancel drops the queue without sending it. |
| Slash | Type `/` while idle to open a dim command list above the composer (Tab complete, Up/Down select, Esc dismiss, Enter run). `/help` writes a short system block of keys and slash names, then returns to the idle composer. `/context` writes a short system block of what this session's run attached: project rules files (name, bytes, truncated), eager and catalog skill ids, last compact (none, counts, or compacted), nested tools on or off, and the session model and mode when set. No file bodies. `/compact` shrinks the live run history. `/copy` (Ctrl+Y while idle) copies the last assistant transcript line to the terminal clipboard via OSC 52; no assistant line is a system line; success is a dim footer `copied`. `/attach path` stages a host file as an ACP prompt part (image, audio, or PDF) without copying it into the workspace; the next Enter or skill command sends it with the draft or skill text. A session that starts with an attachment (`/new` then attach) accepts attachments on any later prompt; a session that started as text stays text. Files over 8MiB or an unsupported type fail before the payload is loaded. `/plan` flags the next prompt of this session for plan write-jail: mutating tools write only `.shikigami/plan.md` until the permission dock Accepts the parked plan; Deny keeps the jail. A session that already ran without the jail is refused (use `/new` then `/plan`). Already jailed is a system line. Settings `run.plan_jail` stay off for sessions that never used `/plan`. `/new` starts a new session in this cwd. `/resume` lists this cwd's sessions via `session/list` (runs only, most recently active first) and opens the same dim list; each row shows age, turn count, and first prompt. Enter loads via `session/load`; Esc dismisses; an empty list is a system line with no picker; load failure stays on the current session and writes a system line. `/run` writes the session's run id with the `shikigami runs` and `shikigami export` commands that accept it, or says there is no run yet. `/usage` writes the session's token use per turn and in total; `?` marks a turn with no reported usage. `/model` lists the models the session offers (`[model].model` and `[model].alternates`, or the plane catalog when governed) with the current one marked; `/model <name>` switches the session's later prompts to it, and the footer then shows that name. A refused switch keeps the model and says why. The choice stays with the session for `--continue` and `/resume`. `/mcp` lists attached MCP servers and their tools; `/mcp disable <name>` hides that server for later prompts of this process; `/mcp enable <name>` restores it. The built-in gateway cannot be disabled. A name that is not attached is refused. The overlay is not saved: `--continue` and `/resume` in a new process follow settings again. `/exit` and `/quit` leave the host. A skill pack is a slash command by its id: `/name [args]` shows `/name [args]` in the transcript, sends the args (or a one-line follow instruction when args are empty) as the user prompt, and asks the host to `load_skill` that id (`_meta.shikigami.loadSkill`). The pack body is a tool result. The host advertises `compact`, `model`, and those skill ids (with the same `skill:` collision prefix) as `available_commands_update` after `session/new` and `session/load`; TUI-only names (`help`, `copy`, `attach`, `exit`, `quit`, `new`, `resume`, `run`, `usage`) stay off that list. The TUI catalog and that advertisement agree on compact, model, and skill ids. Staged attachments go with that prompt. The list shows packs as `name` after the built-ins. Built-ins win a name collision: a pack named like one (`help`, `new`, ...) is listed and reached as `/skill:name`. `/skill:name` keeps working for every pack. Built-ins also list first, so a pack never takes over a built-in's abbreviation (`/co` + Enter still runs `/compact`). A slash word that names no command is not sent: a system line says it is unknown, names the closest command when one is near, and the draft stays in the composer to fix or escape. Start with `//` to send a prompt that begins with `/` (`//text` sends `/text`). A first word with another `/` (a path such as `/usr/bin/env`) is sent as a normal prompt. |
| Mention | `@` at the start of a word, while idle, opens the same dim list with the workspace's files, filtered fuzzily on the path as you type (file-name matches first). The list follows the `glob` tool: files inside the session's workspace only, no symlinks, and nothing that `.shikigamiignore`, `.gitignore`, or the built-in ignore list hides. Folders the process cannot read are skipped. The files are read once per draft, in the background, so typing never waits. Tab or Enter puts `@path ` in place of the word; Esc closes the list for that word. The path is plain prompt text; nothing is attached. |
| Stream | Dense transcript of `session/update` (dim `you` prefix, assistant text, `·` tool name/path, blank line between turns) |
| Tool output | Tool lines show one line each. Ctrl+O expands the last tool to a wrapped `·`-prefixed block; a second Ctrl+O collapses. No-op when there are no tools. A `report` line is a turn's final answer and always shows its whole summary, wrapped. |
| Scroll | Ctrl+K / Ctrl+J move the transcript one visual row; PageUp / PageDown / wheel page it (a new prompt returns to the tail). Overflowing ask/plan docks take PageUp/PageDown and wheel on that slot first; at the top or bottom they resume transcript paging. Mouse capture is on while the alt-screen is up, so drag-select may not work; PageUp/PageDown stay the keyboard page path. |
| Permission | Replaces the composer on `session/request_permission` (title plus argument fields, `> y allow` / `n deny`); Esc cancel. Ungoverned mutating tools also offer `a allow for session` for that tool name until `/new` or a new process. Allow, session-allow, and deny apply only after the dock has been shown and typing has paused; a letter that was part of a draft never allows. Plan write-jail review, escalate, and `child_run` parks keep `y` / `n` only. Overflowing argument lists scroll in the dock; they are not truncated. The transcript stays visible. |
| Plan | Replaces the composer when a `plan` session update arrives; Ctrl-P toggles, Esc hides |
| Cancel | Ctrl-C while a prompt is running sends `session/cancel` |
| Quit | Ctrl-C when idle, Ctrl-D when idle, or `/exit` / `/quit`. When the session has a run, the shell then shows `Continue this session later: shikigami tui --continue` |
| Signal | SIGTERM or SIGHUP cancels a running prompt like Ctrl-C, restores the terminal like a quit, then exits with the signal's status |

`--continue` is fail closed: an unknown or unreadable previous session
starts `session/new`. When another window (another TUI or an ACP client) has
the latest session open, `--continue` starts a new session and writes
`The last session in this folder is open in another window. This is a new session.`
`/resume` marks such a session `in use` before its first prompt, and Enter on
it stays on the current session with a system line. Once the other window
exits or crashes, the session continues as usual. When the previous TUI died mid-step, the loaded session
marks the unfinished tool `not run` (it was waiting for permission) or
`result unknown` (it was running) and adds one system line saying so; the next
prompt asks the pending question again. `/resume` and `--continue` list text and
attachment sessions across a restart, and omit an attachment session whose
payloads are gone because its run ended.

Rendered transcript, permission, plan, command-list, and status text replaces
control characters with visible `�` marks, preserving newlines and tabs.
Escape sequences in model or tool output therefore remain inert.

`/copy` uses OSC 52. Over SSH that can land clipboard contents on the local
terminal; some terminals ignore OSC 52.

The host reports status using [OSC 7501](https://www.superlogical.com/rex/docs/build/program-status)
(revision 0.3): idle, working, blocked for permission, done after a completed
turn, and error. Reports contain only fixed state metadata and `app=shikigami`,
never transcript or tool text. Reports are sent on state changes; exiting the
TUI returns the record to idle. Terminals without support ignore the sequence.

The host requires a terminal. It does not add a theming engine, plugin store,
voice, dashboard, or web view. Status is one dim footer under the composer
(state word, model, `turn <n>` once a prompt was sent, the session's token
use, and keys; long errors go to the transcript). The session's run id is one
`/run` away. Token use is the sum of what each prompt reported (`14.2k tok`);
a turn whose model reported none, or one replayed from before this window, is
unknown, so the total shows as `? tok` or a lower bound such as `≥1.2k tok`,
never as zero. `/usage` lists in and out tokens per turn with the total, and
cached / cache-write counts when the host reported them. No
money is shown.
The state word is plain: `ready`, `running`, `ask`, `done` after a finished
turn, `cancelled`, `turn limit`, `length limit`, `refused`, or `error`; protocol
stop reasons never show. Nested
`child_run` appears as a tool event in the transcript, and in-flight children
also occupy the dim slot above the composer (run id, profile, `running` /
`parked`) until they finish. Nested off never shows that block. The loop redraws only
when state changes. Stderr event JSON is discarded while the alt-screen is up.

## Proof

Deterministic in-process tests in `crates/shikigami-tui` drive the ACP mapping without a
TTY: mutating-tool park → permission dock → resume; a plain start is new and
`--continue` replays the latest session, or starts new and says so; `/resume` picker load, Esc, and fail-closed
unknown id; plan dock from a `session/update`.

`crates/shikigami-cli/tests/tui_pty.rs` runs the built binary in a
pseudo-terminal and checks, from outside the process, that SIGTERM and SIGHUP
leave the alternate screen, turn off mouse capture, bracketed paste, and raw
mode, and show the cursor. It also checks that quitting a session with a run
prints the `--continue` command after the terminal is restored, that
`--continue` reopens that session, and that a second TUI does not take over a
session the first holds until the first is killed.

Accepted contract: [ADR 0014](decisions/0014-usable-guest-hosts.md). Customization (keys, colors, status line) is declarative and user-level only: [ADR 0023](decisions/0023-tui-customization.md). Keys, colors, and a status line command are set in [`[tui]`](settings.md#tui); the footer and `/help` show the bound keys.
