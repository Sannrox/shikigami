# TUI

`shikigami tui` is a thin interactive process host over the in-process ACP
session. Same protocol rusui speaks; not a second turn loop. Evolving, same
rank as [`mcp.md`](mcp.md) and [`acp.md`](acp.md). Not part of the ADR 0004
freeze-core CLI (`version` / `doctor` / `run` / `serve`). Bare `shikigami`
stays usage/help.

```bash
shikigami --state ../tui-state tui
```

The session is inplace on the current directory. `--state` / `SHIKIGAMI_STATE`
must sit outside that directory; the default `./.shikigami-state` is inside it
and fails closed.

Credentials come from the environment, same as CLI. There is no TUI login.
Ungoverned HTTP through a local OpenAI-compatible gateway uses the same
`--config` as `doctor` / `run`; see
[`examples/cliproxy-http.toml`](../examples/cliproxy-http.toml).

## Behavior

| Action | Mapping |
| --- | --- |
| Start | `initialize`, then `session/load` of the last session in this cwd, or `session/new` when load fails or none exists |
| Prompt | Enter sends `session/prompt`. Framed composer (`>` between two `─` rules); short drafts stay one row; Shift+Enter / Alt+Enter insert a newline and the composer grows in place (capped like ask/plan). Bracketed paste inserts as typed text, including newlines. `>` on the first composer line only. Left/Right/Home/End move the cursor; Up/Down move inside a multiline draft and walk sent prompts from the first or last row. Draft stays visible while a turn runs. While busy, a static dim `running` row sits above the composer (same slot as the slash list, above the top `─`); idle has no extra row. Enter while a turn is running queues one follow-up (the current draft) and shows dim `queued  …` in the footer; a second Enter replaces it; Esc drops it unless a slash list, resume list, or plan is open (those dismiss first). After the in-flight turn finishes, the host sends the queued `session/prompt` the same as a normal Enter. Ctrl-C cancel drops the queue without sending it. |
| Slash | Type `/` while idle to open a dim command list above the composer (Tab complete, Up/Down select, Esc dismiss, Enter run). `/help` writes a short system block of keys and slash names, then returns to the idle composer. `/compact` shrinks the live run history. `/copy` (Ctrl+Y while idle) copies the last assistant transcript line to the terminal clipboard via OSC 52; no assistant line is a system line; success is a dim footer `copied`. `/attach path` stages a host file as an ACP prompt part (image, audio, or PDF) without copying it into the workspace; the next Enter or `/skill:name` sends it with the draft or skill text. Attachments are accepted on the first prompt of a session (`/new` then attach); later prompts stay text. Files over 8MiB or an unsupported type fail before the payload is loaded. `/new` starts a new session in this cwd. `/resume` opens the same dim list of persisted sessions for this cwd (short session id, newest first). Enter loads via `session/load`; Esc dismisses; an empty list is a system line with no picker; load failure stays on the current session and writes a system line. `/exit` and `/quit` leave the host. `/skill:name` loads `.shikigami/skills/<name>` or `.agents/skills/<name>` and sends it as the prompt, including any staged attachments. Unknown slash is a normal prompt. |
| Stream | Dense transcript of `session/update` (dim `you` prefix, assistant text, `·` tool name/path, blank line between turns) |
| Tool output | Ctrl+O expands the last tool to a wrapped `·`-prefixed block; a second Ctrl+O collapses. No-op when there are no tools. |
| Scroll | Ctrl+K / Ctrl+J move the transcript one visual row; PageUp / PageDown / wheel page it (a new prompt returns to the tail). Overflowing ask/plan docks take PageUp/PageDown and wheel on that slot first; at the top or bottom they resume transcript paging. Mouse capture is on while the alt-screen is up, so drag-select may not work; PageUp/PageDown stay the keyboard page path. |
| Permission | Replaces the composer on `session/request_permission` (title plus argument fields, `> y allow` / `n deny`); Esc cancel. Plan write-jail review uses this composer. Overflowing argument lists scroll in the dock; they are not truncated. The transcript stays visible. |
| Plan | Replaces the composer when a `plan` session update arrives; Ctrl-P toggles, Esc hides |
| Cancel | Ctrl-C while a prompt is running sends `session/cancel` |
| Quit | Ctrl-C when idle, Ctrl-D when idle, or `/exit` / `/quit` |

Continue-last-in-cwd is fail closed: an unknown or unreadable previous session
starts `session/new`.

Rendered transcript, permission, plan, command-list, and status text replaces
control characters with visible `�` marks, preserving newlines and tabs.
Escape sequences in model or tool output therefore remain inert.

`/copy` uses OSC 52. Over SSH that can land clipboard contents on the local
terminal; some terminals ignore OSC 52.

The host requires a terminal. It does not add a theming engine, plugin store,
voice, dashboard, or web view. Status is one dim footer under the composer
(short session id and keys; long errors go to the transcript). Nested
`child_run` appears as a tool event in the transcript. The loop redraws only
when state changes. Stderr event JSON is discarded while the alt-screen is up.

## Proof

Deterministic in-process tests in `crates/shikigami-tui` drive the ACP mapping without a
TTY: mutating-tool park → permission dock → resume; continue-last-in-cwd
load and fail-closed new session; `/resume` picker load, Esc, and fail-closed
unknown id; plan dock from a `session/update`.

Accepted contract: [ADR 0014](decisions/0014-usable-guest-hosts.md).
