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
| Prompt | Enter sends `session/prompt`. Framed composer (`>` between two `─` rules); short drafts stay one row; Shift+Enter / Alt+Enter insert a newline and the composer grows in place (capped like ask/plan). Bracketed paste inserts as typed text, including newlines. `>` on the first composer line only. Left/Right/Home/End move the cursor; Up/Down move inside a multiline draft and walk sent prompts from the first or last row. Draft stays visible while a turn runs. |
| Slash | Type `/` while idle to open a dim command list above the composer (Tab complete, Up/Down select, Esc dismiss, Enter run). `/compact` shrinks the live run history. `/new` starts a new session in this cwd. `/exit` and `/quit` leave the host. `/skill:name` loads `.shikigami/skills/<name>` or `.agents/skills/<name>` and sends it as the prompt. Unknown slash is a normal prompt. |
| Stream | Dense transcript of `session/update` (dim `you` prefix, assistant text, `·` tool name/path, blank line between turns) |
| Scroll | PageUp / PageDown through the transcript, including while a permission is up; a new prompt returns to the tail |
| Permission | Replaces the composer on `session/request_permission` (title plus argument fields, `> y allow` / `n deny`); Esc cancel. Plan write-jail review uses this composer (leading wrapped rows of the question; it does not page a plan taller than the terminal). The transcript stays visible. |
| Plan | Replaces the composer when a `plan` session update arrives; Ctrl-P toggles, Esc hides |
| Cancel | Ctrl-C while a prompt is running sends `session/cancel` |
| Quit | Ctrl-C when idle, Ctrl-D when idle, or `/exit` / `/quit` |

Continue-last-in-cwd is fail closed: an unknown or unreadable previous session
starts `session/new`.

The host requires a terminal. It does not add a theming engine, plugin store,
voice, dashboard, or web view. Status is one dim footer under the composer
(short session id and keys; long errors go to the transcript). Nested
`child_run` appears as a tool event in the transcript. The loop redraws only
when state changes. Stderr event JSON is discarded while the alt-screen is up.

## Proof

Deterministic in-process tests in `src/tui.rs` drive the ACP mapping without a
TTY: mutating-tool park → permission dock → resume; continue-last-in-cwd
load and fail-closed new session; plan dock from a `session/update`.

Accepted contract: [ADR 0014](decisions/0014-usable-guest-hosts.md).
