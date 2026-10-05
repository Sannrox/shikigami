# TUI

`shikigami tui` is a thin interactive process host over the in-process ACP
session. Same protocol rusui speaks; not a second turn loop. Evolving, same
rank as [`mcp.md`](mcp.md) and [`acp.md`](acp.md). Not part of the ADR 0004
freeze-core CLI (`version` / `doctor` / `run` / `serve`). Bare `shikigami`
stays usage/help.

```bash
shikigami --state ./state tui
```

Credentials come from the environment, same as CLI. There is no TUI login.
Ungoverned HTTP through a local OpenAI-compatible gateway uses the same
`--config` as `doctor` / `run`; see
[`examples/cliproxy-http.toml`](../examples/cliproxy-http.toml).

## Behavior

| Action | Mapping |
| --- | --- |
| Start | `initialize`, then `session/load` of the last session in this cwd, or `session/new` when load fails or none exists |
| Prompt | Enter sends `session/prompt` |
| Stream | Dense transcript of `session/update` (assistant text, tool calls) |
| Scroll | PageUp / PageDown through the transcript; a new prompt returns to the tail |
| Permission | Overlay on `session/request_permission`; `y` allow, `n` deny. Plan write-jail review uses this overlay (leading wrapped rows of the question; the overlay does not page a plan taller than the terminal). |
| Plan | Overlay when a `plan` session update arrives; Ctrl-P toggles, Esc hides |
| Cancel | Ctrl-C while a prompt is running sends `session/cancel` |
| Quit | Ctrl-C when idle, or Ctrl-D when idle |

Continue-last-in-cwd is fail closed: an unknown or unreadable previous session
starts `session/new`.

The host requires a terminal. It does not add a theming engine, plugin store,
voice, dashboard, or web view. Nested `child_run` appears as a tool event in
the transcript.

## Proof

Deterministic in-process tests in `src/tui.rs` drive the ACP mapping without a
TTY: mutating-tool park → permission overlay → resume; continue-last-in-cwd
load and fail-closed new session; plan overlay from a `session/update`.

Accepted contract: [ADR 0014](decisions/0014-usable-guest-hosts.md).
