# Lifecycle hooks

Optional **operator-trusted** subprocess hooks for run and tool boundaries.
Disabled when the `hooks` list is empty (default).

Hooks are **not** a sandbox and **not** a plugin marketplace. Only configure
commands you trust on the host.

The TUI status line command (`[tui.statusline]`, see
[settings](settings.md#tui)) runs the same way: trusted, JSON on stdin,
killed at its time limit.

A command runs in its own process group. At its time limit, or when the run
that started it ends first, the whole group is killed, so processes it started
do not keep running. The command counts as running until its output is
closed: a process it starts in the background keeps running after the command
exits only if it redirects stdin, stdout, and stderr (for example
`job >/dev/null 2>&1 </dev/null &`). One that still holds the command's output
counts against the time limit and is killed with the group.

## Settings

```toml
[[hooks]]
event = "pre_tool"          # pre_run | post_run | pre_tool | post_tool | on_park | stop
command = "/usr/local/bin/my-hook"
args = []
timeout_ms = 5000
fail_closed = true          # abort tool/run on failure or timeout
```

| Field | Default | Description |
| --- | --- | --- |
| `event` | required | When the hook fires |
| `command` | required | Executable path or name on `PATH` |
| `args` | `[]` | Extra argv |
| `timeout_ms` | `5000` | Kill after this duration (capped at 120s; `stop` at 1h) |
| `fail_closed` | `false` | On failure/timeout: fail tool/run if true; ignore if false. `stop` always fails closed. |

## Payload

JSON on stdin:

```json
{"event":"pre_tool","payload":{"run_id":"…","tool":"bash","args_json":"…"}}
```

Env: `SHIKIGAMI_HOOK_EVENT=<event>`.

## `stop`: verify before success

A `stop` hook checks a run before it finishes as a success: when the model
finishes without tools, or reports `success: true`. It does not run on
failure, cancel, timeout, or park, nor for content runs, session hosts
(ACP, TUI, MCP sessions), or replays.

```toml
[[hooks]]
event = "stop"
command = "/opt/checks/verify.sh"   # keep it outside the workspace
timeout_ms = 600000

[run]
max_stop_repairs = 3                # shared by all stop hooks of the run
```

Unlike other hooks, a `stop` hook runs in the workspace (working directory)
under the tool sandbox, because it runs code the model wrote. A path in
`command` is resolved against the harness's working directory before the run,
never the workspace, so the model cannot edit the check.

Stdin carries `{"event":"stop","payload":{"run_id","attempt","stop_hook_active","last_assistant_message"}}`.
`stop_hook_active` is true once an earlier attempt in this run was refused.

| Result | Verdict |
| --- | --- |
| exit 0, no JSON on stdout | pass |
| exit 0, `{"passed": true}` | pass |
| exit 2 | fail; `reason` or stderr goes to the model |
| `{"decision": "block", "reason": "…"}` | fail |
| `{"passed": false}` or a case with `outcome` `fail`/`error` | fail |
| any other exit, a signal, timeout, spawn error, malformed JSON | fail |

The JSON may list `cases`: `[{"suite","name","outcome":"pass|fail|error|skipped","message"}]`.
Without them the hook itself is the one case. Every check emits a
`verification_finished` event (`attempt`, `passed`, `cases`).

On failure the reason (ANSI stripped, cut to its last 4000 characters) becomes
the next user message and the run continues. After `max_stop_repairs` refused
attempts the next failure ends the run with `success: false`, termination
`failed`, and a summary starting `verification failed after N repair attempts`.
The refusal count lives in the checkpoint, so a restart keeps the bound. A
crash mid-check reruns the check.

## Security

- Config owner can run arbitrary commands → treat like shell profile.
- Doctor reports hook **count and command names**, not secrets.
- Prefer fail-open notify hooks; reserve fail-closed for hard gates.
