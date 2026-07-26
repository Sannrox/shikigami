# Lifecycle hooks

Optional **operator-trusted** subprocess hooks for run and tool boundaries.
Disabled when the `hooks` list is empty (default).

Hooks are **not** a sandbox and **not** a plugin marketplace. Only configure
commands you trust on the host.

## Settings

```toml
[[hooks]]
event = "pre_tool"          # pre_run | post_run | pre_tool | post_tool | on_park
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
| `timeout_ms` | `5000` | Kill after this duration (capped at 120s) |
| `fail_closed` | `false` | On failure/timeout: fail tool/run if true; ignore if false |

## Payload

JSON on stdin:

```json
{"event":"pre_tool","payload":{"run_id":"…","tool":"bash","args_json":"…"}}
```

Env: `SHIKIGAMI_HOOK_EVENT=<event>`.

## Security

- Config owner can run arbitrary commands → treat like shell profile.
- Doctor reports hook **count and command names**, not secrets.
- Prefer fail-open notify hooks; reserve fail-closed for hard gates.
