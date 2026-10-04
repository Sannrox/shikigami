# ACP

`shikigami acp` is a thin **Agent Client Protocol** process host over
`Harness`. It is the rusui P1 guest contract and the protocol the in-crate
[`tui`](tui.md) speaks. Evolving, same rank as [`mcp.md`](mcp.md). Not part of
the ADR 0004 freeze-core CLI (`version` / `doctor` / `run` / `serve`).

Transport: JSON-RPC 2.0 on **stdio**, newline-delimited. MCP Content-Length
stays the MCP host. No network bind.

```bash
shikigami --state ./state acp
```

Credentials come from the environment, same as CLI. There is no ACP login.

## Methods

| Method | Direction | Role |
| --- | --- | --- |
| `initialize` | client → agent | Negotiate version; advertise `loadSession` |
| `session/new` | client → agent | Create a session id over a workspace `cwd` |
| `session/load` | client → agent | Restore a known session and replay conversation via `session/update` before responding; **unknown ids fail closed** |
| `session/prompt` | client → agent | Drive one prompt until `end_turn` / cancel / error |
| `session/update` | agent → client | One notification per completed model turn (honest streaming) until the model adapter streams |
| `session/request_permission` | agent → client | Ask=park and freeform escalate |
| `session/cancel` | client → agent | Existing cancel marker |

A **session** is a host id over **runs**. Unattended `shikigami run` still
stops on `report`, park, or limit. ACP treats a no-tool assistant message and
a session `report` as wait (`stopReason: end_turn`) so follow-ups keep the
same conversation. The next `session/prompt` resumes that run. `max_turns`
is a per-prompt budget on session waits, including ask/escalate resumes of
the same prompt. Hitting the budget parks `end_turn` with
`stopReason: max_turn_requests` so the next prompt can continue. A readable
session run that is not waiting fails closed instead of starting a new run.
Background bash jobs are reaped when a run parks; they do not survive
ask=park.
`session/cancel` keeps the session run and parks it for the next prompt.
A later prompt on an outstanding ask/escalate park restores the permission
wait; an unreadable checkpoint fails closed instead of starting a new run.

Ungoverned mutating tools (`write_file`, `edit`, `multi_edit`, `apply_patch`,
`bash`, `bash_background`) **ask=park** with the same tool identity as approval
park. `escalate` parks use the same permission RPC; allow/deny resume with
`resume_answer` (`approved` / `denied`, or `outcome.answer` when the client
sends it). Governed approval parks stay fail-closed. `http-callback` and
sekai-chisei stay brokered. Clients that cannot answer
`session/request_permission` cannot complete a mutating prompt.

Content parts reuse `run_content` bounds. This host concatenates `text` prompt
parts; it does not add a multimodal stack.

## Proof

Deterministic fake-client tests live in `src/acp.rs`. Live rusui stays ignored,
like `plane_live`.

Accepted contract: [ADR 0014](decisions/0014-usable-guest-hosts.md).
