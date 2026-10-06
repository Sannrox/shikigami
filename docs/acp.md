# ACP

`shikigami acp` is a thin **Agent Client Protocol** process host over
`Harness`. It is the rusui P1 guest contract and the protocol the in-crate
[`tui`](tui.md) speaks. Evolving, same rank as [`mcp.md`](mcp.md). Not part of
the ADR 0004 freeze-core CLI (`version` / `doctor` / `run` / `serve`).

Transport: JSON-RPC 2.0 on **stdio**, newline-delimited. Inbound frames may
be up to 32MiB so inline attachments fit under the 16MiB aggregate payload
cap after base64. MCP Content-Length stays the MCP host at 1MiB. No network
bind.

```bash
shikigami --state ./state acp
```

Credentials come from the environment, same as CLI. There is no ACP login.

## Methods

| Method | Direction | Role |
| --- | --- | --- |
| `initialize` | client → agent | Speak protocol version 1. Accept client `protocolVersion` 1 or 2 (number or decimal string). Always reply `protocolVersion: 1` with the capability payload below. Other versions are `-32602 unsupported protocolVersion`. Success is not an agreement to speak v2. |
| `session/new` | client → agent | Create a session id over a workspace `cwd`. Optional `mode` (`low` / `medium` / `high` / `ultra`) freezes the session ([ADR 0016](decisions/0016-session-mode.md)). Omit the field for today's spawn. |
| `session/load` | client → agent | Restore a known session and replay conversation via `session/update` before responding; **unknown ids fail closed** |
| `session/prompt` | client → agent | Drive one prompt until `end_turn` / cancel / error. A `mode` different from the frozen session mode is `-32602`. Attachment parts (`image`, `audio`, `resource`) pass through under ADR 0006 bounds when the adapter can read them; unsupported types fail closed. Omit attachments for today's text host. |
| `session/compact` | client → agent | Shrink the live run's middle history (same cut as auto-compact). Idle only. |
| `session/update` | agent → client | One notification per completed model turn (honest streaming) until the model adapter streams |
| `session/request_permission` | agent → client | Ask=park, plan review, and freeform escalate |
| `session/cancel` | client → agent | Existing cancel marker |

`initialize` reply (`agentInfo.version` is the crate version). `promptCapabilities` is true only for kinds the selected adapter can read (scripted: image, audio, and embeddedContext; HTTP and plane: all false):

```json
{
  "protocolVersion": 1,
  "agentCapabilities": {
    "loadSession": true,
    "promptCapabilities": {
      "image": true,
      "audio": true,
      "embeddedContext": true
    }
  },
  "agentInfo": { "name": "shikigami", "version": "<crate version>" },
  "authMethods": []
}
```

A **session** is a host id over **runs**. Unattended `shikigami run` still
stops on `report`, park, or limit. ACP treats a no-tool assistant message and
a session `report` as wait (`stopReason: end_turn`) so follow-ups keep the
same conversation. The next `session/prompt` resumes that run. `max_turns`
is a per-prompt budget on session waits, including ask/escalate resumes of
the same prompt. Hitting the budget parks `end_turn` with
`stopReason: max_turn_requests` so the next prompt can continue. A readable
session run that is not waiting fails closed instead of starting a new run.
Background bash jobs are reaped when a run parks; they do not survive
ask=park. Nested `child_run` is a tool event on the parent session; there is
no extra ACP method.
`session/cancel` keeps the session run and parks it for the next prompt.
A later prompt on an outstanding ask/escalate park restores the permission
wait; an unreadable checkpoint fails closed instead of starting a new run.

Ungoverned mutating tools (`write_file`, `edit`, `multi_edit`, `apply_patch`,
`bash`, `bash_background`) **ask=park** with the same tool identity as approval
park. `escalate` parks use the same permission RPC; allow/deny resume with
`resume_answer` (`approved` / `denied`, or `outcome.answer` when the client
sends it). Plan write-jail (`run.plan_jail`) parks `report` as `ParkKind::Plan`
with the plan text in the permission question; Allow accepts execute authority
on the same run, Deny rejects, completes failed, and keeps the jail. Governed approval parks stay
fail-closed. `http-callback` and sekai-chisei stay brokered. Clients that cannot
answer `session/request_permission` cannot complete a mutating prompt.

Content parts reuse `run_content` bounds. `session/prompt` concatenates `text`
parts and, when attachments are present on the first prompt of a session,
resolves them for that call through a session-scoped in-memory store
([ADR 0017](decisions/0017-acp-attachments.md)). Later prompts on that run are
text follow-ups. Payload bytes stay private to the session. They are not copied
into the workspace or event log lines. Unsupported types fail closed. Omit
attachments and today's text host is unchanged. Attachment sessions inherit
content v1: no escalate parking, and the 32-part sidecar bound includes
follow-up text. `session/compact` still cuts ChatMessage history.

Session mode is [ADR 0016](decisions/0016-session-mode.md). `session/new` may
name a catalog mode; the mapping lives in `[session.modes]` settings. A later
`session/prompt` that names a different mode is refused. The selected mode,
model, effort, and tool set are a `session/update` with
`sessionUpdate: session_mode`. Omit `mode` and today's spawn is unchanged.

## Proof

Deterministic fake-client tests live in `src/acp.rs`. Live rusui stays ignored,
like `plane_live`.

Accepted contract: [ADR 0014](decisions/0014-usable-guest-hosts.md).
