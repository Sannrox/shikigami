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
shikigami acp
```

Credentials come from the environment, same as CLI. There is no ACP login.

## Methods

| Method | Direction | Role |
| --- | --- | --- |
| `initialize` | client → agent | Speak protocol version 1. Accept client `protocolVersion` 1 or 2 (number or decimal string). Always reply `protocolVersion: 1` with the capability payload below. Other versions are `-32602 unsupported protocolVersion`. Success is not an agreement to speak v2. |
| `session/new` | client → agent | Create a session id over a workspace `cwd`. Optional `mode` (`low` / `medium` / `high` / `ultra`) freezes the session ([ADR 0016](decisions/0016-session-mode.md)). Omit the field for today's spawn. With no `[model]` configured, the built-in demo's workspace is `demo-workspace` under the state root instead of `cwd`, so the demo never writes into the project. |
| `session/load` | client → agent | Restore a known session and replay conversation via `session/update` before responding; **unknown ids fail closed** |
| `session/prompt` | client → agent | Drive one prompt until `end_turn` / cancel / error. A `mode` different from the frozen session mode is `-32602`. Attachment parts (`image`, `audio`, `resource`) pass through under ADR 0006 bounds when the adapter can read them; unsupported types fail closed. Omit attachments for today's text host. |
| `session/set_model` | client → agent | Experimental: the session model selection in the protocol is not stable yet, and only the TUI depends on it. Later prompts of the session use `modelId`; a running prompt keeps its model. The id must be one the session offers: ungoverned, `[model].model` plus `[model].alternates`; governed, the plane catalog. A plane that does not answer refuses with `-32000` and `data.shikigami.kind = "modelCatalog"`, and the model stays (fail closed); an id not offered is `-32602`. The choice is kept with the session across a host restart. Result `{}`. |
| `session/mcp` | client → agent | Experimental: TUI `/mcp`. `action` is `list` (default), `disable`, or `enable`. List returns `{servers:[{name, tools, disabled}]}` from settings plus this process's overlay. Disable/enable require `name`, refuse `gateway` and a name that is not attached (`-32602`), and return `{}`. Disable hides that server on later `session/prompt` of this host; it is not written to settings or the session record. A restarted host follows settings again. |
| `session/compact` | client → agent | Shrink the live run's middle history (same cut as auto-compact). Idle only. |
| `session/update` | agent → client | Assistant text streams as `agent_message_chunk` updates while the model produces it, when the adapter streams (`http` does); each chunk carries the turn's `messageId`. Other adapters send one chunk per completed turn. `session/cancel` during a streamed turn stops the request; the turn is not recorded and the next prompt plans it again. Tool-call arguments do not stream. |
| `session/request_permission` | agent → client | Ask=park, plan review, and freeform escalate |
| `session/cancel` | client → agent | Existing cancel marker |

`initialize` reply (`agentInfo.version` is the crate version). `promptCapabilities` is true only for kinds the selected adapter can read (scripted: image, audio, and embeddedContext; HTTP: image and embeddedContext; plane: all false):

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
Background bash jobs stay running across the session's parks (prompt wait,
ask, escalate, plan) and stay addressable by `bash_job_status`
(poll, or wait with `timeout_ms`) / `bash_job_logs`. Job logs stay in host
scratch, not the workspace. They end
when the run completes or fails, when `session/cancel` interrupts a turn
or a permission wait, and when the host exits on EOF, SIGINT, or SIGTERM.
A host killed with SIGKILL cannot reap them. A host restart loses them:
their ids report `status=unknown`, and nothing restarts them. Exited jobs
stay readable and stop counting against the four-job limit once their
shell exits; leftover children are killed then. Nested `child_run` is a tool event on the parent session; there is
no extra ACP method. `wait=false` tool results, later `session/update`s,
`session/prompt` results, and `session/load` results carry
`_meta.shikigami.children`: `{ "runId", "profile", "status" }` with
`status` one of `running` / `parked` / `done` / `failed`. Clients that ignore
`_meta` keep the tool line only.
`session/cancel` keeps the session run and parks it for the next prompt.
A later prompt on an outstanding ask/escalate park restores the permission
wait; an unreadable checkpoint fails closed instead of starting a new run.

Ungoverned mutating tools (`write_file`, `edit`, `multi_edit`, `apply_patch`,
`bash`, `bash_background`) **ask=park** with the same tool identity as approval
park. The permission options are `allow` (`allow_once`), `allow_session`
(`allow_always`), and `deny` (`reject_once`). `allow_session` records that tool
name on the live host session so later calls of the same name skip the dock;
`/new`, `session/load`, and a new process start empty. The grant is not
checkpointed. `escalate` parks use the same permission RPC without
`allow_session`; allow/deny resume with `resume_answer` (`approved` / `denied`,
or `outcome.answer` when the client sends it). Plan write-jail (`run.plan_jail`)
parks `report` as `ParkKind::Plan` with the plan text in the permission
question; Allow accepts execute authority on the same run, Deny rejects,
completes failed, and keeps the jail. `child_run` `full` / `plan` /
`worktree=true` parks stay one-shot. Ask parks stay fail-closed. Brokered
adapters (`http-callback` / plane) do not ask=park. Clients that cannot answer
`session/request_permission` cannot complete a mutating prompt.

Content parts reuse `run_content` bounds. `session/prompt` concatenates `text`
parts. A session whose first prompt carries attachments is a content session
([ADR 0017](decisions/0017-acp-attachments.md),
[ADR 0021](decisions/0021-session-attachment-custody.md)): any later prompt that
answers an end-of-turn wait may carry more, including media types the adapter
reads that the session has not used yet. The host keeps each payload private to
the session under its state directory (`acp-sessions/<session>.content/`,
owner-only) so `session/load` works after a restart. Payloads are not copied
into the workspace, a checkpoint, or event log lines, and they are deleted when
the session's run ends or is replaced. Host start removes payload directories
whose session record is gone. The 8MiB part, 16MiB aggregate, and 32-part
bounds hold over the whole conversation, follow-up text included; a prompt that
would exceed them fails with `-32602` and the session continues. An attachment
on a session that started as text fails with `-32602`; start a new session to
attach. Unsupported types fail closed. Omit attachments and today's text host
is unchanged. Attachment sessions inherit content v1: no escalate parking.
`session/compact` still cuts ChatMessage history.

`session/load` replays each stored prompt as one `user_message_chunk` with its
own `messageId` (`user-<n>`). A compaction marker replays as a
`user_message_chunk` with `_meta.shikigami.compacted = true`; clients show it
as a note, not as something the user typed. A `report` result replays as its
summary, as it was shown live.

When a restored session's last host went away mid-step, the `session/load`
result carries `_meta.shikigami.interrupted.toolCalls`: one
`{ "toolCallId", "outcome" }` per replayed tool call that has no result.
`outcome` is `not_run` when the run was waiting on a permission, plan, or
question, and `in_doubt` when the recovery diagnosis reports an uncertain tool.
Load does not change the run: the next `session/prompt` asks the pending
question again. Clients that ignore `_meta` see today's replay.

One process drives a session at a time. `session/new` and `session/load` take
an exclusive OS lock on `acp-sessions/<session>.lock` and hold it until the
host exits; a crashed host releases it at once. A `session/load` of a session
another process holds fails with `-32000` and
`data.shikigami.kind = "sessionInUse"`; start a new session instead. The lock
file is empty and older hosts ignore it.

A `session/prompt` that fails because the model failed returns `-32603` with
`data.shikigami.kind = "model"`; other failures carry no `data`. When the
cause is known, `data.shikigami.cause` names it: `scriptEnded` (a scripted
model has no more turns), `unreachable` (no connection or a timeout),
`credentials` (the provider answered 401 or 403), or `account` (402 or 429:
billing, quota, or rate). As after a
cancel, the run then waits for the next prompt, which continues it with its
history.

The `session/prompt` and `session/load` results carry
`_meta.shikigami.runId` once the session has a run: the id that
`shikigami runs <id>` and `shikigami export <id>` accept. A follow-up prompt
usually keeps the same run, but a rejected plan or a new run after a
cancellation changes it, so read it from the latest result.

They also carry `_meta.shikigami.context` once a prompt has composed the
run's standing attachments: `{ "rules": [{ "filename", "bytes", "truncated"? }],
"eager", "catalog", "nested", "model", "mode"? }`. No file bodies. A host
restart reports it again after the next prompt. Clients that ignore `_meta`
are unchanged.

The `session/new` and `session/load` results carry `models`
(experimental): `{"currentModelId": "...", "availableModels": [{"modelId":
"...", "name": "..."}]}`. It is absent when the catalog cannot be read.

A `session/prompt` result also carries `_meta.shikigami.usage`
(`{"inputTokens": n, "outputTokens": n}` plus optional `cachedInputTokens` /
`cacheCreationInputTokens` when the model reported prompt-cache usage) with
the tokens that prompt used, summed over its run segments (a prompt that
waits for a permission is more than one). It is absent when the model
reported no usage (a model that reports zero counts as none); absent means
unknown, not zero. A segment that reports nothing adds nothing, so a prompt
mixing both is a lower bound. A client adds prompts up for a session total.

Session mode is [ADR 0016](decisions/0016-session-mode.md). `session/new` may
name a catalog mode; the mapping lives in `[session.modes]` settings. Invalid
mappings return `-32602` before session creation or first-prompt freezing.
Compatibility: before [#418](https://github.com/Sannrox/shikigami/pull/418),
invalid mappings surfaced later as `-32603` on `session/prompt`, after session
creation and potentially freezing. Clients spanning this change should accept
both codes for this specific mapping error and handle rejection at either
method; other `-32603` failures remain internal errors.
A rejected first-prompt mode leaves the session available for a valid retry. A later
`session/prompt` that names a different mode is refused. The selected mode,
model, effort, and tool set are a `session/update` with
`sessionUpdate: session_mode`. Omit `mode` and today's spawn is unchanged.

## Proof

Deterministic fake-client tests live in `src/acp.rs`. Live rusui stays ignored,
like `plane_live`.

Accepted contract: [ADR 0014](decisions/0014-usable-guest-hosts.md).

Inline attachment `data` and resource `blob` base64 payloads are checked against
the 8 MiB decoded part limit before compaction or decoding allocates buffers.
Whitespace and final padding are accounted for, so valid payloads at the limit
remain accepted.
