# ADR 0016: Session mode frozen at first prompt

- Status: Accepted
- Date: 2026-10-06
- Resolves: [#368](https://github.com/Sannrox/shikigami/issues/368)
- Unblocks: [#372](https://github.com/Sannrox/shikigami/issues/372)
- Does not amend: [ADR 0004](0004-v1-contract.md) freeze-core
- Related: [ADR 0014](0014-usable-guest-hosts.md)
- Research: [guest-session-surfaces.md](../research/guest-session-surfaces.md)

## Context

`shikigami acp` has no mode argument. Every session uses one model
configuration. Interactive guests need a small named dial: the caller
picks how hard the session should work, not which vendor model id the
host happens to speak. The plane model proxy, when shikigami is a rusui
guest, must keep forwarding the body unchanged.

A mode that can change mid-session rewrites the system prompt and tool
set, invalidates provider prompt caches, and continues a conversation
under a different model than the one that started it.

## Decision

Accept [#368](https://github.com/Sannrox/shikigami/issues/368) option 1
together with option 3.

1. **A small named catalog.** Default names are `low`, `medium`, `high`,
   and `ultra`. Names are product vocabulary: they stay when the model
   behind a name changes. `medium` is the default when a host asks for a
   mode without naming one.
2. **A mode selects** the model, reasoning effort, system prompt, and
   tool set. Nested child runs stay [ADR 0015](0015-nested-child-runs.md).
   This decision does not add a first-party second-model consult tool.
3. **Mapping lives in settings.** Operators may retune which model,
   effort, prompt, and tools sit behind a name. They do not register new
   mode names through a loader. Extra names are a later decision.
4. **Frozen at first prompt.** `session/new` (and the equivalent TUI/CLI
   spawn) may name a mode. The session keeps that mode. A later
   `session/prompt` that names a different mode is refused. To work in a
   different mode, start a new session. Omit the mode and today's spawn
   is unchanged.
5. **Visible in events.** The selected mode name is on the run's events.
   Do not put a vendor model id on the ACP/CLI surface that does not
   speak it.
6. **Additive.** Freeze-core CLI stays `version` / `doctor` / `run` /
   `serve`. A session with no mode matches the current spawn.

[#372](https://github.com/Sannrox/shikigami/issues/372) honors this
contract on the ACP host. This ADR does not implement it.

## ACP mapping errors and compatibility

As implemented by [#405](https://github.com/Sannrox/shikigami/issues/405),
an invalid operator mapping (for example, a mode whose tools do not intersect
the host tool set) returns JSON-RPC `-32602` at `session/new`, before creating
any session. A mode supplied on the first `session/prompt` is validated before
freezing; rejection leaves the session unfrozen for a valid retry.

Previously, session creation could succeed and mapping validation failed later
on `session/prompt` with `-32603` from harness construction, potentially after
freezing. That error code and timing changed in
[#418](https://github.com/Sannrox/shikigami/pull/418). `-32603` is no longer the
invalid-mapping signal; it remains an internal-error code for other failures.
Clients supporting both versions should recognize both codes for this specific
mapping error and handle rejection at either method. They should not classify
all `-32603` errors as invalid mappings.

## Consequences

- Hosts pick a stable name. Product and operators retune the mapping
  without renaming sessions.
- Prompt-cache identity and model continuation stay coherent for the
  life of a session.
- Plugin-registered modes stay out: [ADR 0014](0014-usable-guest-hosts.md)
  rejected a plugin store; [ADR 0018](0018-skills-mcp-extension.md)
  rejects a loader.

## Rejected alternatives

1. **No mode object (option 2).** The operator names a model on spawn.
   That passes vendor ids through guests that should not speak them, and
   it has no stable name when the model changes.
2. **Mid-session mode switch.** Rewrites prompt and tools, busts the
   cache, and continues under a different model.
3. **Loader- or plugin-registered modes.** A second extension surface.
   Conflicts with ADR 0014 and ADR 0018.
4. **A marketplace of modes.** Out of scope for [#368](https://github.com/Sannrox/shikigami/issues/368)
   and rejected by the usable-guest closeout.
5. **A rusui model router.** rusui forwards the body; shikigami maps the
   name.
