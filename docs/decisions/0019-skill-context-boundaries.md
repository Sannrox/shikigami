# ADR 0019: No skill-declared context boundaries

- Status: Accepted
- Date: 2026-10-06
- Resolves: [#330](https://github.com/Sannrox/shikigami/issues/330)
- Does not amend: [ADR 0004](0004-v1-contract.md) freeze-core
- Related: [ADR 0014](0014-usable-guest-hosts.md),
  [ADR 0015](0015-nested-child-runs.md),
  [ADR 0018](0018-skills-mcp-extension.md)
- Research: [guest-session-surfaces.md](../research/guest-session-surfaces.md),
  [usable-agent-guest.md](../research/usable-agent-guest.md)

## Context

The run loop compacts only on `compact_after_messages`, a count
threshold that knows nothing about the procedure being executed. Skill
authors know where a procedure's natural seams are. The question is
whether a skill pack may declare an intended reset or compaction point,
given that skill bodies are untrusted text.

[#330](https://github.com/Sannrox/shikigami/issues/330) also asked this
pass to collect evidence on compact-on-context-length-error (today only
the count threshold). That is extra evidence, not a second decision
question.

Since the issue opened, hosts already have TUI `/compact`, ACP
`session/compact`, and the `handoff` tool (brief event for a fresh
session). Nested child runs ([ADR 0015](0015-nested-child-runs.md))
already isolate a subtask's window.

## Decision

Accept [#330](https://github.com/Sannrox/shikigami/issues/330) option 1.

1. **No skill-level boundary.** A skill pack cannot declare a compaction
   or reset seam. Untrusted skill text must not drop the task message,
   policy, or authority limits.
2. **Procedures that need a clean window are a new run or session.** The
   host starts a fresh session and may pass a `handoff` brief as the
   first prompt. Nested `child_run` isolates a subtask inside one
   session. Document that pattern in [skills.md](../skills.md).
3. **Host compact stays host-owned.** `compact_after_messages`, TUI
   `/compact`, and ACP `session/compact` remain. They are not triggered
   by skill frontmatter.
4. **Overflow compact is host policy, not a skill seam.** Compact when
   the model reports a context-length error may be added later as a
   setting next to `compact_after_messages`. It does not give skill
   packs reset authority. It is not in this ADR's implementation scope.

## Consequences

- Progressive skill load (catalog in the prompt plus a load tool)
  remains a later feature. It does not land as part of closing #330.
- Fail-closed events and replay stay as they are: host compact already
  emits `ContextCompacted`. No skill-id attribution for a declared
  boundary is required, because none exists.
- [ADR 0014](0014-usable-guest-hosts.md) left #330 unchanged. This ADR
  closes it.

## Rejected alternatives

1. **Declarative boundary in the skill pack (option 2).** Untrusted text
   would gain reset authority. A buggy or hostile pack could strip
   constraints. Host-split runs and `handoff` already cover the seam.
2. **Skill declares, operator authorizes (option 3).** Still a skill
   format and run-loop change for a case the host can already split.
3. **Boundary as a tool gated by skill-declared points (option 4).** A
   first-party compact tool already exists as host `/compact` and
   `session/compact`. Gating it on skill text still lets the pack shape
   when history disappears.
4. **Remove host compact in favor of handoff only, or remove handoff in
   favor of auto-compact only.** Shikigami already has both. Each serves
   a different guest: count/user compact inside one run; `handoff` for a
   fresh session. Nested children cover in-loop isolation. Dropping
   either would shrink the guest without a compensating gain.
