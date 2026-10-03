# ADR 0015: Nested child runs as first-class Runs

- Status: Accepted
- Date: 2026-10-03
- Supersedes: —
- Amends: the 1.0 recommendation in
  [#90](https://github.com/Sannrox/shikigami/issues/90) (host-owned fan-out
  for the medium contract). Does not amend
  [ADR 0004](0004-v1-contract.md) freeze-core.
- Depends on: [ADR 0002](0002-run-identity.md)
- Related: [ADR 0014](0014-usable-guest-hosts.md) (plan write-jail;
  ACP/TUI hosts)
- Research: [usable-agent-guest.md](../research/usable-agent-guest.md)
- Source: [PR #331](https://github.com/Sannrox/shikigami/pull/331);
  maintainer accepted that closeout without a GitHub Design Discussion

## Context

[#90](https://github.com/Sannrox/shikigami/issues/90) closed with
**host-owned fan-out for 1.0**: hosts call `Harness::run` N times or
enqueue serve jobs. A nested-run tool required a later Design Discussion.

Post-1.0, an ACP/TUI guest needs a model-callable child with its own
context window and a summary return. Hosts cannot fake that from outside
the tool loop. rusui ADR 0040 defers *environment-plane* child sessions;
those are a different object.

## Decision

1. **A child is a `Run`** with its own `run_id` / attempt. The parent
   stores child identities on the checkpoint. There is no second agent
   type.
2. **Default off.** Additive settings. Depth and fan-out caps.
3. **The parent model gets a tool** that starts a child, waits or polls
   it, and receives a structured summary. Background spawn is allowed;
   completion notifies the parent. Children are still runs.
4. **Typed profiles** the model cannot invent: `explore` (read-only),
   `plan` (write-jail from [ADR 0014](0014-usable-guest-hosts.md)),
   `full`. Optional `git-worktree` isolation via the existing workspace
   adapter.
5. **Governed children `begin_run`.** Harvest correlates parent and
   child. A child park is not parent self-approval.
6. **Replay, cancel, timeout, and diagnosis apply per child.**
7. **Freeze-core behavior of a single run is unchanged.** New tool(s)
   and `RunResult` fields are additive.

Implementation collides with plan write-jail and
[#330](https://github.com/Sannrox/shikigami/issues/330) on `src/run/`.
Land one at a time. ACP/TUI hosts ([ADR 0014](0014-usable-guest-hosts.md))
may proceed if they only consume park and events.

## Consequences

- Follow-up is a feature Issue, not a Design Discussion.
- rusui child sessions stay out of this crate.
- Peer agent-teams that chat with each other stay out.

## Rejected alternatives

1. **Keep host-only fan-out forever.** Correct for 1.0. Insufficient for
   a guest whose model must delegate inside one session.
2. **A new “agent” object besides Run.** Violates “runs are the unit of
   work.”
3. **Environment-plane children in shikigami.** rusui owns that graph and
   deferred it.
