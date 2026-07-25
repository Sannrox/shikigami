# ADR 0003: `shikigami serve` local-queue daemon

- Status: Accepted
- Date: 2026-07-25
- Supersedes: —

## Context

One-shot `shikigami run` is insufficient for fleet hosts that must stay up and
accept work. A long-running process is required without turning shikigami into
a second control plane.

## Decision

1. **`shikigami serve` is a thin host** over the same `Harness` library used by
   one-shot CLI runs. It does not own policy, budget, or identity.
2. **v0.x intake is a local filesystem queue** under the state root
   (`$SHIKIGAMI_STATE/queue/inbox/`). Optional HTTP or plane work-unit polling
   may be added later without changing the core loop.
3. **Health** is process-local (HTTP on a loopback/admin port or a simple
   status file). Health is not a multi-tenant control plane.
4. **Graceful shutdown** drains in-flight runs or cancels them on signal
   (SIGTERM/SIGINT), then exits.
5. **Not a control plane.** sekai-chisei remains governance; serve only hosts
   execution.

## Consequences

- Offline tests can drop JSON jobs into the inbox and assert completion.
- Operators get a single binary for both one-shot and daemon modes.
- Plane work-unit admission stays out of scope until identity (#13) and
  harvest (#12) contracts stabilize for multi-host fleets.
