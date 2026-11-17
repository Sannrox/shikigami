# ADR 0021: ACP sessions keep their attachments

- Status: Accepted
- Date: 2026-11-17
- Resolves design for: [#537](https://github.com/Sannrox/shikigami/issues/537)
- Amends: [ADR 0017](0017-acp-attachments.md) §3 for ACP sessions only
- Keeps: [ADR 0006](0006-bounded-content-parts.md) bounds and §6

## Context

An ACP session accepts attachments on its first prompt and holds them in
memory. A later prompt cannot add a screenshot, and after a host restart
a content session cannot load. Every model call resolves every accepted
part of the history, so ADR 0006's 32-part and 16 MiB bounds already act
per session.

## Decision

1. **The session host keeps a private copy.** `shikigami acp` writes each
   accepted attachment under its state directory, per session, named by
   digest, owner-only, with atomic replace. Never in the workspace, the
   event log, a checkpoint, or the content sidecar. Load verifies length
   and digest; a missing or changed payload fails closed as today.
2. **Bounds stay ADR 0006's.** The 8 MiB part, 16 MiB aggregate, and
   32-part limits hold over the whole session conversation. A prompt that
   would exceed them fails with `-32602` before admission, and the session
   continues without it.
3. **Capabilities may widen on resume.** A content session may add kinds
   and media types the selected adapter reads. Hard bounds do not move.
4. **Payloads end with the run.** Completing, failing, or replacing the
   session's run deletes its payloads. Host start removes payload
   directories with no live session record.
5. **Text sessions stay text.** An attachment on a session whose run is a
   text run fails with a clear `-32602`. Converting a text run is out of
   scope.

## Consequences

- Resuming with a user message that carries parts needs a new
  `ContentRunRequestV1` input; [#552](https://github.com/Sannrox/shikigami/issues/552)
  makes that additive first.
- The TUI's "omit content sessions after payload loss" filter applies only
  when payloads really are missing.
- The 32-part bound counts text parts, so long content sessions still
  reach it; that is unchanged.

## Rejected alternatives

1. **Copy attachments into the workspace.** ADR 0017 §1 keeps them out.
2. **A per-session bound above 16 MiB.** Every call resolves the whole
   history, so it would exceed ADR 0006's per-call disclosure bound.
3. **Convert text runs into content runs.** Lossy, bounded by 32 parts,
   and a new engine entry point for an unproven need.
4. **Keep payloads in memory only.** Does not meet "kept across a
   restart".
