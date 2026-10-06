# ADR 0017: ACP prompt attachments, fail closed

- Status: Accepted
- Date: 2026-10-06
- Resolves: [#369](https://github.com/Sannrox/shikigami/issues/369)
- Does not amend: [ADR 0004](0004-v1-contract.md) freeze-core,
  [ADR 0006](0006-bounded-content-parts.md) custody
- Related: [ADR 0014](0014-usable-guest-hosts.md) (content parts reuse
  `run_content` bounds; no new multimodal stack)
- Research: [guest-session-surfaces.md](../research/guest-session-surfaces.md)

## Context

`session/prompt` concatenates `text` parts. `initialize` advertises
`promptCapabilities.image`, `audio`, and `embeddedContext` as false.
Hosts still attach screenshots, PDFs, audio, and video to a prompt.
[ADR 0006](0006-bounded-content-parts.md) already separates durable
descriptors from transient payload and keeps hosts as the payload
custodian. [ADR 0014](0014-usable-guest-hosts.md) forbids a new
multimodal stack and first-party image generation.

The question is input on the ACP host, not generation, and not a plane
upload cap (that cap belongs to rusui).

## Decision

Accept [#369](https://github.com/Sannrox/shikigami/issues/369) option 1.

1. **The prompt carries attachments.** Bytes or host paths travel with
   `session/prompt`. They stay private to the session. They are not
   copied into the workspace or the repository unless the operator or a
   tool writes them there.
2. **Pass through what the selected model can read.** Image, PDF, audio,
   and video parts that the adapter and [ADR 0006](0006-bounded-content-parts.md)
   capabilities allow are resolved for one authorized call and dropped
   after it. Other types fail closed. Do not drop an unsupported part
   silently.
3. **Hosts retain payload custody.** Shikigami does not become a media
   store, transcoder, or disclosure authority. Secrets inside an
   attachment are not copied into a log line by default. Content
   checkpoints store descriptors, not payload bytes.
4. **Workspace drop stays a host action.** The operator may put a file
   on disk and mention it. That is not the ACP attachment contract.
5. **No image generation.** First-party image generation stays out, as
   in ADR 0014.
6. **Capabilities stay honest.** Until a follow-up lands, advertised
   `promptCapabilities` remain false. The follow-up flips only the kinds
   the selected adapter can read, under existing `run_content` bounds.

## Consequences

- A feature Issue implements option 1 on `shikigami acp` and the TUI
  paste/attach path. This ADR does not implement it.
- MCP, serve, and plane intake remain text-only by design (ADR 0006
  later note).
- Plane upload limits stay rusui's.

## Rejected alternatives

1. **Attachments stay outside the ACP host (option 2) as the only
   path.** Workspace mention remains available. It is not a substitute
   for prompt-carried parts a model can read.
2. **Silent drop of unsupported types.** Hides capability mismatch.
3. **Store payload bytes in checkpoints or copy attachments into the
   repo.** Turns harness scratch into a media store.
4. **A new multimodal pipeline or image generation.** Conflicts with
   ADR 0014 and ADR 0006.
