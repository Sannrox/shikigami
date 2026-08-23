# ADR 0006: Bounded multimodal content

- Status: Accepted
- Date: 2026-08-23
- Resolves design for: [Issue #232](https://github.com/Sannrox/shikigami/issues/232)
- Discussion: [#237](https://github.com/Sannrox/shikigami/discussions/237)
- Upstream dependency: [sekai-chisei #717](https://github.com/Sannrox/sekai-chisei/issues/717)

## Context

Shikigami's run boundary represents each task, message, model turn, and tool
result as one text string plus tool calls. That contract preserves text-only
checkpoint resume and transcript export, but it cannot retain the ordered
identity, media type, digest, provenance, or disclosure state of image, audio,
and document parts.

Encoding media in the existing string would hide capability mismatches and
could persist unrestricted payloads in checkpoints, events, and transcripts.
Changing the frozen 1.x request, message, or transcript shapes would break
embedders and existing resume state. The current sekai-proto execution RPC is
also text-only, so adding fields to that message could let an older server
silently discard required content.

## Decision

1. **The canonical contract lands upstream first.** `sekai-proto` and
   `sekai-client` define versioned content descriptors, resolved call payloads,
   capabilities, and a content-specific planning/execution RPC. Existing text
   RPCs remain unchanged; a server without the content RPC fails explicitly
   with `UNIMPLEMENTED`.
2. **Durable identity is separate from transient payload.**
   `ContentPartDescriptorV1` carries a stable part id, kind, normalized media
   type, byte length, SHA-256 digest, credential-free opaque reference,
   provenance, and disclosure state. `ResolvedContentPartV1` adds bounded text
   or bytes only for one authorized call and is never serializable as harness
   state.
3. **Hosts retain payload custody.** A host-supplied resolver loads an opaque
   reference. Shikigami validates descriptors, obtains governance and selected
   adapter capability approval, resolves only authorized parts, verifies length
   and digest, and drops bytes after the call. Shikigami does not become a
   media store, transcoder, or disclosure authority.
4. **Capabilities are explicit and default-deny.** The canonical capability
   type binds a contract version, supported kinds and media types, input/output
   support, reference modes, and per-part and aggregate limits. Unknown,
   malformed, oversized, unsupported, denied, or unverifiable content fails
   before provider disclosure or model execution.
5. **Shikigami's API is additive.** Separate content request, message, model
   turn, tool output, and `Harness::run_content` surfaces reuse the existing run
   lifecycle. Frozen `RunRequest`, `ChatMessage`, `ModelTurn`, `Harness::run`,
   settings v1 behavior, and transcript v1 remain unchanged. Governed content
   never falls back to a local model path.
6. **Local persistence contains metadata only.** A versioned content checkpoint
   sidecar is authoritative for content runs and stores descriptors, order,
   disclosure state, and resolver bindings, never payload bytes. Ordinary
   resume rejects content checkpoints; content resume revalidates every
   reference and digest. Content runs do not compact ordered messages in the
   first version.
7. **Projections are separate and bounded.** Content transcripts and events
   expose only bounded, redacted metadata. The text transcript remains schema
   v1. Replay schema v1 rejects content runs until a separately versioned
   content-replay contract exists.

Content is intended for bounded headless excerpts rather than bulk media. Hard
limits cannot be disabled; versioned settings may lower them. Exact initial
limits are fixed with the upstream protocol against supported provider
constraints.

## Consequences

- Issue #232 remains blocked until the upstream content RPC and typed facade
  land; Shikigami cannot claim governed multimodal support before that
  dependency is available.
- Library embedding is the first Shikigami intake surface. CLI, MCP, serve, and
  plane intake may add content only through the same bounded contract and must
  reject unknown fields.
- Deterministic scripted fixtures can prove ordering, digest validation,
  capability denial, persistence, restart, redaction, and text compatibility
  without a live provider.
- Provider adapters may support different media subsets, but the core type and
  failure semantics remain provider-neutral.

## Rejected alternatives

1. **Encode media in text content.** This silently coerces media, hides adapter
   capabilities, and risks unrestricted persistence.
2. **Ship local-only multimodal support first.** This creates a second de facto
   contract before the governance authority can validate or transport it.
3. **Replace the 1.x text types.** This breaks frozen embed, resume, and
   transcript contracts.
4. **Store payload bytes in checkpoints.** This turns harness scratch into a
   sensitive media store and makes retention and cleanup unsafe.
