# ADR 0005: Governed run replay

- Status: Accepted
- Date: 2026-08-23
- Resolves: [Issue #231](https://github.com/Sannrox/shikigami/issues/231)
- Discussion: [#235](https://github.com/Sannrox/shikigami/discussions/235)

## Context

Checkpoint resume continues one interrupted attempt, while transcript export is
a bounded, redacted audit projection. Neither artifact can authorize or support
a defensible comparison with a new execution: they do not bind the selected
model, effective tool catalog, policy evidence, composed prompt, inputs, and
expected ordered steps.

Replay must not repeat external effects, silently accept changed evidence, or
turn harness-local state into authoritative governance truth. It must also
preserve the 1.x meanings of `run`, resume, export, serve, and MCP.

## Decision

1. **Replay is a new attempt.** `Harness::replay` admits a versioned manifest,
   mints a new `run_id`, uses a fresh isolated workspace, and executes through
   the existing ports-and-settings turn loop. The source run identity is
   evidence, never the resume identity.
2. **Comparison input is immutable.** A host supplies a bounded, versioned
   evidence bundle. The manifest binds its canonical SHA-256 digest plus the
   source identity, task and inputs, composed prompt, model identity, effective
   tool catalog, policy evidence, and retained source evidence.
3. **Replay is observation-only.** `read_file`, `glob`, and `grep` may execute.
   `report` is the terminal comparison signal. Writes, patches, bash, network
   access, todo mutation, escalation, MCP/external tools, and unknown tools are
   denied before execution.
4. **Governed replay fails closed.** Missing, stale, denied, unavailable, or
   unverifiable governance evidence prevents model or tool execution. Replay
   results and checkpoints remain local comparative evidence, not receipts.
5. **Recovery is content-bound.** A replay checkpoint binds the manifest
   digest, source identity, comparison cursor, and isolated workspace. Restart
   may continue only with the same manifest and must not repeat a staged model
   turn or retry an in-doubt effect. Ordered evidence is not compacted, and a
   terminal replay attempt can only return its reconstructed result without
   invoking the model or tools again.
6. **The API is additive.** Replay has separate request and result types.
   Existing `RunRequest`, `RunResult`, settings v1, transcript v1, and host
   behavior do not change. Serve, MCP, and plane-intake replay are out of scope
   for the first implementation.

Comparisons are ordered and explicitly classify surfaces as `equal`, `changed`,
`missing`, or `unsupported`. Nondeterministic natural-language differences are
evidence, not proof that the replay failed or that governance accepted it.

## Consequences

- Deterministic replay is available through the embeddable `Harness` without a
  second turn loop.
- Hosts must retain or construct the dedicated evidence bundle; transcript and
  checkpoint formats do not become replay authority.
- Adapters that cannot verify required governed evidence report replay as
  unsupported or unavailable rather than selecting an ungoverned path.
- Replay checkpoints contain additional bounded local metadata and remain
  subject to the existing state-directory sensitivity and cleanup rules.

## Rejected alternatives

1. **Evidence-only verification.** This validates retained records but does not
   satisfy the requirement to compare a new execution.
2. **Raw checkpoint as the replay contract.** Checkpoints are mutable harness
   scratch and lack required model, catalog, policy, and composed-prompt
   bindings.
3. **A replay flag on `RunRequest`.** This would blur new-attempt replay with
   same-attempt resume and implicitly expose replay semantics through every
   existing host.
