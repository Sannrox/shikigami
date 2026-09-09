# ADR 0010: CLI observation-only run replay

- Status: Accepted
- Date: 2026-09-09
- Resolves: [Issue #251](https://github.com/Sannrox/shikigami/issues/251)
- Discussion: [#259](https://github.com/Sannrox/shikigami/discussions/259)
- Depends on: [ADR 0005](0005-governed-run-replay.md)

## Context

`Harness::replay` already admits a versioned manifest and evidence bundle and
compares a new isolated attempt. Process-only hosts have no supported command
for that API. Adding a `run --replay` flag would blur new-attempt replay with
same-attempt resume. Reconstructing evidence from checkpoints or transcripts
would treat local scratch as comparison authority.

## Decision

1. **Thin host.** Additive `shikigami replay --manifest FILE --evidence FILE`
   reads bounded schema-v1 JSON and calls `Harness::replay`. There is no second
   replay engine.
2. **Same comparisons.** `--json` prints `ReplayReport` schema v1 whose `steps`
   and `terminal` fields are the library `ReplayResult` comparisons. Human
   output is a summary, not a second contract.
3. **Exit status separates outcomes.** Exit `0` means a comparison completed,
   including `changed` / `missing` / `unsupported`. Exit `1` means admission or
   execution failed (unsupported version, digest mismatch, denied effect-capable
   tool, unverifiable required governance, or run failure).
4. **Authority is unchanged.** Observation-only tools, isolated workspaces, and
   fail-closed governed replay remain library admission rules. `--resume` is a
   replay-attempt id. Serve, MCP, and plane-intake replay stay out of scope.

## Consequences

- Operators can invoke replay without embedding the library.
- Hosts still supply the dedicated evidence bundle; export and resume do not
  become replay inputs.

## Rejected alternatives

- `run --replay`: rejected by ADR 0005.
- Inferring evidence from a retained checkpoint or transcript.
- Treating changed comparison as process failure (would collide with
  admission/execution errors).
