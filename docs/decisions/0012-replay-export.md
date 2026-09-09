# ADR 0012: Export validated replay inputs from a retained run

- Status: Accepted
- Date: 2026-09-09
- Resolves: [Issue #253](https://github.com/Sannrox/shikigami/issues/253)
- Discussion: [#263](https://github.com/Sannrox/shikigami/discussions/263)
- Depends on: [ADR 0005](0005-governed-run-replay.md), [ADR 0010](0010-cli-run-replay.md)

## Context

Replay consumers currently assemble schema-v1 manifest and evidence bindings
themselves. Checkpoints, transcripts, and the live workspace are retained
scratch: they do not prove original inputs, prompt body, or model identity.
Guessing a missing digest would make later `replay` look comparable when it is
not.

## Decision

1. **Read-only export.** Additive `shikigami replay-export <run_id> [--json]
   [-o DIR]` and `Harness::export_replay_inputs` reconstruct a schema-v1
   package from retained artifacts, or return a typed incomplete
   `ReplayExportReport`. Export does not execute replay and does not mutate
   run state.
2. **Original inputs are `snapshots/initial` only.** The current workspace and
   transcript export are never original-input proof. The first captured
   `initial` snapshot is preserved across resume; resume never creates or
   recaptures it from a mutated workspace. Content runs, compacted history, non-terminal runs, and
   unmatched prompt ids are incomplete. The prompt digest is current
   `SYSTEM_PROMPT` composed with rules and skills from `snapshots/initial`.
   Skill packs that are not retained inside that snapshot make prompt
   incomplete. Model, policy, and the observation-only replay catalog come from the
   exporting process configuration, the same settings later `replay` will use.
3. **Validate or explain.** A complete package is admitted with
   `ReplayManifest::for_evidence` / `ReplayRequest::admit` before it is
   returned. Incomplete results list the missing binding names and omit
   `manifest` / `evidence`. Digests are never invented. `-o DIR` writes
   `manifest.json` and `evidence.json` only when complete.
4. **Local data, digest-only.** The package stays digest-only. Checkpoints
   remain local scratch, not receipts. There is no network upload.

Exit `0` is a well-formed report (complete or incomplete). Exit `1` means the
run cannot be inspected.

## Consequences

- Operators can produce a replayable package from a retained run that captured
  `workspace.snapshot`, or learn exactly which binding is absent.
- Default `workspace.snapshot = false` yields `missing: ["inputs"]` rather
  than a guessed empty-tree digest.
- Hosts still cannot treat checkpoints or transcripts as comparison authority.

## Rejected alternatives

- Using the current workspace as original inputs.
- Filling prompt, model, or input digests from current settings when the
  retained identity does not match.
- Treating transcript export as replay authority.
- Network upload or including unrestricted payloads.
