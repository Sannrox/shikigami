# ADR 0008: Read-only recovery diagnosis

- Status: Accepted
- Date: 2026-09-09
- Resolves: [Issue #249](https://github.com/Sannrox/shikigami/issues/249)
- Discussion: [#255](https://github.com/Sannrox/shikigami/discussions/255)
- Depends on: [Issue #248](https://github.com/Sannrox/shikigami/issues/248)

## Context

Operators can list and inspect retained run records, but that view does not
explain whether a checkpoint is safe to resume, needs report-only
reconciliation, is in-doubt after a host-side effect, is invalid, or is already
terminal. Guessing from `run.json` status can redispatch an uncertain tool or
treat local scratch as a governed receipt.

Doctor diagnoses configuration. Recovery diagnosis must inspect one retained
run without executing.

## Decision

1. **Additive typed result.** `RecoveryDiagnosis` is schema v1 with
   `run_id`, `class`, `reason`, and `allowed_next_steps`. Breaking field
   changes bump `schema_version`.
2. **Diagnosis is not authority.** `Harness::diagnose_run` and CLI inspection
   perform no model call, tool dispatch, permit redemption, or state mutation.
   Next-step categories (`resume`, `resume_with_answer`, `reconcile_reports`,
   `inspect`, `do_not_execute`) are labels, not permits.
3. **Classes are exhaustive for v1.** First matching local evidence wins:
   unreadable/unsupported checkpoint, `uncertain_tool`, pending reports
   (`report_only`, after resume-prerequisite checks), already-terminal
   registry or finalized replay/content markers, unfinalized replay
   finalization, then other resume-prerequisite failures, then `safe_resume`.
   Resume prerequisites reuse the same workspace-adapter and canonical-path
   checks as actual resume, plus prompt identity. Unfinalized replay
   finalization validates the workspace when it exists, and otherwise
   requires a retained artifact directory. Parked-run reasons stay
   generic; they do not copy free-form park payloads. A failed registry
   record that still has staged reports is `report_only`. Successful runs
   may delete their workspace; that is terminal, not invalid. Unknown
   checkpoint versions fail as `invalid_checkpoint` rather than defaulting
   to resume.
4. **Same object on both hosts.** CLI `--diagnose --json` prints the library
   result. Existing `shikigami runs <id> --json` `RunRecord` JSON stays
   unchanged. Doctor remains configuration diagnosis.
5. **Local scratch stays non-authoritative.** Diagnosis never claims a plane
   receipt. Reasons omit credentials, payloads, and unrestricted logs.

## Consequences

- Embedders can inspect recovery before calling `run` / `run_content` /
  `replay`.
- In-doubt started/completed tool markers stay non-dispatchable; staged
  reports remain replayable under the original identity.
- Parked runs diagnose as `safe_resume` with `resume_with_answer`.

## Rejected alternatives

- Fold recovery into `DoctorReport`: doctor is process configuration, not
  per-run checkpoint evidence.
- Returning only a boolean `resumable`: hides report-only vs in-doubt vs
  invalid, which require different operator next steps.
- Mutating or "repairing" checkpoints during inspect: would grant execution
  authority from an observation API.
