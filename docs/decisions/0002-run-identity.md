# ADR 0002: Run identity and plane operation lineage

- Status: Accepted
- Date: 2026-07-25
- Supersedes: —

## Context

Shikigami issues a `run_id` per harness attempt. sekai-chisei correlates work
with `logical_operation_id`, `attempt_id`, operation receipts, and (elsewhere
in the stack) work units. Without a single mapping, logs, harvest events, and
plane receipts cannot be joined across shikigami and peer hosts (e.g. onmyoji).

## Decision

1. **`run_id` is the attempt id.** Generated as a UUID by the harness (or
   restored on resume). It is the stable key for local checkpoints under
   `.shikigami-state/runs/<run_id>/`.
2. **`operation_id` is the logical operation id.** It is the key for plane
   `ReportOperationEvent`, `GetOperationReceipt`, and
   `ExecutionInput.logical_operation_id`. Default: equal to `run_id` when the
   caller does not supply a parent logical operation.
3. **`attempt_id` equals `run_id`.** Populated on PlanExecution and harvest
   attributes so plane field names match harness terms without inventing a
   second UUID.
4. **Embedders may set `RunRequest.logical_operation_id`.** When a host already
   owns a logical operation (or work-unit correlation id that should surface as
   the plane operation), pass it; the harness still mints a distinct `run_id`
   for the attempt unless resuming.
5. **Work units are not created by shikigami in v0.x.** Peers that use
   sekai work-unit APIs should treat `logical_operation_id` / `run_id` as the
   correlation keys documented here; shikigami does not call CreateWorkUnit.

## Consequences

- Default offline and CLI runs keep a one-line identity (`run_id ==
  operation_id == attempt_id`).
- Hosted / onmyoji embeddings can join multi-step workflows by sharing
  `logical_operation_id` across attempts.
- Changing plane core id generation remains out of scope (non-goal of this ADR).

## Correlation example

See [identity.md](../identity.md).
