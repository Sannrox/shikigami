# Run harvest → plane records

When governance is `sekai-chisei`, a run **harvests** lifecycle facts into the
control plane so postmortems do not depend on local harness state.

Local checkpoints under `.shikigami-state/` remain **non-authoritative** for
governed truth. Offline adapters (`none`, `local`) never write to the plane.

## Object / event model

| Harness concept | Plane surface | Notes |
| --- | --- | --- |
| `run_id` | `attempt_id` | Harness attempt UUID; resume key |
| `operation_id` / `logical_operation_id` | plane `operation_id` | Defaults to `run_id`; override via `RunRequest.logical_operation_id` |
| Run start | `ReportOperationEvent` kind `shikigami.run.begin` | Includes task, principal, namespace |
| Tool attempt | `ReportOperationEvent` kind `shikigami.tool` | `ok`, `tool`, truncated `detail` (allow + deny) |
| Run finish | `ReportOperationEvent` kind `shikigami.run.complete` | success, summary, turns, termination, workspace path ref |
| Inspect | `GetOperationReceipt` | Reconstruct plane-visible history for `operation_id` |

Reporter authorization is requested at begin via
`AuthorizeOperationReporter` for the event kinds above.

## Event attributes (complete)

| Attribute | Meaning |
| --- | --- |
| `success` | bool string |
| `summary` | truncated final summary / error |
| `turns` | completed model turns |
| `termination` | `completed` \| `cancelled` \| `timed_out` \| `max_turns` \| `failed` |
| `workspace` | host path (reference only; not plane storage) |
| `authoritative` | always `plane` on governed harvest |
| `harness` | `shikigami` |

Evidence references on complete include `run_id` and optional `workspace_path`.

## Success and failure

Both successful and failed runs call `complete_run` and emit
`shikigami.run.complete` (when the plane is reachable). Fail-closed profiles
treat plane unavailability as an error at start; mid-run harvest best-efforts
where noted in the adapter.

## Correlating a run

1. Note `run_id` from CLI / events / `RunResult`.
2. On the plane, query `GetOperationReceipt` with `operation_id = run_id`.
3. Filter operation events by kinds `shikigami.*`.

See also [governed-path.md](governed-path.md) for smoke and tool authorization.

## Offline path

`none` and `local` governance implement `complete_run` / `report_tool` as
no-ops. Unit tests cover attribute mapping without a plane.
