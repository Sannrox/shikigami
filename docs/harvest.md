# Run harvest → plane records

When governance is `sekai-chisei`, a run **harvests** lifecycle facts into the
control plane so postmortems do not depend on local harness state.

Local checkpoints under `.shikigami-state/` remain **non-authoritative** for
governed truth. Offline adapters (`none`, `local`) never write to the plane.

## Object / event model

| Harness concept | Plane surface | Notes |
| --- | --- | --- |
| `run_id` | `attempt_id` | Harness attempt UUID; resume key |
| `RunHandle.operation_id` | `ExecutionInput.logical_operation_id` | Host lineage; defaults to `run_id` and may be overridden |
| Host `PlanExecution.plan_id` | aggregate host receipt `operation_id` | Allocates the run planning spine; the host plan is not sent to `ExecutePlanStream` |
| Model `PlanExecution.plan_id` | per-turn model receipt | Each governed model call has its own executed plan and terminal receipt |
| Run start | host `PlanExecution` plus `ReportOperationEvent(attempt_started)` | The authenticated attempt event follows the host receipt's budget parent |
| Tool authorization / execution | External-action decision, signed permit redemption, and `action_performed` | The host must redeem a permitted action before executing it; local checkpoints remain non-authoritative |
| Run finish | `GetOperationReceipt` plus authenticated `ReportOperationEvent(outcome_recorded)` | The host receipt is completed after the model and host-tool facts are linked |
| Inspect | `GetOperationReceipt(host_plan_id)` | Reconstructs the host lifecycle; `model_called` attributes point to per-turn model receipts |

The 1.0 contract authorizes reporting from the authenticated principal and its
namespace write authority. Shikigami sends `x-principal`, the configured
bearer token, and `x-sekai-auth-source`; permission failures are returned as
actionable governance errors. There is no separate reporter-preflight RPC.

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

Both successful and failed runs call `complete_run`. Normal governed model
execution records the terminal `outcome_recorded` event in its own
`ExecutePlanStream` receipt. Shikigami also reports that model receipt as a
`model_called` event on the aggregate host receipt, reports each host tool as
`action_performed`, and closes the host receipt with its own
`outcome_recorded` event. Fail-closed profiles treat plane unavailability,
permission failures, and incomplete receipts as errors; offline adapters never
call the plane.

The causal host sequence is:

```text
host PlanExecution budget
  → attempt_started
  → model_called (model PlanExecution.plan_id)
  → action_performed (zero or more)
  → outcome_recorded
```

If reporting fails after the host has executed a tool, the checkpoint retains
the host/model receipt ids, causal event id, and exact pending event payload.
Resume retries that same event id, so a transport failure cannot replay a host
side effect as a new governance event.

## Correlating a run

1. Note `run_id` from CLI / events / `RunResult`.
2. Obtain the host `PlanExecution.plan_id` from the governed host/plane trace.
3. Query `GetOperationReceipt` with that host receipt operation id.
4. Inspect the causal attempt, model, action, and outcome events. Follow each
   `model_called.plan_operation_id` to the corresponding per-turn model
   receipt when model-level planning or usage detail is needed.

See also [governed-path.md](governed-path.md) for smoke and tool authorization.

## Offline path

`none` and `local` governance implement `complete_run` / `report_tool` as
no-ops. Unit tests cover attribute mapping without a plane.
