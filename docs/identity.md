# Run identity model

How shikigami identifiers map to sekai-chisei plane fields. Authority:
[ADR 0002](decisions/0002-run-identity.md).

## Field map

| Shikigami | Plane / stack field | Default |
| --- | --- | --- |
| `RunResult.run_id` | `attempt_id`, checkpoint dir name | New UUID per attempt |
| `RunHandle.operation_id` | `ExecutionInput.logical_operation_id` | Same as `run_id` unless overridden |
| `RunRequest.logical_operation_id` | seeds logical lineage | Optional; host-supplied |
| host `PlanExecution.plan_id` | aggregate receipt `operation_id` | Plane-generated host lifecycle receipt; created once per run |
| model `PlanExecution.plan_id` | per-turn model receipt | Plane-generated receipt executed for each governed model call |
| resume `resume_run_id` | continues same attempt | Loads checkpoint for that `run_id` |

### Governed path population

| Surface | Fields set |
| --- | --- |
| `PlanExecution` / `ExecutionInput` | `logical_operation_id = RunHandle.operation_id`, `attempt_id = run_id` |
| Host receipt | `PlanExecution` records intent, policy, routing, and budget; authenticated events add attempt, model, tool, and outcome facts |
| Model receipt | `PlanExecution` + `ExecutePlanStream` records one governed model call and its terminal outcome |
| External-action request | `operation_id = host PlanExecution.plan_id`, `attempt_id = run_id` |
| Host event links | `model_called.plan_operation_id` points to the model receipt; `action_performed` carries tool result attributes |
| Receipts | `GetOperationReceipt(host_plan_id)`; follow model receipt ids from `model_called` |

Offline adapters do not write plane fields; local handles still set
`operation_id` for in-process consistency (`local-` prefix when no override).

## Work units

Shikigami does **not** create sekai work units. If onmyoji or another host
admits a work unit, pass that correlation id as
`RunRequest.logical_operation_id` (or map your host’s operation id into that
field) so harvest and PlanExecution attach to the same logical operation.

## Example: correlate logs and receipts

```text
# 1) CLI / library emits run id
run_id=3fa8…   # also attempt_id
operation_id=3fa8…   # unless logical_operation_id was set

# 2) Host logs (stderr events / jsonl)
HarnessEvent / doctor: run_id=3fa8…

# 3) Plane
host_plan_id=plane-generated-host-plan-id
GetOperationReceipt { operation_id: host_plan_id }
# → host planning spine + attempt_started + model_called + action_performed*
#   + outcome_recorded
# model_called.plan_operation_id identifies the per-turn model receipt.

# 4) Parent-owned operation (embedder)
RunRequest {
  task: "...",
  logical_operation_id: Some(parent_op.to_string()), // plane key
  ..
}
# run_id is still a new attempt UUID; host and model receipt ids are generated
# by PlanExecution and the logical id remains in their intent lineage.
```

```rust
use shikigami::{Config, Harness, RunRequest, StateRoot};

async fn under_parent_op(parent_op: &str) -> shikigami::RunResult {
    let harness = Harness::from_config(Config::default(), StateRoot::default_in(".")).unwrap();
    let mut req = RunRequest::new("continue workflow");
    req.logical_operation_id = Some(parent_op.into());
    harness.run(req).await.unwrap()
}
```

## Resume

Resume reuses the same `run_id` (attempt). The governance checkpoint preserves
the original logical lineage, host/model receipt ids, and any pending report;
an explicit `RunRequest.logical_operation_id` still takes precedence when a
host intentionally changes the correlation.
