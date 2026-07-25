# Run identity model

How shikigami identifiers map to sekai-chisei plane fields. Authority:
[ADR 0002](decisions/0002-run-identity.md).

## Field map

| Shikigami | Plane / stack field | Default |
| --- | --- | --- |
| `RunResult.run_id` | `attempt_id`, checkpoint dir name | New UUID per attempt |
| `RunHandle.operation_id` | `operation_id`, `logical_operation_id` | Same as `run_id` unless overridden |
| `RunRequest.logical_operation_id` | seeds `operation_id` | Optional; host-supplied |
| resume `resume_run_id` | continues same attempt | Loads checkpoint for that `run_id` |

### Governed path population

| Surface | Fields set |
| --- | --- |
| `PlanExecution` / `ExecutionInput` | `logical_operation_id = operation_id`, `attempt_id = run_id` |
| External-action request | `operation_id`, `attempt_id = run_id` |
| Harvest `shikigami.run.begin` | `run_id`, `attempt_id`, `logical_operation_id`, `operation_id` |
| Harvest tool / complete | events keyed by `operation_id` |
| Receipts | `GetOperationReceipt(operation_id)` |

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
GetOperationReceipt { operation_id: "3fa8…" }
# → receipt_json + event kinds shikigami.run.begin | shikigami.tool | shikigami.run.complete

# 4) Parent-owned operation (embedder)
RunRequest {
  task: "...",
  logical_operation_id: Some(parent_op.to_string()), // plane key
  ..
}
# run_id is still a new attempt UUID; receipts live under parent_op
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

Resume reuses the same `run_id` (attempt). Prefer keeping the original
`logical_operation_id` if the host supplied one on the first attempt so plane
events stay on the same operation.
