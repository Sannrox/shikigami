# Run identity model

How shikigami identifiers name a local attempt. Authority:
[ADR 0002](decisions/0002-run-identity.md), amended by
[ADR 0024](decisions/0024-local-guest-rusui-plane-caller.md).

When a plane is used, rusui maps these ids onto plane fields. This crate does
not call PlanExecution or harvest.

## Field map

| Shikigami | Role | Default |
| --- | --- | --- |
| `RunResult.run_id` | Attempt id, checkpoint dir name | New UUID per attempt |
| `RunHandle.operation_id` | Logical lineage id | Same as `run_id` unless overridden |
| `RunRequest.logical_operation_id` | Seeds logical lineage | Optional; host-supplied |
| resume `resume_run_id` | Continues same attempt | Loads checkpoint for that `run_id` |
| replay `source_run_id` | Retained comparison evidence | Never reused as the replay attempt id |
| `ReplayResult.run.run_id` | Replay attempt id | New UUID, or the same replay id when restarting that attempt |
| `HarnessEvent` `call_id` | Durable tool-call identity | `tool-{turn}-{index}[-{model_id}]` |
| OTLP span attributes | Local correlation | `run_id`, `attempt_id`, `logical_operation_id`, tool `call_id` — see [tracing.md](tracing.md) |

Offline adapters do not write plane fields; local handles still set
`operation_id` for in-process consistency (`local-` prefix when no override).

## Work units

Shikigami does **not** create plane work units. If a caller already owns a
logical operation, pass that correlation id as
`RunRequest.logical_operation_id`.
