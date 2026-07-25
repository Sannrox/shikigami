# Run metrics export

Process-local counters for fleet operators. **Default builds stay simple** —
no Prometheus client crate; export is JSON and/or Prometheus *text format*.

## Names

| Metric | Type | Meaning |
| --- | --- | --- |
| `shikigami_runs_total` | counter | Runs attempted |
| `shikigami_runs_success_total` | counter | Successful completions |
| `shikigami_runs_failed_total` | counter | Failed / cancelled / timed out |
| `shikigami_runs_parked_total` | counter | Parked (`escalate`) terminations |
| `shikigami_turns_total` | counter | Model turns completed |
| `shikigami_plane_errors_total` | counter | Governance/plane errors observed |
| `shikigami_tokens_input_total` | counter | Input tokens when reported |
| `shikigami_tokens_output_total` | counter | Output tokens when reported |

`RunResult.usage` carries per-run totals (`input_tokens` / `output_tokens`). Zero means unknown, not free.

## API

```rust
use shikigami::{Harness, RunRequest};

let harness = Harness::from_config(config, state)?;
let _ = harness.run(RunRequest::new("demo")).await?;
let snap = harness.metrics.snapshot();
println!("{}", serde_json::to_string_pretty(&snap)?);
println!("{}", snap.to_prometheus());
```

`Harness.metrics` is an `Arc<Metrics>` shared for the process lifetime of that
harness instance (suitable for `serve` fleets).

## CLI

No separate scrape port in v0.x. Operators can:

- scrape JSON from a host wrapper that calls `metrics.snapshot()`, or
- write Prometheus text via `to_prometheus()` on a timer.

## Non-goals

- Full observability platform / tracing backend
- Guaranteed exact counts across process crash (use plane harvest for governed truth)
