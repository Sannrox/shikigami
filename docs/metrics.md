# Run metrics export

Durable counters for fleet operators. Each process writes its current snapshot
under $SHIKIGAMI_STATE/metrics/process-<pid>-<instance>.json; CLI and HTTP exports
aggregate those snapshots with `aggregate.json`. **Default builds stay simple** — no Prometheus
client crate; export is JSON and/or Prometheus *text format*.

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
harness instance. The process snapshot is flushed after each counter update.
Clean shutdown folds it into `aggregate.json`; Unix aggregation can also retire
crashed snapshots after checking the process identity marker. An unclean process
exit can still lose an update that was not flushed. On hosts without a portable
process probe, unproven snapshots remain live rather than being counted twice.

## CLI

CLI:

~~~
shikigami metrics --json
shikigami metrics --prometheus
~~~

Filesystem serve --listen exposes GET /metrics on its authenticated control
surface.

## Non-goals

- Full observability platform / tracing backend
- Governed accounting truth; use plane harvest for that
