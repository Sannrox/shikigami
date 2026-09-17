# Run and tool-call span export

Optional OpenTelemetry export of one run as a trace. **Off by default.**
Spans are a correlation surface, not governance receipts, not a tracing
backend, and not a second source of truth.

The emission boundary is identity-only: policy decides whether anything
leaves the process; a fixed allowlist decides which attributes may leave;
prompts, tool arguments, and tool outputs never become attributes.

## Settings (`[tracing]`)

| Field | Default | Description |
| --- | --- | --- |
| `enabled` | `false` | Create and export spans |
| `exporter` | `"none"` | `none` \| `otlp` |
| `endpoint` | unset | File path, `file://` path, or OTLP/HTTP collector URL |

`enabled = true` requires `exporter = "otlp"` and a non-empty `endpoint`.
Unknown exporters and a missing endpoint fail settings validation. HTTP
endpoints also pass through `[network]` egress. Disabled settings ignore
`exporter` and `endpoint` and leave offline behavior unchanged.

```toml
[tracing]
enabled = true
exporter = "otlp"
endpoint = "/var/log/shikigami/spans.json"
```

An `http://` or `https://` endpoint POSTs OTLP/HTTP JSON to `{endpoint}/v1/traces`
unless the path already contains `/v1/traces`. File endpoints write one OTLP
JSON document at run end.

## Span model

One attempt is one trace.

| Span | Parent | When |
| --- | --- | --- |
| `run` | (root) | Begin after admission; end at the durable run outcome |
| `turn` | `run` | One model turn |
| `tool` | `turn` | One tool call, keyed by the durable call id |

## Attribute allowlist

Span attributes use the same correlation names as harvest / plane identity
([identity.md](identity.md), [ADR 0009](decisions/0009-tool-call-correlation.md)):

| Attribute | Meaning |
| --- | --- |
| `run_id` | Harness attempt id |
| `attempt_id` | Same as `run_id` (plane attempt field) |
| `logical_operation_id` | Inbound operation correlation (`RunRequest.logical_operation_id`, else the attempt) |
| `plan_operation_id` | Plane decision / host `PlanExecution` id when a governance checkpoint recorded one; empty on ungoverned runs |
| `call_id` | Durable tool-call id `tool-{turn}-{index}[-{model_id}]` (tool spans only) |

Resource attributes are `service.name=shikigami` and `service.version`.
No other keys are exported. A defense-in-depth filter drops unknown keys at
the emission boundary.

Never exported: task text, prompts, `args_json`, tool results, summaries,
credentials, or policy secrets.

## Doctor

`doctor` prints `tracing: disabled` or the effective exporter and endpoint.
This is a diagnostic line, not a `DoctorReport` schema change.

## Non-goals

- Hosting a collector, metrics dashboard, or log shipper
- Treating exported spans as plane receipts
- Operator-configurable attribute lists (that would reopen the payload leak)
