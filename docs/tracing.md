# Run and tool-call span export

Optional OpenTelemetry export of one run as a trace. **Off by default.**
Spans are a correlation surface, not governance receipts, not a tracing
backend, and not a second source of truth.

The emission boundary carries metadata only (ids, names, counts): policy
decides whether anything leaves the process; a fixed allowlist decides which
attributes may leave; prompts, tool arguments, and tool outputs never become
attributes.

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
unless the path already contains `/v1/traces`. A file endpoint is appended to:
one OTLP JSON document per line, the same shape a collector's file exporter
writes. A file therefore collects every run and attempt that points at it.

Spans are exported as they end, not at run end. A process that dies mid-run
loses only the spans still open; the turns and tool calls it finished are
already at the endpoint. HTTP posts run in the background and the run awaits
them only at its end, bounded by a 5 s request timeout. A failing or
unreachable endpoint logs a warning and never fails the run.

## Span model

One run is one trace. The trace id derives from the run id, so a resume,
takeover, or park and wake in another process writes into the same trace.
Each attempt adds its own `run` span to it.

| Span | Parent | When |
| --- | --- | --- |
| `run` | (root), or the caller's span | Begin after admission; end at the durable run outcome |
| `chat <model>` | `run` | One model turn (`gen_ai.operation.name=chat`) |
| `execute_tool <tool>` | `chat <model>` | One tool call, keyed by the durable call id (`gen_ai.operation.name=execute_tool`) |
| `job <job_id>` | the `execute_tool bash_background` span that started it | A background job, from start until observed exit (or run end if still running) |

Filtering on the literal names `turn` or `tool` no longer matches. Job spans may end after the run span when a job outlives a park.

## Caller trace context

A caller that already has a trace can hand it to the run as a W3C
`traceparent` (with an optional `tracestate`). The run then uses the caller's
trace id, and its `run` span takes the caller's span as parent.

| Source | How | `parent.source` |
| --- | --- | --- |
| Library | `RunRequest.trace_context` | `request` |
| CLI | `TRACEPARENT` / `TRACESTATE` environment | `env` |
| `serve` | `traceparent` / `tracestate` headers on `POST /runs`; a context in the JSON body is ignored | `header` |
| none | No context given | `none` |

Plane intake passes no context: the claim carries none.

The context is stored in the run checkpoint, so a resume or takeover keeps
the original parent. A malformed `traceparent` logs a warning and the run
starts its own trace; it never fails the run. `tracestate` is kept but never
exported as an attribute.

## Outbound trace context

While tracing is on, the run passes the W3C context of the active span to
what it calls, so their own spans join the run's trace:

| Callee | Carrier | Span |
| --- | --- | --- |
| `bash`, `bash_background`, and other sandboxed processes | `TRACEPARENT` / `TRACESTATE` environment | `execute_tool <tool>` |
| MCP servers, stdio and HTTP | `params._meta.traceparent` / `tracestate` on `tools/call` | `execute_tool <tool>` |
| HTTP MCP servers | `traceparent` / `tracestate` headers | `execute_tool <tool>` |
| HTTP model endpoint | `traceparent` / `tracestate` headers | `chat <model>` |
| Plane calls | `traceparent` / `tracestate` request metadata | `chat <model>` for planning, `execute_tool <tool>` for tool authorization |

`tracestate` is the caller's, passed on unchanged; one that is not at most 512 printable ASCII characters is dropped. A stdio MCP server starts
before the run span exists, so it receives the context per call in `_meta`,
not in its environment. With tracing off, nothing is added.

## Attribute allowlist

Correlation attributes use the same names as run identity
([identity.md](identity.md), [ADR 0009](decisions/0009-tool-call-correlation.md)).
Metadata attributes follow the OpenTelemetry generative-AI semantic
conventions where one exists, and use a `shikigami.` prefix where none does:

| Attribute | Meaning |
| --- | --- |
| `run_id` | Harness attempt id |
| `attempt_id` | Same as `run_id` (plane attempt field) |
| `logical_operation_id` | Inbound operation correlation (`RunRequest.logical_operation_id`, else the attempt) |
| `plan_operation_id` | Governance checkpoint operation id when one was recorded; empty otherwise |
| `call_id` | Durable tool-call id `tool-{turn}-{index}[-{model_id}]` (tool spans only) |
| `gen_ai.tool.name` | Tool name, including `mcp.<server>.<tool>` (tool spans) |
| `gen_ai.operation.name` | `chat` on turn spans, `execute_tool` on tool spans |
| `error.type` | Closed-set tool failure: `not_enabled`, `denied`, `invalid_arguments`, `timeout`, `nonzero_exit`, `error` (failed tool spans). Tool output and error text are never exported |
| `gen_ai.request.model` | Model the turn used (turn spans) |
| `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens` | Token counts, when the model reported usage (turn spans) |
| `gen_ai.usage.cache_read.input_tokens` / `gen_ai.usage.cache_creation.input_tokens` | Cached / written prompt-cache tokens, when the model reported them (turn spans). Subsets of `input_tokens`; omitted when the model did not report cache usage |
| `gen_ai.response.finish_reasons` | Model finish reason when reported: `stop`, `length`, `tool_calls`, `content_filter` (turn spans). Unknown provider strings are dropped |
| `shikigami.prompt_id` | Versioned prompt id (run span) |
| `shikigami.preset_id` | Narrowing preset id, when the run has one (run span) |
| `shikigami.termination` | How the run ended: `completed`, `failed`, `parked`, … (run span) |
| `shikigami.job.id` | Background job id (`job-<n>`) (job spans) |
| `process.exit.code` | Process exit code when the job was observed to exit (job spans). Omitted if the job was still running when the run ended |
| `parent.source` | Where the caller trace context came from: `request`, `env`, `header`, or `none` (run span) |

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
