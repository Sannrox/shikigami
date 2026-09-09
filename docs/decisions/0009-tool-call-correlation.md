# ADR 0009: Stable tool-call correlation in progress events

- Status: Accepted
- Date: 2026-09-09
- Resolves: [Issue #250](https://github.com/Sannrox/shikigami/issues/250)
- Discussion: [#257](https://github.com/Sannrox/shikigami/discussions/257)
- Related: [ADR 0004](0004-v1-contract.md)

## Context

Durable tool execution already has a stable call identity
(`tool-{turn}-{index}[-{model_id}]`) for authorization, staged executions, and
report replay. Live `ToolStart` / `ToolEnd` events and the local event journal
only carried the tool name, so two same-named calls in one turn could not be
correlated after restart.

ADR 0004 freezes `HarnessEvent` as an additive internally tagged enum. A new
envelope or a second pair of event variants would break that 1.x promise.

## Decision

1. **Additive fields.** `HarnessEvent::ToolStart` and `ToolEnd` gain `run_id`,
   `turn`, and `call_id`. The JSON `type` discriminator is unchanged. Missing
   fields deserialize as empty/`0`. Unknown `type` values fail serde
   deserialize.
2. **One identity.** `call_id` is the existing durable `stable_tool_call_id`,
   not the model-provided conversation id. The same string is used for
   authorize, staged execution, staged reports, live events, the local journal,
   and transcript `tool_calls`. Transcript export numbers remaining assistant
   messages from `completed_turns` so compacted tails keep the original turn.
3. **Metadata only.** Correlation fields are identifiers. Arguments,
   credentials, and policy secrets are not added to metadata-only progress.
   `ToolStart.args_json` remains the existing live payload; the journal still
   omits it.
4. **No envelope.** stderr/jsonl progress stays a tagged `HarnessEvent` object.
   `RunEventRecord` and transcript lines stay schema v1 with additive optional
   `call_id`. Events remain best-effort UI, not plane receipts.

## Consequences

- Embedders can match start/end/report/recovery observations for repeated
  tools without guessing from names.
- Matches that ignore extra fields with `..` stay valid. Exhaustive matches
  must name or discard the new fields.

## Rejected alternatives

- Versioned envelope around every event: breaks existing JSONL
  `{ "type": "tool_start", ... }`.
- `ToolStartV2` / `ToolEndV2`: duplicates the additive-enum contract.
- Correlating only on tool name or raw model `tool_call_id`: collides when
  the model repeats or omits ids.
