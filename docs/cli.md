# CLI reference

Command and common option summary for the `shikigami` process host.
For the complete options for a command, use `shikigami <command> --help`.

## Invocation

```text
shikigami [--state DIR] [--config FILE] [--model MODEL] [--preset PRESET] <COMMAND>
```

## Commands

| Command | Purpose |
| --- | --- |
| `version [--json]` | Product identity |
| `doctor [--json] [--models]` | Effective profile, adapters, health, and optionally available models |
| `run <task> [--keep-workspace] [--resume ID] [--answer TEXT] [--plan-jail] [--plan-accept\|--plan-reject] [--nested]` | Execute or resume a run; `escalate` parks need an operator answer; plan write-jail parks need accept or reject; `--nested` enables child-run tools |
| `runs [ID] [--diagnose], cancel ID, logs ID, cleanup ID` | Inspect and control durable local run state ([runs.md](runs.md)) |
| `artifacts ID [--patch]` | Export retained artifact metadata or a captured patch |
| `metrics [--json\|--prometheus]` | Export aggregate durable metrics ([metrics.md](metrics.md)) |
| `serve [--poll-ms N] [--max-jobs N]` | Filesystem-queue daemon host with bounded worker/control options ([serve.md](serve.md)) |
| `mcp` | MCP stdio server: `doctor`, `run`, `run_start`/`run_status`/`run_wait` ([mcp.md](mcp.md)) |
| `acp` | ACP session host, newline JSON-RPC ([acp.md](acp.md)). Evolving; not freeze-core |
| `tui` | Chat with an agent that works on the files in this folder ([tui.md](tui.md)). Evolving; not freeze-core |
| `export <run_id> [-o FILE]` | Offline JSONL transcript from checkpoint ([embedding.md](embedding.md)) |
| `replay --manifest FILE --evidence FILE [--resume ID] [--json]` | Observation-only content-bound replay ([replay.md](replay.md)) |
| `replay-export <run_id> [--json] [-o DIR]` | Reconstruct a replay package from retained artifacts, or report missing bindings ([replay.md](replay.md)) |
| `run-content --request FILE --payloads DIR [--json]` | Bounded content run through a versioned process request ([content.md](content.md)) |

Task text is optional with `run --resume`. Mutating-tool parks resume through
the same ask path; ACP uses `session/request_permission` ([acp.md](acp.md)).

## Common options

| Flag / env | Purpose |
| --- | --- |
| `--state` / `SHIKIGAMI_STATE` | State root. Default: `~/.shikigami/projects/<encoded-cwd>`; `SHIKIGAMI_HOME` replaces `~/.shikigami`. See [settings.md](settings.md#host-state-root). Project-local scratch: `--state ./.shikigami-state`. |
| `--config` / `SHIKIGAMI_CONFIG` | Settings file path |
| `--model` / `SHIKIGAMI_MODEL` | Final model override; HTTP `auto` maps to `gpt-4.1-mini` |
| `--preset` / `SHIKIGAMI_PRESET` | Narrow tools and add a procedure: a built-in name or a preset `.toml` file; `code` means none. See [presets.md](presets.md) |
| `run --keep-workspace` | Keep the workspace after a successful run |
| `run --json` | Print the run result as JSON on stdout (same shape as the MCP `run` result, including report `data`); exit status unchanged |
| `run --plan-jail` / `SHIKIGAMI_RUN_PLAN_JAIL` | Restrict mutating tools to `.shikigami/plan.md` until `--plan-accept` |
| `run --plan-accept` / `--plan-reject` | Resume a parked plan: Accept restores execute authority on the same run; Reject completes failed and keeps the jail so a later `--resume` stays write-jailed |
| `TRACEPARENT` / `TRACESTATE` | W3C trace context: with span export on, `run` continues the caller's trace. See [tracing.md](tracing.md#caller-trace-context) |
| `run --nested` / `SHIKIGAMI_RUN_NESTED` | Enable `child_run` / `child_status` for this run (default off) |

There is **no** `init` command. Config is optional; disk state is created when a
run needs it.


Human-readable CLI output uses the TUI's terminal-text policy: control characters
become U+FFFD, except LF and tab. Model summaries, parked questions, recorded
fields, and errors are displayed without executing terminal escape sequences.
JSON, journals, transcript exports, and artifact exports retain their serialized
data; file output is unchanged.
