# CLI reference

Command and common option summary for the `shikigami` process host.
For the complete options for a command, use `shikigami <command> --help`.

## Invocation

```text
shikigami [--state DIR] [--config FILE] [--model MODEL] <COMMAND>
```

## Commands

| Command | Purpose |
| --- | --- |
| `version [--json]` | Product identity |
| `doctor [--json] [--models]` | Effective profile, adapters, health, and optionally available models |
| `run <task> [--keep-workspace] [--resume ID] [--answer TEXT]` | Execute or resume a run; `escalate` parks need an operator answer |
| `runs [ID] [--diagnose], cancel ID, logs ID, cleanup ID` | Inspect and control durable local run state ([runs.md](runs.md)) |
| `artifacts ID [--patch]` | Export retained artifact metadata or a captured patch |
| `metrics [--json\|--prometheus]` | Export aggregate durable metrics ([metrics.md](metrics.md)) |
| `eval FIXTURE [--json]` | Run offline scripted golden fixtures ([eval.md](eval.md)) |
| `serve [--intake filesystem\|plane] [--poll-ms N] [--max-jobs N]` | Filesystem-queue or plane-claim daemon host; filesystem supports bounded worker/control options ([serve.md](serve.md)) |
| `mcp` | MCP stdio server: `doctor`, `run`, `run_start`/`run_status`/`run_wait` ([mcp.md](mcp.md)) |
| `acp` | ACP session host, newline JSON-RPC ([acp.md](acp.md)). Evolving; not freeze-core |
| `tui` | Interactive terminal host; ACP client of the in-process session ([tui.md](tui.md)). Evolving; not freeze-core |
| `export <run_id> [-o FILE]` | Offline JSONL transcript from checkpoint ([embedding.md](embedding.md)) |
| `replay --manifest FILE --evidence FILE [--resume ID] [--json]` | Observation-only content-bound replay ([replay.md](replay.md)) |
| `replay-export <run_id> [--json] [-o DIR]` | Reconstruct a replay package from retained artifacts, or report missing bindings ([replay.md](replay.md)) |
| `run-content --request FILE --payloads DIR [--json]` | Bounded content run through a versioned process request ([content.md](content.md)) |

Task text is optional with `run --resume`. Approval parks re-validate plane
authority on resume; see the [governed guide](governed-path.md#run-requires-plane--model-providers).

## Common options

| Flag / env | Purpose |
| --- | --- |
| `--state` / `SHIKIGAMI_STATE` | State root (default: `./.shikigami-state`) |
| `--config` / `SHIKIGAMI_CONFIG` | Settings file path |
| `--model` / `SHIKIGAMI_MODEL` | Final model override; `auto` delegates routing to sekai-chisei |
| `run --keep-workspace` | Keep the workspace after a successful run |

There is **no** `init` command. Config is optional; disk state is created when a
run needs it.

