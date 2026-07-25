# `shikigami serve` (local queue daemon)

Long-running host over the same `Harness` library as one-shot `run`.
Authority: [ADR 0003](decisions/0003-serve-daemon.md).

## What it is / is not

| Is | Is not |
| --- | --- |
| Process that polls a local job queue | A control plane or multi-tenant SaaS |
| Same settings / governance adapters as CLI | Replacement for sekai-chisei |
| Offline-testable | Required for one-shot `run` |

## Start

```bash
shikigami --config examples/local-run.toml serve
# optional:
#   --poll-ms 200
#   --max-jobs 1   # process N jobs then exit (useful in tests)
```

Graceful stop: **Ctrl-C** / SIGINT sets shutdown and exits after the current
poll cycle.

## Queue layout

Under `$SHIKIGAMI_STATE` (default `./.shikigami-state`):

```text
queue/
  inbox/          # drop *.json jobs here
  processing/     # claimed by the daemon
  done/           # successful jobs + *.result.json
  failed/         # failures / parked + *.result.json or *.error.txt
  health.json     # process health snapshot
```

## Job file (`inbox/*.json`)

```json
{
  "task": "write the demo marker",
  "keep_workspace": true,
  "logical_operation_id": null,
  "timeout_secs": 120
}
```

## Health

`queue/health.json` example fields: `ok`, `product`, `version`, `queue_inbox`,
`running`, `last_run_id`.

## Operator notes

- Use the same config/env as `run` / `doctor`.
- For fleets, put the binary under process supervision (systemd, tenkai, etc.).
- HTTP and plane work-unit intake are **not** in v0.x; extend via ADR if needed.
