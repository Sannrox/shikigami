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

## Plane claim mapping (library helper)

Direct plane claim polling is implemented by the follow-up intake work, not by
the filesystem daemon described above. The additive
`map_claimed_work(ClaimedPlaneWork, ClaimedWorkPolicy)` library helper freezes
the boundary between an already-claimed plane effect and `RunRequest`:

- the claimed effect must be `runtime_dispatch` in `claimed` state;
- the top-level effect id must be present; the instance/operation ids duplicated
  in the v1 payload and the Action parameters digest must match;
- the bound plane `operation_id` becomes
  `RunRequest.logical_operation_id`;
- inline `task` text is size-bounded, while `artifact_refs` require an
  authorized host resolver to provide `resolved_task`;
- host timeout is a cap (a plane hint may only narrow it);
- `keep_workspace` remains false unless host policy explicitly permits it; and
- unknown fields are ignored and cannot alter host configuration or grant
  authority.

The helper does not call claim RPCs, admit Action types, resolve artifacts, or
execute the run. Those responsibilities remain with the thin host intake
adapter and the existing `Harness`.

## Health

`queue/health.json` example fields: `ok`, `product`, `version`, `queue_inbox`,
`running`, `last_run_id`.

## Operator notes

- Use the same config/env as `run` / `doctor`.
- For fleets, put the binary under process supervision (systemd, tenkai, etc.).
- HTTP and plane work-unit intake are **not** in v0.x; extend via ADR if needed.
