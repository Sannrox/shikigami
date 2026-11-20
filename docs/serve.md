# `shikigami serve` (filesystem queue)

Current long-running host over the same `Harness` library as one-shot `run`.
Authority: [ADR 0003](decisions/0003-serve-daemon.md).

Plane-claim intake (`--intake plane`, agent-turn) was removed. Rusui is the
plane caller ([ADR 0024](decisions/0024-local-guest-rusui-plane-caller.md)).
`--intake` is an unexpected argument.

## What it is / is not

| Is | Is not |
| --- | --- |
| Process that polls a local job queue | A control plane or multi-tenant SaaS |
| Same settings / governance adapters as CLI | A plane claim worker |
| Offline-testable | Required for one-shot `run` |

## Start

```bash
shikigami --config examples/local-run.toml serve
# optional:
#   --poll-ms 200
#   --max-jobs 1   # process N jobs then exit (useful in tests)
#   --concurrency 4 --queue-capacity 256 --retry-limit 1
#   --listen 127.0.0.1:8080 --auth-token-env SHIKIGAMI_SERVE_TOKEN
```

Graceful stop: **Ctrl-C** / SIGINT, and **SIGTERM** on Unix, stop new
filesystem claims and drain already-claimed jobs before exit.

## Queue layout

Under `$SHIKIGAMI_STATE` (default: `~/.shikigami/projects/<encoded-cwd>`):

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

HTTP / control admission writes `p{sortkey}-{uuid}.json` so the daemon can
claim the highest priority file without parsing every inbox JSON. Dropped
files that do not use that name still claim (priority `0`, then filename
order). The claimed job is parsed once and passed into the run; idle polls
stat the inbox mtime instead of rereading every file. `health.json` is compact
and rewritten only when the snapshot changes.

Filesystem workers support bounded priority ordering, concurrency, local
backpressure, and an explicit retry limit. These are host-local queue
semantics. See [runs.md](runs.md) for the authenticated HTTP control/intake
routes.

Queue capacity counts both inbox and processing jobs, and the control-plane
capacity must match the runtime capacity. The `inplace` workspace adapter uses
the configured root directly, so filesystem serve rejects concurrency above
one for that adapter; use isolated `directory` or `git-worktree` workspaces for
parallel jobs.

## Health

`queue/health.json` fields: `ok`, `product`, `version`, `queue_inbox`,
`running`, `running_jobs`, `queue_capacity`, `queue_over_capacity`,
`last_run_id`.

Run `shikigami doctor` with the same config before starting the process.

## Operator notes

- Use the same config/env as `run` / `doctor`.
- For fleets, put the binary under process supervision (systemd, tenkai, etc.)
  and drain with SIGTERM.
- Filesystem intake admits work through authenticated `POST /runs` (see
  [runs.md](runs.md)). Additional intake transports still require a separate
  contract and must preserve the same `Harness` boundaries.
