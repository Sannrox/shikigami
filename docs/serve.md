# `shikigami serve` (filesystem queue or plane claim host)

Long-running host over the same `Harness` library as one-shot `run`.
Authority: [ADR 0003](decisions/0003-serve-daemon.md).

## What it is / is not

| Is | Is not |
| --- | --- |
| Process that polls a local job queue or claims admitted plane work | A control plane or multi-tenant SaaS |
| Same settings / governance adapters as CLI | Replacement for sekai-chisei |
| Offline-testable | Required for one-shot `run` |

## Start

```bash
shikigami --config examples/local-run.toml serve
# optional:
#   --intake filesystem   # default
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

## Plane claim intake

Direct claim intake is explicit; filesystem intake remains the default:

```bash
shikigami --config shikigami.toml serve \
  --intake plane \
  --runtime-id shikigami \
  --claim-ttl-secs 60
```

Requirements:

- build with the default `governance-sekai-chisei` feature;
- set `governance.adapter = "sekai-chisei"`, endpoint, namespace, principal,
  and optional `token_env`;
- use a principal authorized for team-namespace write on the claim namespace;
  and
- keep `runtime_id` aligned with the admitted `runtime_dispatch` payload.

The host lists claimable work, acquires a fenced claim, fetches the parent
ActionInstance parameters, maps them to `RunRequest`, executes the existing
`Harness`, heartbeats while the run is active, and acknowledges `completed`,
`failed`, or `parked`. Governed planning and harvest still use the configured
sekai-chisei governance adapter and the ActionInstance-bound operation id.

Plane intake never admits Action types or instances and never interprets task
text as host configuration or mutation authority. The plane never spawns the
host process.

### Mapping

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

### Health and recovery

- Run `shikigami doctor` with the same config before starting the process.
  Governed/fail-closed profiles report an unhealthy or missing plane as an
  error.
- Process supervision is the plane-intake liveness signal in this slice. The
  filesystem `queue/health.json` file describes filesystem intake only.
- Heartbeats fail closed. Terminal acknowledgement retries with the same fence
  up to five times while its lease remains live; a lost fence, exhausted retry
  budget, or shutdown stops plane intake instead of continuing without claim
  authority. Lease safety uses host-monotonic deadlines bounded from each
  acquire/renew RPC, not cross-host wall-clock comparisons.
- Harness and mapping failures are acknowledged `failed` with a bounded
  reason. In this slice, parked runs are also terminalized as `failed` because
  the plane's `parked` outcome is immediately reclaimable and the resume
  envelope is not implemented yet; this prevents a fresh-run loop.
- Automatic retry budgets and poison-job quarantine are not implemented here;
  failed work stays terminal for operator inspection. Lease expiry, reclaim,
  park/resume, and poison semantics are hardened by
  [#132](https://github.com/Sannrox/shikigami/issues/132).

## Health

`queue/health.json` example fields: `ok`, `product`, `version`, `queue_inbox`,
`running`, `last_run_id`.

## Operator notes

- Use the same config/env as `run` / `doctor`.
- For fleets, put the binary under process supervision (systemd, tenkai, etc.).
- HTTP and plane work-unit intake are **not** in v0.x; extend via ADR if needed.
