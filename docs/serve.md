# `shikigami serve` (filesystem queue or plane claim host)

Current long-running host over the same `Harness` library as one-shot `run`.
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
  --claim-ttl-secs 60 \
  --checkpoint-store-id shikigami-local
```

Requirements:

- build with the default `governance-sekai-chisei` feature;
- set `governance.adapter = "sekai-chisei"`, endpoint, namespace, principal,
  and optional `token_env`;
- use a principal authorized for team-namespace write on the claim namespace;
- when checkpoint resume is enabled, configure the same logical store id in
  the plane's `SEKAI_CHECKPOINT_STORES` allowlist; and
- keep `runtime_id` aligned with the admitted `runtime_dispatch` payload.

The host lists claimable work, acquires a fenced claim, fetches the parent
ActionInstance parameters, maps them to `RunRequest`, executes the existing
`Harness`, heartbeats while the run is active, and acknowledges `completed`,
`failed`, or an intentional `parked` outcome. A park is not immediately
claimable. The plane records it as `awaiting_continuation` until an authorized
`resolve_parked_work/v1` Action is invoked. Governed planning and harvest still
use the configured sekai-chisei governance adapter and the
ActionInstance-bound operation id.

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
- Heartbeats fail closed: loss or expiry cancels the active harness future
  within a bounded grace period. The host never continues execution without a
  live fence. Terminal acknowledgement retries with the same fence
  up to five times while its lease remains live; a lost fence, exhausted retry
  budget, or shutdown stops plane intake instead of continuing without claim
  authority. Lease safety uses host-monotonic deadlines bounded from each
  acquire/renew RPC, not cross-host wall-clock comparisons.
- Harness and mapping failures are acknowledged `failed` with a bounded
  reason. Parked runs are acknowledged `parked` with a durable idempotency key.
  If `--checkpoint-store-id` is configured, the acknowledgement also carries
  the opaque local run id and a SHA-256 digest of `checkpoint.json`; it never
  sends a filesystem path or checkpoint bytes.
- Resolve a parked effect through sekai-chisei's governed parked-work Action.
  Shikigami expects bounded continuation input shaped as
  `{"answer":"operator response"}`. Input and extracted answers are capped at
  16 KiB by the default host policy. Policy denial or pending approval leaves
  the effect parked; successful invocation makes the same effect ready.
- On the next claim, shikigami verifies the continuation digest, stable effect
  and operation ids, store id, opaque checkpoint reference, checkpoint digest,
  and parked checkpoint state. A valid local checkpoint resumes the same
  `run_id` and logical operation. The host reports fenced `resume_started` and
  `resume_succeeded` events.
- If the checkpoint is absent, belongs to another store, has an invalid
  reference, or fails integrity validation, shikigami reports fenced
  `checkpoint_unavailable` and `replacement_started` events. It starts a new
  attempt from the original admitted task plus the governed continuation input,
  while retaining the same plane `operation_id` and `effect_id`.
- The plane owns claim, lease-expiry, and park-cycle counters. Admission
  snapshots the retry limits (currently 8 claims, 3 lease expiries, and 3 park
  cycles by default); exhausted work becomes `dead_lettered` and is not
  returned by claim listing. Shikigami adds no hidden automatic poison-job
  retry loop.

Operator recovery:

1. Inspect the parked effect, immutable park record, operation receipt, and
   checkpoint metadata in sekai-chisei.
2. Submit `resolve_parked_work/v1` for the current `park_generation`, with
   `input_json` containing a non-empty `answer`.
3. Complete any required approval. Approval alone does not make work
   claimable; the resolution Action must be invoked.
4. Keep the original host state available for checkpoint resume, or allow a
   replacement host to report checkpoint unavailability and rebuild under the
   same logical operation.

See sekai-chisei's
[runtime claim contract](https://github.com/Sannrox/sekai-chisei/blob/main/docs/runtime-claim.md)
for resolution, retry, dead-letter, and authorization semantics.

## Health

`queue/health.json` example fields: `ok`, `product`, `version`, `queue_inbox`,
`running`, `last_run_id`.

## Operator notes

- Use the same config/env as `run` / `doctor`.
- For fleets, put the binary under process supervision (systemd, tenkai, etc.).
- Plane-claim intake is shipped. Additional intake transports, including direct
  HTTP admission, require a separate contract and must preserve the same
  `Harness` and governance boundaries.
