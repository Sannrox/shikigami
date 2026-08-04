# Research: plane work intake for `shikigami serve`

Issue: [#129](https://github.com/Sannrox/shikigami/issues/129)  
Date: 2026-07-28  
Status: **recommendation complete**  
Plane dependencies:
[sekai-chisei#395](https://github.com/Sannrox/sekai-chisei/issues/395)
and
[sekai-chisei#399](https://github.com/Sannrox/sekai-chisei/issues/399)
are complete.

## Decision

Adopt a **hybrid sequence**:

1. **Pull/claim is the long-term product path.** An explicit plane-intake
   adapter in the `serve` host lists and claims admitted
   `runtime_dispatch` effects, maps each claim into `RunRequest`, runs the
   existing `Harness`, reports lifecycle against the bound operation, and
   acknowledges the claim.
2. **The filesystem queue remains the offline default and temporary bridge.**
   A plane-adjacent producer may write an already-admitted job into
   `queue/inbox`, but the bridge is not the source of truth for admission or
   leases and must be described as superseded by direct claim intake.
3. **Do not use push-to-host as the primary design.** It requires a reachable
   host, duplicates retry/lease state, and moves scheduling responsibility
   toward the plane.
4. **Host-owned starts remain supported.** CLI and embedders may continue to
   start runs and supply `logical_operation_id`; they are not sufficient for
   unattended plane placement.

This preserves [ADR 0001](../decisions/0001-ports-and-settings.md): intake is a
host adapter selected by settings, while `Harness` and the turn loop remain
independent of the placement source. It also preserves
[ADR 0003](../decisions/0003-serve-daemon.md): `serve` executes claimed work but
does not admit policy, own durable claim state, or become a control plane.

## Evidence

### Current filesystem intake

`shikigami serve` polls `$SHIKIGAMI_STATE/queue/inbox/*.json`. It atomically
moves the next lexically sorted file to `processing/`, maps the job to
`RunRequest`, and then:

- moves a completed harness run plus `*.result.json` to `done/` or `failed/`;
- moves a job whose harness run returns an error to `failed/` and writes
  `*.error.txt`; but
- currently leaves malformed JSON in `processing/`, requiring operator
  recovery.

The version-1 job envelope is:

```json
{
  "task": "write the demo marker",
  "keep_workspace": true,
  "logical_operation_id": "plane-operation-id",
  "timeout_secs": 120
}
```

| Queue field | `RunRequest` field | Notes |
| --- | --- | --- |
| `task` | `task` | Free-form task envelope; not policy admission |
| `keep_workspace` | `keep_workspace` | Host-local retention choice |
| `logical_operation_id` | `logical_operation_id` | Plane operation/receipt correlation |
| `timeout_secs` | `timeout` | Host execution bound |

The queue has no durable admission record, lease generation, heartbeat, or
idempotent acknowledgement. Its rename is only a single-filesystem local
claim, and malformed-job recovery is incomplete. Those limitations are
acceptable for offline use, but make it unsuitable as the long-term plane
placement contract.

### Current governed correlation

The harness always creates a distinct `run_id` for the attempt. A host-supplied
`RunRequest.logical_operation_id` becomes the governed `operation_id`:

- `PlanExecution.ExecutionInput.logical_operation_id = operation_id`;
- `attempt_id = run_id`;
- the 1.0 plane generates a host `PlanExecution.plan_id` receipt identity;
  attempt, model, tool, and completion events are reported against that host
  receipt with causal parents, while each model call has its own executed
  `PlanExecution` receipt; and
- `GetOperationReceipt(host_plan_id)` reconstructs the plane-visible host
  history, with model receipt ids linked from `model_called` attributes.

Direct claim intake should therefore use the ActionInstance-bound
`operation_id` as `RunRequest.logical_operation_id`; it must not mint a second
receipt identity.

### Plane claim contract

The plane-side mapping research chose an `ActionInstance` as a thin admission
envelope bound to one operation receipt spine. Admitted `runtime_dispatch`
effects are the claimable placement unit.

The additive runtime claim API now provides:

| RPC | Host responsibility |
| --- | --- |
| `ListClaimableActionWork` | Filter claimable work by namespace and runtime |
| `ClaimActionWork` | Acquire an exclusive TTL lease with generation and fencing token |
| `HeartbeatActionClaim` | Extend the matching live claim while the run continues |
| `AckActionWork` | Report `completed`, `failed`, or `parked` under the matching fence |

An authorized host needs team-namespace write permission. Expired claims are
reclaimable, and acknowledgements require the matching runtime, generation,
and fencing token. The plane remains the claim source of truth and never
spawns a host process or runs model tools.

## Proposed host boundary

Plane intake belongs beside filesystem intake in the thin `serve` host:

```text
filesystem inbox ─┐
                  ├─ intake adapter → validated RunRequest → Harness
plane pull/claim ─┘                                      │
       ▲                                                  ▼
       └──── heartbeat / fenced ack ← operation harvest + result
```

The adapter owns list, claim, heartbeat, and acknowledgement. The existing
governance adapter continues to own planning, tool authorization, and harvest.
The turn loop must not know whether a caller came from a queue file, plane
claim, CLI, or embedder.

Minimum claim-to-run mapping:

| Claim data | Harness data | Rule |
| --- | --- | --- |
| Bound `operation_id` | `logical_operation_id` | Required; reject if empty |
| Runtime task parameter | `task` | Validate against the frozen Action type; bound size |
| Claim/effect id | host claim state | Never use as a replacement operation id |
| Lease TTL | heartbeat schedule | Does not silently expand run timeout |
| Optional timeout/retention hints | `timeout`, `keep_workspace` | Apply host defaults and caps; plane hints may only narrow policy |
| Unknown parameters | none | Ignore only when the Action type permits them; otherwise fail closed |

Remote parameter text is data inside the admitted task envelope. It must not be
interpreted as configuration, credentials, shell arguments, or instructions to
change host policy.

## Failure and recovery decisions

| Failure | Required host behavior |
| --- | --- |
| Lease expires or heartbeat loses the fence mid-run | Cancel the run and stop mutations; never acknowledge with a stale fence. A later claimant receives a new generation. |
| Run parks for operator input | Persist the normal checkpoint, acknowledge `parked`, and release the live claim. Resume is a new claim/attempt using the same logical operation id. |
| Run completes or fails | Harvest completion first, then make a fenced idempotent acknowledgement. Retry acknowledgement with the same claim identity. |
| Plane unavailable before claim | Start no plane work. Filesystem/offline intake remains available only when the selected profile permits it. |
| Plane unavailable mid-run | A governed fail-closed host cancels when it cannot revalidate/heartbeat. Any best-effort harvest retry must not extend claim authority. |
| Repeated deterministic failure | Poison handling belongs to the intake adapter: after a configured bounded attempt count, acknowledge `failed` with a sanitized reason and require operator intervention. |
| Host crash | The plane lease expires and work becomes reclaimable. Local checkpoint state is recovery evidence, never claim authority. |

Exactly-once external mutations remain a non-goal. Fencing prevents a stale
host from acting as the current claimant; permitted external actions must also
retain their existing idempotency and authorization checks.

## Alternatives

| Option | Result | Reason |
| --- | --- | --- |
| Pull/claim only, immediately | Defer as a migration stance | Correct end state, but removing the proven offline queue would break local-first operation |
| Push into each host | Reject | Reachability, retry, and lease state move toward the plane and duplicate host admission concerns |
| Filesystem bridge only | Temporary only | Useful for dogfood, but it cannot represent plane claim truth or fencing |
| Host-owned starts only | Retain, not primary placement | Correct for CLI/embed use, insufficient for unattended admitted work |
| Hybrid sequence | Choose | Direct claim is the target while offline and interim paths stay explicit |

## Delivery sequence

The existing follow-up issues already express the reviewable slices; do not
create duplicates:

1. [#130](https://github.com/Sannrox/shikigami/issues/130): freeze and test
   claim-payload to `RunRequest` mapping.
2. [#131](https://github.com/Sannrox/shikigami/issues/131): implement direct
   claim, heartbeat, harvest, and acknowledgement as an explicit intake mode.
3. [#132](https://github.com/Sannrox/shikigami/issues/132): harden expiry,
   park/resume, poison, and outage recovery.
4. [#133](https://github.com/Sannrox/shikigami/issues/133): publish the
   operator/integrator contract against implemented behavior.
5. [#134](https://github.com/Sannrox/shikigami/issues/134): optional interim
   filesystem bridge example. Cancel it if direct claim intake lands before
   bridge dogfood has value.

The smallest safe implementation order is #130 → #131 → #132 → #133. #134 may
run after this recommendation because it only produces already-admitted queue
jobs and is explicitly temporary.

## Scope boundaries

- Sekai-chisei owns Action admission, claim state, fencing, and durable
  operation receipts.
- Shikigami owns host execution, local workspaces/checkpoints, and the intake
  adapter process.
- `Harness` remains the shared execution API; `serve` remains a thin host.
- Filesystem intake stays the default without a plane.
- Shikigami never admits Action types, invents policy, or grants external
  mutation authority from task text.
- Tenkai remains delivery-only and is not a runtime intake port.

## Exit result

Choose **hybrid sequencing with pull/claim as the target**. The plane claim API
is no longer a research blocker. Feature work can proceed through #130–#133;
#134 is an optional, explicitly superseded dogfood bridge.
