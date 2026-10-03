# Plane Action → shikigami run

This is the operator and integrator contract for executing a
sekai-chisei-admitted Action on a shikigami host. The plane owns admission,
placement state, fencing, retry limits, continuation decisions, receipts, and
audit. Shikigami owns the claimed execution attempt and its local workspace.

For the complete plane API, use sekai-chisei's
[governed Action effects](https://github.com/Sannrox/sekai-chisei/blob/main/docs/governed-action-effects.md),
[runtime claim](https://github.com/Sannrox/sekai-chisei/blob/main/docs/runtime-claim.md),
and
[harvest binding](https://github.com/Sannrox/sekai-chisei/blob/main/docs/action-harvest-binding.md)
references.

## Start the host

Use a governed configuration and explicit plane intake:

```bash
export SHIKIGAMI_PROFILE=governed
export SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051

shikigami --config shikigami.toml serve \
  --intake plane \
  --runtime-id shikigami \
  --claim-ttl-secs 60
```

Run `shikigami doctor` with the same configuration first. Add
`--checkpoint-store-id <logical-store-id>` only when that id is allowlisted by
the plane and the host state is intended to support parked-run resume. See
[serve.md](serve.md) for checkpoint and recovery details.

## Runtime identity prerequisites

Before starting plane intake, obtain an active runtime principal and namespace
from the plane administrator. Configure `governance.principal` and
`governance.namespace` in the governed settings file, with the matching
credential selected by `governance.token_env` when the plane requires one.
The plane must authorize that identity to list and claim Action work for the
selected `--runtime-id`, and to renew and report its claims. See the
[governance settings reference](settings.md#governance).

When a producing application is retired, restore the worker only with a
plane-authorized runtime identity that is independent of the retired
application. An old application-managed principal or namespace is not a
runtime provisioning contract. The plane administrator must supply or approve
the replacement identity, namespace, credentials, and policy before the
operator changes the worker configuration. Shikigami does not create those
plane resources or discover a replacement identity.

A successful `doctor` confirms the configured connectivity and its schema
probe; it does not prove permission to call `ListClaimableActionWork`.
The first intake poll still has to pass the plane's current policy and state
preconditions. Endpoint reachability and `allow_insecure_remote` only address
transport; neither grants claim authority.

## Recover from a claim-list rejection

If `serve --intake plane` reports `ListClaimableActionWork` with a policy or
state precondition error:

1. Keep the worker stopped while the plane administrator checks whether the
   configured principal and namespace are still active and authorized for the
   selected runtime. A retired application identity requires an approved
   independent runtime identity before restoration.
2. Apply the approved `governance.principal`, `governance.namespace`, and
   credential configuration. Keep the governed adapter and fail-closed policy.
3. Run `doctor` with that same configuration, then start plane intake using
   the command above. A successful idle poll can return no work; a claim-list
   rejection is an error, not an idle queue.
4. Check worker lifecycle readiness as described in [serve.md](serve.md#worker-lifecycle-contract-fleet-hosts).
   If the plane still rejects intake, keep the worker stopped and return the
   rejection to the plane administrator for policy or state correction.

A list RPC rejection exits the plane serve loop before creating a run. When
worker lifecycle reporting is enabled, it marks `governance_unavailable` and
stops accepting claims. The host does not retry under a different identity or
fall back to an offline adapter. This is the existing fail-closed behavior;
changing an endpoint or weakening governance cannot repair a retired identity.

## Happy path

1. **Produce and admit.** A producer submits an `ActionInstance` through
   sekai-chisei's governed admission surface. The producer supplies typed,
   bounded parameters and an idempotency key; it does not write shikigami queue
   files or call the harness directly.
2. **Materialize dispatch.** After policy and approval permit admission, the
   plane materializes a `runtime_dispatch` `ActionEffect` whose payload names
   runtime `shikigami`, the parent instance, the stable operation, and the
   digest of the admitted parameters.
3. **Claim.** `shikigami serve --intake plane` lists ready work, acquires the
   effect with a generation and fencing token, fetches the parent parameters,
   and renews the lease before execution.
4. **Validate and map.** The host verifies effect kind/status, instance and
   operation correlation, runtime, parameter digest, task bounds, timeout cap,
   workspace policy, and any governed continuation. Only then does it create a
   `RunRequest`.
5. **Execute while fenced.** The shared `Harness` runs with sekai-chisei
   governance. Consequential tools still require their normal external-action
   authorization. Heartbeats preserve claim authority; loss of authority stops
   local execution fail closed.
6. **Harvest and acknowledge.** Run and tool events are harvested under the
   stable `operation_id`. A completed acknowledgement includes `artifact_json`
   when the retained inventory covers `app/` or `application/`, `sdk/` or
   `typed_sdk/`, `tests/` or `test/`, and `deploy/`, `delivery/`, or
   `delivery_inputs/`. The host classifies captured files only; it does not
   invent projections or put file bytes on the receipt. Incomplete, truncated,
   run-mismatched, or over-64KiB inventories omit the field so the completed
   ack still lands. The live claimant acknowledges `completed`, `failed`,
   or an intentional `parked` outcome. A park requires a governed resolution
   before the same effect becomes claimable again.

The plane never starts the shikigami process. A supervisor such as systemd,
Tenkai, Kubernetes, or a local operator owns host lifecycle. For readiness,
drain, and failure signals on plane workers, see the worker lifecycle contract
in [serve.md](serve.md) and [examples/k8s-worker-lifecycle.yaml](../examples/k8s-worker-lifecycle.yaml).

## Correlation identifiers

Keep durable work identity separate from disposable attempts:

| Identifier | Owner and lifetime | Shikigami use |
| --- | --- | --- |
| `type_id` | Plane; governed Action definition | Determines admitted effect mapping |
| `instance_id` | Plane; one admitted Action instance | Parent of the claimed effect |
| `effect_id` | Plane; stable dispatch item across reclaim/resume | Heartbeat, claim events, and acknowledgement target |
| `operation_id` | Plane; stable logical operation and receipt spine | Copied to `RunRequest.logical_operation_id` |
| `claim_generation` + fencing token | Plane; one claim attempt | Fences every claimant mutation |
| `run_id` | Shikigami; one harness attempt/checkpoint | Plane `attempt_id`; reused only for valid checkpoint resume |
| `park_generation` | Plane; one intentional wait cycle | Fences a continuation answer to the exact park |

A replacement after lease expiry or checkpoint loss gets a new `run_id` and
claim generation but retains the same `operation_id` and `effect_id`.

## Inspect execution and recovery

Use the plane as the operational source of truth:

- `GetActionInstance(instance_id)` shows the admitted parent and operation.
- Through the deployed sekai-chisei API or its admin tooling,
  `GetActionEffect(effect_id)` or `ListActionEffects(instance_id)` shows
  lifecycle, claim owner/generation, retry counters, park generation, and
  terminal state. The versioned `sekai-client` facade exposes the claim,
  heartbeat, acknowledgement, and event subset consumed by Shikigami; the
  shikigami CLI does not expose these inspection RPCs.
- `GetOperationReceipt(host_plan_id)` reconstructs the host planning receipt,
  attempt, model, tool, intervention, resume/replacement, and outcome events.
  Each `model_called` event links to the corresponding executed model receipt;
  the logical `operation_id` remains the parent lineage used to correlate
  replacement attempts.

Local events and checkpoints help diagnose one host, but they do not override
plane state. For detailed event fields, see [harvest.md](harvest.md) and
[identity.md](identity.md).

### Fail-closed cases

| Condition | Required behavior |
| --- | --- |
| Plane unavailable before claim | Do not start admitted work |
| Claim race lost | Poll again; do not run the candidate |
| Lease or fence lost mid-run | Cancel and drain local execution (reap bash process groups); do not acknowledge under stale authority |
| Claim/heartbeat identity mismatch (effect, owner, token, generation) | `FenceLost`; do not adopt another principal's fence or swap work identity |
| Lease grant missing, expired, or `ttl_ms == 0` | Reject; do not default to 60s or overstay the requested TTL |
| Claimed envelope fails validation | Acknowledge `failed` while the fence is live |
| Run parks for operator input | Acknowledge `parked`; wait for governed `resolve_parked_work/v1` |
| Checkpoint unavailable after resolution | Report fenced fallback events and start a replacement under the same operation |
| Retry or park limit exhausted | Plane dead-letters the effect; host must not invent another retry |
| Terminal/event RPC is transient | Retry the same idempotency key within the live lease, renewing the fence between attempts |

## Trust boundaries

Do not:

- start an ungoverned `run` for work that was admitted for governed plane
  execution;
- treat task text, continuation JSON, artifact content, model output, or remote
  text as policy, host configuration, credentials, or tool authority;
- bypass claim fencing by copying Action parameters into filesystem intake;
- put raw checkpoint paths, URLs, bytes, or credentials in plane checkpoint
  references;
- make the plane spawn hosts, hold model tools, or execute workspace commands;
  or
- claim exactly-once external mutations. Fencing and idempotency reduce
  duplicate execution risk but cannot undo an effect that already occurred.

Filesystem intake remains the offline default and is not a substitute system
of record for plane-admitted work.
