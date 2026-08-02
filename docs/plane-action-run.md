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
   stable `operation_id`. The live claimant acknowledges `completed`, `failed`,
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
  terminal state. Shikigami's vendored client schema contains only the
  claim/heartbeat/ack subset it consumes; the shikigami CLI does not expose
  these inspection RPCs.
- `GetOperationReceipt(operation_id)` reconstructs planning, run, tool,
  intervention, resume/replacement, and outcome events.

Local events and checkpoints help diagnose one host, but they do not override
plane state. For detailed event fields, see [harvest.md](harvest.md) and
[identity.md](identity.md).

### Fail-closed cases

| Condition | Required behavior |
| --- | --- |
| Plane unavailable before claim | Do not start admitted work |
| Claim race lost | Poll again; do not run the candidate |
| Lease or fence lost mid-run | Stop polling/cancel local execution; do not acknowledge under stale authority |
| Claimed envelope fails validation | Acknowledge `failed` while the fence is live |
| Run parks for operator input | Acknowledge `parked`; wait for governed `resolve_parked_work/v1` |
| Checkpoint unavailable after resolution | Report fenced fallback events and start a replacement under the same operation |
| Retry or park limit exhausted | Plane dead-letters the effect; host must not invent another retry |
| Terminal/event RPC is transient | Retry the same idempotency key within the live lease |

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
