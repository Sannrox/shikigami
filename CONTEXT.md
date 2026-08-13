# Domain context

This glossary complements the architecture and product definitions in
[`DESIGN.md`](DESIGN.md).

## Effective settings resolution

The host-side protocol that selects the settings source and applies defaults,
file values, profile presets, environment values, and the final host model
selection before validation and source attribution. `Config`, `StateRoot`, and
`Harness` delegate this ordering to a private deep module; the public settings
schema and ports remain unchanged.

## Harness diagnosis

The host-side health and configuration projection exposed through the stable
`Harness::doctor` and `Harness::doctor_async` interface and `DoctorReport`
schema. One private deep module owns adapter health, effective tool authority,
credential-source reporting, secret redaction, fail-closed classification, and
live plane probing; it does not add a port or adapter seam.

## Builtin tool execution

The run-scoped protocol that decides which builtins exist, which the allow-list
authorizes (including bash helpers that share `bash` authority), and how one
call executes inside the workspace jail. `ToolRegistry` is the only external
interface; catalog expansion, jailed dispatch, and shared bash spawn stay in
the private implementation. It is not a new public port or adapter seam.

## Durable tool batch

One model turn's ordered set of tool calls, including authorization, execution,
stable and conversation tool-call identity, checkpoint markers, report replay,
hooks, events, and park handling. The Run transaction delegates this protocol
to a private deep module; it is not a new public port or adapter seam.

## Run admission and supervision

One Run's host-local protocol around the durable Run transaction, including
checkpoint preflight, registry ownership, independent heartbeat publication,
cancel and timeout bounds checks, transaction invocation, and durable result
or error finalization. `Engine` delegates this protocol to a private deep
module behind its existing public interface; it is not a new public port or
adapter seam.

## Run preparation

One Run's host-local pre-turn protocol, including fresh or resumed state,
workspace and snapshot preparation, artifact baseline capture, context
composition, Tool Registry and MCP attachment, governed admission, initial
checkpoint durability and compensation, recovery replay, and pre-run hooks.
The Run transaction delegates this protocol to a private deep module behind
one preparation interface; it is not a new public port or adapter seam.

## Run artifact lifecycle

One Run's artifact retention protocol, including best-effort initial baseline,
terminal background-job reaping, bounded manifest and patch capture, warning
publication, and Run Registry linkage. Run preparation and the Run transaction
delegate this protocol to a private deep module; stable manifest and export
compatibility remain in the public artifacts module.

## Durable model turn

One run-loop iteration's model-side protocol, including staged resume replay,
turn limits, context compaction, governed planning, usage accounting, assistant
checkpointing, model-report acknowledgement, and post-turn cancellation. The
Run transaction delegates this protocol to a private deep module; it is not a
new public port or adapter seam.

## Claimed run transaction

One plane-acquired claim's fenced execution protocol, including continuation
preparation, claim events, heartbeat and shutdown races, harness execution,
acknowledgement retry (renewing the live fence between attempts), fail-closed
drain after in-run fence loss, and worker lifecycle publication. Plane intake
delegates this protocol to a private deep module; it does not add a public port
or adapter seam.

## Plane session

Shared sekai-chisei plane-connection protocol, including endpoint connect,
token and auth-source metadata, CallOptions correlation, SdkError mapping, and
the live probe. Governed RPC modules and the claim client delegate this
protocol to a private deep module; it is not a new public port or adapter seam.

## Plane claim acquisition

One fenced claim's plane protocol, including claimable listing, claim RPC,
continuation and park snapshot validation, action-instance parameter lookup,
pre-run fence renew, and post-admit heartbeat, ack, and claim-event RPCs with
contention → `FenceLost` mapping. The sekai-chisei claim client delegates this
protocol to a private deep module behind the existing `PlaneIntakePort` seam;
it does not move admission into the harness.

## Plane serve loop

The plane intake host's poll-and-admit protocol, including option validation at
the public entry, lifecycle accepting/draining gates, shutdown races around
claim acquisition, idle poll sleep, max_jobs limits, claim-error observation,
and delegation to the claimed run transaction. Plane intake keeps a thin
`run_plane_serve` interface over this private deep module.

## Governed tool authorization

One stable tool call's sekai-chisei external-action protocol, including risk
classification, request construction, decision interpretation, signed permit
redemption, execution identity, and fail-closed security checks. The
sekai-chisei adapter delegates this protocol to a private deep module behind
the existing governance port seam.

## Governed harvest transaction

One governed run's durable plane-reporting protocol, including local checkpoint
projection and restoration, causal event staging and retry, model and tool
report recovery, and in-doubt tool-execution detection. The sekai-chisei
adapter delegates this protocol to a private deep module; plane receipts remain
authoritative and the existing governance port seam is unchanged.

## Governed harvest event reporting

One governed run's plane RPC reporting protocol, including pending-event send
and retry, stage → ReportOperationEvent → commit, model and tool event-id
digests, receipt lookup, and abort-before-model finalization. The sekai-chisei
adapter delegates this protocol to a private deep module that pairs with the
local harvest transaction state module; the existing governance port seam is
unchanged.

## Governed model turn

One sekai-chisei model-side protocol, including request projection,
PlanExecution, budget and executability decisions, model-operation correlation,
ExecutePlanStream consumption, durable failure reporting, and response
projection. The sekai-chisei adapter delegates this protocol to a private deep
module behind the existing governance port seam.

## Governed run completion

One governed run's authoritative receipt-finalization protocol, including
pending-event retry, required-surface reconciliation, incomplete model-call
abortion, outcome reporting, completeness enforcement, and local harvest-state
release. The sekai-chisei adapter delegates this protocol to a private deep
module behind the existing governance port seam.

## Governed Run admission

One governed Run's pre-turn plane protocol, including lineage validation,
checkpoint restoration, authoritative receipt reconciliation, pending-event
replay, host-receipt creation, attempt establishment, and fail-closed error
policy. The sekai-chisei adapter delegates this protocol to a private deep
module behind the existing governance port seam.

## MCP tool attachment protocol

The run-start protocol that initializes configured MCP servers, discovers and
namespaces their tools, projects tool calls and results, and registers one
remote-tool implementation with the run-scoped Tool Registry. The stdio and
HTTP transport adapters retain framing and network behavior behind a private
transport seam; this is not a new public port or adapter seam.

## MCP stdio framing

The shared bounded `Content-Length` protocol used by both MCP stdio adapters.
One private deep module owns message encoding, header validation, duplicate or
invalid length rejection, and body-size enforcement before allocation; the
client and host adapters retain process and request behavior at their seams.

## MCP background Run lifecycle

The MCP host's single-flight asynchronous Run protocol, including start
admission, event collection, terminal result publication, status snapshots,
timeout behavior, and retained state-change signaling for waits. The MCP host
delegates this protocol to a private deep module; JSON-RPC routing and result
projection remain at the existing host interface.

## Filesystem serve loop

The local serve host's poll-and-drain protocol, including concurrency,
idle poll, `max_jobs`, graceful drain of claimed work, health writes, and
control-task abort. The public `run_serve_with_options` entry keeps option
validation and delegates this protocol to a private deep module; it is not a
new public port or adapter seam.

## Filesystem queue

The local serve intake's durable job lifecycle, including bounded admission,
priority claim, retry, collision-safe terminal archival and result writing, and
health observations. The serve host and local control adapter delegate this
protocol to a private deep module; it is not a new public port or adapter seam.

## Run Control API

The optional authenticated local HTTP operator protocol for health, metrics,
filesystem queue admission, and run inspection, cancellation, events, and
cleanup. The serve host delegates transport, framing, authentication, routing,
and response behavior to a private deep module; the public serve interface and
existing registry, metrics, and filesystem queue seams remain unchanged.
