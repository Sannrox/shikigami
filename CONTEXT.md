# Domain context

This glossary complements the architecture and product definitions in
[`DESIGN.md`](DESIGN.md).

## Durable tool batch

One model turn's ordered set of tool calls, including authorization, execution,
checkpoint markers, report replay, hooks, events, and park handling. The Run
transaction delegates this protocol to a private deep module; it is not a new
public port or adapter seam.

## Durable model turn

One run-loop iteration's model-side protocol, including staged resume replay,
turn limits, context compaction, governed planning, usage accounting, assistant
checkpointing, model-report acknowledgement, and post-turn cancellation. The
Run transaction delegates this protocol to a private deep module; it is not a
new public port or adapter seam.
## Claimed run transaction

One plane-acquired claim's fenced execution protocol, including continuation
preparation, claim events, heartbeat and shutdown races, harness execution,
acknowledgement retry, and worker lifecycle publication. Plane intake delegates
this protocol to a private deep module; it does not add a public port or adapter
seam.

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
