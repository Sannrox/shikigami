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
