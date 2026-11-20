# Domain context

This glossary complements the architecture and product definitions in
[`DESIGN.md`](DESIGN.md).

## Naming

- **Shikigami** — this product (the harness)
- **Run** — one unit of agent work
- Do not call an individual agent attempt “a shikigami”

## Approval park

When authority answers a tool authorization with `require_approval`, the
harness checkpoints and returns instead of executing. Session hosts use
`ask=park` and ACP `session/request_permission`. CLI `run` does not block
waiting for a human. A criteria or policy deny is not a park.

## Identity-only span export

Optional, default-off OpenTelemetry export of one run as a trace. Policy
gates whether anything leaves the process; a fixed attribute allowlist is
enforced at the emission boundary. Spans carry run, attempt, inbound
operation, and tool-call identities only. They are not
receipts and not a tracing backend.

## Delayed evidence queue

Bounded host-local spool for signed already-executed outcomes that cannot
reach receipt authority during an outage. Duplicate identities are
idempotent; conflicting payloads stay conflicted; capacity and retention
fail closed without silent eviction. The queue never admits new work.

## Local-model fallback

Fail-closed admission for one authorized disconnected model interval
([ADR 0007](docs/decisions/0007-governed-local-fallback.md), amended by
[ADR 0024](docs/decisions/0024-local-guest-rusui-plane-caller.md)). A host may
inject a grant; this crate does not call a plane to mint one. Selection
scratch lives on the governance checkpoint and is never a receipt.

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
turn limits, context compaction, `ModelPort` planning, usage accounting,
assistant checkpointing, model-report acknowledgement, and post-turn
cancellation. The Run transaction delegates this protocol to a private deep
module; it is not a new public port or adapter seam.

## MCP tool attachment protocol

The run-start protocol that initializes configured MCP servers, discovers and
namespaces their tools, projects tool calls and results, and registers one
remote-tool implementation with the run-scoped Tool Registry. The stdio and
HTTP transport adapters retain framing and network behavior behind a private
transport seam; this is not a new public port or adapter seam.

## MCP stdio framing

The shared bounded stdio framing used by both MCP stdio adapters. Requests
may use `Content-Length` or newline-delimited JSON; responses are accepted in
either framing. One private deep module owns message encoding, header
validation, duplicate or invalid length rejection, and body-size enforcement
before allocation; the client and host adapters retain process and request
behavior at their seams.

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
