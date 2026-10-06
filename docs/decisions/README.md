# Architecture decision records

Accepted decisions that must outlive a single PR.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-ports-and-settings.md) | Ports and settings (sekai-chisei first-party) | Accepted |
| [0002](0002-run-identity.md) | Run identity and plane operation lineage | Accepted |
| [0003](0003-serve-daemon.md) | `shikigami serve` local-queue daemon | Accepted |
| [0004](0004-v1-contract.md) | v1.0 contract and bright-future sequencing | Accepted |
| [0005](0005-governed-run-replay.md) | Governed run replay | Accepted |
| [0006](0006-bounded-content-parts.md) | Bounded multimodal content | Accepted |
| [0007](0007-governed-local-fallback.md) | Governed local-model fallback | Accepted |
| [0008](0008-recovery-diagnosis.md) | Read-only recovery diagnosis | Accepted |
| [0009](0009-tool-call-correlation.md) | Stable tool-call correlation in progress events | Accepted |
| [0010](0010-cli-run-replay.md) | CLI observation-only run replay | Accepted |
| [0011](0011-cli-content-intake.md) | CLI bounded-content process-host request | Accepted |
| [0012](0012-replay-export.md) | Export validated replay inputs from a retained run | Accepted |
| [0013](0013-os-sandbox-adapter.md) | OS-level sandbox tiers for governed tool execution | Accepted |
| [0014](0014-usable-guest-hosts.md) | Usable guest process hosts (ACP, TUI, session wait, ask=park) | Accepted |
| [0015](0015-nested-child-runs.md) | Nested child runs as first-class Runs | Accepted |
| [0016](0016-session-mode.md) | Session mode frozen at first prompt | Accepted |
| [0017](0017-acp-attachments.md) | ACP prompt attachments, fail closed | Accepted |
| [0018](0018-skills-mcp-extension.md) | Skills and MCP are the only extension surface | Accepted |
| [0019](0019-skill-context-boundaries.md) | No skill-declared context boundaries | Accepted |

## When to write an ADR

Add an ADR when a choice:

- changes a public boundary (ports, settings schema, CLI stability);
- chooses among durable alternatives (e.g. how model turns are governed);
- would be expensive to reverse without a migration story.

Small bugfixes and local refactors do not need ADRs. Capture them in code,
tests, and the PR description instead.

Template: short context, decision, consequences, rejected alternatives — see
0001 for style.
