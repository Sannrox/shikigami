# Architecture decision records

Accepted decisions that must outlive a single PR.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-ports-and-settings.md) | Ports and settings (sekai-chisei first-party) | Accepted |
| [0002](0002-run-identity.md) | Run identity and plane operation lineage | Accepted |
| [0003](0003-serve-daemon.md) | `shikigami serve` local-queue daemon | Accepted |
| [0004](0004-v1-contract.md) | v1.0 contract and bright-future sequencing | Accepted |

## When to write an ADR

Add an ADR when a choice:

- changes a public boundary (ports, settings schema, CLI stability);
- chooses among durable alternatives (e.g. how model turns are governed);
- would be expensive to reverse without a migration story.

Small bugfixes and local refactors do not need ADRs. Capture them in code,
tests, and the PR description instead.

Template: short context, decision, consequences, rejected alternatives — see
0001 for style.
