# Architecture decision records

Accepted decisions that must outlive a single PR.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-ports-and-settings.md) | Ports and settings (sekai-chisei first-party) | Accepted |

## When to write an ADR

Add an ADR when a choice:

- changes a public boundary (ports, settings schema, CLI stability);
- chooses among durable alternatives (e.g. how model turns are governed);
- would be expensive to reverse without a migration story.

Small bugfixes and local refactors do not need ADRs. Capture them in code,
tests, and the PR description instead.

Template: short context, decision, consequences, rejected alternatives — see
0001 for style.
