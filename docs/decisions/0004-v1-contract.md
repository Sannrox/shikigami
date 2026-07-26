# ADR 0004: v1.0 contract and bright-future sequencing

- Status: Accepted
- Date: 2026-07-25
- Supersedes: —
- Resolves: research #29

## Context

Shikigami 0.1.x shipped as a ports+settings headless harness with local and
sekai-chisei paths, cancel/timeout, checkpoint resume, park/escalate, serve
queue, harvest, identity, credentials hygiene, metrics, and release artifacts.
Before 1.0 we need a **must-have / won't-have** contract so OSS and embedders
(onmyoji and peers) know what freezes.

## Decision: medium 1.0 contract

Choose **option 2 (medium)** from the research issue: more than thin library-only,
less than “proven multi-host production everywhere.”

### Must be stable at 1.0

| Area | Contract |
| --- | --- |
| Architecture | ADR 0001 ports + settings; tenkai delivery-only |
| Library | `Harness::{from_config, resolve, doctor, doctor_async, run, run_with_events}` |
| Settings | `version = 1` schema policy (deny unknown; documented breakage) |
| Run | `RunRequest` / `RunResult` / `RunTermination` including park + resume_answer |
| Identity | ADR 0002 (`run_id` = attempt, `logical_operation_id` override) |
| Events | `HarnessEvent` additive evolution; channel sink for embedders |
| CLI | `version`, `doctor`, `run`, `serve` subcommands |
| Offline OSS | `cargo test` without plane remains first-class |
| Governed path | PlanExecution + external-action tool authz + harvest events |
| Doctor JSON | `schema_version` discipline |

### Explicit won't-haves for 1.0

- Second policy brain or replacing sekai-chisei
- Tenkai as runtime config plane
- Multi-tenant SaaS control plane inside shikigami
- Desktop approval UX (hosts own UI; park/resume is the protocol)
- Full work-unit admission ownership (hosts may pass logical_operation_id)
- Guaranteed exactly-once events across crash (checkpoints + plane harvest)
- Universal “every OS forever” release matrix without maintenance

### Bright-future themes (post-1.0 sequencing)

Ordered epics — **not** 1.0 blockers:

1. **Universal execution plane** — richer serve intake (HTTP/plane work-units) after FS queue is proven
2. **Governance-native harvest** — deeper sekai objects / receipts beyond operation events
3. **Delivery-native fleets** — tenkai packaging automation and multi-arch operator kits
4. **Adapter ecosystem** — documented out-of-tree ports; optional model/workspace adapters
5. **Quality loop** — eval harnesses, prompt versioning experiments, metrics scrapers

### Evidence considered

- Embed API freeze list (`docs/embedding.md`) and ADRs 0001–0003
- Green CI, cargo-deny, tagged multi-arch `v0.1.0` release
- Governed path docs + ignored live tests + optional nightly workflow
- Park/serve/metrics/event stream shipped in 0.x

## Consequences

- Pre-1.0 may still break 0.x with CHANGELOG discipline
- 1.0 release checklist: freeze table above + at least one external embed smoke
  (e.g. onmyoji or a documented peer) without requiring plane for offline path
- Follow-up Issues should only open for decided post-1.0 epics, not open-ended research

## Freeze audit

Point-in-time go/no-go against this ADR:
[docs/1.0-freeze-audit.md](../1.0-freeze-audit.md) (research #109, 2026-07-26).
Recommendation: **no-go** until external embed smoke exists (or this ADR is
amended via Design Discussion).

## Rejected alternatives

1. **Thin 1.0** — omits serve/park/harvest already delivering operator value
2. **Broad 1.0** — requires multi-host production proof and plane work-unit ownership too early
