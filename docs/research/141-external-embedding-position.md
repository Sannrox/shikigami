# External library embedding product position

Research closeout for
[#141](https://github.com/Sannrox/shikigami/issues/141), accepted 2026-07-29.

## Recommendation

Retain external Rust library embedding as a supported, stable integration
surface, but position it as an **advanced** path rather than the default product
entry point.

Operators and integrators should start with:

1. CLI `doctor` / `run` for one-shot operation and CI;
2. `shikigami serve` for long-running filesystem or plane-claim intake; or
3. MCP stdio for IDE and tool-client integration.

Embed `Harness` when a process boundary would lose required behavior: typed
results, cooperative cancellation, live in-process events, or direct metrics.

This is positioning, not a contract reduction. The CLI, `serve`, MCP, and
external embedders continue to share the same `Harness`.

## Evidence

| Question | Evidence | Conclusion |
| --- | --- | --- |
| Are there known production external crate consumers? | GitHub code search on 2026-07-29 found no exact public dependency on `Sannrox/shikigami` outside this repository and its smoke repository. No deployment owner or committed future embedder is recorded in the issue or repository. | No production adoption is established. Public search cannot prove that private consumers do not exist. |
| What does the external proof establish? | [`Sannrox/shikigami-embed-smoke`](https://github.com/Sannrox/shikigami-embed-smoke) is maintainer-owned, pinned to `v1.0.0`, and exercises `doctor_async`, `run_with_events`, and transcript export offline. It has no forks or stars and describes itself as an ADR 0004 host proof. | It proves out-of-tree compatibility, not independent adoption. |
| Which capabilities benefit from embedding? | `Harness` exposes typed `RunResult`, `RunRequest.cancel`, `ChannelSink` events, `Metrics`, and port-level composition. | Embedding remains useful when callers require in-process control. |
| Can common hosts avoid embedding? | The shipped CLI, `serve`, and MCP server all invoke the shared `Harness`; `serve` supports filesystem and plane-claim intake. | Common one-shot, daemon, and tool-client paths have maintained process hosts. |
| What maintenance is specific to the public contract? | ADR 0004 and `docs/embedding.md` require semver stability for named library, run, settings, event, transcript, and doctor surfaces. CI runs the in-repository embed smoke; the external smoke must remain compatible with released tags. | The ongoing cost is compatibility review and proof maintenance, even without demonstrated production adoption. |

Repository history shows that issues #107–#115 deliberately established and
froze the embedding proof before `v1.0.0`. That evidence supports retaining the
contract, but it does not establish embedding as the primary integration
choice.

## Compatibility promises retained

The existing 1.x promises remain unchanged:

- `Harness::{from_config, resolve, doctor, doctor_async, run, run_with_events}`;
- version 1 settings and their documented compatibility policy;
- `RunRequest`, `RunResult`, and `RunTermination`, including park and resume;
- additive `HarnessEvent` evolution and the in-process channel sink;
- transcript export schema and doctor JSON schema discipline documented in
  [embedding.md](../embedding.md); and
- offline operation without a governance plane.

Surfaces already documented as evolving or host-only remain outside the
freeze-core promise. This decision does not add new compatibility guarantees.

## Alternatives

| Option | Decision | Reason |
| --- | --- | --- |
| Keep embedding positioned as primary | Rejected | The only external consumer found is a maintainer-owned compatibility proof, while maintained process hosts cover the common paths. |
| Retain support but change positioning | **Selected** | Preserves the 1.x contract and valuable in-process capabilities without implying unproven adoption. |
| Treat the library only as an internal boundary | Rejected for 1.x | It would conflict with ADR 0004 and published semver promises. Reconsideration requires a future major-version contract decision. |
| No action | Rejected | Existing docs explicitly rank the library as primary, which overstates the adoption evidence. |

## Consequences

- Usage documentation recommends CLI, `serve`, or MCP before library embedding.
- The in-repository and external embed smokes remain required compatibility
  proofs.
- `Harness` remains the internal boundary for every host and the stable public
  boundary for advanced external embedders.
- No API, settings, runtime, governance, or security behavior changes.
- No ADR amendment is required because the accepted 1.x contract is unchanged.
