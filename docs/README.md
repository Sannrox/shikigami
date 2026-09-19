# Documentation

Operator and contributor documentation for **shikigami**.

## Start here

| If you want to… | Read |
| --- | --- |
| Install and run offline | [../README.md](../README.md) |
| Understand why the project exists | [../VISION.md](../VISION.md) |
| Understand architecture | [../DESIGN.md](../DESIGN.md) |
| Configure profiles and env vars | [settings.md](settings.md) |
| Run against sekai-chisei | [governed-path.md](governed-path.md) |
| Operate Action admit → claim → run | [plane-action-run.md](plane-action-run.md) |
| Map run outcomes to plane harvest | [harvest.md](harvest.md) |
| Correlate run / operation / attempt ids | [identity.md](identity.md) |
| Versioned prompts and attribution | [prompts.md](prompts.md) |
| Runtime skill packs | [skills.md](skills.md) |
| Plane/model credential patterns | [credentials.md](credentials.md) |
| Run serve (queue or plane claim) + worker lifecycle | [serve.md](serve.md) |
| Inspect runs, cancel, artifacts, and HTTP control | [runs.md](runs.md) |
| Run with bounded image, audio, and document parts | [content.md](content.md) |
| Replay a run from content-bound evidence | [replay.md](replay.md) |
| Run deterministic offline golden fixtures | [eval.md](eval.md) |
| Deliver binary via tenkai | [tenkai-delivery.md](tenkai-delivery.md) |
| Build or consume the OCI image | [oci-image.md](oci-image.md) |
| Run deterministic project checks | [project-verification.md](project-verification.md) |
| Run metrics export | [metrics.md](metrics.md) |
| Run and tool-call span export | [tracing.md](tracing.md) |
| Network egress policy | [network.md](network.md) |
| MCP client and server host | [mcp.md](mcp.md) |
| Lifecycle hooks | [hooks.md](hooks.md) |
| Choose or implement adapters | [adapters.md](adapters.md) |
| Embed the library | [embedding.md](embedding.md) |
| See accepted design decisions | [decisions/](decisions/) |
| Contribute code | [../CONTRIBUTING.md](../CONTRIBUTING.md) |
| Report a vulnerability | [../SECURITY.md](../SECURITY.md) |

## Historical research

Dated closeouts. Use the operator pages above for current behavior.

| Note | Read |
| --- | --- |
| 1.0 freeze audit (2026-07-26 no-go) | [1.0-freeze-audit.md](1.0-freeze-audit.md) |
| External embedding product position (research #141) | [research/141-external-embedding-position.md](research/141-external-embedding-position.md) |
| Profile and adapter configuration recommendation (research #142) | [research/142-profile-adapter-configuration.md](research/142-profile-adapter-configuration.md) |
| Plane work intake recommendation (research #129); shipped as [serve.md](serve.md) / [plane-action-run.md](plane-action-run.md) | [research/129-plane-work-intake.md](research/129-plane-work-intake.md) |
| OS-level sandbox research #281; shipped backends in [settings.md](settings.md) and [ADR 0013](decisions/0013-os-sandbox-adapter.md) | [research/281-os-sandbox-adapter.md](research/281-os-sandbox-adapter.md) |

## Document roles

| Document | Authority |
| --- | --- |
| `SECURITY.md`, license | Highest for safety and legal |
| Accepted ADRs + `VISION.md` / `DESIGN.md` | Product and architecture boundaries |
| `docs/settings.md`, examples | Configuration contract |
| Executable tests | Behavior proof |
| Implementation | Reality; fix docs or code when they disagree |

An inconsistency is a bug. Prefer updating the higher-authority source through
the normal contribution process, then align lower sources in the same change.

## Examples

See [../examples/README.md](../examples/README.md).

## Agent guidelines

Repository operating rules for automated and human agents:
[../AGENTS.md](../AGENTS.md). Project Skills:
[../.agents/skills/](../.agents/skills/).
