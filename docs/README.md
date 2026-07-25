# Documentation

Operator and contributor documentation for **shikigami**.

## Start here

| If you want to… | Read |
| --- | --- |
| Install and run offline | [../README.md](../README.md) |
| Understand why the project exists | [../VISION.md](../VISION.md) |
| Understand architecture | [../DESIGN.md](../DESIGN.md) |
| Configure profiles and env vars | [settings.md](settings.md) |
| Choose or implement adapters | [adapters.md](adapters.md) |
| Embed the library | [embedding.md](embedding.md) |
| See accepted design decisions | [decisions/](decisions/) |
| Contribute code | [../CONTRIBUTING.md](../CONTRIBUTING.md) |
| Report a vulnerability | [../SECURITY.md](../SECURITY.md) |

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
