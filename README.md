# shikigami

`shikigami` (式神) is a **local-first headless agent harness**. It runs agent
work under governance from
[sekai-chisei](https://github.com/Sannrox/sekai-chisei) and is deliverable as a
product through [tenkai](https://github.com/Sannrox/tenkai).

It is not a control plane, not a delivery plane, and not a desktop shell.
Those roles stay with sekai-chisei, tenkai, and operator UIs such as onmyoji.

> **Project status:** early-stage (`v0.1.0`). The product identity, local state
> layout, and CLI skeleton exist. Run execution is not implemented yet.

## Stack role

| Product | Role |
| --- | --- |
| **sekai-chisei** | Durable facts + governed decisions (policy, budget, eval, audit) |
| **tenkai** | Publish, promote, converge, probe, rollback |
| **shikigami** | Headless harness: run lifecycle, workspace, tools, harvest |
| operator UI (e.g. onmyoji) | Human front door; optional host of the same core ideas |

**Naming:** `shikigami` is the **product / harness**. Individual units of work
are **runs** (or workers/sessions in code). Do not call a single agent attempt
“a shikigami.”

## Quickstart

```bash
cargo build --bin shikigamictl

./target/debug/shikigamictl version
./target/debug/shikigamictl init
./target/debug/shikigamictl doctor
```

`init` creates `.shikigami-state/` with `shikigami.toml` and a `runs/` directory.
Operational truth for governed operations will live in sekai-chisei; this root
holds only harness-local install state and run workspaces.

## CLI

| Command | Purpose |
| --- | --- |
| `shikigamictl version [--json]` | Product identity |
| `shikigamictl init` | Create local state root |
| `shikigamictl doctor` | Check local prerequisites |
| `shikigamictl run [TASK]` | Reserved; not implemented yet |

Override the state root with `--state` or `SHIKIGAMI_STATE`.

## Development

```bash
cargo fmt
cargo test
cargo build --all-targets
```

Read [DESIGN.md](DESIGN.md) for product boundaries and the first vertical slice.
Repository workflow notes live in [AGENTS.md](AGENTS.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
