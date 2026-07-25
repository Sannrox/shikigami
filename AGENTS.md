# Repository Guidelines

## Project Structure & Ownership

`shikigami` is a Rust 2024 local-first headless agent harness. It owns run
lifecycle, local workspaces, tool execution, and evidence harvest adapters.
`sekai-chisei` owns durable operational facts and governance decisions.
`tenkai` owns delivery of the harness as a product.

Source layout:

- `src/lib.rs` — application core
- `src/bin/shikigamictl.rs` — embedded CLI host
- `tests/` — CLI and integration smoke tests
- `docs/` — contracts and decisions
- `DESIGN.md` / `VISION.md` — product boundary

Keep domain logic in the library. Treat CLI (and future daemon/UI adapters) as
hosts around shared contracts.

## Build and test

```bash
cargo fmt --check
cargo test
cargo build --all-targets
cargo clippy --all-targets -- -D warnings   # when clippy is available
```

Useful local commands:

```bash
cargo run --bin shikigamictl -- version
cargo run --bin shikigamictl -- init
cargo run --bin shikigamictl -- doctor
```

## Architecture policy

- Do not implement a second policy or budget brain inside this repo
- Do not store operational truth only in `.shikigami-state`
- Prefer versioned contracts for control-plane and tenkai integration
- Prefer failing closed over silent degradation when governance is required
- Individual work units are **runs**, not “shikigami”

## Naming

| Term | Meaning |
| --- | --- |
| shikigami | This product / harness |
| shikigamictl | CLI binary |
| run | One unit of agent work |
| sekai-chisei | Control plane peer |
| tenkai | Delivery peer |

## Ontology

When a portable ontology database exists in this repo, use the `sekai` CLI and
the sekai-ontology skill. Do not invent structural facts the ontology does not
contain.
