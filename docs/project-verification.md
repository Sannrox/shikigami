# Project verification

`shikigami-project` is the executable project policy for local agents,
contributors, and CI. It selects checks from changed paths and emits the same
evidence regardless of which caller starts it.

## Commands

The repository wrapper is the shortest entrypoint:

```text
./scripts/verify.sh plan [--changed|--all] [--base REF] [--check CHECK,...] [--json]
./scripts/verify.sh verify [--changed|--all] [--base REF] [--check CHECK,...] [--json]
```

`--changed` is the default. It includes unstaged changes, staged changes, and
untracked files. `--base REF` additionally includes the committed diff from
`REF...HEAD`; pass it explicitly when checking a branch against a known base.
`--all` selects the complete project gate and cannot be combined with
`--base`. `--check` overrides path routing and accepts a comma-separated list.

`plan` never runs checks. `verify` runs them sequentially and returns:

| Exit code | Meaning |
| --- | --- |
| `0` | Every selected check passed, or no checks were selected. |
| `1` | At least one selected check failed. |
| `2` | The command or change-set request was invalid, or Git could not be queried. |

The verifier does not change tracked files. Cargo may update its normal build
cache under `target/`.

The repository Codex Stop hook invokes `verify --all --json` automatically.
This is a deterministic completion gate; it does not replace the required
agent-assisted `autoreview` for non-trivial changes.

## Change routing

| Changed path | Selected checks |
| --- | --- |
| Markdown, `docs/`, `.agents/`, `AGENTS.md`, `CONTRIBUTING.md` | Local Markdown link validation |
| Rust, Cargo, toolchain, scripts, CI, Docker, or unknown paths | `fmt`, `build`, `test`, `clippy` |
| No changes | No checks unless `--check` or `--all` is supplied |

The full gate contains:

- `fmt`: `cargo fmt --all -- --check`
- `docs`: deterministic local Markdown link validation
- `build`: `cargo build --locked --all-targets`
- `test`: `cargo test --locked`
- `clippy`: `cargo clippy --all-targets --locked -- -D warnings`
- `embed`: `cargo run --locked --example embed_smoke`

Supply-chain checks (`cargo audit` / `cargo deny`) and OCI image checks remain
separate because they require optional tools or Docker; their workflows remain
the authoritative gates for those surfaces.

The path mapper is intentionally conservative for unknown files. Semantic
design, architecture, and structured review remain human or agent judgment;
they are not disguised as deterministic checks.

## Output

Human output goes to stdout, with command diagnostics retaining their normal
stderr behavior. Add `--json` for one report with schema version `1`:

```bash
./scripts/verify.sh verify --changed --base origin/main --json > /tmp/shikigami-verification.json
```

The report records the resolved `HEAD`, changed files, requested checks, each
command, status, exit code, duration, and bounded command output. Captured
stdout and stderr are limited to the final 4,000 bytes per check so a failing
compiler cannot make the evidence artifact unbounded.

## Typical invocations

```bash
# Inspect the checks an agent will need for its working tree.
./scripts/verify.sh plan

# Inspect a topic branch against main without running tests.
./scripts/verify.sh plan --base origin/main --json

# Run the smallest appropriate local gate.
./scripts/verify.sh verify

# Run the complete pre-commit or CI gate.
./scripts/verify.sh verify --all

# Run only documentation and formatting checks after an instruction-file edit.
./scripts/verify.sh verify --check docs,fmt --json

# Keep a machine-readable result for a handoff or PR description.
./scripts/verify.sh verify --all --json > /tmp/shikigami-verification.json
```
