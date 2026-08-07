# Contributing

Thanks for helping improve **shikigami**. This project is at **1.0** (medium
contract; freeze-core surfaces follow semver). Focused changes with tests are
the most valuable.

## Code of conduct

Participation is governed by [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Ways to contribute

- Bug reports and reproductions (offline preferred)
- Documentation fixes and examples
- Deterministic tests
- New **adapters** behind existing ports (or clear proposals for new ports)
- Security reports via [SECURITY.md](SECURITY.md) (private)

## Discussions vs Issues

| Use | For |
| --- | --- |
| **GitHub Discussions** | Cross-boundary design, “how should we…”, long options analysis |
| **Issues** | Concrete bugs, ready features with acceptance evidence, chores |

Issue templates link a **design** Discussions category. If that category is
missing on a fork, enable Discussions and add a category named `Design` (slug
`design`) in the repository Settings → General → Features → Discussions, or
use the **Ideas** category until then.

## Development setup

```bash
git clone https://github.com/Sannrox/shikigami.git
cd shikigami
make update
make validate
make test
make test-integration
```

Requirements: the pinned Rust toolchain from [`rust-toolchain.toml`](rust-toolchain.toml).

### Offline vs live tests

| Suite | Command | Needs |
| --- | --- | --- |
| Default | `cargo test` | Nothing external |
| Live plane | `SEKAI_LIVE=1 SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051 cargo test --test plane_live -- --ignored --nocapture` | Local sekai-chisei |

### Supply chain

| Check | When | Local |
| --- | --- | --- |
| cargo audit | lockfile / Cargo.toml PRs + weekly | (CI via rustsec/audit-check) |
| cargo deny | same paths + `deny.toml` changes | `cargo deny check` |

Policy and allowed licenses live in [`deny.toml`](deny.toml). Failures should
name the crate and license/advisory so they are actionable — add an exception
only with a short comment in `deny.toml`.

**PR rule:** offline `cargo test` must pass. Do not require a control plane for
the default suite.

GitHub Actions CI must stay green. Required checks on `main`:

- Build & Test
- Rustfmt
- Clippy

## Architecture rules

Embed API expectations: [docs/embedding.md](docs/embedding.md) (freeze candidates).


Read [DESIGN.md](DESIGN.md) and
[docs/decisions/0001-ports-and-settings.md](docs/decisions/0001-ports-and-settings.md)
before changing boundaries.

1. **Ports + settings** — do not hard-wire sekai-chisei into the turn loop.
2. **No second policy brain** — do not reimplement budgets/policy in-core.
3. **No tenkai runtime config** — delivery is packaging, not process settings.
4. **Fail closed when required** — governed profiles must not silently degrade.
5. **Library-first** — keep `src/bin/shikigami.rs` thin; put logic in the library.
6. **Runs, not “a shikigami”** — naming for units of work.

Project Skills for repeated workflows live under `.agents/skills/`. Repository
agent rules: [AGENTS.md](AGENTS.md).

## Dependabot

Dependabot opens weekly cargo and GitHub Actions update PRs.

- Merge green **minor/patch** group PRs after required CI checks pass.
- Hold **major** version bumps for explicit review (breaking potential).
- Do not weaken CI or branch protection to land dependency updates.
- Prefer Dependabot groups already configured in `.github/dependabot.yml`.

## Pull requests

1. Keep the change focused (one outcome per PR).
2. Include tests for behavior changes.
3. Run `make update`, `make validate`, `make test`, and
   `make test-integration`, then include the command results in the PR
   description.
4. Describe behavior, risk, and test evidence in the PR body.
5. Link an Issue when one exists.

Commit subjects: short imperative, Conventional Commits welcome
(`feat:`, `fix:`, `docs:`, `chore:`).

Publish PR tips with GitHub-verified commits:

```bash
scripts/gh-verified-push.sh --create-branch-from origin/main --branch <topic> --sync-local
```

Maintainers prefer **squash merges** so `main` stays linear and lands Verified:

```bash
gh pr merge --squash --delete-branch
```

Avoid GitHub rebase-merge when Verified history matters.

## Documentation

User-facing docs are part of the product. Update:

- [README.md](README.md) for entry-point behavior
- [docs/](docs/) for reference material
- [CHANGELOG.md](CHANGELOG.md) for user-visible changes
- Examples under [examples/](examples/) when settings change

Doc index: [docs/README.md](docs/README.md).

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0 ([LICENSE](LICENSE)).
