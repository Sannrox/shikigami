# Project verification

The [`Makefile`](../Makefile) is the deterministic command surface for local
agents, contributors, and CI.

```bash
make update             # apply formatting updates
make validate           # check Markdown links, formatting, and Clippy
make test               # unit tests
make test-integration   # integration tests
make embed              # offline host proof
make all                # all-target build
```

Use the same targets locally and in CI. `make update` is the only target above
that changes source files; the checks do not modify tracked files. Live-plane
tests remain opt-in and are not part of the offline targets.
