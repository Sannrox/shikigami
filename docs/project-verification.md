# Project verification

The [`Makefile`](../Makefile) is the deterministic command surface for local
agents, contributors, and CI. It delegates work to scripts under
[`scripts/make-targets/`](../scripts/make-targets/), so the commands are kept
in one repository-owned place.

```bash
make update             # run sorted scripts/update-*.sh scripts
make validate           # run sorted scripts/validate-*.sh scripts
make test               # unit tests
make test-integration   # integration tests
make test-e2e           # offline host proof
make all                # all-target build
```

`make embed` remains an alias for `make test-e2e`. `make update` is the only
target above that changes source files; the checks do not modify tracked files.
Live-plane tests remain opt-in and are not part of the offline targets.

The validation and update dispatchers sort matching scripts by path and fail if
the repository has no matching scripts. Adding a new deterministic check only
requires adding a `scripts/validate-*.sh` file; no agent hook is involved.
