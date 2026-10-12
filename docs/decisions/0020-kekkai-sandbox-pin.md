# ADR 0020: Pin kekkai for OS sandbox enforcement

- Status: Accepted
- Date: 2026-10-12
- Resolves: [#439](https://github.com/Sannrox/shikigami/issues/439)
- Amends: [ADR 0013](0013-os-sandbox-adapter.md) (settings and doctor contract unchanged)
- Related: [kekkai ADR 0002](https://github.com/Sannrox/kekkai/blob/v0.1.0/docs/decisions/0002-reusable-process-sandbox-library.md)

## Context

ADR 0013 put Landlock, seccomp, and rlimits in `shikigami-engine`. kako
needs the same boundary. Copying the code would fork security-critical
enforcement. kekkai is that shared library; `v0.1.0` is the extraction
plus later host-capability work.

## Decision

1. **Enforcement is kekkai.** `shikigami-engine` deletes its in-tree
   Landlock/seccomp implementation and depends on a pinned kekkai tag
   (`v0.1.0`, commit `73e24f580ed6223b7e8395793e6f2317eb8edfd9`).
2. **Settings stay Shikigami's.** `sandbox.backend`, CLI, and doctor
   lines keep their names and byte-stable output. The engine maps them
   onto kekkai `Settings` (`none` / `rlimit` / `linux_native`,
   `Network::Deny`, `read_only_paths` → `readable_roots`).
3. **No behavior change and no kekkai extras.** Do not adopt cgroup
   limits, guest/userspace, or kekkai's doctor wording. A sandbox
   policy change still needs a Shikigami settings change; a kernel
   enforcement fix needs a kekkai release plus a pin bump here.

## Consequences

- Denial tests, including `linux_native_sandbox`, stay in this
  repository and must keep passing.
- Security fixes in the sandbox land in kekkai first, then this pin.
- The kekkai git pin is a parallel-lane collision surface.

## Rejected alternatives

- **Copy the sandbox into kako.** Drifts from Shikigami's hardened
  tests.
- **Keep an in-tree implementation and also depend on kekkai.** Two
  sources of truth for the same policy.
- **Wait to opt into kekkai cgroup limits.** Out of scope for #439;
  Shikigami can adopt them later without changing settings names.
