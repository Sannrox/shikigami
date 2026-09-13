# ADR 0013: OS-level sandbox tiers for governed tool execution

- Status: Accepted
- Date: 2026-09-13
- Resolves: [Issue #281](https://github.com/Sannrox/shikigami/issues/281)
- Discussion: [#285](https://github.com/Sannrox/shikigami/discussions/285)
- Research: [281-os-sandbox-adapter.md](../research/281-os-sandbox-adapter.md)
- Depends on: [ADR 0001](0001-ports-and-settings.md)

## Context

Tool isolation today is a Rust path jail for in-process file tools plus an
optional `rlimit` backend for Bash children. Bash can read any file the
harness user can read, including plane tokens through `/proc`, and can open
any socket; [network.md](../network.md) and [settings.md](../settings.md) say
so explicitly. Hosted and multi-tenant operation needs a boundary that
enforces what the plane decided, on hosts whose kernel features differ and
must be detected rather than assumed.

The research closeout measured four options on Linux, inside a default
container, and on macOS. Unprivileged user namespaces are blocked by the
current Ubuntu LTS default and by default container profiles; Landlock and
seccomp are available unprivileged in both places. Neither primitive can
filter by destination address: Landlock TCP rules match ports only (a remote
listener on the permitted port was reachable in the prototype) and seccomp
cannot read a `sockaddr`.

## Decision

1. **The sandbox stays a port selected by `sandbox.backend`.** `none` remains
   for local development and produces a doctor warning. `rlimit` remains
   limits-only. Two isolation tiers are added: `linux_native` (production) and
   `seatbelt` (macOS development only). Selecting a backend the host cannot
   provide is a configuration error under every profile; `governed` and
   `fail_closed` therefore refuse both `doctor` and `run`.
2. **`linux_native` is in-process kernel enforcement applied in `pre_exec`** of
   every spawned tool child (Bash foreground and background), with no user
   namespaces and no external helper:
   - **Landlock** ruleset handling every filesystem right the running kernel
     ABI supports, with **ABI 3 (kernel 6.2) as the floor**: below it,
     `truncate(2)` and `open(O_RDONLY | O_TRUNC)` are not mediated, so a tool
     child could empty any file the harness user may write. On ABI 6 and
     newer the ruleset also scopes signals and abstract Unix sockets to the
     sandbox domain. Read and execute are allowed for a fixed system allowlist
     (`/usr`, `/bin`, `/sbin`, `/lib`, `/lib64`, `/lib32`, `/etc/alternatives`,
     `/etc/ssl/certs`, and the individual files `/etc/ld.so.cache`,
     `/etc/passwd`, `/etc/group`, `/etc/nsswitch.conf`, `/etc/localtime`)
     plus operator-declared additive `sandbox.read_only_paths` (absolute,
     existing, canonicalized). Read and write are allowed for `/dev/null`,
     `/dev/zero`, `/dev/urandom`, `/dev/random`, the canonical workspace, and
     one run-scoped scratch directory the harness owns and exports as
     `TMPDIR`. Nothing else is reachable; `/proc`, `/sys`, `$HOME`, `/etc`
     secrets, and `/run/secrets` are denied by default.
   - **seccomp-BPF socket gate.** Tool children get no sockets: `socket()`
     fails with `EPERM` for every address family; `socketpair()` remains, so
     pipelines, `git`, and subprocess-based tooling keep working. This holds
     for `network.egress = deny` and `allowlist` alike; the `[network]`
     allowlist governs the harness's own HTTP clients, not tool children,
     because no unprivileged in-process primitive can enforce a destination
     allowlist. A deployment whose tool children need network must delegate
     it explicitly with `sandbox.tool_sockets = "host"`, which removes the
     gate, keeps the filesystem rules, and makes doctor report tool-child
     egress as **host-enforced, unverified by the harness**. The default is
     `deny`.
   - **Signals.** Landlock already blocks `ptrace` outside the domain. Below
     ABI 6 the seccomp filter additionally denies signal syscalls whose target
     is the harness pid or the broadcast target `-1`; signals to the child's
     own process group stay allowed. Full signal isolation from other
     same-user processes needs ABI 6 or a dedicated service user, and doctor
     says which applies.
   - **Resource limits and process group** from the existing `[sandbox]`
     fields, `PR_SET_NO_NEW_PRIVS`, and `setpgid` for group kill.
3. **Capabilities are probed, not assumed.** The adapter probes Landlock ABI
   and seccomp at construction, refuses ABI 1–2, and handles every filesystem
   right and scope the running ABI reports. `doctor` reports the effective
   tier (`none`, `limits`, `linux_native` with ABI, tool-socket mode, and
   signal scoping, or `seatbelt`) and the writable set. The harness reports
   only what it applied itself; it never claims an outer container or VM
   boundary.
4. **Deployment tiers are host-owned.** Containers (the published OCI image)
   and micro-VMs remain the host's outer boundary. They are not runtime ports
   and never appear in harness settings. `linux_native` composes inside an
   unprivileged container.
5. **`seatbelt` is a documented weaker profile** built on Apple's deprecated
   `sandbox-exec`: deny-default reads with a system allowlist including the
   root directory, canonical workspace read/write, network denied. It is for
   development on macOS and is never a hosted tier. Windows supports `none`
   only.
6. **Scope.** Kernel isolation covers spawned tool processes. In-process file
   tools keep the Rust path jail. MCP server children keep the reconstructed
   environment from settings v1 and are outside this decision.
7. **Sequencing.** [#282](https://github.com/Sannrox/shikigami/issues/282)
   delivers `linux_native` with detection, doctor, fail-closed configuration,
   the total socket deny, and a Linux CI job proving the three denials (host
   secret read, denied socket, write outside the writable set) plus the
   overhead budget of **≤ 10 ms added per tool call**. `tool_sockets = "host"`
   and `seatbelt` are subsequent additive deliverables. An address-level
   egress allowlist for tool children is a separate decision; its candidates
   are seccomp user-notification connect brokering (the supervisor validates
   the destination and connects a duplicate of the child's socket itself) or
   a host-owned boundary (network namespace with a proxy, cgroup BPF, or
   container network policy).

## Consequences

- Governed hosts get a kernel boundary that enforces plane denials without
  privileges, helpers, or user namespaces, and that works inside default
  containers.
- Existing `rlimit` configurations remain valid in 1.x; doctor labels them
  `limits` (no OS isolation). Examples and the governed recipe move to
  `linux_native`.
- Tool children lose ambient reads of the host: toolchains outside the system
  allowlist must be declared through `sandbox.read_only_paths`, and temporary
  files land in the run scratch directory.
- Tool children lose network entirely under the default; package fetches and
  remote calls from Bash need either an explicit host-boundary delegation or
  a later brokered-connect tier. The harness never reports an allowlist it
  cannot enforce.
- The settings surface grows additively (`backend` variants,
  `read_only_paths`, `tool_sockets`); no existing field changes meaning.

## Rejected alternatives

- **User namespaces / bubblewrap-style helper as the baseline.** Blocked by
  default on the current Ubuntu LTS and in default containers; needs an
  external or setuid binary; mount hiding adds nothing once Landlock denies
  by default.
- **Container executor as a runtime port.** Couples the library to a container
  runtime; the OCI image already is the container tier.
- **Micro-VM per run inside the harness.** Belongs to the fleet host.
- **seccomp only** (no path filtering) and **Landlock only** (UDP and raw
  sockets remain open).
- **Loopback egress proxy scoped by a Landlock TCP port rule.** Port rules
  are address-blind; a remote listener on the proxy port bypasses the proxy.
- **Accepting Landlock ABI 1–2 with a seccomp `truncate` filter.** Would also
  have to reject `open(O_RDONLY | O_TRUNC)` by flag inspection; two filters
  emulating one missing right is not worth supporting kernels older than
  6.2 in the production tier.
- **Denying only IP socket families.** Leaves runtime sockets the harness
  user can write to (container runtime, agent forwarding, session bus)
  reachable from tool children.
- **Allowing `/proc` for convenience.** Exposes the harness environment.
- **Approximating an allowlist by leaving sockets open.** Violates fail
  closed.
