# Research: OS-level sandbox adapter for governed tool execution

Research closeout for
[#281](https://github.com/Sannrox/shikigami/issues/281), accepted 2026-09-13.
Decision record: [ADR 0013](../decisions/0013-os-sandbox-adapter.md).
Implementation Issue: [#282](https://github.com/Sannrox/shikigami/issues/282).

## Decision

Select **option 1, narrowed**: an in-process Linux adapter (`linux_native`)
that applies **Landlock** filesystem rules, a **seccomp-BPF socket gate**, and
the existing resource limits to every spawned tool child from `pre_exec`,
without user namespaces and without an external helper binary. Tool children
get **no sockets at all** (`socket()` fails for every address family;
`socketpair()` remains). The `[network]` allowlist keeps applying to the
harness's own HTTP clients. An address-level egress allowlist for tool
children is **not** achievable in-process with Landlock or seccomp alone
(evidence below) and is left to a separate decision.

Containers and micro-VMs are **deployment tiers** the host chooses around the
harness process. They are not runtime ports. The published OCI image is the
container tier; `linux_native` composes inside it unprivileged.

macOS gets a **documented weaker profile** (`seatbelt`, Apple's deprecated
`sandbox-exec` API) for development only. It is not a hosted tier.

The guiding shape is the one used by governed data platforms: policy is
decided centrally (the plane issues permits and denials), enforcement happens
at the lowest trustworthy layer closest to the effect (the kernel boundary
around the tool process), capabilities are **detected and reported, never
assumed**, secrets are never ambient in the execution environment, and the
harness never reports a control it cannot enforce. Layers compose: the host's
outer boundary plus the harness's per-tool policy.

## Evidence

All measurements were taken on 2026-09-13 with a throwaway C launcher that
applies Landlock and seccomp and then `exec`s the tool command. The launcher
and scripts are not part of this repository; the production adapter applies
the same rules from Rust `pre_exec` (no extra `exec`).

### Linux host (Ubuntu 24.04.4, kernel 6.8.0, aarch64, Landlock ABI 4)

LSM list: `lockdown,capability,landlock,yama,apparmor`;
`apparmor_restrict_unprivileged_userns=1` (distribution default).

| Scenario (`bash` child) | No sandbox | Landlock + IP socket gate | Landlock + TCP port rule 18080 | Landlock only (no socket gate) |
| --- | --- | --- | --- | --- |
| read `$HOME/.ssh/<secret>` | read | **EACCES** | **EACCES** | **EACCES** |
| read world-readable `/etc/<secret>` | read | **EACCES** | **EACCES** | **EACCES** |
| read `/run/secrets/<secret>` | read | **EACCES** | **EACCES** | **EACCES** |
| read `/etc/passwd` (allowlisted file) | read | read | read | read |
| write `/tmp/<file>` (outside workspace) | wrote | **EACCES** | **EACCES** | **EACCES** |
| write, mkdir, rmdir inside workspace | ok | ok | ok | ok |
| exec `/usr/bin/*` | ok | ok | ok | ok |
| TCP connect `127.0.0.1:18080` | 200 | **socket EPERM** | 200 | 200 |
| TCP connect `<vm address>:18080` (non-loopback listener) | 200 | **socket EPERM** | **200 (port rule is address-blind)** | 200 |
| TCP connect `127.0.0.1:9` | refused | **socket EPERM** | **connect EACCES (Landlock)** | refused |
| TCP connect `1.1.1.1:80` | 301 | **EPERM** | **EACCES** | 301 |
| UDP sendto `1.1.1.1:53` | sent | **EPERM** | **EPERM** | sent |
| TCP bind `127.0.0.1:18081` | bound | **EPERM** | **EACCES** | bound |
| `AF_UNIX` socket | ok | ok | ok | ok |

Individual files can be allowlisted (`/etc/ld.so.cache`, `/etc/passwd`,
`/etc/group`, `/etc/nsswitch.conf`, `/etc/localtime`) while the rest of `/etc`
stays unreadable, so a world-readable secret next to them is denied.

Landlock alone (last column) proves why the socket gate is required: Landlock
ABI 4 restricts TCP `bind`/`connect` only; UDP and raw sockets stay open.

The third column disproves a "loopback proxy port" design: Landlock network
rules match the **port only**, not the destination address, so permitting the
proxy port also permits any remote host listening on that port. seccomp
cannot inspect the `sockaddr` pointer either. An in-process, unprivileged
address-level allowlist for tool children therefore does not exist with these
two primitives.

### Total socket deny (`socket()` → `EPERM` for every family)

| Tool child under Landlock + total socket deny | Result |
| --- | --- |
| bash pipeline, subshell, `tar`, `gzip`, `sed`, `find`, `grep` | ok |
| `git init`, `add`, `commit`, `log` inside the workspace | ok |
| `python3` `subprocess.run` and `socket.socketpair()` | ok |
| `socket.gethostname()` | ok |
| `socket(AF_UNIX)`, `socket(AF_INET)`, `curl` | **EPERM** |
| `connect()` to a filesystem Unix socket path | **EPERM** (no socket can be created) |

Denying every family instead of only IP families also closes the residual
where a tool child reaches a runtime socket the harness user can write to
(container runtime, agent forwarding, session bus).

### Default container (`docker run` of `ubuntu:24.04`, no extra privileges)

Same launcher, same results: Landlock ABI 4 available, `/root/.secret`
unreadable, workspace writable, `/tmp` denied, TCP socket denied. The default
container seccomp profile permits the `landlock_*` and `seccomp` syscalls, so
the in-process tier composes inside an unprivileged container.

### User namespaces on the same host

| Attempt | Result |
| --- | --- |
| `bwrap --unshare-all` (unprivileged) | `loopback: Failed RTM_NEWADDR: Operation not permitted` |
| `bwrap --unshare-user --unshare-pid` (unprivileged, no netns) | `setting up uid map: Permission denied` |
| `unshare -Urn --map-root-user` | `write failed /proc/self/uid_map: Operation not permitted` |
| `sudo bwrap --unshare-all` | works; egress `Network is unreachable` |

Unprivileged user namespaces are a host policy, not a kernel guarantee: the
current Ubuntu LTS restricts them by default and default container profiles
block `CLONE_NEWUSER`. A sandbox tier that depends on them either fails on
common hosts or requires a privileged helper. Landlock and seccomp need
neither.

### macOS 26.5 (arm64), Seatbelt via `sandbox-exec`

Profile: `(deny default)` with read access to `/`, `/usr`, `/bin`, `/sbin`,
`/System`, `/Library/Preferences`, `/private/var/db`, selected `/dev` nodes,
and `/private/etc/ssl`; read/write on the canonical workspace path; network
denied.

| Scenario | No sandbox | Seatbelt, deny net |
| --- | --- | --- |
| read `$HOME/.ssh/<secret>` | read | **EPERM** |
| read `/tmp/<secret>` | read | **EPERM** |
| write `/tmp/<file>` | wrote | **EPERM** |
| write, mkdir, rmdir inside workspace | ok | ok |
| exec `/usr/bin/*` | ok | ok |
| TCP connect `127.0.0.1:18080`, `127.0.0.1:9`, `1.1.1.1:80` | 200 / refused / 301 | **EPERM** |
| UDP sendto `1.1.1.1:53` | sent | **denied** |

Findings that shape the macOS profile: the root directory itself must be
readable (`(literal "/")`) or `dyld` aborts; Seatbelt matches **canonical**
paths, so a workspace under `/tmp` must be declared as `/private/tmp/...`; and
`sandbox-exec` is a deprecated API with no availability guarantee.

### Overhead per tool spawn (`bash -c true`, 300 spawns per row)

| Host | Baseline | Sandboxed | Added |
| --- | --- | --- | --- |
| Linux VM, Landlock + socket gate (round 1 / 2) | 1.6 ms / 2.8 ms | 2.6 ms / 5.1 ms | +1.0 ms / +2.3 ms |
| Linux VM, Landlock + TCP port rule | 1.6 ms / 2.8 ms | 2.3 ms / 4.8 ms | +0.7 ms / +2.0 ms |
| Linux VM, Landlock only | 1.6 ms / 2.8 ms | 2.4 ms / 2.6 ms | +0.8 ms / −0.2 ms |
| Linux VM, `sudo bwrap --unshare-all` vs `sudo env` | 8.1 ms | 9.5 ms | +1.4 ms |
| macOS, `sandbox-exec` (round 1 / 2) | 11.2 ms / 7.5 ms | 21.0 ms / 22.9 ms | +9.8 ms / +15.4 ms |

VM timings vary by about ±1.5 ms between rounds; the prototype also pays one
extra `exec` that the `pre_exec` implementation does not. The Linux tier adds
low single-digit milliseconds per tool spawn. The **budget recorded for
#282 is ≤ 10 ms added per tool call**, measured in Linux CI against the
unsandboxed spawn in the same job.

## Portability matrix

| Host | Filesystem isolation | Tool-child sockets | Address-level child egress allowlist | Tier reported |
| --- | --- | --- | --- | --- |
| Linux ≥ 6.2 with Landlock in the LSM list (ABI ≥ 3) | Landlock incl. truncate | seccomp total socket deny | not in-process; host boundary or a later brokered-connect decision | `linux_native` |
| Linux 5.13 – 6.1 (ABI 1–2, e.g. RHEL 9, Debian 12, Ubuntu 22.04 GA kernel) | Landlock without truncate mediation | seccomp | — | tier unavailable → configuration error (use an HWE/backport kernel) |
| Linux without Landlock (older kernel or LSM list) | unavailable | seccomp only | — | tier unavailable → configuration error |
| Unprivileged OCI container on a Landlock host | Landlock (proved on kernel 6.8) | seccomp | container network policy (host-owned) | outer container boundary + `linux_native` |
| Micro-VM per run | host-owned | host-owned | host-owned | outer boundary only; harness reports its own tier |
| macOS | Seatbelt (deprecated API) | Seatbelt deny | Seatbelt address filters exist but are untested here | `seatbelt` (development only) |
| Windows | none | none | none | `none` only |

Landlock ABIs add rights over time (`REFER` at 2, `TRUNCATE` at 3, TCP port
rules at 4, device ioctl at 5, abstract-socket and signal scoping at 6). The
floor is ABI 3 because, per the kernel documentation, `truncate(2)` and
`open(O_RDONLY | O_TRUNC)` do not require `WRITE_FILE` and are only mediated
by the `TRUNCATE` right; below that a tool child could empty any file the
harness user may write. ABI 1 is otherwise stricter, not weaker (cross
directory rename and link are always denied). Above the floor the adapter
handles every right and scope the running ABI reports and gains coverage
without a configuration change; below ABI 6, seccomp denies signals to the
harness pid and the broadcast target, and doctor reports that signal scoping
is partial.

The harness reports only the tier it applied itself. It cannot verify the
outer boundary and must not claim it.

## Design constraints carried into ADR 0013

- `sandbox.backend` stays the selector; `none` remains for local development
  with a doctor warning; `rlimit` remains limits-only and is reported as such.
- Explicitly selecting a backend that the host cannot provide is a
  configuration error for every profile (matching the existing `rlimit`
  behavior on non-Unix hosts). Governed and fail-closed profiles therefore
  refuse `doctor` and `run`.
- Landlock ABI 3 is the floor for `linux_native`; ABI 1–2 hosts get a
  configuration error, not a weaker silent tier.
- `/proc` and `/sys` are not allowlisted: same-user `/proc/<pid>/environ`
  would expose the harness's own environment, including plane tokens.
- Writable paths are the workspace and one run-scoped scratch directory that
  the harness owns and exports as `TMPDIR`; both are reported by doctor.
- Kernel isolation applies to spawned tool processes (Bash foreground and
  background). In-process file tools keep the Rust path jail. MCP server
  children are not covered by this decision.
- Tool children under `linux_native` get no sockets regardless of
  `network.egress`; `[network]` keeps governing the harness's own clients. A
  deployment that needs tool-child network must delegate it explicitly to the
  host boundary (`sandbox.tool_sockets = "host"`), which doctor reports as
  unverified by the harness. Silent approximation by leaving sockets open is
  not permitted.
- An address-level allowlist for tool children needs an address-capable
  enforcement point: seccomp user-notification connect brokering (the
  supervisor validates the address and connects a duplicate of the child's
  socket itself), or a host-owned boundary (network namespace with a proxy,
  cgroup BPF, container network policy). That is a separate decision.

## Alternatives

| Option | Outcome |
| --- | --- |
| 1. In-process namespaces + seccomp + Landlock + netns proxy | **Selected without namespaces and without a proxy.** Landlock denies by default, so mount-namespace hiding adds nothing; a network namespace needs user namespaces or privileges. |
| Landlock TCP port rule scoping a loopback proxy | Rejected: port rules are address-blind; a remote listener on the proxy port was reachable in the prototype. |
| Landlock ABI 1–2 with seccomp `truncate` compensation | Rejected: would also need flag inspection of `open(O_RDONLY \| O_TRUNC)`; not worth supporting pre-6.2 kernels in the production tier. |
| 2. External helper (bubblewrap-style) per tool call | Rejected as baseline: depends on unprivileged user namespaces (blocked on Ubuntu 24.04 and in default containers), adds an external binary, and a setuid variant contradicts "no privileged helper". |
| 3. Container executor as a runtime port | Rejected as a port: the harness would depend on a container runtime in-process. The OCI image already provides the container tier and the in-process tier composes inside it. |
| 4. Micro-VM per run | Not a harness concern: the highest hosted tier is provided by the fleet host around the process. Documented in the matrix. |
| seccomp only | Cannot filter by path; filesystem isolation needs Landlock. |
| Landlock only | Leaves UDP and raw sockets open; the socket gate is required. |

## Exit result

The sandbox decision is recorded in ADR 0013 and the Design Discussion linked
from it. [#282](https://github.com/Sannrox/shikigami/issues/282) is ready:
implement `linux_native` behind the sandbox port with detection, doctor
reporting, fail-closed configuration, the total socket deny for tool children,
and a Linux CI job that proves the three denials and the overhead budget.
