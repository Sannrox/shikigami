# Security Policy

## Supported versions

Security fixes target the latest **1.x** release line and current `main`.
Older major lines, prerelease snapshots, and untagged commits do not receive
separate security support.

| Version | Supported |
| --- | --- |
| `1.x` (latest minor/patch) | Yes |
| Current `main` | Yes |
| `0.x` and older commits/snapshots | No |

## Reporting a vulnerability

Please report security vulnerabilities **privately** via GitHub's private
vulnerability reporting: open the
[Security tab](https://github.com/Sannrox/shikigami/security/advisories/new)
and click **"Report a vulnerability"**. This keeps the report confidential
until a fix is available.

Do not open public issues or pull requests for exploitable vulnerabilities.

When reporting, include:

- affected commit or version
- steps to reproduce
- expected impact
- whether credentials, local workspaces, or network exposure are involved

Do not include real credentials or unredacted sensitive data in a report.

Maintainers will use the private advisory to coordinate reproduction, impact
assessment, remediation, and disclosure.

## Security-relevant behavior

Operators and integrators should understand:

- **Tool jail.** File tools reject absolute paths and parent traversal. This is
  not a full OS sandbox; treat host compromise assumptions accordingly.
- **Bash is opt-in.** Default tool allow-lists exclude `bash`. Enabling it
  increases blast radius. Bash receives an explicitly constructed environment;
  configured harness credential variables are always removed.
- **Governance fail-closed.** Profiles with `fail_closed` or `governed` must not
  run when the plane is missing or unhealthy. If you need offline operation,
  use the `local` profile deliberately.
- **Secrets.** Use environment variables for plane tokens and model API keys.
  Never commit `.shikigami-state/`, `.env` files with secrets, or tokens in TOML.
- **Plane trust.** When using sekai-chisei, the plane is a trusted control
  boundary for model routing and policy. Secure the plane and its credentials
  separately (see sekai-chisei security docs).
- **Delivery.** Installing the binary via tenkai or other tools is out of band;
  verify release signatures and supply chain according to your delivery system.

## Safe defaults checklist

- Prefer `profile = local` for demos and CI without a plane.
- Keep `bash` out of `tools.enabled` unless required.
- For managed Bash runs, launch Shikigami with an allowlisted parent
  environment containing only variables tools require.
- Set `fail_closed = true` only when a plane is actually operated.
- Run untrusted tasks in disposable workspaces; use `--keep-workspace` only when
  you need forensic inspection.


## Threat model (tools and workspaces)

### Assets

- Host filesystem outside the run workspace
- Plane credentials and model API keys in the environment
- Integrity of governed audit trails (when using sekai-chisei)

### Adversaries / abuse cases

- Model-generated tool arguments attempting path escape (`../`, absolute paths)
- Overly large file or bash output exhausting disk/memory
- Operator misconfiguration enabling bash or disabling fail-closed governance

### Controls today

- Relative-path jail for file tools; parent and absolute paths rejected
- Default tool allow-list excludes `bash`
- Output and file size caps in the tool executor
- Explicit foreground/background Bash environments with mandatory harness
  credential and shell-startup-variable removal
- Fail-closed doctor/run for governed profiles without a healthy plane
- Secrets via env / `token_env`, not TOML

### Non-goals (v0)

- Full OS sandboxing (containers, seccomp, macOS seatbelt profiles)
- Protecting against a fully compromised host process
- Multi-tenant isolation inside one process

### Tests

Unit tests cover parent-path rejection for file tools. Escape-class coverage
should grow with property tests (see open Issues).
