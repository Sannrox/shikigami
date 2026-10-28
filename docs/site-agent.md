# Site-agent turns

A site agent instance (sekai-chisei ADR 0136) binds a site responsibility,
such as a production line, to the agent member of a published, certified
definition revision. It acts as an org-owned principal. The plane wakes it on
a schedule or on a change to its responsibility and queues an **agent turn**
for a runtime to claim (sekai-chisei #1611).

[`examples/site-agent-turn.toml`](../examples/site-agent-turn.toml) is the
fail-closed runtime settings for those turns (#487).

## What bounds a turn

| Setting | Value | Why |
| --- | --- | --- |
| `profile.name` | `site-agent` | Selects governance `sekai-chisei` with `fail_closed`. A run refuses to start without healthy governance. |
| `governance.principal` | The instance's `principal_id` | The turn acts as the instance, never as the operator. |
| `governance.client_secret_env` | A service principal secret | A static bearer (`governance.token_env`) is refused. |
| `tools.mcp_gateway` | Required | Every remote tool call goes through the governed gateway with the run's job credential. Third-party credentials stay in the gateway. |
| `tools.mcp_servers` | Refused | No direct route to a site system. |
| `run.nested` | Refused | A child run's preset tools would reach past the gateway; a request-level nested flag is ignored too. |
| `tools.enabled` | Only `mcp.gateway.*`, `report`, `escalate` | No shell, write or filesystem tool. A tool outside the list is refused at load with its name. Only the gateway tools named here are registered, whatever else the gateway lists. |

## Enforced by the plane

- **Freeze.** A frozen instance admits no new wake and no new Action from its
  principal. A write already in flight is re-checked when it commits
  (sekai-chisei #1970). The runtime sees a denial from the gateway.
- **Kill switch.** A write freeze on the tenant or namespace refuses the
  Action at admission. The runtime sees a denial.
- **Approval.** An Action that needs approval parks on the plane.

## Not yet

Consuming agent-turn claims is #488, which needs sekai-chisei #1982. That
issue covers per-claim runtime tokens bound to the instance principal, worker
credentials scoped below root, and a `parked` turn outcome with a
continuation. Until both land, this profile configures the runtime but `serve`
does not claim turns.
