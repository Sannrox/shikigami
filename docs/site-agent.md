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
| `governance.principal` | The worker's registered service principal, never an instance's `principal_id` | It lists and claims turns for every instance it serves. A turn acts as its instance only through the turn's runtime token. The plane refuses a worker that is the turn's own instance principal. |
| `governance.client_secret_env` | The worker principal's secret | A static bearer (`governance.token_env`) is refused. No credential acts as an instance outside a claimed turn. |
| `tools.mcp_gateway` | Required | Every remote tool call goes through the governed gateway with the turn's runtime token. Third-party credentials stay in the gateway. |
| `tools.mcp_servers` | Refused | No direct route to a site system. |
| `run.nested` | Refused | A child run's preset tools would reach past the gateway; a request-level nested flag is ignored too. |
| `tools.enabled` | Only `mcp.gateway.*`, `report`, `escalate` | No shell, write or filesystem tool. A tool outside the list is refused at load with its name. Only the gateway tools named here are registered, whatever else the gateway lists. |

The runtime token admits `SubmitActionInstance` and the claim's own lease
calls, not `GetObject` or `GetOperationReceipt`. Enable
`mcp.gateway.sekai.actions.submit` for a turn; the plane refuses
`sekai.objects.get` and `chisei.receipt.read` under the token until it admits
a turn-scoped read set.

## Enforced by the plane

- **Freeze.** A frozen instance admits no new wake and no new Action from its
  principal. A write already in flight is re-checked when it commits
  (sekai-chisei #1970). The runtime sees a denial from the gateway.
- **Kill switch.** A write freeze on the tenant or namespace refuses the
  Action at admission. The runtime sees a denial.
- **Approval.** An Action that needs approval parks on the plane.

## Claiming turns

`serve --intake agent-turn` claims due turns as the worker principal, loads
the instance and its published agent definition, and runs the turn with that
turn's runtime token, whose subject is the instance. Host planning (loading
the instance and definition) stays on the worker principal.

```bash
shikigami --config examples/site-agent-turn.toml serve --intake agent-turn
```

Each poll lists up to eight claimable turns and claims from a random one,
moving on when another worker won it. A poll that finds nothing waits
`--poll-ms` (200 ms), doubling to 5 s with ±20% jitter; a claimed turn resets
the wait.

The specialised `security-triage` profile ([security-triage.md](security-triage.md))
uses these same bounds and is also valid for `--intake agent-turn`.

How a turn ends decides its acknowledgement:

| Run ends with | Ack outcome | Ack fields |
| --- | --- | --- |
| `report` with `success` true or omitted | `completed` | `reason` is the report summary |
| `report` with `success` false, or an error | `failed` | `reason` is the summary or error |
| A park (`escalate`, or an Action awaiting approval) with an Action submitted under the claim | `parked` | `parked_action_instance_id` is that Action; the turn resumes from the continuation once it is decided |
| A park with no Action submitted under the claim | `failed` | `reason` names the missing Action |

A frozen or kill-switched instance is refused without a further apply; the
refusal is acknowledged on the turn. A parked Action is acknowledged `parked`
with that Action's id and resumed from the continuation once it is decided.
A park without an Action submitted under the claim is acknowledged failed.
Abrupt stop leaves the claim unacked so the next generation can reclaim it;
a granted Action is applied once by its grant, and the resumed run reads
that outcome. If the previous local run for this turn is still marked
active, the host also leaves the claim unacked instead of failing it.
