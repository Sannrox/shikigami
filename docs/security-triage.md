# Security-triage site-agent profile

[`examples/security-triage.toml`](../examples/security-triage.toml) is a
specialised [site-agent](site-agent.md) runtime (#465). Chisei wakes it on
new or reopened findings and exposures. It gathers authorized context only
through the governed MCP gateway, then proposes an exposure verdict and,
where justified, a disposition as governed Actions.

It works with a local model. Cloud model routes are a plane concern while a
dark-site profile is active.

## What bounds a turn

The `site-agent` bounds apply, plus a typed verdict and the triage skill.

| Setting | Value | Why |
| --- | --- | --- |
| `profile.name` | `security-triage` | Site-agent fail-closed preset, plus the typed verdict check. |
| `governance.principal` | The instance's `principal_id` | The turn acts as the instance. |
| `tools.mcp_gateway` | Required | Catalog, delivery criticality, lineage, markings, findings, and dispositions are gateway reads. |
| `tools.enabled` | Only `mcp.gateway.*`, `report`, `escalate` | No shell, write or filesystem tool. |
| `context.skills` | `security-triage` | Procedure and report format, from [`examples/security-triage/skills`](../examples/security-triage/skills/security-triage/SKILL.md). |
| `run.nested` | Refused | A child run's preset tools would reach past the gateway. |

## Output

The only output is the final `report`. Its `summary` is one JSON object
matching `pkg:security.exposure-verdict/v1`. Measured facts (component,
verdict, reachable markings, evidence) stay out of `hypotheses`.

A finding on a component that can reach marked data must carry those
markings in `reachable_markings` and a higher `verdict_severity` than the
same finding on an isolated component.

A disposition is a governed Action. Without autonomy for that class the
Action parks for review. A successful report that claims a `recorded`
disposition without `autonomy_granted` is refused.

A successful run without a terminal `report` is blocked.

## Claiming turns

```bash
shikigami --config examples/security-triage.toml serve --intake agent-turn
```

A frozen or kill-switched instance is refused without a further apply. A
reopened disposition is one wake: the host claims the turn, runs once, and
acknowledges it.

## Tests

`tests/security_triage_profile.rs` drives the shipped settings with a
scripted model: a seeded finding on marked data ranks higher than the same
finding on an isolated component, an unreviewed recorded disposition is
refused, a disposition Action without autonomy parks, a reopened
disposition is claimed once, and a frozen instance takes no turns.
