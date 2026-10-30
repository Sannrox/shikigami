# Security-finder reviews

[`examples/security-finder.toml`](../examples/security-finder.toml) configures
a specialised, read-only reviewer (#464). It inspects a repo or bundle and
emits typed findings that match the governed findings package
`pkg:security.finding-exposure/v1`. It never writes, comments, or patches.

## Roles

Roles name capabilities, not a model or provider. Any configured model
adapter can run them, including a local model.

| Role | `[security_finder] role` | Default budget | Allowlist |
| --- | --- | --- | --- |
| Dependency and advisory | `dependency-advisory` | 12 turns / 600s | `read_file`, `glob`, `grep`, `report` |
| Source change | `source-change` | 24 turns / 900s | same |
| Deployment manifest and plan | `deployment-manifest` | 16 turns / 600s | same |
| Authorization boundary | `authorization-boundary` | 16 turns / 600s | same |

An enabled tool outside the role allowlist is refused at load with its name.
A call to any other tool at runtime is `tool not enabled` and recorded as a
failed `tool_end` in the run journal (`events.jsonl`).

## What bounds a run

| Setting | Value | Why |
| --- | --- | --- |
| `profile.name` | `security-finder` | Selects the role overlay and typed-report check. |
| `security_finder.role` | One role above | Own allowlist and budget. |
| `tools.mode` / `tools.enabled` | `read`; subset of the role allowlist, including `report` | No bash, writes, `web_fetch`, MCP, `escalate`, or `child_run`. |
| `run.nested` | Refused | A child run's preset tools would write. |
| `network.egress` | `allowlist` of the model gateway host | Harness-owned HTTP is the model client only. |
| `workspace.adapter` | `inplace` on the input directory | Inputs are read where they are. |
| `context.load_project_rules` | `false` | Inputs are untrusted text. |
| `context.skills` | `security-finder` | Procedure and report format, from [`examples/security-finder/skills`](../examples/security-finder/skills/security-finder/SKILL.md). |

`doctor` shows `tools.visible` as exactly the enabled role tools and no
`tools.external` line.

## Output

The only output is the final `report`. Its `summary` is one JSON object.
The harness stamps `problem_signature` from measured facts (component,
version range, finding class, advisory id) so the same problem agrees
across models. Two advisories on the same component stay distinct.
Hypotheses stay in a separate array.

A successful run without a terminal `report` is blocked, not a clean
review. The configured role is written into the system prompt.

Use findings only from a run that completed with `success = true` and
`coverage.complete = true`. An empty `findings` array with that coverage is
a clean review: the coverage summary is the proof, not silence.

## One-shot CLI and claimed turns

```bash
shikigami --config examples/security-finder.toml run \
  "Review dependencies and advisories in this workspace"
```

A claimed plane Action uses the same profile with governance `sekai-chisei`
and `serve --intake plane`. Agent-turn intake stays the site-agent profile.

## Blocked runs

A run that exhausts its budget ends with `max_turns` or `timed_out`. It has
no typed report, so it is not a clean review. Ending without a terminal
`report` is the same blocked result. A successful `report` whose JSON is
not a typed finding record, or whose coverage is incomplete, is refused as
a failed `report` call. Resuming a successful content checkpoint whose
stored summary is not a typed finding record for the configured role is
the same blocked result. Rerun with a larger budget or a smaller bundle.

## Tests

`tests/security_finder_profile.rs` drives the shipped settings with a
scripted model: a seeded vulnerable dependency and a seeded authorization
gap each yield one typed finding with evidence, a clean fixture yields no
findings and a coverage summary, role allowlists refuse and journal writes
and exec, two configured models agree on signatures, and an exhausted
budget ends blocked.
