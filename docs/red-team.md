# Red-team probe author

[`examples/red-team.toml`](../examples/red-team.toml) configures a run that
keeps an isolation probe catalogue current (#466). It reads an RPC surface
diff (proto changes) and past security bug reports, and proposes declarative
probe specs in the `sekai.probe-spec/v1` format that sekai-chisei's guarded
probe runner executes. The run never calls the target: it has no RPC client
and no route to a deployment.

## What bounds the run

| Setting | Value | Why |
| --- | --- | --- |
| `tools.mode` / `tools.enabled` | `read`; `read_file`, `glob`, `grep`, `report` | No bash, writes, `web_fetch`, MCP, `child_run` or `escalate`. |
| `network.egress` | `allowlist` of the model gateway host | Harness-owned HTTP is the model client only. Never list a probe target. |
| `workspace.adapter` | `inplace` on the input directory | Inputs are read where they are; nothing can write them. |
| `context.load_project_rules` | `false` | Inputs are untrusted text; a rules file among them is not loaded. |
| `context.skills` | `red-team` | The procedure and spec format, from [`examples/red-team/skills`](../examples/red-team/skills/red-team/SKILL.md). |
| `run.max_turns` / `run.timeout_secs` | `16` / `900` | The run budget. |

`doctor` shows `tools.visible` as exactly the four tools above and no
`tools.external` line. A call to any other tool is refused as
`tool not enabled` and recorded as a failed `tool_end` in the run journal
(`events.jsonl`).

## Output

The only output is the final `report`. Its `summary` is one JSON object:

```json
{
  "specs": [
    {
      "schema": "sekai.probe-spec/v1",
      "id": "list-object-comments-anonymous",
      "as": "anonymous",
      "call": {"rpc": "ListObjectComments"},
      "expect": {"codes": ["Unauthenticated", "PermissionDenied"]}
    }
  ],
  "unsupported": [
    {"rpc": "SekaiService.AddObjectComment", "why": "a write; probe specs are read-only"}
  ]
}
```

`unsupported` names added RPCs the spec format cannot express yet, so the
format can be extended for them.

Use the specs only from a run that completed with `success = true`. Write
each spec to its own `*.json` file and pass the directory to the guarded
runner with `--specs`; it validates the whole set and refuses the run on any
invalid spec before making a call.

## Blocked runs

A run that exhausts its budget ends with `max_turns` or `timed_out`. It has no
report, so it carries no specs: a blocked run never looks like a clean run
with nothing found. Rerun with a larger budget or a smaller diff.

## Tests

`tests/red_team_profile.rs` drives the shipped settings with a scripted model:
a fixture diff adding an RPC yields valid specs for it in one run, the tool
allowlist refuses and journals network, exec, write and nesting attempts, and
an exhausted budget ends blocked. A run against a real model is separate
evidence.
