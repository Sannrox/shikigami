# Presets

A preset narrows the operator config for one kind of task: a smaller tool set
and a procedure appended to the system prompt. It never widens authority.
Design: [ADR 0022](decisions/0022-narrowing-presets.md).

## Select a preset

```sh
shikigami --preset definition-work run "Model the billing domain"
shikigami --preset ./presets/triage.toml run "..."
SHIKIGAMI_PRESET=definition-work shikigami doctor
```

- A name picks a built-in preset. A value with a path separator or a `.toml`
  suffix loads a preset file.
- `code` or no selection means no preset.
- The config file cannot select a preset; the host does.
- `doctor` prints `preset: <name>@sha256:<digest>` when one is applied.

Built-in presets:

| Name | Tools | Base config needs |
| --- | --- | --- |
| `code` | the base config unchanged | nothing |
| `definition-work` | `report` and the `sekai.definitions` branch, edit, diff, impact, and proposal tools | an MCP server named `sekai` |

## Preset file

```toml
name = "triage"
description = "Read-only triage."   # for operators; not used at run time
prompt = """
Read the failing test and report the cause. Do not edit files.
"""

[tools]
enabled = ["read_file", "grep", "report", "mcp.docs.search"]
```

No other keys are accepted.

## Narrowing rules

Applying a preset is the last step of settings resolution. Each rule fails
closed with a startup error:

- Every built-in tool must be in the base config's effective tool set. The
  effective set becomes the preset's built-in tools.
- The list needs at least one built-in tool, usually `report`.
- An `mcp.<server>.<tool>` name must name a server the base config attaches.
  Only listed MCP tools are attached, and servers with none listed are not
  started.
- A listed MCP tool keeps the base authority. In-process governance allows it
  only when the base `tools.enabled` lists it.
- Every listed MCP tool must exist once its servers attach, or the run refuses
  to start and names the missing tools.
- The `security-finder` profile refuses presets. Retired `site-agent` and
  `security-triage` names fail at resolve.
- A [session mode](settings.md#sessionmodesname) can narrow the preset's tools
  further.

## Resume

The preset id (`<name>@sha256:<digest of the preset text>`) is stored in the
checkpoint. A resume must select the same preset text, or it refuses. A run
started without a preset resumes only without one.
