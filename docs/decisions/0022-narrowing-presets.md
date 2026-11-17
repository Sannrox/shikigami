# ADR 0022: Presets narrow the operator config

- Status: Accepted
- Date: 2026-11-17
- Resolves design for: [#522](https://github.com/Sannrox/shikigami/issues/522)
- Keeps: [ADR 0016](0016-session-mode.md) session modes, [ADR 0018](0018-skills-mcp-extension.md) extension surface

## Context

A domain task (for example modeling a domain on a definition branch) needs a
small tool set and a procedure. Today an operator copies a whole config file
per domain, including governance, model, MCP servers, and credentials. Each
copy can drift from the base and can quietly widen authority.

The base config already owns the environment: models, servers, credentials,
governance, sandbox, network, hooks, and run bounds. What differs per domain
is which tools the model sees and what it is told.

## Decision

1. **A preset only narrows.** A preset has `name`, `description`, `prompt`,
   and `tools.enabled`, nothing else (`deny_unknown_fields`). It cannot set
   governance, model, MCP servers or gateway, sandbox, network, workspace,
   hooks, run bounds, profile, or skills. Its procedure goes in `prompt`,
   appended to the system prompt as a `# Preset` section.
2. **Narrowing is checked at resolution, fail closed.** Applying a preset is
   the last step of settings resolution, after the file, profile, env, and
   host model override.
   - A built-in tool name must be in the base config's effective enabled
     set. The effective set becomes the preset's list.
   - An `mcp.<server>.<tool>` name must name a server the base config
     attaches. Only listed MCP tools are attached; servers with none listed
     are not started. A listed MCP tool keeps the base authority: in-process
     governance allows it only when the base `tools.enabled` lists it, and
     governed runs leave the decision to the plane as before.
   - Every listed MCP tool must exist once its servers attach, or the run
     refuses to start and names the missing tools. A preset never runs with a
     silently partial catalog.
   - An unknown name, unreadable file, unknown key, or a tool list with no
     built-in tool is a startup error. Profiles with fixed roles
     (`site-agent`, `security-finder`, `security-triage`) refuse presets.
3. **Selection is a host choice.** `--preset <name|path>` (or
   `SHIKIGAMI_PRESET`) selects it; a config file cannot. A name picks a
   preset compiled into the binary; a value with a path separator or a
   `.toml` suffix loads an operator file. `code` and no selection mean no
   preset: the run behaves as before.
4. **Recorded and checked on resume.** The preset id is
   `<name>@sha256:<digest of the preset text>`. It is stored in the
   checkpoint and logged at run start. A resume, including a takeover on
   another instance, must resolve the same id or it refuses to resume. A
   checkpoint without an id resumes only without a preset.
5. **Session modes keep working.** A session mode's `tools` still intersect
   the effective set, so a mode can narrow a preset further, never widen it.

## Consequences

- Shipped presets: `definition-work` (definition-branch tools plus `report`,
  with the proposal procedure as its prompt). `code` is the implicit default.
- An operator enables a domain by adding its MCP server to the base config
  once, then selects the preset per run.
- The checkpoint gains an optional `preset` field. Older checkpoints load;
  older binaries ignore the field.
- Adding a preset is a data change: a new TOML file and one table entry.

## Rejected alternatives

- **Presets as full config files.** Every preset would repeat governance,
  servers, and credentials, which is the duplication this removes, and a
  preset could widen authority.
- **Presets that ship skill packs.** It needs a second skill source beside
  the workspace packs. A short procedure fits the `prompt` field; longer
  procedures stay workspace skill packs the base config lists.
- **Presets setting the session mode.** ADR 0016 names modes by effort, not
  domain.
- **A preset key in the config file.** Repository content could then select
  authority; the host chooses instead, like the model override.
