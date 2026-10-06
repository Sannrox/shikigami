# ADR 0018: Skills and MCP are the only extension surface

- Status: Accepted
- Date: 2026-10-06
- Resolves: [#370](https://github.com/Sannrox/shikigami/issues/370)
- Does not amend: [ADR 0004](0004-v1-contract.md) freeze-core
- Related: [ADR 0014](0014-usable-guest-hosts.md) (no plugin store),
  [ADR 0016](0016-session-mode.md) (no plugin modes),
  [ADR 0019](0019-skill-context-boundaries.md)
- Research: [guest-session-surfaces.md](../research/guest-session-surfaces.md)

## Context

[#370](https://github.com/Sannrox/shikigami/issues/370) asks whether
skills plus MCP cover the tools, commands, and event hooks a project
needs in every session, or whether a separate plugin loader is
required. The usable-guest closeout already rejected a plugin
marketplace. Skill bodies are untrusted model context and are never
executed as code ([skills.md](../skills.md)).

Shikigami already has:

- skill packs (`SKILL.md` procedure text);
- MCP client servers in settings, and `shikigami mcp` as a process host;
- operator-trusted lifecycle hooks ([hooks.md](../hooks.md)), which are
  not a marketplace.

## Decision

Accept [#370](https://github.com/Sannrox/shikigami/issues/370) option 1.

1. **Skills and MCP are the extension surface.** Document that. Add no
   loader.
2. **A skill is markdown procedure.** It is model context. It is never
   code. Executable scripts next to a skill are operator-run tools the
   skill *describes*, not a third loader the harness imports.
3. **MCP is how extra tools enter the registry.** Settings
   `[[tools.mcp_servers]]` remain the operator path. A later additive
   skill-pack `mcp.json` (lazy-load servers when that pack is used) is
   compatible with this decision and is not a loader. It is not in this
   ADR's implementation scope.
4. **Hooks stay operator-configured subprocesses.** They are not a
   plugin API, not a store, and not a way to register tools or modes.
5. **No plugin API.** Do not add `registerTool`, custom mode
   registration, policy plugins, or a JS/TS module loader. Those
   conflict with ADR 0014 and with skill-pack security.

## Consequences

- Projects express tools as MCP, procedures as skills, and trusted
  subprocesses as hooks.
- A missing capability that truly cannot be an MCP server or a skill is
  a new feature Issue. None was named in this research pass.
- Progressive skill load (catalog in the prompt, load on demand) stays
  a follow-up after [ADR 0019](0019-skill-context-boundaries.md). It is
  not a loader.

## Rejected alternatives

1. **A loader is required (option 2).** No capability was named that
   skills, MCP, and existing hooks cannot express.
2. **Executable toolboxes as a first-class harness loader.** Runs
   untrusted pack files as code. Conflicts with [skills.md](../skills.md).
3. **A plugin marketplace or in-tree plugin store.** Rejected by
   ADR 0014.
