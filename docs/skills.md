# Skill packs (runtime)

Named **procedure packs** the agent loads when a task needs them.

## Layout

```text
<workspace>/.shikigami/skills/<id>/SKILL.md
<workspace>/.agents/skills/<id>/SKILL.md
```

Or set `context.skills_root` to another directory (workspace-relative or absolute).
`/skill:name` in the TUI searches `skills_root` first, then `.agents/skills`.
A runtime pack with the same id wins.

## Settings

```toml
[context]
skills_root = ".shikigami/skills"   # optional
skills = ["rust-style", "pr-checklist"]
eager_skills = ["pr-checklist"]     # optional
max_skill_bytes = 32768
```

`skills` is the allow-list. The run prompt lists each pack's id and one-line
description: the `description:` field of YAML front matter, else the first
non-empty line. The agent calls `load_skill` with an id to read the body,
which arrives as a tool result that compaction can later cut. Only ids in
`skills` load.

Packs also listed in `eager_skills` go into the system prompt whole, from
the first turn. Use it for a procedure that must always be present.

`load_skill` is in the default tool sets. When `tools.enabled` leaves it out,
every listed pack goes into the system prompt, as before.

## Attribution

Each skill logs `skill <id> digest=<sha256>` on the event stream: at run
start for eager packs, at the first `load_skill` for the rest. A resumed
run logs again, at its start or its first load.
Digests change when skill body changes.

## Security

Skill bodies are model context only — never executed as code. Operators control
the skills root and which ids are listed. Skills plus MCP are the extension
surface; there is no plugin loader
([ADR 0018](decisions/0018-skills-mcp-extension.md)).

`shikigami tui` offers `/skill:name` from `skills_root` and
`.agents/skills`. Configured `context.skills` is an allow-list; when it is
empty the TUI lists packs that exist on disk. `/skill:name` sends the pack
body as the prompt. Configured ids still reach the run as above.

## Context boundaries

A skill pack cannot declare a compaction or reset seam
([ADR 0019](decisions/0019-skill-context-boundaries.md)). Untrusted skill
text must not drop the task, policy, or authority limits.

Procedures that need a clean window are a new run or session. The host
starts a fresh session and may pass a `handoff` brief as the first prompt.
Nested `child_run` isolates a subtask inside one session. Host compact stays
operator-owned: `compact_after_messages`, TUI `/compact`, ACP
`session/compact`.
