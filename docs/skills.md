# Skill packs (runtime)

Named **procedure packs** loaded into the run system prompt.

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
max_skill_bytes = 32768
```

## Attribution

Each loaded skill logs `skill <id> digest=<sha256>` on the event stream.
Digests change when skill body changes.

## Security

Skill bodies are model context only — never executed as code. Operators control
the skills root and which ids are listed.

`shikigami tui` offers `/skill:name` from `skills_root` and
`.agents/skills`. Configured `context.skills` is an allow-list; when it is
empty the TUI lists packs that exist on disk. `/skill:name` sends the pack
body as the prompt. Configured ids still load into the system prompt as above.
