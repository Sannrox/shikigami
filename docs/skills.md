# Skill packs (runtime)

Named **procedure packs** loaded into the run system prompt. Not the same as
repository contributor Skills under `.agents/skills/`.

## Layout

```text
<workspace>/.shikigami/skills/<id>/SKILL.md
```

Or set `context.skills_root` to another directory (workspace-relative or absolute).

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
