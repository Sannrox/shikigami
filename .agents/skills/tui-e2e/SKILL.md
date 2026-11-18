---
name: tui-e2e
description: Prove terminal UI work with plain-text direction mocks, headless screen-state snapshots, a per-state key map, pseudo-terminal tests against a scripted backend, screen rules as assertions, and a crash and resume check. Use for any change to what a full-screen terminal program shows or how it reacts to keys, a terminal UI redesign, or a terminal UI Pull Request. Uses the shared Sannrox agent-kit installation.
---

# TUI E2E

Use the canonical [tui-e2e skill](https://github.com/Sannrox/agent-kit/tree/main/skills/tui-e2e).
Read the complete installed `~/.agents/agent-kit/skills/tui-e2e/SKILL.md` and the
references it requires before starting. If installed at another operator-selected location
(`$AGENT_KIT_HOME`), use that installation. This repository entrypoint contains no
workflow implementation.

If the skill is missing, run `bash scripts/validate-workflow-skills.sh` from this repository
(it clones or updates the kit), or clone it directly:
`git clone https://github.com/Sannrox/agent-kit ~/.agents/agent-kit`. Run `git pull` in that
checkout to get the latest version; consumers always use `main`.

Run the shared skill's scripts from the repository being changed, preserving its working
directory. Follow this repository's `AGENTS.md` (including `## Agent skill profile`) in addition to the
canonical skill.

Keep shared fixes and instructions upstream in `Sannrox/agent-kit`. When the stub template
changes upstream, run `bash scripts/validate-workflow-skills.sh --render`. Do not add
repository-local helpers or copies.
