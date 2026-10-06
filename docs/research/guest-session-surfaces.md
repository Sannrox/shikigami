# Guest session surfaces (mode, attachments, extension, skill seams)

- Status: **Accepted**
- Date: 2026-10-06
- Contract: [ADR 0016](../decisions/0016-session-mode.md),
  [ADR 0017](../decisions/0017-acp-attachments.md),
  [ADR 0018](../decisions/0018-skills-mcp-extension.md),
  [ADR 0019](../decisions/0019-skill-context-boundaries.md)
- Source: research [#368](https://github.com/Sannrox/shikigami/issues/368),
  [#369](https://github.com/Sannrox/shikigami/issues/369),
  [#370](https://github.com/Sannrox/shikigami/issues/370),
  [#330](https://github.com/Sannrox/shikigami/issues/330)
- Related: [usable-agent-guest.md](usable-agent-guest.md),
  [ADR 0006](../decisions/0006-bounded-content-parts.md),
  [ADR 0014](../decisions/0014-usable-guest-hosts.md)

## Decision questions

1. Should an ACP session carry a mode, fixed at the first prompt, that
   selects model, effort, prompt, and tools? (#368)
2. Should `session/prompt` accept image, PDF, audio, or video
   attachments? (#369)
3. Do skills plus MCP cover extension, or is a plugin loader required?
   (#370)
4. Should a skill pack declare a compaction or reset seam? (#330)

## Recommendations

| Issue | Option | Follow-up |
| --- | --- | --- |
| #368 | 1 + 3: small named catalog; mapping in settings; frozen after first prompt | [#372](https://github.com/Sannrox/shikigami/issues/372) |
| #369 | 1: prompt-carried attachments, fail closed, ADR 0006 custody | Feature Issue after this closeout |
| #370 | 1: skills + MCP only; no loader | None |
| #330 | 1: no skill-level boundary; host-split / `handoff` / host compact | Progressive skill load later; overflow compact is host policy |

Copy **session mechanics** that interactive coding guests already use.
Do not copy chrome: no plugin store, no plugin-registered modes, no
image generation, no media store.

## Evidence

### Session mode (#368)

`shikigami acp` `session/new` takes a workspace `cwd` and returns a
session id. There is no mode field. One settings-selected model serves
every session.

Interactive guests expose a small named dial (`low` / `medium` /
`high` / `ultra` analog). The name is what the caller picks. The
product maps each name to a model, reasoning effort, system prompt, and
tool set, and retunes that mapping as models improve. The thread keeps
the mode chosen on the first message. Changing it later would rewrite
prompt and tools (busting the prompt cache) and continue a conversation
another model started.

Operator retune of the mapping, without a second loader, is ordinary
settings. Plugin-registered extra modes are a loader and conflict with
[ADR 0014](../decisions/0014-usable-guest-hosts.md).

Mid-session switch is the wrong contract for #372: that Issue already
requires later `session/prompt` to refuse a different mode.

### Attachments (#369)

Today the ACP host concatenates `text` prompt parts and advertises
`promptCapabilities.image|audio|embeddedContext = false`
([acp.md](../acp.md)). Library `run_content` already accepts bounded
image, audio, and document parts under [ADR 0006](../decisions/0006-bounded-content-parts.md).

Interactive guests attach files to the thread (composer, paste, drop).
Attachments stay private to the thread and are not copied into the
repository. Types the model can inspect (image, PDF, audio, video) go
to the model; other types are pulled into the workspace only when a
tool needs a file. First-party image **generation** is a different
product and is already out of scope (ADR 0014).

Option 1 is that pattern on ACP: pass through what the selected adapter
can read, fail closed otherwise, keep payload custody on the host.
Option 2 (workspace mention only) remains a host action, not the ACP
contract.

### Skills and MCP (#370)

Shikigami already has skill packs, an MCP client, `shikigami mcp`, and
operator-trusted hooks. Skills are markdown procedure; they are never
executed as code.

Interactive guests add two further loaders in some products: executable
"toolbox" scripts imported as tools, and a language plugin API
(`registerTool`, custom modes, policy hooks, UI). Those run code in the
user's environment. That conflicts with skill-pack security and with
ADR 0014's "no plugin store."

Skill-bundled lazy MCP (`mcp.json` next to `SKILL.md`) is still MCP,
not a loader. It is compatible later as an additive pack capability.
Hooks already cover trusted subprocesses at run/tool boundaries.

No capability was named that skills, MCP, and hooks cannot express.

### Skill-declared context boundaries (#330)

Skill formats in the field are instructions (and optionally MCP). They
do not declare compaction seams. Untrusted skill text must not drop
task or policy.

Host compact is already in shikigami: `compact_after_messages`, TUI
`/compact`, ACP `session/compact`. `handoff` writes a brief a host may
pass as the first prompt of a fresh session. Nested `child_run`
isolates a subtask. Options 2–4 add a skill-format and run-loop change
that those host paths already cover.

**Overflow compact.** Interactive guests compact when the context
window is nearly full, in addition to a user compact command. Some
earlier designs replaced user compact with a new-thread brief, then
brought automatic compact back. Shikigami already has both host compact
and `handoff`. Compact-on-context-length-error is the same *kind* of
host policy as `compact_after_messages`. It is not a skill-declared
seam. A later settings change may add it; #330 does not.

## Alternatives (summary)

| Option | Decision | Reason |
| --- | --- | --- |
| #368 option 2 (no mode) | Rejected | Vendor model ids on the guest surface; no stable name |
| Mid-session mode switch | Rejected | Cache + model continuation |
| Plugin modes / marketplace | Rejected | ADR 0014 / ADR 0018 |
| #369 option 2 only | Rejected as the ACP contract | Workspace drop stays a host action |
| Silent drop of unsupported types | Rejected | Fail closed |
| #370 option 2 (loader) | Rejected | No missing capability; conflicts with no-plugin-store |
| #330 options 2–4 | Rejected | Untrusted text must not reset history |

## Sequencing

- This closeout is documentation. No runtime change.
- [#372](https://github.com/Sannrox/shikigami/issues/372) implements
  ADR 0016 on the ACP host.
- A follow-up feature implements ADR 0017.
- Skill-bundled `mcp.json` and compact-on-context-length-error stay
  optional later settings/pack work, not loaders and not skill seams.
