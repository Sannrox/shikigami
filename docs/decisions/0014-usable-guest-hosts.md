# ADR 0014: Usable guest process hosts (ACP, TUI, session wait, ask=park)

- Status: Accepted
- Date: 2026-10-03
- Supersedes: —
- Amends: [VISION.md](../../VISION.md) principles 1–2 (desktop UI stays a
  client; a terminal process host is in-tree and thin)
- Does not amend: [ADR 0004](0004-v1-contract.md) freeze-core
- Research: [usable-agent-guest.md](../research/usable-agent-guest.md)
- Source: [PR #331](https://github.com/Sannrox/shikigami/pull/331);
  maintainer accepted that closeout without a GitHub Design Discussion
- Related: [ADR 0003](0003-serve-daemon.md),
  [#61](https://github.com/Sannrox/shikigami/issues/61)

## Context

The 1.x harness already executes one countable **run**. Interactive
harnesses and rusui’s environment plane spawn a **guest**: session
create/load, follow-up prompts, streamed tool updates, and permission
RPCs. rusui’s only agent interface is ACP (rusui ADR 0002 / 0011). Until
this decision, ROADMAP A5 drives shikigami through `serve`.

`report` terminates a run. A guest must wait after a no-tool assistant
message. Ungoverned mutating tools execute from the allow-list with no
ask path. `http-callback` and sekai-chisei already broker hosts that have
a plane.

VISION said “UI is a client, not the runtime” and forbade a desktop
shell. A thin terminal process host is a process host, not a desktop
shell.

## Decision

1. **`shikigami acp` is a thin process host** over `Harness`, evolving
   like `shikigami mcp`, not freeze-core. Stdio, newline-delimited
   JSON-RPC (Grok/rusui). MCP Content-Length stays the MCP host.
2. **Minimum session methods:** `initialize` (loadSession, prompt,
   permission), `session/new`, `session/load` (fail closed so the caller
   may `session/new`), `session/prompt`, `session/update`,
   `session/request_permission`, `session/cancel` (existing cancel
   marker).
3. **A session is a host id over runs.** Do not invent a second agent
   object. Unattended CLI `run` still stops on `report`, park, or limit.
   ACP/TUI treat a no-tool assistant message as wait-for-next-prompt
   (`end_turn`). Follow-ups are FIFO on the same session/workspace.
4. **`shikigami tui` is this crate’s interactive host.** It is an ACP
   client of the in-process session, not a second turn loop. Dense
   transcript; permission and plan overlays; Ctrl-C cancels;
   continue-last-in-cwd. No plugin store, theming engine, voice,
   dashboard, or web view. Bare `shikigami` stays usage/help so freeze-core
   subcommands are unchanged.
5. **Ask=park.** Ungoverned mutating tools park with the same tool
   identity as approval park. Structured option labels are allowed;
   freeform `escalate` remains. `http-callback` and sekai-chisei stay
   brokered. Governed `require_approval` still parks. ACP without
   `request_permission` is rejected.
6. **Streaming is honest.** Do not invent token chunks. If the model
   adapter cannot stream, emit one `session/update` per completed model
   turn until it can. `HarnessEvent::ModelTurn` preview is not enough for
   the TUI.
7. **Plan write-jail** is an additive run mode, default off: mutating
   tools fail except writes to one harness-owned plan path; completion
   parks with the plan digest. Hosts own review chrome.
8. **Content parts** reuse `run_content` bounds. No new multimodal stack.
9. **Credentials** come from the environment (per-turn grant), same as
   CLI. No TUI login. Container guests with no network use the configured
   HTTP adapter or plane proxy; the host adds no extra binds.

Proof for ACP: deterministic fake-client tests in-tree. Live rusui stays
ignored, like `plane_live`.

## Consequences

- VISION and DESIGN list ACP and TUI as process hosts. They do not exist
  until implementation Issues land.
- Freeze-core CLI remains `version`, `doctor`, `run`, `serve`.
- Nested child runs are [ADR 0015](0015-nested-child-runs.md).
- [#330](https://github.com/Sannrox/shikigami/issues/330) is unchanged.

## Rejected alternatives

1. **Keep `serve` as the rusui path.** Weaker receipts; rusui’s interface
   is ACP.
2. **Always-approve ACP.** rusui ADR 0002 rejected that so policy can sit
   on `request_permission`.
3. **TUI talks to `Engine` directly.** Forks the rusui guest.
4. **Bare `shikigami` is the TUI.** Surprises scripts; freeze-core entry
   stays subcommands.
5. **File-backed memory / desktop shell / rewind after effects.** Chrome
   or a different product.
