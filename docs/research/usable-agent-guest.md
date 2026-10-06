# Usable agent guest (ACP, TUI, loop copies)

- Status: **Accepted**
- Date: 2026-10-03
- Contract: [ADR 0014](../decisions/0014-usable-guest-hosts.md),
  [ADR 0015](../decisions/0015-nested-child-runs.md)
- Source: [PR #331](https://github.com/Sannrox/shikigami/pull/331)
- Related: research [#90](https://github.com/Sannrox/shikigami/issues/90)
  (host-owned fan-out for 1.0),
  [#61](https://github.com/Sannrox/shikigami/issues/61) (permission modes),
  [#330](https://github.com/Sannrox/shikigami/issues/330) (skill-declared
  context boundaries)

## Decision question

What must shikigami copy from interactive harnesses (Claude Code, Codex,
Grok Build) so it is a **valid guest to spawn** — CLI, MCP, an ACP guest
for rusui, and a thin in-crate TUI — without becoming those products?

## Recommendation

Copy **loop mechanisms**. Do not copy chrome.

1. Add an evolving ACP process host (`shikigami acp`, newline-delimited
   JSON-RPC) over `Harness`, the same rank as `shikigami mcp`. This is
   the rusui P1 guest contract (rusui ADR 0002 / 0011). `serve` stays the
   interim family path until that host exists.
2. Amend VISION so a **terminal process host** is in-tree and thin.
   Desktop shell stays a non-goal. The TUI is this crate’s interactive
   host and must speak the ACP session protocol internally.
3. Treat an ACP/TUI **session** as a host id over **runs**. A no-tool
   assistant message waits for the next prompt (`end_turn`). `report`
   still terminates unattended `run`.
4. Make ungoverned mutating tools **ask=park** (structured options
   allowed). `http-callback` and sekai-chisei stay the brokered paths.
5. After ACP slice 1, Design-Discuss nested **child runs** (typed
   explore / plan-jail / full). [#90](https://github.com/Sannrox/shikigami/issues/90)
   kept fan-out host-owned for 1.0; that deferral still holds until the
   Discussion accepts an in-harness tool. rusui ADR 0040 defers
   *environment-plane* child sessions; in-guest children are a different
   object.
6. Plan write-jail is a harness invariant (deny writes except one plan
   path, then park). Host-split runs cannot enforce it.
7. Keep [#330](https://github.com/Sannrox/shikigami/issues/330). After it
   closes, progressive skill load. Same research pass should cover
   compact-on-context-length-error, not only `compact_after_messages`.

Do **not** copy: desktop shells, plugin marketplaces, vector memory,
peer agent-teams, first-party browser or image generation, rewind after
effects, LSP/git builtins, personas, or replacing runs with a chat
object.

## Evidence

| Question | Evidence | Conclusion |
| --- | --- | --- |
| Is the turn loop already a harness? | v1.1.1: jailed tools, MCP client/server, skills, compaction, hooks, linux_native sandbox, park/resume, serve + plane claim, eval, replay, content, tracing | Execution plane for one run is met. Ahead on governance, identity, harvest, replay, eval, sandbox honesty. |
| Why can rusui not spawn shikigami as P1? | rusui ADR 0002: ACP `initialize`, `session/new` or `session/load`, `session/prompt`, `session/update`, `session/request_permission`. ROADMAP A5: drive shikigami through `serve` until an ACP server exists. MCP stdio here is Content-Length and exposes `run`/`doctor`, not a session. | ACP is the primary product gap. |
| Can a one-shot `run` be that guest? | `report` terminates. Interactive guests wait on a no-tool message (`end_turn`) and accept follow-ups. rusui ADR 0011 persists guest `sessionId` and tries `session/load`. | Session wait is required. `report` stays the unattended terminator. |
| Who owns the TUI? | VISION: desktop shell is a non-goal; process hosts are CLI, serve, MCP, embed. Interactive peers use a bare-binary TUI over the same agent core as ACP. | Thin TUI in this crate, after ACP, over the same protocol. VISION amendment required. |
| Who owns nested agents? | [#90](https://github.com/Sannrox/shikigami/issues/90) closed: host orchestration for 1.0; nested-run tool only after Design Discussion. rusui ADR 0040 defers plane child sessions. Grok/Claude/Codex spawn children from the model tool loop with isolated context. | Hosts cannot fake a child context window from outside the loop. Post-1.0 Discussion is now justified. |
| Permission modes? | [#61](https://github.com/Sannrox/shikigami/issues/61) asked named modes expanding to allow-lists. Shipped 1.x is still an explicit allow-list plus plane/`http-callback`/escalate. rusui maps unmatched ACP permission to fail-closed reject. | Ask=park is the headless copy of ask-mode. Named read/workspace modes can compose later; do not block ACP. |
| Streaming? | `HarnessEvent::ModelTurn` is a post-call preview. ACP `session/update` and a TUI need chunks. HTTP model adapter may not stream yet. | Discussion must choose stream-on-`model-http` vs one update per completed turn until the adapter streams. Do not invent chunks. |
| Memory / browser / rewind? | Grok file memory, first-party browser, `/rewind` after tool effects. Skills and project rules already load text. `web_fetch` plus MCP cover fetch/search/browser. Effects are not time-travel. | Out of scope. |

Public GitHub search on 2026-10-03 found no open shikigami Issue for ACP,
TUI, nested runs, or plan write-jail. Open backlog was [#330](https://github.com/Sannrox/shikigami/issues/330) only.

## Comparison (execution plane)

| Capability | Shikigami 1.1.1 | Claude / Codex / Grok | Verdict |
| --- | --- | --- | --- |
| Turn loop, jail, sandbox | Yes | Yes | Met |
| MCP client and stdio server | Yes | Yes | Met for process hosts |
| Skills | Preload configured ids | On-demand | Gap: loading model |
| Compaction | `compact_after_messages` | User/agent compact; overflow | [#330](https://github.com/Sannrox/shikigami/issues/330) + overflow |
| Nested agents | None | Child context + tool subset | Post-1.0 Discussion ([#90](https://github.com/Sannrox/shikigami/issues/90)) |
| Plan-then-execute | Host-split runs | Write-jail until accept | Copy the jail |
| Approvals | Plane park; escalate; `http-callback` | Ask/allow/deny + TUI | Copy ask=park |
| ACP guest | Missing | Grok ACP | Primary gap |
| Interactive TUI | None | Bare binary | In-crate host over ACP |
| Session wait | `report` ends the run | `end_turn` waits | Required |
| Assistant streaming | Preview event | Chunks on the wire | Required for TUI/ACP |
| Structured questions | Freeform escalate | Options / multi-select | Park payload |
| Memory / rewind / browser | Rules + skills; no rewind; `web_fetch` | Product features | Out of scope |

## Sequence

Protocol first, then TUI over that protocol, then in-guest intelligence.

| Step | Artifact | Notes |
| --- | --- | --- |
| 0 | This closeout | No runtime change |
| 1 | Design Discussion + ADR: ACP host | Session/load, prompt, update, request_permission, cancel; `end_turn` vs `report`; streaming honesty; VISION amendment |
| 1b | Feature: thin TUI | Blocked on (1). Same protocol. Dense, true black, permission/plan overlays. Ctrl-C → cancel. Continue-last-in-cwd. |
| 2 | Ask=park + structured escalate | Shared protocol for ACP, TUI, CLI resume. May share the ACP ADR if host-agnostic. |
| 3 | Design Discussion: nested child runs | Reopens [#90](https://github.com/Sannrox/shikigami/issues/90) for post-1.0. Typed explore / plan-jail / full. Optional worktree. Background spawn. Sequential with other `src/run/` work. |
| 4 | Feature: plan write-jail | Default off. May fold into (3). |
| 5 | [#330](https://github.com/Sannrox/shikigami/issues/330) then progressive skill load | Closed as [ADR 0019](../decisions/0019-skill-context-boundaries.md): no skill-level boundary. Overflow compact is host policy. Progressive skill load stays later. |

ACP host implementation can proceed in parallel with #330 *research*.
Nested-run, plan-jail, and #330 *implementation* collide on `src/run/`.

## Later

2026-10-06: [#330](https://github.com/Sannrox/shikigami/issues/330) closed
as no skill-declared boundary
([ADR 0019](../decisions/0019-skill-context-boundaries.md)). Session mode,
ACP attachments, and the skills+MCP extension surface are
[ADR 0016](../decisions/0016-session-mode.md)–[0018](../decisions/0018-skills-mcp-extension.md).
See [guest-session-surfaces.md](guest-session-surfaces.md).

## Alternatives

| Option | Decision | Reason |
| --- | --- | --- |
| Keep `serve` as the rusui path forever | Rejected | rusui’s only agent interface is ACP; `serve` is weaker receipt capture (rusui ROADMAP A5). |
| ACP without `request_permission` | Rejected | rusui ADR 0002 rejected always-approve for unattended Grok so policy can sit on the permission channel. |
| TUI talking to `Engine` directly | Rejected | Forks from the rusui guest. One protocol. |
| Nested runs as a second agent type | Rejected | Runs stay the unit of work. Children are runs. |
| Host-only fan-out forever ([#90](https://github.com/Sannrox/shikigami/issues/90) 1.0) | Rejected as a 1.x freeze | Correct for 1.0. Interactive guests spawn children from the tool loop; hosts cannot isolate that context. |
| Plan as two host-split runs only | Rejected as sufficient | The execute run can ignore the plan. The jail is the mechanism. |
| File-backed memory / vector index | Rejected | Operator request. Project rules and skills remain. |
| In-crate desktop shell | Rejected | VISION non-goal. TUI is a process host. |

## Consequences

- No freeze-core breakage. ACP, TUI, and new tools are evolving/host-adjacent
  like MCP ([ADR 0004](../decisions/0004-v1-contract.md)).
- VISION / DESIGN process-host list grows only after the ACP Discussion
  accepts the terminal-host amendment.
- Unattended `run` behavior stays `report` / park / limit.
- Do not add a TUI crate until that amendment is accepted.
- [#330](https://github.com/Sannrox/shikigami/issues/330) scope is unchanged.

## Follow-up

Accepted as [ADR 0014](../decisions/0014-usable-guest-hosts.md) and
[ADR 0015](../decisions/0015-nested-child-runs.md). No Design Discussion.
Implementation is feature Issues after those ADRs land.
