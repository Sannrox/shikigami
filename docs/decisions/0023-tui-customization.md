# ADR 0023: TUI customization is declarative

- Status: Accepted
- Date: 2026-11-18
- Resolves: [#605](https://github.com/Sannrox/shikigami/issues/605)
- Keeps: [ADR 0014](0014-usable-guest-hosts.md) thin process host,
  [ADR 0018](0018-skills-mcp-extension.md) extension surface

## Context

Users of editors with in-process scripting may expect the same from the
TUI: their own keys, colors, status line, or panels. ADR 0018 rules out a
plugin API for the harness but does not say what the terminal client may
load, so each request would reopen the question.

The TUI is an evolving ACP client (ADR 0014). It never runs tools, but UI
code still holds authority: it can answer a permission prompt, send a
prompt, and read the transcript, which carries file contents and tool
output. An in-process runtime is therefore a trust surface even without
tool access. A repository that ships client config able to skip a trust
step is a known attack pattern for coding agents.

## Decision

1. **Customization is declarative settings.** Key bindings and colors are
   settings tables over documented action and color names
   ([#603](https://github.com/Sannrox/shikigami/issues/603)). An unknown
   name or key refuses startup.
2. **Computed UI text comes from an operator command.** A status line is
   produced by a command run out of process with a time limit, the same
   trust model as operator hooks
   ([#604](https://github.com/Sannrox/shikigami/issues/604),
   [hooks.md](../hooks.md)).
3. **User-level config only.** TUI customization is never read from the
   project directory, so a cloned repository cannot change how prompts are
   shown or answered.
4. **A different UI is a different frontend.** A host that needs more than
   settings and a status command speaks ACP itself.
5. **In-process UI scripting is deferred.** It is reconsidered only once the
   TUI states are stable and users repeatedly need what 1 and 2 cannot
   express. If a later ADR accepts it, the runtime has no file or network
   access, loads from user-level config only, cannot answer permission
   prompts, runs each call under a time limit, and exposes a versioned API.

## Consequences

- #603 and #604 add settings tables; their exact shape is decided in those
  Issues.
- The TUI's key map becomes a list of named actions, documented in
  [tui.md](../tui.md).
- Requests for scripting become evidence for revisiting point 5, recorded
  as feature Issues.

## Rejected alternatives

- **An in-process scripting runtime now.** It freezes an evolving UI model
  as a public API and adds a trust surface beside the governance path.
- **No customization at all.** It blocks reasonable key and status needs
  that settings and a command cover safely.
- **Customization from project config.** Repository content could then
  change how permission prompts are presented; the host chooses instead,
  like presets (ADR 0022).
