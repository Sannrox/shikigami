//! Slash commands, `/resume`, `/attach`, and `/copy`.

use std::io::{self, Write, stdout};
use std::sync::Arc;

use serde_json::{Value, json};

use shikigami::acp::SessionSummary;

use crate::look::{Action, KeyMap};
use crate::mention;
use crate::protocol::apply_load_result;
use crate::render::{ago, elapsed_since, truncate_one_line, turn_count, turns_label};
use crate::session::{
    FileList, KeyResult, NestedChild, SESSION_IN_USE, SessionBackup, Shared, SlashCommand,
    TranscriptLine, TuiSession,
};
use crate::usage::Usage;

#[cfg(test)]
use crate::render::{composer_inner, line_text, prompt_line};

impl TuiSession {
    pub(crate) fn slash_catalog(&self) -> Vec<SlashCommand> {
        let mut out: Vec<SlashCommand> = BUILTIN_COMMANDS
            .iter()
            .map(|(name, hint)| SlashCommand {
                name: (*name).into(),
                hint: (*hint).into(),
            })
            .collect();
        for id in shikigami::context::list_skill_ids(&self.cwd, self.host.context_settings()) {
            out.push(SlashCommand {
                name: if is_builtin_command(&id) || id.starts_with(SKILL_PREFIX) {
                    format!("{SKILL_PREFIX}{id}")
                } else {
                    id
                },
                hint: "load skill pack".into(),
            });
        }
        out
    }

    pub(crate) fn slash_matches(&self) -> Vec<SlashCommand> {
        let catalog = self.slash_catalog();
        let shared = self.lock_shared();
        if !slash_visible(&shared) {
            return Vec::new();
        }
        let query = slash_query(&shared.input);
        catalog
            .into_iter()
            .filter(|cmd| cmd.name.starts_with(query))
            .collect()
    }

    pub(crate) fn stage_attach(&self, args: &str) -> KeyResult {
        let mut shared = self.lock_shared();
        let path_arg = args.trim();
        if path_arg.is_empty() {
            shared
                .transcript
                .push(TranscriptLine::System("attach requires a path".into()));
            shared.status = "error".into();
            shared.dirty = true;
            return KeyResult::Continue;
        }
        let path = {
            let candidate = std::path::Path::new(path_arg);
            if candidate.is_absolute() {
                candidate.to_path_buf()
            } else {
                self.cwd.join(candidate)
            }
        };
        match acp_part_for_path(&path) {
            Ok(part) => {
                let label = part_label(&part).unwrap_or_else(|| "attachment".into());
                shared.pending_parts.push(part);
                shared.transcript.push(TranscriptLine::System(format!(
                    "attached {} ({label})",
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("file")
                )));
                shared.status = "ready".into();
                shared.dirty = true;
            }
            Err(err) => {
                shared.transcript.push(TranscriptLine::System(err));
                shared.status = "error".into();
                shared.dirty = true;
            }
        }
        KeyResult::Continue
    }

    pub(crate) fn copy_last_assistant(&self) {
        let text = {
            let shared = self.lock_shared();
            shared
                .selected_visible_text()
                .or_else(|| last_assistant(&shared.transcript).map(str::to_string))
        };
        let Some(text) = text else {
            let mut shared = self.lock_shared();
            shared
                .transcript
                .push(TranscriptLine::System("nothing to copy".into()));
            shared.last_osc52 = None;
            shared.dirty = true;
            return;
        };
        let payload = cap_bytes(&text, OSC52_MAX_BYTES).to_string();
        let seq = osc52_sequence(&payload);
        let wrote = {
            let write = *self.osc_write.lock().unwrap_or_else(|e| e.into_inner());
            write(seq.as_bytes())
        };
        let mut shared = self.lock_shared();
        match wrote {
            Ok(()) => {
                shared.last_osc52 = Some(payload);
                shared.status = "copied".into();
                shared.dirty = true;
            }
            Err(err) => {
                shared.last_osc52 = None;
                shared
                    .transcript
                    .push(TranscriptLine::System(format!("copy failed  {err}")));
                shared.status = "error".into();
                shared.dirty = true;
            }
        }
    }

    pub(crate) fn overlay_cmds(&self) -> Vec<SlashCommand> {
        if let Some(sessions) = self.lock_shared().resume.as_ref() {
            return sessions.iter().map(resume_row).collect();
        }
        let slash = self.slash_matches();
        if !slash.is_empty() {
            return slash;
        }
        self.mention_matches()
            .into_iter()
            .map(|path| SlashCommand {
                name: path,
                hint: String::new(),
            })
            .collect()
    }

    /// Ranked workspace files for the open `@` mention; empty when none is open.
    pub(crate) fn mention_matches(&self) -> Vec<String> {
        let mut shared = self.lock_shared();
        let Some((_, query)) = mention_visible(&shared) else {
            return Vec::new();
        };
        match &shared.mention_files {
            FileList::Ready(files) => mention::rank(files, &query),
            FileList::Reading => Vec::new(),
            FileList::NotRead => {
                // Walk off the draw thread; the list fills in when it is done.
                shared.mention_files = FileList::Reading;
                let root = self
                    .host
                    .demo_workspace()
                    .unwrap_or_else(|| self.cwd.clone());
                let state = Arc::clone(&self.client.shared);
                std::thread::spawn(move || {
                    let files = shikigami::tools::workspace_files(&root).unwrap_or_default();
                    let mut shared = state.lock().unwrap_or_else(|e| e.into_inner());
                    if matches!(shared.mention_files, FileList::Reading) {
                        shared.mention_files = FileList::Ready(Arc::new(files));
                        shared.dirty = true;
                    }
                });
                Vec::new()
            }
        }
    }

    /// Replace the open mention with the selected file. False when no
    /// mention is open or nothing matches.
    pub(crate) fn pick_mention(&self) -> bool {
        let matches = self.mention_matches();
        let mut shared = self.lock_shared();
        let Some((start, _)) = mention_visible(&shared) else {
            return false;
        };
        let Some(path) = matches.get(shared.slash_selected.min(matches.len().saturating_sub(1)))
        else {
            return false;
        };
        let (input, cursor) = mention::insert(&shared.input, start, path);
        shared.input = input;
        shared.cursor = cursor;
        shared.slash_selected = 0;
        shared.history_idx = None;
        shared.dirty = true;
        true
    }

    pub(crate) async fn open_resume_picker(&self) {
        let sessions = match self.list_sessions(Some(&self.cwd)).await {
            Ok(sessions) => sessions,
            Err(err) => {
                let mut shared = self.lock_shared();
                shared.transcript.push(TranscriptLine::System(err));
                shared.resume = None;
                shared.dirty = true;
                return;
            }
        };
        let mut shared = self.lock_shared();
        if sessions.is_empty() {
            shared
                .transcript
                .push(TranscriptLine::System("no sessions".into()));
            shared.resume = None;
        } else {
            shared.resume = Some(sessions);
            shared.slash_selected = 0;
        }
        shared.dirty = true;
    }

    /// Classifies a composer line without running it or writing the transcript.
    pub(crate) fn slash_line(&self, input: &str) -> SlashLine {
        let trimmed = input.trim();
        if !trimmed.starts_with('/') || trimmed.starts_with("//") {
            return SlashLine::Prompt;
        }
        let (name, _) = slash_name_args(trimmed);
        let catalog = self.slash_catalog();
        let selected = {
            let shared = self.lock_shared();
            if slash_visible(&shared) {
                let matches: Vec<_> = catalog
                    .iter()
                    .filter(|cmd| cmd.name.starts_with(name))
                    .collect();
                matches
                    .get(shared.slash_selected)
                    .map(|cmd| cmd.name.clone())
            } else {
                None
            }
        };
        let command = selected
            .or_else(|| {
                catalog
                    .iter()
                    .find(|cmd| cmd.name == name)
                    .map(|cmd| cmd.name.clone())
            })
            .or_else(|| {
                name.strip_prefix(SKILL_PREFIX)
                    .filter(|id| !id.is_empty())
                    .map(|_| name.to_string())
            });
        match command {
            Some(command) => SlashLine::Command(command),
            None if name.contains('/') => SlashLine::Prompt,
            None => SlashLine::Unknown,
        }
    }

    /// Runs a typed slash command. `None` means the line is a prompt: no leading
    /// `/`, the `//text` escape, or a path-like first word (`/usr/bin/x fails`).
    /// A command-like word that names nothing is refused with the closest name
    /// and the draft stays in the composer so it can be fixed or escaped.
    pub(crate) fn slash_enter(&self, input: &str) -> Option<SlashOutcome> {
        let (name, args) = slash_name_args(input.trim());
        match self.slash_line(input) {
            SlashLine::Prompt => None,
            SlashLine::Command(command) => Some(SlashOutcome::Ran(self.slash_run(&command, args))),
            SlashLine::Unknown => {
                let line = unknown_command_line(name, &self.slash_catalog());
                let mut shared = self.lock_shared();
                shared.transcript.push(TranscriptLine::System(line));
                shared.dirty = true;
                Some(SlashOutcome::Refused)
            }
        }
    }

    pub(crate) fn slash_run(&self, name: &str, args: &str) -> KeyResult {
        match name {
            "help" => {
                let mut shared = self.lock_shared();
                shared
                    .transcript
                    .push(TranscriptLine::System(help_text(&self.look.keys)));
                shared.dirty = true;
                KeyResult::Continue
            }
            "run" => {
                let mut shared = self.lock_shared();
                let line = match &shared.run_id {
                    Some(id) => format!("Run {id}\nshikigami runs {id}\nshikigami export {id}"),
                    None => "No run yet. Send a prompt to start one.".into(),
                };
                shared.transcript.push(TranscriptLine::System(line));
                shared.dirty = true;
                KeyResult::Continue
            }
            "model" if !args.trim().is_empty() => KeyResult::SetModel(args.trim().to_string()),
            "mcp" => match slash_mcp(args) {
                Some(result) => result,
                None => {
                    let mut shared = self.lock_shared();
                    shared
                        .transcript
                        .push(TranscriptLine::System(MCP_USAGE.into()));
                    shared.dirty = true;
                    KeyResult::Continue
                }
            },
            "model" => {
                let mut shared = self.lock_shared();
                let line = match &shared.models {
                    Some(models) => models.listing(),
                    None => "This session offers no other model.".into(),
                };
                shared.transcript.push(TranscriptLine::System(line));
                shared.dirty = true;
                KeyResult::Continue
            }
            "usage" => {
                let mut shared = self.lock_shared();
                let table = shared
                    .usage
                    .table(turn_count(&shared), shared.context_window_tokens);
                shared.transcript.push(TranscriptLine::System(table));
                shared.dirty = true;
                KeyResult::Continue
            }
            "context" => {
                let mut shared = self.lock_shared();
                let line = context_text(&shared);
                shared.transcript.push(TranscriptLine::System(line));
                shared.dirty = true;
                KeyResult::Continue
            }
            "copy" => {
                self.copy_last_assistant();
                KeyResult::Continue
            }
            "attach" => self.stage_attach(args),
            "compact" => KeyResult::Compact,
            "plan" => KeyResult::Plan,
            "exit" | "quit" => KeyResult::Quit,
            "new" => KeyResult::NewSession,
            "resume" => KeyResult::ResumePicker,
            other => {
                // A catalog skill is `/<id>`; `/skill:<id>` is the alias.
                let id = other.strip_prefix(SKILL_PREFIX).unwrap_or(other);
                match shikigami::context::load_skill(&self.cwd, self.host.context_settings(), id) {
                    Some(_) => {
                        let display = if args.is_empty() {
                            format!("/{other}")
                        } else {
                            format!("/{other} {args}")
                        };
                        let send = skill_prompt_send(id, args);
                        let pending = std::mem::take(&mut self.lock_shared().pending_parts);
                        let parts = prompt_parts(&send, pending);
                        KeyResult::Prompt {
                            display: prompt_display(&display, &parts),
                            send,
                            parts,
                            load_skill: Some(id.to_string()),
                        }
                    }
                    None => {
                        let mut shared = self.lock_shared();
                        shared
                            .transcript
                            .push(TranscriptLine::System(format!("unknown skill {id}")));
                        shared.status = "error".into();
                        shared.dirty = true;
                        KeyResult::Continue
                    }
                }
            }
        }
    }

    pub(crate) async fn compact(&self) -> Result<(), String> {
        {
            let mut shared = self.lock_shared();
            if shared.busy {
                return Err("prompt already in flight".into());
            }
            shared.busy = true;
            shared.status = crate::protocol::COMPACTING_STATUS.into();
            shared.dirty = true;
        }
        let result = self
            .call("session/compact", json!({ "sessionId": self.session_id() }))
            .await;
        let mut shared = self.lock_shared();
        shared.busy = false;
        let result = match result {
            Ok(result) => result,
            Err(err) => {
                shared.status = "error".into();
                shared.dirty = true;
                return Err(err);
            }
        };
        let before = result.get("before").and_then(|v| v.as_u64()).unwrap_or(0);
        let after = result
            .get("after")
            .and_then(|v| v.as_u64())
            .unwrap_or(before);
        if after < before {
            shared.last_compact = Some((before, after));
            shared.transcript.push(TranscriptLine::System(format!(
                "compacted  {before} → {after}"
            )));
        } else {
            shared
                .transcript
                .push(TranscriptLine::System("nothing to compact".into()));
        }
        shared.status = "ready".into();
        shared.dirty = true;
        Ok(())
    }

    pub(crate) async fn new_session(&self) -> Result<(), String> {
        let id = self.create_session(&self.cwd).await?;
        *self.session_id.lock().unwrap_or_else(|e| e.into_inner()) = id;
        let mut shared = self.lock_shared();
        shared.transcript.clear();
        shared.plan = None;
        shared.show_plan = false;
        let _ = shared.clear_permission();
        shared.queued = None;
        shared.pending_parts.clear();
        shared.run_id = None;
        shared.usage = Usage::default();
        shared.selected_tool = None;
        shared.text_selection = None;
        shared.visible_rows.clear();
        shared.nested_children.clear();
        shared.context = None;
        shared.last_compact = None;
        shared.status = "ready".into();
        shared.scroll_back = 0;
        shared.dock_scroll = 0;
        shared.dirty = true;
        Ok(())
    }

    /// `/mcp`, `/mcp disable <name>`, `/mcp enable <name>`.
    pub(crate) async fn mcp(&self, action: &str, name: Option<&str>) {
        let mut params = json!({ "sessionId": self.session_id(), "action": action });
        if let Some(name) = name {
            params["name"] = json!(name);
        }
        let result = self.call("session/mcp", params).await;
        let mut shared = self.lock_shared();
        let line = match result {
            Ok(value) if action == "list" => mcp_listing(&value),
            Ok(_) if action == "disable" => {
                format!("Disabled {} for this session.", name.unwrap_or("server"))
            }
            Ok(_) if action == "enable" => {
                format!("Enabled {} for this session.", name.unwrap_or("server"))
            }
            Ok(_) => "MCP.".into(),
            Err(err) => mcp_error_line(name, &err),
        };
        shared.transcript.push(TranscriptLine::System(line));
        shared.dirty = true;
    }

    /// `/plan`: flag the next first prompt of this session for plan write-jail.
    pub(crate) async fn plan(&self) {
        let result = self
            .call("session/plan", json!({ "sessionId": self.session_id() }))
            .await;
        let mut shared = self.lock_shared();
        let line = match result {
            Ok(value) if value.get("already").and_then(Value::as_bool) == Some(true) => {
                "This session is already in plan write-jail.".into()
            }
            Ok(_) => "Next prompt writes only .shikigami/plan.md until you accept the plan.".into(),
            Err(err) => err,
        };
        shared.transcript.push(TranscriptLine::System(line));
        shared.dirty = true;
    }

    /// `/model <id>`: ask the host to switch the session's later prompts.
    /// A refusal keeps the current model and says why.
    pub(crate) async fn set_model(&self, model_id: &str) {
        let result = self
            .call(
                "session/set_model",
                json!({ "sessionId": self.session_id(), "modelId": model_id }),
            )
            .await;
        let mut shared = self.lock_shared();
        let line = match result {
            Ok(_) => {
                if let Some(models) = shared.models.as_mut() {
                    models.current = model_id.to_string();
                }
                format!("Next prompts use {model_id}.")
            }
            Err(err) => format!("The model stays: {}", err.trim_end_matches('.')),
        };
        shared.transcript.push(TranscriptLine::System(line));
        shared.dirty = true;
    }

    /// Footer and status line model name: the configured label, or the id
    /// the session switched to.
    pub(crate) fn model_label(&self, shared: &Shared) -> String {
        match &shared.models {
            Some(models) if models.current != self.default_model_id => models.current.clone(),
            _ => self.model.clone(),
        }
    }

    pub(crate) async fn load_session(&self, session_id: &str) {
        let previous_id = self.session_id();
        let backup = {
            let mut shared = self.lock_shared();
            let backup = SessionBackup {
                transcript: std::mem::take(&mut shared.transcript),
                plan: shared.plan.take(),
                show_plan: shared.show_plan,
                queued: shared.queued.take(),
                scroll_back: shared.scroll_back,
                dock_scroll: shared.dock_scroll,
                nested_children: std::mem::take(&mut shared.nested_children),
            };
            shared.show_plan = false;
            shared.scroll_back = 0;
            shared.dock_scroll = 0;
            shared.resume = None;
            shared.slash_selected = 0;
            shared.dirty = true;
            backup
        };
        *self.session_id.lock().unwrap_or_else(|e| e.into_inner()) = session_id.to_string();
        match self
            .call("session/load", json!({ "sessionId": session_id }))
            .await
        {
            Ok(result) => {
                let mut shared = self.lock_shared();
                apply_load_result(&mut shared, &result);
                shared.usage = Usage::default();
                shared.selected_tool = None;
                shared.status = "ready".into();
                shared.dirty = true;
            }
            Err(err) => {
                *self.session_id.lock().unwrap_or_else(|e| e.into_inner()) = previous_id;
                let mut shared = self.lock_shared();
                shared.transcript = backup.transcript;
                shared.plan = backup.plan;
                shared.show_plan = backup.show_plan;
                shared.queued = backup.queued;
                shared.scroll_back = backup.scroll_back;
                shared.dock_scroll = backup.dock_scroll;
                shared.nested_children = backup.nested_children;
                let line = if err == shikigami::acp::SESSION_IN_USE {
                    SESSION_IN_USE.to_string()
                } else {
                    err
                };
                shared.transcript.push(TranscriptLine::System(line));
                shared.status = "error".into();
                shared.dirty = true;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn overlay_text(&self) -> Option<String> {
        let shared = self.lock_shared();
        if shared.permission.is_some() || shared.show_plan {
            Some(composer_inner(&shared))
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn transcript_text(&self) -> String {
        self.lock_shared()
            .transcript
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[cfg(test)]
    pub(crate) fn prompt_text(&self) -> String {
        prompt_line(&self.lock_shared())
    }

    #[cfg(test)]
    pub(crate) fn fail_osc_write(&self) {
        *self.osc_write.lock().unwrap_or_else(|e| e.into_inner()) = osc_write_fail;
    }
}

/// Built-in slash commands and their list hints. They win a name collision
/// with a skill pack.
pub(crate) const BUILTIN_COMMANDS: &[(&str, &str)] = &[
    ("help", "keys and slash commands"),
    ("compact", "shrink conversation history"),
    ("context", "attached rules, skills, compact"),
    ("copy", "copy selection or last assistant"),
    ("attach", "add prompt file"),
    ("exit", "quit"),
    ("quit", "quit"),
    ("new", "start a new session"),
    ("plan", "plan write-jail for this session"),
    ("run", "show the run id for runs and export"),
    ("usage", "token use per turn"),
    ("model", "list or switch models"),
    ("mcp", "list or disable MCP servers"),
    ("resume", "load a persisted session"),
];

/// `/skill:<id>` reaches a pack by id even when a built-in has that name.
pub(crate) const SKILL_PREFIX: &str = "skill:";

pub(crate) fn is_builtin_command(name: &str) -> bool {
    BUILTIN_COMMANDS.iter().any(|(builtin, _)| *builtin == name)
}

/// `/help`: keys (with any `[tui.keys]` rebinding) and slash names.
pub(crate) fn help_text(keys: &KeyMap) -> String {
    format!(
        "Enter send  Shift+Enter newline  {} cancel/quit\n/compact  /context  /new  /plan  /resume  /run  /usage  /model  /mcp  /copy  /attach  /exit  /quit  /help\n/<skill> loads a skill pack (/skill:<skill> when a command has its name)",
        keys.label(Action::Interrupt)
    )
}

/// `/context`: standing attachments the live run composed, not a second scan.
pub(crate) fn context_text(shared: &Shared) -> String {
    let Some(context) = shared.context.as_ref() else {
        return "No attached context yet. Send a prompt to load it.".into();
    };
    let mut lines = Vec::new();
    let rules = context
        .get("rules")
        .and_then(Value::as_array)
        .filter(|rules| !rules.is_empty());
    match rules {
        Some(rules) => {
            for rule in rules {
                let name = rule
                    .get("filename")
                    .and_then(Value::as_str)
                    .unwrap_or("file");
                let bytes = rule.get("bytes").and_then(Value::as_u64).unwrap_or(0);
                let truncated = rule
                    .get("truncated")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let mut line = format!("project rules  {name}  {bytes} B");
                if truncated {
                    line.push_str(" truncated");
                }
                lines.push(line);
            }
        }
        None => lines.push("no project rules".into()),
    }
    let eager = string_list(context.get("eager"));
    let catalog = string_list(context.get("catalog"));
    lines.push(match (eager.as_slice(), catalog.as_slice()) {
        ([], []) => "skills  none".into(),
        (eager, []) => format!("skills  eager: {}", eager.join(", ")),
        ([], catalog) => format!("skills  catalog: {}", catalog.join(", ")),
        (eager, catalog) => format!(
            "skills  eager: {}  catalog: {}",
            eager.join(", "),
            catalog.join(", ")
        ),
    });
    lines.push(match shared.last_compact {
        Some((before, after)) => format!("compact  {before} → {after}"),
        None if shared
            .transcript
            .iter()
            .any(|line| matches!(line, TranscriptLine::System(text) if text == crate::protocol::COMPACTED)) =>
        {
            "compact  compacted".into()
        }
        None => "compact  none".into(),
    });
    lines.push(
        if context.get("nested").and_then(Value::as_bool) == Some(true) {
            "nested  on".into()
        } else {
            "nested  off".into()
        },
    );
    if let Some(mode) = context
        .get("mode")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
    {
        lines.push(format!("mode  {mode}"));
    }
    if let Some(model) = context
        .get("model")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .map(str::to_string)
        .or_else(|| shared.models.as_ref().map(|models| models.current.clone()))
    {
        lines.push(format!("model  {model}"));
    }
    lines.join("\n")
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

/// Static busy marker in the slash-list slot above the composer.
pub(crate) const RUNNING_MARKER: &str = "running";

/// Bound OSC 52 so a huge assistant reply cannot stall the TTY.
pub(crate) const OSC52_MAX_BYTES: usize = 32 * 1024;

pub(crate) fn last_assistant(transcript: &[TranscriptLine]) -> Option<&str> {
    transcript.iter().rev().find_map(|line| match line {
        TranscriptLine::Assistant(text) => Some(text.as_str()),
        _ => None,
    })
}

pub(crate) fn cap_bytes(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// User text for a slash-invoked skill: args, or a one-line follow when empty.
pub(crate) fn skill_prompt_send(id: &str, args: &str) -> String {
    if args.is_empty() {
        format!("follow skill `{id}`")
    } else {
        args.to_string()
    }
}

pub(crate) fn prompt_parts(text: &str, pending: Vec<Value>) -> Vec<Value> {
    let mut parts = vec![json!({"type": "text", "text": text})];
    parts.extend(pending);
    parts
}

pub(crate) fn prompt_display(text: &str, parts: &[Value]) -> String {
    let labels: Vec<String> = parts.iter().filter_map(part_label).collect();
    if labels.is_empty() {
        text.to_string()
    } else {
        format!("{text}  {}", labels.join(" "))
    }
}

pub(crate) fn part_label(part: &Value) -> Option<String> {
    match part.get("type").and_then(|value| value.as_str())? {
        "image" | "audio" => part
            .get("mimeType")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        "resource" => part
            .get("resource")
            .and_then(|resource| resource.get("mimeType"))
            .and_then(|value| value.as_str())
            .map(str::to_string),
        _ => None,
    }
}

pub(crate) fn acp_part_for_path(path: &std::path::Path) -> Result<Value, String> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| format!("attach failed  {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("attach path must be a regular file".into());
    }
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let (kind, mime) = match ext.as_str() {
        "png" => ("image", "image/png"),
        "jpg" | "jpeg" => ("image", "image/jpeg"),
        "gif" => ("image", "image/gif"),
        "webp" => ("image", "image/webp"),
        "wav" => ("audio", "audio/wav"),
        "mp3" => ("audio", "audio/mpeg"),
        "m4a" => ("audio", "audio/mp4"),
        "ogg" => ("audio", "audio/ogg"),
        "pdf" => ("resource", "application/pdf"),
        _ => return Err(format!("unsupported attach type `{ext}`")),
    };
    // ADR 0006 part bound; check metadata before allocating the payload.
    const MAX_ATTACH_BYTES: u64 = 8 * 1024 * 1024;
    if metadata.len() > MAX_ATTACH_BYTES {
        return Err("attach exceeds 8MiB".into());
    }
    let bytes = std::fs::read(path).map_err(|error| format!("attach failed  {error}"))?;
    if bytes.len() as u64 > MAX_ATTACH_BYTES {
        return Err("attach exceeds 8MiB".into());
    }
    match kind {
        "resource" => Ok(json!({
            "type": "resource",
            "resource": {
                "uri": path.display().to_string(),
                "mimeType": mime,
                "blob": base64_encode(&bytes)
            }
        })),
        _ => Ok(json!({
            "type": kind,
            "mimeType": mime,
            "data": base64_encode(&bytes)
        })),
    }
}

pub(crate) fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        out.push(TABLE[(a >> 2) as usize] as char);
        out.push(TABLE[(((a & 0x03) << 4) | (b >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b & 0x0f) << 2) | (c >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(c & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub(crate) fn osc52_sequence(payload: &str) -> String {
    format!("\x1b]52;c;{}\x07", base64_encode(payload.as_bytes()))
}

pub(crate) fn osc_write_noop(_: &[u8]) -> io::Result<()> {
    Ok(())
}

pub(crate) fn osc_write_stdout(bytes: &[u8]) -> io::Result<()> {
    let mut out = stdout();
    out.write_all(bytes)?;
    out.flush()
}

#[cfg(test)]
pub(crate) fn osc_write_fail(_: &[u8]) -> io::Result<()> {
    Err(io::Error::other("osc52 write failed"))
}

pub(crate) fn slash_visible(shared: &Shared) -> bool {
    shared.resume.is_none()
        && !shared.busy
        && shared.permission.is_none()
        && !shared.show_plan
        && !shared.slash_dismissed
        && shared.input.starts_with('/')
}

/// The open `@` mention (start, query): same idle rules as the slash list,
/// and not one Esc closed.
pub(crate) fn mention_visible(shared: &Shared) -> Option<(usize, String)> {
    if shared.resume.is_some()
        || shared.busy
        || shared.permission.is_some()
        || shared.show_plan
        || shared.input.starts_with('/')
    {
        return None;
    }
    mention::mention_at(&shared.input, shared.cursor)
        .filter(|(start, _)| shared.mention_dismissed != Some(*start))
}

/// One static dim row in the slash-list slot while a prompt is in flight.
pub(crate) fn running_marker_visible(shared: &Shared) -> bool {
    shared.busy && !shared.dock_open() && inflight_nested_children(shared).is_empty()
}

pub(crate) fn running_marker_row(shared: &Shared) -> String {
    let word = if shared.status == crate::protocol::COMPACTING_STATUS {
        crate::protocol::COMPACTING_STATUS
    } else {
        RUNNING_MARKER
    };
    format!("  {word}")
}

/// Nested children that still belong in the dim slot (`running` / `parked`).
pub(crate) fn inflight_nested_children(shared: &Shared) -> Vec<&NestedChild> {
    shared
        .nested_children
        .iter()
        .filter(|child| matches!(child.status.as_str(), "running" | "parked"))
        .collect()
}

pub(crate) fn nested_children_visible(shared: &Shared) -> bool {
    !shared.dock_open() && !inflight_nested_children(shared).is_empty()
}

pub(crate) fn nested_child_row(child: &NestedChild) -> String {
    format!("  {}  {}  {}", child.run_id, child.profile, child.status)
}

/// A composer line that starts with `/`, before Enter runs it.
pub(crate) enum SlashLine {
    /// Not a slash command: send or queue as a prompt.
    Prompt,
    /// Known catalog command or `/skill:` alias.
    Command(String),
    /// Command-like word that names nothing.
    Unknown,
}

/// What Enter did with a line that starts with `/`.
pub(crate) enum SlashOutcome {
    /// A known command ran; the composer is cleared.
    Ran(KeyResult),
    /// Unknown command; the draft stays for editing.
    Refused,
}

pub(crate) fn slash_waits_for_turn(command: &str) -> String {
    format!("/{command} runs when this turn ends.")
}

/// Refusal for a slash word that names no command, with the closest name when
/// one is near enough to be a typo.
pub(crate) fn unknown_command_line(name: &str, catalog: &[SlashCommand]) -> String {
    let closest = catalog
        .iter()
        .map(|cmd| {
            let score = strsim::jaro_winkler(&name.to_lowercase(), &cmd.name);
            (score, &cmd.name)
        })
        .filter(|(score, _)| *score > 0.7)
        .max_by(|a, b| a.0.total_cmp(&b.0));
    match closest {
        Some((_, near)) => format!(
            "Unknown command /{name}. Did you mean /{near}? Start with // to send it as text."
        ),
        None => {
            format!(
                "Unknown command /{name}. /help lists commands; start with // to send it as text."
            )
        }
    }
}

pub(crate) fn slash_query(input: &str) -> &str {
    slash_name_args(input).0
}

fn slash_mcp(args: &str) -> Option<KeyResult> {
    let rest = args.trim();
    if rest.is_empty() {
        return Some(KeyResult::Mcp {
            action: "list".into(),
            name: None,
        });
    }
    let mut parts = rest.splitn(2, char::is_whitespace);
    match (
        parts.next(),
        parts.next().map(str::trim).filter(|s| !s.is_empty()),
    ) {
        (Some("disable"), Some(name)) => Some(KeyResult::Mcp {
            action: "disable".into(),
            name: Some(name.to_string()),
        }),
        (Some("enable"), Some(name)) => Some(KeyResult::Mcp {
            action: "enable".into(),
            name: Some(name.to_string()),
        }),
        _ => None,
    }
}

fn mcp_listing(result: &Value) -> String {
    let Some(servers) = result
        .get("servers")
        .and_then(|value| value.as_array())
        .filter(|servers| !servers.is_empty())
    else {
        return "No MCP servers attached.".into();
    };
    let mut lines = vec!["MCP".to_string()];
    for server in servers {
        let name = server
            .get("name")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let tools = server
            .get("tools")
            .and_then(|value| value.as_array())
            .map(|tools| {
                tools
                    .iter()
                    .filter_map(|tool| tool.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();
        let disabled = server
            .get("disabled")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let mut line = name.to_string();
        if !tools.is_empty() {
            line.push_str("  ");
            line.push_str(&tools);
        }
        if disabled {
            line.push_str("  disabled");
        }
        lines.push(line);
    }
    lines.join("\n")
}

fn mcp_error_line(name: Option<&str>, err: &str) -> String {
    if err.contains("cannot be disabled") {
        return "gateway cannot be disabled.".into();
    }
    if err.contains("is not attached")
        && let Some(name) = name
    {
        return format!("No MCP server named {name} is attached.");
    }
    err.trim_end_matches('.').to_string()
}

pub(crate) const MCP_USAGE: &str = "Usage: /mcp, /mcp disable <name>, /mcp enable <name>";

pub(crate) fn slash_name_args(input: &str) -> (&str, &str) {
    let rest = input.strip_prefix('/').unwrap_or(input).trim();
    match rest.split_once(char::is_whitespace) {
        Some((name, args)) => (name, args.trim()),
        None => (rest, ""),
    }
}

pub(crate) fn slash_row_text(cmd: &SlashCommand, name_w: usize) -> String {
    if cmd.hint.is_empty() {
        format!("  {}", cmd.name)
    } else {
        format!("  {:<name_w$}  {}", cmd.name, cmd.hint)
    }
}

/// First visible catalog index and row count so selection can move past the cap.
pub(crate) fn slash_window(selected: usize, count: usize, vis: usize) -> (usize, usize) {
    if count == 0 || vis == 0 {
        return (0, 0);
    }
    let vis = vis.min(count);
    let selected = selected.min(count - 1);
    let start = selected
        .saturating_add(1)
        .saturating_sub(vis)
        .min(count - vis);
    (start, vis)
}

/// One `/resume` row: age, then turn count and first prompt. A session
/// another window holds says `in use` before its first prompt.
pub(crate) fn resume_row(session: &SessionSummary) -> SlashCommand {
    let first = session.first_prompt.as_deref().unwrap_or("");
    let in_use = if session.in_use { "in use  " } else { "" };
    SlashCommand {
        name: ago(elapsed_since(session.last_active)),
        hint: format!(
            "{:<9}  {in_use}{}",
            turns_label(session.prompts),
            truncate_one_line(first, 60)
        ),
    }
}
