//! Thin interactive process host: ACP client of the in-process session.
//!
//! Evolving surface, same rank as `shikigami acp`. Not freeze-core.
//! See [ADR 0014](../../../docs/decisions/0014-usable-guest-hosts.md).

use std::io::{self, IsTerminal, Write, stdin, stdout};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyEvent, KeyModifiers, KeyboardEnhancementFlags, MouseEvent, MouseEventKind,
    PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::{Constraint, CrosstermBackend, Layout, Position, Rect, Style};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use unicode_width::UnicodeWidthChar;

use shikigami::acp::{AcpClient, AcpHost};
use shikigami::harness::Harness;
use shikigami::identity::{PRODUCT, VERSION};

/// Run the interactive host on a real terminal until quit.
pub async fn run(harness: Harness) -> Result<(), String> {
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return Err("shikigami tui requires a terminal".into());
    }
    let cwd = std::env::current_dir()
        .map_err(|e| e.to_string())?
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let host = Arc::new(AcpHost::new(harness));
    let session = TuiSession::start(host, &cwd).await?;
    run_terminal(session).await
}

struct ToolLine {
    id: String,
    title: String,
    status: String,
    /// Full rawInput summary or result text. Collapse is `expanded`, not a wipe.
    detail: String,
    expanded: bool,
}

enum TranscriptLine {
    User(String),
    Assistant(String),
    Tool(ToolLine),
    System(String),
}

struct PlanEntry {
    content: String,
    status: String,
}

struct Plan {
    entries: Vec<PlanEntry>,
}

struct PendingPermission {
    title: String,
    raw_input: String,
    tx: oneshot::Sender<Value>,
}

struct QueuedPrompt {
    display: String,
    send: String,
    parts: Vec<Value>,
}

struct Shared {
    transcript: Vec<TranscriptLine>,
    permission: Option<PendingPermission>,
    plan: Option<Plan>,
    show_plan: bool,
    status: String,
    busy: bool,
    /// One follow-up to send after the in-flight prompt finishes.
    queued: Option<QueuedPrompt>,
    input: String,
    cursor: usize,
    history: Vec<String>,
    history_idx: Option<usize>,
    history_scratch: String,
    /// Rows above the follow-tail. 0 means stick to the newest output.
    scroll_back: u16,
    transcript_h: u16,
    /// Rows skipped from the top of an overflowing ask/plan dock.
    dock_scroll: u16,
    /// Last drawn composer height; 0 until draw, so undrawn docks do not steal paging.
    composer_h: u16,
    /// Last drawn composer slot; wheel hit-tests this.
    composer_area: Rect,
    /// Last drawn composer width; Up/Down use it to step visual rows.
    composer_w: u16,
    dirty: bool,
    /// Esc hid the `/` list; cleared once the draft no longer starts with `/`.
    slash_dismissed: bool,
    slash_selected: usize,
    /// Full session ids for the `/resume` picker, newest first.
    resume_ids: Option<Vec<String>>,
    /// Last OSC 52 clipboard payload written by `/copy` or Ctrl+Y.
    last_osc52: Option<String>,
    /// Staged ACP prompt parts from `/attach` (bytes stay in the prompt, not the workspace).
    pending_parts: Vec<Value>,
}

impl Shared {
    fn new() -> Self {
        Self {
            transcript: Vec::new(),
            permission: None,
            plan: None,
            show_plan: false,
            status: "ready".into(),
            busy: false,
            queued: None,
            input: String::new(),
            cursor: 0,
            history: Vec::new(),
            history_idx: None,
            history_scratch: String::new(),
            scroll_back: 0,
            transcript_h: 0,
            dock_scroll: 0,
            composer_h: 0,
            composer_area: Rect::default(),
            composer_w: 80,
            dirty: true,
            slash_dismissed: false,
            slash_selected: 0,
            resume_ids: None,
            last_osc52: None,
            pending_parts: Vec::new(),
        }
    }

    fn insert_char(&mut self, ch: char) {
        self.insert_str(&ch.to_string());
    }

    fn insert_str(&mut self, text: &str) {
        let mut extra = Vec::new();
        let mut chars = text.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\r' {
                extra.push('\n');
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                continue;
            }
            let ch = if ch == '\t' { ' ' } else { ch };
            if ch == '\n' || !ch.is_control() {
                extra.push(ch);
            }
        }
        if extra.is_empty() {
            return;
        }
        let mut chars: Vec<char> = self.input.chars().collect();
        let i = self.cursor.min(chars.len());
        let added = extra.len();
        chars.splice(i..i, extra);
        self.cursor = i + added;
        self.input = chars.into_iter().collect();
        self.history_idx = None;
        self.slash_selected = 0;
        self.sync_slash_dismissed();
        self.dirty = true;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let mut chars: Vec<char> = self.input.chars().collect();
        let i = self.cursor.min(chars.len());
        chars.remove(i - 1);
        self.cursor = i - 1;
        self.input = chars.into_iter().collect();
        self.history_idx = None;
        self.slash_selected = 0;
        self.sync_slash_dismissed();
        self.dirty = true;
    }

    fn delete_forward(&mut self) {
        let mut chars: Vec<char> = self.input.chars().collect();
        let i = self.cursor.min(chars.len());
        if i >= chars.len() {
            return;
        }
        chars.remove(i);
        self.input = chars.into_iter().collect();
        self.history_idx = None;
        self.slash_selected = 0;
        self.sync_slash_dismissed();
        self.dirty = true;
    }

    fn move_cursor(&mut self, delta: isize) {
        let len = self.input.chars().count();
        let next = self.cursor.saturating_add_signed(delta).min(len);
        if next != self.cursor {
            self.cursor = next;
            self.dirty = true;
        }
    }

    fn set_cursor_end(&mut self) {
        let len = self.input.chars().count();
        if self.cursor != len {
            self.cursor = len;
            self.dirty = true;
        }
    }

    fn set_cursor_start(&mut self) {
        if self.cursor != 0 {
            self.cursor = 0;
            self.dirty = true;
        }
    }

    fn clear_input(&mut self) {
        if self.input.is_empty() && self.cursor == 0 {
            return;
        }
        self.input.clear();
        self.cursor = 0;
        self.history_idx = None;
        self.slash_dismissed = false;
        self.slash_selected = 0;
        self.dirty = true;
    }

    fn take_input(&mut self) -> String {
        let text = std::mem::take(&mut self.input);
        self.cursor = 0;
        self.history_idx = None;
        self.show_plan = false;
        self.scroll_back = 0;
        self.dock_scroll = 0;
        self.slash_dismissed = false;
        self.slash_selected = 0;
        self.dirty = true;
        text
    }

    /// Store or replace the one-slot follow-up from the current draft.
    fn queue_follow_up(&mut self) {
        let text = self.take_input();
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        let parts = prompt_parts(trimmed, std::mem::take(&mut self.pending_parts));
        self.queued = Some(QueuedPrompt {
            display: prompt_display(trimmed, &parts),
            send: trimmed.to_string(),
            parts,
        });
    }

    fn drop_queue(&mut self) {
        if self.queued.take().is_some() {
            self.dirty = true;
        }
    }

    fn sync_slash_dismissed(&mut self) {
        if !self.input.starts_with('/') {
            self.slash_dismissed = false;
        }
    }

    fn history_up(&mut self) {
        if self.history.is_empty() {
            return;
        }
        match self.history_idx {
            None => {
                self.history_scratch = self.input.clone();
                self.history_idx = Some(self.history.len() - 1);
            }
            Some(0) => {}
            Some(i) => self.history_idx = Some(i - 1),
        }
        if let Some(i) = self.history_idx {
            self.input = self.history[i].clone();
            self.cursor = self.input.chars().count();
            self.dirty = true;
        }
    }

    fn history_down(&mut self) {
        let Some(i) = self.history_idx else {
            return;
        };
        if i + 1 < self.history.len() {
            self.history_idx = Some(i + 1);
            self.input = self.history[i + 1].clone();
        } else {
            self.history_idx = None;
            self.input = std::mem::take(&mut self.history_scratch);
        }
        self.cursor = self.input.chars().count();
        self.dirty = true;
    }

    fn dock_open(&self) -> bool {
        self.permission.is_some() || (self.show_plan && self.plan.is_some())
    }

    fn dock_max_scroll(&self) -> u16 {
        if !self.dock_open() || self.composer_h == 0 {
            return 0;
        }
        wrapped_rows(&composer_inner(self), self.composer_w.max(1)).saturating_sub(self.composer_h)
    }

    /// Scroll an overflowing dock. False means no dock overflow or already at that edge.
    fn try_scroll_dock(&mut self, dir: i16) -> bool {
        let max = self.dock_max_scroll();
        if max == 0 {
            return false;
        }
        let page = self.composer_h.max(1);
        let next = if dir < 0 {
            self.dock_scroll.saturating_sub(page)
        } else {
            self.dock_scroll.saturating_add(page).min(max)
        };
        if next == self.dock_scroll {
            return false;
        }
        self.dock_scroll = next;
        self.dirty = true;
        true
    }

    fn scroll_transcript(&mut self, rows: u16, up: bool) {
        self.scroll_back = if up {
            self.scroll_back.saturating_add(rows)
        } else {
            self.scroll_back.saturating_sub(rows)
        };
        self.dirty = true;
    }

    fn page_transcript_up(&mut self) {
        self.scroll_transcript(self.transcript_h.max(1), true);
    }

    fn page_transcript_down(&mut self) {
        self.scroll_transcript(self.transcript_h.max(1), false);
    }

    fn line_up(&mut self) {
        self.scroll_transcript(1, true);
    }

    fn line_down(&mut self) {
        self.scroll_transcript(1, false);
    }

    fn page_up(&mut self) {
        if self.try_scroll_dock(-1) {
            return;
        }
        self.page_transcript_up();
    }

    fn page_down(&mut self) {
        if self.try_scroll_dock(1) {
            return;
        }
        self.page_transcript_down();
    }

    /// Wheel over an overflowing dock scrolls it; otherwise (or at the edge) page the transcript.
    fn wheel(&mut self, dir: i16, column: u16, row: u16) {
        let over_dock = self.dock_open() && rect_contains(self.composer_area, column, row);
        if over_dock && self.try_scroll_dock(dir) {
            return;
        }
        if dir < 0 {
            self.page_transcript_up();
        } else {
            self.page_transcript_down();
        }
    }

    /// Last tool only. No-op when the transcript has no tools.
    fn toggle_last_tool(&mut self) {
        let Some(TranscriptLine::Tool(tool)) = self
            .transcript
            .iter_mut()
            .rev()
            .find(|line| matches!(line, TranscriptLine::Tool(_)))
        else {
            return;
        };
        tool.expanded = !tool.expanded;
        self.dirty = true;
    }

    /// Move one visual composer row. False means the cursor is already on an edge.
    fn move_cursor_row(&mut self, delta: isize) -> bool {
        let width = self.composer_w.max(1);
        let rows = prompt_rows(&self.input, width);
        if rows.len() <= 1 {
            return false;
        }
        let (x, y) = prompt_cursor(&self.input, self.cursor, width, &rows);
        let dest = y as isize + delta;
        if dest < 0 || dest >= rows.len() as isize {
            return false;
        }
        let next = char_index_at(&self.input, &rows, dest as usize, x);
        if next == self.cursor {
            return true;
        }
        self.cursor = next;
        self.dirty = true;
        true
    }
}

#[derive(Clone)]
struct TuiClient {
    shared: Arc<Mutex<Shared>>,
}

struct SlashCommand {
    name: String,
    hint: String,
}

struct SessionBackup {
    transcript: Vec<TranscriptLine>,
    plan: Option<Plan>,
    show_plan: bool,
    queued: Option<QueuedPrompt>,
    scroll_back: u16,
    dock_scroll: u16,
}

type OscWrite = fn(&[u8]) -> io::Result<()>;

struct TuiSession {
    host: Arc<AcpHost>,
    client: TuiClient,
    session_id: Mutex<String>,
    cwd: std::path::PathBuf,
    next_id: AtomicU64,
    prompt: Mutex<Option<JoinHandle<Option<Value>>>>,
    osc_write: Mutex<OscWrite>,
}

impl TuiSession {
    async fn start(host: Arc<AcpHost>, cwd: &Path) -> Result<Self, String> {
        let cwd = if cwd.is_absolute() {
            cwd.to_path_buf()
        } else {
            cwd.canonicalize().map_err(|e| e.to_string())?
        };
        let session = Self {
            host,
            client: TuiClient {
                shared: Arc::new(Mutex::new(Shared::new())),
            },
            session_id: Mutex::new(String::new()),
            cwd: cwd.clone(),
            next_id: AtomicU64::new(1),
            prompt: Mutex::new(None),
            osc_write: Mutex::new(osc_write_noop),
        };
        session
            .call(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "clientInfo": { "name": PRODUCT, "version": VERSION }
                }),
            )
            .await?;
        let session_id = if let Some(id) = session.host.last_session_id_for_cwd(&cwd) {
            match session
                .call("session/load", json!({ "sessionId": id }))
                .await
            {
                Ok(_) => id,
                Err(_) => session.create_session(&cwd).await?,
            }
        } else {
            session.create_session(&cwd).await?
        };
        *session.session_id.lock().unwrap_or_else(|e| e.into_inner()) = session_id;
        session.lock_shared().dirty = true;
        Ok(session)
    }

    fn session_id(&self) -> String {
        self.session_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    async fn create_session(&self, cwd: &Path) -> Result<String, String> {
        let result = self
            .call(
                "session/new",
                json!({
                    "cwd": cwd.display().to_string(),
                    "mcpServers": []
                }),
            )
            .await?;
        result
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| "session/new did not return sessionId".into())
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let resp = self
            .host
            .handle(msg, &self.client)
            .await
            .ok_or_else(|| format!("{method} returned no result"))?;
        if let Some(err) = resp.get("error") {
            let message = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("error");
            return Err(message.to_string());
        }
        Ok(resp.get("result").cloned().unwrap_or(Value::Null))
    }

    fn lock_shared(&self) -> std::sync::MutexGuard<'_, Shared> {
        self.client.shared.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[cfg(test)]
    async fn spawn_prompt(&self, text: String) -> Result<(), String> {
        self.spawn_prompt_with(text.clone(), text, Vec::new()).await
    }

    async fn spawn_prompt_with(
        &self,
        display: String,
        send: String,
        parts: Vec<Value>,
    ) -> Result<(), String> {
        {
            let mut shared = self.lock_shared();
            if shared.busy {
                return Err("prompt already in flight".into());
            }
            shared.busy = true;
            shared.status = "running".into();
            shared.scroll_back = 0;
            shared.history.push(display.clone());
            shared.transcript.push(TranscriptLine::User(display));
            shared.dirty = true;
        }

        let session_id = self.session_id();
        self.host.arm_cancel(&session_id).await;

        let prompt = if parts.is_empty() {
            json!([{"type": "text", "text": send}])
        } else {
            Value::Array(parts)
        };
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": prompt
            }
        });
        let host = Arc::clone(&self.host);
        let client = self.client.clone();
        let shared = Arc::clone(&self.client.shared);
        // Stay busy until wait_prompt reaps this handle so Enter cannot spawn over it.
        let task = tokio::spawn(async move {
            let resp = host.handle(msg, &client).await;
            let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
            apply_prompt_result(&mut state, resp.as_ref());
            state.dirty = true;
            resp
        });
        *self.prompt.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        Ok(())
    }

    async fn wait_prompt(&self) -> Option<Value> {
        let task = self.prompt.lock().unwrap_or_else(|e| e.into_inner()).take();
        let resp = match task {
            Some(task) => task.await.ok().flatten(),
            None => None,
        };
        let mut shared = self.lock_shared();
        shared.busy = false;
        shared.dirty = true;
        resp
    }

    /// Send the queued follow-up after `wait_prompt` unless that turn was cancelled.
    async fn drain_queued_prompt(&self, last: Option<&Value>) -> Result<(), String> {
        let queued = {
            let mut shared = self.lock_shared();
            if prompt_cancelled(last) {
                shared.drop_queue();
                None
            } else {
                shared.queued.take()
            }
        };
        match queued {
            Some(QueuedPrompt {
                display,
                send,
                parts,
            }) => self.spawn_prompt_with(display, send, parts).await,
            None => Ok(()),
        }
    }

    fn cancel(&self) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/cancel",
            "params": { "sessionId": self.session_id() }
        });
        let host = Arc::clone(&self.host);
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = host.handle(msg, &client).await;
        });
        let mut shared = self.lock_shared();
        shared.drop_queue();
        if let Some(pending) = shared.permission.take() {
            let _ = pending.tx.send(json!({
                "outcome": { "outcome": "cancelled" }
            }));
            shared.dock_scroll = 0;
        }
        shared.dirty = true;
    }

    fn answer_permission(&self, allow: bool) {
        let mut shared = self.lock_shared();
        let Some(pending) = shared.permission.take() else {
            return;
        };
        let outcome = if allow { "allow" } else { "deny" };
        let _ = pending.tx.send(json!({
            "outcome": { "outcome": outcome, "optionId": outcome }
        }));
        shared.dock_scroll = 0;
        shared.dirty = true;
    }

    fn slash_catalog(&self) -> Vec<SlashCommand> {
        let mut out = vec![
            SlashCommand {
                name: "help".into(),
                hint: "keys and slash commands".into(),
            },
            SlashCommand {
                name: "compact".into(),
                hint: "shrink conversation history".into(),
            },
            SlashCommand {
                name: "copy".into(),
                hint: "copy last assistant".into(),
            },
            SlashCommand {
                name: "attach".into(),
                hint: "add prompt file".into(),
            },
            SlashCommand {
                name: "exit".into(),
                hint: "quit".into(),
            },
            SlashCommand {
                name: "quit".into(),
                hint: "quit".into(),
            },
            SlashCommand {
                name: "new".into(),
                hint: "start a new session".into(),
            },
            SlashCommand {
                name: "resume".into(),
                hint: "load a persisted session".into(),
            },
        ];
        for id in shikigami::context::list_skill_ids(&self.cwd, self.host.context_settings()) {
            out.push(SlashCommand {
                name: format!("skill:{id}"),
                hint: "load skill pack".into(),
            });
        }
        out
    }

    fn slash_matches(&self) -> Vec<SlashCommand> {
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

    fn stage_attach(&self, args: &str) -> KeyResult {
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

    fn copy_last_assistant(&self) {
        let text = {
            let shared = self.lock_shared();
            last_assistant(&shared.transcript).map(str::to_string)
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

    fn overlay_cmds(&self) -> Vec<SlashCommand> {
        let ids = self.lock_shared().resume_ids.clone();
        if let Some(ids) = ids {
            return ids
                .iter()
                .map(|id| SlashCommand {
                    name: short_session_id(id).to_string(),
                    hint: String::new(),
                })
                .collect();
        }
        self.slash_matches()
    }

    fn open_resume_picker(&self) -> KeyResult {
        let ids = self.host.session_ids_for_cwd(&self.cwd);
        let mut shared = self.lock_shared();
        if ids.is_empty() {
            shared
                .transcript
                .push(TranscriptLine::System("no sessions".into()));
            shared.resume_ids = None;
        } else {
            shared.resume_ids = Some(ids);
            shared.slash_selected = 0;
        }
        shared.dirty = true;
        KeyResult::Continue
    }

    fn slash_enter(&self, input: &str) -> Option<KeyResult> {
        let trimmed = input.trim();
        if !trimmed.starts_with('/') {
            return None;
        }
        let (name, args) = slash_name_args(trimmed);
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
                name.strip_prefix("skill:")
                    .filter(|id| !id.is_empty())
                    .map(|_| name.to_string())
            })?;
        Some(self.slash_run(&command, args))
    }

    fn slash_run(&self, name: &str, args: &str) -> KeyResult {
        match name {
            "help" => {
                let mut shared = self.lock_shared();
                shared
                    .transcript
                    .push(TranscriptLine::System(HELP_TEXT.into()));
                shared.dirty = true;
                KeyResult::Continue
            }
            "copy" => {
                self.copy_last_assistant();
                KeyResult::Continue
            }
            "attach" => self.stage_attach(args),
            "compact" => KeyResult::Compact,
            "exit" | "quit" => KeyResult::Quit,
            "new" => KeyResult::NewSession,
            "resume" => self.open_resume_picker(),
            other => {
                let Some(id) = other.strip_prefix("skill:") else {
                    return KeyResult::Continue;
                };
                match shikigami::context::load_skill(&self.cwd, self.host.context_settings(), id) {
                    Some(pack) => {
                        let display = if args.is_empty() {
                            format!("/{other}")
                        } else {
                            format!("/{other} {args}")
                        };
                        let send = if args.is_empty() {
                            pack.body
                        } else {
                            format!("{}\n\n{args}", pack.body)
                        };
                        let pending = std::mem::take(&mut self.lock_shared().pending_parts);
                        let parts = prompt_parts(&send, pending);
                        KeyResult::Prompt {
                            display: prompt_display(&display, &parts),
                            send,
                            parts,
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

    async fn compact(&self) -> Result<(), String> {
        let result = self
            .call("session/compact", json!({ "sessionId": self.session_id() }))
            .await?;
        let before = result.get("before").and_then(|v| v.as_u64()).unwrap_or(0);
        let after = result
            .get("after")
            .and_then(|v| v.as_u64())
            .unwrap_or(before);
        let mut shared = self.lock_shared();
        if after < before {
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

    async fn new_session(&self) -> Result<(), String> {
        let id = self.create_session(&self.cwd).await?;
        *self.session_id.lock().unwrap_or_else(|e| e.into_inner()) = id;
        let mut shared = self.lock_shared();
        shared.transcript.clear();
        shared.plan = None;
        shared.show_plan = false;
        shared.permission = None;
        shared.queued = None;
        shared.pending_parts.clear();
        shared.status = "ready".into();
        shared.scroll_back = 0;
        shared.dock_scroll = 0;
        shared.dirty = true;
        Ok(())
    }

    async fn load_session(&self, session_id: &str) {
        let backup = {
            let mut shared = self.lock_shared();
            let backup = SessionBackup {
                transcript: std::mem::take(&mut shared.transcript),
                plan: shared.plan.take(),
                show_plan: shared.show_plan,
                queued: shared.queued.take(),
                scroll_back: shared.scroll_back,
                dock_scroll: shared.dock_scroll,
            };
            shared.show_plan = false;
            shared.scroll_back = 0;
            shared.dock_scroll = 0;
            shared.resume_ids = None;
            shared.slash_selected = 0;
            shared.dirty = true;
            backup
        };
        match self
            .call("session/load", json!({ "sessionId": session_id }))
            .await
        {
            Ok(_) => {
                *self.session_id.lock().unwrap_or_else(|e| e.into_inner()) = session_id.to_string();
                let mut shared = self.lock_shared();
                shared.status = "ready".into();
                shared.dirty = true;
            }
            Err(err) => {
                let mut shared = self.lock_shared();
                shared.transcript = backup.transcript;
                shared.plan = backup.plan;
                shared.show_plan = backup.show_plan;
                shared.queued = backup.queued;
                shared.scroll_back = backup.scroll_back;
                shared.dock_scroll = backup.dock_scroll;
                shared.transcript.push(TranscriptLine::System(err));
                shared.status = "error".into();
                shared.dirty = true;
            }
        }
    }

    #[cfg(test)]
    fn overlay_text(&self) -> Option<String> {
        let shared = self.lock_shared();
        if shared.permission.is_some() || shared.show_plan {
            Some(composer_inner(&shared))
        } else {
            None
        }
    }

    #[cfg(test)]
    fn transcript_text(&self) -> String {
        self.lock_shared()
            .transcript
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[cfg(test)]
    fn prompt_text(&self) -> String {
        prompt_line(&self.lock_shared())
    }

    #[cfg(test)]
    fn fail_osc_write(&self) {
        *self.osc_write.lock().unwrap_or_else(|e| e.into_inner()) = osc_write_fail;
    }
}

fn line_text(line: &TranscriptLine) -> String {
    match line {
        TranscriptLine::User(text) => format!("you  {text}"),
        TranscriptLine::Assistant(text) => text.clone(),
        TranscriptLine::Tool(tool) => format_tool(tool),
        TranscriptLine::System(text) => text.clone(),
    }
}

fn format_tool(tool: &ToolLine) -> String {
    let rest = if tool.detail.is_empty() {
        tool.status.as_str()
    } else {
        tool.detail.as_str()
    };
    let failed = tool.status == "failed" && !tool.detail.is_empty();
    if !tool.expanded {
        let one = truncate_one_line(rest, 80);
        return if failed {
            format!("· {}  {one}  failed", tool.title)
        } else {
            format!("· {}  {one}", tool.title)
        };
    }
    let mut lines = rest.lines();
    let first = lines.next().unwrap_or("");
    let mut out = if failed {
        format!("· {}  {first}  failed", tool.title)
    } else {
        format!("· {}  {first}", tool.title)
    };
    for line in lines {
        out.push('\n');
        out.push_str("· ");
        out.push_str(line);
    }
    out
}

fn permission_dock(pending: &PendingPermission) -> String {
    let mut out = format!("ask  {}", pending.title);
    let body = permission_fields(&pending.raw_input);
    if !body.is_empty() {
        out.push('\n');
        out.push_str(&body);
    }
    out
}

fn permission_fields(raw_input: &str) -> String {
    match serde_json::from_str::<Value>(raw_input) {
        Ok(Value::Object(map)) if !map.is_empty() => map
            .iter()
            .filter_map(|(key, value)| {
                let shown = field_preview(value);
                if shown.is_empty() {
                    None
                } else {
                    Some(align_field(key, &shown))
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Ok(Value::String(s)) => field_text(&s),
        _ => {
            let trimmed = raw_input.trim();
            if trimmed.is_empty() || trimmed == "{}" {
                String::new()
            } else {
                field_text(trimmed)
            }
        }
    }
}

fn field_preview(value: &Value) -> String {
    match value {
        Value::String(s) => field_text(s),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => field_text(&other.to_string()),
    }
}

fn field_text(s: &str) -> String {
    s.trim_end_matches(['\r', '\n']).to_string()
}

fn align_field(key: &str, shown: &str) -> String {
    let mut lines = shown.lines();
    let first = lines.next().unwrap_or("");
    let mut out = format!("{key:<8} {first}");
    let indent = " ".repeat(9);
    for line in lines {
        out.push('\n');
        out.push_str(&indent);
        out.push_str(line);
    }
    out
}

fn format_plan(plan: &Plan) -> String {
    let mut out = String::from("plan");
    for (i, entry) in plan.entries.iter().enumerate() {
        if entry.status.is_empty() {
            out.push_str(&format!("\n{}. {}", i + 1, entry.content));
        } else {
            out.push_str(&format!("\n{}. {}  {}", i + 1, entry.content, entry.status));
        }
    }
    out
}

fn composer_inner(shared: &Shared) -> String {
    if let Some(pending) = shared.permission.as_ref() {
        format!("{}\n> y allow\n  n deny", permission_dock(pending))
    } else if shared.show_plan
        && let Some(plan) = shared.plan.as_ref()
    {
        format_plan(plan)
    } else {
        prompt_line(shared)
    }
}

fn prompt_line(shared: &Shared) -> String {
    format!("> {}", shared.input)
}

const HELP_TEXT: &str = "Enter send  Shift+Enter newline  Ctrl-C cancel/quit\n/compact  /new  /resume  /copy  /attach  /exit  /quit  /skill:name  /help";

/// Static busy marker in the slash-list slot above the composer.
const RUNNING_MARKER: &str = "running";

/// Bound OSC 52 so a huge assistant reply cannot stall the TTY.
const OSC52_MAX_BYTES: usize = 32 * 1024;

fn last_assistant(transcript: &[TranscriptLine]) -> Option<&str> {
    transcript.iter().rev().find_map(|line| match line {
        TranscriptLine::Assistant(text) => Some(text.as_str()),
        _ => None,
    })
}

fn cap_bytes(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn prompt_parts(text: &str, pending: Vec<Value>) -> Vec<Value> {
    let mut parts = vec![json!({"type": "text", "text": text})];
    parts.extend(pending);
    parts
}

fn prompt_display(text: &str, parts: &[Value]) -> String {
    let labels: Vec<String> = parts.iter().filter_map(part_label).collect();
    if labels.is_empty() {
        text.to_string()
    } else {
        format!("{text}  {}", labels.join(" "))
    }
}

fn part_label(part: &Value) -> Option<String> {
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

fn acp_part_for_path(path: &std::path::Path) -> Result<Value, String> {
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

fn base64_encode(input: &[u8]) -> String {
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

fn osc52_sequence(payload: &str) -> String {
    format!("\x1b]52;c;{}\x07", base64_encode(payload.as_bytes()))
}

fn osc_write_noop(_: &[u8]) -> io::Result<()> {
    Ok(())
}

fn osc_write_stdout(bytes: &[u8]) -> io::Result<()> {
    let mut out = stdout();
    out.write_all(bytes)?;
    out.flush()
}

#[cfg(test)]
fn osc_write_fail(_: &[u8]) -> io::Result<()> {
    Err(io::Error::other("osc52 write failed"))
}

fn slash_visible(shared: &Shared) -> bool {
    shared.resume_ids.is_none()
        && !shared.busy
        && shared.permission.is_none()
        && !shared.show_plan
        && !shared.slash_dismissed
        && shared.input.starts_with('/')
}

/// One static dim row in the slash-list slot while a prompt is in flight.
fn running_marker_visible(shared: &Shared) -> bool {
    shared.busy && !shared.dock_open()
}

fn running_marker_row() -> String {
    format!("  {RUNNING_MARKER}")
}

fn slash_query(input: &str) -> &str {
    slash_name_args(input).0
}

fn slash_name_args(input: &str) -> (&str, &str) {
    let rest = input.strip_prefix('/').unwrap_or(input).trim();
    match rest.split_once(char::is_whitespace) {
        Some((name, args)) => (name, args.trim()),
        None => (rest, ""),
    }
}

fn slash_row_text(cmd: &SlashCommand, name_w: usize) -> String {
    if cmd.hint.is_empty() {
        format!("  {}", cmd.name)
    } else {
        format!("  {:<name_w$}  {}", cmd.name, cmd.hint)
    }
}

/// First visible catalog index and row count so selection can move past the cap.
fn slash_window(selected: usize, count: usize, vis: usize) -> (usize, usize) {
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

fn short_session_id(id: &str) -> &str {
    let rest = id.strip_prefix("sess-").unwrap_or(id);
    rest.get(..8).unwrap_or(rest)
}

fn status_line(session_id: &str, shared: &Shared, width: u16) -> String {
    let mut parts = vec![
        shared.status.clone(),
        short_session_id(session_id).to_string(),
    ];
    if shared.permission.is_some() {
        parts.push("esc cancel".into());
    } else if shared.busy {
        parts.push("^c cancel".into());
    } else if shared.resume_ids.is_some() {
        parts.push("esc".into());
        parts.push("^c quit".into());
    } else if slash_visible(shared) {
        parts.push("tab pick".into());
        parts.push("esc".into());
        parts.push("^c quit".into());
    } else {
        parts.push("^c quit".into());
    }
    if shared.plan.is_some() {
        parts.push("^p plan".into());
    }
    if let Some(queued) = shared.queued.as_ref() {
        parts.push(format!(
            "queued  {}",
            truncate_one_line(&queued.display, 32)
        ));
    }
    if shared
        .transcript
        .iter()
        .any(|line| matches!(line, TranscriptLine::Tool(_)))
    {
        parts.push("^o tool".into());
    }
    parts.push("pgup/pgdn/wheel".into());
    truncate_display(&format!("  {}", parts.join("  ")), width as usize)
}

fn char_cols(ch: char) -> usize {
    UnicodeWidthChar::width(ch).unwrap_or(0)
}

fn display_cols(s: &str) -> usize {
    s.chars().map(char_cols).sum()
}

fn take_cols(s: &str, max: usize) -> String {
    let mut out = String::new();
    let mut used = 0;
    for ch in s.chars() {
        let w = char_cols(ch);
        if used + w > max {
            break;
        }
        out.push(ch);
        used += w;
    }
    out
}

fn truncate_display(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if display_cols(s) <= max {
        return s.to_string();
    }
    if max <= 3 {
        return take_cols(s, max);
    }
    format!("{}...", take_cols(s, max.saturating_sub(3)))
}

fn line_kind(line: &TranscriptLine) -> u8 {
    match line {
        TranscriptLine::User(_) => 0,
        TranscriptLine::Assistant(_) => 1,
        TranscriptLine::Tool(_) => 2,
        TranscriptLine::System(_) => 3,
    }
}

fn spaced_rows(transcript: &[TranscriptLine]) -> Vec<String> {
    let mut rows = Vec::new();
    let mut last: Option<u8> = None;
    for line in transcript {
        let kind = line_kind(line);
        if last.is_some_and(|prev| prev != kind) {
            rows.push(String::new());
        }
        rows.extend(line_text(line).lines().map(str::to_string));
        last = Some(kind);
    }
    rows
}

fn rule_line(width: u16) -> Line<'static> {
    Line::from(Span::styled("─".repeat(width as usize), dim_style()))
}

fn compact_value(value: &Value) -> String {
    match value {
        Value::Object(map) if map.is_empty() => String::new(),
        Value::Object(map) => {
            const KEYS: &[&str] = &["path", "command", "query", "url", "pattern", "name", "file"];
            let mut parts = Vec::new();
            for key in KEYS {
                if let Some(v) = map.get(*key) {
                    let shown = scalar_preview(v, 60);
                    if !shown.is_empty() {
                        parts.push(shown);
                    }
                }
            }
            if parts.is_empty() {
                for (key, v) in map.iter().take(3) {
                    if key == "content" || key == "contents" {
                        continue;
                    }
                    let shown = scalar_preview(v, 40);
                    if !shown.is_empty() {
                        parts.push(format!("{key} {shown}"));
                    }
                }
            }
            parts.join("  ")
        }
        Value::String(s) => truncate_one_line(s, 80),
        _ => String::new(),
    }
}

fn scalar_preview(value: &Value, max: usize) -> String {
    match value {
        Value::String(s) => truncate_one_line(s, max),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => truncate_one_line(&other.to_string(), max),
    }
}

fn truncate_one_line(s: &str, max: usize) -> String {
    let s = s.lines().next().unwrap_or("").trim();
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let keep: String = s.chars().take(max.saturating_sub(3)).collect();
        format!("{keep}...")
    }
}

struct PromptRow {
    start: usize,
    end: usize,
}

fn wrap_logical(chars: &[char], start: usize, end: usize, avail: usize) -> Vec<PromptRow> {
    if start == end {
        return vec![PromptRow { start, end }];
    }
    let mut rows = Vec::new();
    let mut i = start;
    while i < end {
        let row_start = i;
        let mut used = 0;
        while i < end {
            let w = char_cols(chars[i]);
            if w == 0 {
                i += 1;
                continue;
            }
            if used > 0 && used + w > avail {
                break;
            }
            used += w;
            i += 1;
            if used >= avail {
                break;
            }
        }
        if i == row_start {
            i += 1;
        }
        rows.push(PromptRow {
            start: row_start,
            end: i,
        });
    }
    rows
}

fn composer_avail(width: u16) -> usize {
    // `> ` takes two cells; keep one so the caret can sit after a full row.
    width.saturating_sub(3).max(1) as usize
}

fn prompt_rows(input: &str, width: u16) -> Vec<PromptRow> {
    let chars: Vec<char> = input.chars().collect();
    let avail = composer_avail(width);
    let mut rows = Vec::new();
    let mut start = 0;
    for (i, &ch) in chars.iter().enumerate() {
        if ch == '\n' {
            rows.extend(wrap_logical(&chars, start, i, avail));
            start = i + 1;
        }
    }
    rows.extend(wrap_logical(&chars, start, chars.len(), avail));
    rows
}

fn prompt_cursor(input: &str, cursor: usize, width: u16, rows: &[PromptRow]) -> (u16, u16) {
    let chars: Vec<char> = input.chars().collect();
    let pos = cursor.min(chars.len());
    let prefix = 2u16;
    if rows.is_empty() {
        return (prefix.min(width.saturating_sub(1)), 0);
    }
    let mut row_i = rows.len() - 1;
    for (i, row) in rows.iter().enumerate() {
        let next = rows.get(i + 1).map(|r| r.start);
        // A soft-wrap boundary index belongs to the filled row so Up/Down
        // stay on that row's reserved caret cell.
        if next.is_none_or(|next| pos < next || (pos == row.end && next == row.end)) {
            row_i = i;
            break;
        }
    }
    let row = &rows[row_i];
    let upto = pos.clamp(row.start, row.end);
    let before: String = chars[row.start..upto].iter().collect();
    let x = prefix.saturating_add(display_cols(&before) as u16);
    (x.min(width.saturating_sub(1)), row_i as u16)
}

fn char_index_at(input: &str, rows: &[PromptRow], row: usize, col: u16) -> usize {
    let Some(target) = rows.get(row) else {
        return input.chars().count();
    };
    let chars: Vec<char> = input.chars().collect();
    let want = col.saturating_sub(2) as usize;
    let mut used = 0;
    let mut i = target.start;
    while i < target.end {
        let w = char_cols(chars[i]);
        if used + w > want {
            break;
        }
        used += w;
        i += 1;
    }
    i
}

fn prompt_block(input: &str, cursor: usize, width: u16) -> (Vec<String>, u16, u16) {
    let chars: Vec<char> = input.chars().collect();
    let rows = prompt_rows(input, width);
    let (x, y) = prompt_cursor(input, cursor, width, &rows);
    let mut lines = Vec::with_capacity(rows.len().max(1));
    for (i, row) in rows.iter().enumerate() {
        let text: String = chars[row.start..row.end].iter().collect();
        if i == 0 {
            lines.push(format!("> {text}"));
        } else {
            lines.push(format!("  {text}"));
        }
    }
    if lines.is_empty() {
        lines.push("> ".into());
    }
    (lines, x, y)
}

#[async_trait]
impl AcpClient for TuiClient {
    async fn notify(&self, method: &str, params: Value) -> Result<(), String> {
        if method != "session/update" {
            return Ok(());
        }
        let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        apply_update(&mut shared, &params);
        Ok(())
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        if method != "session/request_permission" {
            return Err(format!("unexpected agent request {method}"));
        }
        let (tx, rx) = oneshot::channel();
        let title = params
            .pointer("/toolCall/title")
            .and_then(|v| v.as_str())
            .unwrap_or("tool")
            .to_string();
        let raw_input = params
            .pointer("/toolCall/rawInput")
            .cloned()
            .unwrap_or(json!({}));
        {
            let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            shared.status = "ask".into();
            shared.permission = Some(PendingPermission {
                title,
                raw_input: raw_input.to_string(),
                tx,
            });
            shared.dock_scroll = 0;
            shared.dirty = true;
        }
        rx.await
            .map_err(|_| "permission response dropped".to_string())
    }
}

fn apply_update(shared: &mut Shared, params: &Value) {
    let update = params.get("update").unwrap_or(params);
    let kind = update
        .get("sessionUpdate")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match kind {
        "user_message_chunk" => {
            if let Some(text) = chunk_text(update) {
                merge_text(
                    &mut shared.transcript,
                    TranscriptLine::User(String::new()),
                    text,
                );
            }
        }
        "agent_message_chunk" => {
            if let Some(text) = chunk_text(update) {
                merge_text(
                    &mut shared.transcript,
                    TranscriptLine::Assistant(String::new()),
                    text,
                );
            }
        }
        "tool_call" => {
            let id = update
                .get("toolCallId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let title = update
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("tool")
                .to_string();
            let status = update
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("pending")
                .to_string();
            let detail = update
                .get("rawInput")
                .map(compact_value)
                .filter(|s| !s.is_empty())
                .unwrap_or_default();
            shared.transcript.push(TranscriptLine::Tool(ToolLine {
                id,
                title,
                status,
                detail,
                expanded: false,
            }));
        }
        "tool_call_update" => {
            let id = update
                .get("toolCallId")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let status = update
                .get("status")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let detail = tool_update_detail(update);
            if let Some(TranscriptLine::Tool(tool)) = shared
                .transcript
                .iter_mut()
                .rev()
                .find(|line| matches!(line, TranscriptLine::Tool(t) if t.id == id))
            {
                if let Some(status) = status {
                    tool.status = status;
                }
                if let Some(detail) = detail {
                    tool.detail = detail;
                }
            }
        }
        "plan" => {
            let entries = update
                .get("entries")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    let content = entry.get("content").and_then(|v| v.as_str())?;
                    Some(PlanEntry {
                        content: content.to_string(),
                        status: entry
                            .get("status")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                    })
                })
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                shared.plan = Some(Plan { entries });
                shared.show_plan = true;
                shared.dock_scroll = 0;
            }
        }
        _ => return,
    }
    shared.dirty = true;
}

fn chunk_text(update: &Value) -> Option<String> {
    update
        .pointer("/content/text")
        .or_else(|| update.get("text"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn tool_update_detail(update: &Value) -> Option<String> {
    update
        .pointer("/content/0/content/text")
        .or_else(|| update.pointer("/content/0/text"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn merge_text(lines: &mut Vec<TranscriptLine>, kind: TranscriptLine, text: String) {
    match (lines.last_mut(), kind) {
        (Some(TranscriptLine::User(existing)), TranscriptLine::User(_)) => existing.push_str(&text),
        (Some(TranscriptLine::Assistant(existing)), TranscriptLine::Assistant(_)) => {
            existing.push_str(&text)
        }
        (_, TranscriptLine::User(_)) => lines.push(TranscriptLine::User(text)),
        (_, TranscriptLine::Assistant(_)) => lines.push(TranscriptLine::Assistant(text)),
        _ => lines.push(TranscriptLine::System(text)),
    }
}

fn prompt_cancelled(resp: Option<&Value>) -> bool {
    resp.and_then(|v| v.pointer("/result/stopReason"))
        .and_then(|v| v.as_str())
        == Some("cancelled")
}

fn apply_prompt_result(shared: &mut Shared, resp: Option<&Value>) {
    if let Some(reason) = resp
        .and_then(|v| v.pointer("/result/stopReason"))
        .and_then(|v| v.as_str())
    {
        shared.status = reason.to_string();
        return;
    }
    if let Some(err) = resp
        .and_then(|v| v.pointer("/error/message"))
        .and_then(|v| v.as_str())
    {
        shared
            .transcript
            .push(TranscriptLine::System(err.to_string()));
        shared.status = "error".into();
        return;
    }
    shared.status = "ready".into();
}

fn follow_pad(content_rows: u16, view_h: u16, scroll_back: u16) -> u16 {
    if scroll_back > 0 {
        0
    } else {
        view_h.saturating_sub(content_rows)
    }
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut out = stdout();
        let _ = execute!(
            out,
            PopKeyboardEnhancementFlags,
            DisableMouseCapture,
            DisableBracketedPaste,
            LeaveAlternateScreen,
            crossterm::cursor::Show
        );
    }
}

/// Drop stderr writes while the alt-screen owns the TTY so event JSON cannot
/// land on the composer (default events adapter is `stderr`).
#[cfg(unix)]
struct StderrSilence {
    saved: i32,
}

#[cfg(unix)]
impl StderrSilence {
    fn apply() -> Option<Self> {
        use std::os::fd::AsRawFd;
        let saved = unsafe { libc::dup(libc::STDERR_FILENO) };
        if saved < 0 {
            return None;
        }
        let Ok(null) = std::fs::OpenOptions::new().write(true).open("/dev/null") else {
            unsafe { libc::close(saved) };
            return None;
        };
        if unsafe { libc::dup2(null.as_raw_fd(), libc::STDERR_FILENO) } < 0 {
            unsafe { libc::close(saved) };
            return None;
        }
        Some(Self { saved })
    }
}

#[cfg(unix)]
impl Drop for StderrSilence {
    fn drop(&mut self) {
        unsafe {
            libc::dup2(self.saved, libc::STDERR_FILENO);
            libc::close(self.saved);
        }
    }
}

async fn run_terminal(session: TuiSession) -> Result<(), String> {
    enable_raw_mode().map_err(|e| e.to_string())?;
    let _guard = TerminalGuard;
    let mut out = stdout();
    execute!(
        out,
        EnterAlternateScreen,
        EnableBracketedPaste,
        EnableMouseCapture
    )
    .map_err(|e| e.to_string())?;
    let _ = execute!(
        out,
        PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
    );
    #[cfg(unix)]
    let _stderr = StderrSilence::apply();
    let mut terminal = Terminal::new(CrosstermBackend::new(out)).map_err(|e| e.to_string())?;
    *session.osc_write.lock().unwrap_or_else(|e| e.into_inner()) = osc_write_stdout;

    let (tx, mut rx) = mpsc::channel(128);
    std::thread::spawn(move || {
        while let Ok(ev) = event::read() {
            if tx.blocking_send(ev).is_err() {
                break;
            }
        }
    });

    loop {
        if session.lock_shared().dirty {
            terminal
                .draw(|frame| draw(frame, &session))
                .map_err(|e| e.to_string())?;
        }

        let finished = {
            let guard = session.prompt.lock().unwrap_or_else(|e| e.into_inner());
            guard.as_ref().is_some_and(|task| task.is_finished())
        };
        if finished {
            let last = session.wait_prompt().await;
            if let Err(err) = session.drain_queued_prompt(last.as_ref()).await {
                let mut shared = session.lock_shared();
                shared.transcript.push(TranscriptLine::System(err));
                shared.status = "error".into();
                shared.dirty = true;
            }
            session.lock_shared().dirty = true;
            continue;
        }

        let ev = match tokio::time::timeout(Duration::from_millis(80), rx.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) => break,
            Err(_) => continue,
        };
        match handle_event(&session, ev) {
            KeyResult::Quit => break,
            KeyResult::Prompt {
                display,
                send,
                parts,
            } => {
                if let Err(err) = session.spawn_prompt_with(display, send, parts).await {
                    let mut shared = session.lock_shared();
                    shared.transcript.push(TranscriptLine::System(err));
                    shared.status = "error".into();
                    shared.dirty = true;
                }
            }
            KeyResult::Compact => {
                if let Err(err) = session.compact().await {
                    let mut shared = session.lock_shared();
                    shared.transcript.push(TranscriptLine::System(err));
                    shared.status = "error".into();
                    shared.dirty = true;
                }
            }
            KeyResult::NewSession => {
                if let Err(err) = session.new_session().await {
                    let mut shared = session.lock_shared();
                    shared.transcript.push(TranscriptLine::System(err));
                    shared.status = "error".into();
                    shared.dirty = true;
                }
            }
            KeyResult::LoadSession(id) => session.load_session(&id).await,
            KeyResult::Continue => {}
        }
    }
    Ok(())
}

#[derive(Debug)]
enum KeyResult {
    Continue,
    Quit,
    Prompt {
        display: String,
        send: String,
        parts: Vec<Value>,
    },
    Compact,
    NewSession,
    LoadSession(String),
}

fn handle_event(session: &TuiSession, ev: Event) -> KeyResult {
    match ev {
        Event::Key(key) if key.kind == event::KeyEventKind::Press => handle_key(session, key),
        Event::Mouse(mouse) => {
            handle_mouse(session, mouse);
            KeyResult::Continue
        }
        Event::Paste(text) => {
            handle_paste(session, &text);
            KeyResult::Continue
        }
        Event::Resize(_, _) => {
            session.lock_shared().dirty = true;
            KeyResult::Continue
        }
        _ => KeyResult::Continue,
    }
}

fn handle_mouse(session: &TuiSession, mouse: MouseEvent) {
    let dir = match mouse.kind {
        MouseEventKind::ScrollUp => -1,
        MouseEventKind::ScrollDown => 1,
        _ => return,
    };
    session.lock_shared().wheel(dir, mouse.column, mouse.row);
}

fn handle_key(session: &TuiSession, key: KeyEvent) -> KeyResult {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    if ctrl && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C')) {
        if session.lock_shared().busy {
            session.cancel();
            return KeyResult::Continue;
        }
        return KeyResult::Quit;
    }
    if ctrl && matches!(key.code, KeyCode::Char('d') | KeyCode::Char('D')) {
        return if session.lock_shared().busy {
            KeyResult::Continue
        } else {
            KeyResult::Quit
        };
    }
    if ctrl && matches!(key.code, KeyCode::Char('p') | KeyCode::Char('P')) {
        let mut shared = session.lock_shared();
        if shared.plan.is_some() {
            shared.show_plan = !shared.show_plan;
            shared.dirty = true;
        }
        return KeyResult::Continue;
    }
    if ctrl && matches!(key.code, KeyCode::Char('o') | KeyCode::Char('O')) {
        session.lock_shared().toggle_last_tool();
        return KeyResult::Continue;
    }
    if ctrl && matches!(key.code, KeyCode::Char('y') | KeyCode::Char('Y')) {
        let idle = {
            let shared = session.lock_shared();
            !shared.busy && shared.permission.is_none()
        };
        if idle {
            session.copy_last_assistant();
        }
        return KeyResult::Continue;
    }
    if ctrl && matches!(key.code, KeyCode::Char('k') | KeyCode::Char('K')) {
        session.lock_shared().line_up();
        return KeyResult::Continue;
    }
    // Ctrl+J is LF. Some terminals report it as Char('j')+CONTROL, Enter+CONTROL, or Char('\n').
    if (ctrl
        && matches!(
            key.code,
            KeyCode::Char('j') | KeyCode::Char('J') | KeyCode::Enter
        ))
        || matches!(key.code, KeyCode::Char('\n'))
    {
        session.lock_shared().line_down();
        return KeyResult::Continue;
    }

    let asking = session.lock_shared().permission.is_some();
    if asking {
        match key.code {
            KeyCode::Char('y' | 'Y' | 'a' | 'A' | '1') => session.answer_permission(true),
            KeyCode::Char('n' | 'N' | 'd' | 'D' | '2') => session.answer_permission(false),
            KeyCode::PageUp => session.lock_shared().page_up(),
            KeyCode::PageDown => session.lock_shared().page_down(),
            KeyCode::Esc => session.cancel(),
            _ => {}
        }
        return KeyResult::Continue;
    }

    if ctrl && matches!(key.code, KeyCode::Char('a') | KeyCode::Char('A')) {
        session.lock_shared().set_cursor_start();
        return KeyResult::Continue;
    }
    if ctrl && matches!(key.code, KeyCode::Char('e') | KeyCode::Char('E')) {
        session.lock_shared().set_cursor_end();
        return KeyResult::Continue;
    }
    if ctrl && matches!(key.code, KeyCode::Char('u') | KeyCode::Char('U')) {
        session.lock_shared().clear_input();
        return KeyResult::Continue;
    }

    let (slash_open, resume_open) = {
        let shared = session.lock_shared();
        (slash_visible(&shared), shared.resume_ids.is_some())
    };
    let list_open = slash_open || resume_open;
    match key.code {
        KeyCode::PageUp => session.lock_shared().page_up(),
        KeyCode::PageDown => session.lock_shared().page_down(),
        KeyCode::Left => session.lock_shared().move_cursor(-1),
        KeyCode::Right => session.lock_shared().move_cursor(1),
        KeyCode::Home => session.lock_shared().set_cursor_start(),
        KeyCode::End => session.lock_shared().set_cursor_end(),
        KeyCode::Up if list_open => {
            let mut shared = session.lock_shared();
            if shared.slash_selected > 0 {
                shared.slash_selected -= 1;
                shared.dirty = true;
            }
        }
        KeyCode::Down if list_open => {
            let n = session.overlay_cmds().len();
            let mut shared = session.lock_shared();
            if n > 0 && shared.slash_selected + 1 < n {
                shared.slash_selected += 1;
                shared.dirty = true;
            }
        }
        KeyCode::Up => {
            let mut shared = session.lock_shared();
            if !shared.move_cursor_row(-1) {
                shared.history_up();
            }
        }
        KeyCode::Down => {
            let mut shared = session.lock_shared();
            if !shared.move_cursor_row(1) {
                shared.history_down();
            }
        }
        KeyCode::Tab if slash_open => {
            let matches = session.slash_matches();
            let mut shared = session.lock_shared();
            if let Some(cmd) = matches.get(shared.slash_selected) {
                let args = slash_name_args(&shared.input).1;
                shared.input = if args.is_empty() {
                    format!("/{}", cmd.name)
                } else {
                    format!("/{} {args}", cmd.name)
                };
                shared.cursor = shared.input.chars().count();
                shared.dirty = true;
            }
        }
        KeyCode::Char(ch) if !ctrl => session.lock_shared().insert_char(ch),
        KeyCode::Backspace => session.lock_shared().backspace(),
        KeyCode::Delete => session.lock_shared().delete_forward(),
        KeyCode::Enter => {
            if key
                .modifiers
                .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT)
            {
                session.lock_shared().insert_char('\n');
                return KeyResult::Continue;
            }
            {
                let mut shared = session.lock_shared();
                if shared.busy {
                    shared.queue_follow_up();
                    return KeyResult::Continue;
                }
                if let Some(ids) = shared.resume_ids.take() {
                    let idx = shared.slash_selected.min(ids.len().saturating_sub(1));
                    shared.slash_selected = 0;
                    shared.dirty = true;
                    if let Some(id) = ids.get(idx).cloned() {
                        return KeyResult::LoadSession(id);
                    }
                    return KeyResult::Continue;
                }
            }
            let text = session.lock_shared().input.clone();
            if let Some(result) = session.slash_enter(&text) {
                session.lock_shared().take_input();
                return result;
            }
            let (text, pending) = {
                let mut shared = session.lock_shared();
                let text = shared.take_input();
                let pending = std::mem::take(&mut shared.pending_parts);
                (text, pending)
            };
            let trimmed = text.trim();
            if trimmed.is_empty() {
                if !pending.is_empty() {
                    let mut shared = session.lock_shared();
                    shared.pending_parts = pending;
                    shared
                        .transcript
                        .push(TranscriptLine::System("prompt requires a text part".into()));
                    shared.status = "error".into();
                    shared.dirty = true;
                }
            } else {
                let parts = prompt_parts(trimmed, pending);
                return KeyResult::Prompt {
                    display: prompt_display(trimmed, &parts),
                    send: trimmed.to_string(),
                    parts,
                };
            }
        }
        KeyCode::Esc => {
            let mut shared = session.lock_shared();
            if shared.resume_ids.take().is_some() {
                shared.slash_selected = 0;
                shared.dirty = true;
            } else if slash_visible(&shared) {
                shared.slash_dismissed = true;
                shared.dirty = true;
            } else if shared.show_plan {
                shared.show_plan = false;
                shared.dirty = true;
            } else {
                shared.drop_queue();
            }
        }
        _ => {}
    }
    KeyResult::Continue
}

fn handle_paste(session: &TuiSession, text: &str) {
    if session.lock_shared().permission.is_some() {
        return;
    }
    session.lock_shared().insert_str(text);
}

fn dim_style() -> Style {
    Style::new().add_modifier(Modifier::DIM)
}

fn styled_lines(line: &TranscriptLine) -> Vec<Line<'static>> {
    let style = match line {
        TranscriptLine::Assistant(_) => Style::new(),
        _ => dim_style(),
    };
    line_text(line)
        .lines()
        .map(|row| Line::from(Span::styled(row.to_string(), style)))
        .collect()
}

fn draw(frame: &mut Frame, session: &TuiSession) {
    let (slash_cmds, slash_selected) = {
        let cmds = session.overlay_cmds();
        let selected = session.lock_shared().slash_selected;
        (cmds, selected)
    };
    let mut shared = session.lock_shared();
    let area = frame.area();
    let asking = shared.permission.is_some();
    let planning = shared.show_plan && shared.plan.is_some() && !asking;
    let inner = composer_inner(&shared);
    let wrap_w = area.width.max(1);
    shared.composer_w = wrap_w;
    let (prompt_lines, cursor_x, cursor_y) = prompt_block(&shared.input, shared.cursor, wrap_w);
    let slash_cap = area.height.saturating_sub(6).min(8) as usize;
    if !slash_cmds.is_empty() {
        shared.slash_selected = slash_selected.min(slash_cmds.len() - 1);
    }
    let (slash_start, slash_vis) = slash_window(shared.slash_selected, slash_cmds.len(), slash_cap);
    let slash_h = slash_vis as u16;
    let running_row = running_marker_visible(&shared) && slash_h == 0;
    let overlay_h = if slash_h > 0 {
        slash_h
    } else if running_row {
        1
    } else {
        0
    };
    let composer_budget = area.height.saturating_sub(4 + overlay_h).max(1);
    let composer_h = if asking || planning {
        composer_height(&inner, area)
    } else {
        (prompt_lines.len().max(1) as u16).min(composer_budget)
    };
    let mut constraints = vec![Constraint::Min(1)];
    if overlay_h > 0 {
        constraints.push(Constraint::Length(overlay_h));
    }
    constraints.extend([
        Constraint::Length(1),
        Constraint::Length(composer_h),
        Constraint::Length(1),
        Constraint::Length(1),
    ]);
    let chunks = Layout::vertical(constraints).split(area);

    let rows = spaced_rows(&shared.transcript);
    let body = rows.join("\n");
    let mut idx = 0usize;
    let view = chunks[idx];
    idx += 1;
    let content_rows = if body.is_empty() {
        0
    } else {
        wrapped_rows(&body, view.width.max(1))
    };
    let pad = follow_pad(content_rows, view.height, shared.scroll_back);
    let mut lines: Vec<Line> = Vec::new();
    for _ in 0..pad {
        lines.push(Line::default());
    }
    let mut last: Option<u8> = None;
    for line in &shared.transcript {
        let kind = line_kind(line);
        if last.is_some_and(|prev| prev != kind) {
            lines.push(Line::default());
        }
        lines.extend(styled_lines(line));
        last = Some(kind);
    }
    shared.transcript_h = view.height;
    let (follow_y, x) = scroll_offset(&body, view);
    if shared.scroll_back > follow_y {
        shared.scroll_back = follow_y;
    }
    let y = if pad > 0 {
        0
    } else {
        follow_y.saturating_sub(shared.scroll_back)
    };
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((y, x)),
        view,
    );

    if slash_h > 0 {
        let slash_area = chunks[idx];
        idx += 1;
        let name_w = slash_cmds
            .iter()
            .skip(slash_start)
            .take(slash_vis)
            .map(|cmd| cmd.name.len())
            .max()
            .unwrap_or(8)
            .max(8);
        let selected = shared.slash_selected;
        let rows: Vec<Line> = slash_cmds
            .iter()
            .skip(slash_start)
            .take(slash_vis)
            .enumerate()
            .map(|(i, cmd)| {
                let text =
                    truncate_display(&slash_row_text(cmd, name_w), slash_area.width as usize);
                if slash_start + i == selected {
                    Line::from(Span::styled(
                        text,
                        Style::new().add_modifier(Modifier::REVERSED),
                    ))
                } else {
                    Line::from(Span::styled(text, dim_style()))
                }
            })
            .collect();
        frame.render_widget(Paragraph::new(rows), slash_area);
    } else if running_row {
        let overlay = chunks[idx];
        idx += 1;
        let text = truncate_display(&running_marker_row(), overlay.width as usize);
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(text, dim_style()))),
            overlay,
        );
    }

    frame.render_widget(Paragraph::new(rule_line(chunks[idx].width)), chunks[idx]);
    idx += 1;

    let composer = chunks[idx];
    idx += 1;
    let composer_h = composer.height;
    shared.composer_h = composer_h;
    shared.composer_area = composer;
    if asking || planning {
        let max = wrapped_rows(&inner, wrap_w).saturating_sub(composer_h);
        if shared.dock_scroll > max {
            shared.dock_scroll = max;
        }
        let styled: Vec<Line> = inner
            .lines()
            .map(|row| {
                if row.starts_with("> y allow") {
                    Line::from(Span::styled(
                        row.to_string(),
                        Style::new().add_modifier(Modifier::REVERSED),
                    ))
                } else {
                    Line::from(row.to_string())
                }
            })
            .collect();
        frame.render_widget(
            Paragraph::new(styled)
                .wrap(Wrap { trim: false })
                .scroll((shared.dock_scroll, 0)),
            composer,
        );
    } else {
        let scroll = cursor_y.saturating_sub(composer_h.saturating_sub(1));
        let shown: Vec<Line> = prompt_lines
            .iter()
            .skip(scroll as usize)
            .take(composer_h as usize)
            .map(|row| Line::from(row.clone()))
            .collect();
        frame.render_widget(Paragraph::new(shown), composer);
        frame.set_cursor_position(Position {
            x: composer
                .x
                .saturating_add(cursor_x.min(composer.width.saturating_sub(1))),
            y: composer.y.saturating_add(cursor_y.saturating_sub(scroll)),
        });
    }

    frame.render_widget(Paragraph::new(rule_line(chunks[idx].width)), chunks[idx]);
    idx += 1;
    frame.render_widget(
        Paragraph::new(status_line(
            &session.session_id(),
            &shared,
            chunks[idx].width,
        ))
        .style(dim_style()),
        chunks[idx],
    );
    shared.dirty = false;
}

fn wrapped_rows(text: &str, width: u16) -> u16 {
    Paragraph::new(text)
        .wrap(Wrap { trim: false })
        .line_count(width.max(1))
        .min(u16::MAX as usize) as u16
}

fn composer_height(text: &str, area: Rect) -> u16 {
    let rows = wrapped_rows(text, area.width.max(1)).max(1);
    let max = area.height.saturating_sub(4).max(1);
    rows.min(max)
}

fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x
        && row >= area.y
        && column < area.x.saturating_add(area.width)
        && row < area.y.saturating_add(area.height)
}

fn scroll_offset(body: &str, area: Rect) -> (u16, u16) {
    let rows = wrapped_rows(body, area.width.max(1));
    let h = area.height.max(1);
    (rows.saturating_sub(h), 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    use shikigami::config::Config;
    use shikigami::state::StateRoot;
    use tempfile::tempdir;

    fn scripted_host(dir: &Path, script: &str) -> AcpHost {
        let state = StateRoot::new(dir.join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.model.script_json = Some(script.into());
        config.events.adapter = "none".into();
        config.workspace.root = dir.join("ws").to_string_lossy().into();
        let harness = Harness::from_config(config, state).unwrap();
        AcpHost::new(harness)
    }

    async fn wait_until<F>(mut pred: F)
    where
        F: FnMut() -> bool,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        while tokio::time::Instant::now() < deadline {
            if pred() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for TUI state");
    }

    #[tokio::test]
    async fn continue_last_in_cwd_loads_previous_session() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(
            dir.path(),
            r#"[{"content":"hello"},{"content":"again"}]"#,
        ));
        let first = TuiSession::start(Arc::clone(&host), &cwd).await.unwrap();
        first.spawn_prompt("hi".into()).await.unwrap();
        let stop = first.wait_prompt().await;
        assert_eq!(
            stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
            Some(&json!("end_turn"))
        );
        let id = first.session_id();
        assert!(first.transcript_text().contains("hello"));
        drop(first);

        let second = TuiSession::start(host, &cwd).await.unwrap();
        assert_eq!(second.session_id(), id);
        assert!(
            second.transcript_text().contains("hello"),
            "load must replay history: {}",
            second.transcript_text()
        );
    }

    #[tokio::test]
    async fn continue_last_in_cwd_fail_closed_starts_new() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let fake = format!("sess-{}", uuid::Uuid::new_v4());
        let sessions = dir.path().join("state/acp-sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        std::fs::write(
            sessions.join(format!("{fake}.json")),
            serde_json::to_vec(&json!({
                "session_id": fake,
                "cwd": cwd.display().to_string(),
                "run_id": "missing-run"
            }))
            .unwrap(),
        )
        .unwrap();

        let session = TuiSession::start(host, &cwd).await.unwrap();
        assert_ne!(session.session_id(), fake);
        assert!(session.session_id().starts_with("sess-"));
        assert!(session.transcript_text().is_empty());
    }

    #[tokio::test]
    async fn park_mutating_tool_renders_permission_overlay_then_resumes() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        ));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.spawn_prompt("write".into()).await.unwrap();
        wait_until(|| session.overlay_text().is_some()).await;
        let overlay = session.overlay_text().expect("permission overlay");
        assert!(
            overlay.contains("write_file") || overlay.contains("allow"),
            "overlay={overlay}"
        );
        assert!(overlay.contains("y allow"));
        assert!(
            overlay.contains("ok.txt"),
            "ask should show path field: {overlay}"
        );
        assert!(!overlay.contains("{\"path\""), "ask should not dump JSON");
        assert!(!cwd.join("ok.txt").exists());
        session.answer_permission(true);
        let stop = session.wait_prompt().await;
        assert_eq!(
            stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
            Some(&json!("end_turn"))
        );
        assert_eq!(std::fs::read_to_string(cwd.join("ok.txt")).unwrap(), "hi\n");
        assert!(session.transcript_text().contains("wrote it"));
    }

    #[test]
    fn scroll_offset_counts_wrapped_rows() {
        let body = "a".repeat(40);
        let area = Rect::new(0, 0, 10, 2);
        assert_eq!(scroll_offset(&body, area), (2, 0));
        let short = Rect::new(0, 0, 80, 20);
        assert_eq!(scroll_offset("hello", short), (0, 0));
        let words = "123456 123456 123456";
        assert_eq!(scroll_offset(words, Rect::new(0, 0, 10, 2)), (1, 0));
    }

    #[test]
    fn follow_pad_pins_short_transcript_to_the_composer() {
        assert_eq!(follow_pad(2, 20, 0), 18);
        assert_eq!(follow_pad(30, 20, 0), 0);
        assert_eq!(follow_pad(2, 20, 5), 0);
    }

    #[test]
    fn prompt_error_lands_in_transcript_not_the_footer() {
        let mut shared = Shared::new();
        apply_prompt_result(
            &mut shared,
            Some(&json!({
                "error": { "message": "inplace workspace must not contain the harness state directory" }
            })),
        );
        assert_eq!(shared.status, "error");
        assert!(
            shared.transcript.iter().any(
                |line| matches!(line, TranscriptLine::System(text) if text.contains("inplace"))
            )
        );
        let footer = status_line("sess-c0fa033e-aaaa", &shared, 40);
        assert!(footer.contains("error"));
        assert!(!footer.contains("inplace"));
        assert!(footer.chars().count() <= 40);
    }

    #[test]
    fn composer_height_uses_wrapped_rows() {
        let args = "x".repeat(400);
        let text = format!("ask  write_file\n{args}\n> y allow\n  n deny");
        let area = Rect::new(0, 0, 20, 24);
        assert_eq!(composer_height(&text, area), 20);
        assert_eq!(
            composer_height("ask\nok\n> y allow\n  n deny", Rect::new(0, 0, 80, 24)),
            4
        );
    }

    #[tokio::test]
    async fn cancel_after_spawn_is_observed() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        ));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.spawn_prompt("write".into()).await.unwrap();
        session.cancel();
        let stop = session.wait_prompt().await;
        assert_eq!(
            stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
            Some(&json!("cancelled"))
        );
        assert!(!cwd.join("ok.txt").exists());
    }

    async fn start_session(dir: &tempfile::TempDir, script: &str) -> TuiSession {
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), script));
        TuiSession::start(host, &cwd).await.unwrap()
    }

    fn enter_key() -> KeyEvent {
        KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)
    }

    fn user_prompts(session: &TuiSession) -> Vec<String> {
        session
            .lock_shared()
            .transcript
            .iter()
            .filter_map(|line| match line {
                TranscriptLine::User(text) => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    fn queued_display(session: &TuiSession) -> Option<String> {
        session
            .lock_shared()
            .queued
            .as_ref()
            .map(|queued| queued.display.clone())
    }

    async fn spawn_then_queue(session: &TuiSession, text: &str) {
        session.spawn_prompt("write".into()).await.unwrap();
        type_text(session, text);
        assert!(matches!(
            handle_key(session, enter_key()),
            KeyResult::Continue
        ));
    }

    #[tokio::test]
    async fn enter_while_busy_queues_follow_up_without_second_prompt() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        spawn_then_queue(&session, "follow up").await;
        assert_eq!(session.lock_shared().input, "");
        assert_eq!(session.prompt_text(), "> ");
        assert_eq!(queued_display(&session).as_deref(), Some("follow up"));
        assert_eq!(user_prompts(&session), vec!["write".to_string()]);
        assert!(
            session
                .prompt
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some()
        );
        let footer = status_line(&session.session_id(), &session.lock_shared(), 80);
        assert!(footer.contains("queued  follow up"), "{footer}");
        let err = session.spawn_prompt("too soon".into()).await.unwrap_err();
        assert!(err.contains("already in flight"), "{err}");
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn enter_while_busy_replaces_queued_follow_up() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        spawn_then_queue(&session, "first").await;
        type_text(&session, "second");
        assert!(matches!(
            handle_key(&session, enter_key()),
            KeyResult::Continue
        ));
        assert_eq!(queued_display(&session).as_deref(), Some("second"));
        assert_eq!(session.lock_shared().input, "");
        assert_eq!(user_prompts(&session), vec!["write".to_string()]);
        let footer = status_line(&session.session_id(), &session.lock_shared(), 80);
        assert!(footer.contains("queued  second"), "{footer}");
        assert!(!footer.contains("queued  first"), "{footer}");
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn esc_drops_queued_follow_up() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        spawn_then_queue(&session, "follow up").await;
        assert_eq!(queued_display(&session).as_deref(), Some("follow up"));
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
            KeyResult::Continue
        ));
        assert!(queued_display(&session).is_none());
        let footer = status_line(&session.session_id(), &session.lock_shared(), 80);
        assert!(!footer.contains("queued"), "{footer}");
        assert_eq!(user_prompts(&session), vec!["write".to_string()]);
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn esc_hides_plan_before_dropping_queue() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        spawn_then_queue(&session, "follow up").await;
        {
            let mut shared = session.lock_shared();
            shared.plan = Some(Plan {
                entries: vec![PlanEntry {
                    content: "inspect".into(),
                    status: "pending".into(),
                }],
            });
            shared.show_plan = true;
        }
        let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        assert!(matches!(handle_key(&session, esc), KeyResult::Continue));
        assert!(!session.lock_shared().show_plan);
        assert_eq!(queued_display(&session).as_deref(), Some("follow up"));
        assert!(matches!(handle_key(&session, esc), KeyResult::Continue));
        assert!(queued_display(&session).is_none());
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn finish_drains_queued_follow_up_into_session_prompt() {
        let dir = tempdir().unwrap();
        let session = start_session(
            &dir,
            r#"[{"content":"first reply"},{"content":"queued reply"}]"#,
        )
        .await;
        spawn_then_queue(&session, "follow up").await;
        let first = session.wait_prompt().await;
        assert_eq!(
            first.as_ref().and_then(|v| v.pointer("/result/stopReason")),
            Some(&json!("end_turn"))
        );
        session.drain_queued_prompt(first.as_ref()).await.unwrap();
        let second = session.wait_prompt().await;
        assert_eq!(
            second
                .as_ref()
                .and_then(|v| v.pointer("/result/stopReason")),
            Some(&json!("end_turn"))
        );
        assert!(queued_display(&session).is_none());
        assert_eq!(
            user_prompts(&session),
            vec!["write".to_string(), "follow up".to_string()]
        );
        let text = session.transcript_text();
        assert!(text.contains("first reply"), "{text}");
        assert!(text.contains("queued reply"), "{text}");
    }

    #[tokio::test]
    async fn cancel_drops_queued_follow_up_without_sending() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        spawn_then_queue(&session, "follow up").await;
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert!(matches!(handle_key(&session, ctrl_c), KeyResult::Continue));
        assert!(queued_display(&session).is_none());
        let stop = session.wait_prompt().await;
        session.drain_queued_prompt(stop.as_ref()).await.unwrap();
        assert!(queued_display(&session).is_none());
        assert_eq!(user_prompts(&session), vec!["write".to_string()]);
        assert!(
            session
                .prompt
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_none()
        );
        assert!(!session.transcript_text().contains("you  follow up"));
    }

    #[tokio::test]
    async fn slash_while_busy_is_a_character_not_a_list() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        session.spawn_prompt("write".into()).await.unwrap();
        type_text(&session, "/compact");
        assert_eq!(session.lock_shared().input, "/compact");
        assert!(!slash_visible(&session.lock_shared()));
        assert!(session.slash_matches().is_empty());
        assert!(matches!(
            handle_key(&session, enter_key()),
            KeyResult::Continue
        ));
        assert_eq!(queued_display(&session).as_deref(), Some("/compact"));
        assert_eq!(session.lock_shared().input, "");
        let _ = session.wait_prompt().await;
    }

    fn prompt_task_finished(session: &TuiSession) -> bool {
        session
            .prompt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|task| task.is_finished())
    }

    fn draw_if_dirty(session: &TuiSession) -> bool {
        if session.lock_shared().dirty {
            let _ = drawn_text(session);
            true
        } else {
            false
        }
    }

    fn buffer_row(buf: &ratatui::buffer::Buffer, y: u16) -> String {
        let mut out = String::new();
        for x in 0..buf.area().width {
            if let Some(cell) = buf.cell((x, y)) {
                out.push_str(cell.symbol());
            }
        }
        out.trim_end().to_string()
    }

    fn row_is_dim(buf: &ratatui::buffer::Buffer, y: u16) -> bool {
        let mut saw = false;
        for x in 0..buf.area().width {
            let Some(cell) = buf.cell((x, y)) else {
                continue;
            };
            if cell.symbol().trim().is_empty() {
                continue;
            }
            if !cell.modifier.contains(Modifier::DIM) {
                return false;
            }
            saw = true;
        }
        saw
    }

    fn slot_above_composer_rule(session: &TuiSession) -> (String, bool) {
        let backend = ratatui::backend::TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, session)).unwrap();
        let composer_y = session.lock_shared().composer_area.y;
        let buf = terminal.backend().buffer();
        let rule = buffer_row(buf, composer_y.saturating_sub(1));
        assert!(
            !rule.is_empty() && rule.chars().all(|ch| ch == '─'),
            "expected composer rule, got {rule:?}"
        );
        let y = composer_y.saturating_sub(2);
        (buffer_row(buf, y), row_is_dim(buf, y))
    }

    #[tokio::test]
    async fn idle_draw_has_no_running_row_above_composer() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        assert!(!session.lock_shared().busy);
        let (row, _) = slot_above_composer_rule(&session);
        assert_ne!(row.trim(), RUNNING_MARKER, "{row:?}");
        let footer = status_line(&session.session_id(), &session.lock_shared(), 80);
        assert!(!footer.contains(RUNNING_MARKER), "{footer}");
    }

    #[tokio::test]
    async fn spawn_prompt_shows_dim_running_row_until_idle() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        let (idle_row, _) = slot_above_composer_rule(&session);
        assert_ne!(idle_row.trim(), RUNNING_MARKER, "{idle_row:?}");

        session.spawn_prompt("write".into()).await.unwrap();
        assert!(session.lock_shared().busy);
        let (row, dim) = slot_above_composer_rule(&session);
        assert_eq!(row.trim(), RUNNING_MARKER, "{row:?}");
        assert!(dim, "running marker must be dim: {row:?}");

        let _ = session.wait_prompt().await;
        assert!(!session.lock_shared().busy);
        let (done_row, _) = slot_above_composer_rule(&session);
        assert_ne!(done_row.trim(), RUNNING_MARKER, "{done_row:?}");
    }

    #[tokio::test]
    async fn running_marker_skips_idle_ticks() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        assert!(draw_if_dirty(&session));
        for _ in 0..3 {
            assert!(!draw_if_dirty(&session));
        }

        session.spawn_prompt("write".into()).await.unwrap();
        wait_until(|| prompt_task_finished(&session)).await;
        assert!(session.lock_shared().busy);
        let mut frames = 0usize;
        for _ in 0..5 {
            if draw_if_dirty(&session) {
                frames += 1;
            }
        }
        assert_eq!(frames, 1, "busy marker must not force extra idle frames");
        assert!(!session.lock_shared().dirty);
        let (row, dim) = slot_above_composer_rule(&session);
        assert_eq!(row.trim(), RUNNING_MARKER, "{row:?}");
        assert!(dim, "running marker must be dim: {row:?}");

        let _ = session.wait_prompt().await;
        frames = 0;
        for _ in 0..5 {
            if draw_if_dirty(&session) {
                frames += 1;
            }
        }
        assert_eq!(frames, 1);
        assert!(!session.lock_shared().busy);
        let (done_row, _) = slot_above_composer_rule(&session);
        assert_ne!(done_row.trim(), RUNNING_MARKER, "{done_row:?}");
    }

    #[tokio::test]
    async fn running_marker_reserves_composer_height() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        session.spawn_prompt("write".into()).await.unwrap();
        session
            .lock_shared()
            .insert_str(&format!("{}\n", "line").repeat(30));
        let _ = drawn_text(&session);
        let composer_h = session.lock_shared().composer_h;
        assert!(
            composer_h <= 19,
            "busy overlay must stay in the 24-row budget: composer_h={composer_h}"
        );
        let (row, dim) = slot_above_composer_rule(&session);
        assert_eq!(row.trim(), RUNNING_MARKER, "{row:?}");
        assert!(dim, "running marker must be dim: {row:?}");
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn letter_p_types_when_a_plan_exists() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session
            .client
            .notify(
                "session/update",
                json!({
                    "sessionId": session.session_id(),
                    "update": {
                        "sessionUpdate": "plan",
                        "entries": [{"content": "inspect the jail", "status": "pending"}]
                    }
                }),
            )
            .await
            .unwrap();
        let key = KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE);
        assert!(matches!(handle_key(&session, key), KeyResult::Continue));
        assert_eq!(session.lock_shared().input, "p");
    }

    fn wheel(kind: MouseEventKind, column: u16, row: u16) -> Event {
        Event::Mouse(MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        })
    }

    #[tokio::test]
    async fn page_up_scrolls_back_and_new_prompt_returns_to_tail() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.lock_shared().transcript_h = 10;
        let key = KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE);
        assert!(matches!(handle_key(&session, key), KeyResult::Continue));
        assert_eq!(session.lock_shared().scroll_back, 10);
        session.spawn_prompt("next".into()).await.unwrap();
        assert_eq!(session.lock_shared().scroll_back, 0);
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn mouse_scroll_pages_transcript_like_page_keys() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        session.lock_shared().transcript_h = 10;
        assert!(matches!(
            handle_event(&session, wheel(MouseEventKind::ScrollUp, 0, 0)),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 10);
        assert!(matches!(
            handle_event(&session, wheel(MouseEventKind::ScrollDown, 0, 0)),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 0);
    }

    fn ctrl_char(ch: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL)
    }

    #[tokio::test]
    async fn ctrl_jk_scrolls_transcript_one_row() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        session.lock_shared().transcript_h = 10;
        type_text(&session, "hello");
        assert!(matches!(
            handle_key(&session, ctrl_char('k')),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 1);
        assert!(matches!(
            handle_key(&session, ctrl_char('k')),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 2);
        assert!(matches!(
            handle_key(&session, ctrl_char('j')),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 1);
        assert_eq!(session.lock_shared().input, "hello");
        assert!(matches!(
            handle_key(&session, ctrl_char('j')),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 0);
        assert!(matches!(
            handle_key(&session, ctrl_char('j')),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 0);
        assert_eq!(session.lock_shared().input, "hello");
        assert!(matches!(
            handle_key(
                &session,
                KeyEvent::new(KeyCode::Char('\n'), KeyModifiers::CONTROL)
            ),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().input, "hello");
        assert_eq!(session.lock_shared().scroll_back, 0);
        assert!(matches!(
            handle_key(
                &session,
                KeyEvent::new(KeyCode::Char('\n'), KeyModifiers::NONE)
            ),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().input, "hello");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 10);
        session.spawn_prompt("next".into()).await.unwrap();
        assert_eq!(session.lock_shared().scroll_back, 0);
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn plan_update_renders_plan_overlay() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session
            .client
            .notify(
                "session/update",
                json!({
                    "sessionId": session.session_id(),
                    "update": {
                        "sessionUpdate": "plan",
                        "entries": [
                            {"content": "inspect the jail", "status": "pending"}
                        ]
                    }
                }),
            )
            .await
            .unwrap();
        let overlay = session.overlay_text().expect("plan overlay");
        assert!(overlay.contains("inspect the jail"), "overlay={overlay}");
        assert!(overlay.contains("pending"), "overlay={overlay}");
    }

    #[tokio::test]
    async fn tool_call_summarizes_path_from_raw_input() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session
            .client
            .notify(
                "session/update",
                json!({
                    "sessionId": session.session_id(),
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "c1",
                        "title": "write_file",
                        "status": "pending",
                        "rawInput": {"path": "hello.txt", "content": "hi\n"}
                    }
                }),
            )
            .await
            .unwrap();
        let text = session.transcript_text();
        assert!(text.contains("write_file"), "{text}");
        assert!(text.contains("hello.txt"), "{text}");
        assert!(!text.contains("{\"path\""), "{text}");
    }

    fn ctrl_o() -> KeyEvent {
        KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL)
    }

    async fn notify_multiline_tool(session: &TuiSession) {
        session
            .client
            .notify(
                "session/update",
                json!({
                    "sessionId": session.session_id(),
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "t1",
                        "title": "bash",
                        "status": "pending",
                        "rawInput": {"command": "cat notes"}
                    }
                }),
            )
            .await
            .unwrap();
        session
            .client
            .notify(
                "session/update",
                json!({
                    "sessionId": session.session_id(),
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "t1",
                        "status": "completed",
                        "content": [{
                            "type": "content",
                            "content": {
                                "type": "text",
                                "text": "first line of stdout\nlater-unique-line\nthird"
                            }
                        }]
                    }
                }),
            )
            .await
            .unwrap();
    }

    fn drawn_text(session: &TuiSession) -> String {
        let backend = ratatui::backend::TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, session)).unwrap();
        let buf = terminal.backend().buffer();
        let area = buf.area();
        let mut out = String::new();
        for y in 0..area.height {
            for x in 0..area.width {
                if let Some(cell) = buf.cell((x, y)) {
                    out.push_str(cell.symbol());
                }
            }
            out.push('\n');
        }
        out
    }

    fn tool_line_count(text: &str) -> usize {
        text.lines().filter(|line| line.starts_with('·')).count()
    }

    #[tokio::test]
    async fn collapsed_tool_stays_one_line_for_multiline_result() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        notify_multiline_tool(&session).await;
        let text = session.transcript_text();
        assert_eq!(tool_line_count(&text), 1, "{text}");
        assert!(text.contains("first line of stdout"), "{text}");
        assert!(!text.contains("later-unique-line"), "{text}");
        assert!(!drawn_text(&session).contains("later-unique-line"));
    }

    #[tokio::test]
    async fn ctrl_o_expands_last_tool_and_collapses() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        notify_multiline_tool(&session).await;
        type_text(&session, "draft");
        assert!(matches!(
            handle_key(&session, ctrl_o()),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().input, "draft");
        let text = session.transcript_text();
        assert!(text.contains("later-unique-line"), "{text}");
        assert!(tool_line_count(&text) > 1, "{text}");
        let drawn = drawn_text(&session);
        assert!(drawn.contains("later-unique-line"), "{drawn}");
        match session.lock_shared().transcript.last() {
            Some(TranscriptLine::Tool(tool)) => {
                assert!(tool.expanded);
                assert!(tool.detail.contains("later-unique-line"));
                assert_eq!(format_tool(tool), text);
            }
            _ => panic!("expected tool line"),
        }
        assert!(matches!(
            handle_key(&session, ctrl_o()),
            KeyResult::Continue
        ));
        let text = session.transcript_text();
        assert_eq!(tool_line_count(&text), 1, "{text}");
        assert!(!text.contains("later-unique-line"), "{text}");
        assert!(!drawn_text(&session).contains("later-unique-line"));
        match session.lock_shared().transcript.last() {
            Some(TranscriptLine::Tool(tool)) => {
                assert!(!tool.expanded);
                assert!(tool.detail.contains("later-unique-line"));
            }
            _ => panic!("expected tool line"),
        }
        assert_eq!(session.lock_shared().input, "draft");
    }

    #[tokio::test]
    async fn ctrl_o_without_tools_is_a_noop() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "hello");
        session.lock_shared().dirty = false;
        assert!(matches!(
            handle_key(&session, ctrl_o()),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().input, "hello");
        assert!(session.transcript_text().is_empty());
        assert!(!session.lock_shared().dirty);
    }

    fn install_permission(session: &TuiSession, raw_input: String) {
        let (tx, _rx) = oneshot::channel();
        let mut shared = session.lock_shared();
        shared.permission = Some(PendingPermission {
            title: "write_file".into(),
            raw_input,
            tx,
        });
    }

    fn tall_permission_input() -> String {
        let mut lines = Vec::new();
        for i in 0..40 {
            lines.push(format!("arg-line-{i:02}"));
        }
        lines.push("later-unique-arg-line".into());
        json!({ "command": lines.join("\n") }).to_string()
    }

    fn page_key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn page_dock_down_until(session: &TuiSession, needle: &str) -> String {
        let mut text = String::new();
        for _ in 0..8 {
            let before = session.lock_shared().dock_scroll;
            assert!(matches!(
                handle_event(session, Event::Key(page_key(KeyCode::PageDown))),
                KeyResult::Continue
            ));
            text = drawn_text(session);
            if text.contains(needle) || session.lock_shared().dock_scroll == before {
                break;
            }
        }
        text
    }

    #[tokio::test]
    async fn asking_still_pages_transcript() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        install_permission(&session, r#"{"path":"ok.txt"}"#.into());
        let _ = drawn_text(&session);
        assert_eq!(session.lock_shared().dock_max_scroll(), 0);
        session.lock_shared().transcript_h = 10;
        assert!(matches!(
            handle_key(&session, page_key(KeyCode::PageUp)),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 10);
        assert_eq!(session.lock_shared().dock_scroll, 0);
        assert!(session.lock_shared().permission.is_some());
    }

    #[tokio::test]
    async fn overflowing_permission_body_is_reachable() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        install_permission(&session, tall_permission_input());
        let overlay = session.overlay_text().expect("permission overlay");
        assert!(
            overlay.contains("later-unique-arg-line"),
            "ask must keep later argument lines: {overlay}"
        );
        let first = drawn_text(&session);
        assert!(session.lock_shared().dock_max_scroll() > 0);
        assert!(
            !first.contains("later-unique-arg-line"),
            "overflow must clip later arguments until scrolled: {first}"
        );
        let back = session.lock_shared().scroll_back;
        let paged = page_dock_down_until(&session, "later-unique-arg-line");
        assert!(
            paged.contains("later-unique-arg-line"),
            "PageDown on an overflowing dock must reveal later arguments: {paged}"
        );
        assert_eq!(session.lock_shared().scroll_back, back);
        assert!(session.lock_shared().dock_scroll > 0);
        assert!(matches!(
            handle_event(&session, Event::Key(page_key(KeyCode::PageUp))),
            KeyResult::Continue
        ));
        let reset = drawn_text(&session);
        assert!(
            !reset.contains("later-unique-arg-line"),
            "PageUp on a scrolled dock must hide later arguments again: {reset}"
        );
        let (x, y) = {
            let shared = session.lock_shared();
            (shared.composer_area.x, shared.composer_area.y)
        };
        let mut wheeled = reset;
        for _ in 0..8 {
            assert!(matches!(
                handle_event(&session, wheel(MouseEventKind::ScrollDown, x, y)),
                KeyResult::Continue
            ));
            wheeled = drawn_text(&session);
            if wheeled.contains("later-unique-arg-line") {
                break;
            }
        }
        assert!(
            wheeled.contains("later-unique-arg-line"),
            "wheel on the dock must reveal later arguments: {wheeled}"
        );
        assert_eq!(session.lock_shared().scroll_back, back);
        session.lock_shared().dock_scroll = 0;
        session.lock_shared().transcript_h = 10;
        session.lock_shared().scroll_back = 0;
        assert!(matches!(
            handle_event(&session, wheel(MouseEventKind::ScrollUp, 0, 0)),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().scroll_back, 10);
        assert_eq!(session.lock_shared().dock_scroll, 0);
    }

    #[tokio::test]
    async fn cursor_inserts_in_the_middle() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        for ch in ['a', 'b', 'c'] {
            handle_key(
                &session,
                KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE),
            );
        }
        handle_key(&session, KeyEvent::new(KeyCode::Left, KeyModifiers::NONE));
        handle_key(
            &session,
            KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
        );
        assert_eq!(session.lock_shared().input, "abxc");
        assert_eq!(session.lock_shared().cursor, 3);
    }

    #[tokio::test]
    async fn up_recalls_previous_prompt() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.spawn_prompt("first".into()).await.unwrap();
        let _ = session.wait_prompt().await;
        handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(session.lock_shared().input, "first");
        handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(session.lock_shared().input, "");
    }

    #[tokio::test]
    async fn shift_enter_inserts_newline_without_sending() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "hi");
        let shift = KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT);
        assert!(matches!(handle_key(&session, shift), KeyResult::Continue));
        assert_eq!(session.lock_shared().input, "hi\n");
        let alt = KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT);
        assert!(matches!(handle_key(&session, alt), KeyResult::Continue));
        assert_eq!(session.lock_shared().input, "hi\n\n");
        type_text(&session, "there");
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt { display, send, .. } => {
                assert_eq!(display, "hi\n\nthere");
                assert_eq!(send, "hi\n\nthere");
            }
            other => panic!("expected send, got {other:?}"),
        }
    }

    #[test]
    fn paste_inserts_newlines() {
        let mut shared = Shared::new();
        shared.insert_str("error\r\n--> src/tui.rs\n");
        assert_eq!(shared.input, "error\n--> src/tui.rs\n");
        assert_eq!(shared.cursor, shared.input.chars().count());
        shared.insert_str("\tfn");
        assert_eq!(shared.input, "error\n--> src/tui.rs\n fn");
        let mut cr = Shared::new();
        cr.insert_str("first\rsecond");
        assert_eq!(cr.input, "first\nsecond");
    }

    #[tokio::test]
    async fn arrows_move_inside_multiline_draft() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "ab");
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT));
        type_text(&session, "cd");
        assert_eq!(session.lock_shared().input, "ab\ncd");
        assert_eq!(session.lock_shared().cursor, 5);
        handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(session.lock_shared().cursor, 2);
        handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(session.lock_shared().cursor, 5);
        session.lock_shared().clear_input();
        session.spawn_prompt("first".into()).await.unwrap();
        let _ = session.wait_prompt().await;
        type_text(&session, "xy");
        handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(session.lock_shared().input, "first");
    }

    #[test]
    fn session_update_marks_dirty() {
        let mut shared = Shared::new();
        shared.dirty = false;
        apply_update(
            &mut shared,
            &json!({
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": "hi" }
                }
            }),
        );
        assert!(shared.dirty);
        shared.dirty = false;
        apply_update(
            &mut shared,
            &json!({ "update": { "sessionUpdate": "noop" } }),
        );
        assert!(!shared.dirty);
    }

    #[test]
    fn prompt_block_grows_and_wraps() {
        let (lines, x, y) = prompt_block("ab\ncd", 5, 80);
        assert_eq!(lines, vec!["> ab".to_string(), "  cd".to_string()]);
        assert_eq!(y, 1);
        assert_eq!(x, 4);
        let (lines, x, y) = prompt_block("ab", 1, 80);
        assert_eq!(lines, vec!["> ab".to_string()]);
        assert_eq!(y, 0);
        assert_eq!(x, 3);
        let (lines, x, y) = prompt_block("abc", 3, 6);
        assert_eq!(lines, vec!["> abc".to_string()]);
        assert_eq!(y, 0);
        assert_eq!(x, 5);
        let (lines, x, y) = prompt_block("abcdefghij", 10, 6);
        assert_eq!(
            lines,
            vec![
                "> abc".to_string(),
                "  def".to_string(),
                "  ghi".to_string(),
                "  j".to_string()
            ]
        );
        assert_eq!(y, 3);
        assert_eq!(x, 3);
        let (lines, x, y) = prompt_block("中文", 1, 6);
        assert_eq!(lines, vec!["> 中".to_string(), "  文".to_string()]);
        assert_eq!(y, 0);
        assert_eq!(x, 4);
        let (lines, _, y) = prompt_block("中文", 2, 4);
        assert_eq!(lines, vec!["> 中".to_string(), "  文".to_string()]);
        assert_eq!(y, 1);
        let mut shared = Shared::new();
        shared.composer_w = 6;
        shared.insert_str("abc\nabc");
        assert_eq!(shared.cursor, 7);
        assert!(shared.move_cursor_row(-1));
        assert_eq!(shared.cursor, 3);
        shared.clear_input();
        shared.insert_str("abcdef");
        assert_eq!(shared.cursor, 6);
        let (_, x, y) = prompt_block(&shared.input, 3, 6);
        assert_eq!((x, y), (5, 0));
        assert!(shared.move_cursor_row(-1));
        assert_eq!(shared.cursor, 3);
        let (_, x, y) = prompt_block(&shared.input, shared.cursor, 6);
        assert_eq!((x, y), (5, 0));
        assert!(shared.move_cursor_row(1));
        assert_eq!(shared.cursor, 6);
    }

    #[test]
    fn permission_fields_keeps_multiline_command() {
        let body = permission_fields(r#"{"command":"echo checking\nrm -rf /tmp/secret"}"#);
        assert!(body.contains("echo checking"), "{body}");
        assert!(body.contains("rm -rf /tmp/secret"), "{body}");
        let long = "a".repeat(2000);
        let body = permission_fields(&format!(r#"{{"command":"{long}"}}"#));
        assert!(
            body.contains(&long),
            "ask must keep the full command, not a truncated prefix"
        );
    }

    #[test]
    fn slash_window_scrolls_to_keep_selection_visible() {
        assert_eq!(slash_window(0, 12, 8), (0, 8));
        assert_eq!(slash_window(7, 12, 8), (0, 8));
        assert_eq!(slash_window(8, 12, 8), (1, 8));
        assert_eq!(slash_window(11, 12, 8), (4, 8));
        assert_eq!(slash_window(0, 3, 8), (0, 3));
        assert_eq!(slash_window(0, 0, 8), (0, 0));
    }

    fn type_text(session: &TuiSession, text: &str) {
        for ch in text.chars() {
            handle_key(
                session,
                KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE),
            );
        }
    }

    #[tokio::test]
    async fn slash_enter_maps_compact_exit_new_and_unknown() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();

        type_text(&session, "/c");
        assert_eq!(
            session
                .slash_matches()
                .iter()
                .map(|cmd| cmd.name.as_str())
                .collect::<Vec<_>>(),
            vec!["compact", "copy"]
        );
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::Compact
        ));

        type_text(&session, "/exit");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::Quit
        ));

        type_text(&session, "/new");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::NewSession
        ));

        type_text(&session, "/hello");
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt { display, send, .. } => {
                assert_eq!(display, "/hello");
                assert_eq!(send, "/hello");
            }
            other => panic!("expected prompt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn slash_help_writes_system_block_and_stays_idle() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        assert!(session.slash_catalog().iter().any(|cmd| cmd.name == "help"));
        type_text(&session, "/help");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::Continue
        ));
        assert!(session.lock_shared().input.is_empty());
        assert!(!session.lock_shared().busy);
        assert!(session.overlay_text().is_none());
        let text = session.transcript_text();
        assert!(text.contains("Enter"), "{text}");
        assert!(text.contains("Shift+Enter"), "{text}");
        assert!(text.contains("Ctrl-C"), "{text}");
        assert!(text.contains("/compact"), "{text}");
        assert!(text.contains("/new"), "{text}");
        assert!(text.contains("/resume"), "{text}");
        assert!(text.contains("/copy"), "{text}");
        assert!(text.contains("/attach"), "{text}");
        assert!(text.contains("/exit"), "{text}");
        assert!(text.contains("/quit"), "{text}");
        assert!(
            session
                .lock_shared()
                .transcript
                .iter()
                .any(|line| matches!(line, TranscriptLine::System(_))),
            "{text}"
        );
    }

    #[tokio::test]
    async fn slash_attach_sends_acp_image_part() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let png = b"\x89PNG UNIQUE-TUI-401-IMAGE";
        std::fs::write(cwd.join("shot.png"), png).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"saw-image"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        assert!(
            session
                .slash_catalog()
                .iter()
                .any(|cmd| cmd.name == "attach")
        );
        type_text(&session, "/attach shot.png");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::Continue
        ));
        let staged = session.lock_shared().pending_parts.clone();
        assert_eq!(staged.len(), 1, "{staged:?}");
        assert_eq!(staged[0]["type"], "image");
        assert_eq!(staged[0]["mimeType"], "image/png");
        type_text(&session, "inspect");
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt {
                display,
                send,
                parts,
            } => {
                assert_eq!(send, "inspect");
                assert!(display.contains("image/png"), "{display}");
                assert!(!display.contains("UNIQUE-TUI-401-IMAGE"), "{display}");
                assert_eq!(parts[0]["type"], "text");
                assert_eq!(parts[1]["type"], "image");
                session
                    .spawn_prompt_with(display, send, parts)
                    .await
                    .unwrap();
                let stop = session.wait_prompt().await;
                assert_eq!(
                    stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
                    Some(&json!("end_turn"))
                );
            }
            other => panic!("expected prompt, got {other:?}"),
        }
        let leaked = std::fs::read_dir(&cwd)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .any(|entry| {
                let path = entry.path();
                path.file_name().and_then(|name| name.to_str()) != Some("shot.png")
                    && std::fs::read(&path)
                        .ok()
                        .is_some_and(|bytes| bytes.windows(png.len()).any(|window| window == png))
            });
        assert!(!leaked, "attachment bytes copied into the workspace");
    }

    #[tokio::test]
    async fn slash_attach_rejects_oversize_file_before_staging() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let huge = cwd.join("huge.png");
        std::fs::write(&huge, vec![0u8; 8 * 1024 * 1024 + 1]).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"unused"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "/attach huge.png");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::Continue
        ));
        let shared = session.lock_shared();
        assert!(
            shared.pending_parts.is_empty(),
            "{:?}",
            shared.pending_parts
        );
        let system: Vec<&str> = shared
            .transcript
            .iter()
            .filter_map(|line| match line {
                TranscriptLine::System(text) => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            system.iter().any(|text| text.contains("8MiB")),
            "{system:?}"
        );
    }

    fn ctrl_y() -> KeyEvent {
        KeyEvent::new(KeyCode::Char('y'), KeyModifiers::CONTROL)
    }

    async fn session_with_assistant(dir: &tempfile::TempDir, reply: &str) -> TuiSession {
        let session = start_session(dir, &format!(r#"[{{"content":{}}}]"#, json!(reply))).await;
        session.spawn_prompt("hi".into()).await.unwrap();
        let _ = session.wait_prompt().await;
        session
    }

    fn run_copy(session: &TuiSession) {
        assert!(session.slash_catalog().iter().any(|cmd| cmd.name == "copy"));
        type_text(session, "/copy");
        assert!(matches!(
            handle_key(session, enter_key()),
            KeyResult::Continue
        ));
    }

    #[tokio::test]
    async fn slash_copy_records_last_assistant_as_osc52_payload() {
        let dir = tempdir().unwrap();
        let session = session_with_assistant(&dir, "last assistant reply").await;
        run_copy(&session);
        assert_eq!(
            session.lock_shared().last_osc52.as_deref(),
            Some("last assistant reply")
        );
        assert_eq!(session.lock_shared().status, "copied");
        let footer = status_line(&session.session_id(), &session.lock_shared(), 80);
        assert!(footer.contains("copied"), "{footer}");
        let text = session.transcript_text();
        assert!(text.contains("last assistant reply"), "{text}");
        assert!(!text.contains("nothing to copy"), "{text}");
        assert!(
            !session
                .lock_shared()
                .transcript
                .iter()
                .any(|line| matches!(line, TranscriptLine::System(_))),
            "{text}"
        );
        assert_eq!(
            osc52_sequence("last assistant reply"),
            format!(
                "\x1b]52;c;{}\x07",
                base64_encode("last assistant reply".as_bytes())
            )
        );
    }

    #[tokio::test]
    async fn slash_copy_without_assistant_is_a_system_line() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        session
            .lock_shared()
            .transcript
            .push(TranscriptLine::System("keep-me".into()));
        session
            .lock_shared()
            .transcript
            .push(TranscriptLine::User("hi".into()));
        run_copy(&session);
        assert!(session.lock_shared().last_osc52.is_none());
        let text = session.transcript_text();
        assert!(text.contains("keep-me"), "{text}");
        assert!(text.contains("nothing to copy"), "{text}");
        assert_ne!(session.lock_shared().status, "copied");
    }

    #[tokio::test]
    async fn ctrl_y_while_idle_copies_last_assistant() {
        let dir = tempdir().unwrap();
        let session = session_with_assistant(&dir, "idle copy").await;
        type_text(&session, "draft");
        assert!(matches!(
            handle_key(&session, ctrl_y()),
            KeyResult::Continue
        ));
        assert_eq!(session.lock_shared().input, "draft");
        assert_eq!(
            session.lock_shared().last_osc52.as_deref(),
            Some("idle copy")
        );
        assert_eq!(session.lock_shared().status, "copied");
    }

    #[tokio::test]
    async fn ctrl_y_while_busy_does_not_copy() {
        let dir = tempdir().unwrap();
        let session = session_with_assistant(&dir, "previous").await;
        session.spawn_prompt("again".into()).await.unwrap();
        assert!(session.lock_shared().busy);
        session.lock_shared().last_osc52 = None;
        assert!(matches!(
            handle_key(&session, ctrl_y()),
            KeyResult::Continue
        ));
        assert!(session.lock_shared().last_osc52.is_none());
        assert_ne!(session.lock_shared().status, "copied");
        let _ = session.wait_prompt().await;
    }

    #[tokio::test]
    async fn copy_caps_huge_assistant_payload() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        let huge = "x".repeat(OSC52_MAX_BYTES + 64);
        session
            .lock_shared()
            .transcript
            .push(TranscriptLine::Assistant(huge));
        run_copy(&session);
        let payload = session
            .lock_shared()
            .last_osc52
            .clone()
            .expect("capped payload");
        assert_eq!(payload.len(), OSC52_MAX_BYTES);
        assert!(payload.bytes().all(|b| b == b'x'));
        assert_eq!(session.lock_shared().status, "copied");
    }

    #[tokio::test]
    async fn copy_write_error_is_a_system_line() {
        let dir = tempdir().unwrap();
        let session = session_with_assistant(&dir, "secret reply").await;
        session.fail_osc_write();
        run_copy(&session);
        assert!(session.lock_shared().last_osc52.is_none());
        let text = session.transcript_text();
        assert!(text.contains("copy failed"), "{text}");
        assert!(text.contains("secret reply"), "{text}");
        assert_eq!(session.lock_shared().status, "error");
    }

    #[test]
    fn osc52_sequence_encodes_payload() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"hi"), "aGk=");
        assert_eq!(osc52_sequence("hi"), "\x1b]52;c;aGk=\x07");
        assert_eq!(cap_bytes("éé", 3), "é");
        assert_eq!(cap_bytes("abc", 8), "abc");
    }

    #[tokio::test]
    async fn slash_esc_dismisses_list_and_keeps_draft() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "/compact");
        assert!(slash_visible(&session.lock_shared()));
        handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!slash_visible(&session.lock_shared()));
        assert_eq!(session.lock_shared().input, "/compact");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::Compact
        ));
    }

    #[tokio::test]
    async fn slash_arrows_move_selection_not_history() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.spawn_prompt("first".into()).await.unwrap();
        let _ = session.wait_prompt().await;
        type_text(&session, "/");
        handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(session.lock_shared().input, "/");
        assert_eq!(session.lock_shared().slash_selected, 1);
        handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(session.lock_shared().slash_selected, 0);
        assert_eq!(session.lock_shared().input, "/");
    }

    #[tokio::test]
    async fn slash_enter_runs_highlighted_skill_from_prefix() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        let skill_dir = cwd.join(".shikigami/skills/demo");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "/skill:");
        assert_eq!(
            session
                .slash_matches()
                .iter()
                .map(|cmd| cmd.name.as_str())
                .collect::<Vec<_>>(),
            vec!["skill:demo"]
        );
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt { display, send, .. } => {
                assert_eq!(display, "/skill:demo");
                assert!(send.contains("prefer tests first"), "{send}");
            }
            other => panic!("expected highlighted skill prompt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn slash_skill_sends_pack_body() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        let skill_dir = cwd.join(".shikigami/skills/demo");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "/skill:demo extra");
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt { display, send, .. } => {
                assert_eq!(display, "/skill:demo extra");
                assert!(send.contains("prefer tests first"), "{send}");
                assert!(send.contains("extra"), "{send}");
            }
            other => panic!("expected skill prompt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn slash_skill_includes_staged_attachments() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        let skill_dir = cwd.join(".shikigami/skills/demo");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
        let png = b"\x89PNG UNIQUE-TUI-401-SKILL";
        std::fs::write(cwd.join("shot.png"), png).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "/attach shot.png");
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            KeyResult::Continue
        ));
        type_text(&session, "/skill:demo");
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt {
                display,
                send,
                parts,
            } => {
                assert!(display.contains("/skill:demo"), "{display}");
                assert!(display.contains("image/png"), "{display}");
                assert!(send.contains("prefer tests first"), "{send}");
                assert_eq!(parts[0]["type"], "text");
                assert_eq!(parts[1]["type"], "image");
                assert_eq!(parts[1]["mimeType"], "image/png");
            }
            other => panic!("expected skill prompt with attachment, got {other:?}"),
        }
        assert!(
            session.lock_shared().pending_parts.is_empty(),
            "staged parts must be consumed by /skill:"
        );
    }

    #[tokio::test]
    async fn slash_skill_loads_agents_skills() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        let skill_dir = cwd.join(".agents/skills/verify-change");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "run make validate\n").unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        type_text(&session, "/skill:verify-change");
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt { display, send, .. } => {
                assert_eq!(display, "/skill:verify-change");
                assert!(send.contains("run make validate"), "{send}");
            }
            other => panic!("expected agents skill prompt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn slash_new_clears_transcript_and_changes_session() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"hello"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.spawn_prompt("hi".into()).await.unwrap();
        let _ = session.wait_prompt().await;
        assert!(session.transcript_text().contains("hello"));
        let before = session.session_id();
        session.new_session().await.unwrap();
        assert_ne!(session.session_id(), before);
        assert!(session.transcript_text().is_empty());
    }

    fn write_session_file(
        root: &Path,
        session_id: &str,
        cwd: &Path,
        run_id: Option<&str>,
        mtime: SystemTime,
    ) {
        let dir = root.join("state/acp-sessions");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{session_id}.json"));
        std::fs::write(
            &path,
            serde_json::to_vec(&json!({
                "session_id": session_id,
                "cwd": cwd.display().to_string(),
                "run_id": run_id
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
    }

    fn touch_session(root: &Path, session_id: &str, mtime: SystemTime) {
        let path = root
            .join("state/acp-sessions")
            .join(format!("{session_id}.json"));
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
    }

    fn open_resume(session: &TuiSession) {
        type_text(session, "/resume");
        assert!(
            session
                .slash_catalog()
                .iter()
                .any(|cmd| cmd.name == "resume")
        );
        assert!(matches!(
            handle_key(session, enter_key()),
            KeyResult::Continue
        ));
    }

    async fn two_cwd_sessions(dir: &tempfile::TempDir) -> (TuiSession, String, String) {
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(
            dir.path(),
            r#"[{"content":"hello"},{"content":"again"}]"#,
        ));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.spawn_prompt("hi".into()).await.unwrap();
        let _ = session.wait_prompt().await;
        let older = session.session_id();
        session.new_session().await.unwrap();
        let newer = session.session_id();
        let now = SystemTime::now();
        touch_session(dir.path(), &older, now - Duration::from_secs(60));
        touch_session(dir.path(), &newer, now);
        let other = dir.path().join("other");
        std::fs::create_dir_all(&other).unwrap();
        write_session_file(
            dir.path(),
            &format!("sess-{}", uuid::Uuid::new_v4()),
            &other,
            None,
            now + Duration::from_secs(60),
        );
        (session, older, newer)
    }

    #[tokio::test]
    async fn resume_lists_persisted_sessions_for_cwd() {
        let dir = tempdir().unwrap();
        let (session, older, newer) = two_cwd_sessions(&dir).await;
        open_resume(&session);
        let ids = session
            .lock_shared()
            .resume_ids
            .clone()
            .expect("resume picker");
        assert_eq!(ids, vec![newer.clone(), older.clone()]);
        let names: Vec<String> = session
            .overlay_cmds()
            .into_iter()
            .map(|cmd| cmd.name)
            .collect();
        assert_eq!(
            names,
            vec![
                short_session_id(&newer).to_string(),
                short_session_id(&older).to_string()
            ]
        );
        let drawn = drawn_text(&session);
        assert!(drawn.contains(short_session_id(&newer)), "{drawn}");
        assert!(drawn.contains(short_session_id(&older)), "{drawn}");
        assert!(!drawn.contains("acp-sessions"), "{drawn}");
        assert!(
            !drawn.contains(&session.cwd.display().to_string()),
            "{drawn}"
        );
        assert!(names.iter().all(|name| name.len() <= 8));
    }

    #[tokio::test]
    async fn resume_enter_loads_older_session() {
        let dir = tempdir().unwrap();
        let (session, older, newer) = two_cwd_sessions(&dir).await;
        assert_eq!(session.session_id(), newer);
        assert!(session.transcript_text().is_empty());
        open_resume(&session);
        handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        match handle_key(&session, enter_key()) {
            KeyResult::LoadSession(id) => {
                assert_eq!(id, older);
                session.load_session(&id).await;
            }
            other => panic!("expected load, got {other:?}"),
        }
        assert_eq!(session.session_id(), older);
        assert!(session.lock_shared().resume_ids.is_none());
        assert!(
            session.transcript_text().contains("hello"),
            "{}",
            session.transcript_text()
        );
    }

    #[tokio::test]
    async fn resume_esc_keeps_current_session() {
        let dir = tempdir().unwrap();
        let (session, _, newer) = two_cwd_sessions(&dir).await;
        open_resume(&session);
        assert!(session.lock_shared().resume_ids.is_some());
        assert!(matches!(
            handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
            KeyResult::Continue
        ));
        assert!(session.lock_shared().resume_ids.is_none());
        assert_eq!(session.session_id(), newer);
        assert!(session.transcript_text().is_empty());
        assert!(session.overlay_cmds().is_empty());
    }

    #[tokio::test]
    async fn resume_unknown_id_stays_on_current_session() {
        let dir = tempdir().unwrap();
        let (session, _, newer) = two_cwd_sessions(&dir).await;
        session
            .lock_shared()
            .transcript
            .push(TranscriptLine::System("keep-me".into()));
        session.load_session("sess-missing").await;
        assert_eq!(session.session_id(), newer);
        let text = session.transcript_text();
        assert!(text.contains("keep-me"), "{text}");
        assert!(text.contains("unknown session"), "{text}");
        assert!(session.lock_shared().resume_ids.is_none());
    }

    #[tokio::test]
    async fn resume_empty_list_is_a_system_line() {
        let dir = tempdir().unwrap();
        let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
        std::fs::remove_dir_all(dir.path().join("state/acp-sessions")).unwrap();
        open_resume(&session);
        assert!(session.lock_shared().resume_ids.is_none());
        assert!(session.overlay_cmds().is_empty());
        let text = session.transcript_text();
        assert!(text.contains("no sessions"), "{text}");
        assert!(!session.lock_shared().busy);
    }
}
