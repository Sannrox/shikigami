//! Session state, transcript, and the ACP-backed TUI session.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ratatui::prelude::Rect;
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use shikigami::acp::{AcpHost, SessionSummary};
use shikigami::identity::{PRODUCT, VERSION};

use crate::look::Look;
use crate::mention;
use crate::protocol::{apply_load_result, apply_prompt_result, prompt_cancelled};
use crate::render::{
    ago, char_index_at, composer_inner, elapsed_since, prompt_cursor, prompt_rows, rect_contains,
    turns_label, wrapped_rows,
};
use crate::slash::{osc_write_noop, prompt_display, prompt_parts};
use crate::statusline::StatusLine;
use crate::usage::Usage;

/// Printed to the shell after quitting a session that has a run.
pub(crate) const CONTINUE_HINT: &str = "Continue this session later: shikigami tui --continue";

/// `--continue` found no session with a run in this folder.
pub(crate) const NOTHING_TO_CONTINUE: &str =
    "No earlier session in this folder to continue. This is a new session.";

/// `--continue` found a session but could not load it.
pub(crate) const CONTINUE_FAILED: &str =
    "The last session in this folder could not be opened. This is a new session.";

/// `--continue` found the last session held by another process.
pub(crate) const CONTINUE_IN_USE: &str =
    "The last session in this folder is open in another window. This is a new session.";

/// `/resume` picked a session another process holds.
pub(crate) const SESSION_IN_USE: &str =
    "That session is open in another window. Close it there, or pick another one.";

/// The footer's model name. A scripted model has no real name to show.
pub(crate) fn model_label(model: &shikigami::config::ModelSettings) -> String {
    match (model.adapter.as_str(), model.script_json.is_some()) {
        ("scripted", false) => "demo".into(),
        ("scripted", true) => "scripted".into(),
        _ => model.model.clone(),
    }
}

/// Allow/deny keys count only after the permission dock has been drawn and
/// typing has been idle this long. Fail closed: a letter that was part of a
/// word never grants a tool call.
pub(crate) const PERMISSION_ANSWER_SETTLE: Duration = Duration::from_millis(300);

pub(crate) struct ToolLine {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) status: String,
    /// Full rawInput summary or result text. Collapse is `expanded`, not a wipe.
    pub(crate) detail: String,
    pub(crate) expanded: bool,
}

/// One nested child the host reported on the parent session.
pub(crate) struct NestedChild {
    pub(crate) run_id: String,
    pub(crate) profile: String,
    pub(crate) status: String,
}

pub(crate) enum TranscriptLine {
    User(String),
    Assistant(String),
    Tool(ToolLine),
    System(String),
}

pub(crate) struct PlanEntry {
    pub(crate) content: String,
    pub(crate) status: String,
}

pub(crate) struct Plan {
    pub(crate) entries: Vec<PlanEntry>,
}

pub(crate) struct PendingPermission {
    pub(crate) title: String,
    pub(crate) raw_input: String,
    pub(crate) tx: oneshot::Sender<Value>,
    /// Host offered `allow_always` for this ungoverned mutating tool.
    pub(crate) session_allow: bool,
}

pub(crate) struct QueuedPrompt {
    pub(crate) display: String,
    pub(crate) send: String,
    pub(crate) parts: Vec<Value>,
}

pub(crate) struct Shared {
    pub(crate) transcript: Vec<TranscriptLine>,
    pub(crate) permission: Option<PendingPermission>,
    /// Set on the first draw of the current permission dock.
    pub(crate) permission_drawn: bool,
    /// Last composer or discarded pre-prompt key. `None` means idle (a sent
    /// prompt, not a word still being typed).
    pub(crate) last_input_at: Option<Instant>,
    pub(crate) plan: Option<Plan>,
    pub(crate) show_plan: bool,
    pub(crate) status: String,
    pub(crate) busy: bool,
    /// One follow-up to send after the in-flight prompt finishes.
    pub(crate) queued: Option<QueuedPrompt>,
    pub(crate) input: String,
    pub(crate) cursor: usize,
    pub(crate) history: Vec<String>,
    pub(crate) history_idx: Option<usize>,
    pub(crate) history_scratch: String,
    /// Rows above the follow-tail. 0 means stick to the newest output.
    pub(crate) scroll_back: u16,
    pub(crate) transcript_h: u16,
    /// Rows skipped from the top of an overflowing ask/plan dock.
    pub(crate) dock_scroll: u16,
    /// Last drawn composer height; 0 until draw, so undrawn docks do not steal paging.
    pub(crate) composer_h: u16,
    /// Last drawn composer slot; wheel hit-tests this.
    pub(crate) composer_area: Rect,
    /// Last drawn composer width; Up/Down use it to step visual rows.
    pub(crate) composer_w: u16,
    pub(crate) dirty: bool,
    /// Esc hid the `/` list; cleared once the draft no longer starts with `/`.
    pub(crate) slash_dismissed: bool,
    pub(crate) slash_selected: usize,
    /// Start of the `@` mention Esc closed; it stays closed while that word is edited.
    pub(crate) mention_dismissed: Option<usize>,
    /// Workspace files for the `@` list, listed once per draft.
    pub(crate) mention_files: FileList,
    /// Sessions in the open `/resume` picker, most recently active first.
    pub(crate) resume: Option<Vec<SessionSummary>>,
    /// Last OSC 52 clipboard payload written by `/copy` or Ctrl+Y.
    pub(crate) last_osc52: Option<String>,
    /// Staged ACP prompt parts from `/attach` (bytes stay in the prompt, not the workspace).
    pub(crate) pending_parts: Vec<Value>,
    /// The session's run as the host last reported it; `/run` shows it.
    pub(crate) run_id: Option<String>,
    /// Models the session offers, from `session/new` or `session/load`;
    /// `None` when the host offers no switch.
    pub(crate) models: Option<SessionModels>,
    /// First output line of the `[tui.statusline]` command; `None` shows the
    /// default model and turn.
    pub(crate) statusline: Option<String>,
    /// Token use the host reported per turn; the footer and `/usage` show it.
    pub(crate) usage: Usage,
    /// `[run] context_window_tokens` from the host; the footer percent uses it.
    pub(crate) context_window_tokens: Option<u64>,
    /// Nested children the host last reported; the dim slot above the composer.
    pub(crate) nested_children: Vec<NestedChild>,
    /// Standing attachments the host last reported for this run; `/context`.
    pub(crate) context: Option<Value>,
    /// Last `/compact` that shrank history; `/context` shows the counts.
    pub(crate) last_compact: Option<(u64, u64)>,
    /// Last drawn overlay height (slash list, nested children, or running).
    pub(crate) overlay_h: u16,
}

impl Shared {
    pub(crate) fn new() -> Self {
        Self {
            transcript: Vec::new(),
            permission: None,
            permission_drawn: false,
            last_input_at: None,
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
            mention_dismissed: None,
            mention_files: FileList::NotRead,
            resume: None,
            last_osc52: None,
            pending_parts: Vec::new(),
            run_id: None,
            models: None,
            statusline: None,
            usage: Usage::default(),
            context_window_tokens: None,
            nested_children: Vec::new(),
            context: None,
            last_compact: None,
            overlay_h: 0,
        }
    }

    pub(crate) fn note_input(&mut self) {
        self.last_input_at = Some(Instant::now());
    }

    pub(crate) fn mark_input_idle(&mut self) {
        self.last_input_at = None;
    }

    pub(crate) fn begin_permission(&mut self, pending: PendingPermission) {
        self.status = "ask".into();
        self.permission = Some(pending);
        self.permission_drawn = false;
        self.dock_scroll = 0;
        self.dirty = true;
    }

    pub(crate) fn clear_permission(&mut self) -> Option<PendingPermission> {
        self.permission_drawn = false;
        self.permission.take()
    }

    pub(crate) fn permission_answers_live(&self, now: Instant) -> bool {
        self.permission.is_some()
            && self.permission_drawn
            && self
                .last_input_at
                .is_none_or(|at| now.saturating_duration_since(at) >= PERMISSION_ANSWER_SETTLE)
    }

    pub(crate) fn insert_char(&mut self, ch: char) {
        self.insert_str(&ch.to_string());
    }

    pub(crate) fn insert_str(&mut self, text: &str) {
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
        self.note_input();
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

    pub(crate) fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.note_input();
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

    pub(crate) fn delete_forward(&mut self) {
        let mut chars: Vec<char> = self.input.chars().collect();
        let i = self.cursor.min(chars.len());
        if i >= chars.len() {
            return;
        }
        self.note_input();
        chars.remove(i);
        self.input = chars.into_iter().collect();
        self.history_idx = None;
        self.slash_selected = 0;
        self.sync_slash_dismissed();
        self.dirty = true;
    }

    pub(crate) fn move_cursor(&mut self, delta: isize) {
        let len = self.input.chars().count();
        let next = self.cursor.saturating_add_signed(delta).min(len);
        if next != self.cursor {
            self.cursor = next;
            self.dirty = true;
        }
    }

    pub(crate) fn set_cursor_end(&mut self) {
        let len = self.input.chars().count();
        if self.cursor != len {
            self.cursor = len;
            self.dirty = true;
        }
    }

    pub(crate) fn set_cursor_start(&mut self) {
        if self.cursor != 0 {
            self.cursor = 0;
            self.dirty = true;
        }
    }

    pub(crate) fn clear_input(&mut self) {
        if self.input.is_empty() && self.cursor == 0 {
            return;
        }
        self.input.clear();
        self.cursor = 0;
        self.history_idx = None;
        self.slash_dismissed = false;
        self.slash_selected = 0;
        self.mention_dismissed = None;
        self.mention_files = FileList::NotRead;
        self.dirty = true;
    }

    pub(crate) fn take_input(&mut self) -> String {
        let text = std::mem::take(&mut self.input);
        self.mark_input_idle();
        self.cursor = 0;
        self.history_idx = None;
        self.show_plan = false;
        self.scroll_back = 0;
        self.dock_scroll = 0;
        self.slash_dismissed = false;
        self.slash_selected = 0;
        self.mention_dismissed = None;
        self.mention_files = FileList::NotRead;
        self.dirty = true;
        text
    }

    /// Store or replace the one-slot follow-up from the current draft.
    pub(crate) fn queue_follow_up(&mut self) {
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

    pub(crate) fn drop_queue(&mut self) {
        if self.queued.take().is_some() {
            self.dirty = true;
        }
    }

    pub(crate) fn sync_slash_dismissed(&mut self) {
        if !self.input.starts_with('/') {
            self.slash_dismissed = false;
        }
        // Esc closed one mention word; once that word is gone, `@` opens again.
        if self.mention_dismissed.is_some()
            && mention::mention_at(&self.input, self.cursor).map(|(start, _)| start)
                != self.mention_dismissed
        {
            self.mention_dismissed = None;
        }
    }

    pub(crate) fn history_up(&mut self) {
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

    pub(crate) fn history_down(&mut self) {
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

    pub(crate) fn dock_open(&self) -> bool {
        self.permission.is_some() || (self.show_plan && self.plan.is_some())
    }

    pub(crate) fn dock_max_scroll(&self) -> u16 {
        if !self.dock_open() || self.composer_h == 0 {
            return 0;
        }
        wrapped_rows(&composer_inner(self), self.composer_w.max(1)).saturating_sub(self.composer_h)
    }

    /// Scroll an overflowing dock. False means no dock overflow or already at that edge.
    pub(crate) fn try_scroll_dock(&mut self, dir: i16) -> bool {
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

    pub(crate) fn scroll_transcript(&mut self, rows: u16, up: bool) {
        self.scroll_back = if up {
            self.scroll_back.saturating_add(rows)
        } else {
            self.scroll_back.saturating_sub(rows)
        };
        self.dirty = true;
    }

    pub(crate) fn page_transcript_up(&mut self) {
        self.scroll_transcript(self.transcript_h.max(1), true);
    }

    pub(crate) fn page_transcript_down(&mut self) {
        self.scroll_transcript(self.transcript_h.max(1), false);
    }

    pub(crate) fn line_up(&mut self) {
        self.scroll_transcript(1, true);
    }

    pub(crate) fn line_down(&mut self) {
        self.scroll_transcript(1, false);
    }

    pub(crate) fn page_up(&mut self) {
        if self.try_scroll_dock(-1) {
            return;
        }
        self.page_transcript_up();
    }

    pub(crate) fn page_down(&mut self) {
        if self.try_scroll_dock(1) {
            return;
        }
        self.page_transcript_down();
    }

    /// Wheel over an overflowing dock scrolls it; otherwise (or at the edge) page the transcript.
    pub(crate) fn wheel(&mut self, dir: i16, column: u16, row: u16) {
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
    pub(crate) fn toggle_last_tool(&mut self) {
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
    pub(crate) fn move_cursor_row(&mut self, delta: isize) -> bool {
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
pub(crate) struct TuiClient {
    pub(crate) shared: Arc<Mutex<Shared>>,
    pub(crate) session_id: Arc<Mutex<String>>,
}

/// The `@` list's workspace files for the current draft.
#[derive(Default)]
pub(crate) enum FileList {
    #[default]
    NotRead,
    /// A background walk is listing them.
    Reading,
    Ready(Arc<Vec<String>>),
}

/// A session's model catalog as the host offers it (ACP `models`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionModels {
    pub(crate) current: String,
    pub(crate) available: Vec<String>,
}

impl SessionModels {
    pub(crate) fn of(result: &Value) -> Option<Self> {
        let models = result.get("models")?;
        Some(Self {
            current: models.get("currentModelId")?.as_str()?.to_string(),
            available: models
                .get("availableModels")?
                .as_array()?
                .iter()
                .filter_map(|m| m.get("modelId")?.as_str().map(str::to_string))
                .collect(),
        })
    }

    /// `/model` without a name: the offered models, the current one marked.
    pub(crate) fn listing(&self) -> String {
        let names: Vec<String> = self
            .available
            .iter()
            .map(|id| {
                if *id == self.current {
                    format!("{id} (current)")
                } else {
                    id.clone()
                }
            })
            .collect();
        format!(
            "Models: {}\n/model <name> switches from the next prompt.",
            names.join(", ")
        )
    }
}

pub(crate) struct SlashCommand {
    pub(crate) name: String,
    pub(crate) hint: String,
}

pub(crate) struct SessionBackup {
    pub(crate) transcript: Vec<TranscriptLine>,
    pub(crate) plan: Option<Plan>,
    pub(crate) show_plan: bool,
    pub(crate) queued: Option<QueuedPrompt>,
    pub(crate) scroll_back: u16,
    pub(crate) dock_scroll: u16,
    pub(crate) nested_children: Vec<NestedChild>,
}

pub(crate) type OscWrite = fn(&[u8]) -> io::Result<()>;

pub(crate) struct TuiSession {
    pub(crate) host: Arc<AcpHost>,
    pub(crate) client: TuiClient,
    pub(crate) session_id: Arc<Mutex<String>>,
    pub(crate) cwd: std::path::PathBuf,
    pub(crate) next_id: AtomicU64,
    pub(crate) prompt: Mutex<Option<JoinHandle<Option<Value>>>>,
    pub(crate) osc_write: Mutex<OscWrite>,
    /// Model name for the footer; empty hides it.
    pub(crate) model: String,
    /// The configured model id; a session on another one (`/model`) shows
    /// that id in the footer instead of `model`.
    pub(crate) default_model_id: String,
    /// Key bindings and colors from `[tui]`.
    pub(crate) look: Look,
    /// `[tui.statusline]` command, when configured.
    pub(crate) statusline: Option<StatusLine>,
}

impl TuiSession {
    pub(crate) async fn start(
        host: Arc<AcpHost>,
        cwd: &Path,
        continue_last: bool,
    ) -> Result<Self, String> {
        let cwd = if cwd.is_absolute() {
            cwd.to_path_buf()
        } else {
            cwd.canonicalize().map_err(|e| e.to_string())?
        };
        let default_model_id = host.configured_model();
        let context_window_tokens = host.context_window_tokens();
        let session_id = Arc::new(Mutex::new(String::new()));
        let mut shared = Shared::new();
        shared.context_window_tokens = context_window_tokens;
        let session = Self {
            host,
            client: TuiClient {
                shared: Arc::new(Mutex::new(shared)),
                session_id: Arc::clone(&session_id),
            },
            session_id,
            cwd: cwd.clone(),
            next_id: AtomicU64::new(1),
            prompt: Mutex::new(None),
            osc_write: Mutex::new(osc_write_noop),
            model: String::new(),
            default_model_id,
            look: Look::default(),
            statusline: None,
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
        let session_id = if continue_last {
            session.continue_latest(&cwd).await?
        } else {
            session.create_session(&cwd).await?
        };
        *session.session_id.lock().unwrap_or_else(|e| e.into_inner()) = session_id;
        session.lock_shared().dirty = true;
        Ok(session)
    }

    pub(crate) fn session_id(&self) -> String {
        self.session_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `--continue`: load the most recently active session for `cwd`. When
    /// there is none, or it fails to load, start a new one and say so.
    pub(crate) async fn continue_latest(&self, cwd: &Path) -> Result<String, String> {
        let latest = match self.list_sessions(Some(cwd)).await {
            Ok(sessions) => sessions.into_iter().next(),
            Err(_) => {
                let id = self.create_session(cwd).await?;
                self.push_system(CONTINUE_FAILED);
                return Ok(id);
            }
        };
        let Some(latest) = latest else {
            let id = self.create_session(cwd).await?;
            self.push_system(NOTHING_TO_CONTINUE);
            return Ok(id);
        };
        if latest.in_use {
            let id = self.create_session(cwd).await?;
            self.push_system(CONTINUE_IN_USE);
            return Ok(id);
        }
        match self
            .call("session/load", json!({ "sessionId": latest.session_id }))
            .await
        {
            Ok(result) => {
                let mut shared = self.lock_shared();
                apply_load_result(&mut shared, &result);
                shared.transcript.push(TranscriptLine::System(format!(
                    "Continued your session from {} ({}).",
                    ago(elapsed_since(latest.last_active)),
                    turns_label(latest.prompts)
                )));
                Ok(latest.session_id)
            }
            Err(err) => {
                let id = self.create_session(cwd).await?;
                // Another window may take it between the list and the load.
                self.push_system(if err == shikigami::acp::SESSION_IN_USE {
                    CONTINUE_IN_USE
                } else {
                    CONTINUE_FAILED
                });
                Ok(id)
            }
        }
    }

    /// `session/list` for this cwd, or every session in the state root.
    pub(crate) async fn list_sessions(
        &self,
        cwd: Option<&Path>,
    ) -> Result<Vec<SessionSummary>, String> {
        let mut params = json!({});
        if let Some(cwd) = cwd {
            params["cwd"] = json!(cwd.display().to_string());
        }
        let result = self.call("session/list", params).await?;
        SessionSummary::from_list(&result)
    }

    pub(crate) fn push_system(&self, text: &str) {
        let mut shared = self.lock_shared();
        shared.transcript.push(TranscriptLine::System(text.into()));
        shared.dirty = true;
    }

    pub(crate) async fn create_session(&self, cwd: &Path) -> Result<String, String> {
        let result = self
            .call(
                "session/new",
                json!({
                    "cwd": cwd.display().to_string(),
                    "mcpServers": []
                }),
            )
            .await?;
        self.lock_shared().models = SessionModels::of(&result);
        result
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| "session/new did not return sessionId".into())
    }

    pub(crate) async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
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

    pub(crate) fn lock_shared(&self) -> std::sync::MutexGuard<'_, Shared> {
        self.client.shared.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[cfg(test)]
    pub(crate) async fn spawn_prompt(&self, text: String) -> Result<(), String> {
        self.spawn_prompt_with(text.clone(), text, Vec::new(), None)
            .await
    }

    pub(crate) async fn spawn_prompt_with(
        &self,
        display: String,
        send: String,
        parts: Vec<Value>,
        load_skill: Option<String>,
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
        let mut params = json!({
            "sessionId": session_id,
            "prompt": prompt
        });
        if let Some(skill) = load_skill {
            params["_meta"]["shikigami"]["loadSkill"] = json!(skill);
        }
        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/prompt",
            "params": params
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

    pub(crate) async fn wait_prompt(&self) -> Option<Value> {
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
    pub(crate) async fn drain_queued_prompt(&self, last: Option<&Value>) -> Result<(), String> {
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
            }) => self.spawn_prompt_with(display, send, parts, None).await,
            None => Ok(()),
        }
    }

    pub(crate) fn cancel(&self) {
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
        if let Some(pending) = shared.clear_permission() {
            let _ = pending.tx.send(json!({
                "outcome": { "outcome": "cancelled" }
            }));
            shared.dock_scroll = 0;
        }
        shared.dirty = true;
    }

    pub(crate) fn answer_permission(&self, allow: bool) {
        let option = if allow { "allow" } else { "deny" };
        self.send_permission(option, option);
    }

    pub(crate) fn answer_session_permission(&self) {
        self.send_permission("selected", "allow_session");
    }

    fn send_permission(&self, outcome: &str, option_id: &str) {
        let mut shared = self.lock_shared();
        let Some(pending) = shared.clear_permission() else {
            return;
        };
        let _ = pending.tx.send(json!({
            "outcome": { "outcome": outcome, "optionId": option_id }
        }));
        shared.dock_scroll = 0;
        shared.dirty = true;
    }
}

#[derive(Debug)]
pub(crate) enum KeyResult {
    Continue,
    Quit,
    Prompt {
        display: String,
        send: String,
        parts: Vec<Value>,
        load_skill: Option<String>,
    },
    Compact,
    NewSession,
    LoadSession(String),
    /// `/model <id>`: switch later prompts to `id`.
    SetModel(String),
    /// `/mcp` list, or disable/enable one attached server for this process.
    Mcp {
        action: String,
        name: Option<String>,
    },
    /// `/plan`: flag the next prompt of this session for plan write-jail.
    Plan,
    /// `/resume`: list this cwd's sessions via `session/list`, then pick.
    ResumePicker,
}
