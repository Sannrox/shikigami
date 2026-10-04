//! Thin interactive process host: ACP client of the in-process session.
//!
//! Evolving surface, same rank as `shikigami acp`. Not freeze-core.
//! See [ADR 0014](../docs/decisions/0014-usable-guest-hosts.md).

use std::io::{IsTerminal, stdin, stdout};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::{Constraint, CrosstermBackend, Layout, Rect, Style, Stylize};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::acp::{AcpClient, AcpHost};
use crate::harness::Harness;
use crate::identity::{PRODUCT, VERSION};

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
    detail: String,
}

enum TranscriptLine {
    User(String),
    Assistant(String),
    Tool(ToolLine),
    System(String),
}

struct Plan {
    entries: Vec<String>,
}

struct PendingPermission {
    title: String,
    raw_input: String,
    tx: oneshot::Sender<Value>,
}

struct Shared {
    transcript: Vec<TranscriptLine>,
    permission: Option<PendingPermission>,
    plan: Option<Plan>,
    show_plan: bool,
    status: String,
    busy: bool,
    input: String,
}

#[derive(Clone)]
struct TuiClient {
    shared: Arc<Mutex<Shared>>,
}

struct TuiSession {
    host: Arc<AcpHost>,
    client: TuiClient,
    session_id: String,
    next_id: AtomicU64,
    prompt: Mutex<Option<JoinHandle<Option<Value>>>>,
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
                shared: Arc::new(Mutex::new(Shared {
                    transcript: Vec::new(),
                    permission: None,
                    plan: None,
                    show_plan: false,
                    status: "ready".into(),
                    busy: false,
                    input: String::new(),
                })),
            },
            session_id: String::new(),
            next_id: AtomicU64::new(1),
            prompt: Mutex::new(None),
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
        let mut started = session;
        started.session_id = session_id;
        Ok(started)
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

    async fn spawn_prompt(&self, text: String) -> Result<(), String> {
        {
            let mut shared = self.lock_shared();
            if shared.busy {
                return Err("prompt already in flight".into());
            }
            shared.busy = true;
            shared.status = "running".into();
            shared.transcript.push(TranscriptLine::User(text.clone()));
        }

        self.host.arm_cancel(&self.session_id).await;

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/prompt",
            "params": {
                "sessionId": self.session_id,
                "prompt": [{"type": "text", "text": text}]
            }
        });
        let host = Arc::clone(&self.host);
        let client = self.client.clone();
        let shared = Arc::clone(&self.client.shared);
        let task = tokio::spawn(async move {
            let resp = host.handle(msg, &client).await;
            let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
            state.busy = false;
            state.status = match resp.as_ref().and_then(|v| v.pointer("/result/stopReason")) {
                Some(Value::String(reason)) => reason.clone(),
                _ => resp
                    .as_ref()
                    .and_then(|v| v.pointer("/error/message"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("ready")
                    .to_string(),
            };
            resp
        });
        *self.prompt.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        Ok(())
    }

    async fn wait_prompt(&self) -> Option<Value> {
        let task = self.prompt.lock().unwrap_or_else(|e| e.into_inner()).take();
        match task {
            Some(task) => task.await.ok().flatten(),
            None => None,
        }
    }

    fn cancel(&self) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/cancel",
            "params": { "sessionId": self.session_id }
        });
        let host = Arc::clone(&self.host);
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = host.handle(msg, &client).await;
        });
        if let Some(pending) = self.lock_shared().permission.take() {
            let _ = pending.tx.send(json!({
                "outcome": { "outcome": "cancelled" }
            }));
        }
    }

    fn answer_permission(&self, allow: bool) {
        let Some(pending) = self.lock_shared().permission.take() else {
            return;
        };
        let outcome = if allow { "allow" } else { "deny" };
        let _ = pending.tx.send(json!({
            "outcome": { "outcome": outcome, "optionId": outcome }
        }));
    }

    #[cfg(test)]
    fn overlay_text(&self) -> Option<String> {
        let shared = self.lock_shared();
        if let Some(pending) = shared.permission.as_ref() {
            return Some(format_permission(pending));
        }
        if shared.show_plan
            && let Some(plan) = shared.plan.as_ref()
        {
            return Some(format_plan(plan));
        }
        None
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
}

fn line_text(line: &TranscriptLine) -> String {
    match line {
        TranscriptLine::User(text) => format!("> {text}"),
        TranscriptLine::Assistant(text) => text.clone(),
        TranscriptLine::Tool(tool) => {
            if tool.detail.is_empty() {
                format!("{} {}", tool.title, tool.status)
            } else {
                format!("{} {}: {}", tool.title, tool.status, tool.detail)
            }
        }
        TranscriptLine::System(text) => text.clone(),
    }
}

fn format_permission(pending: &PendingPermission) -> String {
    format!(
        "allow {}?\n{}\ny allow  n deny",
        pending.title, pending.raw_input
    )
}

fn format_plan(plan: &Plan) -> String {
    let mut out = String::from("plan\n");
    for (i, entry) in plan.entries.iter().enumerate() {
        out.push_str(&format!("{}. {entry}\n", i + 1));
    }
    out
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
            shared.transcript.push(TranscriptLine::Tool(ToolLine {
                id,
                title,
                status,
                detail: String::new(),
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
                    entry
                        .get("content")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                })
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                shared.plan = Some(Plan { entries });
                shared.show_plan = true;
            }
        }
        _ => {}
    }
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

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut out = stdout();
        let _ = execute!(out, LeaveAlternateScreen, crossterm::cursor::Show);
    }
}

async fn run_terminal(session: TuiSession) -> Result<(), String> {
    enable_raw_mode().map_err(|e| e.to_string())?;
    let _guard = TerminalGuard;
    let mut out = stdout();
    execute!(out, EnterAlternateScreen).map_err(|e| e.to_string())?;
    let mut terminal = Terminal::new(CrosstermBackend::new(out)).map_err(|e| e.to_string())?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        while let Ok(ev) = event::read() {
            if tx.send(ev).is_err() {
                break;
            }
        }
    });

    loop {
        terminal
            .draw(|frame| draw(frame, &session))
            .map_err(|e| e.to_string())?;

        let finished = {
            let guard = session.prompt.lock().unwrap_or_else(|e| e.into_inner());
            guard.as_ref().is_some_and(|task| task.is_finished())
        };
        if finished {
            let _ = session.wait_prompt().await;
        }

        let ev = match tokio::time::timeout(Duration::from_millis(80), rx.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) => break,
            Err(_) => continue,
        };
        match ev {
            Event::Key(key) if key.kind == event::KeyEventKind::Press => {
                match handle_key(&session, key) {
                    KeyResult::Quit => break,
                    KeyResult::Prompt(text) => {
                        if let Err(err) = session.spawn_prompt(text).await {
                            session.lock_shared().status = err;
                        }
                    }
                    KeyResult::Continue => {}
                }
            }
            Event::Resize(_, _) => {}
            _ => {}
        }
    }
    Ok(())
}

enum KeyResult {
    Continue,
    Quit,
    Prompt(String),
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
        }
        return KeyResult::Continue;
    }

    let asking = session.lock_shared().permission.is_some();
    if asking {
        match key.code {
            KeyCode::Char('y' | 'Y' | 'a' | 'A' | '1') => session.answer_permission(true),
            KeyCode::Char('n' | 'N' | 'd' | 'D' | '2') => session.answer_permission(false),
            KeyCode::Esc => session.cancel(),
            _ => {}
        }
        return KeyResult::Continue;
    }

    match key.code {
        KeyCode::Char(ch) if !ctrl => session.lock_shared().input.push(ch),
        KeyCode::Backspace => {
            session.lock_shared().input.pop();
        }
        KeyCode::Enter => {
            if session.lock_shared().busy {
                return KeyResult::Continue;
            }
            let text = {
                let mut shared = session.lock_shared();
                let text = std::mem::take(&mut shared.input);
                shared.show_plan = false;
                text
            };
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return KeyResult::Prompt(trimmed.to_string());
            }
        }
        KeyCode::Esc => {
            session.lock_shared().show_plan = false;
        }
        _ => {}
    }
    KeyResult::Continue
}

fn draw(frame: &mut Frame, session: &TuiSession) {
    let shared = session.lock_shared();
    let overlay = if let Some(pending) = shared.permission.as_ref() {
        Some(format_permission(pending))
    } else if shared.show_plan {
        shared.plan.as_ref().map(format_plan)
    } else {
        None
    };
    let overlay_h = overlay
        .as_ref()
        .map(|text| (text.lines().count() as u16 + 2).clamp(3, 10))
        .unwrap_or(0);

    let area = frame.area();
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(overlay_h),
        Constraint::Length(1),
    ])
    .split(area);

    let status = format!("{PRODUCT} tui  {}  {}", session.session_id, shared.status);
    frame.render_widget(Paragraph::new(status), chunks[0]);

    let body = shared
        .transcript
        .iter()
        .map(line_text)
        .collect::<Vec<_>>()
        .join("\n");
    let offset = scroll_offset(&body, chunks[1]);
    frame.render_widget(
        Paragraph::new(body)
            .wrap(Wrap { trim: false })
            .scroll(offset),
        chunks[1],
    );

    if let Some(text) = overlay {
        let title = if shared.permission.is_some() {
            "permission"
        } else {
            "plan"
        };
        frame.render_widget(
            Paragraph::new(text)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(title)),
            chunks[2],
        );
    }

    let prompt = if shared.busy && shared.permission.is_none() {
        format!("{}…", shared.status)
    } else {
        format!("> {}", shared.input)
    };
    frame.render_widget(
        Paragraph::new(prompt).style(Style::new().white()),
        chunks[3],
    );
}

fn scroll_offset(body: &str, area: Rect) -> (u16, u16) {
    let width = area.width.max(1) as usize;
    let mut rows: u16 = 0;
    for line in body.lines() {
        let chars = line.chars().count().max(1);
        let wrapped = chars.div_ceil(width) as u16;
        rows = rows.saturating_add(wrapped);
    }
    let h = area.height.max(1);
    (rows.saturating_sub(h), 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::state::StateRoot;
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
        let id = first.session_id.clone();
        assert!(first.transcript_text().contains("hello"));
        drop(first);

        let second = TuiSession::start(host, &cwd).await.unwrap();
        assert_eq!(second.session_id, id);
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
        assert_ne!(session.session_id, fake);
        assert!(session.session_id.starts_with("sess-"));
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

    #[tokio::test]
    async fn enter_while_busy_keeps_draft() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
        let session = TuiSession::start(host, &cwd).await.unwrap();
        session.lock_shared().busy = true;
        session.lock_shared().input = "follow up".into();
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
        assert!(matches!(handle_key(&session, key), KeyResult::Continue));
        assert_eq!(session.lock_shared().input, "follow up");
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
                    "sessionId": session.session_id,
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
                    "sessionId": session.session_id,
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
    }
}
