use crate::input::{handle_event, handle_key};
use crate::render::draw;
use crate::session::*;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
use ratatui::Terminal;
use serde_json::{Value, json};
use shikigami::acp::{AcpClient, AcpHost};
use shikigami::config::Config;
use shikigami::harness::Harness;
use shikigami::state::StateRoot;
use tokio::sync::oneshot;

pub(super) fn scripted_host(dir: &Path, script: &str) -> AcpHost {
    scripted_host_with(dir, script, |_| {})
}

pub(super) fn scripted_host_with(
    dir: &Path,
    script: &str,
    tweak: impl FnOnce(&mut Config),
) -> AcpHost {
    let state = StateRoot::new(dir.join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.into());
    config.events.adapter = "none".into();
    config.workspace.root = dir.join("ws").to_string_lossy().into();
    tweak(&mut config);
    let harness = Harness::from_config(config, state).unwrap();
    AcpHost::new(harness)
}

pub(super) async fn wait_until<F>(mut pred: F)
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

/// #598: a plain start is a new session; `--continue` replays the
/// latest one and says how old it is.
pub(super) async fn start_session(dir: &tempfile::TempDir, script: &str) -> TuiSession {
    start_session_with(dir, script, |_| {}).await
}

pub(super) async fn start_session_with(
    dir: &tempfile::TempDir,
    script: &str,
    tweak: impl FnOnce(&mut Config),
) -> TuiSession {
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host_with(dir.path(), script, tweak));
    TuiSession::start(host, &cwd, false).await.unwrap()
}

/// `start_session` with `--continue`: the latest session in `project`,
/// after the previous host was dropped (a relaunch). Retries while the session reads as in use: a
/// process another test forks in parallel keeps the old lock's descriptor
/// until it execs.
pub(super) async fn continue_session(dir: &tempfile::TempDir, script: &str) -> TuiSession {
    continue_session_with(dir, script, |_| {}).await
}

pub(super) async fn continue_session_with(
    dir: &tempfile::TempDir,
    script: &str,
    tweak: impl Fn(&mut Config),
) -> TuiSession {
    let cwd = dir.path().join("project");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let host = Arc::new(scripted_host_with(dir.path(), script, |config| {
            tweak(config)
        }));
        let session = TuiSession::start(host, &cwd, true).await.unwrap();
        if session.transcript_text() != CONTINUE_IN_USE || tokio::time::Instant::now() > deadline {
            return session;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A continued session's transcript without its closing continued line.
pub(super) fn replayed_text(session: &TuiSession) -> String {
    let text = session.transcript_text();
    let (replayed, last) = text.rsplit_once('\n').expect("replay and continued line");
    assert!(last.starts_with("Continued your session from"), "{text}");
    replayed.to_string()
}

pub(super) fn enter_key() -> KeyEvent {
    KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)
}

pub(super) fn user_prompts(session: &TuiSession) -> Vec<String> {
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

pub(super) fn queued_display(session: &TuiSession) -> Option<String> {
    session
        .lock_shared()
        .queued
        .as_ref()
        .map(|queued| queued.display.clone())
}

pub(super) async fn spawn_then_queue(session: &TuiSession, text: &str) {
    session.spawn_prompt("write".into()).await.unwrap();
    type_text(session, text);
    assert!(matches!(
        handle_key(session, enter_key()),
        KeyResult::Continue
    ));
}

pub(super) fn prompt_task_finished(session: &TuiSession) -> bool {
    session
        .prompt
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_some_and(|task| task.is_finished())
}

pub(super) fn draw_if_dirty(session: &TuiSession) -> bool {
    if session.lock_shared().dirty {
        let _ = drawn_text(session);
        true
    } else {
        false
    }
}

pub(super) fn buffer_row(buf: &ratatui::buffer::Buffer, y: u16) -> String {
    let mut out = String::new();
    for x in 0..buf.area().width {
        if let Some(cell) = buf.cell((x, y)) {
            out.push_str(cell.symbol());
        }
    }
    out.trim_end().to_string()
}

pub(super) fn row_is_dim(buf: &ratatui::buffer::Buffer, y: u16) -> bool {
    let mut saw = false;
    for x in 0..buf.area().width {
        let Some(cell) = buf.cell((x, y)) else {
            continue;
        };
        if cell.symbol().trim().is_empty() {
            continue;
        }
        if !cell.modifier.contains(ratatui::style::Modifier::DIM) {
            return false;
        }
        saw = true;
    }
    saw
}

pub(super) fn slot_above_composer_rule(session: &TuiSession) -> (String, bool) {
    overlay_rows_above_composer(session, 80, 24)
        .into_iter()
        .next()
        .unwrap_or_default()
}

/// Dim overlay rows between the transcript and the composer rule.
pub(super) fn overlay_rows_above_composer(
    session: &TuiSession,
    width: u16,
    height: u16,
) -> Vec<(String, bool)> {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| draw(frame, session)).unwrap();
    let composer_y = session.lock_shared().composer_area.y;
    let overlay_h = session.lock_shared().overlay_h;
    let buf = terminal.backend().buffer();
    let rule = buffer_row(buf, composer_y.saturating_sub(1));
    assert!(
        !rule.is_empty() && rule.chars().all(|ch| ch == '─'),
        "expected composer rule, got {rule:?}"
    );
    if overlay_h == 0 {
        return vec![(buffer_row(buf, composer_y.saturating_sub(2)), false)];
    }
    (0..overlay_h)
        .map(|i| {
            let y = composer_y.saturating_sub(1 + overlay_h - i);
            (buffer_row(buf, y), row_is_dim(buf, y))
        })
        .collect()
}

pub(super) fn children_update(session_id: &str, children: serde_json::Value) -> serde_json::Value {
    json!({
        "sessionId": session_id,
        "update": {
            "sessionUpdate": "tool_call_update",
            "_meta": { "shikigami": { "children": children } }
        }
    })
}

pub(super) fn wheel(kind: MouseEventKind, column: u16, row: u16) -> Event {
    Event::Mouse(MouseEvent {
        kind,
        column,
        row,
        modifiers: KeyModifiers::NONE,
    })
}

pub(super) fn ctrl_char(ch: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL)
}

pub(super) fn ctrl_o() -> KeyEvent {
    KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL)
}

pub(super) async fn notify_multiline_tool(session: &TuiSession) {
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

pub(super) fn drawn_text(session: &TuiSession) -> String {
    let mut out = drawn_rows(session, 80, 24).join("\n");
    out.push('\n');
    out
}

/// One headless render at `width`x`height`, one string per row, trailing
/// spaces trimmed.
pub(super) fn drawn_rows(session: &TuiSession, width: u16, height: u16) -> Vec<String> {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| draw(frame, session)).unwrap();
    let buf = terminal.backend().buffer();
    (0..height)
        .map(|y| {
            // A wide character owns the next cell too; read it once, as a
            // terminal shows it.
            let mut row = String::new();
            let mut x = 0;
            while x < width {
                let symbol = buf.cell((x, y)).map_or(" ", |cell| cell.symbol());
                row.push_str(symbol);
                x += unicode_width::UnicodeWidthStr::width(symbol).max(1) as u16;
            }
            row.trim_end().to_string()
        })
        .collect()
}

/// Screen rules every render test checks: nothing wider than the screen,
/// no control characters, no debug or JSON dumps of program values.
pub(super) fn assert_screen_rules(rows: &[String], width: u16) {
    let screen = rows.join("\n");
    for row in rows {
        assert!(
            unicode_width::UnicodeWidthStr::width(row.as_str()) <= width as usize,
            "row wider than {width}: {row:?}\n{screen}"
        );
        assert!(
            !row.chars().any(|ch| ch.is_control() && ch != '\t'),
            "control character on screen: {row:?}"
        );
    }
    for dump in ["Some(", "None", "{\"", "\\u{", "Err("] {
        assert!(!screen.contains(dump), "{dump} on screen:\n{screen}");
    }
}

/// At the narrow and short sizes the permission dock still shows both
/// answers, paging if it overflows, and the answer key still works.
pub(super) fn tool_line_count(text: &str) -> usize {
    text.lines().filter(|line| line.starts_with('·')).count()
}

pub(super) fn install_permission(session: &TuiSession, raw_input: String) {
    drop(install_permission_choice(session, raw_input, false));
}

pub(super) fn install_permission_choice(
    session: &TuiSession,
    raw_input: String,
    session_allow: bool,
) -> oneshot::Receiver<Value> {
    let (tx, rx) = oneshot::channel();
    session.lock_shared().begin_permission(PendingPermission {
        title: "write_file".into(),
        raw_input,
        tx,
        session_allow,
    });
    rx
}

pub(super) fn char_key(ch: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE)
}

/// The user has seen the dock and typing has paused.
pub(super) fn arm_permission_answers(session: &TuiSession) {
    let mut shared = session.lock_shared();
    shared.permission_drawn = true;
    shared.last_input_at = None;
}

pub(super) fn tall_permission_input() -> String {
    let mut lines = Vec::new();
    for i in 0..40 {
        lines.push(format!("arg-line-{i:02}"));
    }
    lines.push("later-unique-arg-line".into());
    json!({ "command": lines.join("\n") }).to_string()
}

pub(super) fn page_key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

pub(super) fn page_dock_down_until(session: &TuiSession, needle: &str) -> String {
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

pub(super) async fn wait_for_mention_files(session: &TuiSession) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    session.mention_matches();
    while !matches!(session.lock_shared().mention_files, FileList::Ready(_)) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "file list never read"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(super) fn type_text(session: &TuiSession, text: &str) {
    for ch in text.chars() {
        handle_key(
            session,
            KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE),
        );
    }
}

pub(super) fn last_system(session: &TuiSession) -> String {
    session
        .lock_shared()
        .transcript
        .iter()
        .rev()
        .find_map(|line| match line {
            TranscriptLine::System(text) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

pub(super) fn ctrl_y() -> KeyEvent {
    KeyEvent::new(KeyCode::Char('y'), KeyModifiers::CONTROL)
}

pub(super) async fn session_with_assistant(dir: &tempfile::TempDir, reply: &str) -> TuiSession {
    let session = start_session(dir, &format!(r#"[{{"content":{}}}]"#, json!(reply))).await;
    session.spawn_prompt("hi".into()).await.unwrap();
    let _ = session.wait_prompt().await;
    session
}

pub(super) fn run_copy(session: &TuiSession) {
    assert!(session.slash_catalog().iter().any(|cmd| cmd.name == "copy"));
    type_text(session, "/copy");
    assert!(matches!(
        handle_key(session, enter_key()),
        KeyResult::Continue
    ));
}

pub(super) fn slash_names(session: &TuiSession) -> Vec<String> {
    session
        .slash_matches()
        .into_iter()
        .map(|cmd| cmd.name)
        .collect()
}

/// A session in `project` with skill packs `ids` under `.agents/skills`.
pub(super) async fn session_with_skills(dir: &tempfile::TempDir, ids: &[&str]) -> TuiSession {
    let cwd = dir.path().join("project");
    for id in ids {
        let skill_dir = cwd.join(".agents/skills").join(id);
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), format!("{id} pack body\n")).unwrap();
    }
    start_session(dir, r#"[{"content":"ok"}]"#).await
}

/// #650: a skill is a slash command by its id, listed without `skill:`.
pub(super) fn set_last_active(session: &TuiSession, mtime: SystemTime) {
    let run_id = session.lock_shared().run_id.clone().expect("session run");
    let runs = session.cwd.parent().unwrap().join("state/runs");
    std::fs::OpenOptions::new()
        .write(true)
        .open(shikigami::checkpoint::path_for(&runs, &run_id))
        .unwrap()
        .set_modified(mtime)
        .unwrap();
}

pub(super) fn open_resume(session: &TuiSession) {
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

/// Two sessions with runs in `project` (older answered "hello", newer
/// is current) and one in another folder that `/resume` must not list.
pub(super) async fn two_cwd_sessions(dir: &tempfile::TempDir) -> (TuiSession, String, String) {
    let cwd = dir.path().join("project");
    let other = dir.path().join("other");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&other).unwrap();
    let host = Arc::new(scripted_host(
        dir.path(),
        r#"[{"content":"hello"},{"content":"again"}]"#,
    ));
    let now = SystemTime::now();
    let elsewhere = TuiSession::start(Arc::clone(&host), &other, false)
        .await
        .unwrap();
    elsewhere.spawn_prompt("elsewhere".into()).await.unwrap();
    let _ = elsewhere.wait_prompt().await;
    set_last_active(&elsewhere, now + Duration::from_secs(60));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session
        .spawn_prompt("fix the flaky login test".into())
        .await
        .unwrap();
    let _ = session.wait_prompt().await;
    set_last_active(&session, now - Duration::from_secs(3 * 3600));
    let older = session.session_id();
    session.new_session().await.unwrap();
    session
        .spawn_prompt("add a health check".into())
        .await
        .unwrap();
    let _ = session.wait_prompt().await;
    set_last_active(&session, now);
    let newer = session.session_id();
    (session, older, newer)
}
