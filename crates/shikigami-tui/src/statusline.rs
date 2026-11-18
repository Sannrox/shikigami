//! `[tui.statusline]`: an operator command whose first output line replaces
//! the footer's model and turn. It runs out of process, off the draw path, and
//! only when the state it is given changes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use shikigami::config::TuiStatusLine;
use shikigami::hooks::run_operator_command;
use tokio::task::JoinHandle;

use crate::{Shared, TranscriptLine, truncate_one_line};

/// Bumped when a payload field changes meaning or goes away.
const SCHEMA_VERSION: u32 = 1;

/// Upper bound on `timeout_ms`, so a typo cannot park a run for minutes.
const MAX_TIMEOUT: Duration = Duration::from_secs(5);

/// Session state handed to the command as JSON on stdin.
pub(crate) struct Snapshot<'a> {
    pub session_id: &'a str,
    pub cwd: &'a str,
    pub model: &'a str,
    pub status: &'a str,
    pub turn: usize,
    pub run_id: Option<&'a str>,
}

impl Snapshot<'_> {
    fn payload(&self) -> String {
        json!({
            "schema_version": SCHEMA_VERSION,
            "session_id": self.session_id,
            "cwd": self.cwd,
            "model": self.model,
            "status": self.status,
            "turn": self.turn,
            "run_id": self.run_id,
        })
        .to_string()
    }
}

/// Owns at most one running command. `tick` is cheap and never waits.
pub(crate) struct StatusLine {
    settings: TuiStatusLine,
    last_payload: Option<String>,
    running: Option<JoinHandle<()>>,
    warned: Arc<AtomicBool>,
}

impl StatusLine {
    pub(crate) fn new(settings: TuiStatusLine) -> Self {
        Self {
            settings,
            last_payload: None,
            running: None,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the command when `snapshot` differs from the last one sent and
    /// none is running. A change seen mid-run is picked up on a later tick.
    /// The result lands in `shared.statusline`; the first failure also adds
    /// one transcript line.
    pub(crate) fn tick(&mut self, snapshot: &Snapshot<'_>, shared: &Arc<Mutex<Shared>>) {
        if self
            .running
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let payload = snapshot.payload();
        if self.last_payload.as_ref() == Some(&payload) {
            return;
        }
        self.last_payload = Some(payload.clone());
        let command = self.settings.command.clone();
        let limit = Duration::from_millis(self.settings.timeout_ms.max(1)).min(MAX_TIMEOUT);
        let shared = Arc::clone(shared);
        let warned = Arc::clone(&self.warned);
        self.running = Some(tokio::spawn(async move {
            let result = run_operator_command(&command, &[], &[], &payload, limit, true).await;
            let mut shared = shared.lock().unwrap_or_else(|e| e.into_inner());
            match result {
                Ok(stdout) => {
                    let text = String::from_utf8_lossy(&stdout);
                    let line = text.lines().next().unwrap_or("").trim();
                    shared.statusline = (!line.is_empty()).then(|| line.to_string());
                }
                Err(err) => {
                    shared.statusline = None;
                    if !warned.swap(true, Ordering::Relaxed) {
                        shared.transcript.push(TranscriptLine::System(format!(
                            "Status line command failed ({}). Showing the default footer.",
                            truncate_one_line(&err, 120)
                        )));
                    }
                }
            }
            shared.dirty = true;
        }));
    }
}

impl Drop for StatusLine {
    fn drop(&mut self) {
        if let Some(task) = self.running.take() {
            task.abort();
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn script(dir: &std::path::Path, body: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(format!("status-{}.sh", uuid::Uuid::new_v4()));
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn settings(command: String, timeout_ms: u64) -> TuiStatusLine {
        serde_json::from_value(json!({ "command": command, "timeout_ms": timeout_ms })).unwrap()
    }

    fn snapshot(turn: usize) -> Snapshot<'static> {
        Snapshot {
            session_id: "s1",
            cwd: "/work",
            model: "scripted",
            status: "ready",
            turn,
            run_id: None,
        }
    }

    async fn settle(line: &mut StatusLine) {
        if let Some(task) = line.running.take() {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn first_line_of_output_reaches_the_footer_and_sees_the_payload() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = script(
            dir.path(),
            r#"read -r payload; case "$payload" in *'"schema_version":1'*'"turn":2'*) echo "on main"; echo second;; *) echo "bad $payload";; esac"#,
        );
        let shared = Arc::new(Mutex::new(Shared::new()));
        let mut line = StatusLine::new(settings(cmd, 2000));
        line.tick(&snapshot(2), &shared);
        settle(&mut line).await;
        assert_eq!(
            shared.lock().unwrap().statusline.as_deref(),
            Some("on main")
        );
    }

    #[tokio::test]
    async fn a_hanging_command_does_not_block_and_warns_once() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = script(dir.path(), "sleep 10");
        let shared = Arc::new(Mutex::new(Shared::new()));
        shared.lock().unwrap().statusline = Some("stale".into());
        let mut line = StatusLine::new(settings(cmd, 50));

        let started = std::time::Instant::now();
        line.tick(&snapshot(1), &shared);
        assert!(started.elapsed() < Duration::from_millis(50));
        settle(&mut line).await;
        line.tick(&snapshot(2), &shared);
        settle(&mut line).await;

        let shared = shared.lock().unwrap();
        assert_eq!(shared.statusline, None);
        let warnings: Vec<_> = shared
            .transcript
            .iter()
            .filter(|l| matches!(l, TranscriptLine::System(t) if t.starts_with("Status line command failed (timed out")))
            .collect();
        assert_eq!(warnings.len(), 1);
    }

    #[tokio::test]
    async fn a_failing_command_names_its_exit_and_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = script(dir.path(), "echo out; echo boom >&2; exit 3");
        let shared = Arc::new(Mutex::new(Shared::new()));
        let mut line = StatusLine::new(settings(cmd, 2000));
        line.tick(&snapshot(1), &shared);
        settle(&mut line).await;
        let shared = shared.lock().unwrap();
        assert!(
            matches!(shared.transcript.as_slice(), [TranscriptLine::System(t)] if t.contains("(exit status: 3: boom)")),
            "{:?}",
            shared
                .transcript
                .iter()
                .map(|l| match l {
                    TranscriptLine::System(t) => t.as_str(),
                    _ => "",
                })
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn an_unchanged_state_does_not_rerun_the_command() {
        let dir = tempfile::tempdir().unwrap();
        let count = dir.path().join("count");
        let cmd = script(
            dir.path(),
            &format!("echo x >> {}; echo ok", count.display()),
        );
        let shared = Arc::new(Mutex::new(Shared::new()));
        let mut line = StatusLine::new(settings(cmd, 2000));
        for _ in 0..3 {
            line.tick(&snapshot(1), &shared);
            settle(&mut line).await;
        }
        line.tick(&snapshot(2), &shared);
        settle(&mut line).await;
        assert_eq!(
            std::fs::read_to_string(count).unwrap().lines().count(),
            2,
            "{:?}",
            shared.lock().unwrap().transcript.len()
        );
    }
}
