//! Terminal process host loop.

use std::io::{IsTerminal, stdin, stdout};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::prelude::CrosstermBackend;
use tokio::sync::mpsc;

use shikigami::acp::AcpHost;
use shikigami::harness::Harness;

use crate::input::{drain_keys_pressed_before_permission_frame, handle_event};
use crate::look::Look;
use crate::render::{draw, turn_count};
use crate::session::{CONTINUE_HINT, KeyResult, Shared, TranscriptLine, TuiSession, model_label};
use crate::slash::osc_write_stdout;
use crate::statusline::{self, StatusLine};

pub async fn run(harness: Harness) -> Result<(), String> {
    run_terminal_host(harness, false).await
}

/// `run`, but reopening the latest session in this folder (`--continue`).
pub async fn run_continuing(harness: Harness) -> Result<(), String> {
    run_terminal_host(harness, true).await
}

pub(crate) async fn run_terminal_host(harness: Harness, continue_last: bool) -> Result<(), String> {
    let no_color = std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty());
    let look = Look::from_settings(&harness.config.tui, no_color)?;
    let statusline = harness.config.tui.statusline.clone().map(StatusLine::new);
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return Err("shikigami tui requires a terminal".into());
    }
    let cwd = std::env::current_dir()
        .map_err(|e| e.to_string())?
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let model = model_label(&harness.config.model);
    let model_port = harness.model_handle();
    let host = Arc::new(AcpHost::new(harness));
    let demo = host.demo_workspace();
    let mut session = TuiSession::start(host, &cwd, continue_last).await?;
    session.model = model;
    session.look = look;
    session.statusline = statusline;
    if let Some(demo) = demo {
        session
            .lock_shared()
            .transcript
            .extend(demo_model_notice(&demo).map(TranscriptLine::System));
    }
    let shared = Arc::clone(&session.client.shared);
    tokio::spawn(note_unreachable_model(model_port, Arc::clone(&shared)));
    let mut termination = Termination::install()?;
    run_terminal(session, &mut termination).await?;
    if shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .run_id
        .is_some()
    {
        println!("{CONTINUE_HINT}");
    }
    // The terminal is restored by now; end the process the way the signal would have.
    termination.reraise();
    Ok(())
}

/// Launch found the configured model address unreachable. Typing still works.
pub(crate) const MODEL_UNREACHABLE_AT_LAUNCH: &str = "The model address in your settings cannot be reached. Check the address, or start the model server. You can still type; prompts fail until it answers.";

/// Writes [`MODEL_UNREACHABLE_AT_LAUNCH`] when the model's address does not
/// answer. Runs beside the first screen so launch never waits on the network.
pub(crate) async fn note_unreachable_model(
    model: Arc<dyn shikigami::model::ModelPort>,
    shared: Arc<Mutex<Shared>>,
) {
    if let Err(shikigami::model::ModelError::Unreachable(_)) = model.check_reachable().await {
        let mut shared = shared.lock().unwrap_or_else(|e| e.into_inner());
        shared
            .transcript
            .push(TranscriptLine::System(MODEL_UNREACHABLE_AT_LAUNCH.into()));
        shared.dirty = true;
    }
}

/// Launch lines when the built-in demo answers. The scratch directory it
/// writes to gets a line of its own so a long path stays readable.
pub(crate) fn demo_model_notice(workspace: &Path) -> [String; 2] {
    [
        "No model configured. This is the built-in demo: it asks to write SHIKIGAMI_OK.txt in this scratch directory, not in your project, then stops. To use a real model, set [model] in a settings file and start with --config <file>.".into(),
        workspace.display().to_string(),
    ]
}

/// Grace period for a running turn to record its cancellation before a
/// termination signal ends the process.
pub(crate) const TERMINATION_GRACE: Duration = Duration::from_secs(2);

#[cfg(unix)]
pub(crate) struct Termination {
    term: tokio::signal::unix::Signal,
    hup: tokio::signal::unix::Signal,
    received: Option<libc::c_int>,
}

#[cfg(unix)]
impl Termination {
    pub(crate) fn install() -> Result<Self, String> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            term: signal(SignalKind::terminate()).map_err(|e| e.to_string())?,
            hup: signal(SignalKind::hangup()).map_err(|e| e.to_string())?,
            received: None,
        })
    }

    /// Resolves when a termination signal arrives. Cancel-safe.
    pub(crate) async fn recv(&mut self) {
        let signal = tokio::select! {
            _ = self.term.recv() => libc::SIGTERM,
            _ = self.hup.recv() => libc::SIGHUP,
        };
        self.received = Some(signal);
    }

    pub(crate) fn reraise(self) {
        if let Some(signal) = self.received {
            unsafe {
                libc::signal(signal, libc::SIG_DFL);
                libc::raise(signal);
            }
        }
    }
}

#[cfg(not(unix))]
pub(crate) struct Termination;

#[cfg(not(unix))]
impl Termination {
    pub(crate) fn install() -> Result<Self, String> {
        Ok(Self)
    }

    pub(crate) async fn recv(&mut self) {
        std::future::pending().await
    }

    pub(crate) fn reraise(self) {}
}

pub(crate) struct TerminalGuard;

/// OSC 7501 revision 0.3. Only fixed metadata leaves the TUI; transcript and
/// tool text never become terminal control sequences.
pub(crate) fn program_status(shared: &Shared) -> &'static [u8] {
    if shared.permission.is_some() {
        b"\x1b]7501;state=blocked:kind=permission:app=shikigami\x1b\\"
    } else if shared.busy {
        b"\x1b]7501;state=working:app=shikigami\x1b\\"
    } else {
        match shared.status.as_str() {
            "error" => b"\x1b]7501;state=error:app=shikigami\x1b\\",
            "done" => b"\x1b]7501;state=done:app=shikigami\x1b\\",
            _ => b"\x1b]7501;state=idle:app=shikigami\x1b\\",
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = osc_write_stdout(b"\x1b]7501;state=idle:app=shikigami\x1b\\");
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
pub(crate) struct StderrSilence {
    saved: i32,
}

#[cfg(unix)]
impl StderrSilence {
    pub(crate) fn apply() -> Option<Self> {
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

pub(crate) async fn run_terminal(
    mut session: TuiSession,
    termination: &mut Termination,
) -> Result<(), String> {
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

    let mut statusline = session.statusline.take();
    let cwd = session.cwd.to_string_lossy().into_owned();
    let mut last_program_status = None;
    loop {
        if let Some(statusline) = statusline.as_mut() {
            let (status, turn, run_id) = {
                let shared = session.lock_shared();
                (
                    shared.status.clone(),
                    turn_count(&shared),
                    shared.run_id.clone(),
                )
            };
            let session_id = session.session_id();
            let model = session.model_label(&session.lock_shared());
            statusline.tick(
                &statusline::Snapshot {
                    session_id: &session_id,
                    cwd: &cwd,
                    model: &model,
                    status: &status,
                    turn,
                    run_id: run_id.as_deref(),
                },
                &session.client.shared,
            );
        }
        let status = program_status(&session.lock_shared());
        if last_program_status != Some(status) {
            osc_write_stdout(status).map_err(|e| e.to_string())?;
            last_program_status = Some(status);
        }
        let first_permission_frame = {
            let shared = session.lock_shared();
            shared.permission.is_some() && !shared.permission_drawn && shared.dirty
        };
        if session.lock_shared().dirty {
            terminal
                .draw(|frame| draw(frame, &session))
                .map_err(|e| e.to_string())?;
        }
        if first_permission_frame {
            drain_keys_pressed_before_permission_frame(&mut rx, &session);
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

        let ev = tokio::select! {
            ev = tokio::time::timeout(Duration::from_millis(80), rx.recv()) => match ev {
                Ok(Some(ev)) => ev,
                Ok(None) => break,
                Err(_) => continue,
            },
            () = termination.recv() => {
                // Same cancel as ^c on a running turn, then quit.
                if session.lock_shared().busy {
                    session.cancel();
                    let _ = tokio::time::timeout(TERMINATION_GRACE, session.wait_prompt()).await;
                }
                break;
            }
        };
        match handle_event(&session, ev) {
            KeyResult::Quit => break,
            KeyResult::Prompt {
                display,
                send,
                parts,
                load_skill,
            } => {
                if let Err(err) = session
                    .spawn_prompt_with(display, send, parts, load_skill)
                    .await
                {
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
            KeyResult::SetModel(id) => session.set_model(&id).await,
            KeyResult::Mcp { action, name } => session.mcp(&action, name.as_deref()).await,
            KeyResult::Plan => session.plan().await,
            KeyResult::ResumePicker => session.open_resume_picker().await,
            KeyResult::Continue => {}
        }
    }
    Ok(())
}
