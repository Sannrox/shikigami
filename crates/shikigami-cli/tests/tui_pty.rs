//! The built `shikigami tui` in a pseudo-terminal, observed from outside the
//! process: the bytes it writes and the terminal modes it leaves behind.
#![cfg(unix)]

use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(20);

/// A child process whose controlling terminal is a fresh 80x24 pty. The test
/// keeps both sides open so it can read the terminal modes after exit.
struct PtyChild {
    child: Child,
    master: OwnedFd,
    _slave: OwnedFd,
    output: Arc<Mutex<Vec<u8>>>,
}

impl PtyChild {
    fn spawn(mut cmd: Command) -> Self {
        let (mut master, mut slave) = (-1, -1);
        let mut size = libc::winsize {
            ws_row: 24,
            ws_col: 80,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &raw mut size,
            )
        };
        assert_eq!(rc, 0, "openpty: {}", std::io::Error::last_os_error());
        let (master, slave) =
            unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) };
        let stdio = || Stdio::from(slave.try_clone().unwrap());
        cmd.stdin(stdio()).stdout(stdio()).stderr(stdio());
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = cmd.spawn().expect("spawn shikigami tui");

        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&output);
        let mut reader = File::from(master.try_clone().unwrap());
        // Detached: the read blocks while the test holds the slave open.
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 {
                    break;
                }
                sink.lock().unwrap().extend_from_slice(&buf[..n]);
            }
        });
        Self {
            child,
            master,
            _slave: slave,
            output,
        }
    }

    fn output(&self) -> Vec<u8> {
        self.output.lock().unwrap().clone()
    }

    fn termios(&self) -> libc::termios {
        let mut t = unsafe { std::mem::zeroed::<libc::termios>() };
        assert_eq!(
            unsafe { libc::tcgetattr(self.master.as_raw_fd(), &mut t) },
            0
        );
        t
    }

    fn cooked(&self) -> bool {
        let lflag = self.termios().c_lflag;
        lflag & libc::ICANON != 0 && lflag & libc::ECHO != 0
    }

    /// Poll until `ready` holds, or fail with the last capture.
    fn wait_for(&mut self, what: &str, ready: impl Fn(&Self) -> bool) {
        let start = Instant::now();
        while !ready(self) {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "exited ({status}) before {what}: {:?}",
                    lossy(&self.output())
                );
            }
            assert!(
                start.elapsed() < BOUND,
                "timed out waiting for {what}: {:?}",
                lossy(&self.output())
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_exit(&mut self) -> std::process::ExitStatus {
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(start.elapsed() < BOUND, "tui did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for PtyChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The 80x24 screen a user sees after everything the TUI wrote so far. Use
/// it for text: the TUI redraws only changed cells, so a line's bytes are not
/// contiguous once the transcript scrolls.
fn screen(tui: &PtyChild) -> String {
    let size = |n| std::num::NonZeroU16::new(n).unwrap();
    let mut parser = vt100::Parser::new(size(24), size(80), 0);
    parser.process(&tui.output());
    parser.screen().contents()
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// A TUI on a scripted session, with its own project and state directories.
/// `script` is the scripted model's JSON steps; `None` keeps the example's default.
fn start_tui(dir: &tempfile::TempDir, script: Option<&str>) -> PtyChild {
    let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/local-run.toml");
    let mut config = std::fs::read_to_string(example).unwrap();
    if let Some(script) = script {
        // The example ends in the [model] table.
        config.push_str(&format!("script_json = '{script}'\n"));
    }
    start_tui_with(dir, &config, &[])
}

/// A TUI on `config` (settings file text) with extra environment `env`.
fn start_tui_with(dir: &tempfile::TempDir, config: &str, env: &[(&str, &str)]) -> PtyChild {
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(dir.path().join("config.toml"), config).unwrap();
    launch_tui(dir, env, &[])
}

/// Launches `shikigami tui <tui_args>` on the settings file and directories
/// `start_tui_with` wrote under `dir`.
fn launch_tui(dir: &tempfile::TempDir, env: &[(&str, &str)], tui_args: &[&str]) -> PtyChild {
    let project = dir.path().join("project");
    let config_path = dir.path().join("config.toml");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_shikigami"));
    cmd.current_dir(&project)
        .env("TERM", "xterm-256color")
        .env("LANG", "C.UTF-8")
        .env("NO_COLOR", "1")
        .envs(env.iter().copied())
        .arg("--config")
        .arg(&config_path)
        .arg("--state")
        .arg(dir.path().join("state"))
        .arg("tui")
        .args(tui_args);
    let mut tui = PtyChild::spawn(cmd);
    tui.wait_for("the alternate screen and raw mode", |t| {
        contains(&t.output(), b"\x1b[?1049h") && !t.cooked()
    });
    tui
}

fn temp_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("tui-pty-{}-", std::process::id()))
        .tempdir()
        .unwrap()
}

/// Asserts that the bytes written after `before` restore the terminal and that
/// the pty is back in cooked mode.
fn assert_restored(tui: &PtyChild, before: usize, name: &str) {
    let restore = [
        (&b"\x1b[?1049l"[..], "leave alternate screen"),
        (b"\x1b[?1000l", "mouse capture off"),
        (b"\x1b[?2004l", "bracketed paste off"),
        (b"\x1b[?25h", "cursor visible"),
    ];
    // The child has exited; give the reader thread a moment to drain the pty.
    let start = Instant::now();
    let after = loop {
        let after = tui.output()[before..].to_vec();
        if restore.iter().all(|(seq, _)| contains(&after, seq))
            || start.elapsed() > Duration::from_secs(2)
        {
            break after;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    for (seq, what) in restore {
        assert!(
            contains(&after, seq),
            "{name}: no {what}: {:?}",
            lossy(&after)
        );
    }
    assert!(tui.cooked(), "{name}: raw mode still on");
}

/// Waits until the turn started after `before` has finished, so ^c quits
/// instead of cancelling a turn that is still settling. Keys on the fixed
/// terminal status signal: the footer redraw only writes the cells that changed.
fn wait_turn_done(tui: &mut PtyChild, before: usize) {
    tui.wait_for("the finished turn", |t| {
        contains(&t.output()[before..], b"\x1b]7501;state=done")
    });
}

fn send(tui: &PtyChild, bytes: &[u8]) {
    let n = unsafe { libc::write(tui.master.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
    assert_eq!(n, bytes.len() as isize, "write to pty");
}

/// #591: a termination signal restores the terminal the way ^c does and the
/// process still reports that the signal ended it.
#[test]
fn termination_signal_restores_the_terminal() {
    for (signal, name) in [(libc::SIGTERM, "SIGTERM"), (libc::SIGHUP, "SIGHUP")] {
        let dir = temp_dir();
        let mut tui = start_tui(&dir, None);
        let before = tui.output().len();
        assert_eq!(unsafe { libc::kill(tui.child.id() as i32, signal) }, 0);
        let status = tui.wait_exit();
        assert_eq!(status.signal(), Some(signal), "{name}: {status}");
        assert_restored(&tui, before, name);
    }
}

/// #594: every way a person quits leaves a usable terminal and a clean exit.
#[test]
fn quit_keys_restore_the_terminal() {
    for (keys, name) in [(&b"\x03"[..], "^c"), (b"\x04", "^d"), (b"/exit\r", "/exit")] {
        let dir = temp_dir();
        let mut tui = start_tui(&dir, None);
        let before = tui.output().len();
        send(&tui, keys);
        let status = tui.wait_exit();
        assert!(status.success(), "{name}: {status}");
        assert_restored(&tui, before, name);
    }
}

/// #594: model text that carries terminal escapes reaches the real terminal
/// inert. It cannot retitle the window, clear the screen, recolor, or ring the bell.
#[test]
fn hostile_model_text_cannot_drive_the_terminal() {
    let dir = temp_dir();
    let script = r#"[{"content":"title\u001b]0;pwned\u0007 clear\u001b[2J red\u001b[31mRED bell\u0007 done-marker"}]"#;
    let mut tui = start_tui(&dir, Some(script));
    let before = tui.output().len();
    send(&tui, b"say it\r");
    tui.wait_for("the reply", |t| {
        contains(&t.output()[before..], b"done-marker")
    });
    wait_turn_done(&mut tui, before);
    let after = tui.output()[before..].to_vec();
    for (seq, what) in [
        (&b"\x1b]0;pwned"[..], "window title"),
        (b"\x1b[2J", "clear screen"),
        (b"\x1b[31m", "red foreground"),
        (b"\x07", "bell"),
    ] {
        assert!(
            !contains(&after, seq),
            "{what} reached the terminal: {:?}",
            lossy(&after)
        );
    }
    assert!(
        contains(&after, b"RED"),
        "visible text dropped: {:?}",
        lossy(&after)
    );
    let before = tui.output().len();
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
    assert_restored(&tui, before, "^c after hostile text");
}

/// Polls until the exited child's output after `before` holds `needle`;
/// the reader thread may still be draining the pty.
fn drained_contains(tui: &PtyChild, before: usize, needle: &[u8]) -> bool {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        if contains(&tui.output()[before..], needle) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// #598: quitting a session with a run prints the way back to the shell, a
/// plain launch starts new, and `--continue` reopens the session.
#[test]
fn quit_prints_the_way_back_and_continue_reopens() {
    const HINT: &[u8] = b"Continue this session later: shikigami tui --continue";
    let dir = temp_dir();
    let script = r#"[{"content":"first-reply"},{"content":"second-reply"}]"#;
    let mut tui = start_tui(&dir, Some(script));
    send(&tui, b"remember me\r");
    tui.wait_for("the reply", |t| contains(&t.output(), b"first-reply"));
    wait_turn_done(&mut tui, 0);
    let before = tui.output().len();
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
    assert!(
        drained_contains(&tui, before, HINT),
        "{:?}",
        lossy(&tui.output()[before..])
    );
    let after = tui.output()[before..].to_vec();
    let left = after.windows(8).position(|w| w == b"\x1b[?1049l").unwrap();
    let hint = after.windows(HINT.len()).position(|w| w == HINT).unwrap();
    assert!(
        hint > left,
        "the hint lands in the shell, not the TUI screen"
    );

    let mut plain = launch_tui(&dir, &[], &[]);
    send(&plain, b"\x03");
    assert!(plain.wait_exit().success());
    assert!(
        !contains(&plain.output(), b"remember me"),
        "{:?}",
        lossy(&plain.output())
    );
    assert!(!contains(&plain.output(), HINT), "no run, no hint");

    let mut continued = launch_tui(&dir, &[], &["--continue"]);
    continued.wait_for("the continued line", |t| {
        contains(
            &t.output(),
            b"Continued your session from just now (1 turn).",
        )
    });
    assert!(contains(&continued.output(), b"remember me"));
}

/// #601: the run id `/run` prints, passed unchanged to `shikigami export`,
/// yields this session's transcript.
#[test]
fn run_id_on_screen_exports_the_session() {
    let dir = temp_dir();
    let mut tui = start_tui(&dir, Some(r#"[{"content":"exported-reply"}]"#));
    send(&tui, b"hello\r");
    tui.wait_for("the reply", |t| contains(&t.output(), b"exported-reply"));
    wait_turn_done(&mut tui, 0);
    let before = tui.output().len();
    send(&tui, b"/run\r");
    tui.wait_for("the run line", |t| {
        contains(&t.output()[before..], b"shikigami export ")
    });
    let screen = lossy(&tui.output()[before..]);
    let run_id: String = screen
        .split("shikigami export ")
        .nth(1)
        .unwrap()
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    assert!(!run_id.is_empty(), "{screen:?}");
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());

    let export = Command::new(env!("CARGO_BIN_EXE_shikigami"))
        .current_dir(dir.path().join("project"))
        .arg("--config")
        .arg(dir.path().join("config.toml"))
        .arg("--state")
        .arg(dir.path().join("state"))
        .arg("export")
        .arg(&run_id)
        .output()
        .unwrap();
    let stdout = lossy(&export.stdout);
    assert!(export.status.success(), "{}", lossy(&export.stderr));
    assert!(stdout.contains("hello"), "{stdout}");
    assert!(stdout.contains("exported-reply"), "{stdout}");
}

/// #603: `[tui.keys]` rebinds an action on the real terminal, and a bad
/// entry refuses launch with the entry named.
#[test]
fn tui_keys_rebind_and_a_bad_entry_refuses_launch() {
    let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/local-run.toml");
    let base = std::fs::read_to_string(example).unwrap();

    let dir = temp_dir();
    let mut tui = start_tui_with(
        &dir,
        &format!("{base}\n[tui.keys]\ninterrupt = \"ctrl+g\"\n"),
        &[],
    );
    tui.wait_for("the rebound footer", |t| contains(&t.output(), b"^g quit"));
    send(&tui, b"\x07");
    assert!(tui.wait_exit().success());

    let dir = temp_dir();
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!("{base}\n[tui.keys]\ntoggle_plna = \"ctrl+g\"\n"),
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_shikigami"))
        .current_dir(&project)
        .arg("--config")
        .arg(&config)
        .arg("--state")
        .arg(dir.path().join("state"))
        .arg("tui")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = lossy(&out.stderr);
    assert!(
        stderr.contains("tui.keys.toggle_plna: unknown action"),
        "{stderr}"
    );
}

/// #604: a `[tui.statusline]` command's first line shows in the footer; one
/// that hangs past its limit says so, and while it hangs the screen still
/// answers keys.
#[test]
fn tui_statusline_command_shows_and_a_hanging_one_does_not_freeze() {
    use std::os::unix::fs::PermissionsExt;
    let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/local-run.toml");
    let base = std::fs::read_to_string(example).unwrap();
    let with_command = |dir: &tempfile::TempDir, body: &str, timeout_ms: u64| {
        let script = dir.path().join("status.sh");
        std::fs::write(&script, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        format!(
            "{base}\n[tui.statusline]\ncommand = {:?}\ntimeout_ms = {timeout_ms}\n",
            script.display().to_string()
        )
    };

    let dir = temp_dir();
    let config = with_command(&dir, r#"read -r p; echo "custom-status-ok""#, 2000);
    let mut tui = start_tui_with(&dir, &config, &[]);
    tui.wait_for("the custom status", |t| {
        screen(t).contains("custom-status-ok")
    });
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());

    let dir = temp_dir();
    let config = with_command(&dir, "sleep 30", 300);
    let mut tui = start_tui_with(&dir, &config, &[]);
    tui.wait_for("the failure line", |t| {
        screen(t).contains("timed out after 300ms")
    });
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());

    let dir = temp_dir();
    let config = with_command(&dir, "sleep 30", 4000);
    let mut tui = start_tui_with(&dir, &config, &[]);
    let quit = Instant::now();
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
    assert!(
        quit.elapsed() < Duration::from_secs(3),
        "{:?}",
        quit.elapsed()
    );
}

/// #624: the demo asks, writes, and reports, but its write lands in the
/// scratch directory under the state root and the project stays empty.
#[test]
fn demo_write_stays_out_of_the_project() {
    let dir = temp_dir();
    let mut tui = start_tui(&dir, None);
    tui.wait_for("the demo notice", |t| {
        contains(&t.output(), b"No model configured")
    });
    let before = tui.output().len();
    send(&tui, b"hi\r");
    tui.wait_for("the permission ask", |t| screen(t).contains("y allow"));
    send(&tui, b"y");
    wait_turn_done(&mut tui, before);
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());

    let project = dir.path().join("project");
    assert_eq!(std::fs::read_dir(&project).unwrap().count(), 0);
    let marker = dir.path().join("state/demo-workspace/SHIKIGAMI_OK.txt");
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "ok\n");
}

/// Session-allow (`a`) lets later calls of the same mutating tool through
/// without a second dock; a different tool still asks.
#[test]
fn session_allow_key_skips_the_next_write() {
    let dir = temp_dir();
    let script = r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"},{"name":"write_file","args_json":"{\"path\":\"b.txt\",\"content\":\"b\\n\"}"}]},{"content":"wrote-both"}]"#;
    let mut tui = start_tui(&dir, Some(script));
    let before = tui.output().len();
    send(&tui, b"write\r");
    tui.wait_for("the session-allow hint", |t| {
        screen(t).contains("a allow for session")
    });
    let typed = Instant::now();
    tui.wait_for("typing to settle", |t| {
        typed.elapsed() >= Duration::from_millis(400) && screen(t).contains("a allow for session")
    });
    send(&tui, b"a");
    tui.wait_for("both writes", |t| {
        contains(&t.output()[before..], b"wrote-both")
    });
    wait_turn_done(&mut tui, before);
    let shown = screen(&tui);
    assert!(
        shown.contains("write_file  a.txt  ok") && shown.contains("write_file  b.txt  ok"),
        "{shown}"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("project/a.txt")).unwrap(),
        "a\n"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("project/b.txt")).unwrap(),
        "b\n"
    );
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// #646: letters typed as a follow-up while a permission ask opens must not
/// grant the tool. A deliberate `y` after the dock is idle still allows it.
#[test]
fn letters_typed_as_text_do_not_allow_a_tool() {
    let dir = temp_dir();
    let script = r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},{"content":"wrote it"}]"#;
    let mut tui = start_tui(&dir, Some(script));
    send(&tui, b"write it\rand");
    tui.wait_for("the permission ask", |t| screen(t).contains("y allow"));
    let project_file = dir.path().join("project/ok.txt");
    assert!(
        !project_file.exists(),
        "queued follow-up letters must not allow"
    );
    send(&tui, b"xxx");
    tui.wait_for("letters on the dock ignored", |t| {
        screen(t).contains("y allow")
    });
    assert!(
        !project_file.exists(),
        "letters typed on the dock must not allow"
    );
    let typed = Instant::now();
    tui.wait_for("typing to settle", |t| {
        typed.elapsed() >= Duration::from_millis(400) && screen(t).contains("y allow")
    });
    let before = tui.output().len();
    send(&tui, b"y");
    wait_turn_done(&mut tui, before);
    assert_eq!(std::fs::read_to_string(&project_file).unwrap(), "hi\n");
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// #609: a scripted turn's reported usage shows in the footer total and in the
/// `/usage` table.
#[test]
fn token_use_shows_in_the_footer_and_usage() {
    let dir = temp_dir();
    let mut tui = start_tui(
        &dir,
        Some(r#"[{"content":"counted","usage":{"input_tokens":1200,"output_tokens":340}}]"#),
    );
    let before = tui.output().len();
    send(&tui, b"hi\r");
    wait_turn_done(&mut tui, before);
    tui.wait_for("the footer total", |t| screen(t).contains("1.5k tok"));
    send(&tui, b"/usage\r");
    tui.wait_for("the usage table", |t| {
        let screen = screen(t);
        screen
            .lines()
            .any(|l| l.split_whitespace().eq(["total", "1.2k", "340"]))
    });
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// #686: `/plan` on an idle session names the plan write path.
#[test]
fn plan_names_the_write_jail() {
    let dir = temp_dir();
    let mut tui = start_tui(&dir, Some(r#"[{"content":"ok"}]"#));
    send(&tui, b"/plan\r");
    tui.wait_for("the plan jail line", |t| {
        screen(t).contains(".shikigami/plan.md")
    });
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// #686: `/plan` after a run that started without the jail is refused.
#[test]
fn plan_refuses_after_a_run() {
    let dir = temp_dir();
    let mut tui = start_tui(&dir, Some(r#"[{"content":"ok"}]"#));
    let before = tui.output().len();
    send(&tui, b"hi\r");
    wait_turn_done(&mut tui, before);
    send(&tui, b"/plan\r");
    tui.wait_for("the refuse line", |t| {
        screen(t).contains("already ran without the jail")
    });
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// #685: `/context` names the project rules file the run attached.
#[test]
fn context_names_attached_project_rules() {
    let dir = temp_dir();
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("AGENTS.md"), "prefer tests\n").unwrap();
    let mut tui = start_tui(&dir, Some(r#"[{"content":"ok"}]"#));
    let before = tui.output().len();
    send(&tui, b"hi\r");
    wait_turn_done(&mut tui, before);
    send(&tui, b"/context\r");
    tui.wait_for("the context block", |t| screen(t).contains("AGENTS.md"));
    tui.wait_for("nested off", |t| screen(t).contains("nested  off"));
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// #602: with no model configured the built-in demo answers, and launch says
/// so before the first prompt; a configured model shows no such line.
#[test]
fn launch_names_the_demo_model() {
    let dir = temp_dir();
    let mut tui = start_tui(&dir, None);
    tui.wait_for("the demo notice", |t| {
        contains(&t.output(), b"No model configured")
    });

    let dir = temp_dir();
    let mut tui = start_tui(&dir, Some(r#"[{"content":"configured-reply"}]"#));
    send(&tui, b"hi\r");
    tui.wait_for("the reply", |t| contains(&t.output(), b"configured-reply"));
    assert!(
        !contains(&tui.output(), b"No model configured"),
        "{:?}",
        lossy(&tui.output())
    );
}

/// #602: an unset key variable stops launch with the variable and the fix.
#[test]
fn missing_model_key_names_the_variable_to_set() {
    let dir = temp_dir();
    let var = format!("SHIKIGAMI_TEST_UNSET_KEY_{}", std::process::id());
    let config = dir.path().join("http.toml");
    std::fs::write(
        &config,
        format!(
            "version = 1\n[model]\nadapter = \"http\"\nbase_url = \"http://127.0.0.1:9/v1\"\nmodel = \"m\"\napi_key_env = \"{var}\"\n"
        ),
    )
    .unwrap();
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_shikigami"))
        .current_dir(&project)
        .env_remove(&var)
        .arg("--config")
        .arg(&config)
        .arg("--state")
        .arg(dir.path().join("state"))
        .arg("tui")
        .output()
        .unwrap();
    let stderr = lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(
        stderr.contains(&format!("set {var} in the environment")),
        "{stderr}"
    );
}

/// #615: an unreachable model says what failed and what to do before the
/// upstream detail, and the TUI returns to idle.
#[test]
fn unreachable_model_names_the_next_step() {
    let dir = temp_dir();
    let var = format!("SHIKIGAMI_TEST_KEY_{}", std::process::id());
    // Port 9 (discard) is closed on test hosts, so the connection is refused.
    let config = format!(
        "version = 1\n[model]\nadapter = \"http\"\nbase_url = \"http://127.0.0.1:9/v1\"\nmodel = \"m\"\napi_key_env = \"{var}\"\n"
    );
    let mut tui = start_tui_with(&dir, &config, &[(&var, "test-key")]);
    let before = tui.output().len();
    send(&tui, b"hi\r");
    tui.wait_for("the model failure", |t| {
        t.output().len() > before && screen(t).contains("Could not reach the model.")
    });
    // One ^c quits: the TUI is idle again, not stuck in the failed turn.
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// A model provider on an ephemeral local port that answers every request
/// with `status` (e.g. `402 Payment Required`). Returns the port.
fn stub_provider(status: &'static str) -> u16 {
    use std::io::Write;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            // Read the whole request so closing does not reset it mid-send.
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            while let Ok(n @ 1..) = stream.read(&mut buf) {
                request.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&request);
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        break;
                    }
                }
            }
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
        }
    });
    port
}

/// #622: a provider that rejects the key and one that refuses for billing
/// each lead with their own cause and next step.
#[test]
fn provider_refusals_name_their_cause() {
    for (status, lead) in [
        (
            "401 Unauthorized",
            &b"The model provider rejected the key."[..],
        ),
        (
            "402 Payment Required",
            &b"The model provider refused for billing, quota, or rate limits."[..],
        ),
    ] {
        let dir = temp_dir();
        let var = format!("SHIKIGAMI_TEST_KEY_{}", std::process::id());
        let port = stub_provider(status);
        let config = format!(
            "version = 1\n[model]\nadapter = \"http\"\nbase_url = \"http://127.0.0.1:{port}/v1\"\nmodel = \"m\"\napi_key_env = \"{var}\"\n"
        );
        let mut tui = start_tui_with(&dir, &config, &[(&var, "test-key")]);
        let before = tui.output().len();
        send(&tui, b"hi\r");
        tui.wait_for(status, |t| contains(&t.output()[before..], lead));
        send(&tui, b"\x03");
        assert!(tui.wait_exit().success());
    }
}

/// #627: launch says when the model address cannot be reached, before any
/// prompt; a reachable address (any HTTP answer) says nothing.
#[test]
fn launch_says_when_the_model_cannot_be_reached() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let closed_port = closed.local_addr().unwrap().port();
    drop(closed);
    let open_port = stub_provider("404 Not Found");
    for (port, reachable) in [(closed_port, false), (open_port, true)] {
        let dir = temp_dir();
        let var = format!("SHIKIGAMI_TEST_KEY_{}", std::process::id());
        let config = format!(
            "version = 1\n[model]\nadapter = \"http\"\nbase_url = \"http://127.0.0.1:{port}/v1\"\nmodel = \"m\"\napi_key_env = \"{var}\"\n"
        );
        let mut tui = start_tui_with(&dir, &config, &[(&var, "test-key")]);
        let notice = "The model address in your settings cannot be reached.";
        if reachable {
            // The check has had time to answer; the line never comes.
            std::thread::sleep(Duration::from_secs(4));
            assert!(!screen(&tui).contains(notice), "{}", screen(&tui));
        } else {
            tui.wait_for("the unreachable notice", |t| screen(t).contains(notice));
        }
        send(&tui, b"\x03");
        assert!(tui.wait_exit().success());
    }
}

/// #595: a second window does not take over the session the first holds; it
/// starts a new one and says why. Once the holder is killed (a crash), the
/// session continues at once.
#[test]
fn a_second_window_does_not_take_over_a_held_session() {
    let dir = temp_dir();
    let mut first = start_tui(&dir, Some(r#"[{"content":"first-reply"}]"#));
    send(&first, b"hello\r");
    first.wait_for("the reply", |t| contains(&t.output(), b"first-reply"));
    wait_turn_done(&mut first, 0);

    let mut second = launch_tui(&dir, &[], &["--continue"]);
    second.wait_for("the in-use line", |t| {
        contains(&t.output(), b"open in another window")
    });
    assert!(!contains(&second.output(), b"first-reply"));
    send(&second, b"\x03");
    assert!(second.wait_exit().success());

    drop(first);
    let mut third = launch_tui(&dir, &[], &["--continue"]);
    third.wait_for("the continued session", |t| {
        contains(&t.output(), b"Continued your session")
    });
    assert!(contains(&third.output(), b"first-reply"));
}

/// #607: `@` lists workspace files (not ignored ones, not files outside the
/// project) and Tab puts the picked path into the prompt.
#[test]
fn at_mention_picks_a_workspace_file() {
    let dir = temp_dir();
    let project = dir.path().join("project");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::write(project.join("src/main.rs"), "").unwrap();
    std::fs::write(project.join(".gitignore"), "secret.rs\n").unwrap();
    std::fs::write(project.join("secret.rs"), "").unwrap();
    std::fs::write(dir.path().join("outside.rs"), "").unwrap();
    let mut tui = start_tui(&dir, Some(r#"[{"content":"mention-reply"}]"#));
    send(&tui, b"read @");
    tui.wait_for("the file list", |t| screen(t).contains("  src/main.rs"));
    let listed = screen(&tui);
    assert!(!listed.contains("secret.rs"), "{listed}");
    assert!(!listed.contains("outside.rs"), "{listed}");
    send(&tui, b"mai\t\r");
    tui.wait_for("the reply", |t| screen(t).contains("mention-reply"));
    assert!(
        screen(&tui).contains("read @src/main.rs"),
        "{}",
        screen(&tui)
    );
    wait_turn_done(&mut tui, 0);
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}

/// #650 / #694: a skill pack is a slash command by its id. `/demo extra`
/// shows that line, loads the pack as `load_skill` (body stays out of the
/// user prompt), the list offers `demo`, `/skill:demo` still works, and a
/// pack named like a built-in does not shadow it.
#[test]
fn skill_pack_runs_as_its_own_slash_command() {
    let dir = temp_dir();
    let skills = dir.path().join("project/.agents/skills");
    for (id, body) in [("demo", "demo-pack-body"), ("help", "help-pack-body")] {
        std::fs::create_dir_all(skills.join(id)).unwrap();
        std::fs::write(skills.join(id).join("SKILL.md"), format!("{body}\n")).unwrap();
    }
    let mut tui = start_tui(
        &dir,
        Some(
            r#"[{"requires_context":["demo-pack-body","extra"],"content":"skill-reply"},{"requires_context":["demo-pack-body"],"content":"alias-reply"}]"#,
        ),
    );
    send(&tui, b"/de");
    tui.wait_for("the skill in the slash list", |t| {
        screen(t)
            .lines()
            .any(|l| l.split_whitespace().eq(["demo", "load", "skill", "pack"]))
    });
    send(&tui, b"mo extra\r");
    tui.wait_for("the skill reply", |t| screen(t).contains("skill-reply"));
    assert!(
        screen(&tui).contains("you  /demo extra"),
        "{}",
        screen(&tui)
    );
    wait_turn_done(&mut tui, 0);

    let before = tui.output().len();
    send(&tui, b"/skill:demo\r");
    tui.wait_for("the alias reply", |t| screen(t).contains("alias-reply"));
    wait_turn_done(&mut tui, before);

    send(&tui, b"/help\r");
    tui.wait_for("the built-in help", |t| {
        screen(t).contains("Shift+Enter newline")
    });
    assert!(!screen(&tui).contains("help-pack-body"), "{}", screen(&tui));
    send(&tui, b"\x03");
    assert!(tui.wait_exit().success());
}
