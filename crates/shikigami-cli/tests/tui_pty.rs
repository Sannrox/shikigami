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

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// A TUI on a scripted session, with its own project and state directories.
/// `script` is the scripted model's JSON steps; `None` keeps the example's default.
fn start_tui(dir: &tempfile::TempDir, script: Option<&str>) -> PtyChild {
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/local-run.toml");
    let mut config = std::fs::read_to_string(example).unwrap();
    if let Some(script) = script {
        // The example ends in the [model] table.
        config.push_str(&format!("script_json = '{script}'\n"));
    }
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, config).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_shikigami"));
    cmd.current_dir(&project)
        .env("TERM", "xterm-256color")
        .env("LANG", "C.UTF-8")
        .env("NO_COLOR", "1")
        .arg("--config")
        .arg(&config_path)
        .arg("--state")
        .arg(dir.path().join("state"))
        .arg("tui");
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

/// #601: the run id `/run` prints, passed unchanged to `shikigami export`,
/// yields this session's transcript.
#[test]
fn run_id_on_screen_exports_the_session() {
    let dir = temp_dir();
    let mut tui = start_tui(&dir, Some(r#"[{"content":"exported-reply"}]"#));
    send(&tui, b"hello\r");
    tui.wait_for("the reply", |t| contains(&t.output(), b"exported-reply"));
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
