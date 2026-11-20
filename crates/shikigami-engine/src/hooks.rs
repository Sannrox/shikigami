//! Operator-trusted lifecycle hooks (settings-driven subprocesses).
//!
//! Hooks are **not** a sandbox. Only run commands you trust. The exception is
//! `stop`: it checks code the model wrote, so it runs in the workspace under
//! the tool sandbox and returns a verdict ([`stop_verdict`]).

use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::process::Command;
use tokio::time::timeout;

use crate::config::HookSettings;
use crate::events::{VerificationCase, VerificationOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookEvent {
    PreRun,
    PostRun,
    PreTool,
    PostTool,
    OnPark,
    /// New user prompt on a session-wait run (ACP/TUI). Not ask/plan/escalate resume.
    PrePrompt,
}

impl HookEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreRun => "pre_run",
            Self::PostRun => "post_run",
            Self::PreTool => "pre_tool",
            Self::PostTool => "post_tool",
            Self::OnPark => "on_park",
            Self::PrePrompt => "pre_prompt",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pre_run" => Some(Self::PreRun),
            "post_run" => Some(Self::PostRun),
            "pre_tool" => Some(Self::PreTool),
            "post_tool" => Some(Self::PostTool),
            "on_park" => Some(Self::OnPark),
            "pre_prompt" => Some(Self::PrePrompt),
            _ => None,
        }
    }
}

/// Prompt text placed on `pre_prompt` stdin. Long prompts are cut with a marker.
pub(crate) const PRE_PROMPT_CHARS: usize = 4000;

pub(crate) fn truncated_prompt(text: &str) -> String {
    let count = text.chars().count();
    if count <= PRE_PROMPT_CHARS {
        return text.to_string();
    }
    let mut body: String = text.chars().take(PRE_PROMPT_CHARS).collect();
    body.push_str("\n\n… [prompt truncated]\n");
    body
}

/// Run all hooks matching `event`. Returns error only when a matching hook is
/// fail-closed and fails/times out.
pub async fn run_hooks(
    hooks: &[HookSettings],
    event: HookEvent,
    payload: Value,
) -> Result<(), String> {
    for h in hooks {
        if HookEvent::parse(&h.event) != Some(event) {
            continue;
        }
        if h.command.is_empty() {
            continue;
        }
        if let Err(e) = invoke_hook(h, event, &payload).await
            && h.fail_closed
        {
            return Err(format!(
                "hook `{}` ({}) fail-closed: {e}",
                h.command,
                event.as_str()
            ));
        }
    }
    Ok(())
}

async fn invoke_hook(hook: &HookSettings, event: HookEvent, payload: &Value) -> Result<(), String> {
    let body = json!({
        "event": event.as_str(),
        "payload": payload,
    });
    let body_str = serde_json::to_string(&body).map_err(|e| e.to_string())?;
    let limit = Duration::from_millis(hook.timeout_ms.clamp(1, 120_000));
    run_operator_command(
        &hook.command,
        &hook.args,
        &[("SHIKIGAMI_HOOK_EVENT", event.as_str())],
        &body_str,
        limit,
        false,
    )
    .await
    .map(drop)
}

/// Run an operator-trusted command with `stdin` (plus a newline) and wait at
/// most `limit`. The command leads its own process group; when the limit
/// passes, or the call is dropped first, the whole group is killed, so
/// processes it started do not outlive it. The wait ends when the command's
/// output closes, so only a background process with its own stdio survives a
/// command that finishes on time. Returns stdout
/// when `capture_stdout` is set, else an empty buffer. Lifecycle hooks and
/// the TUI status line command use it.
pub async fn run_operator_command(
    command: &str,
    args: &[String],
    env: &[(&str, &str)],
    stdin: &str,
    limit: Duration,
    capture_stdout: bool,
) -> Result<Vec<u8>, String> {
    let mut cmd = Command::new(command);
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.args(args)
        .stdin(Stdio::piped())
        .stdout(if capture_stdout {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .envs(env.iter().copied());
    let mut child = spawn_retrying_busy(&mut cmd)
        .await
        .map_err(|e| format!("spawn: {e}"))?;
    let mut group = GroupKill(child.id());

    if let Some(mut pipe) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        let _ = pipe.write_all(stdin.as_bytes()).await;
        let _ = pipe.write_all(b"\n").await;
        drop(pipe);
    }

    match timeout(limit, child.wait_with_output()).await {
        Ok(Ok(out)) => {
            group.0 = None;
            if out.status.success() {
                Ok(out.stdout)
            } else {
                let err = String::from_utf8_lossy(&out.stderr);
                let err = err.trim();
                Err(if err.is_empty() {
                    out.status.to_string()
                } else {
                    format!("{}: {err}", out.status)
                })
            }
        }
        Ok(Err(e)) => Err(format!("wait: {e}")),
        Err(_) => Err(format!("timed out after {limit:?}")),
    }
}

/// Spawn, retrying briefly while the executable is "text file busy": on Linux
/// a script that was just written can still be open for writing in a process
/// forked meanwhile (by another thread) until that process execs.
async fn spawn_retrying_busy(cmd: &mut Command) -> std::io::Result<tokio::process::Child> {
    let mut attempt = 0;
    loop {
        match cmd.spawn() {
            Err(e) if e.kind() == std::io::ErrorKind::ExecutableFileBusy && attempt < 5 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(10 * attempt)).await;
            }
            other => return other,
        }
    }
}

/// Kills the operator command's process group on drop unless disarmed.
struct GroupKill(Option<u32>);

impl Drop for GroupKill {
    fn drop(&mut self) {
        crate::sandbox::kill_process_group(self.0.take());
    }
}

/// Hook event that gates a successful completion. It is not a [`HookEvent`]:
/// [`run_hooks`] skips it, and the run loop runs it through the sandbox.
pub const STOP_EVENT: &str = "stop";

/// Ceiling for a `stop` hook's `timeout_ms`. A test suite needs longer than
/// the two minutes other hooks get.
pub const STOP_HOOK_MAX_TIMEOUT_MS: u64 = 3_600_000;

/// Longest failure text kept for the model and the event, from the end.
const STOP_REASON_CHARS: usize = 4_000;

/// What one `stop` hook decided. `reason` goes back to the model on failure.
#[derive(Debug, PartialEq, Eq)]
pub struct StopVerdict {
    pub passed: bool,
    pub reason: String,
    pub cases: Vec<VerificationCase>,
}

/// JSON a `stop` hook may print on stdout.
#[derive(serde::Deserialize)]
struct StopReply {
    #[serde(default)]
    passed: Option<bool>,
    #[serde(default)]
    decision: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    cases: Vec<VerificationCase>,
}

/// Resolve a `stop` hook command on the host, before the sandbox applies. A
/// path is taken relative to the harness process, never to the workspace, so
/// the model cannot rewrite the check that judges it. A bare name uses `PATH`.
pub fn stop_hook_program(command: &str) -> std::path::PathBuf {
    let path = std::path::Path::new(command);
    if path.components().count() > 1 {
        std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
    } else {
        path.to_path_buf()
    }
}

/// Judge one `stop` hook run. `Err` is a hook that did not finish.
///
/// Pass: exit 0 with no JSON, or JSON that neither blocks, says
/// `"passed": false`, nor lists a failing case. Fail: exit 2, a block, a
/// failing case, malformed JSON, any other exit, or no exit at all. A broken
/// hook never lets a run finish as a success.
pub fn stop_verdict(command: &str, output: Result<std::process::Output, String>) -> StopVerdict {
    let output = match output {
        Ok(output) => output,
        Err(error) => {
            return failed_hook(
                command,
                VerificationOutcome::Error,
                format!("stop hook `{command}` did not finish: {error}"),
            );
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let reply = if stdout.trim_start().starts_with('{') {
        match serde_json::from_str::<StopReply>(stdout.trim()) {
            Ok(reply) => Some(reply),
            Err(error) => {
                return failed_hook(
                    command,
                    VerificationOutcome::Error,
                    format!("stop hook `{command}` printed malformed JSON: {error}"),
                );
            }
        }
    } else {
        None
    };
    let code = output.status.code();
    let reply_blocks = reply.as_ref().is_some_and(|reply| {
        reply.decision.as_deref() == Some("block")
            || reply.passed == Some(false)
            || reply.cases.iter().any(|case| {
                matches!(
                    case.outcome,
                    VerificationOutcome::Fail | VerificationOutcome::Error
                )
            })
    });
    let (reply_reason, mut cases) = reply
        .map(|reply| (reply.reason, reply.cases))
        .unwrap_or_default();
    for case in &mut cases {
        case.message = clean_tail(&case.message);
    }
    let outcome = match code {
        Some(0) if !reply_blocks => None,
        Some(0) | Some(2) => Some(VerificationOutcome::Fail),
        _ => Some(VerificationOutcome::Error),
    };
    let Some(outcome) = outcome else {
        if cases.is_empty() {
            cases.push(hook_case(command, VerificationOutcome::Pass, String::new()));
        }
        return StopVerdict {
            passed: true,
            reason: String::new(),
            cases,
        };
    };
    let detail = reply_reason
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or_else(|| {
            if stderr.trim().is_empty() {
                stdout.into_owned()
            } else {
                stderr.into_owned()
            }
        });
    let reason = clean_tail(&match (outcome, code) {
        (VerificationOutcome::Error, Some(code)) => {
            format!("stop hook `{command}` exited {code}: {detail}")
        }
        (VerificationOutcome::Error, None) => {
            format!("stop hook `{command}` was killed by a signal: {detail}")
        }
        _ => detail,
    });
    if cases.is_empty() {
        cases.push(hook_case(command, outcome, reason.clone()));
    }
    StopVerdict {
        passed: false,
        reason,
        cases,
    }
}

fn failed_hook(command: &str, outcome: VerificationOutcome, reason: String) -> StopVerdict {
    let reason = clean_tail(&reason);
    StopVerdict {
        passed: false,
        cases: vec![hook_case(command, outcome, reason.clone())],
        reason,
    }
}

fn hook_case(command: &str, outcome: VerificationOutcome, message: String) -> VerificationCase {
    VerificationCase {
        suite: STOP_EVENT.into(),
        name: command.into(),
        outcome,
        message,
    }
}

/// Strip ANSI escape sequences and keep the last [`STOP_REASON_CHARS`].
fn clean_tail(text: &str) -> String {
    let mut clean = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            clean.push(c);
            continue;
        }
        // CSI: ESC [ params final-byte; other escapes: ESC + one char.
        if chars.next_if_eq(&'[').is_some() {
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        } else {
            chars.next();
        }
    }
    let clean = clean.trim();
    let count = clean.chars().count();
    if count <= STOP_REASON_CHARS {
        return clean.to_owned();
    }
    let tail: String = clean.chars().skip(count - STOP_REASON_CHARS).collect();
    format!(
        "[... {} characters cut ...]{tail}",
        count - STOP_REASON_CHARS
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HookSettings;

    fn write_script(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hook.sh");
        std::fs::write(&path, body).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).unwrap();
        }
        (dir, path)
    }

    /// #638: a background process with its own stdio outlives a command that
    /// finishes on time.
    #[cfg(unix)]
    #[tokio::test]
    async fn on_time_exit_keeps_a_detached_background_process() {
        let pid_file = tempfile::NamedTempFile::new().unwrap();
        let (_dir, script) = write_script(&format!(
            "#!/bin/sh\nsleep 30 >/dev/null 2>&1 </dev/null &\necho $! > {}\n",
            pid_file.path().display()
        ));
        run_operator_command(
            script.to_str().unwrap(),
            &[],
            &[],
            "{}",
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap();
        let pid: libc::pid_t = std::fs::read_to_string(pid_file.path())
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        unsafe { libc::kill(pid, libc::SIGKILL) };
        assert!(alive, "detached background process {pid} was killed");
    }

    /// #638: a command that times out takes the processes it started with it.
    /// #661: wait until the fixture child is running before the timeout can
    /// fire, so an empty pid file is not a false failure under load.
    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_the_commands_children() {
        let pid_file = tempfile::NamedTempFile::new().unwrap();
        let (_dir, script) = write_script(&format!(
            "#!/bin/sh\nsleep 30 &\necho $! > {}\nsleep 30\n",
            pid_file.path().display()
        ));
        let run = run_operator_command(
            script.to_str().unwrap(),
            &[],
            &[],
            "{}",
            Duration::from_secs(2),
            false,
        );
        tokio::pin!(run);

        let pid = loop {
            if let Ok(pid) = std::fs::read_to_string(pid_file.path())
                .unwrap_or_default()
                .trim()
                .parse::<libc::pid_t>()
                && pid > 1
                && unsafe { libc::kill(pid, 0) } == 0
            {
                break pid;
            }
            tokio::select! {
                biased;
                result = &mut run => {
                    panic!("command ended before the fixture child pid was written: {result:?}");
                }
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        };

        let err = run.await.unwrap_err();
        assert!(err.starts_with("timed out"), "{err}");

        let started = std::time::Instant::now();
        // The orphan is reaped by init shortly after the kill.
        while unsafe { libc::kill(pid, 0) } == 0 {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "grandchild {pid} still running"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[test]
    fn pre_prompt_round_trips() {
        assert_eq!(HookEvent::parse("pre_prompt"), Some(HookEvent::PrePrompt));
        assert_eq!(HookEvent::PrePrompt.as_str(), "pre_prompt");
        let long = "x".repeat(PRE_PROMPT_CHARS + 8);
        let cut = truncated_prompt(&long);
        assert!(cut.contains("… [prompt truncated]"));
        assert!(cut.chars().count() > PRE_PROMPT_CHARS);
        assert!(truncated_prompt("short") == "short");
    }

    #[tokio::test]
    async fn fail_closed_pre_tool_blocks() {
        let (_dir, path) = write_script("#!/bin/sh\nexit 1\n");
        let hooks = vec![HookSettings {
            event: "pre_tool".into(),
            command: path.to_string_lossy().into(),
            args: vec![],
            timeout_ms: 2_000,
            fail_closed: true,
        }];
        let err = run_hooks(
            &hooks,
            HookEvent::PreTool,
            json!({"run_id":"r","tool":"bash"}),
        )
        .await
        .unwrap_err();
        assert!(err.contains("fail-closed"), "{err}");
    }

    #[tokio::test]
    async fn fail_open_ignores_failure() {
        let (_dir, path) = write_script("#!/bin/sh\nexit 1\n");
        let hooks = vec![HookSettings {
            event: "post_run".into(),
            command: path.to_string_lossy().into(),
            args: vec![],
            timeout_ms: 2_000,
            fail_closed: false,
        }];
        run_hooks(&hooks, HookEvent::PostRun, json!({"run_id":"r"}))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn timeout_fail_closed() {
        let (_dir, path) = write_script("#!/bin/sh\nsleep 5\n");
        let hooks = vec![HookSettings {
            event: "pre_run".into(),
            command: path.to_string_lossy().into(),
            args: vec![],
            timeout_ms: 100,
            fail_closed: true,
        }];
        let err = run_hooks(&hooks, HookEvent::PreRun, json!({}))
            .await
            .unwrap_err();
        assert!(err.contains("timed out"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn stop_verdict_fails_closed() {
        use std::os::unix::process::ExitStatusExt;
        let out = |code: i32, stdout: &str, stderr: &str| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(code << 8),
                stdout: stdout.as_bytes().to_vec(),
                stderr: stderr.as_bytes().to_vec(),
            })
        };
        let block = r#"{"decision":"block","reason":"2 tests failed"}"#;
        let failing_case = r#"{"cases":[{"suite":"unit","name":"adds","outcome":"fail","message":"\u001b[31mleft != right\u001b[0m"}]}"#;
        let cases = [
            ("exit 0, no output", out(0, "", ""), true, ""),
            ("exit 0, plain text", out(0, "all good", ""), true, ""),
            ("exit 0, passed", out(0, r#"{"passed":true}"#, ""), true, ""),
            ("exit 0, block", out(0, block, ""), false, "2 tests failed"),
            (
                "exit 0, failing case",
                out(0, failing_case, "boom"),
                false,
                "boom",
            ),
            (
                "exit 2, stderr",
                out(2, "", "test x failed"),
                false,
                "test x failed",
            ),
            ("exit 1", out(1, "", "oops"), false, "exited 1: oops"),
            (
                "malformed JSON",
                out(0, "{not json", ""),
                false,
                "malformed JSON",
            ),
            (
                "did not finish",
                Err("timed out after 1s".into()),
                false,
                "did not finish: timed out",
            ),
        ];
        for (label, output, passed, reason) in cases {
            let verdict = stop_verdict("verify", output);
            assert_eq!(verdict.passed, passed, "{label}: {verdict:?}");
            assert!(verdict.reason.contains(reason), "{label}: {verdict:?}");
            assert!(!verdict.cases.is_empty(), "{label}: no cases");
        }
        let verdict = stop_verdict("verify", out(0, failing_case, ""));
        assert_eq!(verdict.cases[0].message, "left != right");
    }
}
