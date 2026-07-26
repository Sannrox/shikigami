//! Operator-trusted lifecycle hooks (settings-driven subprocesses).
//!
//! Hooks are **not** a sandbox. Only run commands you trust.

use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::process::Command;
use tokio::time::timeout;

use crate::config::HookSettings;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookEvent {
    PreRun,
    PostRun,
    PreTool,
    PostTool,
    OnPark,
}

impl HookEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreRun => "pre_run",
            Self::PostRun => "post_run",
            Self::PreTool => "pre_tool",
            Self::PostTool => "post_tool",
            Self::OnPark => "on_park",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pre_run" => Some(Self::PreRun),
            "post_run" => Some(Self::PostRun),
            "pre_tool" => Some(Self::PreTool),
            "post_tool" => Some(Self::PostTool),
            "on_park" => Some(Self::OnPark),
            _ => None,
        }
    }
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

    let mut child = Command::new(&hook.command)
        .args(&hook.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env("SHIKIGAMI_HOOK_EVENT", event.as_str())
        .spawn()
        .map_err(|e| format!("spawn: {e}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        let _ = stdin.write_all(body_str.as_bytes()).await;
        let _ = stdin.write_all(b"\n").await;
        drop(stdin);
    }

    match timeout(limit, child.wait_with_output()).await {
        Ok(Ok(out)) => {
            if out.status.success() {
                Ok(())
            } else {
                let err = String::from_utf8_lossy(&out.stderr);
                Err(format!("exit {}: {err}", out.status))
            }
        }
        Ok(Err(e)) => Err(format!("wait: {e}")),
        Err(_) => Err(format!("timed out after {limit:?}")),
    }
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
}
