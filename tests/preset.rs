//! `--preset` end to end (ADR 0022): resolution narrows the base config, the
//! run attaches only the listed MCP tools, and a missing tool refuses to start.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use shikigami::{ChannelSink, Harness, HarnessEvent, RunRequest, StateRoot};
use tempfile::{TempDir, tempdir};

/// Base config with two offline mock MCP servers, `m` and `x`, both authorized.
fn write_base(dir: &Path, script: &str) -> PathBuf {
    let path = dir.join("shikigami.toml");
    let config = format!(
        r#"version = 1
[governance]
adapter = "none"
[model]
adapter = "scripted"
script_json = '''{script}'''
[events]
adapter = "none"
[workspace]
root = "{ws}"
[tools]
mode = "custom"
enabled = ["read_file", "report", "mcp.m.echo", "mcp.x.echo"]
[[tools.mcp_servers]]
name = "m"
command = "mock"
[[tools.mcp_servers]]
name = "x"
command = "mock"
"#,
        ws = dir.join("ws").display()
    );
    std::fs::write(&path, config).unwrap();
    path
}

fn write_preset(dir: &Path, tools: &str) -> PathBuf {
    let path = dir.join("narrow.toml");
    std::fs::write(
        &path,
        format!("name = \"narrow\"\nprompt = \"Echo once.\"\n[tools]\nenabled = [{tools}]\n"),
    )
    .unwrap();
    path
}

fn harness(dir: &TempDir, script: &str, tools: &str) -> Harness {
    let config = write_base(dir.path(), script);
    let preset = write_preset(dir.path(), tools);
    Harness::resolve_with_preset(
        Some(&config),
        StateRoot::new(dir.path().join("state")),
        dir.path(),
        None,
        Some(preset.to_str().unwrap()),
    )
    .unwrap()
}

#[tokio::test]
async fn preset_attaches_only_listed_tools_and_logs_its_id() {
    let dir = tempdir().unwrap();
    let script = r#"[
        {"tool_calls":[{"name":"mcp.x.echo","args_json":"{\"text\":\"no\"}"}]},
        {"tool_calls":[{"name":"mcp.m.echo","args_json":"{\"text\":\"hi\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
    ]"#;
    let harness = harness(&dir, script, r#""report", "mcp.m.echo""#);
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(RunRequest::new("echo"), Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        events.iter().any(|event| matches!(event,
            HarnessEvent::Message { text, .. } if text.starts_with("preset narrow@sha256:"))),
        "{events:?}"
    );
    let tool_ok = |tool: &str| {
        events.iter().find_map(|event| match event {
            HarnessEvent::ToolEnd { name, ok, .. } if name == tool => Some(*ok),
            _ => None,
        })
    };
    assert_eq!(tool_ok("mcp.m.echo"), Some(true), "{events:#?}");
    assert_eq!(
        tool_ok("mcp.x.echo"),
        Some(false),
        "unlisted server stays detached"
    );
}

#[tokio::test]
async fn preset_naming_a_missing_mcp_tool_refuses_to_start() {
    let dir = tempdir().unwrap();
    let harness = harness(&dir, "[]", r#""report", "mcp.m.absent""#);
    let error = harness
        .run(RunRequest::new("echo"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("do not offer mcp.m.absent"), "{error}");
}

#[tokio::test]
async fn resume_refuses_a_different_preset() {
    let dir = tempdir().unwrap();
    let script = r#"[{"tool_calls":[{"name":"mcp.m.echo","args_json":"{\"text\":\"hi\"}"}]}]"#;
    // The script runs out mid-run, leaving a resumable checkpoint. An error
    // carries no run id, so read it from the runs directory.
    harness(&dir, script, r#""report", "mcp.m.echo""#)
        .run(RunRequest::new("echo"))
        .await
        .unwrap_err();
    let runs = StateRoot::new(dir.path().join("state")).runs_dir();
    let run_id = std::fs::read_dir(runs)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();

    let unnarrowed = Harness::resolve_with_preset(
        Some(&dir.path().join("shikigami.toml")),
        StateRoot::new(dir.path().join("state")),
        dir.path(),
        None,
        None,
    )
    .unwrap();
    let mut resume = RunRequest::new("");
    resume.resume_run_id = Some(run_id);
    let error = unnarrowed.run(resume).await.unwrap_err().to_string();
    assert!(error.contains("this resume selects no preset"), "{error}");
}
