use shikigami::{Config, Harness, RunRequest, StateRoot};
use tempfile::tempdir;

#[tokio::test]
async fn local_scripted_end_to_end() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws-root").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let mut request = RunRequest::new("demo");
    request.keep_workspace = true;
    request.resume_run_id = None;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    assert!(result.turns >= 2);
    assert_eq!(result.termination, shikigami::RunTermination::Completed);
    let marker = result.workspace.join("SHIKIGAMI_OK.txt");
    assert!(marker.is_file(), "expected {}", marker.display());
}

#[tokio::test]
async fn custom_script_edit_flow() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "none".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"x.txt\",\"content\":\"one\"}"}]},
        {"tool_calls":[{"name":"edit","args_json":"{\"path\":\"x.txt\",\"old\":\"one\",\"new\":\"two\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"edited\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.events.adapter = "jsonl".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let mut request = RunRequest::new("edit flow");
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    let text = std::fs::read_to_string(result.workspace.join("x.txt")).unwrap();
    assert_eq!(text, "two");
}

#[tokio::test]
async fn live_event_stream_receives_scripted_sequence() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("demo");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    assert!(
        events
            .iter()
            .any(|e| matches!(e, HarnessEvent::Prompt { .. })),
        "missing Prompt: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, HarnessEvent::ToolStart { name, .. } if name == "write_file")),
        "missing write_file start: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, HarnessEvent::RunFinished { success: true, .. })),
        "missing RunFinished: {events:?}"
    );
}

#[tokio::test]
async fn bash_tool_events_cannot_emit_configured_harness_credentials() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let token_name = "SHIKIGAMI_TEST_EVENT_PLANE_TOKEN_153";
    let token_value = "must-not-reach-events";
    // SAFETY: unique integration-test name, removed immediately after the run.
    unsafe {
        std::env::set_var(token_name, token_value);
    }
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.governance.token_env = Some(token_name.into());
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"bash","args_json":"{\"command\":\"printf '%s' \\\"${SHIKIGAMI_TEST_EVENT_PLANE_TOKEN_153-unset}\\\"\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"credential isolated\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.tools.enabled = vec!["bash".into(), "report".into()];
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("prove credential isolation");
    request.keep_workspace = true;
    let result = harness.run_with_events(request, Some(Arc::new(sink))).await;
    // SAFETY: cleanup of the unique integration-test name above.
    unsafe {
        std::env::remove_var(token_name);
    }
    let result = result.unwrap();
    assert!(result.success);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::ToolEnd {
                name,
                ok: true,
                detail
            } if name == "bash" && detail == "unset"
        )),
        "missing isolated Bash ToolEnd: {events:?}"
    );
    assert!(
        !format!("{events:?}").contains(token_value),
        "synthetic credential appeared in event stream"
    );
}

/// Denied authorize_tool must not execute the host tool (allow-list path).
/// Governed external-action deny uses the same run-loop branch.
#[tokio::test]
async fn denied_tool_authorization_does_not_execute() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    // Only report is enabled — write_file must be denied and not create the file.
    config.tools.enabled = vec!["report".into()];
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"FORBIDDEN.txt\",\"content\":\"nope\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done without write\",\"success\":true}"}]}
    ]"#
        .into(),
    );

    let harness = Harness::from_config(config, state).unwrap();
    let mut request = RunRequest::new("deny write");
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    assert!(
        !result.workspace.join("FORBIDDEN.txt").exists(),
        "denied write_file must not create the file"
    );
}
