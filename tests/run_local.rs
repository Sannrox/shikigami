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
