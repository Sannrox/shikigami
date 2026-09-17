use std::fs;
use std::path::Path;

use shikigami::{Config, Harness, RunRequest, StateRoot};
use tempfile::tempdir;

const SECRET_TASK: &str = "SECRET_TASK_PAYLOAD_284";
const SECRET_INPUT: &str = "SECRET_TOOL_INPUT_284";
const SECRET_OUTPUT: &str = "SECRET_TOOL_OUTPUT_284";

#[tokio::test]
async fn scripted_run_exports_identity_spans_without_payloads() {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("spans.json");
    let mut config = offline_config(dir.path());
    config.tracing.enabled = true;
    config.tracing.exporter = "otlp".into();
    config.tracing.endpoint = Some(spans_path.to_string_lossy().into());
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"note.txt\",\"content\":\"SECRET_TOOL_OUTPUT_284 SECRET_TOOL_INPUT_284\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"SECRET_TASK_PAYLOAD_284\",\"success\":true}"}]}
    ]"#
        .into(),
    );

    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let mut request = RunRequest::new(SECRET_TASK);
    request.logical_operation_id = Some("parent-op-284".into());
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success, "{}", result.summary);

    let exported = fs::read_to_string(&spans_path).unwrap();
    assert!(exported.contains("\"name\": \"run\""), "{exported}");
    assert!(exported.contains("\"name\": \"turn\""), "{exported}");
    assert!(exported.contains("\"name\": \"tool\""), "{exported}");
    assert!(exported.contains(&result.run_id), "{exported}");
    assert!(exported.contains("parent-op-284"), "{exported}");
    assert!(exported.contains("\"key\": \"run_id\""), "{exported}");
    assert!(exported.contains("\"key\": \"attempt_id\""), "{exported}");
    assert!(
        exported.contains("\"key\": \"logical_operation_id\""),
        "{exported}"
    );
    assert!(
        exported.contains("\"key\": \"plan_operation_id\""),
        "{exported}"
    );
    assert!(exported.contains("\"key\": \"call_id\""), "{exported}");
    assert!(exported.contains("tool-"), "{exported}");
    for secret in [SECRET_TASK, SECRET_INPUT, SECRET_OUTPUT] {
        assert!(
            !exported.contains(secret),
            "payload leaked into spans: {secret}\n{exported}"
        );
    }
}

#[tokio::test]
async fn disabled_exporter_leaves_offline_run_unchanged() {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("must-not-exist.json");
    let mut config = offline_config(dir.path());
    config.tracing.enabled = false;
    config.tracing.exporter = "otlp".into();
    config.tracing.endpoint = Some(spans_path.to_string_lossy().into());

    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let report = harness.doctor();
    assert!(report.ok);
    assert!(
        report
            .lines
            .iter()
            .any(|line| line.contains("tracing:    disabled")),
        "{:?}",
        report.lines
    );

    let mut request = RunRequest::new("demo");
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    assert!(
        !spans_path.exists(),
        "disabled exporter wrote {}",
        spans_path.display()
    );
}

fn offline_config(root: &Path) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = root.join("ws").to_string_lossy().into();
    config
}
