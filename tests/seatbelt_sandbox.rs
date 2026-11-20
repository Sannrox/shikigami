//! Acceptance for #659: `seatbelt` isolation, doctor/run fail-closed when the
//! tier is unavailable, and the three denials plus an in-workspace build on macOS.

use std::path::Path;

use shikigami::{Config, EgressMode, Harness, RunRequest, SandboxBackend, StateRoot};
use tempfile::tempdir;

#[tokio::test]
async fn unavailable_seatbelt_fails_doctor_and_run_when_required() {
    if cfg!(target_os = "macos") {
        return;
    }
    let dir = tempdir().unwrap();
    let mut config = isolation_config(dir.path());
    config.governance.fail_closed = true;
    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let report = harness.doctor();
    assert!(!report.ok, "{:?}", report.lines);
    assert!(
        report
            .lines
            .iter()
            .any(|line| line.contains("seatbelt") && line.contains("macOS")),
        "{:?}",
        report.lines
    );
    let error = harness
        .run(RunRequest::new("must not start"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("seatbelt"), "{error}");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn seatbelt_denies_host_secret_socket_and_outside_write() {
    use std::fs;
    use std::sync::Arc;

    use shikigami::{ChannelSink, HarnessEvent};
    let dir = tempdir().unwrap();
    let secret = dir.path().join("host.secret");
    fs::write(&secret, "plane-token\n").unwrap();
    let escape = dir.path().join("outside.txt");
    let workspace = dir.path().join("ws");
    fs::create_dir_all(&workspace).unwrap();

    let mut config = isolation_config(dir.path());
    config.workspace.root = workspace.to_string_lossy().into();
    config.model.script_json = Some(script_json(&secret, &escape));

    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let report = harness.doctor();
    assert!(report.ok, "{:?}", report.lines);
    assert!(
        report
            .lines
            .iter()
            .any(|line| line.contains("backend=seatbelt")),
        "{:?}",
        report.lines
    );

    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("seatbelt denials");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success, "{}", result.summary);
    assert!(result.workspace.join("ok.txt").is_file());
    assert!(result.workspace.join("built.txt").is_file());
    assert!(!escape.exists(), "write escaped to {}", escape.display());

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    let failed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            HarnessEvent::ToolEnd {
                name,
                ok: false,
                detail,
                ..
            } if name == "bash" => Some(detail.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(failed.len(), 3, "{events:?}");
    assert!(
        failed.iter().any(|detail| {
            detail.contains("host.secret")
                || detail.contains("Permission")
                || detail.contains("denied")
        }),
        "{failed:?}"
    );
}

fn isolation_config(root: &Path) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.tools.enabled = vec!["bash".into(), "report".into()];
    config.sandbox.backend = SandboxBackend::Seatbelt;
    config.network.egress = EgressMode::Deny;
    config.workspace.root = root.join("ws").to_string_lossy().into();
    config
}

#[cfg(target_os = "macos")]
fn script_json(secret: &Path, escape: &Path) -> String {
    format!(
        r#"[
        {{"tool_calls":[{{"name":"bash","args_json":"{}"}}]}},
        {{"tool_calls":[{{"name":"bash","args_json":"{}"}}]}},
        {{"tool_calls":[{{"name":"bash","args_json":"{}"}}]}},
        {{"tool_calls":[{{"name":"bash","args_json":"{}"}}]}},
        {{"tool_calls":[{{"name":"bash","args_json":"{}"}}]}},
        {{"tool_calls":[{{"name":"report","args_json":"{{\"summary\":\"isolated\",\"success\":true}}"}}]}}
    ]"#,
        json_arg(&format!("cat {}", secret.display())),
        json_arg("echo >/dev/tcp/127.0.0.1/9"),
        json_arg(&format!("echo escaped > {}", escape.display())),
        json_arg("echo ok > ok.txt"),
        json_arg("echo echo built > build.sh && /bin/bash ./build.sh > built.txt"),
    )
}

#[cfg(target_os = "macos")]
fn json_arg(command: &str) -> String {
    serde_json::to_string(&serde_json::json!({ "command": command }))
        .unwrap()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}
