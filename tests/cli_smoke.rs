use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use shikigami::Config;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn version_prints_product_identity() {
    cargo_bin_cmd!("shikigami")
        .arg("version")
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "shikigami {}",
            env!("CARGO_PKG_VERSION")
        )));
}

#[test]
fn doctor_succeeds_on_local_defaults() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigami")
        .args(["--state", state.to_str().unwrap(), "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("status: ok"))
        .stdout(predicate::str::contains("profile:   local"))
        .stdout(predicate::str::contains("gov:       none"))
        .stdout(predicate::str::contains("tools.mode:       custom"))
        .stdout(predicate::str::contains("tools.configured: (none)"))
        .stdout(predicate::str::contains("tools.implicit:   (none)"))
        .stdout(predicate::str::contains(
            "tools.visible:    [read_file, write_file",
        ));
}

#[test]
fn doctor_models_reports_default_auto_route() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigami")
        .args(["--state", state.to_str().unwrap(), "doctor", "--models"])
        .assert()
        .success()
        .stdout(predicate::str::contains("models:"))
        .stdout(predicate::str::contains("auto (default)"));
}

#[test]
fn doctor_model_flag_overrides_configured_model() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "doctor",
            "--models",
            "--model",
            "local-selected-model",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("local-selected-model (default)"));
}

#[test]
fn doctor_fails_governed_without_endpoint() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");
    let config = dir.path().join("governed.toml");
    fs::write(
        &config,
        r#"
version = 1
[profile]
name = "governed"
"#,
    )
    .expect("write config");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "doctor",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("status: fail"));
}

#[test]
fn doctor_models_preserves_report_when_catalog_unavailable() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");
    let config = dir.path().join("governed.toml");
    fs::write(
        &config,
        r#"
version = 1
[profile]
name = "governed"
"#,
    )
    .expect("write config");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "doctor",
            "--models",
            "--json",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("\"ok\": false"))
        .stdout(predicate::str::contains("\"available_models\": []"))
        .stdout(predicate::str::contains("model_catalog_error"));
}

#[test]
fn local_scripted_run_writes_marker() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");
    let config = dir.path().join("local.toml");
    fs::write(
        &config,
        r#"
version = 1
[profile]
name = "local"
[governance]
adapter = "local"
[model]
adapter = "scripted"
[workspace]
adapter = "directory"
root = "."
[events]
adapter = "none"
"#,
    )
    .expect("write");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "run",
            "scripted demo",
            "--keep-workspace",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("success=true"));
}

#[test]
fn runs_diagnose_json_matches_library_terminal_class() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");
    let config = dir.path().join("local.toml");
    fs::write(
        &config,
        r#"
version = 1
[profile]
name = "local"
[governance]
adapter = "local"
[model]
adapter = "scripted"
[workspace]
adapter = "directory"
root = "."
[events]
adapter = "none"
"#,
    )
    .expect("write");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "run",
            "scripted demo",
            "--keep-workspace",
        ])
        .assert()
        .success();

    let listed = cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "runs",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let listing = String::from_utf8(listed).expect("runs utf8");
    let run_id = listing
        .lines()
        .next()
        .and_then(|line| line.split('\t').next())
        .expect("run id")
        .to_string();

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "runs",
            &run_id,
            "--diagnose",
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"schema_version\": 1"))
        .stdout(predicate::str::contains("\"class\": \"terminal\""))
        .stdout(predicate::str::contains("\"do_not_execute\""));
}

#[test]
fn plane_intake_rejects_ungoverned_host() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "serve",
            "--intake",
            "plane",
            "--max-jobs",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "plane intake requires governance.adapter = \"sekai-chisei\"",
        ));
}

fn replay_fixture(dir: &std::path::Path) -> (Config, PathBuf, PathBuf, PathBuf) {
    use shikigami::model::{ChatMessage, ToolCall};
    use shikigami::{
        ReplayBindings, ReplayEvidenceBundle, ReplayManifest, ReplayTerminalEvidence,
        SYSTEM_PROMPT, empty_workspace_digest, steps_from_messages, text_digest,
    };

    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.events.adapter = "none".into();
    config.workspace.adapter = "directory".into();
    config.workspace.root = dir.join("workspaces").to_string_lossy().into();
    config.model.adapter = "scripted".into();
    config.model.model = "deterministic-replay-model".into();
    config.model.script_json = Some(
        r#"[{"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}]"#
            .into(),
    );
    let config_path = dir.join("replay.toml");
    config.save(&config_path).expect("save config");

    let expected_messages = vec![
        ChatMessage {
            role: "assistant".into(),
            content: String::new(),
            tool_call_id: String::new(),
            tool_calls: vec![ToolCall {
                id: "report-1".into(),
                name: "report".into(),
                args_json: r#"{"summary":"done","success":true}"#.into(),
            }],
        },
        ChatMessage {
            role: "tool".into(),
            content: "report: done".into(),
            tool_call_id: "report-1".into(),
            tool_calls: vec![],
        },
    ];
    let bindings = ReplayBindings::for_replay(
        &config,
        "replay the source",
        SYSTEM_PROMPT,
        empty_workspace_digest(),
        text_digest("source checkpoint"),
    )
    .expect("bindings");
    let evidence = ReplayEvidenceBundle {
        schema_version: 1,
        source_run_id: "source-run-1".into(),
        source_logical_operation_id: Some("logical-operation-1".into()),
        task: "replay the source".into(),
        bindings,
        expected_steps: steps_from_messages(&expected_messages).expect("steps"),
        expected_terminal: ReplayTerminalEvidence {
            success: true,
            termination: "completed".into(),
            summary_digest: text_digest("done"),
        },
    };
    let manifest = ReplayManifest::for_evidence(&evidence).expect("manifest");
    let evidence_path = dir.join("evidence.json");
    let manifest_path = dir.join("manifest.json");
    fs::write(
        &evidence_path,
        serde_json::to_vec_pretty(&evidence).expect("evidence json"),
    )
    .expect("write evidence");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    (config, config_path, manifest_path, evidence_path)
}

#[tokio::test]
async fn replay_json_matches_library_comparisons() {
    use shikigami::{Harness, ReplayReport, ReplayRequest, StateRoot};

    let dir = tempdir().expect("tempdir");
    let (config, config_path, manifest_path, evidence_path) = replay_fixture(dir.path());
    let state = dir.path().join("state");
    let harness = Harness::from_config(config.clone(), StateRoot::new(&state)).expect("harness");
    let evidence: shikigami::ReplayEvidenceBundle =
        serde_json::from_slice(&fs::read(&evidence_path).expect("read evidence")).expect("parse");
    let manifest: shikigami::ReplayManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest")).expect("parse");
    let library = harness
        .replay(ReplayRequest::new(manifest, evidence))
        .await
        .expect("library replay");
    let library_report = library.report();

    let stdout = cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
            "replay",
            "--manifest",
            manifest_path.to_str().unwrap(),
            "--evidence",
            evidence_path.to_str().unwrap(),
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let cli: ReplayReport = serde_json::from_slice(&stdout).expect("cli json");
    assert_eq!(cli.schema_version, 1);
    assert_eq!(cli.steps, library_report.steps);
    assert_eq!(cli.terminal, library_report.terminal);
    assert_eq!(cli.success, library_report.success);
    assert_ne!(cli.run_id, "source-run-1");
}

#[test]
fn replay_rejects_unsupported_evidence_version() {
    let dir = tempdir().expect("tempdir");
    let (_config, config_path, manifest_path, evidence_path) = replay_fixture(dir.path());
    let mut evidence: serde_json::Value =
        serde_json::from_slice(&fs::read(&evidence_path).expect("read")).expect("json");
    evidence["schema_version"] = serde_json::json!(99);
    fs::write(
        &evidence_path,
        serde_json::to_vec_pretty(&evidence).expect("write"),
    )
    .expect("write");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            dir.path().join("state").to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
            "replay",
            "--manifest",
            manifest_path.to_str().unwrap(),
            "--evidence",
            evidence_path.to_str().unwrap(),
            "--json",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unsupported evidence schema version",
        ));
}

#[test]
fn replay_rejects_digest_mismatch_before_execution() {
    let dir = tempdir().expect("tempdir");
    let (_config, config_path, manifest_path, evidence_path) = replay_fixture(dir.path());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read")).expect("json");
    manifest["evidence_digest"] = serde_json::json!(shikigami::text_digest("tampered-evidence"));
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("write"),
    )
    .expect("write");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            dir.path().join("state").to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
            "replay",
            "--manifest",
            manifest_path.to_str().unwrap(),
            "--evidence",
            evidence_path.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("digest"));
}

#[tokio::test]
async fn run_content_json_matches_library_and_omits_payloads() {
    use shikigami::{
        ContentDisclosureState, ContentMessageV1, ContentPartDescriptor, ContentPartKind,
        ContentProcessRequestV1, ContentProcessResultV1, ContentProvenanceV1, Harness, StateRoot,
        digest_bytes,
    };

    let dir = tempdir().expect("tempdir");
    let payloads = dir.path().join("payloads");
    fs::create_dir_all(&payloads).expect("payloads");
    let bytes = b"hello from file";
    fs::write(payloads.join("payload-text-1"), bytes).expect("write payload");
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.events.adapter = "none".into();
    config.workspace.adapter = "directory".into();
    config.workspace.root = dir.path().join("workspaces").to_string_lossy().into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[{"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"ok\",\"success\":true}"}]}]"#
            .into(),
    );
    let config_path = dir.path().join("content.toml");
    config.save(&config_path).expect("save config");

    let request = ContentProcessRequestV1 {
        schema_version: 1,
        task: "inspect bounded content".into(),
        messages: vec![ContentMessageV1 {
            role: "user".into(),
            parts: vec![ContentPartDescriptor {
                part_id: "text-1".into(),
                kind: ContentPartKind::Text,
                media_type: "text/plain".into(),
                byte_length: bytes.len() as u64,
                sha256_digest: digest_bytes(bytes),
                reference: "payload-text-1".into(),
                provenance: ContentProvenanceV1 {
                    source: "cli".into(),
                    source_id: "fixture".into(),
                    source_version: "v1".into(),
                    observed_at_ms: 1,
                },
                disclosure_state: ContentDisclosureState::Accepted,
                disclosure_reason: String::new(),
            }],
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }],
        capabilities: None,
        keep_workspace: true,
        resume_run_id: None,
        payloads: [("payload-text-1".into(), "payload-text-1".into())]
            .into_iter()
            .collect(),
    };
    let request_path = dir.path().join("request.json");
    fs::write(
        &request_path,
        serde_json::to_vec_pretty(&request).expect("request json"),
    )
    .expect("write request");

    let state = dir.path().join("state");
    let harness = Harness::from_config(config, StateRoot::new(&state)).expect("harness");
    let library = harness
        .run_content(
            request
                .clone()
                .into_run_request(&payloads)
                .expect("run request"),
        )
        .await
        .expect("library content");
    let library_report = library.report();

    let stdout = cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
            "run-content",
            "--request",
            request_path.to_str().unwrap(),
            "--payloads",
            payloads.to_str().unwrap(),
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let text = String::from_utf8(stdout).expect("utf8");
    assert!(!text.contains("hello from file"), "{text}");
    let cli: ContentProcessResultV1 = serde_json::from_str(&text).expect("cli json");
    assert_eq!(cli.schema_version, 1);
    assert_eq!(cli.success, library_report.success);
    assert_eq!(cli.termination, library_report.termination);
    assert_eq!(
        cli.messages[0].parts[0].sha256_digest,
        library_report.messages[0].parts[0].sha256_digest
    );
}

#[test]
fn run_content_rejects_unknown_request_fields() {
    let dir = tempdir().expect("tempdir");
    let payloads = dir.path().join("payloads");
    fs::create_dir_all(&payloads).expect("payloads");
    let request_path = dir.path().join("request.json");
    fs::write(
        &request_path,
        br#"{"schema_version":1,"task":"x","messages":[],"payloads":{},"bytes":"nope"}"#,
    )
    .expect("write");
    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            dir.path().join("state").to_str().unwrap(),
            "run-content",
            "--request",
            request_path.to_str().unwrap(),
            "--payloads",
            payloads.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown field"));
}
