use std::sync::Arc;

use shikigami::model::{ChatMessage, ToolCall};
use shikigami::{
    Config, EventSink, Harness, HarnessEvent, ReplayBindings, ReplayComparisonStatus,
    ReplayEvidenceBundle, ReplayManifest, ReplayRequest, ReplayStepEvidence,
    ReplayTerminalEvidence, RunRequest, SYSTEM_PROMPT, StateRoot, empty_workspace_digest,
    steps_from_messages, text_digest, workspace_digest,
};
use tempfile::tempdir;
use tokio::sync::watch;

struct CancelAfterTool {
    sender: watch::Sender<bool>,
}

impl EventSink for CancelAfterTool {
    fn id(&self) -> &'static str {
        "cancel-after-tool"
    }

    fn emit(&self, event: HarnessEvent) {
        if matches!(event, HarnessEvent::ToolEnd { .. }) {
            let _ = self.sender.send(true);
        }
    }

    fn health_detail(&self) -> String {
        "ok".into()
    }
}

fn base_config(root: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.events.adapter = "none".into();
    config.workspace.adapter = "directory".into();
    config.workspace.root = root.join("workspaces").to_string_lossy().into();
    config.model.adapter = "scripted".into();
    config.model.model = "deterministic-replay-model".into();
    config.model.script_json = Some(
        r#"[{"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}]"#
            .into(),
    );
    config
}

fn successful_request(config: &Config) -> ReplayRequest {
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
        config,
        "replay the source",
        SYSTEM_PROMPT,
        empty_workspace_digest(),
        text_digest("source checkpoint"),
    )
    .unwrap();
    let evidence = ReplayEvidenceBundle {
        schema_version: 1,
        source_run_id: "source-run-1".into(),
        source_logical_operation_id: Some("logical-operation-1".into()),
        task: "replay the source".into(),
        bindings,
        expected_steps: steps_from_messages(&expected_messages).unwrap(),
        expected_terminal: ReplayTerminalEvidence {
            success: true,
            termination: "completed".into(),
            summary_digest: text_digest("done"),
        },
    };
    let manifest = ReplayManifest::for_evidence(&evidence).unwrap();
    let mut request = ReplayRequest::new(manifest, evidence);
    request.keep_workspace = true;
    request
}

#[tokio::test]
async fn replay_runs_as_new_isolated_attempt_and_compares_evidence() {
    let root = tempdir().unwrap();
    let config = base_config(root.path());
    let state = StateRoot::new(root.path().join("state"));
    let harness = Harness::from_config(config.clone(), state.clone()).unwrap();

    let result = harness.replay(successful_request(&config)).await.unwrap();

    assert_ne!(result.run.run_id, "source-run-1");
    assert!(result.run.success);
    assert!(result.run.workspace.is_dir());
    assert!(
        result
            .steps
            .iter()
            .all(|step| step.status == ReplayComparisonStatus::Equal)
    );
    assert_eq!(result.terminal.status, ReplayComparisonStatus::Equal);
    let checkpoint =
        shikigami::checkpoint::Checkpoint::load(&state.runs_dir(), &result.run.run_id).unwrap();
    let replay = checkpoint.replay.expect("replay checkpoint");
    assert_eq!(replay.manifest_digest, result.manifest_digest);
    assert_eq!(replay.source_run_id, "source-run-1");
    assert_eq!(replay.workspace, result.run.workspace.display().to_string());
    assert_eq!(replay.comparison_cursor, 2);
}

#[tokio::test]
async fn replay_rejects_model_binding_drift_before_model_execution() {
    let root = tempdir().unwrap();
    let original = base_config(root.path());
    let request = successful_request(&original);
    let mut changed = original;
    changed.model.model = "different-model".into();
    let harness = Harness::from_config(changed, StateRoot::new(root.path().join("state"))).unwrap();

    let error = harness.replay(request).await.unwrap_err();

    assert!(
        error.to_string().contains("binding `model` changed"),
        "{error}"
    );
}

#[tokio::test]
async fn replay_rejects_execution_policy_drift_before_admission() {
    let root = tempdir().unwrap();
    let original = base_config(root.path());
    let request = successful_request(&original);
    let mut changed = original;
    changed.run.max_turns -= 1;
    let state = StateRoot::new(root.path().join("state"));
    let harness = Harness::from_config(changed, state.clone()).unwrap();

    let error = harness.replay(request).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("binding `policy_evidence` changed"),
        "{error}"
    );
    assert_eq!(std::fs::read_dir(state.runs_dir()).unwrap().count(), 0);
}

#[tokio::test]
async fn replay_denies_effect_capable_tool_before_execution() {
    let root = tempdir().unwrap();
    let mut config = base_config(root.path());
    config.model.script_json = Some(
        r#"[{"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"pwned.txt\",\"content\":\"no\"}"}]}]"#
            .into(),
    );
    let state = StateRoot::new(root.path().join("state"));
    let harness = Harness::from_config(config.clone(), state.clone()).unwrap();
    let mut request = successful_request(&config);
    request.evidence.expected_steps = Vec::<ReplayStepEvidence>::new();
    request.manifest = ReplayManifest::for_evidence(&request.evidence).unwrap();

    let error = harness.replay(request).await.unwrap_err();

    assert!(error.to_string().contains("replay forbids"), "{error}");
    let mut stack = vec![root.path().join("workspaces")];
    while let Some(path) = stack.pop() {
        if !path.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                stack.push(entry.path());
            } else {
                assert_ne!(entry.file_name(), "pwned.txt");
            }
        }
    }
}

#[tokio::test]
async fn replay_fails_closed_when_governance_evidence_is_required() {
    let root = tempdir().unwrap();
    let mut config = base_config(root.path());
    let request = successful_request(&config);
    config.governance.fail_closed = true;
    let state = StateRoot::new(root.path().join("state"));
    let harness = Harness::from_config(config, state.clone()).unwrap();

    let error = harness.replay(request).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("governed replay evidence cannot be verified"),
        "{error}"
    );
    assert_eq!(std::fs::read_dir(state.runs_dir()).unwrap().count(), 0);
}

#[tokio::test]
async fn replay_restart_keeps_attempt_identity_and_does_not_repeat_completed_steps() {
    let root = tempdir().unwrap();
    let mut first_config = base_config(root.path());
    first_config.model.script_json = Some(
        r#"[
            {"usage":{"input_tokens":2,"output_tokens":3},"tool_calls":[{"id":"grep-1","name":"grep","args_json":"{\"pattern\":\"needle\"}"}]},
            {"usage":{"input_tokens":5,"output_tokens":7},"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
        ]"#
        .into(),
    );
    let restarted_config = first_config.clone();
    let state = StateRoot::new(root.path().join("state"));
    let mut first_request = successful_request(&first_config);
    let (cancel_sender, cancel_receiver) = watch::channel(false);
    first_request.cancel = Some(cancel_receiver);
    first_request.evidence.expected_steps.clear();
    first_request.manifest = ReplayManifest::for_evidence(&first_request.evidence).unwrap();
    let manifest = first_request.manifest.clone();
    let evidence = first_request.evidence.clone();
    let first = Harness::from_config(first_config, state.clone()).unwrap();

    let error = first
        .replay_with_events(
            first_request,
            Some(Arc::new(CancelAfterTool {
                sender: cancel_sender,
            })),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");

    let replay_run_id = std::fs::read_dir(state.runs_dir())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.path().join("checkpoint.json").is_file())
        .unwrap()
        .file_name()
        .to_string_lossy()
        .into_owned();

    let mut resume_request = ReplayRequest::new(manifest, evidence);
    resume_request.keep_workspace = true;
    resume_request.resume_run_id = Some(replay_run_id.clone());

    let restarted = Harness::from_config(restarted_config, state.clone()).unwrap();
    let result = restarted.replay(resume_request).await.unwrap();

    assert_eq!(result.run.run_id, replay_run_id);
    assert_eq!(result.run.turns, 2);
    assert_eq!(result.run.usage.input_tokens, 7);
    assert_eq!(result.run.usage.output_tokens, 10);
    let checkpoint =
        shikigami::checkpoint::Checkpoint::load(&state.runs_dir(), &result.run.run_id).unwrap();
    let model_steps = steps_from_messages(&checkpoint.messages)
        .unwrap()
        .into_iter()
        .filter(|step| step.kind == shikigami::ReplayStepKind::Model)
        .count();
    assert_eq!(model_steps, 2);
}

#[tokio::test]
async fn replay_requires_report_for_terminal_completion() {
    let root = tempdir().unwrap();
    let mut config = base_config(root.path());
    config.model.script_json = Some(r#"[{"content":"plain completion"}]"#.into());
    let harness =
        Harness::from_config(config.clone(), StateRoot::new(root.path().join("state"))).unwrap();
    let mut request = successful_request(&config);
    request.evidence.expected_steps.clear();
    request.manifest = ReplayManifest::for_evidence(&request.evidence).unwrap();

    let error = harness.replay(request).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("replay requires the terminal `report` tool"),
        "{error}"
    );
}

#[tokio::test]
async fn ordinary_resume_is_denied_while_replay_resume_recovers_terminal_result() {
    let root = tempdir().unwrap();
    let config = base_config(root.path());
    let state = StateRoot::new(root.path().join("state"));
    let harness = Harness::from_config(config.clone(), state.clone()).unwrap();
    let request = successful_request(&config);
    let manifest = request.manifest.clone();
    let evidence = request.evidence.clone();
    let replay = harness.replay(request).await.unwrap();
    let mut resume = shikigami::RunRequest::new("");
    resume.resume_run_id = Some(replay.run.run_id.clone());
    resume.keep_workspace = true;

    let error = harness.run(resume).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("replay checkpoints must be resumed"),
        "{error}"
    );

    // Simulate a process stopping after terminal outcome durability but before
    // artifact/workspace finalization was marked complete.
    let mut checkpoint =
        shikigami::checkpoint::Checkpoint::load(&state.runs_dir(), &replay.run.run_id).unwrap();
    let terminal = checkpoint
        .replay
        .as_mut()
        .unwrap()
        .terminal
        .as_mut()
        .unwrap();
    terminal.finalized = false;
    terminal.artifact_dir = None;
    checkpoint.save(&state.runs_dir()).unwrap();

    let mut replay_resume = ReplayRequest::new(manifest, evidence);
    replay_resume.resume_run_id = Some(replay.run.run_id.clone());
    let recovered = harness.replay(replay_resume).await.unwrap();
    assert_eq!(recovered.run.run_id, replay.run.run_id);
    assert_eq!(recovered.run.summary, replay.run.summary);
    assert_eq!(recovered.run.turns, replay.run.turns);
    assert_eq!(recovered.steps, replay.steps);
    assert_eq!(recovered.terminal, replay.terminal);
    let checkpoint =
        shikigami::checkpoint::Checkpoint::load(&state.runs_dir(), &recovered.run.run_id).unwrap();
    assert!(checkpoint.replay.unwrap().terminal.unwrap().finalized);
}

#[tokio::test]
async fn terminal_recovery_rejects_a_workspace_outside_the_run_boundary() {
    let root = tempdir().unwrap();
    let config = base_config(root.path());
    let state = StateRoot::new(root.path().join("state"));
    let harness = Harness::from_config(config.clone(), state.clone()).unwrap();
    let request = successful_request(&config);
    let manifest = request.manifest.clone();
    let evidence = request.evidence.clone();
    let replay = harness.replay(request).await.unwrap();
    let outside = root.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    let marker = outside.join("keep.txt");
    std::fs::write(&marker, "keep").unwrap();

    let mut checkpoint =
        shikigami::checkpoint::Checkpoint::load(&state.runs_dir(), &replay.run.run_id).unwrap();
    checkpoint.workspace = outside.clone();
    checkpoint.keep_workspace = false;
    let replay_checkpoint = checkpoint.replay.as_mut().unwrap();
    replay_checkpoint.workspace = outside.display().to_string();
    replay_checkpoint.terminal.as_mut().unwrap().finalized = false;
    checkpoint.save(&state.runs_dir()).unwrap();

    let mut resume = ReplayRequest::new(manifest, evidence);
    resume.resume_run_id = Some(replay.run.run_id);
    let error = harness.replay(resume).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match configured workspace"),
        "{error}"
    );
    assert!(marker.is_file());
}

#[tokio::test]
async fn export_from_snapshot_enabled_run_is_admissible_and_ignores_mutated_workspace() {
    let root = tempdir().unwrap();
    let mut config = base_config(root.path());
    config.workspace.snapshot = true;
    let state = StateRoot::new(root.path().join("state"));
    let harness = Harness::from_config(config.clone(), state.clone()).unwrap();
    let mut request = RunRequest::new("export source");
    request.keep_workspace = true;
    let run = harness.run(request).await.unwrap();
    assert!(run.success);
    std::fs::write(run.workspace.join("later.txt"), "after").unwrap();

    let report = harness.export_replay_inputs(&run.run_id).unwrap();
    assert!(report.complete, "{:?}", report.missing);
    let evidence = report.evidence.expect("complete evidence");
    let manifest = report.manifest.expect("complete manifest");
    assert_eq!(evidence.bindings.inputs_digest, empty_workspace_digest());
    assert_ne!(
        workspace_digest(&run.workspace).unwrap(),
        evidence.bindings.inputs_digest
    );
    let reconstructed = ReplayManifest::for_evidence(&evidence).unwrap();
    assert_eq!(reconstructed, manifest);

    let replay_harness = Harness::from_config(config, state).unwrap();
    let replayed = replay_harness
        .replay(ReplayRequest::new(manifest, evidence))
        .await
        .unwrap();
    assert_ne!(replayed.run.run_id, run.run_id);
    assert!(
        replayed
            .steps
            .iter()
            .all(|step| step.status == ReplayComparisonStatus::Equal)
    );
    assert_eq!(replayed.terminal.status, ReplayComparisonStatus::Equal);
}

#[tokio::test]
async fn export_after_resume_still_binds_the_first_snapshot() {
    let root = tempdir().unwrap();
    let mut config = base_config(root.path());
    config.workspace.snapshot = true;
    config.run.max_turns = 1;
    config.model.script_json = Some(
        r#"[
            {"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"later.txt\",\"content\":\"after\"}"}]},
            {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
        ]"#
        .into(),
    );
    let state = StateRoot::new(root.path().join("state"));
    let first = Harness::from_config(config.clone(), state.clone()).unwrap();
    let mut request = RunRequest::new("export after resume");
    request.keep_workspace = true;
    let error = first.run(request).await.unwrap_err();
    assert!(error.to_string().contains("max turns"), "{error}");
    let run_id = std::fs::read_dir(state.runs_dir())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.path().join("checkpoint.json").is_file())
        .unwrap()
        .file_name()
        .to_string_lossy()
        .into_owned();
    let checkpoint = shikigami::checkpoint::Checkpoint::load(&state.runs_dir(), &run_id).unwrap();
    assert!(checkpoint.workspace.join("later.txt").is_file());

    config.run.max_turns = 8;
    let resumed = Harness::from_config(config, state.clone()).unwrap();
    let mut resume = RunRequest::new("export after resume");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(run_id.clone());
    let run = resumed.run(resume).await.unwrap();
    assert!(run.success);
    assert!(run.workspace.join("later.txt").is_file());

    let report = resumed.export_replay_inputs(&run.run_id).unwrap();
    assert!(report.complete, "{:?}", report.missing);
    let evidence = report.evidence.expect("complete evidence");
    assert_eq!(evidence.bindings.inputs_digest, empty_workspace_digest());
    assert!(
        !state
            .runs_dir()
            .join(&run.run_id)
            .join("snapshots/initial/later.txt")
            .exists()
    );
}

#[tokio::test]
async fn export_does_not_treat_a_resume_capture_as_original_inputs() {
    let root = tempdir().unwrap();
    let mut config = base_config(root.path());
    config.workspace.snapshot = false;
    config.run.max_turns = 1;
    config.model.script_json = Some(
        r#"[
            {"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"later.txt\",\"content\":\"after\"}"}]},
            {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
        ]"#
        .into(),
    );
    let state = StateRoot::new(root.path().join("state"));
    let first = Harness::from_config(config.clone(), state.clone()).unwrap();
    let mut request = RunRequest::new("no original snapshot");
    request.keep_workspace = true;
    let error = first.run(request).await.unwrap_err();
    assert!(error.to_string().contains("max turns"), "{error}");
    let run_id = std::fs::read_dir(state.runs_dir())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.path().join("checkpoint.json").is_file())
        .unwrap()
        .file_name()
        .to_string_lossy()
        .into_owned();

    config.workspace.snapshot = true;
    config.run.max_turns = 8;
    let resumed = Harness::from_config(config, state.clone()).unwrap();
    let mut resume = RunRequest::new("no original snapshot");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(run_id.clone());
    let run = resumed.run(resume).await.unwrap();
    assert!(run.success);
    assert!(run.workspace.join("later.txt").is_file());

    let report = resumed.export_replay_inputs(&run.run_id).unwrap();
    assert!(!report.complete);
    assert!(report.missing.iter().any(|item| item == "inputs"));
    assert!(
        !state
            .runs_dir()
            .join(&run.run_id)
            .join("snapshots/initial")
            .exists()
    );
}
