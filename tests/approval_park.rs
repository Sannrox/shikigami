//! Scripted-plane park/resume for `require_approval` (#283).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use shikigami::checkpoint::{
    ApprovalPark, Checkpoint, StagedToolExecution, StagedToolReport, ToolExecutionStatus,
};
use shikigami::config::Config;
use shikigami::content::ContentModelTurnV1;
use shikigami::governance::{
    ApprovalState, ContentTurnContext, GovernanceError, GovernancePort, LocalGovernance, RunHandle,
    RunOutcome, now_unix_ms, resolve_approval_wait,
};
use shikigami::model::{ChatMessage, ModelTurn};
use shikigami::registry::RunRegistry;
use shikigami::run::{Engine, ParkKind, RunRequest, RunTermination};
use shikigami::state::StateRoot;
use shikigami::tools::ToolDef;
use shikigami::{events, export_replay_inputs, model, workspace};
use tempfile::tempdir;

const APPROVAL_ID: &str = "approval-scripted-1";
const WRITE_ARGS: &str = r#"{"path":"hello.txt","content":"approved"}"#;

fn write_script() -> String {
    r#"[
        {"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"hello.txt\",\"content\":\"approved\"}"}]},
        {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
    ]"#
    .into()
}

fn engine_config(root: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(write_script());
    config.events.adapter = "none".into();
    config.workspace.root = root.join("ws").to_string_lossy().into();
    config.tools.enabled = vec!["write_file".into(), "read_file".into(), "report".into()];
    config
}

fn build_engine(
    config: Config,
    state: &StateRoot,
    state_mutex: Arc<Mutex<ApprovalState>>,
) -> Engine {
    build_engine_with_inject(config, state, state_mutex, Arc::new(Mutex::new(None)))
}

fn build_engine_with_inject(
    config: Config,
    state: &StateRoot,
    state_mutex: Arc<Mutex<ApprovalState>>,
    inject_authorize: Arc<Mutex<Option<GovernanceError>>>,
) -> Engine {
    let inner = LocalGovernance::from_config(&config);
    Engine::new(
        config.clone(),
        Arc::new(ScriptedApprovalGovernance {
            inner,
            state: state_mutex,
            inject_authorize,
        }),
        Arc::from(workspace::from_config(&config).expect("workspace")),
        Arc::from(model::from_config(&config).expect("model")),
        Arc::from(events::from_config(&config, &state.runs_dir()).expect("events")),
        state.runs_dir(),
        Arc::new(RunRegistry::new(state.path()).expect("registry")),
    )
}

struct ScriptedApprovalGovernance {
    inner: LocalGovernance,
    state: Arc<Mutex<ApprovalState>>,
    inject_authorize: Arc<Mutex<Option<GovernanceError>>>,
}

impl ScriptedApprovalGovernance {
    fn current_state(&self) -> ApprovalState {
        self.state.lock().expect("approval state").clone()
    }
}

#[async_trait]
impl GovernancePort for ScriptedApprovalGovernance {
    fn id(&self) -> &'static str {
        "scripted-approval"
    }

    fn health_detail(&self) -> String {
        "scripted require_approval plane".into()
    }

    fn health_ok(&self) -> bool {
        true
    }

    async fn begin_run(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
    ) -> Result<RunHandle, GovernanceError> {
        self.inner
            .begin_run(run_id, task, logical_operation_id)
            .await
    }

    async fn begin_run_with_checkpoint(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        checkpoint: Option<&shikigami::checkpoint::GovernanceCheckpoint>,
    ) -> Result<RunHandle, GovernanceError> {
        self.inner
            .begin_run_with_checkpoint(run_id, task, logical_operation_id, checkpoint)
            .await
    }

    fn checkpoint_state(
        &self,
        run_id: &str,
    ) -> Option<shikigami::checkpoint::GovernanceCheckpoint> {
        self.inner.checkpoint_state(run_id)
    }

    async fn stage_tool_execution(
        &self,
        handle: &RunHandle,
        execution: StagedToolExecution,
    ) -> Result<(), GovernanceError> {
        self.inner.stage_tool_execution(handle, execution).await
    }

    async fn mark_tool_execution_started(
        &self,
        handle: &RunHandle,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.inner
            .mark_tool_execution_started(handle, call_id)
            .await
    }

    async fn mark_tool_execution_complete(
        &self,
        handle: &RunHandle,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.inner
            .mark_tool_execution_complete(handle, call_id)
            .await
    }

    async fn clear_staged_tool_executions(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        self.inner.clear_staged_tool_executions(handle).await
    }

    async fn recover_staged_tool_executions(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        self.inner.recover_staged_tool_executions(handle).await
    }

    async fn stage_tool_reports(
        &self,
        handle: &RunHandle,
        reports: Vec<StagedToolReport>,
    ) -> Result<(), GovernanceError> {
        self.inner.stage_tool_reports(handle, reports).await
    }

    async fn replay_staged_tool_reports(&self, handle: &RunHandle) -> Result<(), GovernanceError> {
        self.inner.replay_staged_tool_reports(handle).await
    }

    async fn plan_turn(
        &self,
        handle: &RunHandle,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
        local_model: &dyn shikigami::model::ModelPort,
    ) -> Result<ModelTurn, GovernanceError> {
        self.inner
            .plan_turn(handle, system, messages, tools, local_model)
            .await
    }

    async fn plan_content_turn(
        &self,
        handle: &RunHandle,
        system: &str,
        context: ContentTurnContext<'_>,
    ) -> Result<ContentModelTurnV1, GovernanceError> {
        self.inner.plan_content_turn(handle, system, context).await
    }

    async fn authorize_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError> {
        self.authorize_tool_with_id(handle, name, name, args_json)
            .await
    }

    async fn authorize_tool_with_id(
        &self,
        handle: &RunHandle,
        call_id: &str,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError> {
        if matches!(
            name,
            "report" | "escalate" | "todo_write" | "read_file" | "glob" | "grep"
        ) {
            return self
                .inner
                .authorize_tool_with_id(handle, call_id, name, args_json)
                .await;
        }
        if let Some(park) = self
            .inner
            .checkpoint_state(&handle.run_id)
            .and_then(|checkpoint| checkpoint.approval_park)
            && park.call_id == call_id
        {
            if let Some(error) = self
                .inject_authorize
                .lock()
                .expect("inject authorize")
                .take()
            {
                return Err(error);
            }
            return resolve_approval_wait(&park, self.current_state(), now_unix_ms());
        }
        Err(GovernanceError::RequireApproval {
            approval_id: APPROVAL_ID.into(),
            authorization_id: format!("auth:{call_id}"),
            request_digest: shikigami::replay::text_digest(args_json),
            expires_at_ms: 0,
            deadline_ms: 0,
            reason: "scripted plane requires approval".into(),
        })
    }

    async fn approval_state(&self, approval_id: &str) -> Result<ApprovalState, GovernanceError> {
        if approval_id != APPROVAL_ID {
            return Err(GovernanceError::Message(format!(
                "unknown approval `{approval_id}`"
            )));
        }
        Ok(self.current_state())
    }

    async fn record_approval_park(
        &self,
        handle: &RunHandle,
        park: ApprovalPark,
    ) -> Result<(), GovernanceError> {
        self.inner.record_approval_park(handle, park).await
    }

    async fn clear_approval_park(&self, handle: &RunHandle) -> Result<(), GovernanceError> {
        self.inner.clear_approval_park(handle).await
    }

    async fn report_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        self.inner.report_tool(handle, name, ok, detail).await
    }

    async fn report_tool_with_id(
        &self,
        handle: &RunHandle,
        call_id: &str,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        self.inner
            .report_tool_with_id(handle, call_id, name, ok, detail)
            .await
    }

    async fn complete_run(
        &self,
        handle: &RunHandle,
        outcome: RunOutcome,
    ) -> Result<(), GovernanceError> {
        self.inner.complete_run(handle, outcome).await
    }
}

fn assert_parked(checkpoint: &Checkpoint, workspace: &std::path::Path) {
    assert!(checkpoint.park.is_some(), "expected park scratch");
    let park = checkpoint.approval_park().expect("approval park identity");
    assert_eq!(park.approval_id, APPROVAL_ID);
    assert_eq!(park.tool_name, "write_file");
    assert_eq!(
        checkpoint.park.as_ref().map(|park| park.kind),
        Some(shikigami::ParkKind::Approval),
        "the park kind is recorded durably, not inferred from the approval identity"
    );
    assert_eq!(
        format!("sha256:{}", park.arguments_digest),
        shikigami::replay::digest_bytes(WRITE_ARGS.as_bytes()),
        "park must bind the exact parked arguments"
    );
    assert!(
        checkpoint
            .governance
            .as_ref()
            .unwrap()
            .pending_tool_executions
            .iter()
            .all(|execution| execution.status == ToolExecutionStatus::Authorizing),
        "parked execution must stay authorizing"
    );
    assert!(
        !checkpoint
            .messages
            .iter()
            .any(|message| message.role == "tool" && message.content.contains("approved")),
        "parked export must not invent a tool result"
    );
    assert!(
        !workspace.join("hello.txt").is_file(),
        "tool must not execute while parked"
    );
}

#[tokio::test]
async fn require_approval_parks_and_resume_pending_stays_parked() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let engine = build_engine(config.clone(), &state, Arc::clone(&plane));
    let mut request = RunRequest::new("write after approval");
    request.keep_workspace = true;
    let result = engine.run(request).await.unwrap();
    assert_eq!(result.termination, RunTermination::Parked);
    assert_eq!(result.park.as_ref().unwrap().kind, ParkKind::Approval);
    assert_eq!(
        result.park.as_ref().unwrap().approval_id.as_deref(),
        Some(APPROVAL_ID)
    );
    let checkpoint = Checkpoint::load(&state.runs_dir(), &result.run_id).unwrap();
    assert_parked(&checkpoint, &result.workspace);

    let resume_engine = build_engine(config, &state, plane);
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(result.run_id.clone());
    let parked = resume_engine.run(resume).await.unwrap();
    assert_eq!(parked.termination, RunTermination::Parked);
    assert_eq!(
        parked.park.as_ref().unwrap().approval_id.as_deref(),
        Some(APPROVAL_ID)
    );
    assert!(!result.workspace.join("hello.txt").is_file());
}

#[tokio::test]
async fn transient_reauthorization_keeps_the_approval_wait() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Approved {
        permit_id: "permit-1".into(),
    }));
    let engine = build_engine(config.clone(), &state, Arc::clone(&plane));
    let mut request = RunRequest::new("write after approval");
    request.keep_workspace = true;
    let parked = engine.run(request).await.unwrap();
    assert_eq!(parked.termination, RunTermination::Parked);

    let inject = Arc::new(Mutex::new(Some(GovernanceError::Unavailable(
        "plane temporarily unavailable".into(),
    ))));
    let resume_engine = build_engine_with_inject(config, &state, plane, inject);
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(parked.run_id.clone());
    let error = resume_engine.run(resume).await.expect_err("transient");
    assert!(error.to_string().contains("unavailable"), "{error}");
    let checkpoint = Checkpoint::load(&state.runs_dir(), &parked.run_id).unwrap();
    assert_eq!(checkpoint.approval_park().unwrap().approval_id, APPROVAL_ID);
    assert!(!parked.workspace.join("hello.txt").is_file());
}

#[tokio::test]
async fn approval_resume_executes_the_parked_tool_once() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let engine = build_engine(config.clone(), &state, Arc::clone(&plane));
    let mut request = RunRequest::new("write after approval");
    request.keep_workspace = true;
    let parked = engine.run(request).await.unwrap();
    assert_eq!(parked.termination, RunTermination::Parked);
    *plane.lock().unwrap() = ApprovalState::Approved {
        permit_id: "permit-1".into(),
    };

    let resume_engine = build_engine(config, &state, plane);
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(parked.run_id);
    let completed = resume_engine.run(resume).await.unwrap();
    assert!(completed.success);
    assert_eq!(completed.termination, RunTermination::Completed);
    let body = std::fs::read_to_string(completed.workspace.join("hello.txt")).unwrap();
    assert_eq!(body, "approved");
}

#[tokio::test]
async fn denied_approval_resumes_without_effect() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let engine = build_engine(config.clone(), &state, Arc::clone(&plane));
    let mut request = RunRequest::new("write after approval");
    request.keep_workspace = true;
    let parked = engine.run(request).await.unwrap();
    *plane.lock().unwrap() = ApprovalState::Denied {
        reason: "operator denied".into(),
    };

    let resume_engine = build_engine(config, &state, plane);
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(parked.run_id);
    let finished = resume_engine.run(resume).await.unwrap();
    assert!(!parked.workspace.join("hello.txt").is_file());
    assert!(
        finished.success || finished.termination == RunTermination::Completed,
        "denied tool should continue to report: {} {}",
        finished.success,
        finished.summary
    );
}

#[tokio::test]
async fn process_death_while_parked_recovers_same_approval_identity() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let run_id;
    let workspace;
    {
        let engine = build_engine(config.clone(), &state, Arc::clone(&plane));
        let mut request = RunRequest::new("write after approval");
        request.keep_workspace = true;
        let parked = engine.run(request).await.unwrap();
        run_id = parked.run_id;
        workspace = parked.workspace;
    }
    let checkpoint = Checkpoint::load(&state.runs_dir(), &run_id).unwrap();
    assert_parked(&checkpoint, &workspace);

    let recovered = build_engine(config, &state, plane);
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(run_id);
    let parked = recovered.run(resume).await.unwrap();
    assert_eq!(parked.termination, RunTermination::Parked);
    assert_eq!(
        parked.park.as_ref().unwrap().approval_id.as_deref(),
        Some(APPROVAL_ID)
    );
    assert_parked(
        &Checkpoint::load(&state.runs_dir(), &parked.run_id).unwrap(),
        &workspace,
    );
}

#[tokio::test]
async fn earlier_serial_tool_is_kept_when_a_later_call_parks() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let mut config = engine_config(dir.path());
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[
            {"id":"read-1","name":"read_file","args_json":"{\"path\":\"missing.txt\"}"},
            {"id":"write-1","name":"write_file","args_json":"{\"path\":\"hello.txt\",\"content\":\"approved\"}"}
        ]},
        {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let engine = build_engine(config.clone(), &state, plane);
    let mut request = RunRequest::new("read then write");
    request.keep_workspace = true;
    let parked = engine.run(request).await.unwrap();
    assert_eq!(parked.termination, RunTermination::Parked);
    let checkpoint = Checkpoint::load(&state.runs_dir(), &parked.run_id).unwrap();
    assert!(
        checkpoint
            .messages
            .iter()
            .any(|message| message.role == "tool" && message.tool_call_id == "read-1"),
        "completed prefix must be checkpointed before the approval park"
    );
    let governance = checkpoint.governance.as_ref().unwrap();
    let park = governance.approval_park.as_ref().unwrap();
    assert_eq!(
        park.call_id, "tool-1-1-write-1",
        "parked write must keep its original batch index"
    );
    assert!(
        !governance
            .pending_tool_executions
            .iter()
            .any(|execution| execution.call_id == "tool-1-0-write-1"
                || execution.call_id == "tool-1-0-read-1"),
        "resume must not reindex the parked write or leave the prefix as started"
    );
    assert!(
        governance
            .pending_tool_executions
            .iter()
            .any(|execution| execution.call_id == "tool-1-1-write-1"
                && execution.status == ToolExecutionStatus::Authorizing),
        "parked write must stay authorizing under its original call id"
    );
    assert!(
        governance
            .pending_tool_executions
            .iter()
            .all(|execution| execution.status == ToolExecutionStatus::Authorizing),
        "parked write must stay authorizing; completed prefix must not remain started"
    );

    let resume_engine = build_engine(
        config,
        &state,
        Arc::new(Mutex::new(ApprovalState::Approved {
            permit_id: "permit-1".into(),
        })),
    );
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(parked.run_id);
    let completed = resume_engine.run(resume).await.unwrap();
    assert!(completed.success, "{}", completed.summary);
    assert_eq!(
        std::fs::read_to_string(completed.workspace.join("hello.txt")).unwrap(),
        "approved"
    );
}

#[tokio::test]
async fn parked_replay_export_includes_approval_identity() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let engine = build_engine(config.clone(), &state, plane);
    let mut request = RunRequest::new("write after approval");
    request.keep_workspace = true;
    let parked = engine.run(request).await.unwrap();
    let report = export_replay_inputs(&state, &parked.run_id, &config).unwrap();
    assert!(!report.complete);
    assert!(report.missing.iter().any(|item| item == "terminal"));
    assert_eq!(report.approval_id.as_deref(), Some(APPROVAL_ID));
    assert!(report.evidence.is_none());
}

async fn resume_approved_after_tampering_park(
    tamper: impl FnOnce(&mut ApprovalPark),
) -> (
    shikigami::run::RunResult,
    String,
    std::path::PathBuf,
    tempfile::TempDir,
) {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let engine = build_engine(config.clone(), &state, Arc::clone(&plane));
    let mut request = RunRequest::new("write after approval");
    request.keep_workspace = true;
    let parked = engine.run(request).await.unwrap();
    assert_eq!(parked.termination, RunTermination::Parked);

    let mut checkpoint = Checkpoint::load(&state.runs_dir(), &parked.run_id).unwrap();
    let park = checkpoint
        .governance
        .as_mut()
        .and_then(|governance| governance.approval_park.as_mut())
        .expect("approval park identity");
    tamper(park);
    checkpoint.save(&state.runs_dir()).unwrap();

    *plane.lock().unwrap() = ApprovalState::Approved {
        permit_id: "permit-1".into(),
    };
    let resume_engine = build_engine(config, &state, plane);
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(parked.run_id);
    let finished = resume_engine.run(resume).await.unwrap();
    let transcript = Checkpoint::load(&state.runs_dir(), &finished.run_id)
        .map(|checkpoint| {
            checkpoint
                .messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    (finished, transcript, parked.workspace, dir)
}

#[tokio::test]
async fn resume_with_a_different_tool_name_is_denied_without_effect() {
    let (_finished, transcript, workspace, _dir) =
        resume_approved_after_tampering_park(|park| park.tool_name = "bash".into()).await;
    assert!(
        !workspace.join("hello.txt").is_file(),
        "a park bound to another tool must not execute the resumed call"
    );
    assert!(
        transcript.contains("does not match the resumed tool call"),
        "{transcript}"
    );
}

#[tokio::test]
async fn resume_with_different_arguments_is_denied_without_effect() {
    let (_finished, _transcript, workspace, _dir) = resume_approved_after_tampering_park(|park| {
        park.arguments_digest = "0".repeat(64);
    })
    .await;
    assert!(
        !workspace.join("hello.txt").is_file(),
        "a park bound to other arguments must not execute the resumed call"
    );
}

#[tokio::test]
async fn resume_without_a_parked_arguments_digest_fails_closed() {
    let (_finished, _transcript, workspace, _dir) =
        resume_approved_after_tampering_park(|park| park.arguments_digest.clear()).await;
    assert!(
        !workspace.join("hello.txt").is_file(),
        "an unbound park must not execute the resumed call"
    );
}

/// Park a run, rewrite its checkpoint, approve, and resume with `answer`.
async fn resume_after_rewriting_checkpoint(
    rewrite: impl FnOnce(&mut Checkpoint),
    answer: Option<&str>,
) -> (
    Result<shikigami::run::RunResult, shikigami::run::RunError>,
    std::path::PathBuf,
    tempfile::TempDir,
) {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let config = engine_config(dir.path());
    let plane = Arc::new(Mutex::new(ApprovalState::Pending));
    let engine = build_engine(config.clone(), &state, Arc::clone(&plane));
    let mut request = RunRequest::new("write after approval");
    request.keep_workspace = true;
    let parked = engine.run(request).await.unwrap();
    assert_eq!(parked.termination, RunTermination::Parked);

    let mut checkpoint = Checkpoint::load(&state.runs_dir(), &parked.run_id).unwrap();
    rewrite(&mut checkpoint);
    checkpoint.save(&state.runs_dir()).unwrap();

    *plane.lock().unwrap() = ApprovalState::Approved {
        permit_id: "permit-1".into(),
    };
    let resume_engine = build_engine(config, &state, plane);
    let mut resume = RunRequest::new("");
    resume.keep_workspace = true;
    resume.resume_run_id = Some(parked.run_id);
    resume.resume_answer = answer.map(str::to_string);
    (resume_engine.run(resume).await, parked.workspace, dir)
}

fn strip_approval_identity(checkpoint: &mut Checkpoint) {
    checkpoint
        .governance
        .as_mut()
        .expect("governance checkpoint")
        .approval_park = None;
}

#[tokio::test]
async fn stripping_the_approval_identity_cannot_turn_the_wait_into_an_escalate_answer() {
    let (outcome, workspace, _dir) =
        resume_after_rewriting_checkpoint(strip_approval_identity, Some("just run it")).await;
    let error = outcome.expect_err("a stripped approval park must not accept an operator answer");
    assert!(
        error
            .to_string()
            .contains("disagrees with its approval identity"),
        "{error}"
    );
    assert!(
        !workspace.join("hello.txt").is_file(),
        "the approval-parked tool must not run after the strip"
    );
}

#[tokio::test]
async fn stripping_the_approval_identity_without_an_answer_is_also_refused() {
    let (outcome, workspace, _dir) =
        resume_after_rewriting_checkpoint(strip_approval_identity, None).await;
    assert!(outcome.is_err(), "a stripped approval park must not resume");
    assert!(!workspace.join("hello.txt").is_file());
}

#[tokio::test]
async fn relabelling_an_approval_park_as_escalate_is_refused() {
    let (outcome, workspace, _dir) = resume_after_rewriting_checkpoint(
        |checkpoint| {
            checkpoint.park.as_mut().expect("park").kind = shikigami::ParkKind::Escalate;
        },
        Some("just run it"),
    )
    .await;
    let error = outcome.expect_err("an approval identity with an escalate kind must not resume");
    assert!(
        error
            .to_string()
            .contains("disagrees with its approval identity"),
        "{error}"
    );
    assert!(!workspace.join("hello.txt").is_file());
}
