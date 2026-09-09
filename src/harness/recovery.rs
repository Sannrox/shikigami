//! Read-only recovery diagnosis for one retained run (ADR 0008).

use serde::{Deserialize, Serialize};

use crate::checkpoint::{Checkpoint, CheckpointError, GovernanceCheckpoint, ToolExecutionStatus};
use crate::config::Config;
use crate::registry::{RunRecord, RunRegistry};
use crate::state::StateRoot;

use super::HarnessError;

/// Stable recovery-diagnosis JSON contract (`schema_version` = 1).
///
/// Breaking field renames/removals require a schema_version bump and CHANGELOG.
pub const RECOVERY_DIAGNOSIS_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryDiagnosis {
    pub schema_version: u32,
    pub run_id: String,
    pub class: RecoveryClass,
    pub reason: String,
    pub allowed_next_steps: Vec<RecoveryNextStep>,
}

impl RecoveryDiagnosis {
    pub const SCHEMA_VERSION: u32 = RECOVERY_DIAGNOSIS_SCHEMA_VERSION;

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("RecoveryDiagnosis is always serializable")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryClass {
    SafeResume,
    ReportOnly,
    UncertainTool,
    InvalidCheckpoint,
    Terminal,
}

impl RecoveryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SafeResume => "safe_resume",
            Self::ReportOnly => "report_only",
            Self::UncertainTool => "uncertain_tool",
            Self::InvalidCheckpoint => "invalid_checkpoint",
            Self::Terminal => "terminal",
        }
    }
}

/// Operator categories. These are not execution permits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryNextStep {
    Resume,
    ResumeWithAnswer,
    ReconcileReports,
    Inspect,
    DoNotExecute,
}

impl RecoveryNextStep {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Resume => "resume",
            Self::ResumeWithAnswer => "resume_with_answer",
            Self::ReconcileReports => "reconcile_reports",
            Self::Inspect => "inspect",
            Self::DoNotExecute => "do_not_execute",
        }
    }
}

/// Inspect one retained run without executing.
pub fn diagnose_run(
    state: &StateRoot,
    run_id: &str,
    config: &Config,
) -> Result<RecoveryDiagnosis, HarnessError> {
    let registry = RunRegistry::inspect(state.path());
    diagnose_with_registry(&registry, &state.runs_dir(), run_id, config)
}

pub(super) fn diagnose_with_registry(
    registry: &RunRegistry,
    runs_dir: &std::path::Path,
    run_id: &str,
    config: &Config,
) -> Result<RecoveryDiagnosis, HarnessError> {
    let record = registry.load(run_id)?;
    let checkpoint = match Checkpoint::load(runs_dir, run_id) {
        Ok(checkpoint) => Some(checkpoint),
        Err(CheckpointError::Missing(_)) => None,
        Err(error) => return Ok(invalid(run_id, error.to_string())),
    };
    let content_terminal = match checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.content.as_ref())
    {
        Some(binding) => match crate::content::load_sidecar(runs_dir, run_id, binding) {
            Ok(sidecar) => sidecar.terminal.as_ref().map(|terminal| terminal.finalized),
            Err(error) => return Ok(invalid(run_id, error.to_string())),
        },
        None => None,
    };
    let live_owner = registry.run_is_active(run_id).unwrap_or(false);
    Ok(with_live_owner(
        classify(
            run_id,
            &record,
            checkpoint.as_ref(),
            content_terminal,
            config,
            runs_dir,
        ),
        live_owner,
    ))
}

fn classify(
    run_id: &str,
    record: &RunRecord,
    checkpoint: Option<&Checkpoint>,
    content_terminal: Option<bool>,
    config: &Config,
    runs_dir: &std::path::Path,
) -> RecoveryDiagnosis {
    let Some(checkpoint) = checkpoint else {
        if is_registry_terminal(record) {
            return terminal(run_id, "registry is terminal and no checkpoint is retained");
        }
        return invalid(run_id, "checkpoint is missing");
    };

    if let Some(execution) = in_doubt_execution(checkpoint.governance.as_ref()) {
        return RecoveryDiagnosis {
            schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
            run_id: run_id.into(),
            class: RecoveryClass::UncertainTool,
            reason: format!(
                "tool execution `{}` (`{}`) is in-doubt ({})",
                execution.call_id,
                execution.name,
                execution_status_label(&execution.status)
            ),
            allowed_next_steps: vec![RecoveryNextStep::Inspect, RecoveryNextStep::DoNotExecute],
        };
    }

    if let Some(count) = pending_report_count(checkpoint.governance.as_ref()) {
        if let Some(invalid_resume) =
            resume_prerequisite_failure(run_id, checkpoint, config, runs_dir)
        {
            return invalid_resume;
        }
        return RecoveryDiagnosis {
            schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
            run_id: run_id.into(),
            class: RecoveryClass::ReportOnly,
            reason: format!(
                "{count} staged tool report(s) can be replayed without re-executing the tool"
            ),
            allowed_next_steps: vec![
                RecoveryNextStep::ReconcileReports,
                RecoveryNextStep::Resume,
                RecoveryNextStep::Inspect,
            ],
        };
    }

    if checkpoint
        .replay
        .as_ref()
        .and_then(|replay| replay.terminal.as_ref())
        .is_some_and(|terminal| terminal.finalized)
        || content_terminal == Some(true)
        || is_registry_terminal(record)
    {
        return terminal(
            run_id,
            "retained run is already terminal; do not start a new attempt from this checkpoint",
        );
    }

    if checkpoint
        .replay
        .as_ref()
        .and_then(|replay| replay.terminal.as_ref())
        .is_some_and(|terminal| !terminal.finalized)
    {
        if let Some(invalid_resume) =
            replay_finalization_failure(run_id, record, checkpoint, config, runs_dir)
        {
            return invalid_resume;
        }
        return RecoveryDiagnosis {
            schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
            run_id: run_id.into(),
            class: RecoveryClass::SafeResume,
            reason: "replay terminal is recorded; resume through Harness::replay to finalize"
                .into(),
            allowed_next_steps: vec![RecoveryNextStep::Resume, RecoveryNextStep::Inspect],
        };
    }

    if let Some(invalid_resume) = resume_prerequisite_failure(run_id, checkpoint, config, runs_dir)
    {
        return invalid_resume;
    }

    if checkpoint.park.is_some() {
        return RecoveryDiagnosis {
            schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
            run_id: run_id.into(),
            class: RecoveryClass::SafeResume,
            reason: "run is parked; supply resume_answer / --answer to continue".into(),
            allowed_next_steps: vec![
                RecoveryNextStep::ResumeWithAnswer,
                RecoveryNextStep::Inspect,
            ],
        };
    }

    if content_terminal == Some(false) {
        return RecoveryDiagnosis {
            schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
            run_id: run_id.into(),
            class: RecoveryClass::SafeResume,
            reason: "content terminal is recorded; resume through Harness::run_content to finalize"
                .into(),
            allowed_next_steps: vec![RecoveryNextStep::Resume, RecoveryNextStep::Inspect],
        };
    }

    let reason = if checkpoint.replay.is_some() {
        "replay attempt is resumable through Harness::replay with the same manifest"
    } else if checkpoint.content.is_some() {
        "content run is resumable through Harness::run_content"
    } else if authorizing_only(checkpoint.governance.as_ref()) {
        "authorizing-only tool marker is safe to retry under the original identity"
    } else {
        "checkpoint is resumable; diagnosis does not execute the run"
    };
    RecoveryDiagnosis {
        schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
        run_id: run_id.into(),
        class: RecoveryClass::SafeResume,
        reason: reason.into(),
        allowed_next_steps: vec![RecoveryNextStep::Resume, RecoveryNextStep::Inspect],
    }
}

fn replay_finalization_failure(
    run_id: &str,
    record: &RunRecord,
    checkpoint: &Checkpoint,
    config: &Config,
    runs_dir: &std::path::Path,
) -> Option<RecoveryDiagnosis> {
    match checkpoint.workspace.try_exists() {
        Ok(true) => {
            if let Err(error) =
                crate::run::validate_resumed_workspace(config, runs_dir, run_id, checkpoint)
            {
                return Some(invalid(run_id, error.to_string()));
            }
            None
        }
        Ok(false) => {
            let retained = record
                .artifact_dir
                .as_deref()
                .filter(|path| std::path::Path::new(path).is_dir());
            if retained.is_none() {
                Some(invalid(
                    run_id,
                    format!(
                        "replay workspace is unavailable before artifact finalization: {}",
                        checkpoint.workspace.display()
                    ),
                ))
            } else {
                None
            }
        }
        Err(error) => Some(invalid(
            run_id,
            format!(
                "replay workspace cannot be inspected: {}: {error}",
                checkpoint.workspace.display()
            ),
        )),
    }
}

fn resume_prerequisite_failure(
    run_id: &str,
    checkpoint: &Checkpoint,
    config: &Config,
    runs_dir: &std::path::Path,
) -> Option<RecoveryDiagnosis> {
    if let Err(error) = crate::run::validate_resumed_workspace(config, runs_dir, run_id, checkpoint)
    {
        return Some(invalid(run_id, error.to_string()));
    }
    if let Err(error) = checkpoint.validate_prompt(crate::run::SYSTEM_PROMPT) {
        return Some(invalid(run_id, error.to_string()));
    }
    None
}

fn is_registry_terminal(record: &RunRecord) -> bool {
    matches!(
        record.status.as_str(),
        "completed" | "failed" | "cancelled" | "timed_out" | "max_turns"
    )
}

fn in_doubt_execution(
    governance: Option<&GovernanceCheckpoint>,
) -> Option<&crate::checkpoint::StagedToolExecution> {
    governance.and_then(|governance| {
        governance
            .pending_tool_executions
            .iter()
            .find(|execution| execution.status != ToolExecutionStatus::Authorizing)
    })
}

fn authorizing_only(governance: Option<&GovernanceCheckpoint>) -> bool {
    governance.is_some_and(|governance| {
        !governance.pending_tool_executions.is_empty()
            && governance
                .pending_tool_executions
                .iter()
                .all(|execution| execution.status == ToolExecutionStatus::Authorizing)
    })
}

fn pending_report_count(governance: Option<&GovernanceCheckpoint>) -> Option<usize> {
    let count = governance
        .map(|governance| governance.pending_tool_reports.len())
        .unwrap_or(0);
    (count > 0).then_some(count)
}

fn execution_status_label(status: &ToolExecutionStatus) -> &'static str {
    match status {
        ToolExecutionStatus::Authorizing => "authorizing",
        ToolExecutionStatus::Started => "started",
        ToolExecutionStatus::Completed => "completed",
    }
}

fn invalid(run_id: &str, reason: impl Into<String>) -> RecoveryDiagnosis {
    RecoveryDiagnosis {
        schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
        run_id: run_id.into(),
        class: RecoveryClass::InvalidCheckpoint,
        reason: reason.into(),
        allowed_next_steps: vec![RecoveryNextStep::Inspect, RecoveryNextStep::DoNotExecute],
    }
}

fn with_live_owner(mut diagnosis: RecoveryDiagnosis, live: bool) -> RecoveryDiagnosis {
    if !live {
        return diagnosis;
    }
    diagnosis.reason = format!(
        "{}; owner lease is live, wait until it expires before resume",
        diagnosis.reason
    );
    diagnosis.allowed_next_steps.retain(|step| {
        matches!(
            step,
            RecoveryNextStep::Inspect | RecoveryNextStep::DoNotExecute
        )
    });
    if !diagnosis
        .allowed_next_steps
        .contains(&RecoveryNextStep::Inspect)
    {
        diagnosis.allowed_next_steps.push(RecoveryNextStep::Inspect);
    }
    if !diagnosis
        .allowed_next_steps
        .contains(&RecoveryNextStep::DoNotExecute)
    {
        diagnosis
            .allowed_next_steps
            .push(RecoveryNextStep::DoNotExecute);
    }
    diagnosis
}

fn terminal(run_id: &str, reason: impl Into<String>) -> RecoveryDiagnosis {
    RecoveryDiagnosis {
        schema_version: RecoveryDiagnosis::SCHEMA_VERSION,
        run_id: run_id.into(),
        class: RecoveryClass::Terminal,
        reason: reason.into(),
        allowed_next_steps: vec![RecoveryNextStep::Inspect, RecoveryNextStep::DoNotExecute],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{
        CHECKPOINT_VERSION, ParkedState, StagedToolExecution, StagedToolReport,
    };
    use crate::replay::{ReplayCheckpoint, ReplayTerminalCheckpoint};
    use crate::run::{RunResult, RunTermination};
    use crate::state::StateRoot;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn state() -> (tempfile::TempDir, StateRoot, RunRegistry) {
        let directory = tempdir().unwrap();
        let state = StateRoot::new(directory.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        let registry = RunRegistry::new(state.path()).unwrap();
        (directory, state, registry)
    }

    fn diagnose(state: &StateRoot, run_id: &str) -> Result<RecoveryDiagnosis, HarnessError> {
        diagnose_run(state, run_id, &Config::default())
    }

    fn stale_owner(state: &StateRoot, run_id: &str) {
        let run_dir = state.runs_dir().join(run_id);
        let _ = std::fs::remove_file(run_dir.join("owner"));
        let path = run_dir.join("run.json");
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record["last_heartbeat_at_ms"] = serde_json::json!(0);
        std::fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    }

    fn checkpoint(run_id: &str, workspace: PathBuf) -> Checkpoint {
        Checkpoint {
            version: CHECKPOINT_VERSION,
            run_id: run_id.into(),
            task: "task".into(),
            prompt_id: crate::checkpoint::prompt_id(crate::run::SYSTEM_PROMPT),
            messages: vec![],
            completed_turns: 1,
            workspace,
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: None,
            todos: vec![],
            governance: Some(GovernanceCheckpoint {
                operation_id: "op".into(),
                logical_operation_id: "op".into(),
                ..GovernanceCheckpoint::default()
            }),
            replay: None,
            content: None,
        }
    }

    fn unfinalized_replay(run_id: &str, workspace: PathBuf) -> Checkpoint {
        let mut checkpoint = checkpoint(run_id, workspace);
        checkpoint.replay = Some(ReplayCheckpoint {
            manifest_digest: "manifest".into(),
            source_run_id: "source".into(),
            workspace: checkpoint.workspace.display().to_string(),
            comparison_cursor: 0,
            usage: crate::model::TokenUsage::default(),
            terminal: Some(ReplayTerminalCheckpoint {
                success: true,
                termination: RunTermination::Completed,
                summary: "replayed".into(),
                prompt_id: "p".into(),
                usage: crate::model::TokenUsage::default(),
                cost: None,
                finalized: false,
                artifact_dir: None,
            }),
        });
        checkpoint
    }

    #[test]
    fn missing_run_fails_explicitly() {
        let (_directory, state, _registry) = state();
        let error = diagnose(&state, "missing").unwrap_err();
        assert!(error.to_string().contains("not found"), "{error}");
    }

    #[test]
    fn missing_checkpoint_on_active_run_is_invalid() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(diagnosis.reason.contains("missing"), "{}", diagnosis.reason);
        assert_eq!(
            diagnosis.allowed_next_steps,
            vec![RecoveryNextStep::Inspect, RecoveryNextStep::DoNotExecute]
        );
    }

    #[test]
    fn unsupported_checkpoint_version_is_invalid() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace);
        checkpoint.version = 99;
        let path = crate::checkpoint::path_for(&state.runs_dir(), "run-1");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec_pretty(&checkpoint).unwrap()).unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(
            diagnosis.reason.contains("unsupported checkpoint version"),
            "{}",
            diagnosis.reason
        );
    }

    #[test]
    fn started_execution_is_uncertain() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace);
        checkpoint
            .governance
            .as_mut()
            .unwrap()
            .pending_tool_executions = vec![StagedToolExecution {
            call_id: "tool-1-0-effect-1".into(),
            name: "bash".into(),
            args_json: "{}".into(),
            status: ToolExecutionStatus::Started,
        }];
        checkpoint.save(&state.runs_dir()).unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::UncertainTool);
        assert!(
            diagnosis.reason.contains("in-doubt"),
            "{}",
            diagnosis.reason
        );
        assert!(!diagnosis.reason.contains("args_json"));
        assert_eq!(
            diagnosis.allowed_next_steps,
            vec![RecoveryNextStep::Inspect, RecoveryNextStep::DoNotExecute]
        );
    }

    #[test]
    fn staged_reports_are_report_only() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace);
        checkpoint.governance.as_mut().unwrap().pending_tool_reports = vec![StagedToolReport {
            call_id: "tool-1-0-effect-1".into(),
            name: "bash".into(),
            ok: true,
            detail: "ok".into(),
        }];
        checkpoint.save(&state.runs_dir()).unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::ReportOnly);
        assert!(
            diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::ReconcileReports)
        );
    }

    #[test]
    fn parked_run_is_safe_resume_with_answer() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace);
        checkpoint.park = Some(ParkedState {
            reason: "need human".into(),
            question: "approve?".into(),
            tool_call_id: "esc-1".into(),
        });
        checkpoint.save(&state.runs_dir()).unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::SafeResume);
        assert!(
            !diagnosis.reason.contains("need human"),
            "{}",
            diagnosis.reason
        );
        assert!(
            !diagnosis.reason.contains("approve?"),
            "{}",
            diagnosis.reason
        );
        assert_eq!(
            diagnosis.allowed_next_steps,
            vec![
                RecoveryNextStep::ResumeWithAnswer,
                RecoveryNextStep::Inspect
            ]
        );
    }

    #[test]
    fn failed_registry_with_staged_reports_is_report_only() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace.clone());
        checkpoint.governance.as_mut().unwrap().pending_tool_reports = vec![StagedToolReport {
            call_id: "tool-1-0-effect-1".into(),
            name: "bash".into(),
            ok: true,
            detail: "ok".into(),
        }];
        checkpoint.save(&state.runs_dir()).unwrap();
        registry
            .finish_result(&RunResult {
                run_id: "run-1".into(),
                success: false,
                summary: "governance report failed".into(),
                turns: 1,
                workspace,
                artifact_dir: None,
                termination: RunTermination::Failed,
                park: None,
                prompt_id: "p".into(),
                usage: crate::model::TokenUsage::default(),
                cost: None,
                todos: vec![],
            })
            .unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::ReportOnly);
        assert!(
            diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::ReconcileReports)
        );
    }

    #[test]
    fn completed_run_is_terminal() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        checkpoint("run-1", workspace.clone())
            .save(&state.runs_dir())
            .unwrap();
        registry
            .finish_result(&RunResult {
                run_id: "run-1".into(),
                success: true,
                summary: "done".into(),
                turns: 1,
                workspace,
                artifact_dir: None,
                termination: RunTermination::Completed,
                park: None,
                prompt_id: "p".into(),
                usage: crate::model::TokenUsage::default(),
                cost: None,
                todos: vec![],
            })
            .unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::Terminal);
        assert!(
            diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::DoNotExecute)
        );
    }

    #[test]
    fn live_owner_lease_strips_resume_next_steps() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        checkpoint("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::SafeResume);
        assert!(
            diagnosis.reason.contains("owner lease is live"),
            "{}",
            diagnosis.reason
        );
        assert!(
            !diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::Resume)
        );
        assert!(
            diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::Inspect)
        );
    }

    #[test]
    fn authorizing_only_is_safe_resume() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace);
        checkpoint
            .governance
            .as_mut()
            .unwrap()
            .pending_tool_executions = vec![StagedToolExecution {
            call_id: "tool-1-0-effect-1".into(),
            name: "bash".into(),
            args_json: "{}".into(),
            status: ToolExecutionStatus::Authorizing,
        }];
        checkpoint.save(&state.runs_dir()).unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::SafeResume);
        assert!(
            diagnosis.reason.contains("authorizing-only"),
            "{}",
            diagnosis.reason
        );
        assert!(
            diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::Resume)
        );
    }

    #[test]
    fn prompt_mismatch_is_invalid_not_safe_resume() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace);
        checkpoint.prompt_id = crate::checkpoint::prompt_id("other prompt");
        checkpoint.save(&state.runs_dir()).unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(diagnosis.reason.contains("prompt"), "{}", diagnosis.reason);
    }

    #[test]
    fn completed_run_without_workspace_is_still_terminal() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        checkpoint("run-1", workspace.clone())
            .save(&state.runs_dir())
            .unwrap();
        registry
            .finish_result(&RunResult {
                run_id: "run-1".into(),
                success: true,
                summary: "done".into(),
                turns: 1,
                workspace,
                artifact_dir: None,
                termination: RunTermination::Completed,
                park: None,
                prompt_id: "p".into(),
                usage: crate::model::TokenUsage::default(),
                cost: None,
                todos: vec![],
            })
            .unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::Terminal);
    }

    #[test]
    fn missing_workspace_is_invalid_not_safe_resume() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        checkpoint("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(
            diagnosis.reason.contains("cannot be resolved")
                || diagnosis.reason.contains("unavailable"),
            "{}",
            diagnosis.reason
        );
    }

    #[test]
    fn terminal_registry_wins_over_parked_checkpoint() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace.clone());
        checkpoint.park = Some(ParkedState {
            reason: "need human".into(),
            question: "approve?".into(),
            tool_call_id: "esc-1".into(),
        });
        checkpoint.save(&state.runs_dir()).unwrap();
        registry
            .finish_result(&RunResult {
                run_id: "run-1".into(),
                success: false,
                summary: "failed after park".into(),
                turns: 1,
                workspace,
                artifact_dir: None,
                termination: RunTermination::Failed,
                park: None,
                prompt_id: "p".into(),
                usage: crate::model::TokenUsage::default(),
                cost: None,
                todos: vec![],
            })
            .unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::Terminal);
    }

    #[test]
    fn missing_content_sidecar_is_invalid() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut checkpoint = checkpoint("run-1", workspace);
        checkpoint.content = Some(crate::content::ContentCheckpointBinding {
            schema_version: crate::content::CONTENT_SCHEMA_VERSION,
            generation: 1,
            slot: "content-checkpoint-a.json".into(),
            sha256_digest: "deadbeef".into(),
            resolver_id: "memory".into(),
        });
        checkpoint.save(&state.runs_dir()).unwrap();
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
    }

    #[test]
    fn workspace_adapter_mismatch_is_invalid() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        checkpoint("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        stale_owner(&state, "run-1");
        let mut config = Config::default();
        config.workspace.adapter = "git-worktree".into();
        let diagnosis = diagnose_run(&state, "run-1", &config).unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(diagnosis.reason.contains("adapter"), "{}", diagnosis.reason);
    }

    #[test]
    fn workspace_path_mismatch_is_invalid() {
        let (directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let other = directory.path().join("other-workspace");
        std::fs::create_dir_all(&other).unwrap();
        checkpoint("run-1", other).save(&state.runs_dir()).unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(
            diagnosis
                .reason
                .contains("does not match configured workspace"),
            "{}",
            diagnosis.reason
        );
    }

    #[test]
    fn unfinalized_replay_with_workspace_is_safe_resume() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        unfinalized_replay("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::SafeResume);
        assert!(diagnosis.reason.contains("replay"), "{}", diagnosis.reason);
        assert!(
            diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::Resume)
        );
    }

    #[test]
    fn unfinalized_replay_without_workspace_or_artifacts_is_invalid() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        unfinalized_replay("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(
            diagnosis
                .reason
                .contains("unavailable before artifact finalization"),
            "{}",
            diagnosis.reason
        );
    }

    #[test]
    fn unfinalized_replay_without_workspace_uses_retained_artifacts() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        let artifacts = state.runs_dir().join("run-1").join("artifacts");
        std::fs::create_dir_all(&artifacts).unwrap();
        unfinalized_replay("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        registry.set_artifact_dir("run-1", &artifacts).unwrap();
        stale_owner(&state, "run-1");
        let diagnosis = diagnose(&state, "run-1").unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::SafeResume);
        assert!(
            diagnosis
                .allowed_next_steps
                .contains(&RecoveryNextStep::Resume)
        );
    }

    #[test]
    fn unfinalized_replay_workspace_mismatch_is_invalid() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        unfinalized_replay("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        stale_owner(&state, "run-1");
        let mut config = Config::default();
        config.workspace.adapter = "git-worktree".into();
        let diagnosis = diagnose_run(&state, "run-1", &config).unwrap();
        assert_eq!(diagnosis.class, RecoveryClass::InvalidCheckpoint);
        assert!(diagnosis.reason.contains("adapter"), "{}", diagnosis.reason);
    }

    #[test]
    fn diagnose_run_does_not_create_state_directories() {
        let directory = tempdir().unwrap();
        let state = StateRoot::new(directory.path().join("missing-state"));
        let error = diagnose(&state, "run-1").unwrap_err();
        assert!(error.to_string().contains("not found"), "{error}");
        assert!(!state.path().join("runs").exists());
        assert!(!state.path().join("run-controls").exists());
    }

    #[test]
    fn diagnose_run_does_not_mutate_checkpoint_bytes() {
        let (_directory, state, registry) = state();
        registry.start("run-1", "task", None, None).unwrap();
        let workspace = state.runs_dir().join("run-1").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        checkpoint("run-1", workspace)
            .save(&state.runs_dir())
            .unwrap();
        let path = crate::checkpoint::path_for(&state.runs_dir(), "run-1");
        let before = std::fs::read(&path).unwrap();
        diagnose(&state, "run-1").unwrap();
        let after = std::fs::read(&path).unwrap();
        assert_eq!(before, after);
    }
}
