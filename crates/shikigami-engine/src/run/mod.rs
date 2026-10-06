//! Run lifecycle engine.
//!
//! Public surface: [`Engine`], [`RunRequest`], [`RunResult`], and related types.
//! Internals:
//! - [`supervision`] — run admission, ownership, heartbeat, cancel/timeout bounds, and finalization
//! - [`preparation`] — workspace materialize, governed receipt recovery, admission hooks
//! - [`transaction`] — durable turn loop after preparation
//! - [`model_turn`] — durable model turns and context compaction
//! - [`tool_batch`] — durable tool batches and stable call identity
//! - [`session::RunSession`] — owned attempt state + deep checkpoint interface
//! - [`resume`] — checkpoint workspace boundary checks
//! - [`artifact_lifecycle`] — retained artifact finalization

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;

#[cfg(test)]
use crate::checkpoint::Checkpoint;
use crate::checkpoint::CheckpointError;
use crate::config::Config;
use crate::content::{
    ContentCapabilitiesV1, ContentMessageV1, ContentResolver, ContentRunRequestV1,
};
use crate::events::{EventSink, HarnessEvent};
use crate::governance::{GovernanceError, GovernancePort};
use crate::model::{CostEstimate, ModelError, ModelPort, TokenUsage};
use crate::registry::RunRegistry;
use crate::replay::{ReplayError, ReplayRequest, ReplayResult};
use crate::tools::{TodoItem, ToolError};
use crate::workspace::{WorkspaceError, WorkspacePort};

mod artifact_lifecycle;
mod model_turn;
mod nested;
mod preparation;
mod resume;
mod session;
mod supervision;
mod tool_batch;
mod transaction;

use supervision::RunSupervision;

pub use model_turn::compact_messages;
pub use resume::validate_resumed_workspace;

/// Default system prompt body (see [`crate::prompts`] for versioned id / digest).
pub const SYSTEM_PROMPT: &str = crate::prompts::HARNESS_V1.body;

/// How a run ended (success or not).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTermination {
    Completed,
    Cancelled,
    TimedOut,
    MaxTurns,
    Failed,
    /// Parked for an escalate answer, a plane approval, ask=park, or the next
    /// session prompt (`ParkKind::PromptWait`).
    Parked,
}

impl RunTermination {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::MaxTurns => "max_turns",
            Self::Failed => "failed",
            Self::Parked => "parked",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunRequest {
    pub task: String,
    pub keep_workspace: bool,
    pub timeout: Option<Duration>,
    pub cancel: Option<watch::Receiver<bool>>,
    /// When set, load checkpoint for this run id and continue.
    pub resume_run_id: Option<String>,
    /// Optional plane logical operation id (parent / host correlation).
    /// When unset, defaults to the harness `run_id` (attempt id).
    pub logical_operation_id: Option<String>,
    /// Operator answer when resuming a parked run (from `escalate`).
    pub resume_answer: Option<String>,
    /// Restore workspace from this snapshot name before continuing (e.g. `"initial"`).
    pub restore_snapshot: Option<String>,
    /// ACP/TUI session host: no-tool assistant waits; mutating tools ask=park.
    /// Unattended `run` leaves this false.
    pub session_wait: bool,
    /// Follow-up user prompt when resuming a `ParkKind::PromptWait` session run.
    pub resume_prompt: Option<String>,
    /// Allow or deny a `ParkKind::Ask` mutating tool on resume.
    pub resume_ask: Option<AskDecision>,
    /// Restrict mutating tools to the harness-owned plan path until accept.
    /// Fresh runs also pick this up from `run.plan_jail` settings.
    pub plan_jail: bool,
    /// Accept or reject a `ParkKind::Plan` park on resume.
    pub resume_plan: Option<PlanDecision>,
    /// Enable nested child-run tools for this attempt when settings allow.
    pub nested: bool,
    /// Depth of this run (0 = root). Children are parent + 1.
    pub nested_depth: u32,
    /// Parent run id when this request is a nested child.
    pub parent_run_id: Option<String>,
    /// Typed child profile; the model cannot invent one.
    pub nested_profile: Option<ChildProfile>,
    /// Pre-assigned run id (nested children persist identity before start).
    pub assigned_run_id: Option<String>,
}

/// Session-host decision for an ask=park mutating tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AskDecision {
    Allow,
    Deny,
}

/// Operator decision for a parked plan write-jail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanDecision {
    Accept,
    Reject,
}

/// Typed nested-child profile. The model cannot invent values outside this set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildProfile {
    Explore,
    Plan,
    Full,
}

impl ChildProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explore => "explore",
            Self::Plan => "plan",
            Self::Full => "full",
        }
    }
}

impl RunRequest {
    pub fn new(task: impl Into<String>) -> Self {
        Self {
            task: task.into(),
            keep_workspace: false,
            timeout: None,
            cancel: None,
            resume_run_id: None,
            logical_operation_id: None,
            resume_answer: None,
            restore_snapshot: None,
            session_wait: false,
            resume_prompt: None,
            resume_ask: None,
            plan_jail: false,
            resume_plan: None,
            nested: false,
            nested_depth: 0,
            parent_run_id: None,
            nested_profile: None,
            assigned_run_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunResult {
    pub run_id: String,
    pub success: bool,
    pub summary: String,
    pub turns: u32,
    pub workspace: PathBuf,
    /// Durable local artifact directory, retained after workspace cleanup.
    pub artifact_dir: Option<PathBuf>,
    pub termination: RunTermination,
    /// Set when `termination == Parked`.
    pub park: Option<ParkInfo>,
    /// Versioned prompt id used for this run (`name:sha256`).
    pub prompt_id: String,
    /// Aggregated token usage when reported by model turns (zeros if unknown).
    pub usage: TokenUsage,
    /// Cost estimate when both model cost rates are configured; otherwise `None`.
    pub cost: Option<CostEstimate>,
    /// Final run-scoped todo checklist (empty if never set).
    pub todos: Vec<TodoItem>,
}

pub(crate) struct ContentExecution {
    pub messages: Vec<ContentMessageV1>,
    pub capabilities: ContentCapabilitiesV1,
    pub resolver: Arc<dyn ContentResolver>,
}

pub use crate::checkpoint::ParkKind;

/// Operator-visible park payload (library + CLI).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ParkInfo {
    pub reason: String,
    pub question: String,
    pub tool_call_id: String,
    #[serde(default)]
    pub kind: ParkKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Host-visible tool identity (stable turn-qualified id) for session permission RPCs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_call_id: Option<String>,
    /// Tool arguments for session permission RPCs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_json: Option<String>,
    /// SHA-256 of `.shikigami/plan.md` when `kind` is `Plan`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plan_digest: String,
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    Governance(#[from] GovernanceError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Tool(#[from] ToolError),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("run: {0}")]
    Message(String),
    #[error("max turns exceeded ({0})")]
    MaxTurns(u32),
    #[error("run cancelled")]
    Cancelled,
    #[error("run timed out after {0:?}")]
    TimedOut(Duration),
}

impl RunError {
    pub fn termination(&self) -> RunTermination {
        match self {
            Self::Cancelled => RunTermination::Cancelled,
            Self::TimedOut(_) => RunTermination::TimedOut,
            Self::MaxTurns(_) => RunTermination::MaxTurns,
            _ => RunTermination::Failed,
        }
    }

    /// These failures leave a local checkpoint that is intentionally
    /// resumable. Keep the governed receipt open so the resumed run can
    /// continue its causal event sequence instead of writing a terminal
    /// outcome against an incomplete local attempt.
    fn leaves_governance_open(&self) -> bool {
        match self {
            Self::Cancelled | Self::TimedOut(_) | Self::MaxTurns(_) => true,
            Self::Governance(error) => !matches!(error, GovernanceError::Denied(_)),
            _ => false,
        }
    }
}

/// Low-level run engine.
///
/// Most hosts should use `shikigami::Harness`. The fields remain public for
/// compatibility with existing 1.x embedders; new construction should use
/// [`Engine::new`] so first-party wiring stays at one interface.
pub struct Engine {
    pub config: Config,
    pub governance: Arc<dyn GovernancePort>,
    pub workspace: Arc<dyn WorkspacePort>,
    pub model: Arc<dyn ModelPort>,
    pub events: Arc<dyn EventSink>,
    pub state_runs: PathBuf,
    pub registry: Arc<RunRegistry>,
}

impl Engine {
    /// Construct a low-level engine from resolved settings and adapters.
    pub fn new(
        config: Config,
        governance: Arc<dyn GovernancePort>,
        workspace: Arc<dyn WorkspacePort>,
        model: Arc<dyn ModelPort>,
        events: Arc<dyn EventSink>,
        state_runs: PathBuf,
        registry: Arc<RunRegistry>,
    ) -> Self {
        Self {
            config,
            governance,
            workspace,
            model,
            events,
            state_runs,
            registry,
        }
    }

    async fn report_governance_tool_with_id(
        &self,
        handle: &crate::governance::RunHandle,
        call_id: &str,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), RunError> {
        if let Err(error) = self
            .governance
            .report_tool_with_id(handle, call_id, name, ok, detail)
            .await
            && self.config.requires_governance()
        {
            return Err(error.into());
        }
        Ok(())
    }

    async fn report_governance_model(
        &self,
        handle: &crate::governance::RunHandle,
    ) -> Result<(), RunError> {
        if let Err(error) = self.governance.report_model_turn(handle, true).await
            && self.config.requires_governance()
        {
            return Err(error.into());
        }
        Ok(())
    }

    pub(super) fn emit(&self, run_id: &str, event: HarnessEvent) {
        self.registry.append_event(run_id, &event);
        self.events.emit(event);
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        self.run_with_checkpoint_digest(request, None).await
    }

    pub async fn run_content(&self, request: ContentRunRequestV1) -> Result<RunResult, RunError> {
        RunSupervision::new(self).execute_content(request).await
    }

    pub async fn run_with_checkpoint_digest(
        &self,
        request: RunRequest,
        expected_checkpoint_digest: Option<&str>,
    ) -> Result<RunResult, RunError> {
        RunSupervision::new(self)
            .execute(request, expected_checkpoint_digest)
            .await
    }

    pub async fn replay(&self, request: ReplayRequest) -> Result<ReplayResult, ReplayError> {
        let execution = request.admit()?;
        if let Some(run_id) = request.resume_run_id.as_deref()
            && let Some(mut recovered) = crate::replay::recover_terminal_replay(
                &self.state_runs,
                self.registry.as_ref(),
                run_id,
                &execution,
            )?
        {
            let registry_record = self.registry.load(run_id)?;
            if self.registry.run_is_active(run_id)? {
                return Err(ReplayError::Run(RunError::Message(format!(
                    "replay attempt {run_id} is still finalizing"
                ))));
            }
            if !recovered.finalized {
                let artifact_dir = if recovered.run.workspace.try_exists()? {
                    let checkpoint = crate::checkpoint::Checkpoint::load(&self.state_runs, run_id)?;
                    recovered.run.workspace = resume::validate_resumed_workspace(
                        &self.config,
                        &self.state_runs,
                        run_id,
                        &checkpoint,
                    )?;
                    let tools = crate::tools::ToolRegistry::from_config(
                        &recovered.run.workspace,
                        &self.config,
                    )
                    .map_err(RunError::from)?;
                    let artifact_dir = artifact_lifecycle::RunArtifactLifecycle::new(self)
                        .finalize(run_id, &recovered.run.workspace, &tools)
                        .await;
                    if !recovered.keep_workspace && recovered.run.success {
                        let cleanup = match recovered.workspace_adapter.as_str() {
                            "directory" => crate::workspace::WorkspaceCleanup::RemoveDir,
                            "git-worktree" => crate::workspace::git_worktree_cleanup(
                                &recovered.run.workspace,
                                run_id,
                                &self.config.workspace.branch_prefix,
                                std::path::Path::new(&self.config.workspace.root),
                            ),
                            _ => crate::workspace::WorkspaceCleanup::None,
                        };
                        let workspace = crate::workspace::MaterializedWorkspace {
                            path: recovered.run.workspace.clone(),
                            adapter: recovered.workspace_adapter,
                            cleanup,
                        };
                        let _ = crate::workspace::apply_cleanup(&workspace);
                    }
                    artifact_dir
                } else {
                    Some(
                        registry_record
                            .artifact_dir
                            .clone()
                            .filter(|path| std::path::Path::new(path).is_dir())
                            .map(std::path::PathBuf::from)
                            .ok_or_else(|| {
                                ReplayError::Run(RunError::Message(format!(
                                    "replay workspace is unavailable before artifact finalization: {}",
                                    recovered.run.workspace.display()
                                )))
                            })?,
                    )
                };
                recovered.run.artifact_dir = artifact_dir.clone();
                self.emit(
                    run_id,
                    HarnessEvent::RunFinished {
                        run_id: run_id.into(),
                        success: recovered.run.success,
                        summary: recovered.run.summary.clone(),
                    },
                );
                crate::replay::mark_replay_finalized(
                    &self.state_runs,
                    run_id,
                    artifact_dir.as_deref(),
                )?;
                self.registry.finish_result(&recovered.run)?;
            } else if matches!(registry_record.status.as_str(), "starting" | "running") {
                self.registry.finish_result(&recovered.run)?;
            }
            return crate::replay::complete_replay(&self.state_runs, recovered.run, execution);
        }
        let mut run_request = RunRequest::new(request.evidence.task.clone());
        run_request.keep_workspace = request.keep_workspace;
        run_request.cancel = request.cancel;
        run_request.resume_run_id = request.resume_run_id;
        run_request.logical_operation_id = execution.logical_operation_id.clone();
        let run = RunSupervision::new(self)
            .execute_replay(run_request, execution.clone())
            .await?;
        crate::replay::complete_replay(&self.state_runs, run, execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint;
    use crate::config::Config;
    use crate::events;
    use crate::governance;
    use crate::model::{ModelTurn, ScriptedModel, ToolCall};
    use crate::state::StateRoot;
    use crate::tools;
    use crate::workspace;
    use std::process::Command;
    use tempfile::tempdir;

    #[test]
    fn resumed_workspace_must_match_configured_run_boundary() {
        let dir = tempdir().unwrap();
        let state_runs = dir.path().join("state").join("runs");
        let run_id = "abc-123";
        let expected = state_runs.join(run_id).join("workspace");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&expected).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let mut config = Config::default();
        config.workspace.adapter = "directory".into();
        config.workspace.root = ".".into();
        let mut checkpoint = Checkpoint {
            version: checkpoint::CHECKPOINT_VERSION,
            run_id: run_id.into(),
            task: "t".into(),
            prompt_id: checkpoint::prompt_id(SYSTEM_PROMPT),
            messages: vec![],
            completed_turns: 0,
            workspace: outside,
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: None,
            todos: vec![],
            governance: None,
            replay: None,
            content: None,
            prompt_start_turns: None,
            plan_jail: false,
            nested: false,
            children: vec![],
            nested_depth: 0,
            parent_run_id: String::new(),
            nested_profile: String::new(),
            tools_mode: String::new(),
            tools_enabled: Vec::new(),
        };

        let err =
            validate_resumed_workspace(&config, &state_runs, run_id, &checkpoint).unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");

        checkpoint.workspace = expected.canonicalize().unwrap();
        let validated =
            validate_resumed_workspace(&config, &state_runs, run_id, &checkpoint).unwrap();
        assert_eq!(validated, checkpoint.workspace);
    }

    #[cfg(unix)]
    #[test]
    fn resumed_workspace_rejects_symlink_substitution() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let state_runs = dir.path().join("state").join("runs");
        let run_id = "abc-123";
        let run_dir = state_runs.join(run_id);
        let expected = run_dir.join("workspace");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, &expected).unwrap();
        let mut config = Config::default();
        config.workspace.adapter = "directory".into();
        config.workspace.root = ".".into();
        let checkpoint = Checkpoint {
            version: checkpoint::CHECKPOINT_VERSION,
            run_id: run_id.into(),
            task: "t".into(),
            prompt_id: checkpoint::prompt_id(SYSTEM_PROMPT),
            messages: vec![],
            completed_turns: 0,
            workspace: expected,
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: None,
            todos: vec![],
            governance: None,
            replay: None,
            content: None,
            prompt_start_turns: None,
            plan_jail: false,
            nested: false,
            children: vec![],
            nested_depth: 0,
            parent_run_id: String::new(),
            nested_profile: String::new(),
            tools_mode: String::new(),
            tools_enabled: Vec::new(),
        };

        let err =
            validate_resumed_workspace(&config, &state_runs, run_id, &checkpoint).unwrap_err();
        assert!(
            err.to_string().contains("must not contain symlinks"),
            "{err}"
        );
    }

    fn base_config(dir: &tempfile::TempDir) -> Config {
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        config.model.adapter = "scripted".into();
        config
    }

    #[tokio::test]
    async fn cancel_before_first_turn_errors() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"x\",\"success\":true}"}]}]"#
                .into(),
        );
        state.ensure_ready_for_runs().unwrap();
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(crate::model::from_config(&config).unwrap()),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let (tx, rx) = watch::channel(true);
        let _keep = tx;
        let err = eng
            .run(RunRequest {
                task: "t".into(),
                keep_workspace: true,
                timeout: None,
                cancel: Some(rx),
                resume_run_id: None,
                logical_operation_id: None,
                resume_answer: None,
                restore_snapshot: None,
                session_wait: false,
                resume_prompt: None,
                resume_ask: None,
                plan_jail: false,
                resume_plan: None,
                nested: false,
                nested_depth: 0,
                parent_run_id: None,
                nested_profile: None,
                assigned_run_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RunError::Cancelled));
    }

    #[tokio::test]
    async fn timeout_zero_errors_at_boundary() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let config = base_config(&dir);
        state.ensure_ready_for_runs().unwrap();
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(crate::model::from_config(&config).unwrap()),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let err = eng
            .run(RunRequest {
                task: "t".into(),
                keep_workspace: true,
                timeout: Some(Duration::from_secs(0)),
                cancel: None,
                resume_run_id: None,
                logical_operation_id: None,
                resume_answer: None,
                restore_snapshot: None,
                session_wait: false,
                resume_prompt: None,
                resume_ask: None,
                plan_jail: false,
                resume_plan: None,
                nested: false,
                nested_depth: 0,
                parent_run_id: None,
                nested_profile: None,
                assigned_run_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RunError::TimedOut(_)));
    }

    #[tokio::test]
    async fn resume_after_partial_script() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();

        // First run: only write a file (no report) with max_turns 1 → MaxTurns but checkpoint saved
        let mut config = base_config(&dir);
        config.run.max_turns = 1;
        let model = ScriptedModel::from_turns(vec![ModelTurn {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "1".into(),
                name: "write_file".into(),
                args_json: r#"{"path":"partial.txt","content":"hello"}"#.into(),
            }],
            usage: None,
        }]);
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::new(model),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config: config.clone(),
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let err = eng
            .run(RunRequest {
                task: "partial".into(),
                keep_workspace: true,
                timeout: None,
                cancel: None,
                resume_run_id: None,
                logical_operation_id: None,
                resume_answer: None,
                restore_snapshot: None,
                session_wait: false,
                resume_prompt: None,
                resume_ask: None,
                plan_jail: false,
                resume_plan: None,
                nested: false,
                nested_depth: 0,
                parent_run_id: None,
                nested_profile: None,
                assigned_run_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RunError::MaxTurns(1)));

        // Find checkpoint under runs/
        let runs = state.runs_dir();
        let run_id = std::fs::read_dir(&runs)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().join("checkpoint.json").is_file())
            .unwrap()
            .file_name()
            .to_string_lossy()
            .into_owned();

        // Resume with the original write plus the next report. Cursor restore
        // skips the already completed write turn.
        let mut config2 = base_config(&dir);
        config2.run.max_turns = 10;
        let model2 = ScriptedModel::from_turns(vec![
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "1".into(),
                    name: "write_file".into(),
                    args_json: r#"{"path":"partial.txt","content":"hello"}"#.into(),
                }],
                usage: None,
            },
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "2".into(),
                    name: "report".into(),
                    args_json: r#"{"summary":"resumed ok","success":true}"#.into(),
                }],
                usage: None,
            },
        ]);
        let eng2 = Engine {
            governance: Arc::from(governance::from_config(&config2).unwrap()),
            workspace: Arc::from(workspace::from_config(&config2).unwrap()),
            model: Arc::new(model2),
            events: Arc::from(events::from_config(&config2, &state.runs_dir()).unwrap()),
            config: config2,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let result = eng2
            .run(RunRequest {
                task: String::new(),
                keep_workspace: true,
                timeout: None,
                cancel: None,
                resume_run_id: Some(run_id.clone()),
                logical_operation_id: None,
                resume_answer: None,
                restore_snapshot: None,
                session_wait: false,
                resume_prompt: None,
                resume_ask: None,
                plan_jail: false,
                resume_plan: None,
                nested: false,
                nested_depth: 0,
                parent_run_id: None,
                nested_profile: None,
                assigned_run_id: None,
            })
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.run_id, run_id);
        assert!(result.workspace.join("partial.txt").is_file());
        assert_eq!(result.summary, "resumed ok");
    }

    #[tokio::test]
    async fn parallel_safe_read_tools_in_one_turn() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        let ws = dir.path().join("ws-root");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("a.txt"), "aaa").unwrap();
        std::fs::write(ws.join("b.txt"), "bbb").unwrap();

        let mut config = base_config(&dir);
        config.run.tool_concurrency = 4;
        config.workspace.root = ws.to_string_lossy().into();
        let model = ScriptedModel::from_turns(vec![
            ModelTurn {
                content: String::new(),
                tool_calls: vec![
                    ToolCall {
                        id: "1".into(),
                        name: "read_file".into(),
                        args_json: r#"{"path":"a.txt"}"#.into(),
                    },
                    ToolCall {
                        id: "2".into(),
                        name: "read_file".into(),
                        args_json: r#"{"path":"b.txt"}"#.into(),
                    },
                ],
                usage: None,
            },
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "3".into(),
                    name: "report".into(),
                    args_json: r#"{"summary":"parallel ok","success":true}"#.into(),
                }],
                usage: None,
            },
        ]);
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::new(model),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let mut req = RunRequest::new("read both");
        req.keep_workspace = true;
        let result = eng.run(req).await.unwrap();
        assert!(result.success);
        assert_eq!(result.summary, "parallel ok");
        assert!(tools::is_parallel_safe_tool("read_file"));
        assert!(!tools::is_parallel_safe_tool("write_file"));
        assert!(!tools::is_parallel_safe_tool("report"));
    }

    #[tokio::test]
    async fn todo_write_survives_checkpoint_resume() {
        use crate::tools::TodoStatus;

        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();

        let mut config = base_config(&dir);
        config.run.max_turns = 1;
        let model = ScriptedModel::from_turns(vec![ModelTurn {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "t1".into(),
                name: "todo_write".into(),
                args_json:
                    r#"{"items":[{"id":"a","content":"ship feature","status":"in_progress"}]}"#
                        .into(),
            }],
            usage: None,
        }]);
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::new(model),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config: config.clone(),
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let err = eng
            .run(RunRequest {
                task: "with todos".into(),
                keep_workspace: true,
                timeout: None,
                cancel: None,
                resume_run_id: None,
                logical_operation_id: None,
                resume_answer: None,
                restore_snapshot: None,
                session_wait: false,
                resume_prompt: None,
                resume_ask: None,
                plan_jail: false,
                resume_plan: None,
                nested: false,
                nested_depth: 0,
                parent_run_id: None,
                nested_profile: None,
                assigned_run_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RunError::MaxTurns(1)));

        let runs = state.runs_dir();
        let run_id = std::fs::read_dir(&runs)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().join("checkpoint.json").is_file())
            .unwrap()
            .file_name()
            .to_string_lossy()
            .into_owned();
        let cp = Checkpoint::load(&runs, &run_id).unwrap();
        assert_eq!(cp.todos.len(), 1);
        assert_eq!(cp.todos[0].id, "a");
        assert_eq!(cp.todos[0].status, TodoStatus::InProgress);

        let mut config2 = base_config(&dir);
        config2.run.max_turns = 5;
        let model2 = ScriptedModel::from_turns(vec![
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "t1".into(),
                    name: "todo_write".into(),
                    args_json:
                        r#"{"items":[{"id":"a","content":"ship feature","status":"in_progress"}]}"#
                            .into(),
                }],
                usage: None,
            },
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "t2".into(),
                    name: "todo_write".into(),
                    args_json:
                        r#"{"items":[{"id":"a","content":"ship feature","status":"completed"}]}"#
                            .into(),
                }],
                usage: None,
            },
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "t3".into(),
                    name: "report".into(),
                    args_json: r#"{"summary":"todos done","success":true}"#.into(),
                }],
                usage: None,
            },
        ]);
        let eng2 = Engine {
            governance: Arc::from(governance::from_config(&config2).unwrap()),
            workspace: Arc::from(workspace::from_config(&config2).unwrap()),
            model: Arc::new(model2),
            events: Arc::from(events::from_config(&config2, &state.runs_dir()).unwrap()),
            config: config2,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let result = eng2
            .run(RunRequest {
                task: String::new(),
                keep_workspace: true,
                timeout: None,
                cancel: None,
                resume_run_id: Some(run_id),
                logical_operation_id: None,
                resume_answer: None,
                restore_snapshot: None,
                session_wait: false,
                resume_prompt: None,
                resume_ask: None,
                plan_jail: false,
                resume_plan: None,
                nested: false,
                nested_depth: 0,
                parent_run_id: None,
                nested_profile: None,
                assigned_run_id: None,
            })
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.todos.len(), 1);
        assert_eq!(result.todos[0].status, TodoStatus::Completed);
    }

    #[tokio::test]
    async fn logical_operation_id_override_on_handle() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"ok\",\"success\":true}"}]}]"#
                .into(),
        );
        state.ensure_ready_for_runs().unwrap();
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(crate::model::from_config(&config).unwrap()),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let mut req = RunRequest::new("with parent op");
        req.keep_workspace = true;
        req.logical_operation_id = Some("parent-op-42".into());
        let result = eng.run(req).await.unwrap();
        assert!(result.success);
        // run_id remains a distinct attempt UUID
        assert_ne!(result.run_id, "parent-op-42");
        assert!(!result.run_id.is_empty());
    }

    #[tokio::test]
    async fn escalate_parks_and_resume_with_answer() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();

        let config = base_config(&dir);
        let model = ScriptedModel::from_turns(vec![ModelTurn {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "esc-1".into(),
                name: "escalate".into(),
                args_json: r#"{"reason":"need human","question":"approve?"}"#.into(),
            }],
            usage: None,
        }]);
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::new(model),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config: config.clone(),
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let mut req = RunRequest::new("needs approval");
        req.keep_workspace = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert!(!parked.success);
        assert!(parked.park.is_some());
        assert_eq!(parked.park.as_ref().unwrap().question, "approve?");

        // Resume without answer must fail loudly (no silent deny/success).
        let eng2 = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(crate::model::from_config(&config).unwrap()),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config: config.clone(),
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let err = eng2
            .run(RunRequest {
                task: String::new(),
                keep_workspace: true,
                timeout: None,
                cancel: None,
                resume_run_id: Some(parked.run_id.clone()),
                logical_operation_id: None,
                resume_answer: None,
                restore_snapshot: None,
                session_wait: false,
                resume_prompt: None,
                resume_ask: None,
                plan_jail: false,
                resume_plan: None,
                nested: false,
                nested_depth: 0,
                parent_run_id: None,
                nested_profile: None,
                assigned_run_id: None,
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("parked"), "{err}");

        // Resume with answer continues and can report success. Include the
        // original escalate turn so cursor restore skips it.
        let model3 = ScriptedModel::from_turns(vec![
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "esc-1".into(),
                    name: "escalate".into(),
                    args_json: r#"{"reason":"need human","question":"approve?"}"#.into(),
                }],
                usage: None,
            },
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "r1".into(),
                    name: "report".into(),
                    args_json: r#"{"summary":"approved and done","success":true}"#.into(),
                }],
                usage: None,
            },
        ]);
        let eng3 = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::new(model3),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(parked.run_id.clone());
        resume.resume_answer = Some("yes, proceed".into());
        let done = eng3.run(resume).await.unwrap();
        assert!(done.success);
        assert_eq!(done.termination, RunTermination::Completed);
        assert_eq!(done.summary, "approved and done");
        assert!(done.park.is_none());
    }

    fn engine(dir: &tempfile::TempDir, config: Config) -> Engine {
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(crate::model::from_config(&config).unwrap()),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        }
    }

    /// Parent model from `parent_script`; child spawn uses `config.model.script_json`.
    fn engine_nested(dir: &tempfile::TempDir, mut config: Config, parent_script: &str) -> Engine {
        config.run.nested = true;
        let child_script = config.model.script_json.clone();
        config.model.script_json = Some(parent_script.into());
        let parent_model = crate::model::from_config(&config).unwrap();
        config.model.script_json = child_script;
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(parent_model),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        }
    }

    fn child_run_then_report(profile: &str, task: &str, parent_summary: &str) -> String {
        serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": profile,
                        "task": task
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": parent_summary,
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string()
    }

    fn write_then_report(path: &str, content: &str, summary: &str) -> String {
        serde_json::json!([
            {
                "tool_calls": [{
                    "name": "write_file",
                    "args_json": serde_json::json!({
                        "path": path,
                        "content": content
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": summary,
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string()
    }

    fn nested_child_tool_payload(checkpoint: &Checkpoint) -> serde_json::Value {
        checkpoint
            .messages
            .iter()
            .find_map(|message| {
                if message.role != "tool" {
                    return None;
                }
                serde_json::from_str::<serde_json::Value>(&message.content).ok()
            })
            .expect("parent child_run tool payload")
    }

    fn run_record_generation(path: &std::path::Path) -> Option<u64> {
        let meta = std::fs::metadata(path).ok()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Some(meta.ino())
        }
        #[cfg(not(unix))]
        {
            Some(
                meta.modified()
                    .ok()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()?
                    .as_nanos() as u64,
            )
        }
    }

    fn list_run_record_ids(runs_root: &std::path::Path) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(runs_root) else {
            return Vec::new();
        };
        entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .join(crate::registry::RUN_RECORD_FILENAME)
                    .is_file()
            })
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect()
    }

    /// Count atomic `run.json` replaces per already-active run id.
    async fn count_run_record_writes(
        runs_root: &std::path::Path,
        run_ids: &[String],
        window: Duration,
    ) -> std::collections::HashMap<String, usize> {
        let mut last: std::collections::HashMap<String, Option<u64>> = run_ids
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    run_record_generation(
                        &runs_root
                            .join(id)
                            .join(crate::registry::RUN_RECORD_FILENAME),
                    ),
                )
            })
            .collect();
        let mut writes: std::collections::HashMap<String, usize> =
            run_ids.iter().map(|id| (id.clone(), 0)).collect();
        let end = tokio::time::Instant::now() + window;
        while tokio::time::Instant::now() < end {
            tokio::time::sleep(Duration::from_millis(5)).await;
            for id in run_ids {
                let generation = run_record_generation(
                    &runs_root
                        .join(id)
                        .join(crate::registry::RUN_RECORD_FILENAME),
                );
                if last.get(id) != Some(&generation) {
                    *writes.get_mut(id).expect("tracked run id") += 1;
                    last.insert(id.clone(), generation);
                }
            }
        }
        writes
    }

    async fn wait_for_run_records(runs_root: &std::path::Path, min: usize) -> Vec<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let ids = list_run_record_ids(runs_root);
            if ids.len() >= min {
                return ids;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {min} run records"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn session_wait_parks_prompt_wait_on_no_tool() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(r#"[{"content":"hello"}]"#.into());
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("hi");
        req.keep_workspace = true;
        req.session_wait = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::PromptWait);
    }

    #[tokio::test]
    async fn session_wait_report_parks_prompt_wait() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}]"#
                .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("go");
        req.keep_workspace = true;
        req.session_wait = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        assert_eq!(parked.summary, "done");
    }

    #[tokio::test]
    async fn session_wait_parks_ask_on_mutating_tool() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]}]"#
                .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        req.session_wait = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Ask);
        assert!(!parked.workspace.join("ok.txt").exists());
    }

    #[tokio::test]
    async fn session_wait_persists_allowed_prefix_before_next_ask() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[
              {"tool_calls":[
                {"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"},
                {"name":"write_file","args_json":"{\"path\":\"b.txt\",\"content\":\"b\\n\"}"}
              ]},
              {"content":"done"}
            ]"#
            .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        req.session_wait = true;
        let first = eng.run(req).await.unwrap();
        assert_eq!(first.park.as_ref().unwrap().kind, ParkKind::Ask);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.session_wait = true;
        resume.resume_run_id = Some(first.run_id.clone());
        resume.resume_ask = Some(AskDecision::Allow);
        let second = eng.run(resume).await.unwrap();
        assert_eq!(second.park.as_ref().unwrap().kind, ParkKind::Ask);
        assert_eq!(
            std::fs::read_to_string(second.workspace.join("a.txt")).unwrap(),
            "a\n"
        );
        assert!(!second.workspace.join("b.txt").exists());
        assert_ne!(
            second.park.as_ref().unwrap().tool_call_id,
            first.park.as_ref().unwrap().tool_call_id
        );

        let mut resume2 = RunRequest::new("");
        resume2.keep_workspace = true;
        resume2.session_wait = true;
        resume2.resume_run_id = Some(second.run_id.clone());
        resume2.resume_ask = Some(AskDecision::Allow);
        let third = eng.run(resume2).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(third.workspace.join("a.txt")).unwrap(),
            "a\n"
        );
        assert_eq!(
            std::fs::read_to_string(third.workspace.join("b.txt")).unwrap(),
            "b\n"
        );
    }

    #[tokio::test]
    async fn session_wait_deny_still_asks_later_mutating_tools() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"}]},
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"b.txt\",\"content\":\"b\\n\"}"}]}
            ]"#
                .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        req.session_wait = true;
        let first = eng.run(req).await.unwrap();
        assert_eq!(first.park.as_ref().unwrap().kind, ParkKind::Ask);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.session_wait = true;
        resume.resume_run_id = Some(first.run_id.clone());
        resume.resume_ask = Some(AskDecision::Deny);
        let second = eng.run(resume).await.unwrap();
        assert_eq!(second.park.as_ref().unwrap().kind, ParkKind::Ask);
        assert!(!second.workspace.join("a.txt").exists());
        assert!(!second.workspace.join("b.txt").exists());
    }

    #[tokio::test]
    async fn session_wait_allow_does_not_reuse_across_turns() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[
              {"tool_calls":[{"id":"same","name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"}]},
              {"tool_calls":[{"id":"same","name":"write_file","args_json":"{\"path\":\"b.txt\",\"content\":\"b\\n\"}"}]}
            ]"#
                .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        req.session_wait = true;
        let first = eng.run(req).await.unwrap();
        assert_eq!(first.park.as_ref().unwrap().kind, ParkKind::Ask);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.session_wait = true;
        resume.resume_run_id = Some(first.run_id.clone());
        resume.resume_ask = Some(AskDecision::Allow);
        let second = eng.run(resume).await.unwrap();
        assert_eq!(second.park.as_ref().unwrap().kind, ParkKind::Ask);
        assert!(second.workspace.join("a.txt").exists());
        assert!(!second.workspace.join("b.txt").exists());
    }

    #[tokio::test]
    async fn session_wait_follow_ups_do_not_share_unattended_max_turns() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.max_turns = 2;
        config.model.script_json =
            Some(r#"[{"content":"a"},{"content":"b"},{"content":"c"}]"#.into());
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("go");
        req.keep_workspace = true;
        req.session_wait = true;
        let first = eng.run(req).await.unwrap();
        assert_eq!(first.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        assert_eq!(first.turns, 1);

        let mut second_req = RunRequest::new("");
        second_req.keep_workspace = true;
        second_req.session_wait = true;
        second_req.resume_run_id = Some(first.run_id.clone());
        second_req.resume_prompt = Some("more".into());
        let second = eng.run(second_req).await.unwrap();
        assert_eq!(second.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        assert_eq!(second.turns, 2);

        let mut third_req = RunRequest::new("");
        third_req.keep_workspace = true;
        third_req.session_wait = true;
        third_req.resume_run_id = Some(second.run_id.clone());
        third_req.resume_prompt = Some("again".into());
        let third = eng.run(third_req).await.unwrap();
        assert_eq!(third.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        assert_eq!(third.turns, 3);
        assert_eq!(third.summary, "c");
    }

    #[tokio::test]
    async fn session_wait_single_prompt_still_hits_max_turns() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.max_turns = 2;
        config.model.script_json = Some(
            r#"[
              {"tool_calls":[{"name":"read_file","args_json":"{\"path\":\"missing.txt\"}"}]},
              {"tool_calls":[{"name":"read_file","args_json":"{\"path\":\"missing.txt\"}"}]},
              {"content":"done"}
            ]"#
            .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("look");
        req.keep_workspace = true;
        req.session_wait = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        assert_eq!(parked.park.as_ref().unwrap().reason, "max_turns");
    }

    #[tokio::test]
    async fn session_wait_ask_resume_shares_prompt_max_turns() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.max_turns = 2;
        config.model.script_json = Some(
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"}]},
              {"tool_calls":[{"name":"read_file","args_json":"{\"path\":\"a.txt\"}"}]},
              {"content":"done"}
            ]"#
            .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        req.session_wait = true;
        let first = eng.run(req).await.unwrap();
        assert_eq!(first.park.as_ref().unwrap().kind, ParkKind::Ask);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.session_wait = true;
        resume.resume_run_id = Some(first.run_id.clone());
        resume.resume_ask = Some(AskDecision::Allow);
        let second = eng.run(resume).await.unwrap();
        assert_eq!(second.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        assert_eq!(second.park.as_ref().unwrap().reason, "max_turns");
        assert_ne!(second.summary, "done");
        assert_eq!(
            std::fs::read_to_string(second.workspace.join("a.txt")).unwrap(),
            "a\n"
        );
    }

    #[tokio::test]
    async fn session_wait_allow_keeps_ask_park_until_the_tool_runs() {
        let dir = tempdir().unwrap();
        let seen = dir.path().join("seen.json");
        let hook = dir.path().join("pre_tool.sh");
        let checkpoint = dir.path().join("state").join("runs");
        std::fs::write(
            &hook,
            format!(
                "#!/bin/sh\ncp \"{}/\"*/checkpoint.json \"{}\"\nexit 0\n",
                checkpoint.display(),
                seen.display()
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&hook).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&hook, perms).unwrap();
        }
        let mut config = base_config(&dir);
        config.hooks.push(crate::config::HookSettings {
            event: "pre_tool".into(),
            command: hook.to_string_lossy().into(),
            args: vec![],
            timeout_ms: 2_000,
            fail_closed: true,
        });
        config.model.script_json = Some(
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"}]},
              {"content":"done"}
            ]"#
            .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        req.session_wait = true;
        let first = eng.run(req).await.unwrap();
        assert_eq!(first.park.as_ref().unwrap().kind, ParkKind::Ask);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.session_wait = true;
        resume.resume_run_id = Some(first.run_id.clone());
        resume.resume_ask = Some(AskDecision::Allow);
        let second = eng.run(resume).await.unwrap();
        assert_eq!(second.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        let snapshot: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&seen).unwrap()).unwrap();
        assert_eq!(snapshot["park"]["kind"], "ask");
    }

    #[tokio::test]
    async fn session_wait_allow_is_consumed_when_hooks_reject() {
        let dir = tempdir().unwrap();
        let hook = dir.path().join("pre_tool.sh");
        std::fs::write(&hook, "#!/bin/sh\ngrep -q blocked.txt && exit 1\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&hook).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&hook, perms).unwrap();
        }
        let mut config = base_config(&dir);
        config.hooks.push(crate::config::HookSettings {
            event: "pre_tool".into(),
            command: hook.to_string_lossy().into(),
            args: vec![],
            timeout_ms: 2_000,
            fail_closed: true,
        });
        config.model.script_json = Some(
            r#"[
              {"tool_calls":[{"id":"same","name":"write_file","args_json":"{\"path\":\"blocked.txt\",\"content\":\"x\\n\"}"}]},
              {"tool_calls":[{"id":"same","name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"y\\n\"}"}]}
            ]"#
            .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        req.session_wait = true;
        let first = eng.run(req).await.unwrap();
        assert_eq!(first.park.as_ref().unwrap().kind, ParkKind::Ask);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.session_wait = true;
        resume.resume_run_id = Some(first.run_id.clone());
        resume.resume_ask = Some(AskDecision::Allow);
        let second = eng.run(resume).await.unwrap();
        assert_eq!(second.park.as_ref().unwrap().kind, ParkKind::Ask);
        assert!(!second.workspace.join("blocked.txt").exists());
        assert!(!second.workspace.join("ok.txt").exists());
    }

    fn plan_jail_script() -> String {
        let write = |path: &str, content: &str| {
            serde_json::json!({
                "tool_calls": [{
                    "name": "write_file",
                    "args_json": serde_json::json!({"path": path, "content": content}).to_string()
                }]
            })
        };
        let report = |summary: &str| {
            serde_json::json!({
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({"summary": summary, "success": true}).to_string()
                }]
            })
        };
        serde_json::json!([
            write("ok.txt", "no\n"),
            write(crate::tools::PLAN_JAIL_PATH, "# do it\n"),
            report("planned"),
            write("ok.txt", "yes\n"),
            report("executed"),
        ])
        .to_string()
    }

    #[tokio::test]
    async fn plan_jail_denies_writes_outside_the_plan_path_and_parks_on_report() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(plan_jail_script());
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        let park = parked.park.as_ref().unwrap();
        assert_eq!(park.kind, ParkKind::Plan);
        assert!(!park.plan_digest.is_empty());
        assert!(!parked.workspace.join("ok.txt").exists());
        assert_eq!(
            std::fs::read_to_string(parked.workspace.join(crate::tools::PLAN_JAIL_PATH)).unwrap(),
            "# do it\n"
        );

        let mut missing = RunRequest::new("");
        missing.keep_workspace = true;
        missing.resume_run_id = Some(parked.run_id.clone());
        let err = eng.run(missing).await.unwrap_err();
        assert!(err.to_string().contains("resume_plan"), "{err}");
        assert!(!parked.workspace.join("ok.txt").exists());
    }

    #[tokio::test]
    async fn plan_jail_reject_completes_failed_without_execute() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(plan_jail_script());
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Plan);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(parked.run_id.clone());
        resume.resume_plan = Some(PlanDecision::Reject);
        let rejected = eng.run(resume).await.unwrap();
        assert_eq!(rejected.termination, RunTermination::Failed);
        assert!(!rejected.success);
        assert_eq!(rejected.summary, "plan rejected");
        assert!(!parked.workspace.join("ok.txt").exists());
        let checkpoint = Checkpoint::load(&eng.state_runs, &parked.run_id).unwrap();
        assert!(checkpoint.plan_jail);
        assert!(checkpoint.park.is_none());
    }

    #[tokio::test]
    async fn plan_jail_reject_then_resume_stays_jailed() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(plan_jail_script());
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Plan);

        let mut reject = RunRequest::new("");
        reject.keep_workspace = true;
        reject.resume_run_id = Some(parked.run_id.clone());
        reject.resume_plan = Some(PlanDecision::Reject);
        let rejected = eng.run(reject).await.unwrap();
        assert_eq!(rejected.termination, RunTermination::Failed);
        assert_eq!(rejected.summary, "plan rejected");

        let mut again = RunRequest::new("");
        again.keep_workspace = true;
        again.resume_run_id = Some(parked.run_id.clone());
        let continued = eng.run(again).await.unwrap();
        assert!(!parked.workspace.join("ok.txt").exists());
        assert_eq!(
            continued.park.as_ref().map(|park| park.kind),
            Some(ParkKind::Plan)
        );
    }

    #[tokio::test]
    async fn plan_jail_accept_allows_workspace_writes() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(plan_jail_script());
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Plan);

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(parked.run_id.clone());
        resume.resume_plan = Some(PlanDecision::Accept);
        let done = eng.run(resume).await.unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        assert!(done.success);
        assert_eq!(
            std::fs::read_to_string(parked.workspace.join("ok.txt")).unwrap(),
            "yes\n"
        );
    }

    #[tokio::test]
    async fn plan_jail_rejects_plan_decision_on_a_fresh_run() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"no\\n\"}"}]},{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}]"#
                .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        req.resume_plan = Some(PlanDecision::Accept);
        let err = eng.run(req).await.unwrap_err();
        assert!(err.to_string().contains("resume_plan"), "{err}");
    }

    #[tokio::test]
    async fn plan_jail_from_settings_applies_to_fresh_runs() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.plan_jail = true;
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"no\\n\"}"}]},{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"blocked\",\"success\":true}"}]}]"#
                .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Plan);
        assert!(!parked.workspace.join("ok.txt").exists());
    }

    #[tokio::test]
    async fn plan_jail_off_leaves_existing_runs_unconstrained() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}]"#
                .into(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("write");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        assert_eq!(
            std::fs::read_to_string(done.workspace.join("ok.txt")).unwrap(),
            "hi\n"
        );
    }

    #[tokio::test]
    async fn plan_jail_report_takes_precedence_over_session_wait() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(plan_jail_script());
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        req.session_wait = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Plan);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn plan_jail_denies_symlink_plan_path() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("secret.txt"), "keep\n").unwrap();
        std::fs::create_dir_all(project.join(".shikigami")).unwrap();
        std::os::unix::fs::symlink(
            project.join("secret.txt"),
            project.join(crate::tools::PLAN_JAIL_PATH),
        )
        .unwrap();
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        let write_plan = serde_json::json!({
            "path": crate::tools::PLAN_JAIL_PATH,
            "content": "pwned\n"
        })
        .to_string();
        let report = serde_json::json!({"summary": "planned", "success": true}).to_string();
        config.model.script_json = Some(
            serde_json::json!([
                {"tool_calls":[{"name":"write_file","args_json": write_plan}]},
                {"tool_calls":[{"name":"report","args_json": report}]}
            ])
            .to_string(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Plan);
        assert_eq!(
            std::fs::read_to_string(project.join("secret.txt")).unwrap(),
            "keep\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn plan_jail_denies_hard_linked_plan_path() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("secret.txt"), "keep\n").unwrap();
        std::fs::create_dir_all(project.join(".shikigami")).unwrap();
        std::fs::hard_link(
            project.join("secret.txt"),
            project.join(crate::tools::PLAN_JAIL_PATH),
        )
        .unwrap();
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        let write_plan = serde_json::json!({
            "path": crate::tools::PLAN_JAIL_PATH,
            "content": "pwned\n"
        })
        .to_string();
        let report = serde_json::json!({"summary": "planned", "success": true}).to_string();
        config.model.script_json = Some(
            serde_json::json!([
                {"tool_calls":[{"name":"write_file","args_json": write_plan}]},
                {"tool_calls":[{"name":"report","args_json": report}]}
            ])
            .to_string(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Plan);
        assert_eq!(
            std::fs::read_to_string(project.join("secret.txt")).unwrap(),
            "keep\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn plan_jail_report_does_not_follow_a_plan_symlink() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("secret.txt"), "host-secret\n").unwrap();
        std::fs::create_dir_all(project.join(".shikigami")).unwrap();
        std::os::unix::fs::symlink(
            project.join("secret.txt"),
            project.join(crate::tools::PLAN_JAIL_PATH),
        )
        .unwrap();
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        let report = serde_json::json!({"summary": "planned", "success": true}).to_string();
        config.model.script_json = Some(
            serde_json::json!([{"tool_calls":[{"name":"report","args_json": report}]}]).to_string(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("plan");
        req.keep_workspace = true;
        req.plan_jail = true;
        let parked = eng.run(req).await.unwrap();
        let park = parked.park.as_ref().unwrap();
        assert_eq!(park.kind, ParkKind::Plan);
        assert!(park.plan_digest.is_empty());
        assert!(
            !park.question.contains("host-secret"),
            "plan review leaked symlink target: {}",
            park.question
        );
    }

    #[tokio::test]
    async fn nested_explore_child_cannot_write_and_parent_stores_run_id() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(write_then_report("pwn.txt", "no\n", "explored"));
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        let child = &parent.children[0];
        assert_eq!(child.profile, "explore");
        assert_eq!(child.task, "scout");
        let payload = nested_child_tool_payload(&parent);
        assert_eq!(payload["run_id"], child.run_id);
        assert_eq!(payload["profile"], "explore");
        assert_eq!(payload["summary"], "explored");
        let child_cp = Checkpoint::load(&eng.state_runs, &child.run_id).unwrap();
        assert_eq!(child_cp.parent_run_id, done.run_id);
        assert_eq!(child_cp.nested_depth, 1);
        assert!(!child_cp.workspace.join("pwn.txt").exists());
        assert!(!done.workspace.join("pwn.txt").exists());
        assert!(
            child_cp
                .messages
                .iter()
                .any(|message| message.content.contains("tool not enabled: write_file")),
            "explore child must deny writes: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_plan_child_is_write_jailed() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        let other = serde_json::json!({
            "path": "other.txt",
            "content": "no\n"
        })
        .to_string();
        let plan = serde_json::json!({
            "path": crate::tools::PLAN_JAIL_PATH,
            "content": "# plan\n"
        })
        .to_string();
        let report = serde_json::json!({"summary": "planned", "success": true}).to_string();
        config.model.script_json = Some(
            serde_json::json!([
                {"tool_calls":[{"name":"write_file","args_json": other}]},
                {"tool_calls":[{"name":"write_file","args_json": plan}]},
                {"tool_calls":[{"name":"report","args_json": report}]}
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("plan", "draft", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = &parent.children[0].run_id;
        let payload = nested_child_tool_payload(&parent);
        assert_eq!(payload["termination"], "parked");
        assert_eq!(payload["park"]["kind"], "plan");
        let child_cp = Checkpoint::load(&eng.state_runs, child_id).unwrap();
        assert_eq!(child_cp.park.as_ref().unwrap().kind, ParkKind::Plan);
        assert!(child_cp.plan_jail);
        assert!(
            child_cp
                .workspace
                .join(crate::tools::PLAN_JAIL_PATH)
                .is_file()
        );
        assert!(!child_cp.workspace.join("other.txt").exists());
    }

    #[tokio::test]
    async fn nested_parent_complete_keeps_shared_workspace_for_parked_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(plan_jail_script());
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("plan", "draft", "parent done"),
        );
        let req = RunRequest::new("delegate");
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = parent.children[0].run_id.clone();
        let child = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert_eq!(child.park.as_ref().unwrap().kind, ParkKind::Plan);
        assert!(
            child.workspace.exists(),
            "parent complete must not remove a shared parked child workspace"
        );
        assert!(child.workspace.join(crate::tools::PLAN_JAIL_PATH).is_file());

        let mut resume_config = base_config(&dir);
        resume_config.model.script_json = Some(plan_jail_script());
        let resume_eng = engine(&dir, resume_config);
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(child_id.clone());
        resume.resume_plan = Some(PlanDecision::Accept);
        let child_done = resume_eng.run(resume).await.unwrap();
        assert!(
            !child.workspace.join("ok.txt").exists(),
            "Accept on a nested plan child must not lift the write-jail"
        );
        let child_after = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert!(child_after.plan_jail);
        assert_ne!(
            child_done.termination,
            RunTermination::Completed,
            "nested plan Accept must not restore execute authority: {}",
            child_done.summary
        );
    }

    #[tokio::test]
    async fn nested_full_child_mutates_when_parent_may() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(write_then_report("ok.txt", "yes\n", "wrote"));
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "edit", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = &parent.children[0].run_id;
        let child_cp = Checkpoint::load(&eng.state_runs, child_id).unwrap();
        assert_eq!(
            std::fs::read_to_string(child_cp.workspace.join("ok.txt")).unwrap(),
            "yes\n"
        );
        assert_eq!(child_cp.workspace, done.workspace);
        assert_eq!(
            std::fs::read_to_string(done.workspace.join("ok.txt")).unwrap(),
            "yes\n"
        );
    }

    #[tokio::test]
    async fn nested_depth_cap_refuses_grandchild_tools() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested_max_depth = 1;
        config.model.script_json = Some(child_run_then_report("explore", "deeper", "child done"));
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        let child_cp = Checkpoint::load(&eng.state_runs, &parent.children[0].run_id).unwrap();
        assert!(child_cp.children.is_empty());
        assert!(
            child_cp.messages.iter().any(|message| {
                message.content.contains("tool not enabled: child_run")
                    || message.content.contains("nested depth cap")
            }),
            "depth cap must refuse grandchild child_run: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_fan_out_cap_fail_closed() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested_max_children = 1;
        config.model.script_json = Some(write_then_report("x.txt", "x\n", "child"));
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({"profile":"explore","task":"one"}).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({"profile":"explore","task":"two"}).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({"summary":"parent done","success":true}).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        assert!(
            parent.messages.iter().any(|message| {
                message.role == "tool" && message.content.contains("fan-out cap")
            }),
            "second child_run must fail closed: {:?}",
            parent.messages
        );
    }

    #[tokio::test]
    async fn nested_default_off_has_no_child_tool() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(child_run_then_report("explore", "scout", "parent done"));
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert!(parent.children.is_empty());
        assert!(
            parent
                .messages
                .iter()
                .any(|message| { message.content.contains("tool not enabled: child_run") }),
            "default off must not expose child_run: {:?}",
            parent.messages
        );
    }

    #[tokio::test]
    async fn nested_request_flag_enables_tools_when_settings_are_off() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested = false;
        config.model.script_json = Some(write_then_report("pwn.txt", "no\n", "explored"));
        let child_script = config.model.script_json.clone();
        config.model.script_json = Some(child_run_then_report("explore", "scout", "parent done"));
        let parent_model = crate::model::from_config(&config).unwrap();
        config.model.script_json = child_script;
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(parent_model),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
            registry: Arc::new(RunRegistry::new(state.path()).unwrap()),
        };
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.nested = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
    }

    #[tokio::test]
    async fn nested_child_begin_run_is_distinct_and_park_is_not_parent_approval() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "approve child?"
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "ask", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        assert_eq!(done.summary, "parent done");
        assert!(done.park.is_none());
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = &parent.children[0].run_id;
        assert_ne!(child_id, &done.run_id);
        let child_cp = Checkpoint::load(&eng.state_runs, child_id).unwrap();
        assert_eq!(child_cp.park.as_ref().unwrap().kind, ParkKind::Escalate);
        let parent_op = parent
            .governance
            .as_ref()
            .map(|gov| gov.operation_id.as_str())
            .unwrap_or_default();
        let child_op = child_cp
            .governance
            .as_ref()
            .map(|gov| gov.operation_id.as_str())
            .unwrap_or_default();
        assert!(!parent_op.is_empty());
        assert!(!child_op.is_empty());
        assert_ne!(parent_op, child_op);
        let child_record = eng.registry.load(child_id).unwrap();
        assert_eq!(
            child_record.logical_operation_id.as_deref(),
            Some(done.run_id.as_str())
        );
        let payload = nested_child_tool_payload(&parent);
        assert_eq!(payload["termination"], "parked");
        assert_eq!(payload["park"]["kind"], "escalate");
    }

    #[tokio::test]
    async fn nested_explore_resume_keeps_read_only_tools() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "approve explore?"
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = parent.children[0].run_id.clone();
        let child_cp = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert_eq!(child_cp.nested_profile, "explore");
        assert_eq!(child_cp.park.as_ref().unwrap().kind, ParkKind::Escalate);

        let mut resume_config = base_config(&dir);
        resume_config.model.script_json = Some(
            serde_json::json!([
                {"content": "already parked"},
                {
                    "tool_calls": [{
                        "name": "write_file",
                        "args_json": serde_json::json!({
                            "path": "pwn.txt",
                            "content": "no\n"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "resumed",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let resume_eng = engine(&dir, resume_config);
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(child_id.clone());
        resume.resume_answer = Some("yes".into());
        let resumed = resume_eng.run(resume).await.unwrap();
        let child_after = Checkpoint::load(&resume_eng.state_runs, &child_id).unwrap();
        assert!(!child_after.workspace.join("pwn.txt").exists());
        assert!(
            child_after
                .messages
                .iter()
                .any(|message| { message.content.contains("tool not enabled: write_file") }),
            "explore resume must keep read-only tools: {:?}",
            child_after.messages
        );
        assert_ne!(resumed.termination, RunTermination::Failed);
    }

    #[tokio::test]
    async fn nested_full_child_resume_keeps_spawn_tool_mode() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::Read;
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "approve full?"
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "edit", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = parent.children[0].run_id.clone();
        let child_cp = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert_eq!(child_cp.nested_profile, "full");
        assert_eq!(child_cp.tools_mode, "read");
        assert_eq!(child_cp.park.as_ref().unwrap().kind, ParkKind::Escalate);

        let mut resume_config = base_config(&dir);
        resume_config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        resume_config.model.script_json = Some(
            serde_json::json!([
                {"content": "already parked"},
                {
                    "tool_calls": [{
                        "name": "write_file",
                        "args_json": serde_json::json!({
                            "path": "pwn.txt",
                            "content": "no\n"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "resumed",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let resume_eng = engine(&dir, resume_config);
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(child_id.clone());
        resume.resume_answer = Some("yes".into());
        let resumed = resume_eng.run(resume).await.unwrap();
        let child_after = Checkpoint::load(&resume_eng.state_runs, &child_id).unwrap();
        assert!(!child_after.workspace.join("pwn.txt").exists());
        assert!(
            child_after
                .messages
                .iter()
                .any(|message| { message.content.contains("tool not enabled: write_file") }),
            "full child resume must keep spawn-time read tools: {:?}",
            child_after.messages
        );
        assert_ne!(resumed.termination, RunTermination::Failed);
    }

    #[tokio::test]
    async fn nested_full_child_run_asks_on_parent_then_completes() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(write_then_report("ok.txt", "yes\n", "wrote"));
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "edit", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.session_wait = true;
        let asked = eng.run(req).await.unwrap();
        assert_eq!(asked.park.as_ref().unwrap().kind, ParkKind::Ask);
        let parent = Checkpoint::load(&eng.state_runs, &asked.run_id).unwrap();
        assert!(
            parent.children.is_empty(),
            "full child_run must ask before spawn"
        );
        assert!(!asked.workspace.join("ok.txt").exists());

        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.session_wait = true;
        resume.resume_run_id = Some(asked.run_id.clone());
        resume.resume_ask = Some(AskDecision::Allow);
        let done = eng.run(resume).await.unwrap();
        assert_eq!(done.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let payload = nested_child_tool_payload(&parent);
        assert_eq!(payload["termination"], "completed");
        assert_eq!(payload["success"], true);
        assert_eq!(
            std::fs::read_to_string(done.workspace.join("ok.txt")).unwrap(),
            "yes\n"
        );
    }

    #[tokio::test]
    async fn nested_explore_child_does_not_attach_parent_mcp_tools() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config
            .tools
            .mcp_servers
            .push(crate::config::McpServerSettings {
                name: "demo".into(),
                command: "mock".into(),
                args: vec![],
                transport: "stdio".into(),
                url: None,
                token_env: None,
                framing: crate::config::McpFraming::ContentLength,
                timeout_secs: 30,
            });
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "mcp.demo.echo",
                        "args_json": "{\"text\":\"hi\"}"
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "explored",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_cp = Checkpoint::load(&eng.state_runs, &parent.children[0].run_id).unwrap();
        assert!(
            child_cp.messages.iter().any(|message| {
                message.content.contains("mcp.demo.echo")
                    && (message.content.contains("not enabled")
                        || message.content.contains("unknown")
                        || message.content.contains("denies"))
            }),
            "explore child must not execute parent MCP tools: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_explore_child_denies_parallel_web_fetch_before_authorize() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.tool_concurrency = 4;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [
                        {
                            "name": "web_fetch",
                            "args_json": "{\"url\":\"https://example.com/a\"}"
                        },
                        {
                            "name": "web_fetch",
                            "args_json": "{\"url\":\"https://example.com/b\"}"
                        }
                    ]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "explored",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_cp = Checkpoint::load(&eng.state_runs, &parent.children[0].run_id).unwrap();
        let denied = child_cp
            .messages
            .iter()
            .filter(|message| {
                message.role == "tool" && message.content.contains("tool not enabled: web_fetch")
            })
            .count();
        assert_eq!(
            denied, 2,
            "explore parallel web_fetch must deny before execute: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_plan_child_does_not_attach_parent_mcp_tools() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config
            .tools
            .mcp_servers
            .push(crate::config::McpServerSettings {
                name: "demo".into(),
                command: "mock".into(),
                args: vec![],
                transport: "stdio".into(),
                url: None,
                token_env: None,
                framing: crate::config::McpFraming::ContentLength,
                timeout_secs: 30,
            });
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "mcp.demo.echo",
                        "args_json": "{\"text\":\"hi\"}"
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "planned",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("plan", "draft", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_cp = Checkpoint::load(&eng.state_runs, &parent.children[0].run_id).unwrap();
        assert!(child_cp.plan_jail);
        assert!(
            child_cp.messages.iter().any(|message| {
                message.content.contains("mcp.demo.echo")
                    && (message.content.contains("not enabled")
                        || message.content.contains("unknown")
                        || message.content.contains("denies"))
            }),
            "plan child must not attach parent MCP servers: {:?}",
            child_cp.messages
        );
        assert!(
            child_cp.messages.iter().all(|message| {
                !message.content.contains("plan jail") && !message.content.contains("plan_jail")
            }),
            "denied MCP must be missing, not jailed: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_plan_jail_full_child_does_not_attach_parent_mcp_tools() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.plan_jail = true;
        config
            .tools
            .mcp_servers
            .push(crate::config::McpServerSettings {
                name: "demo".into(),
                command: "mock".into(),
                args: vec![],
                transport: "stdio".into(),
                url: None,
                token_env: None,
                framing: crate::config::McpFraming::ContentLength,
                timeout_secs: 30,
            });
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "mcp.demo.echo",
                        "args_json": "{\"text\":\"hi\"}"
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "wrote",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "edit", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.plan_jail = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_cp = Checkpoint::load(&eng.state_runs, &parent.children[0].run_id).unwrap();
        assert!(child_cp.plan_jail);
        assert!(
            child_cp.messages.iter().any(|message| {
                message.content.contains("mcp.demo.echo")
                    && (message.content.contains("not enabled")
                        || message.content.contains("unknown")
                        || message.content.contains("denies"))
            }),
            "jailed full child must not attach parent MCP servers: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_child_resume_does_not_reenable_nested_tools() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested_max_depth = 2;
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "approve explore?"
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = parent.children[0].run_id.clone();

        let mut resume_config = base_config(&dir);
        resume_config.run.nested = true;
        resume_config.run.nested_max_depth = 2;
        resume_config.model.script_json = Some(
            serde_json::json!([
                {"content": "already parked"},
                {
                    "tool_calls": [{
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "full",
                            "task": "pwn"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "write_file",
                        "args_json": serde_json::json!({
                            "path": "pwn.txt",
                            "content": "no\n"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "resumed",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let resume_eng = engine(&dir, resume_config);
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(child_id.clone());
        resume.resume_answer = Some("yes".into());
        resume.nested = true;
        let resumed = resume_eng.run(resume).await.unwrap();
        let child_after = Checkpoint::load(&resume_eng.state_runs, &child_id).unwrap();
        assert!(child_after.children.is_empty());
        assert!(!child_after.workspace.join("pwn.txt").exists());
        assert!(
            child_after
                .messages
                .iter()
                .any(|message| { message.content.contains("tool not enabled: child_run") }),
            "resumed nested child must not inherit child_run: {:?}",
            child_after.messages
        );
        assert_ne!(resumed.termination, RunTermination::Failed);
    }

    #[tokio::test]
    async fn nested_wait_false_reports_running_after_registry_start() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "explored",
                        "success": true
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "explore",
                        "task": "scout",
                        "wait": false
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let payload = nested_child_tool_payload(&parent);
        assert_eq!(payload["status"], "running");
        let child_id = parent.children[0].run_id.clone();
        assert!(
            eng.registry.load(&child_id).is_ok(),
            "wait=false must not return running before registry.start: {payload}"
        );
    }

    fn wait_false_start_harness(
        dir: &tempfile::TempDir,
        parent_id: &str,
    ) -> (
        Engine,
        super::session::RunSession,
        crate::tools::ToolRegistry,
        RunRequest,
    ) {
        let mut config = base_config(dir);
        config.run.nested = true;
        let eng = engine(dir, config);
        let ws = dir.path().join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        let tools = crate::tools::ToolRegistry::from_config(&ws, &eng.config).unwrap();
        let mut session = super::session::RunSession::new(
            eng.state_runs.clone(),
            Arc::clone(&eng.governance),
            parent_id,
            "delegate",
            ws,
            "inplace",
            true,
            vec![],
            0,
        );
        session.nested = true;
        let mut request = RunRequest::new("delegate");
        request.keep_workspace = true;
        (eng, session, tools, request)
    }

    fn queue_test_background_child(
        session: &mut super::session::RunSession,
        registry: &Arc<crate::registry::RunRegistry>,
        child_id: &str,
        start_delay: Option<Duration>,
        exit_immediately: bool,
    ) -> (usize, ToolCall, super::nested::QueuedBackgroundChild) {
        session.children.push(crate::checkpoint::ChildRunRecord {
            run_id: child_id.into(),
            profile: "explore".into(),
            task: "scout".into(),
        });
        let start_rx = registry.watch_start(child_id).unwrap();
        let handle = if exit_immediately {
            tokio::spawn(async {})
        } else {
            let registry = Arc::clone(registry);
            let child_id = child_id.to_owned();
            tokio::spawn(async move {
                if let Some(delay) = start_delay {
                    tokio::time::sleep(delay).await;
                }
                registry.start(&child_id, "scout", None, None).unwrap();
                tokio::time::sleep(Duration::from_secs(30)).await;
            })
        };
        session.push_background_child_id(child_id, handle);
        (
            session.children.len() - 1,
            ToolCall {
                id: String::new(),
                name: "child_run".into(),
                args_json: String::new(),
            },
            super::nested::QueuedBackgroundChild::testing(
                child_id,
                ChildProfile::Explore,
                start_rx,
                exit_immediately,
            ),
        )
    }

    #[tokio::test]
    async fn nested_wait_false_start_wait_is_notify_driven() {
        let dir = tempdir().unwrap();
        let (eng, mut session, tools, request) = wait_false_start_harness(&dir, "parent-notify");
        let delay = Duration::from_millis(80);
        let queued = vec![
            queue_test_background_child(
                &mut session,
                &eng.registry,
                "child-start-a",
                Some(delay),
                false,
            ),
            queue_test_background_child(
                &mut session,
                &eng.registry,
                "child-start-b",
                Some(delay),
                false,
            ),
        ];
        let loads_a = crate::checkpoint::checkpoint_load_count_for("child-start-a");
        let loads_b = crate::checkpoint::checkpoint_load_count_for("child-start-b");
        let saves = crate::checkpoint::checkpoint_save_count_for("parent-notify");
        let started = tokio::time::Instant::now();
        let outcomes = tokio::time::timeout(
            Duration::from_secs(1),
            super::nested::wait_for_queued_background_children(
                &eng,
                &mut session,
                &request,
                &tools,
                queued,
                started,
                None,
            ),
        )
        .await
        .expect("start wait must resolve on registry notify without a child checkpoint")
        .unwrap();
        assert_eq!(outcomes.len(), 2);
        for (_, _, outcome) in &outcomes {
            let crate::tools::ToolOutput::Text(text) = outcome.as_ref().expect("start outcome")
            else {
                panic!("expected text start payload: {outcome:?}");
            };
            let payload: serde_json::Value = serde_json::from_str(text).unwrap();
            assert_eq!(payload["status"], "running");
        }
        assert_eq!(
            crate::checkpoint::checkpoint_load_count_for("child-start-a") - loads_a,
            0,
            "start wait must not Checkpoint::load child-start-a"
        );
        assert_eq!(
            crate::checkpoint::checkpoint_load_count_for("child-start-b") - loads_b,
            0,
            "start wait must not Checkpoint::load child-start-b"
        );
        assert_eq!(
            crate::checkpoint::checkpoint_save_count_for("parent-notify") - saves,
            1,
            "queued wait=false slot claims must share one parent save"
        );
        assert!(Checkpoint::load(&eng.state_runs, "child-start-a").is_err());
        assert!(Checkpoint::load(&eng.state_runs, "child-start-b").is_err());
        for id in ["child-start-a", "child-start-b"] {
            if let Some(handle) = session.take_background_child(id) {
                handle.abort();
            }
        }
    }

    #[tokio::test]
    async fn nested_wait_false_failed_start_surfaces_without_grace_hang() {
        let dir = tempdir().unwrap();
        let (eng, mut session, tools, request) = wait_false_start_harness(&dir, "parent-fail");
        let queued = vec![queue_test_background_child(
            &mut session,
            &eng.registry,
            "child-fail-start",
            None,
            true,
        )];
        let started = tokio::time::Instant::now();
        let outcomes = tokio::time::timeout(
            Duration::from_millis(500),
            super::nested::wait_for_queued_background_children(
                &eng,
                &mut session,
                &request,
                &tools,
                queued,
                started,
                None,
            ),
        )
        .await
        .expect("failed start must surface without waiting out parent-bound grace")
        .unwrap();
        let crate::tools::ToolOutput::Text(text) = outcomes[0].2.as_ref().expect("start outcome")
        else {
            panic!("expected text start payload: {:?}", outcomes[0].2);
        };
        let payload: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(payload["termination"], "cancelled");
        assert_eq!(payload["success"], false);
        assert!(
            payload["summary"]
                .as_str()
                .is_some_and(|summary| summary.contains("cancelled before start")),
            "{payload}"
        );
        assert!(session.children.is_empty());
    }

    #[tokio::test]
    async fn nested_background_children_share_one_runtime() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        let n = config.run.nested_max_children.max(1);
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "explored",
                        "success": true
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let mut parent_turns = Vec::new();
        for i in 0..n {
            parent_turns.push(serde_json::json!({
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "explore",
                        "task": format!("scout-{i}"),
                        "wait": false
                    }).to_string()
                }]
            }));
        }
        parent_turns.push(serde_json::json!({
            "tool_calls": [{
                "name": "report",
                "args_json": serde_json::json!({
                    "summary": "parent done",
                    "success": true
                }).to_string()
            }]
        }));
        let parent_script = serde_json::Value::Array(parent_turns).to_string();
        let builds_before = super::nested::background_runtime_builds();
        let spawns_before = super::nested::background_spawns();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), n as usize);
        let spawns = super::nested::background_spawns() - spawns_before;
        let builds = super::nested::background_runtime_builds() - builds_before;
        assert!(
            n > 1,
            "fan-out default must spawn more than one child to prove pooling"
        );
        assert!(
            spawns >= n,
            "each wait=false child must go through the pooled spawn helper, spawns={spawns}"
        );
        assert!(
            builds <= 1,
            "N background children must share one runtime, not N; builds={builds}"
        );
    }

    #[tokio::test]
    async fn nested_wait_false_batch_starts_children_concurrently() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "explored",
                        "success": true
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "a",
                            "wait": false
                        }).to_string()
                    },
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "b",
                            "wait": false
                        }).to_string()
                    },
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "c",
                            "wait": false
                        }).to_string()
                    }
                ]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 3);
        assert_eq!(super::nested::last_background_batch_wait(), 3);
        assert!(!tools::is_parallel_safe_tool("child_run"));
        let running = parent
            .messages
            .iter()
            .filter(|message| {
                message.role == "tool"
                    && serde_json::from_str::<serde_json::Value>(&message.content)
                        .ok()
                        .is_some_and(|payload| payload["status"] == "running")
            })
            .count();
        assert_eq!(running, 3, "each wait=false child must report running");
    }

    #[tokio::test]
    async fn nested_wait_false_batch_honors_fan_out_cap() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested_max_children = 2;
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "explored",
                        "success": true
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "one",
                            "wait": false
                        }).to_string()
                    },
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "two",
                            "wait": false
                        }).to_string()
                    },
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "three",
                            "wait": false
                        }).to_string()
                    }
                ]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 2);
        assert_eq!(super::nested::last_background_batch_wait(), 2);
        assert!(
            parent.messages.iter().any(|message| {
                message.role == "tool" && message.content.contains("fan-out cap")
            }),
            "third wait=false child_run must fail closed: {:?}",
            parent.messages
        );
    }

    #[tokio::test]
    async fn nested_wait_false_mixed_with_write_stays_serial() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        let read = serde_json::json!({"path": "marker.txt"}).to_string();
        let report = serde_json::json!({"summary": "saw it", "success": true}).to_string();
        config.model.script_json = Some(
            serde_json::json!([
                {"tool_calls":[{"name":"read_file","args_json": read}]},
                {"tool_calls":[{"name":"report","args_json": report}]}
            ])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [
                    {
                        "name": "write_file",
                        "args_json": serde_json::json!({
                            "path": "marker.txt",
                            "content": "from-parent\n"
                        }).to_string()
                    },
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "scout",
                            "wait": false
                        }).to_string()
                    }
                ]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        assert_eq!(
            super::nested::last_background_batch_wait(),
            1,
            "write + child_run must not share a nested start-wait"
        );
        let child_cp = Checkpoint::load(&eng.state_runs, &parent.children[0].run_id).unwrap();
        assert!(
            child_cp
                .messages
                .iter()
                .any(|message| message.content.contains("from-parent")),
            "serial write then child_run must materialize before the child reads: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_wait_true_batch_stays_serial() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "explored",
                        "success": true
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "one"
                        }).to_string()
                    },
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "two"
                        }).to_string()
                    }
                ]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 2);
        let summaries = parent
            .messages
            .iter()
            .filter(|message| message.role == "tool")
            .filter_map(|message| serde_json::from_str::<serde_json::Value>(&message.content).ok())
            .filter(|payload| payload["summary"] == "explored")
            .count();
        assert_eq!(
            summaries, 2,
            "wait=true children must complete serially in the calling turn: {:?}",
            parent.messages
        );
    }

    #[tokio::test]
    async fn nested_unattended_park_finalizes_background_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "full",
                        "task": "slow",
                        "wait": false
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "parent park"
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let parked = tokio::time::timeout(std::time::Duration::from_secs(2), eng.run(req))
            .await
            .expect("unattended park must not wait for the child's sleep")
            .unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Escalate);
        let children: Vec<_> = eng
            .registry
            .list()
            .unwrap()
            .into_iter()
            .filter(|record| record.run_id != parked.run_id)
            .collect();
        assert!(
            !children.is_empty(),
            "wait=false child must be recorded: {children:?}"
        );
        assert!(
            children.iter().all(|record| record.status != "running"),
            "unattended park must finalize wait=false children before return: {children:?}"
        );
    }

    #[tokio::test]
    async fn nested_parent_complete_cancels_detached_background_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2 && printf late > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "full",
                        "task": "slow",
                        "wait": false
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "parent park"
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.session_wait = true;
        let asked = eng.run(req).await.unwrap();
        assert_eq!(asked.park.as_ref().unwrap().kind, ParkKind::Ask);
        let mut allow = RunRequest::new("");
        allow.keep_workspace = true;
        allow.session_wait = true;
        allow.resume_run_id = Some(asked.run_id.clone());
        allow.resume_ask = Some(AskDecision::Allow);
        let parked = tokio::time::timeout(std::time::Duration::from_secs(2), eng.run(allow))
            .await
            .expect("parent park must not wait for wait=false child")
            .unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Escalate);
        let mut resume_config = base_config(&dir);
        resume_config.run.nested = true;
        resume_config.model.script_json = Some(
            serde_json::json!([
                {"content": "turn 0"},
                {"content": "turn 1"},
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "parent done",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let resume_eng = engine(&dir, resume_config);
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(parked.run_id.clone());
        resume.resume_answer = Some("yes".into());
        let done = resume_eng.run(resume).await.unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let records = resume_eng.registry.list().unwrap();
            let children: Vec<_> = records
                .iter()
                .filter(|record| record.run_id != done.run_id)
                .collect();
            assert!(
                !children.is_empty(),
                "detached child must be recorded in the registry"
            );
            for record in &children {
                assert!(
                    record.cancel_requested || record.status != "running",
                    "parent complete must request_cancel the detached child: {record:?}"
                );
            }
            if children.iter().all(|record| record.status != "running") {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("detached child still running after cancel timeout: {children:?}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // Wait past a kill-vs-write race on the child's `sleep 2 && printf`.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            !done.workspace.join("late.txt").exists(),
            "cancelled detached child must not keep writing the shared workspace"
        );
    }

    #[tokio::test]
    async fn nested_parent_cancel_after_park_stops_detached_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 5 && printf late > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "full",
                        "task": "slow",
                        "wait": false
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "parent park"
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.session_wait = true;
        let asked = eng.run(req).await.unwrap();
        assert_eq!(asked.park.as_ref().unwrap().kind, ParkKind::Ask);
        let mut allow = RunRequest::new("");
        allow.keep_workspace = true;
        allow.session_wait = true;
        allow.resume_run_id = Some(asked.run_id.clone());
        allow.resume_ask = Some(AskDecision::Allow);
        let parked = tokio::time::timeout(std::time::Duration::from_secs(2), eng.run(allow))
            .await
            .expect("parent park must not wait for wait=false child")
            .unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        assert_eq!(parked.park.as_ref().unwrap().kind, ParkKind::Escalate);
        let mut resume_config = base_config(&dir);
        resume_config.run.nested = true;
        resume_config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        resume_config.model.script_json = Some(
            serde_json::json!([
                {"content": "turn 0"},
                {"content": "turn 1"},
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 10"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "parent done",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let resume_eng = engine(&dir, resume_config);
        let registry = Arc::clone(&resume_eng.registry);
        let parent_id = parked.run_id.clone();
        let cancel = tokio::spawn(async move {
            for _ in 0..80 {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                if registry.run_is_active(&parent_id).unwrap_or(false) {
                    let _ = registry.cancel(&parent_id);
                    return;
                }
            }
        });
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(parked.run_id.clone());
        resume.resume_answer = Some("yes".into());
        let err = tokio::time::timeout(std::time::Duration::from_secs(4), resume_eng.run(resume))
            .await
            .expect("parent cancel must not wait for the child's sleep 5")
            .unwrap_err();
        assert!(
            matches!(err, RunError::Cancelled),
            "parent must cancel: {err}"
        );
        let _ = cancel.await;
        for record in resume_eng.registry.list().unwrap() {
            assert_ne!(
                record.status, "running",
                "cancel after park must wait until detached children stop: {record:?}"
            );
        }
        // Past the pre-fix 2s grace: a still-active child would finish the write.
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        assert!(
            !parked.workspace.join("late.txt").exists(),
            "detached child must not write the shared workspace after parent bounds"
        );
    }

    #[tokio::test]
    async fn check_bounds_does_not_rewrite_the_run_record() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.model.script_json = Some(r#"[{"content":"unused"}]"#.into());
        let eng = engine(&dir, config);
        eng.registry.start("run-1", "task", None, None).unwrap();
        let req = RunRequest::new("task");
        let started = tokio::time::Instant::now();
        let path = eng
            .registry
            .run_dir("run-1")
            .unwrap()
            .join(crate::registry::RUN_RECORD_FILENAME);
        let before = std::fs::read(&path).unwrap();
        let gen_before = run_record_generation(&path);
        for _ in 0..20 {
            supervision::check_bounds(&eng, "run-1", &req, started, None, "").unwrap();
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "check_bounds must not heartbeat/rewrite run.json"
        );
        assert_eq!(run_record_generation(&path), gen_before);
    }

    #[tokio::test]
    async fn nested_wait_true_does_not_heartbeat_run_records_at_poll_rate() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "slow", "parent done"),
        );
        let runs_root = eng.registry.runs_root().to_path_buf();
        let sampler = tokio::spawn(async move {
            let ids = wait_for_run_records(&runs_root, 2).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            count_run_record_writes(&runs_root, &ids, Duration::from_millis(500)).await
        });
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = tokio::time::timeout(Duration::from_secs(5), eng.run(req))
            .await
            .expect("nested wait=true run must finish")
            .unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        assert_eq!(done.summary, "parent done");
        let writes = sampler.await.expect("write sampler");
        assert!(
            writes.len() >= 2,
            "parent and child must both have run records: {writes:?}"
        );
        for (run_id, count) in &writes {
            assert!(
                *count <= 2,
                "run {run_id} rewrote run.json {count} times in 500ms; 50ms check_bounds heartbeat is ~20 Hz"
            );
        }
    }

    #[tokio::test]
    async fn nested_parent_cancel_stops_waited_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2 && printf done > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "slow", "parent done"),
        );
        let registry = Arc::clone(&eng.registry);
        let state_runs = eng.state_runs.clone();
        let cancel = tokio::spawn(async move {
            for _ in 0..40 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let Ok(records) = registry.list() else {
                    continue;
                };
                for record in records {
                    let Ok(checkpoint) = Checkpoint::load(&state_runs, &record.run_id) else {
                        continue;
                    };
                    if checkpoint.parent_run_id.is_empty() && !checkpoint.children.is_empty() {
                        let _ = registry.cancel(&record.run_id);
                        return;
                    }
                }
            }
        });
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let err = tokio::time::timeout(std::time::Duration::from_secs(3), eng.run(req))
            .await
            .expect("parent cancel must not wait for the full child sleep")
            .unwrap_err();
        assert!(
            matches!(err, RunError::Cancelled),
            "parent must cancel: {err}"
        );
        let _ = cancel.await;
        for record in eng.registry.list().unwrap() {
            assert_ne!(
                record.status, "running",
                "dropped child must finalize: {record:?}"
            );
        }
    }

    #[tokio::test]
    async fn nested_waited_child_cancel_does_not_cancel_parent() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2 && printf late > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "slow", "parent done"),
        );
        let registry = Arc::clone(&eng.registry);
        let state_runs = eng.state_runs.clone();
        let cancel = tokio::spawn(async move {
            for _ in 0..40 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let Ok(records) = registry.list() else {
                    continue;
                };
                for record in records {
                    let Ok(checkpoint) = Checkpoint::load(&state_runs, &record.run_id) else {
                        continue;
                    };
                    if !checkpoint.parent_run_id.is_empty() {
                        let _ = registry.cancel(&record.run_id);
                        return;
                    }
                }
            }
        });
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = tokio::time::timeout(std::time::Duration::from_secs(3), eng.run(req))
            .await
            .expect("parent must finish after an independent child cancel")
            .unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        assert_eq!(done.summary, "parent done");
        let _ = cancel.await;
        assert!(
            !done.workspace.join("late.txt").exists(),
            "cancelled waited child must not keep writing the shared workspace"
        );
    }

    #[tokio::test]
    async fn nested_parent_timeout_stops_waited_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2 && printf done > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "slow", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.timeout = Some(Duration::from_millis(200));
        let err = tokio::time::timeout(Duration::from_secs(3), eng.run(req))
            .await
            .expect("parent timeout must not wait for the full child sleep")
            .unwrap_err();
        assert!(
            matches!(err, RunError::TimedOut(_)),
            "parent must time out while waiting on a child: {err}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        for record in eng.registry.list().unwrap() {
            assert_ne!(
                record.status, "running",
                "timed-out wait=true parent must finalize children: {record:?}"
            );
            if let Some(workspace) = &record.workspace {
                assert!(
                    !std::path::Path::new(workspace).join("late.txt").exists(),
                    "timed-out waited child must not keep writing the shared workspace"
                );
            }
        }
    }

    #[tokio::test]
    async fn nested_parent_timeout_covers_background_child_join() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 5 && printf late > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "full",
                        "task": "slow",
                        "wait": false
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.timeout = Some(std::time::Duration::from_millis(200));
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), eng.run(req))
            .await
            .expect("parent timeout must return")
            .unwrap_err();
        assert!(
            matches!(err, RunError::TimedOut(_)),
            "parent must time out while joining a wait=false child: {err}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        for record in eng.registry.list().unwrap() {
            assert_ne!(
                record.status, "running",
                "parent timeout must wait for wait=false children to stop: {record:?}"
            );
            if let Some(workspace) = &record.workspace {
                assert!(
                    !std::path::Path::new(workspace).join("late.txt").exists(),
                    "timed-out parent must not leave child bash writing the shared workspace"
                );
            }
        }
    }

    #[tokio::test]
    async fn nested_plan_jail_parent_can_start_explore_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.plan_jail = true;
        config.model.script_json = Some(write_then_report("pwn.txt", "no\n", "explored"));
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.plan_jail = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].profile, "explore");
        let payload = nested_child_tool_payload(&parent);
        assert_eq!(payload["profile"], "explore");
        assert_ne!(
            done.termination,
            RunTermination::Failed,
            "plan-jail parent must dispatch child_run: {}",
            done.summary
        );
    }

    #[tokio::test]
    async fn nested_plan_jail_full_child_stays_jailed() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.plan_jail = true;
        config.model.script_json = Some(write_then_report("pwn.txt", "no\n", "wrote"));
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "edit", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.plan_jail = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].profile, "full");
        let child = Checkpoint::load(&eng.state_runs, &parent.children[0].run_id).unwrap();
        assert!(child.plan_jail);
        assert!(!child.workspace.join("pwn.txt").exists());
    }

    #[tokio::test]
    async fn nested_plan_jail_full_child_accept_does_not_widen_jail() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.plan_jail = true;
        config.model.script_json = Some(plan_jail_script());
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("full", "edit", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.plan_jail = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert!(parent.plan_jail);
        assert_eq!(parent.children[0].profile, "full");
        let child_id = parent.children[0].run_id.clone();
        let child = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert_eq!(child.park.as_ref().unwrap().kind, ParkKind::Plan);
        assert!(child.plan_jail);
        assert!(!child.workspace.join("ok.txt").exists());

        let mut resume_config = base_config(&dir);
        resume_config.model.script_json = Some(plan_jail_script());
        let resume_eng = engine(&dir, resume_config);
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(child_id.clone());
        resume.resume_plan = Some(PlanDecision::Accept);
        let child_done = resume_eng.run(resume).await.unwrap();
        assert!(
            !child_done.workspace.join("ok.txt").exists(),
            "Accept on a nested child must not widen jail while the parent is jailed"
        );
        let child_after = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert!(child_after.plan_jail);
        let parent_after = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert!(parent_after.plan_jail);
    }

    #[tokio::test]
    async fn nested_plan_jail_parent_denies_worktree_child() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.plan_jail = true;
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "explore",
                        "task": "scout",
                        "worktree": true
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.plan_jail = true;
        let done = eng.run(req).await.unwrap();
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert!(
            parent.children.is_empty(),
            "plan-jail must not materialize a git-worktree child"
        );
        assert!(
            parent
                .messages
                .iter()
                .any(|message| { message.role == "tool" && message.content.contains("worktree") }),
            "denied child_run must name worktree: {:?}",
            parent.messages
        );
    }

    fn init_git_repo(path: &std::path::Path) {
        std::fs::create_dir_all(path).unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet", "-b", "main"])
                .current_dir(path)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["config", "user.email", "test@example.invalid"])
                .current_dir(path)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["config", "user.name", "Test"])
                .current_dir(path)
                .status()
                .unwrap()
                .success()
        );
        std::fs::write(path.join("README"), "ok\n").unwrap();
        assert!(
            Command::new("git")
                .args(["add", "."])
                .current_dir(path)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["commit", "--quiet", "-m", "init"])
                .current_dir(path)
                .status()
                .unwrap()
                .success()
        );
    }

    fn git_worktree_list(repo: &std::path::Path) -> String {
        String::from_utf8(
            Command::new("git")
                .args(["-C", &repo.to_string_lossy(), "worktree", "list"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn nested_worktree_child_resume_removes_git_worktree() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        init_git_repo(&project);
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        config.model.script_json = Some(plan_jail_script());
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "plan",
                        "task": "draft",
                        "worktree": true
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let parent = eng.run(req).await.unwrap();
        assert_eq!(parent.termination, RunTermination::Completed);
        let parent_cp = Checkpoint::load(&eng.state_runs, &parent.run_id).unwrap();
        let child_id = parent_cp.children[0].run_id.clone();
        let child_cp = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert_eq!(child_cp.workspace_adapter, "git-worktree");
        assert_eq!(child_cp.park.as_ref().unwrap().kind, ParkKind::Plan);
        let listed = git_worktree_list(&project);
        assert!(
            listed.contains(&child_cp.workspace.display().to_string()),
            "parked plan worktree must stay registered: {listed}"
        );

        let mut resume_config = base_config(&dir);
        resume_config.workspace.adapter = "inplace".into();
        resume_config.workspace.root = project.to_string_lossy().into();
        resume_config.model.script_json = Some(plan_jail_script());
        let resume_eng = engine(&dir, resume_config);
        let mut resume = RunRequest::new("");
        resume.resume_run_id = Some(child_id.clone());
        resume.resume_plan = Some(PlanDecision::Accept);
        let done = resume_eng.run(resume).await.unwrap();
        assert!(
            !child_cp.workspace.join("ok.txt").exists(),
            "Accept on a nested plan child must not lift the write-jail"
        );
        let child_after = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert!(child_after.plan_jail);
        assert_ne!(done.termination, RunTermination::Completed);
        let listed = git_worktree_list(&project);
        assert!(
            listed.contains(&child_cp.workspace.display().to_string()),
            "nested plan Accept is not a successful complete; worktree stays: {listed}"
        );
        assert!(
            child_cp.workspace.exists(),
            "isolated worktree must remain while the child is still jailed"
        );
    }

    #[tokio::test]
    async fn nested_worktree_full_child_complete_removes_git_worktree() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        init_git_repo(&project);
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        config.model.script_json = Some(write_then_report("ok.txt", "yes\n", "wrote"));
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "full",
                        "task": "edit",
                        "worktree": true
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let parent = eng.run(req).await.unwrap();
        assert_eq!(parent.termination, RunTermination::Completed);
        let parent_cp = Checkpoint::load(&eng.state_runs, &parent.run_id).unwrap();
        let child_id = parent_cp.children[0].run_id.clone();
        let child_cp = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert_eq!(child_cp.workspace_adapter, "git-worktree");
        let payload = nested_child_tool_payload(&parent_cp);
        assert_eq!(payload["termination"], "completed");
        assert_eq!(payload["success"], true);
        let listed = git_worktree_list(&project);
        assert!(
            !listed.contains(&child_id),
            "successful non-park complete must git worktree remove: {listed}"
        );
        assert!(
            !child_cp.workspace.exists(),
            "isolated worktree directory must be gone"
        );
    }

    #[tokio::test]
    async fn nested_worktree_child_cancel_removes_git_worktree() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        init_git_repo(&project);
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2 && printf late > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "full",
                        "task": "slow",
                        "worktree": true
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let registry = Arc::clone(&eng.registry);
        let state_runs = eng.state_runs.clone();
        let project_for_cancel = project.clone();
        let cancel = tokio::spawn(async move {
            for _ in 0..40 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let Ok(records) = registry.list() else {
                    continue;
                };
                for record in records {
                    let Ok(checkpoint) = Checkpoint::load(&state_runs, &record.run_id) else {
                        continue;
                    };
                    if checkpoint.parent_run_id.is_empty() {
                        continue;
                    }
                    let listed = git_worktree_list(&project_for_cancel);
                    if listed.contains(&checkpoint.workspace.display().to_string()) {
                        let _ = registry.cancel(&record.run_id);
                        return;
                    }
                }
            }
        });
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = tokio::time::timeout(std::time::Duration::from_secs(4), eng.run(req))
            .await
            .expect("parent must finish after child cancel")
            .unwrap();
        assert_eq!(done.termination, RunTermination::Completed);
        assert_eq!(done.summary, "parent done");
        let _ = cancel.await;
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = parent.children[0].run_id.clone();
        let child_cp = Checkpoint::load(&eng.state_runs, &child_id).unwrap();
        assert_eq!(child_cp.workspace_adapter, "git-worktree");
        let listed = git_worktree_list(&project);
        assert!(
            !listed.contains(&child_id),
            "cancel after materialize must git worktree remove: {listed}"
        );
        assert!(
            !child_cp.workspace.exists(),
            "isolated worktree directory must be gone after cancel"
        );
    }

    #[tokio::test]
    async fn nested_worktree_parent_cancel_removes_git_worktree() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        init_git_repo(&project);
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.model.script_json = Some(
            serde_json::json!([
                {
                    "tool_calls": [{
                        "name": "bash",
                        "args_json": serde_json::json!({
                            "command": "sleep 2 && printf late > late.txt"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "slept",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "full",
                        "task": "slow",
                        "worktree": true
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let registry = Arc::clone(&eng.registry);
        let state_runs = eng.state_runs.clone();
        let project_for_cancel = project.clone();
        let cancel = tokio::spawn(async move {
            for _ in 0..40 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let Ok(records) = registry.list() else {
                    continue;
                };
                for record in records {
                    let Ok(checkpoint) = Checkpoint::load(&state_runs, &record.run_id) else {
                        continue;
                    };
                    if checkpoint.parent_run_id.is_empty() && !checkpoint.children.is_empty() {
                        let child_id = &checkpoint.children[0].run_id;
                        let Ok(child) = Checkpoint::load(&state_runs, child_id) else {
                            continue;
                        };
                        let listed = git_worktree_list(&project_for_cancel);
                        if listed.contains(&child.workspace.display().to_string()) {
                            let _ = registry.cancel(&record.run_id);
                            return;
                        }
                    }
                }
            }
        });
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let err = tokio::time::timeout(std::time::Duration::from_secs(4), eng.run(req))
            .await
            .expect("parent cancel must not wait for the full child sleep")
            .unwrap_err();
        assert!(
            matches!(err, RunError::Cancelled),
            "parent must cancel: {err}"
        );
        let _ = cancel.await;
        let listed = git_worktree_list(&project);
        assert!(
            listed.lines().filter(|line| !line.is_empty()).count() <= 1,
            "parent cancel must leave only the primary checkout: {listed}"
        );
    }

    #[tokio::test]
    async fn nested_explore_child_sees_parent_inplace_files() {
        let dir = tempdir().unwrap();
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("seen.txt"), "from-parent\n").unwrap();
        let mut config = base_config(&dir);
        config.workspace.adapter = "inplace".into();
        config.workspace.root = project.to_string_lossy().into();
        let read = serde_json::json!({"path": "seen.txt"}).to_string();
        let report = serde_json::json!({"summary": "saw it", "success": true}).to_string();
        config.model.script_json = Some(
            serde_json::json!([
                {"tool_calls":[{"name":"read_file","args_json": read}]},
                {"tool_calls":[{"name":"report","args_json": report}]}
            ])
            .to_string(),
        );
        let eng = engine_nested(
            &dir,
            config,
            &child_run_then_report("explore", "scout", "parent done"),
        );
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        let child_id = &parent.children[0].run_id;
        let child_cp = Checkpoint::load(&eng.state_runs, child_id).unwrap();
        assert_eq!(child_cp.workspace, done.workspace);
        assert!(
            child_cp
                .messages
                .iter()
                .any(|message| message.content.contains("from-parent")),
            "explore child must read parent files: {:?}",
            child_cp.messages
        );
    }

    #[tokio::test]
    async fn nested_spawn_failure_does_not_burn_fan_out() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested_max_children = 1;
        config.model.script_json = Some(write_then_report("pwn.txt", "no\n", "explored"));
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "explore",
                        "task": "isolated",
                        "worktree": true
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "explore",
                        "task": "scout"
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].task, "scout");
        assert!(
            parent.messages.iter().any(|message| {
                message.role == "tool"
                    && message
                        .content
                        .contains("worktree=true requires a git checkout")
            }),
            "failed worktree spawn must surface as a tool error: {:?}",
            parent.messages
        );
    }

    #[tokio::test]
    async fn nested_worktree_add_failure_does_not_burn_fan_out() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested_max_children = 1;
        let ws = dir.path().join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join(".git"), "not a git dir\n").unwrap();
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "explored",
                        "success": true
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let parent_script = serde_json::json!([
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "explore",
                        "task": "isolated",
                        "worktree": true
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "child_run",
                    "args_json": serde_json::json!({
                        "profile": "explore",
                        "task": "scout"
                    }).to_string()
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let eng = engine_nested(&dir, config, &parent_script);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        let done = eng.run(req).await.unwrap();
        assert_eq!(done.summary, "parent done");
        let parent = Checkpoint::load(&eng.state_runs, &done.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].task, "scout");
        assert!(
            parent.messages.iter().any(|message| {
                message.role == "tool"
                    && (message.content.contains("git")
                        || message.content.contains("worktree")
                        || message.content.contains("Workspace"))
            }),
            "failed git worktree add must surface as a tool error: {:?}",
            parent.messages
        );
    }

    #[tokio::test]
    async fn nested_resume_keeps_nested_tools_without_request_flag() {
        let dir = tempdir().unwrap();
        let mut config = base_config(&dir);
        config.run.nested = false;
        config.model.script_json = Some(
            serde_json::json!([{
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": serde_json::json!({
                        "reason": "need human",
                        "question": "park"
                    }).to_string()
                }]
            }])
            .to_string(),
        );
        let eng = engine(&dir, config);
        let mut req = RunRequest::new("delegate");
        req.keep_workspace = true;
        req.nested = true;
        let parked = eng.run(req).await.unwrap();
        assert_eq!(parked.termination, RunTermination::Parked);
        let parked_cp = Checkpoint::load(&eng.state_runs, &parked.run_id).unwrap();
        assert!(parked_cp.nested);

        let mut resume_config = base_config(&dir);
        resume_config.run.nested = false;
        resume_config.model.script_json = Some(write_then_report("pwn.txt", "no\n", "explored"));
        let child_script = resume_config.model.script_json.clone();
        resume_config.model.script_json = Some(
            serde_json::json!([
                {"content": "already parked"},
                {
                    "tool_calls": [{
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "scout"
                        }).to_string()
                    }]
                },
                {
                    "tool_calls": [{
                        "name": "report",
                        "args_json": serde_json::json!({
                            "summary": "resumed",
                            "success": true
                        }).to_string()
                    }]
                }
            ])
            .to_string(),
        );
        let parent_model = crate::model::from_config(&resume_config).unwrap();
        resume_config.model.script_json = child_script;
        let resume_eng = engine(&dir, resume_config);
        let resume_eng = Engine {
            model: Arc::from(parent_model),
            ..resume_eng
        };
        let mut resume = RunRequest::new("");
        resume.keep_workspace = true;
        resume.resume_run_id = Some(parked.run_id.clone());
        resume.resume_answer = Some("yes".into());
        let done = resume_eng.run(resume).await.unwrap();
        assert_eq!(done.summary, "resumed");
        let parent = Checkpoint::load(&resume_eng.state_runs, &parked.run_id).unwrap();
        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].profile, "explore");
    }
}
