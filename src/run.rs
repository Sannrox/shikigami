//! Run lifecycle engine.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::checkpoint::{
    self, Checkpoint, CheckpointError, ParkedState, StagedToolExecution, StagedToolReport,
    ToolExecutionStatus,
};
use crate::config::Config;
use crate::events::{EventSink, HarnessEvent};
use crate::governance::{GovernanceError, GovernancePort, RunOutcome};
use crate::hooks::{self, HookEvent};
use crate::model::{
    ChatMessage, CostEstimate, ModelError, ModelPort, ModelTurn, TokenUsage, ToolCall,
};
use crate::registry::RunRegistry;
use crate::tools::{self, TodoItem, ToolError, ToolOutput, ToolRegistry};
use crate::workspace::{MaterializedWorkspace, WorkspaceCleanup, WorkspaceError, WorkspacePort};
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Default system prompt body (see [`crate::prompts`] for versioned id / digest).
pub const SYSTEM_PROMPT: &str = crate::prompts::HARNESS_V1.body;

/// Compact middle of the message list when over `threshold`.
/// Keeps the first message (task) and the last `keep_tail` messages.
/// Returns `(before, after)` when compaction ran.
pub fn compact_messages(
    messages: &mut Vec<ChatMessage>,
    threshold: usize,
    keep_tail: usize,
) -> Option<(usize, usize)> {
    let before = messages.len();
    if before <= threshold || before <= keep_tail + 1 {
        return None;
    }
    let head = messages.first().cloned()?;
    let tail_start = before.saturating_sub(keep_tail);
    let tail: Vec<ChatMessage> = messages[tail_start..].to_vec();
    let dropped = before.saturating_sub(1 + tail.len());
    let summary = ChatMessage {
        role: "user".into(),
        content: format!(
            "[context compacted: {dropped} earlier messages omitted; continue the original task]"
        ),
        tool_call_id: String::new(),
        tool_calls: vec![],
    };
    *messages = std::iter::once(head)
        .chain(std::iter::once(summary))
        .chain(tail)
        .collect();
    Some((before, messages.len()))
}

/// How a run ended (success or not).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTermination {
    Completed,
    Cancelled,
    TimedOut,
    MaxTurns,
    Failed,
    /// Awaiting operator answer via resume (escalate tool).
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
        }
    }
}

fn configured_workspace_adapter(config: &Config) -> &str {
    match config.workspace.adapter.as_str() {
        "directory-inplace" => "inplace",
        other => other,
    }
}

fn canonical_workspace_below(root: &Path, suffix: &[&str]) -> Result<PathBuf, RunError> {
    let trusted_root = root.canonicalize().map_err(|error| {
        RunError::Message(format!(
            "configured workspace root cannot be resolved: {}: {error}",
            root.display()
        ))
    })?;
    let mut candidate = root.to_path_buf();
    for component in suffix {
        candidate.push(component);
        let metadata = std::fs::symlink_metadata(&candidate).map_err(|error| {
            RunError::Message(format!(
                "expected checkpoint workspace cannot be inspected: {}: {error}",
                candidate.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RunError::Message(format!(
                "checkpoint workspace path must not contain symlinks: {}",
                candidate.display()
            )));
        }
    }
    let expected = candidate.canonicalize().map_err(|error| {
        RunError::Message(format!(
            "expected checkpoint workspace cannot be resolved: {}: {error}",
            candidate.display()
        ))
    })?;
    if !expected.starts_with(&trusted_root) {
        return Err(RunError::Message(format!(
            "checkpoint workspace {} escapes configured root {}",
            expected.display(),
            trusted_root.display()
        )));
    }
    Ok(expected)
}

fn validate_resumed_workspace(
    config: &Config,
    state_runs: &Path,
    resume_id: &str,
    checkpoint: &Checkpoint,
) -> Result<PathBuf, RunError> {
    let actual = checkpoint.workspace.canonicalize().map_err(|error| {
        RunError::Message(format!(
            "checkpoint workspace cannot be resolved: {}: {error}",
            checkpoint.workspace.display()
        ))
    })?;
    let configured_adapter = configured_workspace_adapter(config);
    let checkpoint_adapter = if checkpoint.workspace_adapter.is_empty() {
        configured_adapter
    } else {
        checkpoint.workspace_adapter.as_str()
    };
    if checkpoint_adapter != configured_adapter {
        return Err(RunError::Message(format!(
            "checkpoint workspace adapter `{checkpoint_adapter}` does not match configured adapter `{configured_adapter}`"
        )));
    }
    let expected = match configured_adapter {
        "directory" => {
            let root = PathBuf::from(&config.workspace.root);
            if root.as_os_str() == "." {
                canonical_workspace_below(state_runs, &[resume_id, "workspace"])?
            } else {
                canonical_workspace_below(&root, &["shikigami-runs", resume_id])?
            }
        }
        "git-worktree" => canonical_workspace_below(state_runs, &[resume_id, "worktree"])?,
        "inplace" => PathBuf::from(&config.workspace.root)
            .canonicalize()
            .map_err(|error| {
                RunError::Message(format!(
                    "configured workspace root cannot be resolved: {}: {error}",
                    config.workspace.root
                ))
            })?,
        other => {
            return Err(RunError::Message(format!(
                "cannot validate checkpoint workspace for adapter `{other}`"
            )));
        }
    };
    if actual != expected {
        return Err(RunError::Message(format!(
            "checkpoint workspace {} does not match configured workspace {}",
            actual.display(),
            expected.display()
        )));
    }
    Ok(actual)
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

/// Operator-visible park payload (library + CLI).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ParkInfo {
    pub reason: String,
    pub question: String,
    pub tool_call_id: String,
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
/// Most hosts should use [`crate::Harness`]. The fields remain public for
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

    fn stable_tool_call_id(call: &ToolCall, turn: u32, index: usize) -> String {
        if call.id.is_empty() {
            format!("tool-{turn}-{index}")
        } else {
            format!("tool-{turn}-{index}-{}", call.id)
        }
    }

    fn conversation_tool_call_id(call: &ToolCall, turn: u32, index: usize) -> String {
        if call.id.is_empty() {
            format!("tool-{turn}-{index}")
        } else {
            call.id.clone()
        }
    }

    fn check_bounds(
        &self,
        run_id: &str,
        request: &RunRequest,
        started: tokio::time::Instant,
        timeout: Option<Duration>,
    ) -> Result<(), RunError> {
        self.registry.heartbeat(run_id).map_err(|error| {
            RunError::Message(format!("run registry heartbeat failed: {error}"))
        })?;
        if let Some(rx) = &request.cancel
            && *rx.borrow()
        {
            return Err(RunError::Cancelled);
        }
        if self.registry.cancel_requested(run_id) {
            return Err(RunError::Cancelled);
        }
        if let Some(limit) = timeout
            && started.elapsed() >= limit
        {
            return Err(RunError::TimedOut(limit));
        }
        Ok(())
    }

    fn emit(&self, run_id: &str, event: HarnessEvent) {
        self.registry.append_event(run_id, &event);
        self.events.emit(event);
    }

    fn capture_artifacts(&self, run_id: &str, workspace: &Path) -> Option<PathBuf> {
        match crate::artifacts::capture_run_artifacts(&self.state_runs, run_id, workspace) {
            Ok(path) => {
                let _ = self.registry.set_artifact_dir(run_id, &path);
                Some(path)
            }
            Err(error) => {
                self.emit(
                    run_id,
                    HarnessEvent::Message {
                        level: "warn".into(),
                        text: format!("artifact capture failed: {error}"),
                    },
                );
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn save_checkpoint(
        &self,
        run_id: &str,
        task: &str,
        messages: &[ChatMessage],
        turns: u32,
        workspace: &Path,
        keep_workspace: bool,
        park: Option<ParkedState>,
        todos: Vec<TodoItem>,
        workspace_adapter: &str,
    ) -> Result<(), RunError> {
        let cp = Checkpoint {
            version: checkpoint::CHECKPOINT_VERSION,
            run_id: run_id.into(),
            task: task.into(),
            prompt_id: checkpoint::prompt_id(SYSTEM_PROMPT),
            messages: messages.to_vec(),
            completed_turns: turns,
            workspace: workspace.to_path_buf(),
            keep_workspace,
            workspace_adapter: workspace_adapter.into(),
            park,
            todos,
            governance: self.governance.checkpoint_state(run_id),
        };
        cp.save(&self.state_runs)?;
        Ok(())
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        self.run_with_checkpoint_digest(request, None).await
    }

    pub(crate) async fn run_with_checkpoint_digest(
        &self,
        request: RunRequest,
        expected_checkpoint_digest: Option<&str>,
    ) -> Result<RunResult, RunError> {
        let mut resume_checkpoint = None;
        if let Some(resume_id) = &request.resume_run_id {
            let (checkpoint, digest) = Checkpoint::load_with_digest(&self.state_runs, resume_id)?;
            if expected_checkpoint_digest.is_some_and(|expected| expected != digest) {
                return Err(RunError::Message(format!(
                    "checkpoint digest mismatch for run {resume_id}"
                )));
            }
            checkpoint.validate_prompt(SYSTEM_PROMPT)?;
            let _ =
                validate_resumed_workspace(&self.config, &self.state_runs, resume_id, &checkpoint)?;
            let workspace_adapter = if checkpoint.workspace_adapter.is_empty() {
                configured_workspace_adapter(&self.config)
            } else {
                checkpoint.workspace_adapter.as_str()
            };
            if request.restore_snapshot.is_some() && workspace_adapter == "inplace" {
                return Err(RunError::Message(
                    "restore_snapshot is not supported with workspace adapter `inplace`".into(),
                ));
            }
            if checkpoint.park.is_some() && request.resume_answer.is_none() {
                return Err(RunError::Message(format!(
                    "run {resume_id} is parked; supply resume_answer / --answer to continue"
                )));
            }
            if checkpoint.park.is_none() && request.resume_answer.is_some() {
                return Err(RunError::Message(
                    "resume_answer provided but run is not parked".into(),
                ));
            }
            resume_checkpoint = Some(checkpoint);
        } else if expected_checkpoint_digest.is_some() {
            return Err(RunError::Message(
                "checkpoint digest requires a resumed run".into(),
            ));
        }
        let run_id = request
            .resume_run_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        if let Err(error) = self.registry.start(
            &run_id,
            &request.task,
            request.logical_operation_id.as_deref(),
            None,
        ) {
            return Err(RunError::Message(format!(
                "run registry start failed: {error}"
            )));
        }
        let heartbeat_registry = Arc::clone(&self.registry);
        let heartbeat_run_id = run_id.clone();
        let heartbeat_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                if heartbeat_registry.heartbeat(&heartbeat_run_id).is_err() {
                    break;
                }
            }
        });
        let result = self
            .run_inner(request, run_id.clone(), resume_checkpoint)
            .await;
        heartbeat_task.abort();
        let _ = heartbeat_task.await;
        match &result {
            Ok(result) => {
                let _ = self.registry.finish_result(result);
            }
            Err(error) => {
                let _ = self.registry.finish_error(&run_id, error);
            }
        }
        result
    }

    async fn run_inner(
        &self,
        request: RunRequest,
        fresh_run_id: String,
        resume_checkpoint: Option<Checkpoint>,
    ) -> Result<RunResult, RunError> {
        let started = tokio::time::Instant::now();
        let timeout = request
            .timeout
            .or_else(|| self.config.run.timeout_secs.map(Duration::from_secs));

        let (
            run_id,
            mut messages,
            mut turns,
            ws,
            task,
            keep_workspace,
            initial_todos,
            governance_checkpoint,
        ) = if let Some(resume_id) = &request.resume_run_id {
            let cp = resume_checkpoint.ok_or_else(|| {
                RunError::Message(format!("checkpoint snapshot missing for run {resume_id}"))
            })?;
            let resumed_workspace =
                validate_resumed_workspace(&self.config, &self.state_runs, resume_id, &cp)?;
            self.emit(
                resume_id,
                HarnessEvent::Status {
                    status: "resuming".into(),
                },
            );
            let ws = MaterializedWorkspace {
                path: resumed_workspace,
                adapter: if cp.workspace_adapter.is_empty() {
                    configured_workspace_adapter(&self.config).into()
                } else {
                    cp.workspace_adapter.clone()
                },
                cleanup: if cp.keep_workspace || cp.workspace_adapter == "inplace" {
                    WorkspaceCleanup::None
                } else {
                    WorkspaceCleanup::RemoveDir
                },
            };
            let task = if request.task.is_empty() {
                cp.task.clone()
            } else {
                request.task.clone()
            };
            let mut messages = cp.messages;
            if let Some(park) = &cp.park {
                let answer = request.resume_answer.as_ref().ok_or_else(|| {
                        RunError::Message(format!(
                            "run {resume_id} is parked (reason: {}); supply resume_answer / --answer to continue",
                            park.reason
                        ))
                    })?;
                messages.push(ChatMessage {
                    role: "tool".into(),
                    content: format!("operator answer: {answer}"),
                    tool_call_id: park.tool_call_id.clone(),
                    tool_calls: vec![],
                });
            } else if request.resume_answer.is_some() {
                return Err(RunError::Message(
                    "resume_answer provided but run is not parked".into(),
                ));
            }
            (
                cp.run_id,
                messages,
                cp.completed_turns,
                ws,
                task,
                cp.keep_workspace || request.keep_workspace,
                cp.todos,
                cp.governance,
            )
        } else {
            let run_id = fresh_run_id;
            self.emit(
                &run_id,
                HarnessEvent::Status {
                    status: "starting".into(),
                },
            );
            let ws = self.workspace.materialize(&run_id, &self.state_runs)?;
            let messages = vec![ChatMessage {
                role: "user".into(),
                content: request.task.clone(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            }];
            (
                run_id,
                messages,
                0u32,
                ws,
                request.task.clone(),
                request.keep_workspace,
                Vec::new(),
                None,
            )
        };

        self.registry
            .update_running(
                &run_id,
                &task,
                request.logical_operation_id.as_deref(),
                &ws.path,
                turns,
            )
            .map_err(|error| RunError::Message(format!("run registry update failed: {error}")))?;

        if let Err(error) =
            crate::artifacts::capture_run_baseline(&self.state_runs, &run_id, &ws.path)
        {
            self.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "warn".into(),
                    text: format!("artifact baseline capture failed: {error}"),
                },
            );
        }

        let prompt_id = crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT);
        let project_rules = crate::context::load_project_rules(&ws.path, &self.config.context);
        let skills = crate::context::load_skills(&ws.path, &self.config.context);
        let system_prompt =
            crate::context::compose_system_prompt(SYSTEM_PROMPT, project_rules.as_ref(), &skills);
        if request.restore_snapshot.is_some() && ws.adapter == "inplace" {
            return Err(RunError::Message(
                "restore_snapshot is not supported with workspace adapter `inplace`".into(),
            ));
        }
        self.emit(
            &run_id,
            HarnessEvent::Prompt {
                prompt_id: prompt_id.clone(),
            },
        );
        if let Some(ref rules) = project_rules {
            self.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("project_rules {} digest={}", rules.filename, rules.digest),
                },
            );
        }
        for s in &skills {
            self.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("skill {} digest={}", s.id, s.digest),
                },
            );
        }
        self.emit(
            &run_id,
            HarnessEvent::Message {
                level: "info".into(),
                text: format!("workspace {}", ws.path.display()),
            },
        );

        if let Some(name) = &request.restore_snapshot {
            crate::workspace::restore_snapshot(&ws.path, &self.state_runs, &run_id, name)?;
            self.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("restored snapshot `{name}`"),
                },
            );
        } else if self.config.workspace.snapshot {
            let dest =
                crate::workspace::take_snapshot(&ws.path, &self.state_runs, &run_id, "initial")?;
            self.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("snapshot initial at {}", dest.display()),
                },
            );
        }

        let mut tools = ToolRegistry::from_config(&ws.path, &self.config)?;
        tools.set_todos(initial_todos);
        if !self.config.tools.mcp_servers.is_empty() {
            crate::mcp::attach_mcp_servers(&mut tools, &self.config).await?;
        }
        let tool_defs = tools.definitions();
        let tools = Arc::new(tools);

        let max_turns = self.config.run.max_turns;
        let mut final_summary = String::from("completed without report");
        let mut success = false;
        let mut termination = RunTermination::Completed;
        let mut usage = TokenUsage::default();
        // A checkpoint taken after model execution but before governance
        // reporting ends with the assistant message. Reuse that durable
        // result on resume instead of charging the plane for a new model call.
        let governed_model_checkpoint = request.resume_run_id.is_some()
            && governance_checkpoint
                .as_ref()
                .is_some_and(|checkpoint| !checkpoint.model_operation_id.is_empty());
        let mut staged_turn = governed_model_checkpoint
            .then(|| {
                messages
                    .last()
                    .filter(|message| message.role == "assistant")
                    .map(|message| ModelTurn {
                        content: message.content.clone(),
                        tool_calls: message.tool_calls.clone(),
                        usage: None,
                    })
            })
            .flatten();

        let handle = self
            .governance
            .begin_run_with_checkpoint(
                &run_id,
                &task,
                request.logical_operation_id.as_deref(),
                governance_checkpoint.as_ref(),
            )
            .await?;

        // Persist the host receipt correlation before any resumable work. A
        // fresh run (or a legacy deferred checkpoint) has no durable host id
        // yet; if this write fails, compensate the newly created remote
        // receipt so retry cannot orphan it.
        let host_receipt_was_durable = governance_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !checkpoint.operation_id.is_empty());
        if let Err(error) = self.save_checkpoint(
            &run_id,
            &task,
            &messages,
            turns,
            &ws.path,
            keep_workspace,
            None,
            tools.todos(),
            &ws.adapter,
        ) {
            if !host_receipt_was_durable
                && let Err(compensation) = self
                    .governance
                    .abort_uncheckpointed_run(
                        &handle,
                        &format!("initial checkpoint failed: {error}"),
                    )
                    .await
            {
                return Err(RunError::Message(format!(
                    "initial checkpoint failed: {error}; governance receipt compensation failed: {compensation}"
                )));
            }
            return Err(error);
        }

        self.governance
            .recover_staged_tool_executions(&handle)
            .await?;
        if let Err(error) = self.governance.replay_staged_tool_reports(&handle).await
            && self.config.requires_governance()
        {
            return Err(error.into());
        }

        // Persist any replay cursor changes before the first turn begins.
        self.save_checkpoint(
            &run_id,
            &task,
            &messages,
            turns,
            &ws.path,
            keep_workspace,
            None,
            tools.todos(),
            &ws.adapter,
        )?;

        if let Err(error) = hooks::run_hooks(
            &self.config.hooks,
            HookEvent::PreRun,
            json!({
                "run_id": run_id,
                "task": task,
                "resume": request.resume_run_id.is_some(),
            }),
        )
        .await
        {
            return Err(RunError::Governance(GovernanceError::Message(format!(
                "pre-run hook failed; governed state remains checkpointed for resume: {error}"
            ))));
        }

        // Preserve an escalation park if reporting that park fails after the
        // park has already been durably written.
        let mut pending_park: Option<ParkedState> = None;

        // Ok(Some(park)) when escalated; Ok(None) when finished normally.
        let result: Result<Option<ParkInfo>, RunError> = async {
            loop {
                self.check_bounds(&run_id, &request, started, timeout)?;

                if staged_turn.is_none() && turns >= max_turns {
                    return Err(RunError::MaxTurns(max_turns));
                }
                if staged_turn.is_none()
                    && let Some(threshold) = self.config.run.compact_after_messages
                {
                    let keep = self.config.run.compact_keep_tail.max(2) as usize;
                    if let Some((before, after)) =
                        compact_messages(&mut messages, threshold as usize, keep)
                    {
                        self.emit(&run_id, HarnessEvent::ContextCompacted { before, after });
                    }
                }
                self.emit(
                    &run_id,
                    HarnessEvent::Status {
                        status: "planning".into(),
                    },
                );
                let turn = if let Some(turn) = staged_turn.take() {
                    // The assistant result is already in the checkpoint. A
                    // stable plane event id makes this retry idempotent if the
                    // prior process reported it before failing to advance.
                    self.report_governance_model(&handle).await?;
                    self.save_checkpoint(
                        &run_id,
                        &task,
                        &messages,
                        turns,
                        &ws.path,
                        keep_workspace,
                        None,
                        tools.todos(),
                        &ws.adapter,
                    )?;
                    turn
                } else {
                    let turn = self
                        .governance
                        .plan_turn(
                            &handle,
                            &system_prompt,
                            &messages,
                            &tool_defs,
                            self.model.as_ref(),
                        )
                        .await?;
                    turns += 1;
                    if let Some(u) = turn.usage {
                        usage.input_tokens = usage.input_tokens.saturating_add(u.input_tokens);
                        usage.output_tokens = usage.output_tokens.saturating_add(u.output_tokens);
                    }
                    self.emit(
                        &run_id,
                        HarnessEvent::ModelTurn {
                            turn: turns,
                            content_preview: turn.content.chars().take(200).collect(),
                        },
                    );

                    messages.push(ChatMessage {
                        role: "assistant".into(),
                        content: turn.content.clone(),
                        tool_call_id: String::new(),
                        tool_calls: turn.tool_calls.clone(),
                    });

                    // The model result is durable before any remote harvest
                    // report can fail or before host tools can run.
                    self.save_checkpoint(
                        &run_id,
                        &task,
                        &messages,
                        turns,
                        &ws.path,
                        keep_workspace,
                        None,
                        tools.todos(),
                        &ws.adapter,
                    )?;
                    self.report_governance_model(&handle).await?;
                    self.save_checkpoint(
                        &run_id,
                        &task,
                        &messages,
                        turns,
                        &ws.path,
                        keep_workspace,
                        None,
                        tools.todos(),
                        &ws.adapter,
                    )?;
                    turn
                };

                // Cancellation is checked after the returned model turn is
                // durable and its receipt event is acknowledged. A stopped
                // run can therefore resume without repeating that call.
                self.check_bounds(&run_id, &request, started, timeout)?;

                if turn.tool_calls.is_empty() {
                    final_summary = if turn.content.is_empty() {
                        "model finished without tools".into()
                    } else {
                        turn.content
                    };
                    success = true;
                    termination = RunTermination::Completed;
                    break;
                }

                let exclusive = turn
                    .tool_calls
                    .iter()
                    .any(|c| tools::must_be_exclusive_batch(&c.name));
                if exclusive && turn.tool_calls.len() != 1 {
                    for c in &turn.tool_calls {
                        messages.push(ChatMessage {
                            role: "tool".into(),
                            content: "tool batch rejected: report/escalate must be the only call"
                                .into(),
                            tool_call_id: c.id.clone(),
                            tool_calls: vec![],
                        });
                    }
                    self.save_checkpoint(
                        &run_id,
                        &task,
                        &messages,
                        turns,
                        &ws.path,
                        keep_workspace,
                        None,
                        tools.todos(),
                        &ws.adapter,
                    )?;
                    continue;
                }

                let concurrency = self.config.run.tool_concurrency.max(1) as usize;
                let hooks_need_serial = self
                    .config
                    .hooks
                    .iter()
                    .any(|h| matches!(h.event.as_str(), "pre_tool" | "post_tool" | "on_park"));
                let can_parallel = concurrency > 1
                    && !hooks_need_serial
                    && turn.tool_calls.len() > 1
                    && turn
                        .tool_calls
                        .iter()
                        .all(|c| tools::is_parallel_safe_tool(&c.name))
                    && turn
                        .tool_calls
                        .iter()
                        .all(|c| !self.governance.tool_requires_execution_checkpoint(&c.name));

                // Ordered ToolStart for stable live streams.
                for call in &turn.tool_calls {
                    self.emit(
                        &run_id,
                        HarnessEvent::ToolStart {
                            name: call.name.clone(),
                            args_json: call.args_json.clone(),
                        },
                    );
                }

                // Parallel path only for all-read batches (no report/park/write).
                let batch_outcomes: Vec<(ToolCall, Result<ToolOutput, String>)> = if can_parallel {
                    self.check_bounds(&run_id, &request, started, timeout)?;
                    let sem = Arc::new(Semaphore::new(concurrency));
                    let mut set = JoinSet::new();
                    for (i, call) in turn.tool_calls.iter().cloned().enumerate() {
                        let tools = Arc::clone(&tools);
                        let gov = Arc::clone(&self.governance);
                        let handle = handle.clone();
                        let sem = Arc::clone(&sem);
                        set.spawn(async move {
                            let _permit = sem.acquire().await.expect("semaphore");
                            let stable_call_id = Engine::stable_tool_call_id(&call, turns, i);
                            if let Err(e) = gov
                                .authorize_tool_with_id(
                                    &handle,
                                    &stable_call_id,
                                    &call.name,
                                    &call.args_json,
                                )
                                .await
                            {
                                return (i, call, Err(e.to_string()));
                            }
                            match tools.execute(&call.name, &call.args_json).await {
                                Ok(out) => (i, call, Ok(out)),
                                Err(e) => (i, call, Err(e.to_string())),
                            }
                        });
                    }
                    let mut raw = Vec::with_capacity(turn.tool_calls.len());
                    while let Some(joined) = set.join_next().await {
                        match joined {
                            Ok(item) => raw.push(item),
                            Err(e) => {
                                return Err(RunError::Message(format!("tool task join: {e}")));
                            }
                        }
                    }
                    raw.sort_by_key(|(i, _, _)| *i);
                    raw.into_iter().map(|(_, call, res)| (call, res)).collect()
                } else {
                    let mut out = Vec::with_capacity(turn.tool_calls.len());
                    for (index, call) in turn.tool_calls.iter().enumerate() {
                        self.check_bounds(&run_id, &request, started, timeout)?;
                        if let Err(e) = hooks::run_hooks(
                            &self.config.hooks,
                            HookEvent::PreTool,
                            json!({
                                "run_id": run_id,
                                "tool": call.name,
                                "args_json": call.args_json,
                            }),
                        )
                        .await
                        {
                            out.push((call.clone(), Err(e)));
                            continue;
                        }
                        let stable_call_id = Self::stable_tool_call_id(call, turns, index);
                        if self
                            .governance
                            .tool_requires_execution_checkpoint(&call.name)
                        {
                            self.governance
                                .stage_tool_execution(
                                    &handle,
                                    StagedToolExecution {
                                        call_id: stable_call_id.clone(),
                                        name: call.name.clone(),
                                        args_json: call.args_json.clone(),
                                        status: ToolExecutionStatus::Authorizing,
                                    },
                                )
                                .await?;
                            self.save_checkpoint(
                                &run_id,
                                &task,
                                &messages,
                                turns,
                                &ws.path,
                                keep_workspace,
                                None,
                                tools.todos(),
                                &ws.adapter,
                            )?;
                        }
                        if let Err(e) = self
                            .governance
                            .authorize_tool_with_id(
                                &handle,
                                &stable_call_id,
                                &call.name,
                                &call.args_json,
                            )
                            .await
                        {
                            out.push((call.clone(), Err(e.to_string())));
                            continue;
                        }
                        if self
                            .governance
                            .tool_requires_execution_checkpoint(&call.name)
                        {
                            self.governance
                                .mark_tool_execution_started(&handle, &stable_call_id)
                                .await?;
                            self.save_checkpoint(
                                &run_id,
                                &task,
                                &messages,
                                turns,
                                &ws.path,
                                keep_workspace,
                                None,
                                tools.todos(),
                                &ws.adapter,
                            )?;
                        }
                        match tools.execute(&call.name, &call.args_json).await {
                            Ok(o) => out.push((call.clone(), Ok(o))),
                            Err(e) => out.push((call.clone(), Err(e.to_string()))),
                        }
                    }
                    out
                };

                // The execution phase above completes every call in the
                // batch before this reporting phase starts. Stage the entire
                // batch first so a required governance error cannot leave
                // later host-side effects absent from the resume checkpoint.
                for (call, outcome) in &batch_outcomes {
                    match outcome {
                        Ok(ToolOutput::Text(text)) => messages.push(ChatMessage {
                            role: "tool".into(),
                            content: text.clone(),
                            tool_call_id: call.id.clone(),
                            tool_calls: vec![],
                        }),
                        Ok(ToolOutput::Report(report)) => messages.push(ChatMessage {
                            role: "tool".into(),
                            content: format!("report: {}", report.summary),
                            tool_call_id: call.id.clone(),
                            tool_calls: vec![],
                        }),
                        Ok(ToolOutput::Park(_)) => {}
                        Err(detail) => messages.push(ChatMessage {
                            role: "tool".into(),
                            content: detail.clone(),
                            tool_call_id: call.id.clone(),
                            tool_calls: vec![],
                        }),
                    }
                }

                for (index, (call, _)) in batch_outcomes.iter().enumerate() {
                    if self
                        .governance
                        .tool_requires_execution_checkpoint(&call.name)
                    {
                        self.governance
                            .mark_tool_execution_complete(
                                &handle,
                                &Self::stable_tool_call_id(call, turns, index),
                            )
                            .await?;
                    }
                }
                let staged_reports = batch_outcomes
                    .iter()
                    .enumerate()
                    .map(|(index, (call, outcome))| StagedToolReport {
                        call_id: Self::stable_tool_call_id(call, turns, index),
                        name: call.name.clone(),
                        ok: match outcome {
                            Ok(ToolOutput::Text(_)) => true,
                            Ok(ToolOutput::Report(report)) => report.success,
                            Ok(ToolOutput::Park(_)) | Err(_) => false,
                        },
                        detail: match outcome {
                            Ok(ToolOutput::Text(text)) => text.clone(),
                            Ok(ToolOutput::Report(report)) => report.summary.clone(),
                            Ok(ToolOutput::Park(park)) => format!("parked: {}", park.reason),
                            Err(detail) => detail.clone(),
                        },
                    })
                    .collect();
                self.governance
                    .stage_tool_reports(&handle, staged_reports)
                    .await?;
                // The completed execution markers are no longer needed once
                // the report intents are staged in memory. Clear them before
                // the single checkpoint that makes those replayable reports
                // durable; a saved checkpoint never contains both a completed
                // effect marker and a safe report replay queue.
                self.governance
                    .clear_staged_tool_executions(&handle)
                    .await?;
                self.save_checkpoint(
                    &run_id,
                    &task,
                    &messages,
                    turns,
                    &ws.path,
                    keep_workspace,
                    None,
                    tools.todos(),
                    &ws.adapter,
                )?;

                let mut terminal_report = false;
                for (index, (call, outcome)) in batch_outcomes.into_iter().enumerate() {
                    let report_call_id = Self::stable_tool_call_id(&call, turns, index);
                    match outcome {
                        Ok(ToolOutput::Text(text)) => {
                            self.report_governance_tool_with_id(
                                &handle,
                                &report_call_id,
                                &call.name,
                                true,
                                &text,
                            )
                            .await?;
                            if call.name == "todo_write" {
                                let items = tools.todos();
                                self.emit(
                                    &run_id,
                                    HarnessEvent::TodosUpdated {
                                        summary: text.chars().take(500).collect(),
                                        item_count: items.len(),
                                    },
                                );
                            }
                            self.emit(
                                &run_id,
                                HarnessEvent::ToolEnd {
                                    name: call.name.clone(),
                                    ok: true,
                                    detail: text.chars().take(500).collect(),
                                },
                            );
                            let _ = hooks::run_hooks(
                                &self.config.hooks,
                                HookEvent::PostTool,
                                json!({
                                    "run_id": run_id,
                                    "tool": call.name,
                                    "ok": true,
                                }),
                            )
                            .await;
                        }
                        Ok(ToolOutput::Report(report)) => {
                            self.report_governance_tool_with_id(
                                &handle,
                                &report_call_id,
                                "report",
                                report.success,
                                &report.summary,
                            )
                            .await?;
                            self.emit(
                                &run_id,
                                HarnessEvent::ToolEnd {
                                    name: "report".into(),
                                    ok: report.success,
                                    detail: report.summary.clone(),
                                },
                            );
                            final_summary = report.summary;
                            success = report.success;
                            termination = RunTermination::Completed;
                            terminal_report = true;
                        }
                        Ok(ToolOutput::Park(park)) => {
                            let detail = format!("parked: {}", park.reason);
                            let parked = ParkedState {
                                reason: park.reason.clone(),
                                question: park.question.clone(),
                                tool_call_id: Self::conversation_tool_call_id(&call, turns, index),
                            };
                            let info = ParkInfo {
                                reason: park.reason.clone(),
                                question: park.question.clone(),
                                tool_call_id: Self::conversation_tool_call_id(&call, turns, index),
                            };
                            final_summary = park.reason.clone();
                            success = false;
                            termination = RunTermination::Parked;
                            pending_park = Some(parked.clone());
                            let report_result = self
                                .report_governance_tool_with_id(
                                    &handle,
                                    &report_call_id,
                                    "escalate",
                                    false,
                                    &detail,
                                )
                                .await;
                            // Save before reporting so a resume checkpoint
                            // carries the park state and the exact pending
                            // event for retry if the report fails.
                            self.save_checkpoint(
                                &run_id,
                                &task,
                                &messages,
                                turns,
                                &ws.path,
                                true,
                                Some(parked),
                                tools.todos(),
                                &ws.adapter,
                            )?;
                            report_result?;
                            self.emit(
                                &run_id,
                                HarnessEvent::ToolEnd {
                                    name: "escalate".into(),
                                    ok: false,
                                    detail: detail.clone(),
                                },
                            );
                            let _ = hooks::run_hooks(
                                &self.config.hooks,
                                HookEvent::OnPark,
                                json!({
                                    "run_id": run_id,
                                    "reason": park.reason,
                                    "question": park.question,
                                }),
                            )
                            .await;
                            return Ok(Some(info));
                        }
                        Err(detail) => {
                            self.report_governance_tool_with_id(
                                &handle,
                                &report_call_id,
                                &call.name,
                                false,
                                &detail,
                            )
                            .await?;
                            self.emit(
                                &run_id,
                                HarnessEvent::ToolEnd {
                                    name: call.name.clone(),
                                    ok: false,
                                    detail: detail.clone(),
                                },
                            );
                            let _ = hooks::run_hooks(
                                &self.config.hooks,
                                HookEvent::PostTool,
                                json!({
                                    "run_id": run_id,
                                    "tool": call.name,
                                    "ok": false,
                                }),
                            )
                            .await;
                        }
                    }
                    self.save_checkpoint(
                        &run_id,
                        &task,
                        &messages,
                        turns,
                        &ws.path,
                        keep_workspace,
                        None,
                        tools.todos(),
                        &ws.adapter,
                    )?;
                }
                self.save_checkpoint(
                    &run_id,
                    &task,
                    &messages,
                    turns,
                    &ws.path,
                    keep_workspace,
                    None,
                    tools.todos(),
                    &ws.adapter,
                )?;
                if terminal_report {
                    return Ok(None);
                }
            }
            Ok(None)
        }
        .await;

        let park_info = match &result {
            Ok(park) => park.clone(),
            Err(_) => None,
        };

        let (success, final_summary, termination) = match result {
            Ok(_) => (success, final_summary, termination),
            Err(e) => {
                let summary = e.to_string();
                tools.kill_background_jobs().await;
                // The complete batch was staged before reporting, so this
                // checkpoint cannot replay an already executed host tool.
                let _ = self.save_checkpoint(
                    &run_id,
                    &task,
                    &messages,
                    turns,
                    &ws.path,
                    true, // keep workspace on failure for resume/inspection
                    pending_park.clone(),
                    tools.todos(),
                    &ws.adapter,
                );
                if !e.leaves_governance_open() {
                    let completion = self
                        .governance
                        .complete_run(
                            &handle,
                            RunOutcome {
                                success: false,
                                summary: summary.clone(),
                                turns,
                                termination: e.termination().as_str().into(),
                                workspace: ws.path.display().to_string(),
                            },
                        )
                        .await;
                    if completion.is_err() {
                        let _ = self.save_checkpoint(
                            &run_id,
                            &task,
                            &messages,
                            turns,
                            &ws.path,
                            true,
                            pending_park.clone(),
                            tools.todos(),
                            &ws.adapter,
                        );
                    } else {
                        // `complete_run` has forgotten the in-memory receipt
                        // state; persist that finalized boundary so a later
                        // resume cannot reuse a terminal plane receipt.
                        let _ = self.save_checkpoint(
                            &run_id,
                            &task,
                            &messages,
                            turns,
                            &ws.path,
                            true,
                            pending_park.clone(),
                            tools.todos(),
                            &ws.adapter,
                        );
                    }
                }
                // Do not delete workspace on cancel/timeout/max-turns so resume works.
                self.emit(
                    &run_id,
                    HarnessEvent::RunFinished {
                        run_id: run_id.clone(),
                        success: false,
                        summary: summary.clone(),
                    },
                );
                // Reap after all error-path bookkeeping and immediately
                // before inventory so descendants cannot keep mutating the
                // workspace after the failed run has been recorded.
                tools.kill_background_jobs().await;
                self.capture_artifacts(&run_id, &ws.path);
                return Err(e);
            }
        };

        if termination != RunTermination::Parked {
            let completion = self
                .governance
                .complete_run(
                    &handle,
                    RunOutcome {
                        success,
                        summary: final_summary.clone(),
                        turns,
                        termination: termination.as_str().into(),
                        workspace: ws.path.display().to_string(),
                    },
                )
                .await;
            if let Err(error) = completion {
                let _ = self.save_checkpoint(
                    &run_id,
                    &task,
                    &messages,
                    turns,
                    &ws.path,
                    true,
                    None,
                    tools.todos(),
                    &ws.adapter,
                );
                tools.kill_background_jobs().await;
                self.capture_artifacts(&run_id, &ws.path);
                return Err(error.into());
            }
            // Successful completion clears adapter-owned receipt correlation
            // from the durable checkpoint. Parked runs intentionally retain
            // it for their governed continuation.
            if let Err(error) = self.save_checkpoint(
                &run_id,
                &task,
                &messages,
                turns,
                &ws.path,
                keep_workspace,
                None,
                tools.todos(),
                &ws.adapter,
            ) {
                tools.kill_background_jobs().await;
                self.capture_artifacts(&run_id, &ws.path);
                return Err(error);
            }
        }

        // Always reap background shells before taking the final inventory.
        tools.kill_background_jobs().await;
        let artifact_dir = self.capture_artifacts(&run_id, &ws.path);

        // Keep workspace on park; only delete on successful non-park completion.
        if !keep_workspace && success && termination != RunTermination::Parked {
            let _ = self.workspace.cleanup(&ws);
        }

        self.emit(
            &run_id,
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
                success,
                summary: final_summary.clone(),
            },
        );

        let cost = CostEstimate::from_usage_and_rates(
            usage,
            self.config.model.input_usd_micros_per_mtok,
            self.config.model.output_usd_micros_per_mtok,
        );

        let _ = hooks::run_hooks(
            &self.config.hooks,
            HookEvent::PostRun,
            json!({
                "run_id": run_id,
                "success": success,
                "termination": termination.as_str(),
                "summary": final_summary,
            }),
        )
        .await;

        Ok(RunResult {
            run_id,
            success,
            summary: final_summary,
            turns,
            workspace: ws.path,
            artifact_dir,
            termination,
            park: park_info,
            prompt_id,
            usage,
            cost,
            todos: tools.todos(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::events;
    use crate::governance;
    use crate::model::{ModelTurn, ScriptedModel, ToolCall};
    use crate::state::StateRoot;
    use crate::workspace;
    use tempfile::tempdir;

    #[test]
    fn compact_messages_shrinks_list() {
        let mut msgs: Vec<ChatMessage> = (0..20)
            .map(|i| ChatMessage {
                role: if i == 0 { "user" } else { "assistant" }.into(),
                content: format!("m{i}"),
                tool_call_id: String::new(),
                tool_calls: vec![],
            })
            .collect();
        let (before, after) = compact_messages(&mut msgs, 10, 4).unwrap();
        assert_eq!(before, 20);
        assert!(after < before);
        assert_eq!(msgs[0].content, "m0");
        assert!(msgs[1].content.contains("compacted"));
        assert_eq!(msgs.last().unwrap().content, "m19");
    }

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

        // Resume with report-only script and higher max_turns
        let mut config2 = base_config(&dir);
        config2.run.max_turns = 10;
        let model2 = ScriptedModel::from_turns(vec![ModelTurn {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "2".into(),
                name: "report".into(),
                args_json: r#"{"summary":"resumed ok","success":true}"#.into(),
            }],
            usage: None,
        }]);
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
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("parked"), "{err}");

        // Resume with answer continues and can report success.
        let model3 = ScriptedModel::from_turns(vec![ModelTurn {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "r1".into(),
                name: "report".into(),
                args_json: r#"{"summary":"approved and done","success":true}"#.into(),
            }],
            usage: None,
        }]);
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
}
