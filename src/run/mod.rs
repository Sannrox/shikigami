//! Run lifecycle engine.
//!
//! Public surface: [`Engine`], [`RunRequest`], [`RunResult`], and related types.
//! Internals:
//! - [`supervision`] — run admission, ownership, heartbeat, and finalization
//! - [`session::RunSession`] — owned attempt state + deep checkpoint interface
//! - [`resume`] — checkpoint workspace boundary checks

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;

#[cfg(test)]
use crate::checkpoint::Checkpoint;
use crate::checkpoint::CheckpointError;
use crate::config::Config;
use crate::events::{EventSink, HarnessEvent};
use crate::governance::{GovernanceError, GovernancePort};
use crate::model::{ChatMessage, CostEstimate, ModelError, ModelPort, TokenUsage, ToolCall};
use crate::registry::RunRegistry;
use crate::tools::{TodoItem, ToolError};
use crate::workspace::{WorkspaceError, WorkspacePort};

mod artifact_lifecycle;
mod model_turn;
mod preparation;
mod resume;
mod session;
mod supervision;
mod tool_batch;
mod transaction;

use supervision::RunSupervision;

#[cfg(test)]
use resume::validate_resumed_workspace;

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

    pub(super) fn emit(&self, run_id: &str, event: HarnessEvent) {
        self.registry.append_event(run_id, &event);
        self.events.emit(event);
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        self.run_with_checkpoint_digest(request, None).await
    }

    pub(crate) async fn run_with_checkpoint_digest(
        &self,
        request: RunRequest,
        expected_checkpoint_digest: Option<&str>,
    ) -> Result<RunResult, RunError> {
        RunSupervision::new(self)
            .execute(request, expected_checkpoint_digest)
            .await
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
