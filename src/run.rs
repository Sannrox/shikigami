//! Run lifecycle engine.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::checkpoint::{self, Checkpoint, CheckpointError, ParkedState};
use crate::config::Config;
use crate::events::{EventSink, HarnessEvent};
use crate::governance::{GovernanceError, GovernancePort, RunOutcome};
use crate::model::{ChatMessage, ModelError, ModelPort, TokenUsage, ToolCall};
use crate::tools::{self, TodoItem, ToolError, ToolOutput, ToolRegistry};
use crate::workspace::{MaterializedWorkspace, WorkspaceCleanup, WorkspaceError, WorkspacePort};
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

#[derive(Debug, Clone)]
pub struct RunResult {
    pub run_id: String,
    pub success: bool,
    pub summary: String,
    pub turns: u32,
    pub workspace: PathBuf,
    pub termination: RunTermination,
    /// Set when `termination == Parked`.
    pub park: Option<ParkInfo>,
    /// Versioned prompt id used for this run (`name:sha256`).
    pub prompt_id: String,
    /// Aggregated token usage when reported by model turns (zeros if unknown).
    pub usage: TokenUsage,
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
}

pub struct Engine {
    pub config: Config,
    pub governance: Arc<dyn GovernancePort>,
    pub workspace: Arc<dyn WorkspacePort>,
    pub model: Arc<dyn ModelPort>,
    pub events: Arc<dyn EventSink>,
    pub state_runs: PathBuf,
}

impl Engine {
    fn check_bounds(
        &self,
        request: &RunRequest,
        started: tokio::time::Instant,
        timeout: Option<Duration>,
    ) -> Result<(), RunError> {
        if let Some(rx) = &request.cancel
            && *rx.borrow()
        {
            return Err(RunError::Cancelled);
        }
        if let Some(limit) = timeout
            && started.elapsed() >= limit
        {
            return Err(RunError::TimedOut(limit));
        }
        Ok(())
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
            park,
            todos,
        };
        cp.save(&self.state_runs)?;
        Ok(())
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        let started = tokio::time::Instant::now();
        let timeout = request
            .timeout
            .or_else(|| self.config.run.timeout_secs.map(Duration::from_secs));

        let (run_id, mut messages, mut turns, ws, task, keep_workspace, initial_todos) =
            if let Some(resume_id) = &request.resume_run_id {
                let cp = Checkpoint::load(&self.state_runs, resume_id)?;
                cp.validate_prompt(SYSTEM_PROMPT)?;
                if !cp.workspace.is_dir() {
                    return Err(RunError::Message(format!(
                        "checkpoint workspace missing: {}",
                        cp.workspace.display()
                    )));
                }
                self.events.emit(HarnessEvent::Status {
                    status: "resuming".into(),
                });
                let ws = MaterializedWorkspace {
                    path: cp.workspace.clone(),
                    adapter: "resumed".into(),
                    cleanup: if cp.keep_workspace {
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
                )
            } else {
                let run_id = Uuid::new_v4().to_string();
                self.events.emit(HarnessEvent::Status {
                    status: "starting".into(),
                });
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
                )
            };

        let prompt_id = crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT);
        let project_rules = crate::context::load_project_rules(&ws.path, &self.config.context);
        let skills = crate::context::load_skills(&ws.path, &self.config.context);
        let system_prompt =
            crate::context::compose_system_prompt(SYSTEM_PROMPT, project_rules.as_ref(), &skills);
        let handle = self
            .governance
            .begin_run(&run_id, &task, request.logical_operation_id.as_deref())
            .await?;

        self.events.emit(HarnessEvent::Prompt {
            prompt_id: prompt_id.clone(),
        });
        if let Some(ref rules) = project_rules {
            self.events.emit(HarnessEvent::Message {
                level: "info".into(),
                text: format!("project_rules {} digest={}", rules.filename, rules.digest),
            });
        }
        for s in &skills {
            self.events.emit(HarnessEvent::Message {
                level: "info".into(),
                text: format!("skill {} digest={}", s.id, s.digest),
            });
        }
        self.events.emit(HarnessEvent::Message {
            level: "info".into(),
            text: format!("workspace {}", ws.path.display()),
        });

        if let Some(name) = &request.restore_snapshot {
            crate::workspace::restore_snapshot(&ws.path, &self.state_runs, &run_id, name)?;
            self.events.emit(HarnessEvent::Message {
                level: "info".into(),
                text: format!("restored snapshot `{name}`"),
            });
        } else if self.config.workspace.snapshot {
            let dest =
                crate::workspace::take_snapshot(&ws.path, &self.state_runs, &run_id, "initial")?;
            self.events.emit(HarnessEvent::Message {
                level: "info".into(),
                text: format!("snapshot initial at {}", dest.display()),
            });
        }

        let enabled = self.config.tools.effective_enabled();
        let mut tools = ToolRegistry::with_builtins_ignore(
            &ws.path,
            enabled,
            self.config.tools.bash_timeout_secs,
            self.config.network.clone(),
            self.config.tools.respect_ignore,
        )?;
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

        // Persist initial checkpoint so resume works mid-run.
        self.save_checkpoint(
            &run_id,
            &task,
            &messages,
            turns,
            &ws.path,
            keep_workspace,
            None,
            tools.todos(),
        )?;

        // Ok(Some(park)) when escalated; Ok(None) when finished normally.
        let result: Result<Option<ParkInfo>, RunError> = async {
            loop {
                self.check_bounds(&request, started, timeout)?;

                if turns >= max_turns {
                    return Err(RunError::MaxTurns(max_turns));
                }
                if let Some(threshold) = self.config.run.compact_after_messages {
                    let keep = self.config.run.compact_keep_tail.max(2) as usize;
                    if let Some((before, after)) =
                        compact_messages(&mut messages, threshold as usize, keep)
                    {
                        self.events
                            .emit(HarnessEvent::ContextCompacted { before, after });
                    }
                }
                self.events.emit(HarnessEvent::Status {
                    status: "planning".into(),
                });
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
                self.events.emit(HarnessEvent::ModelTurn {
                    turn: turns,
                    content_preview: turn.content.chars().take(200).collect(),
                });

                self.check_bounds(&request, started, timeout)?;

                messages.push(ChatMessage {
                    role: "assistant".into(),
                    content: turn.content.clone(),
                    tool_call_id: String::new(),
                    tool_calls: turn.tool_calls.clone(),
                });

                self.save_checkpoint(
                    &run_id,
                    &task,
                    &messages,
                    turns,
                    &ws.path,
                    keep_workspace,
                    None,
                    tools.todos(),
                )?;

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
                    )?;
                    continue;
                }

                let concurrency = self.config.run.tool_concurrency.max(1) as usize;
                let can_parallel = concurrency > 1
                    && turn.tool_calls.len() > 1
                    && turn
                        .tool_calls
                        .iter()
                        .all(|c| tools::is_parallel_safe_tool(&c.name));

                // Ordered ToolStart for stable live streams.
                for call in &turn.tool_calls {
                    self.events.emit(HarnessEvent::ToolStart {
                        name: call.name.clone(),
                        args_json: call.args_json.clone(),
                    });
                }

                // Parallel path only for all-read batches (no report/park/write).
                let batch_outcomes: Vec<(ToolCall, Result<ToolOutput, String>)> = if can_parallel {
                    self.check_bounds(&request, started, timeout)?;
                    let sem = Arc::new(Semaphore::new(concurrency));
                    let mut set = JoinSet::new();
                    for (i, call) in turn.tool_calls.iter().cloned().enumerate() {
                        let tools = Arc::clone(&tools);
                        let gov = Arc::clone(&self.governance);
                        let handle = handle.clone();
                        let sem = Arc::clone(&sem);
                        set.spawn(async move {
                            let _permit = sem.acquire().await.expect("semaphore");
                            if let Err(e) = gov
                                .authorize_tool(&handle, &call.name, &call.args_json)
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
                    for call in &turn.tool_calls {
                        self.check_bounds(&request, started, timeout)?;
                        if let Err(e) = self
                            .governance
                            .authorize_tool(&handle, &call.name, &call.args_json)
                            .await
                        {
                            out.push((call.clone(), Err(e.to_string())));
                            continue;
                        }
                        match tools.execute(&call.name, &call.args_json).await {
                            Ok(o) => out.push((call.clone(), Ok(o))),
                            Err(e) => out.push((call.clone(), Err(e.to_string()))),
                        }
                    }
                    out
                };

                for (call, outcome) in batch_outcomes {
                    match outcome {
                        Ok(ToolOutput::Text(text)) => {
                            let _ = self
                                .governance
                                .report_tool(&handle, &call.name, true, &text)
                                .await;
                            if call.name == "todo_write" {
                                let items = tools.todos();
                                self.events.emit(HarnessEvent::TodosUpdated {
                                    summary: text.chars().take(500).collect(),
                                    item_count: items.len(),
                                });
                            }
                            self.events.emit(HarnessEvent::ToolEnd {
                                name: call.name.clone(),
                                ok: true,
                                detail: text.chars().take(500).collect(),
                            });
                            messages.push(ChatMessage {
                                role: "tool".into(),
                                content: text,
                                tool_call_id: call.id.clone(),
                                tool_calls: vec![],
                            });
                        }
                        Ok(ToolOutput::Report(report)) => {
                            let _ = self
                                .governance
                                .report_tool(&handle, "report", report.success, &report.summary)
                                .await;
                            self.events.emit(HarnessEvent::ToolEnd {
                                name: "report".into(),
                                ok: report.success,
                                detail: report.summary.clone(),
                            });
                            final_summary = report.summary;
                            success = report.success;
                            termination = RunTermination::Completed;
                            messages.push(ChatMessage {
                                role: "tool".into(),
                                content: format!("report: {}", final_summary),
                                tool_call_id: call.id.clone(),
                                tool_calls: vec![],
                            });
                            self.save_checkpoint(
                                &run_id,
                                &task,
                                &messages,
                                turns,
                                &ws.path,
                                keep_workspace,
                                None,
                                tools.todos(),
                            )?;
                            return Ok(None);
                        }
                        Ok(ToolOutput::Park(park)) => {
                            let detail = format!("parked: {}", park.reason);
                            let _ = self
                                .governance
                                .report_tool(&handle, "escalate", false, &detail)
                                .await;
                            self.events.emit(HarnessEvent::ToolEnd {
                                name: "escalate".into(),
                                ok: false,
                                detail: detail.clone(),
                            });
                            let parked = ParkedState {
                                reason: park.reason.clone(),
                                question: park.question.clone(),
                                tool_call_id: call.id.clone(),
                            };
                            let info = ParkInfo {
                                reason: park.reason.clone(),
                                question: park.question.clone(),
                                tool_call_id: call.id.clone(),
                            };
                            final_summary = park.reason;
                            success = false;
                            termination = RunTermination::Parked;
                            self.save_checkpoint(
                                &run_id,
                                &task,
                                &messages,
                                turns,
                                &ws.path,
                                true,
                                Some(parked),
                                tools.todos(),
                            )?;
                            return Ok(Some(info));
                        }
                        Err(detail) => {
                            let _ = self
                                .governance
                                .report_tool(&handle, &call.name, false, &detail)
                                .await;
                            self.events.emit(HarnessEvent::ToolEnd {
                                name: call.name.clone(),
                                ok: false,
                                detail: detail.clone(),
                            });
                            messages.push(ChatMessage {
                                role: "tool".into(),
                                content: detail,
                                tool_call_id: call.id.clone(),
                                tool_calls: vec![],
                            });
                        }
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
                )?;
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
                let _ = self.save_checkpoint(
                    &run_id,
                    &task,
                    &messages,
                    turns,
                    &ws.path,
                    true, // keep workspace on failure for resume/inspection
                    None,
                    tools.todos(),
                );
                let _ = self
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
                // Do not delete workspace on cancel/timeout/max-turns so resume works.
                self.events.emit(HarnessEvent::RunFinished {
                    run_id: run_id.clone(),
                    success: false,
                    summary: summary.clone(),
                });
                return Err(e);
            }
        };

        self.governance
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
            .await?;

        // Keep workspace on park; only delete on successful non-park completion.
        if !keep_workspace && success && termination != RunTermination::Parked {
            let _ = self.workspace.cleanup(&ws);
        }

        self.events.emit(HarnessEvent::RunFinished {
            run_id: run_id.clone(),
            success,
            summary: final_summary.clone(),
        });

        Ok(RunResult {
            run_id,
            success,
            summary: final_summary,
            turns,
            workspace: ws.path,
            termination,
            park: park_info,
            prompt_id,
            usage,
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
