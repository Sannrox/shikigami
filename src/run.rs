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
use crate::model::{ChatMessage, ModelError, ModelPort};
use crate::tools::{ToolError, ToolExecutor, ToolOutput};
use crate::workspace::{MaterializedWorkspace, WorkspaceCleanup, WorkspaceError, WorkspacePort};

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
        };
        cp.save(&self.state_runs)?;
        Ok(())
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        let started = tokio::time::Instant::now();
        let timeout = request
            .timeout
            .or_else(|| self.config.run.timeout_secs.map(Duration::from_secs));

        let (run_id, mut messages, mut turns, ws, task, keep_workspace) = if let Some(resume_id) =
            &request.resume_run_id
        {
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
            )
        };

        let prompt_id = crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT);
        let handle = self
            .governance
            .begin_run(&run_id, &task, request.logical_operation_id.as_deref())
            .await?;

        self.events.emit(HarnessEvent::Prompt {
            prompt_id: prompt_id.clone(),
        });
        self.events.emit(HarnessEvent::Message {
            level: "info".into(),
            text: format!("workspace {}", ws.path.display()),
        });

        let enabled = self.config.tools.effective_enabled();
        let tools = ToolExecutor::new(
            &ws.path,
            enabled.clone(),
            self.config.tools.bash_timeout_secs,
        )?;
        let tool_defs = ToolExecutor::definitions_json(&enabled);

        let max_turns = self.config.run.max_turns;
        let mut final_summary = String::from("completed without report");
        let mut success = false;
        let mut termination = RunTermination::Completed;

        // Persist initial checkpoint so resume works mid-run.
        self.save_checkpoint(
            &run_id,
            &task,
            &messages,
            turns,
            &ws.path,
            keep_workspace,
            None,
        )?;

        // Ok(Some(park)) when escalated; Ok(None) when finished normally.
        let result: Result<Option<ParkInfo>, RunError> = async {
            loop {
                self.check_bounds(&request, started, timeout)?;

                if turns >= max_turns {
                    return Err(RunError::MaxTurns(max_turns));
                }
                self.events.emit(HarnessEvent::Status {
                    status: "planning".into(),
                });
                let turn = self
                    .governance
                    .plan_turn(
                        &handle,
                        SYSTEM_PROMPT,
                        &messages,
                        &tool_defs,
                        self.model.as_ref(),
                    )
                    .await?;
                turns += 1;
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
                    .any(|c| c.name == "report" || c.name == "escalate");
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
                    )?;
                    continue;
                }

                for call in &turn.tool_calls {
                    self.check_bounds(&request, started, timeout)?;

                    self.events.emit(HarnessEvent::ToolStart {
                        name: call.name.clone(),
                        args_json: call.args_json.clone(),
                    });
                    if let Err(e) = self
                        .governance
                        .authorize_tool(&handle, &call.name, &call.args_json)
                        .await
                    {
                        let detail = e.to_string();
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
                        continue;
                    }

                    match tools.execute(&call.name, &call.args_json).await {
                        Ok(ToolOutput::Text(text)) => {
                            let _ = self
                                .governance
                                .report_tool(&handle, &call.name, true, &text)
                                .await;
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
                            // No tool result yet — operator answer is injected on resume.
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
                                true, // always keep workspace while parked
                                Some(parked),
                            )?;
                            return Ok(Some(info));
                        }
                        Err(e) => {
                            let detail = e.to_string();
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
                    &run_id, &task, &messages, turns, &ws.path,
                    true, // keep workspace on failure for resume/inspection
                    None,
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
            })
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.run_id, run_id);
        assert!(result.workspace.join("partial.txt").is_file());
        assert_eq!(result.summary, "resumed ok");
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
