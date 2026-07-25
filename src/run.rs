//! Run lifecycle engine.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::checkpoint::{self, Checkpoint, CheckpointError};
use crate::config::Config;
use crate::events::{EventSink, HarnessEvent};
use crate::governance::{GovernanceError, GovernancePort, RunOutcome};
use crate::model::{ChatMessage, ModelError, ModelPort};
use crate::tools::{ToolError, ToolExecutor, ToolOutput};
use crate::workspace::{MaterializedWorkspace, WorkspaceCleanup, WorkspaceError, WorkspacePort};

pub const SYSTEM_PROMPT: &str = include_str!("prompts/harness-v1.md");

/// How a run ended (success or not).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTermination {
    Completed,
    Cancelled,
    TimedOut,
    MaxTurns,
    Failed,
}

impl RunTermination {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::MaxTurns => "max_turns",
            Self::Failed => "failed",
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
}

impl RunRequest {
    pub fn new(task: impl Into<String>) -> Self {
        Self {
            task: task.into(),
            keep_workspace: false,
            timeout: None,
            cancel: None,
            resume_run_id: None,
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

    fn save_checkpoint(
        &self,
        run_id: &str,
        task: &str,
        messages: &[ChatMessage],
        turns: u32,
        workspace: &Path,
        keep_workspace: bool,
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
        };
        cp.save(&self.state_runs)?;
        Ok(())
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        let started = tokio::time::Instant::now();
        let timeout = request
            .timeout
            .or_else(|| self.config.run.timeout_secs.map(Duration::from_secs));

        let (run_id, mut messages, mut turns, ws, task, keep_workspace) =
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
                (
                    cp.run_id,
                    cp.messages,
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

        let handle = self.governance.begin_run(&run_id, &task).await?;

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
        self.save_checkpoint(&run_id, &task, &messages, turns, &ws.path, keep_workspace)?;

        let result = async {
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

                self.save_checkpoint(&run_id, &task, &messages, turns, &ws.path, keep_workspace)?;

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

                let has_report = turn.tool_calls.iter().any(|c| c.name == "report");
                if has_report && turn.tool_calls.len() != 1 {
                    for c in &turn.tool_calls {
                        messages.push(ChatMessage {
                            role: "tool".into(),
                            content: "tool batch rejected: report must be the only call".into(),
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
                            )?;
                            return Ok(());
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
                self.save_checkpoint(&run_id, &task, &messages, turns, &ws.path, keep_workspace)?;
            }
            Ok(())
        }
        .await;

        let (success, final_summary, termination) = match result {
            Ok(()) => (success, final_summary, termination),
            Err(e) => {
                let summary = e.to_string();
                let _ = self.save_checkpoint(
                    &run_id, &task, &messages, turns, &ws.path,
                    true, // keep workspace on failure for resume/inspection
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

        if !keep_workspace && success {
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
            })
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.run_id, run_id);
        assert!(result.workspace.join("partial.txt").is_file());
        assert_eq!(result.summary, "resumed ok");
    }
}
