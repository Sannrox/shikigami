//! Run lifecycle engine.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::config::Config;
use crate::events::{EventSink, HarnessEvent};
use crate::governance::{GovernanceError, GovernancePort, RunOutcome};
use crate::model::{ChatMessage, ModelError, ModelPort};
use crate::tools::{ToolError, ToolExecutor, ToolOutput};
use crate::workspace::{WorkspaceError, WorkspacePort};

pub const SYSTEM_PROMPT: &str = include_str!("prompts/harness-v1.md");

/// How a run ended (success or not).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTermination {
    /// Completed with a report or natural model stop.
    Completed,
    /// Cooperative cancel observed at a turn boundary.
    Cancelled,
    /// Wall-clock deadline exceeded at a turn boundary.
    TimedOut,
    /// Hit `max_turns` without a terminal report.
    MaxTurns,
    /// Failed with an error (governance, tools, model, etc.).
    Failed,
}

#[derive(Debug, Clone)]
pub struct RunRequest {
    pub task: String,
    /// When true, keep workspace after success (default false for directory).
    pub keep_workspace: bool,
    /// Optional overall wall-clock deadline. Overrides config when set.
    pub timeout: Option<Duration>,
    /// Cooperative cancel flag; checked at turn boundaries.
    pub cancel: Option<watch::Receiver<bool>>,
}

impl RunRequest {
    pub fn new(task: impl Into<String>) -> Self {
        Self {
            task: task.into(),
            keep_workspace: false,
            timeout: None,
            cancel: None,
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

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        let run_id = Uuid::new_v4().to_string();
        let started = tokio::time::Instant::now();
        let timeout = request
            .timeout
            .or_else(|| self.config.run.timeout_secs.map(Duration::from_secs));

        self.events.emit(HarnessEvent::Status {
            status: "starting".into(),
        });

        let handle = self.governance.begin_run(&run_id, &request.task).await?;

        let ws = self.workspace.materialize(&run_id, &self.state_runs)?;
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

        let mut messages = vec![ChatMessage {
            role: "user".into(),
            content: request.task.clone(),
            tool_call_id: String::new(),
            tool_calls: vec![],
        }];

        let max_turns = self.config.run.max_turns;
        let mut turns = 0u32;
        let mut final_summary = String::from("completed without report");
        let mut success = false;
        let mut termination = RunTermination::Completed;

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
            }
            Ok(())
        }
        .await;

        let (success, final_summary, termination) = match result {
            Ok(()) => (success, final_summary, termination),
            Err(e) => {
                let summary = e.to_string();
                let _ = self
                    .governance
                    .complete_run(
                        &handle,
                        RunOutcome {
                            success: false,
                            summary: summary.clone(),
                        },
                    )
                    .await;
                if !request.keep_workspace {
                    let _ = self.workspace.cleanup(&ws);
                }
                self.events.emit(HarnessEvent::RunFinished {
                    run_id: run_id.clone(),
                    success: false,
                    summary: summary.clone(),
                });
                // Cancelled / timed out / max turns are structured outcomes, not
                // silent successes. Surface via Result::Err so CLI exits non-zero.
                return Err(e);
            }
        };

        self.governance
            .complete_run(
                &handle,
                RunOutcome {
                    success,
                    summary: final_summary.clone(),
                },
            )
            .await?;

        if !request.keep_workspace && success {
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
    use tokio::sync::watch;

    fn engine(config: Config, state: &StateRoot) -> Engine {
        state.ensure_ready_for_runs().unwrap();
        Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::from(crate::model::from_config(&config).unwrap()),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
        }
    }

    #[tokio::test]
    async fn cancel_before_first_turn_errors() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        // Never-ending script would hang without cancel.
        config.model.adapter = "scripted".into();
        config.model.script_json = Some(
            r#"[{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"x\",\"success\":true}"}]}]"#
                .into(),
        );

        let (tx, rx) = watch::channel(true);
        let _keep = tx;
        let eng = engine(config, &state);
        let err = eng
            .run(RunRequest {
                task: "t".into(),
                keep_workspace: true,
                timeout: None,
                cancel: Some(rx),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RunError::Cancelled));
        assert_eq!(err.termination(), RunTermination::Cancelled);
    }

    #[tokio::test]
    async fn timeout_zero_errors_at_boundary() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "none".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        config.model.adapter = "scripted".into();

        let eng = engine(config, &state);
        let err = eng
            .run(RunRequest {
                task: "t".into(),
                keep_workspace: true,
                timeout: Some(Duration::from_secs(0)),
                cancel: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RunError::TimedOut(_)));
    }

    #[tokio::test]
    async fn completed_run_reports_termination() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        config.model.adapter = "scripted".into();

        let eng = engine(config, &state);
        let result = eng
            .run(RunRequest {
                task: "demo".into(),
                keep_workspace: true,
                timeout: Some(Duration::from_secs(30)),
                cancel: None,
            })
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.termination, RunTermination::Completed);
    }

    #[tokio::test]
    async fn max_turns_errors() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "none".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        config.run.max_turns = 1;
        // Script keeps calling write_file forever-ish: one write then no report
        // force max turns: two turns of write without report with max 1 fails on second plan
        let model = ScriptedModel::from_turns(vec![
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "1".into(),
                    name: "write_file".into(),
                    args_json: r#"{"path":"a.txt","content":"x"}"#.into(),
                }],
            },
            ModelTurn {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "2".into(),
                    name: "write_file".into(),
                    args_json: r#"{"path":"b.txt","content":"y"}"#.into(),
                }],
            },
        ]);
        state.ensure_ready_for_runs().unwrap();
        let eng = Engine {
            governance: Arc::from(governance::from_config(&config).unwrap()),
            workspace: Arc::from(workspace::from_config(&config).unwrap()),
            model: Arc::new(model),
            events: Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            config,
            state_runs: state.runs_dir(),
        };
        let err = eng
            .run(RunRequest {
                task: "t".into(),
                keep_workspace: true,
                timeout: None,
                cancel: None,
            })
            .await
            .unwrap_err();
        // After first turn, turns=1 and max_turns=1 so second iteration hits MaxTurns
        assert!(matches!(err, RunError::MaxTurns(1)));
    }
}
