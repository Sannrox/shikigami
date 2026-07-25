//! Run lifecycle engine.

use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;
use uuid::Uuid;

use crate::config::Config;
use crate::events::{EventSink, HarnessEvent};
use crate::governance::{GovernanceError, GovernancePort, RunOutcome};
use crate::model::{ChatMessage, ModelError, ModelPort};
use crate::tools::{ToolError, ToolExecutor, ToolOutput};
use crate::workspace::{WorkspaceError, WorkspacePort};

pub const SYSTEM_PROMPT: &str = include_str!("prompts/harness-v1.md");

#[derive(Debug, Clone)]
pub struct RunRequest {
    pub task: String,
    /// When true, keep workspace after success (default false for directory).
    pub keep_workspace: bool,
}

#[derive(Debug, Clone)]
pub struct RunResult {
    pub run_id: String,
    pub success: bool,
    pub summary: String,
    pub turns: u32,
    pub workspace: PathBuf,
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
    pub async fn run(&self, request: RunRequest) -> Result<RunResult, RunError> {
        let run_id = Uuid::new_v4().to_string();
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

        let result = async {
            loop {
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

                messages.push(ChatMessage {
                    role: "assistant".into(),
                    content: turn.content.clone(),
                    tool_call_id: String::new(),
                    tool_calls: turn.tool_calls.clone(),
                });

                if turn.tool_calls.is_empty() {
                    // No tools: treat as natural completion.
                    final_summary = if turn.content.is_empty() {
                        "model finished without tools".into()
                    } else {
                        turn.content
                    };
                    success = true;
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

        let (success, final_summary) = match result {
            Ok(()) => (success, final_summary),
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
            // Keep failed workspaces for inspection; clean successful sandboxes.
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
        })
    }
}
