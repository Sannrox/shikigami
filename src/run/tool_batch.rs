//! Durable execution and reporting for one model-produced tool batch.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::checkpoint::{ParkedState, StagedToolExecution, StagedToolReport, ToolExecutionStatus};
use crate::events::HarnessEvent;
use crate::governance::RunHandle;
use crate::hooks::{self, HookEvent};
use crate::model::{ChatMessage, ModelTurn, ToolCall};
use crate::tools::{self, ToolOutput, ToolRegistry};

use super::session::RunSession;
use super::{Engine, ParkInfo, RunError, RunRequest};

pub(super) enum ToolBatchOutcome {
    Continue,
    Completed { summary: String, success: bool },
    Parked { info: ParkInfo, summary: String },
}

/// Deep private module that owns the durable protocol around a tool batch.
pub(super) struct DurableToolBatch<'a> {
    engine: &'a Engine,
    request: &'a RunRequest,
    started: tokio::time::Instant,
    timeout: Option<Duration>,
    handle: &'a RunHandle,
    tools: Arc<ToolRegistry>,
}

impl<'a> DurableToolBatch<'a> {
    pub(super) fn new(
        engine: &'a Engine,
        request: &'a RunRequest,
        started: tokio::time::Instant,
        timeout: Option<Duration>,
        handle: &'a RunHandle,
        tools: Arc<ToolRegistry>,
    ) -> Self {
        Self {
            engine,
            request,
            started,
            timeout,
            handle,
            tools,
        }
    }

    /// Execute, checkpoint, and report a complete batch before returning.
    pub(super) async fn execute(
        &self,
        turn: &ModelTurn,
        session: &mut RunSession,
        pending_park: &mut Option<ParkedState>,
    ) -> Result<ToolBatchOutcome, RunError> {
        let request = self.request;
        let started = self.started;
        let timeout = self.timeout;
        let handle = self.handle;
        let tools = Arc::clone(&self.tools);
        let exclusive = turn
            .tool_calls
            .iter()
            .any(|c| tools::must_be_exclusive_batch(&c.name));
        if exclusive && turn.tool_calls.len() != 1 {
            for c in &turn.tool_calls {
                session.messages.push(ChatMessage {
                    role: "tool".into(),
                    content: "tool batch rejected: report/escalate must be the only call".into(),
                    tool_call_id: c.id.clone(),
                    tool_calls: vec![],
                });
            }
            session.save(tools.as_ref())?;
            return Ok(ToolBatchOutcome::Continue);
        }

        let concurrency = self.engine.config.run.tool_concurrency.max(1) as usize;
        let hooks_need_serial = self
            .engine
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
            && turn.tool_calls.iter().all(|c| {
                !self
                    .engine
                    .governance
                    .tool_requires_execution_checkpoint(&c.name)
            });

        // Ordered ToolStart for stable live streams.
        for call in &turn.tool_calls {
            self.engine.emit(
                &session.run_id,
                HarnessEvent::ToolStart {
                    name: call.name.clone(),
                    args_json: call.args_json.clone(),
                },
            );
        }

        // Parallel path only for all-read batches (no report/park/write).
        let batch_outcomes: Vec<(ToolCall, Result<ToolOutput, String>)> = if can_parallel {
            self.engine
                .check_bounds(&session.run_id, request, started, timeout)?;
            let sem = Arc::new(Semaphore::new(concurrency));
            let mut set = JoinSet::new();
            let turn_number = session.turns;
            for (i, call) in turn.tool_calls.iter().cloned().enumerate() {
                let tools = Arc::clone(&tools);
                let gov = Arc::clone(&self.engine.governance);
                let handle = handle.clone();
                let sem = Arc::clone(&sem);
                set.spawn(async move {
                    let _permit = sem.acquire().await.expect("semaphore");
                    let stable_call_id = Engine::stable_tool_call_id(&call, turn_number, i);
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
                self.engine
                    .check_bounds(&session.run_id, request, started, timeout)?;
                if let Err(e) = hooks::run_hooks(
                    &self.engine.config.hooks,
                    HookEvent::PreTool,
                    json!({
                        "run_id": session.run_id,
                        "tool": call.name,
                        "args_json": call.args_json,
                    }),
                )
                .await
                {
                    out.push((call.clone(), Err(e)));
                    continue;
                }
                let stable_call_id = Engine::stable_tool_call_id(call, session.turns, index);
                if self
                    .engine
                    .governance
                    .tool_requires_execution_checkpoint(&call.name)
                {
                    self.engine
                        .governance
                        .stage_tool_execution(
                            handle,
                            StagedToolExecution {
                                call_id: stable_call_id.clone(),
                                name: call.name.clone(),
                                args_json: call.args_json.clone(),
                                status: ToolExecutionStatus::Authorizing,
                            },
                        )
                        .await?;
                    session.save(tools.as_ref())?;
                }
                if let Err(e) = self
                    .engine
                    .governance
                    .authorize_tool_with_id(handle, &stable_call_id, &call.name, &call.args_json)
                    .await
                {
                    out.push((call.clone(), Err(e.to_string())));
                    continue;
                }
                if self
                    .engine
                    .governance
                    .tool_requires_execution_checkpoint(&call.name)
                {
                    self.engine
                        .governance
                        .mark_tool_execution_started(handle, &stable_call_id)
                        .await?;
                    session.save(tools.as_ref())?;
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
                Ok(ToolOutput::Text(text)) => session.messages.push(ChatMessage {
                    role: "tool".into(),
                    content: text.clone(),
                    tool_call_id: call.id.clone(),
                    tool_calls: vec![],
                }),
                Ok(ToolOutput::Report(report)) => session.messages.push(ChatMessage {
                    role: "tool".into(),
                    content: format!("report: {}", report.summary),
                    tool_call_id: call.id.clone(),
                    tool_calls: vec![],
                }),
                Ok(ToolOutput::Park(_)) => {}
                Err(detail) => session.messages.push(ChatMessage {
                    role: "tool".into(),
                    content: detail.clone(),
                    tool_call_id: call.id.clone(),
                    tool_calls: vec![],
                }),
            }
        }

        for (index, (call, _)) in batch_outcomes.iter().enumerate() {
            if self
                .engine
                .governance
                .tool_requires_execution_checkpoint(&call.name)
            {
                self.engine
                    .governance
                    .mark_tool_execution_complete(
                        handle,
                        &Engine::stable_tool_call_id(call, session.turns, index),
                    )
                    .await?;
            }
        }
        let staged_reports = batch_outcomes
            .iter()
            .enumerate()
            .map(|(index, (call, outcome))| StagedToolReport {
                call_id: Engine::stable_tool_call_id(call, session.turns, index),
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
        self.engine
            .governance
            .stage_tool_reports(handle, staged_reports)
            .await?;
        // The completed execution markers are no longer needed once
        // the report intents are staged in memory. Clear them before
        // the single checkpoint that makes those replayable reports
        // durable; a saved checkpoint never contains both a completed
        // effect marker and a safe report replay queue.
        self.engine
            .governance
            .clear_staged_tool_executions(handle)
            .await?;
        session.save(tools.as_ref())?;

        let mut terminal_report = None;
        for (index, (call, outcome)) in batch_outcomes.into_iter().enumerate() {
            let report_call_id = Engine::stable_tool_call_id(&call, session.turns, index);
            match outcome {
                Ok(ToolOutput::Text(text)) => {
                    self.engine
                        .report_governance_tool_with_id(
                            handle,
                            &report_call_id,
                            &call.name,
                            true,
                            &text,
                        )
                        .await?;
                    if call.name == "todo_write" {
                        let items = tools.todos();
                        self.engine.emit(
                            &session.run_id,
                            HarnessEvent::TodosUpdated {
                                summary: text.chars().take(500).collect(),
                                item_count: items.len(),
                            },
                        );
                    }
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: call.name.clone(),
                            ok: true,
                            detail: text.chars().take(500).collect(),
                        },
                    );
                    let _ = hooks::run_hooks(
                        &self.engine.config.hooks,
                        HookEvent::PostTool,
                        json!({
                            "run_id": session.run_id,
                            "tool": call.name,
                            "ok": true,
                        }),
                    )
                    .await;
                }
                Ok(ToolOutput::Report(report)) => {
                    self.engine
                        .report_governance_tool_with_id(
                            handle,
                            &report_call_id,
                            "report",
                            report.success,
                            &report.summary,
                        )
                        .await?;
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: "report".into(),
                            ok: report.success,
                            detail: report.summary.clone(),
                        },
                    );
                    terminal_report = Some((report.summary, report.success));
                }
                Ok(ToolOutput::Park(park)) => {
                    let detail = format!("parked: {}", park.reason);
                    let parked = ParkedState {
                        reason: park.reason.clone(),
                        question: park.question.clone(),
                        tool_call_id: Engine::conversation_tool_call_id(
                            &call,
                            session.turns,
                            index,
                        ),
                    };
                    let info = ParkInfo {
                        reason: park.reason.clone(),
                        question: park.question.clone(),
                        tool_call_id: Engine::conversation_tool_call_id(
                            &call,
                            session.turns,
                            index,
                        ),
                    };
                    *pending_park = Some(parked.clone());
                    let report_result = self
                        .engine
                        .report_governance_tool_with_id(
                            handle,
                            &report_call_id,
                            "escalate",
                            false,
                            &detail,
                        )
                        .await;
                    // Save before reporting so a resume checkpoint
                    // carries the park state and the exact pending
                    // event for retry if the report fails.
                    session.save_recoverable(Some(parked), tools.as_ref())?;
                    report_result?;
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: "escalate".into(),
                            ok: false,
                            detail: detail.clone(),
                        },
                    );
                    let _ = hooks::run_hooks(
                        &self.engine.config.hooks,
                        HookEvent::OnPark,
                        json!({
                            "run_id": session.run_id,
                            "reason": park.reason,
                            "question": park.question,
                        }),
                    )
                    .await;
                    return Ok(ToolBatchOutcome::Parked {
                        info,
                        summary: park.reason,
                    });
                }
                Err(detail) => {
                    self.engine
                        .report_governance_tool_with_id(
                            handle,
                            &report_call_id,
                            &call.name,
                            false,
                            &detail,
                        )
                        .await?;
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: call.name.clone(),
                            ok: false,
                            detail: detail.clone(),
                        },
                    );
                    let _ = hooks::run_hooks(
                        &self.engine.config.hooks,
                        HookEvent::PostTool,
                        json!({
                            "run_id": session.run_id,
                            "tool": call.name,
                            "ok": false,
                        }),
                    )
                    .await;
                }
            }
            session.save(tools.as_ref())?;
        }
        session.save(tools.as_ref())?;
        match terminal_report {
            Some((summary, success)) => Ok(ToolBatchOutcome::Completed { summary, success }),
            None => Ok(ToolBatchOutcome::Continue),
        }
    }
}
