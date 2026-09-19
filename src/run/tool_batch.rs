//! Durable execution and reporting for one model-produced tool batch.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::checkpoint::{
    ApprovalPark, ParkedState, StagedToolExecution, StagedToolReport, ToolExecutionStatus,
};
use crate::events::HarnessEvent;
use crate::governance::{GovernanceError, RunHandle, now_unix_ms};
use crate::hooks::{self, HookEvent};
use crate::model::{ChatMessage, ModelTurn, TokenUsage, ToolCall, stable_tool_call_id};
use crate::tools::{self, ToolOutput, ToolRegistry};

use super::session::RunSession;
use super::supervision::check_bounds;
use super::{Engine, ParkInfo, ParkKind, RunError, RunRequest};

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
    replay: bool,
}

impl<'a> DurableToolBatch<'a> {
    pub(super) fn new(
        engine: &'a Engine,
        request: &'a RunRequest,
        started: tokio::time::Instant,
        timeout: Option<Duration>,
        handle: &'a RunHandle,
        tools: Arc<ToolRegistry>,
        replay: bool,
    ) -> Self {
        Self {
            engine,
            request,
            started,
            timeout,
            handle,
            tools,
            replay,
        }
    }

    /// Execute, checkpoint, and report a complete batch before returning.
    pub(super) async fn execute(
        &self,
        turn: &ModelTurn,
        session: &mut RunSession,
        pending_park: &mut Option<ParkedState>,
        usage: TokenUsage,
    ) -> Result<ToolBatchOutcome, RunError> {
        let request = self.request;
        let started = self.started;
        let timeout = self.timeout;
        let handle = self.handle;
        let tools = Arc::clone(&self.tools);
        if session.is_content() && turn.tool_calls.iter().any(|call| call.name == "escalate") {
            return Err(RunError::Governance(
                crate::governance::GovernanceError::Denied(
                    "bounded content runs do not support parking".into(),
                ),
            ));
        }
        if self.replay {
            for call in &turn.tool_calls {
                if crate::tools::replay_tool_authority(&call.name)
                    == crate::tools::ReplayToolAuthority::Denied
                {
                    return Err(RunError::Governance(
                        crate::governance::GovernanceError::Denied(format!(
                            "replay forbids effect-capable or unknown tool `{}`",
                            call.name
                        )),
                    ));
                }
            }
        }
        let exclusive = turn
            .tool_calls
            .iter()
            .any(|c| tools::must_be_exclusive_batch(&c.name));
        if exclusive && turn.tool_calls.len() != 1 {
            for (index, c) in turn.tool_calls.iter().enumerate() {
                let detail = "tool batch rejected: report/escalate must be the only call";
                let tool_call_id = if session.is_content() {
                    conversation_tool_call_id(c, session.turns, index)
                } else {
                    c.id.clone()
                };
                if session.is_content() {
                    session
                        .append_content_tool_text(tool_call_id.clone(), detail.into())
                        .await?;
                }
                session.messages.push(ChatMessage {
                    role: "tool".into(),
                    content: if session.is_content() {
                        String::new()
                    } else {
                        detail.into()
                    },
                    tool_call_id,
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
        for (index, call) in turn.tool_calls.iter().enumerate() {
            if already_answered(session, call, index) {
                continue;
            }
            let call_id = stable_tool_call_id(call, session.turns, index);
            session.spans.start_tool(&call_id);
            self.engine.emit(
                &session.run_id,
                HarnessEvent::ToolStart {
                    name: call.name.clone(),
                    args_json: projected_detail(session.is_content(), &call.args_json),
                    run_id: session.run_id.clone(),
                    turn: session.turns,
                    call_id,
                },
            );
        }

        // Parallel path only when every call is parallel-safe (reads/`web_fetch`),
        // no execution-checkpoint tools, and no pre/post_tool or on_park hooks.
        let batch_outcomes: Vec<(usize, ToolCall, Result<ToolOutput, String>)> = if can_parallel {
            check_bounds(self.engine, &session.run_id, request, started, timeout)?;
            let sem = Arc::new(Semaphore::new(concurrency));
            let mut set = JoinSet::new();
            let turn_number = session.turns;
            for (i, call) in turn.tool_calls.iter().cloned().enumerate() {
                if already_answered(session, &call, i) {
                    continue;
                }
                let tools = Arc::clone(&tools);
                let gov = Arc::clone(&self.engine.governance);
                let handle = handle.clone();
                let sem = Arc::clone(&sem);
                set.spawn(async move {
                    let _permit = sem.acquire().await.expect("semaphore");
                    let stable_call_id = stable_tool_call_id(&call, turn_number, i);
                    if let Err(e) = gov
                        .authorize_tool_with_id(
                            &handle,
                            &stable_call_id,
                            &call.name,
                            &call.args_json,
                        )
                        .await
                    {
                        if matches!(e, GovernanceError::RequireApproval { .. }) {
                            return (i, call, Err(format!("parallel batch cannot park: {e}")));
                        }
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
            raw
        } else {
            let mut out = Vec::with_capacity(turn.tool_calls.len());
            for (index, call) in turn.tool_calls.iter().enumerate() {
                if already_answered(session, call, index) {
                    continue;
                }
                check_bounds(self.engine, &session.run_id, request, started, timeout)?;
                let stable_call_id = stable_tool_call_id(call, session.turns, index);
                // Pre-tool hooks are an execution-authorization boundary, not
                // a durable projection. They must inspect the exact transient
                // arguments that authorization and the host tool will receive.
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
                    if self
                        .engine
                        .governance
                        .checkpoint_state(&session.run_id)
                        .and_then(|checkpoint| checkpoint.approval_park)
                        .is_some_and(|park| park.call_id == stable_call_id)
                    {
                        self.engine.governance.clear_approval_park(handle).await?;
                    }
                    out.push((index, call.clone(), Err(e)));
                    continue;
                }
                if self
                    .engine
                    .governance
                    .tool_requires_execution_checkpoint(&call.name)
                {
                    let durable_args = if session.is_content() {
                        session
                            .durable_content_tool_arguments(index)
                            .ok_or_else(|| {
                                RunError::Message(
                                    "durable content tool arguments are missing".into(),
                                )
                            })?
                    } else {
                        call.args_json.clone()
                    };
                    self.engine
                        .governance
                        .stage_tool_execution(
                            handle,
                            StagedToolExecution {
                                call_id: stable_call_id.clone(),
                                name: call.name.clone(),
                                args_json: durable_args,
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
                    if let GovernanceError::RequireApproval {
                        approval_id,
                        authorization_id,
                        request_digest,
                        expires_at_ms,
                        deadline_ms,
                        reason,
                    } = e
                    {
                        let emissions =
                            self.persist_completed_prefix(session, handle, &out).await?;
                        if !emissions.is_empty() {
                            let durable_args = if session.is_content() {
                                session
                                    .durable_content_tool_arguments(index)
                                    .ok_or_else(|| {
                                        RunError::Message(
                                            "durable content tool arguments are missing".into(),
                                        )
                                    })?
                            } else {
                                call.args_json.clone()
                            };
                            self.engine
                                .governance
                                .stage_tool_execution(
                                    handle,
                                    StagedToolExecution {
                                        call_id: stable_call_id.clone(),
                                        name: call.name.clone(),
                                        args_json: durable_args,
                                        status: ToolExecutionStatus::Authorizing,
                                    },
                                )
                                .await?;
                        }
                        return self
                            .park_for_approval(
                                session,
                                pending_park,
                                call,
                                index,
                                stable_call_id.clone(),
                                ApprovalPark {
                                    approval_id,
                                    call_id: stable_call_id,
                                    tool_name: call.name.clone(),
                                    authorization_id,
                                    request_digest,
                                    expires_at_ms,
                                    parked_at_ms: now_unix_ms(),
                                    deadline_ms,
                                },
                                reason,
                                emissions,
                            )
                            .await;
                    }
                    if self
                        .engine
                        .governance
                        .checkpoint_state(&session.run_id)
                        .and_then(|checkpoint| checkpoint.approval_park)
                        .is_some_and(|park| park.call_id == stable_call_id)
                    {
                        if matches!(e, GovernanceError::Denied(_)) {
                            self.engine.governance.clear_approval_park(handle).await?;
                        } else {
                            return Err(e.into());
                        }
                    }
                    out.push((index, call.clone(), Err(e.to_string())));
                    continue;
                }
                if self
                    .engine
                    .governance
                    .checkpoint_state(&session.run_id)
                    .and_then(|checkpoint| checkpoint.approval_park)
                    .is_some_and(|park| park.call_id == stable_call_id)
                {
                    self.engine.governance.clear_approval_park(handle).await?;
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
                    Ok(o) => out.push((index, call.clone(), Ok(o))),
                    Err(e) => out.push((index, call.clone(), Err(e.to_string()))),
                }
            }
            out
        };

        // The execution phase above completes every call in the
        // batch before this reporting phase starts. Stage the entire
        // batch first so a required governance error cannot leave
        // later host-side effects absent from the resume checkpoint.
        let mut content_terminal_report = None;
        for (index, call, outcome) in &batch_outcomes {
            let tool_call_id = conversation_tool_call_id(call, session.turns, *index);
            match outcome {
                Ok(ToolOutput::Text(text)) => {
                    if session.is_content() {
                        session
                            .append_content_tool_text(tool_call_id.clone(), text.clone())
                            .await?;
                    }
                    session.messages.push(ChatMessage {
                        role: "tool".into(),
                        content: if session.is_content() {
                            String::new()
                        } else {
                            text.clone()
                        },
                        tool_call_id,
                        tool_calls: vec![],
                    });
                }
                Ok(ToolOutput::Report(report)) => {
                    let detail = format!("report: {}", report.summary);
                    if session.is_content() {
                        session
                            .append_content_tool_text(tool_call_id.clone(), report.summary.clone())
                            .await?;
                        content_terminal_report = Some((report.success, report.summary.clone()));
                    }
                    session.messages.push(ChatMessage {
                        role: "tool".into(),
                        content: if session.is_content() {
                            String::new()
                        } else {
                            detail
                        },
                        tool_call_id,
                        tool_calls: vec![],
                    });
                }
                Ok(ToolOutput::Park(_)) => {}
                Err(detail) => {
                    if session.is_content() {
                        session
                            .append_content_tool_text(tool_call_id.clone(), detail.clone())
                            .await?;
                    }
                    session.messages.push(ChatMessage {
                        role: "tool".into(),
                        content: if session.is_content() {
                            String::new()
                        } else {
                            detail.clone()
                        },
                        tool_call_id,
                        tool_calls: vec![],
                    });
                }
            }
        }

        for (index, call, _) in &batch_outcomes {
            if self
                .engine
                .governance
                .tool_requires_execution_checkpoint(&call.name)
            {
                self.engine
                    .governance
                    .mark_tool_execution_complete(
                        handle,
                        &stable_tool_call_id(call, session.turns, *index),
                    )
                    .await?;
            }
        }
        let staged_reports = batch_outcomes
            .iter()
            .map(|(index, call, outcome)| StagedToolReport {
                call_id: stable_tool_call_id(call, session.turns, *index),
                name: call.name.clone(),
                ok: match outcome {
                    Ok(ToolOutput::Text(_)) => true,
                    Ok(ToolOutput::Report(report)) => report.success,
                    Ok(ToolOutput::Park(_)) | Err(_) => false,
                },
                detail: match outcome {
                    Ok(ToolOutput::Text(text)) => projected_detail(session.is_content(), text),
                    Ok(ToolOutput::Report(report)) => {
                        projected_detail(session.is_content(), &report.summary)
                    }
                    Ok(ToolOutput::Park(park)) => format!("parked: {}", park.reason),
                    Err(detail) => projected_detail(session.is_content(), detail),
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
        // A terminal marker is recoverable only when the same checkpoint also
        // contains the pending governance report. Preparation drains those
        // reports before transaction-level terminal recovery.
        if let Some((success, summary)) = content_terminal_report {
            session.mark_content_terminal(
                success,
                super::RunTermination::Completed,
                &summary,
                usage,
            )?;
        }
        session.save(tools.as_ref())?;

        let mut terminal_report = None;
        for (index, call, outcome) in batch_outcomes {
            let report_call_id = stable_tool_call_id(&call, session.turns, index);
            match outcome {
                Ok(ToolOutput::Text(text)) => {
                    let detail = projected_detail(session.is_content(), &text);
                    self.engine
                        .report_governance_tool_with_id(
                            handle,
                            &report_call_id,
                            &call.name,
                            true,
                            &detail,
                        )
                        .await?;
                    if call.name == "todo_write" {
                        let items = tools.todos();
                        self.engine.emit(
                            &session.run_id,
                            HarnessEvent::TodosUpdated {
                                summary: detail.chars().take(500).collect(),
                                item_count: items.len(),
                            },
                        );
                    }
                    session.spans.end_tool(&report_call_id, true);
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: call.name.clone(),
                            ok: true,
                            detail: detail.chars().take(500).collect(),
                            run_id: session.run_id.clone(),
                            turn: session.turns,
                            call_id: report_call_id.clone(),
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
                    let detail = projected_detail(session.is_content(), &report.summary);
                    self.engine
                        .report_governance_tool_with_id(
                            handle,
                            &report_call_id,
                            "report",
                            report.success,
                            &detail,
                        )
                        .await?;
                    session.spans.end_tool(&report_call_id, report.success);
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: "report".into(),
                            ok: report.success,
                            detail,
                            run_id: session.run_id.clone(),
                            turn: session.turns,
                            call_id: report_call_id.clone(),
                        },
                    );
                    terminal_report = Some((report.summary, report.success));
                }
                Ok(ToolOutput::Park(park)) => {
                    let detail = format!("parked: {}", park.reason);
                    let parked = ParkedState {
                        reason: park.reason.clone(),
                        question: park.question.clone(),
                        tool_call_id: conversation_tool_call_id(&call, session.turns, index),
                    };
                    let info = ParkInfo {
                        reason: park.reason.clone(),
                        question: park.question.clone(),
                        tool_call_id: conversation_tool_call_id(&call, session.turns, index),
                        kind: ParkKind::Escalate,
                        approval_id: None,
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
                    // Report first, then save. The checkpoint still
                    // carries the park and the exact pending event so
                    // resume can retry if the report failed.
                    session.save_recoverable(Some(parked), tools.as_ref())?;
                    report_result?;
                    session.spans.end_tool(&report_call_id, false);
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: "escalate".into(),
                            ok: false,
                            detail: detail.clone(),
                            run_id: session.run_id.clone(),
                            turn: session.turns,
                            call_id: report_call_id.clone(),
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
                    let reported_detail = projected_detail(session.is_content(), &detail);
                    self.engine
                        .report_governance_tool_with_id(
                            handle,
                            &report_call_id,
                            &call.name,
                            false,
                            &reported_detail,
                        )
                        .await?;
                    session.spans.end_tool(&report_call_id, false);
                    self.engine.emit(
                        &session.run_id,
                        HarnessEvent::ToolEnd {
                            name: call.name.clone(),
                            ok: false,
                            detail: reported_detail,
                            run_id: session.run_id.clone(),
                            turn: session.turns,
                            call_id: report_call_id.clone(),
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

    async fn persist_completed_prefix(
        &self,
        session: &mut RunSession,
        handle: &RunHandle,
        out: &[(usize, ToolCall, Result<ToolOutput, String>)],
    ) -> Result<Vec<(String, String, bool, String)>, RunError> {
        if out.is_empty() {
            return Ok(Vec::new());
        }
        let mut reports = Vec::with_capacity(out.len());
        let mut emissions = Vec::with_capacity(out.len());
        for (index, call, outcome) in out {
            let tool_call_id = conversation_tool_call_id(call, session.turns, *index);
            let report_call_id = stable_tool_call_id(call, session.turns, *index);
            let (ok, detail) = match outcome {
                Ok(ToolOutput::Text(text)) => {
                    if session.is_content() {
                        session
                            .append_content_tool_text(tool_call_id.clone(), text.clone())
                            .await?;
                    }
                    session.messages.push(ChatMessage {
                        role: "tool".into(),
                        content: if session.is_content() {
                            String::new()
                        } else {
                            text.clone()
                        },
                        tool_call_id,
                        tool_calls: vec![],
                    });
                    (true, projected_detail(session.is_content(), text))
                }
                Ok(ToolOutput::Report(report)) => {
                    let detail = format!("report: {}", report.summary);
                    if session.is_content() {
                        session
                            .append_content_tool_text(tool_call_id.clone(), report.summary.clone())
                            .await?;
                    }
                    session.messages.push(ChatMessage {
                        role: "tool".into(),
                        content: if session.is_content() {
                            String::new()
                        } else {
                            detail.clone()
                        },
                        tool_call_id,
                        tool_calls: vec![],
                    });
                    (
                        report.success,
                        projected_detail(session.is_content(), &report.summary),
                    )
                }
                Ok(ToolOutput::Park(_)) => continue,
                Err(detail) => {
                    if session.is_content() {
                        session
                            .append_content_tool_text(tool_call_id.clone(), detail.clone())
                            .await?;
                    }
                    session.messages.push(ChatMessage {
                        role: "tool".into(),
                        content: if session.is_content() {
                            String::new()
                        } else {
                            detail.clone()
                        },
                        tool_call_id,
                        tool_calls: vec![],
                    });
                    (false, projected_detail(session.is_content(), detail))
                }
            };
            if self
                .engine
                .governance
                .tool_requires_execution_checkpoint(&call.name)
            {
                self.engine
                    .governance
                    .mark_tool_execution_complete(handle, &report_call_id)
                    .await?;
            }
            reports.push(StagedToolReport {
                call_id: report_call_id.clone(),
                name: call.name.clone(),
                ok,
                detail: detail.clone(),
            });
            emissions.push((report_call_id, call.name.clone(), ok, detail));
        }
        self.engine
            .governance
            .stage_tool_reports(handle, reports)
            .await?;
        self.engine
            .governance
            .clear_staged_tool_executions(handle)
            .await?;
        Ok(emissions)
    }

    #[allow(clippy::too_many_arguments)]
    async fn park_for_approval(
        &self,
        session: &mut RunSession,
        pending_park: &mut Option<ParkedState>,
        call: &ToolCall,
        index: usize,
        report_call_id: String,
        park: ApprovalPark,
        reason: String,
        emissions: Vec<(String, String, bool, String)>,
    ) -> Result<ToolBatchOutcome, RunError> {
        if park.approval_id.trim().is_empty() {
            return Err(RunError::Governance(GovernanceError::Message(
                "require_approval is missing approval_id".into(),
            )));
        }
        let question = format!("approval {} is pending", park.approval_id);
        let tool_call_id = conversation_tool_call_id(call, session.turns, index);
        let parked = ParkedState {
            reason: reason.clone(),
            question: question.clone(),
            tool_call_id: tool_call_id.clone(),
        };
        let info = ParkInfo {
            reason: reason.clone(),
            question,
            tool_call_id,
            kind: ParkKind::Approval,
            approval_id: Some(park.approval_id.clone()),
        };
        self.engine
            .governance
            .record_approval_park(self.handle, park)
            .await?;
        *pending_park = Some(parked.clone());
        session.save_recoverable(Some(parked), self.tools.as_ref())?;
        for (prefix_call_id, name, ok, detail) in emissions {
            self.engine
                .report_governance_tool_with_id(self.handle, &prefix_call_id, &name, ok, &detail)
                .await?;
            if name == "todo_write" {
                let items = self.tools.todos();
                self.engine.emit(
                    &session.run_id,
                    HarnessEvent::TodosUpdated {
                        summary: detail.chars().take(500).collect(),
                        item_count: items.len(),
                    },
                );
            }
            session.spans.end_tool(&prefix_call_id, ok);
            self.engine.emit(
                &session.run_id,
                HarnessEvent::ToolEnd {
                    name: name.clone(),
                    ok,
                    detail,
                    run_id: session.run_id.clone(),
                    turn: session.turns,
                    call_id: prefix_call_id,
                },
            );
            let _ = hooks::run_hooks(
                &self.engine.config.hooks,
                HookEvent::PostTool,
                json!({
                    "run_id": session.run_id,
                    "tool": name,
                    "ok": ok,
                }),
            )
            .await;
        }
        session.spans.end_tool(&report_call_id, false);
        self.engine.emit(
            &session.run_id,
            HarnessEvent::ToolEnd {
                name: call.name.clone(),
                ok: false,
                detail: format!("parked: {reason}"),
                run_id: session.run_id.clone(),
                turn: session.turns,
                call_id: report_call_id,
            },
        );
        let _ = hooks::run_hooks(
            &self.engine.config.hooks,
            HookEvent::OnPark,
            json!({
                "run_id": session.run_id,
                "reason": reason,
                "question": info.question,
            }),
        )
        .await;
        Ok(ToolBatchOutcome::Parked {
            info,
            summary: reason,
        })
    }
}

fn already_answered(session: &RunSession, call: &ToolCall, index: usize) -> bool {
    let Some(assistant_idx) = session
        .messages
        .iter()
        .rposition(|message| message.role == "assistant")
    else {
        return false;
    };
    let results: Vec<&str> = session.messages[assistant_idx + 1..]
        .iter()
        .filter(|message| message.role == "tool")
        .map(|message| message.tool_call_id.as_str())
        .collect();
    tool_result_answers_call(
        &results,
        call,
        session.turns,
        index,
        &session.messages[assistant_idx].tool_calls,
    )
}

pub(super) fn conversation_tool_call_id(call: &ToolCall, turn: u32, index: usize) -> String {
    if call.id.is_empty() {
        format!("tool-{turn}-{index}")
    } else {
        call.id.clone()
    }
}

/// Match a persisted tool result to a batch call. Accepts the current
/// synthesized conversation id and legacy empty ids from older checkpoints.
pub(super) fn tool_result_answers_call(
    results: &[&str],
    call: &ToolCall,
    turn: u32,
    index: usize,
    batch: &[ToolCall],
) -> bool {
    let synthesized = conversation_tool_call_id(call, turn, index);
    if results
        .iter()
        .any(|tool_call_id| *tool_call_id == synthesized)
        && (call.id.is_empty() || batch_occurrences_before(batch, index, &call.id) == 0)
    {
        return true;
    }
    let earlier_same = batch_occurrences_before(batch, index, &call.id);
    let matching = results
        .iter()
        .filter(|tool_call_id| **tool_call_id == call.id)
        .count();
    matching > earlier_same
}

fn batch_occurrences_before(batch: &[ToolCall], index: usize, id: &str) -> usize {
    batch
        .get(..index)
        .unwrap_or(&[])
        .iter()
        .filter(|candidate| candidate.id == id)
        .count()
}

fn projected_detail(content_run: bool, detail: &str) -> String {
    crate::content::project_bounded_text(content_run, "bounded_content", detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_ids_qualify_model_ids_and_synthesize_missing_ones() {
        let named = ToolCall {
            id: "call-1".into(),
            name: "read_file".into(),
            args_json: "{}".into(),
        };
        let anonymous = ToolCall {
            id: String::new(),
            name: "read_file".into(),
            args_json: "{}".into(),
        };
        assert_eq!(stable_tool_call_id(&named, 2, 3), "tool-2-3-call-1");
        assert_eq!(stable_tool_call_id(&anonymous, 2, 3), "tool-2-3");
        assert_eq!(conversation_tool_call_id(&named, 2, 3), "call-1");
        assert_eq!(conversation_tool_call_id(&anonymous, 2, 3), "tool-2-3");
    }

    #[test]
    fn tool_result_match_accepts_synthesized_and_legacy_empty_ids() {
        let first = ToolCall {
            id: String::new(),
            name: "read_file".into(),
            args_json: "{}".into(),
        };
        let second = ToolCall {
            id: String::new(),
            name: "write_file".into(),
            args_json: "{}".into(),
        };
        let batch = [first.clone(), second.clone()];
        assert!(tool_result_answers_call(&[""], &first, 1, 0, &batch));
        assert!(!tool_result_answers_call(&[""], &second, 1, 1, &batch));
        assert!(tool_result_answers_call(
            &["tool-1-1"],
            &second,
            1,
            1,
            &batch
        ));
        assert!(tool_result_answers_call(
            &["read-1"],
            &ToolCall {
                id: "read-1".into(),
                name: "read_file".into(),
                args_json: "{}".into(),
            },
            1,
            0,
            &[]
        ));
        let reused = ToolCall {
            id: "write-1".into(),
            name: "write_file".into(),
            args_json: "{}".into(),
        };
        let reused_batch = [reused.clone(), reused.clone()];
        assert!(tool_result_answers_call(
            &["write-1"],
            &reused,
            1,
            0,
            &reused_batch
        ));
        assert!(!tool_result_answers_call(
            &["write-1"],
            &reused,
            1,
            1,
            &reused_batch
        ));
        assert!(tool_result_answers_call(
            &["write-1", "write-1"],
            &reused,
            1,
            1,
            &reused_batch
        ));
    }
}
