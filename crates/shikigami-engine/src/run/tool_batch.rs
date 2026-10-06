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
use crate::governance::{
    GovernanceError, RunHandle, bind_parked_call, now_unix_ms, park_arguments_digest,
};
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

fn plan_jail_deny(call: &ToolCall, workspace: &std::path::Path) -> Option<String> {
    if !tools::plan_jail_allows(&call.name, &call.args_json) {
        if call.name == "child_run" {
            return Some("plan jail: child_run worktree=true is denied".into());
        }
        return Some(format!(
            "plan jail: mutating tools may only write `{}`",
            tools::PLAN_JAIL_PATH
        ));
    }
    if tools::mutates_workspace(&call.name) && !tools::plan_jail_destination_ok(workspace) {
        return Some(format!(
            "plan jail: `{}` must be a regular workspace file",
            tools::PLAN_JAIL_PATH
        ));
    }
    None
}

/// Isolated `wait=false` `child_run`s may share one start-wait under
/// `nested_max_children`. Workspace-mutating tools, `wait=true`, and
/// `worktree=true` keep the whole batch serial. Parallel-safe reads may
/// share the isolated batch through the serial checkpoint protocol.
/// Reserved children launch (one parent save) before a later read runs.
fn batch_can_concurrent_child_starts(
    calls: &[ToolCall],
    session_wait: bool,
    plan_jail: bool,
    session_asks: bool,
    hooks_need_serial: bool,
) -> bool {
    if hooks_need_serial || calls.len() <= 1 {
        return false;
    }
    let mut any_wait_false = false;
    for call in calls {
        if call.name == "child_run"
            && super::nested::child_run_allows_concurrent_start(&call.args_json)
        {
            any_wait_false = true;
            if session_wait
                && !plan_jail
                && session_asks
                && super::nested::session_ask_park(&call.name, &call.args_json)
            {
                return false;
            }
            continue;
        }
        if tools::is_parallel_safe_tool(&call.name) {
            continue;
        }
        return false;
    }
    any_wait_false
}

#[allow(clippy::too_many_arguments)]
async fn flush_pending_background_children(
    engine: &super::Engine,
    session: &mut RunSession,
    request: &RunRequest,
    tools: &ToolRegistry,
    pending: &mut Vec<(usize, ToolCall, super::nested::QueuedBackgroundChild)>,
    out: &mut Vec<(usize, ToolCall, Result<ToolOutput, String>)>,
    started: tokio::time::Instant,
    timeout: Option<Duration>,
) -> Result<(), RunError> {
    if pending.is_empty() {
        return Ok(());
    }
    let waited = super::nested::wait_for_queued_background_children(
        engine,
        session,
        request,
        tools,
        std::mem::take(pending),
        started,
        timeout,
    )
    .await?;
    out.extend(waited);
    out.sort_by_key(|(i, _, _)| *i);
    Ok(())
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
            })
            && !(request.session_wait
                && turn
                    .tool_calls
                    .iter()
                    .any(|c| tools::mutates_workspace(&c.name)));

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

        // Parallel JoinSet path only when every call is parallel-safe
        // (reads/`web_fetch`). `child_run` is not globally parallel-safe:
        // wait=true stays serial, and a mutating tool in the same batch
        // keeps children off JoinSet. Isolated wait=false starts share one
        // nested start-wait under `nested_max_children` on the serial path.
        let concurrent_child_starts = batch_can_concurrent_child_starts(
            &turn.tool_calls,
            request.session_wait,
            session.plan_jail,
            self.engine.governance.session_asks_mutating_tools(),
            hooks_need_serial,
        );
        let batch_outcomes: Vec<(usize, ToolCall, Result<ToolOutput, String>)> = if can_parallel {
            check_bounds(
                self.engine,
                &session.run_id,
                request,
                started,
                timeout,
                &session.parent_run_id,
            )?;
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
                    if !tools.is_enabled(&call.name) {
                        // Same pre-authorize gate as the serial path: a
                        // read-only explore child must not redeem a
                        // parent/plane permit for parallel-safe tools
                        // outside its allow-list (`web_fetch`).
                        let err = format!("tool not enabled: {}", call.name);
                        return (i, call, Err(err));
                    }
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
            let nested_sem = concurrent_child_starts.then(|| {
                let remaining = self
                    .engine
                    .config
                    .run
                    .nested_max_children
                    .max(1)
                    .saturating_sub(session.children.len() as u32);
                Semaphore::new(remaining as usize)
            });
            let mut pending_background: Vec<(
                usize,
                ToolCall,
                super::nested::QueuedBackgroundChild,
            )> = Vec::new();
            let mut out = Vec::with_capacity(turn.tool_calls.len());
            let batch = async {
                for (index, call) in turn.tool_calls.iter().enumerate() {
                    if already_answered(session, call, index) {
                        continue;
                    }
                    if let Err(error) = check_bounds(
                        self.engine,
                        &session.run_id,
                        request,
                        started,
                        timeout,
                        &session.parent_run_id,
                    ) {
                        flush_pending_background_children(
                            self.engine,
                            session,
                            request,
                            tools.as_ref(),
                            &mut pending_background,
                            &mut out,
                            started,
                            timeout,
                        )
                        .await?;
                        return Err(error);
                    }
                    if !tools.is_enabled(&call.name) {
                        // Deny before authorize so a read-only explore child
                        // (or a parent with nested off) cannot redeem a
                        // parent/plane permit for `child_run` / writes.
                        out.push((
                            index,
                            call.clone(),
                            Err(format!("tool not enabled: {}", call.name)),
                        ));
                        continue;
                    }
                    let nested = matches!(call.name.as_str(), "child_run" | "child_status");
                    let stable_call_id = stable_tool_call_id(call, session.turns, index);
                    let conversation_id = conversation_tool_call_id(call, session.turns, index);
                    let ask_granted = session
                        .ask_allow_call_id()
                        .is_some_and(|token| token == stable_call_id || token == conversation_id);
                    if session.plan_jail
                        && let Some(denied) = plan_jail_deny(call, &session.workspace)
                    {
                        out.push((index, call.clone(), Err(denied)));
                        continue;
                    }
                    if request.session_wait
                        && !session.plan_jail
                        && super::nested::session_ask_park(&call.name, &call.args_json)
                        && self.engine.governance.session_asks_mutating_tools()
                        && !ask_granted
                    {
                        flush_pending_background_children(
                            self.engine,
                            session,
                            request,
                            tools.as_ref(),
                            &mut pending_background,
                            &mut out,
                            started,
                            timeout,
                        )
                        .await?;
                        let emissions =
                            self.persist_completed_prefix(session, handle, &out).await?;
                        return Ok(Some(
                            self.park_for_ask(
                                session,
                                pending_park,
                                call,
                                index,
                                conversation_id,
                                emissions,
                            )
                            .await?,
                        ));
                    }
                    // One-shot grant: consume when this approved call is attempted,
                    // including hook and authorization rejection paths.
                    if ask_granted {
                        session.set_ask_allow_call_id(None);
                        session.clear_resumed_ask_park();
                    }
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
                    let approval_park = self
                        .engine
                        .governance
                        .checkpoint_state(&session.run_id)
                        .and_then(|checkpoint| checkpoint.approval_park)
                        .filter(|park| park.call_id == stable_call_id);
                    let parked_call_bound = approval_park.as_ref().map_or(Ok(()), |park| {
                        bind_parked_call(park, &call.name, &call.args_json)
                    });
                    let requires_claim = self
                        .engine
                        .governance
                        .tool_requires_execution_checkpoint(&call.name);
                    if parked_call_bound.is_ok() && approval_park.is_some() && requires_claim {
                        // Parked resume: if the exclusive claim already exists,
                        // refuse after bind and before Authorizing save or plane
                        // redeem. Do not take a new claim yet — authorize may
                        // still return pending. The permit path claims below.
                        if self
                            .engine
                            .registry
                            .tool_execution_is_claimed(&session.run_id, &stable_call_id)
                            .unwrap_or(true)
                        {
                            flush_pending_background_children(
                                self.engine,
                                session,
                                request,
                                tools.as_ref(),
                                &mut pending_background,
                                &mut out,
                                started,
                                timeout,
                            )
                            .await?;
                            return Err(GovernanceError::Message(format!(
                            "host effect refused, not claimed exclusively: tool call `{stable_call_id}` in run {} is already claimed by another execution attempt",
                            session.run_id
                        ))
                        .into());
                        }
                    }
                    if parked_call_bound.is_ok() && requires_claim {
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
                    let authorization = match parked_call_bound {
                        Ok(()) => {
                            self.engine
                                .governance
                                .authorize_tool_with_id(
                                    handle,
                                    &stable_call_id,
                                    &call.name,
                                    &call.args_json,
                                )
                                .await
                        }
                        Err(error) => Err(error),
                    };
                    if let Err(e) = authorization {
                        if let GovernanceError::RequireApproval {
                            approval_id,
                            authorization_id,
                            request_digest,
                            expires_at_ms,
                            deadline_ms,
                            reason,
                        } = e
                        {
                            // Wait for already-queued wait=false starts so their
                            // tool results are in `out` before this park. Resume
                            // must see those calls as answered.
                            flush_pending_background_children(
                                self.engine,
                                session,
                                request,
                                tools.as_ref(),
                                &mut pending_background,
                                &mut out,
                                started,
                                timeout,
                            )
                            .await?;
                            let emissions =
                                self.persist_completed_prefix(session, handle, &out).await?;
                            if !emissions.is_empty() {
                                let durable_args = if session.is_content() {
                                    session.durable_content_tool_arguments(index).ok_or_else(
                                        || {
                                            RunError::Message(
                                                "durable content tool arguments are missing".into(),
                                            )
                                        },
                                    )?
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
                            return Ok(Some(
                                self.park_for_approval(
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
                                        arguments_digest: park_arguments_digest(&call.args_json),
                                        expires_at_ms,
                                        parked_at_ms: now_unix_ms(),
                                        deadline_ms,
                                    },
                                    reason,
                                    emissions,
                                )
                                .await?,
                            ));
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
                    if requires_claim {
                        self.engine
                            .registry
                            .claim_tool_execution(&session.run_id, &stable_call_id)
                            .map_err(|error| {
                                GovernanceError::Message(format!(
                                    "host effect refused, not claimed exclusively: {error}"
                                ))
                            })?;
                        self.engine
                            .governance
                            .mark_tool_execution_started(handle, &stable_call_id)
                            .await?;
                        session.save(tools.as_ref())?;
                    }
                    // Clear the park only after the exclusive claim (when required)
                    // and the durable `Started` save, so a crash in this window
                    // keeps the parked approval.
                    if self
                        .engine
                        .governance
                        .checkpoint_state(&session.run_id)
                        .and_then(|checkpoint| checkpoint.approval_park)
                        .is_some_and(|park| park.call_id == stable_call_id)
                    {
                        self.engine.governance.clear_approval_park(handle).await?;
                    }
                    let executed = if nested {
                        if let Some(sem) = nested_sem.as_ref()
                            && call.name == "child_run"
                            && super::nested::child_run_allows_concurrent_start(&call.args_json)
                        {
                            let permit = match sem.try_acquire() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    let max_children =
                                        self.engine.config.run.nested_max_children.max(1);
                                    out.push((
                                    index,
                                    call.clone(),
                                    Err(format!(
                                        "nested fan-out cap ({max_children}) refuses another child_run"
                                    )),
                                ));
                                    continue;
                                }
                            };
                            match super::nested::queue_wait_false_child_run(
                                self.engine,
                                session,
                                request,
                                call,
                            ) {
                                Ok(queued) => {
                                    permit.forget();
                                    pending_background.push((index, call.clone(), queued));
                                    continue;
                                }
                                Err(error)
                                    if matches!(
                                        error,
                                        RunError::Cancelled | RunError::TimedOut(_)
                                    ) =>
                                {
                                    flush_pending_background_children(
                                        self.engine,
                                        session,
                                        request,
                                        tools.as_ref(),
                                        &mut pending_background,
                                        &mut out,
                                        started,
                                        timeout,
                                    )
                                    .await?;
                                    return Err(error);
                                }
                                Err(error) => Err(error.to_string()),
                            }
                        } else {
                            flush_pending_background_children(
                                self.engine,
                                session,
                                request,
                                tools.as_ref(),
                                &mut pending_background,
                                &mut out,
                                started,
                                timeout,
                            )
                            .await?;
                            match super::nested::execute_child_tool(
                                self.engine,
                                session,
                                request,
                                tools.as_ref(),
                                call,
                                started,
                                timeout,
                            )
                            .await
                            {
                                Ok(output) => Ok(output),
                                Err(error)
                                    if matches!(
                                        error,
                                        RunError::Cancelled | RunError::TimedOut(_)
                                    ) =>
                                {
                                    flush_pending_background_children(
                                        self.engine,
                                        session,
                                        request,
                                        tools.as_ref(),
                                        &mut pending_background,
                                        &mut out,
                                        started,
                                        timeout,
                                    )
                                    .await?;
                                    return Err(error);
                                }
                                Err(error) => Err(error.to_string()),
                            }
                        }
                    } else {
                        flush_pending_background_children(
                            self.engine,
                            session,
                            request,
                            tools.as_ref(),
                            &mut pending_background,
                            &mut out,
                            started,
                            timeout,
                        )
                        .await?;
                        match super::supervision::run_until_cancelled(
                            self.engine,
                            &session.run_id,
                            request,
                            started,
                            timeout,
                            &session.parent_run_id,
                            tools.execute(&call.name, &call.args_json),
                        )
                        .await
                        {
                            Ok(Ok(output)) => Ok(output),
                            Ok(Err(error)) => Err(error.to_string()),
                            Err(error) => {
                                flush_pending_background_children(
                                    self.engine,
                                    session,
                                    request,
                                    tools.as_ref(),
                                    &mut pending_background,
                                    &mut out,
                                    started,
                                    timeout,
                                )
                                .await?;
                                return Err(error);
                            }
                        }
                    };
                    match executed {
                        Ok(o) => out.push((index, call.clone(), Ok(o))),
                        Err(e) => out.push((index, call.clone(), Err(e))),
                    }
                }
                flush_pending_background_children(
                    self.engine,
                    session,
                    request,
                    tools.as_ref(),
                    &mut pending_background,
                    &mut out,
                    started,
                    timeout,
                )
                .await?;
                Ok(None)
            };
            match batch.await {
                Ok(None) => out,
                Ok(Some(outcome)) => return Ok(outcome),
                Err(error) => {
                    let had_unlaunched = super::nested::abandon_queued_background_children(
                        self.engine,
                        session,
                        pending_background.drain(..).map(|(_, _, child)| child),
                    );
                    if had_unlaunched {
                        let _ = session.save(tools.as_ref());
                    }
                    return Err(error);
                }
            }
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
                Ok(ToolOutput::Report(report)) if !session.plan_jail || session.is_content() => {
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
                Ok(ToolOutput::Report(_)) => {}
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
        if let Some((success, summary)) = content_terminal_report
            && !request.session_wait
        {
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
                    if call.name == "handoff"
                        && let Some(event) =
                            handoff_brief_event(session.is_content(), &call.args_json)
                    {
                        self.engine.emit(&session.run_id, event);
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
                    if session.plan_jail && !session.is_content() {
                        return self
                            .park_for_plan(
                                session,
                                pending_park,
                                &call,
                                index,
                                conversation_tool_call_id(&call, session.turns, index),
                                report.summary.clone(),
                            )
                            .await;
                    }
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
                        kind: ParkKind::Escalate,
                        allow_call_id: String::new(),
                        plan_digest: String::new(),
                    };
                    let info = ParkInfo {
                        reason: park.reason.clone(),
                        question: park.question.clone(),
                        tool_call_id: conversation_tool_call_id(&call, session.turns, index),
                        kind: ParkKind::Escalate,
                        approval_id: None,
                        display_call_id: Some(stable_tool_call_id(&call, session.turns, index)),
                        args_json: Some(call.args_json.clone()),
                        plan_digest: String::new(),
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
            Some((summary, _success)) if request.session_wait => {
                let parked = ParkedState {
                    reason: "end_turn".into(),
                    question: String::new(),
                    tool_call_id: String::new(),
                    kind: ParkKind::PromptWait,
                    allow_call_id: String::new(),
                    plan_digest: String::new(),
                };
                *pending_park = Some(parked.clone());
                session.save_recoverable(Some(parked.clone()), tools.as_ref())?;
                Ok(ToolBatchOutcome::Parked {
                    info: ParkInfo {
                        reason: parked.reason,
                        question: parked.question,
                        tool_call_id: parked.tool_call_id,
                        kind: ParkKind::PromptWait,
                        approval_id: None,
                        display_call_id: None,
                        args_json: None,
                        plan_digest: String::new(),
                    },
                    summary,
                })
            }
            Some((summary, success)) => Ok(ToolBatchOutcome::Completed { summary, success }),
            None => Ok(ToolBatchOutcome::Continue),
        }
    }

    async fn persist_completed_prefix(
        &self,
        session: &mut RunSession,
        handle: &RunHandle,
        out: &[(usize, ToolCall, Result<ToolOutput, String>)],
    ) -> Result<Vec<(String, String, bool, String, String)>, RunError> {
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
            emissions.push((
                report_call_id,
                call.name.clone(),
                ok,
                detail,
                call.args_json.clone(),
            ));
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

    async fn park_for_ask(
        &self,
        session: &mut RunSession,
        pending_park: &mut Option<ParkedState>,
        call: &ToolCall,
        index: usize,
        conversation_id: String,
        emissions: Vec<(String, String, bool, String, String)>,
    ) -> Result<ToolBatchOutcome, RunError> {
        let reason = format!("ask:{}", call.name);
        let question = format!("Allow `{}`?", call.name);
        let tool_call_id = if conversation_id.is_empty() {
            conversation_tool_call_id(call, session.turns, index)
        } else {
            conversation_id
        };
        let parked = ParkedState {
            reason: reason.clone(),
            question: question.clone(),
            tool_call_id: tool_call_id.clone(),
            kind: ParkKind::Ask,
            allow_call_id: stable_tool_call_id(call, session.turns, index),
            plan_digest: String::new(),
        };
        let info = ParkInfo {
            reason: reason.clone(),
            question,
            tool_call_id,
            kind: ParkKind::Ask,
            approval_id: None,
            display_call_id: Some(stable_tool_call_id(call, session.turns, index)),
            args_json: Some(call.args_json.clone()),
            plan_digest: String::new(),
        };
        *pending_park = Some(parked.clone());
        session.save_recoverable(Some(parked), self.tools.as_ref())?;
        self.emit_prefix_tool_ends(session, emissions).await?;
        Ok(ToolBatchOutcome::Parked {
            info,
            summary: format!("ask=park: {}", call.name),
        })
    }

    async fn park_for_plan(
        &self,
        session: &mut RunSession,
        pending_park: &mut Option<ParkedState>,
        call: &ToolCall,
        index: usize,
        conversation_id: String,
        summary: String,
    ) -> Result<ToolBatchOutcome, RunError> {
        let (digest, text) = match tools::read_plan_jail_file(&session.workspace) {
            Some(bytes) => (
                crate::digest::sha256_prefixed(&bytes),
                String::from_utf8_lossy(&bytes).into_owned(),
            ),
            None => (String::new(), String::new()),
        };
        let question = if text.is_empty() {
            "Accept plan? (plan file missing or empty)".into()
        } else {
            format!("Accept plan?\n\n{text}")
        };
        let tool_call_id = if conversation_id.is_empty() {
            conversation_tool_call_id(call, session.turns, index)
        } else {
            conversation_id
        };
        let parked = ParkedState {
            reason: "plan".into(),
            question: question.clone(),
            tool_call_id: tool_call_id.clone(),
            kind: ParkKind::Plan,
            allow_call_id: String::new(),
            plan_digest: digest.clone(),
        };
        let info = ParkInfo {
            reason: "plan".into(),
            question,
            tool_call_id,
            kind: ParkKind::Plan,
            approval_id: None,
            display_call_id: Some(stable_tool_call_id(call, session.turns, index)),
            args_json: Some(call.args_json.clone()),
            plan_digest: digest,
        };
        *pending_park = Some(parked.clone());
        session.save_recoverable(Some(parked), self.tools.as_ref())?;
        Ok(ToolBatchOutcome::Parked { info, summary })
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
        emissions: Vec<(String, String, bool, String, String)>,
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
            kind: ParkKind::Approval,
            allow_call_id: String::new(),
            plan_digest: String::new(),
        };
        let info = ParkInfo {
            reason: reason.clone(),
            question,
            tool_call_id,
            kind: ParkKind::Approval,
            approval_id: Some(park.approval_id.clone()),
            display_call_id: None,
            args_json: None,
            plan_digest: String::new(),
        };
        self.engine
            .governance
            .record_approval_park(self.handle, park)
            .await?;
        *pending_park = Some(parked.clone());
        session.save_recoverable(Some(parked), self.tools.as_ref())?;
        self.emit_prefix_tool_ends(session, emissions).await?;
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

    async fn emit_prefix_tool_ends(
        &self,
        session: &mut RunSession,
        emissions: Vec<(String, String, bool, String, String)>,
    ) -> Result<(), RunError> {
        for (prefix_call_id, name, ok, detail, args_json) in emissions {
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
            if name == "handoff"
                && ok
                && let Some(event) = handoff_brief_event(session.is_content(), &args_json)
            {
                self.engine.emit(&session.run_id, event);
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
        Ok(())
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

fn project_brief_list(content_run: bool, items: &[String]) -> Vec<String> {
    items
        .iter()
        .map(|item| projected_detail(content_run, item))
        .collect()
}

/// Per-call brief from that call's args. Content runs keep only projected text.
fn handoff_brief_event(content_run: bool, args_json: &str) -> Option<HarnessEvent> {
    let brief = tools::apply_handoff(args_json).ok()?;
    Some(HarnessEvent::HandoffBrief {
        task: projected_detail(content_run, &brief.task),
        decisions: project_brief_list(content_run, &brief.decisions),
        files: project_brief_list(content_run, &brief.files),
        ignore: project_brief_list(content_run, &brief.ignore),
    })
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

    fn child_call(wait: bool, worktree: bool) -> ToolCall {
        let mut args = serde_json::json!({
            "profile": "explore",
            "task": "scout",
            "wait": wait
        });
        if worktree {
            args["worktree"] = serde_json::json!(true);
        }
        ToolCall {
            id: String::new(),
            name: "child_run".into(),
            args_json: args.to_string(),
        }
    }

    #[test]
    fn isolated_wait_false_children_can_start_together() {
        let calls = [child_call(false, false), child_call(false, false)];
        assert!(batch_can_concurrent_child_starts(
            &calls, false, false, true, false
        ));
        let with_read = [
            child_call(false, false),
            ToolCall {
                id: String::new(),
                name: "read_file".into(),
                args_json: r#"{"path":"a.txt"}"#.into(),
            },
        ];
        assert!(batch_can_concurrent_child_starts(
            &with_read, false, false, true, false
        ));
        assert!(!tools::is_parallel_safe_tool("child_run"));
    }

    #[test]
    fn writes_wait_true_and_worktree_keep_child_batch_serial() {
        let write_and_child = [
            ToolCall {
                id: String::new(),
                name: "write_file".into(),
                args_json: r#"{"path":"a.txt","content":"x"}"#.into(),
            },
            child_call(false, false),
        ];
        assert!(!batch_can_concurrent_child_starts(
            &write_and_child,
            false,
            false,
            true,
            false
        ));
        let wait_true = [child_call(true, false), child_call(true, false)];
        assert!(!batch_can_concurrent_child_starts(
            &wait_true, false, false, true, false
        ));
        let worktree = [child_call(false, true), child_call(false, false)];
        assert!(!batch_can_concurrent_child_starts(
            &worktree, false, false, true, false
        ));
        let full = ToolCall {
            id: String::new(),
            name: "child_run".into(),
            args_json: r#"{"profile":"full","task":"edit","wait":false}"#.into(),
        };
        assert!(!batch_can_concurrent_child_starts(
            &[full.clone(), full],
            true,
            false,
            true,
            false
        ));
        assert!(!batch_can_concurrent_child_starts(
            &[child_call(false, false)],
            false,
            false,
            true,
            false
        ));
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

    #[test]
    fn handoff_brief_event_keeps_each_call_and_projects_content_runs() {
        let first = r#"{"task":"first","decisions":["a"],"files":["a.rs"],"ignore":["tmp"]}"#;
        let second = r#"{"task":"second","decisions":["b"],"files":["b.rs"],"ignore":["gen"]}"#;
        match (
            handoff_brief_event(false, first),
            handoff_brief_event(false, second),
        ) {
            (
                Some(HarnessEvent::HandoffBrief {
                    task: task_a,
                    decisions: decisions_a,
                    files: files_a,
                    ignore: ignore_a,
                }),
                Some(HarnessEvent::HandoffBrief {
                    task: task_b,
                    decisions: decisions_b,
                    files: files_b,
                    ignore: ignore_b,
                }),
            ) => {
                assert_eq!(task_a, "first");
                assert_eq!(decisions_a, vec!["a".to_string()]);
                assert_eq!(files_a, vec!["a.rs".to_string()]);
                assert_eq!(ignore_a, vec!["tmp".to_string()]);
                assert_eq!(task_b, "second");
                assert_eq!(decisions_b, vec!["b".to_string()]);
                assert_eq!(files_b, vec!["b.rs".to_string()]);
                assert_eq!(ignore_b, vec!["gen".to_string()]);
            }
            other => panic!("{other:?}"),
        }

        match handoff_brief_event(true, first) {
            Some(HarnessEvent::HandoffBrief {
                task,
                decisions,
                files,
                ignore,
            }) => {
                assert_eq!(task, projected_detail(true, "first"));
                assert_eq!(decisions, vec![projected_detail(true, "a")]);
                assert_eq!(files, vec![projected_detail(true, "a.rs")]);
                assert_eq!(ignore, vec![projected_detail(true, "tmp")]);
                assert!(!task.contains("first"), "{task}");
            }
            other => panic!("{other:?}"),
        }
        assert!(handoff_brief_event(false, r#"{}"#).is_none());
    }
}
