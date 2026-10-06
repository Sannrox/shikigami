//! Complete durable run transaction behind one internal interface.
//!
//! This module owns the durable turn loop after [`super::preparation`] has
//! materialized the workspace, recovered governed receipts, and run admission
//! hooks: model/tool ordering, checkpoint durability, park/failure recovery,
//! completion, and artifact finalization. Engine remains the stable public
//! construction interface.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::watch;

use crate::checkpoint::{Checkpoint, ParkKind, ParkedState};
use crate::events::HarnessEvent;
use crate::governance::RunOutcome;
use crate::hooks::{self, HookEvent};
use crate::model::{ChatMessage, CostEstimate};
use crate::replay::{ReplayExecution, ReplayTerminalCheckpoint};
use crate::tracing_export::RunSpanTrace;

use super::model_turn::DurableModelTurn;
use super::supervision::check_bounds;
use super::tool_batch::{DurableToolBatch, ToolBatchOutcome};
use super::{ContentExecution, Engine, ParkInfo, RunError, RunRequest, RunResult, RunTermination};

pub(super) struct RunTransaction<'a> {
    engine: &'a Engine,
}

impl<'a> RunTransaction<'a> {
    pub(super) fn new(engine: &'a Engine) -> Self {
        Self { engine }
    }

    pub(super) async fn execute(
        &self,
        request: RunRequest,
        fresh_run_id: String,
        resume_checkpoint: Option<Checkpoint>,
        replay: Option<ReplayExecution>,
        content: Option<ContentExecution>,
    ) -> Result<RunResult, RunError> {
        let started = tokio::time::Instant::now();
        let timeout = request
            .timeout
            .or_else(|| self.engine.config.run.timeout_secs.map(Duration::from_secs));

        let super::preparation::PreparedRun {
            mut session,
            workspace: ws,
            tools,
            tool_defs,
            system_prompt,
            prompt_id,
            handle,
            governance_checkpoint,
        } = super::preparation::prepare(
            self.engine,
            &request,
            fresh_run_id,
            resume_checkpoint,
            replay.as_ref(),
            content.as_ref(),
        )
        .await?;
        let plan_operation_id = governance_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.operation_id.as_str())
            .unwrap_or("");
        session.spans = RunSpanTrace::begin(&self.engine.config, &handle, plan_operation_id)
            .map_err(|error| RunError::Message(error.to_string()))?;
        if request.resume_run_id.is_some() {
            self.engine.model.restore_replay_cursor(session.turns)?;
        }

        let mut final_summary = String::from("completed without report");
        let mut success = false;
        let mut termination = RunTermination::Completed;

        // Preserve a durable park (escalate or approval) if reporting that
        // park fails after the park has already been written.
        let mut pending_park: Option<ParkedState> = None;
        let mut model_turns = DurableModelTurn::new(
            self.engine,
            &request,
            started,
            timeout,
            &handle,
            &system_prompt,
            &tool_defs,
            Arc::clone(&tools),
            governance_checkpoint.as_ref(),
            &session,
            replay.is_some(),
        );
        let recovered_content_terminal = session.recover_content_terminal().await?;
        let recovered_usage = recovered_content_terminal
            .as_ref()
            .map(|(terminal, _)| terminal.usage);
        let recovered_text_report = recovered_text_report(&session.messages);

        // Ok(Some(park)) when parked (escalate or approval); Ok(None) when
        // finished normally.
        let result: Result<Option<ParkInfo>, RunError> =
            if let Some((terminal, summary)) = recovered_content_terminal {
                final_summary = summary;
                success = terminal.success;
                termination = terminal.termination;
                Ok(None)
            } else if let Some((report_success, summary)) = recovered_text_report {
                final_summary = summary;
                success = report_success;
                termination = RunTermination::Completed;
                Ok(None)
            } else if request.resume_plan == Some(super::PlanDecision::Reject) {
                final_summary = "plan rejected".into();
                success = false;
                termination = RunTermination::Failed;
                Ok(None)
            } else {
                async {
                    loop {
                        let turn = match model_turns.next(&mut session).await {
                            Err(RunError::MaxTurns(limit)) if request.session_wait => {
                                let parked = ParkedState {
                                    reason: "max_turns".into(),
                                    question: String::new(),
                                    tool_call_id: String::new(),
                                    kind: ParkKind::PromptWait,
                                    allow_call_id: String::new(),
                                    plan_digest: String::new(),
                                };
                                pending_park = Some(parked.clone());
                                session.save_recoverable(Some(parked.clone()), tools.as_ref())?;
                                termination = RunTermination::Parked;
                                final_summary = format!("reached max_turns ({limit})");
                                success = false;
                                return Ok(Some(ParkInfo {
                                    reason: parked.reason,
                                    question: parked.question,
                                    tool_call_id: parked.tool_call_id,
                                    kind: ParkKind::PromptWait,
                                    approval_id: None,
                                    display_call_id: None,
                                    args_json: None,
                                    plan_digest: String::new(),
                                }));
                            }
                            result => result?,
                        };

                        if turn.tool_calls.is_empty() {
                            if replay.is_some() {
                                return Err(RunError::Message(
                                    "replay requires the terminal `report` tool".into(),
                                ));
                            }
                            final_summary = if turn.content.is_empty() {
                                "model finished without tools".into()
                            } else {
                                turn.content
                            };
                            success = true;
                            if request.session_wait {
                                let parked = ParkedState {
                                    reason: "end_turn".into(),
                                    question: String::new(),
                                    tool_call_id: String::new(),
                                    kind: ParkKind::PromptWait,
                                    allow_call_id: String::new(),
                                    plan_digest: String::new(),
                                };
                                pending_park = Some(parked.clone());
                                session.save_recoverable(Some(parked.clone()), tools.as_ref())?;
                                termination = RunTermination::Parked;
                                return Ok(Some(ParkInfo {
                                    reason: parked.reason,
                                    question: parked.question,
                                    tool_call_id: parked.tool_call_id,
                                    kind: ParkKind::PromptWait,
                                    approval_id: None,
                                    display_call_id: None,
                                    args_json: None,
                                    plan_digest: String::new(),
                                }));
                            }
                            termination = RunTermination::Completed;
                            if session.is_content() {
                                session.mark_content_terminal(
                                    success,
                                    termination,
                                    &final_summary,
                                    model_turns.usage(),
                                )?;
                                session.save(tools.as_ref())?;
                            }
                            break;
                        }

                        let outcome = DurableToolBatch::new(
                            self.engine,
                            &request,
                            started,
                            timeout,
                            &handle,
                            Arc::clone(&tools),
                            replay.is_some(),
                        )
                        .execute(&turn, &mut session, &mut pending_park, model_turns.usage())
                        .await?;
                        match outcome {
                            ToolBatchOutcome::Continue => continue,
                            ToolBatchOutcome::Completed {
                                summary,
                                success: report_success,
                            } => {
                                final_summary = summary;
                                success = report_success;
                                termination = RunTermination::Completed;
                                return Ok(None);
                            }
                            ToolBatchOutcome::Parked { info, summary } => {
                                final_summary = summary;
                                success = false;
                                termination = RunTermination::Parked;
                                return Ok(Some(info));
                            }
                        }
                    }
                    Ok(None)
                }
                .await
            };

        let park_info = match &result {
            Ok(park) => park.clone(),
            Err(_) => None,
        };
        let usage = recovered_usage.unwrap_or_else(|| model_turns.usage());

        let (success, final_summary, termination) = match result {
            Ok(_) => (success, final_summary, termination),
            Err(e) => {
                let summary = e.to_string();
                let projected_summary = projected_summary(session.is_content(), &summary);
                let _ = self
                    .finish_nested_children(&mut session, &request, started, timeout, true, true)
                    .await;
                tools.kill_background_jobs().await;
                // The complete batch was staged before reporting, so this
                // checkpoint cannot replay an already executed host tool.
                // Keep the workspace on failure for resume/inspection.
                let _ = session.save_recoverable(pending_park.clone(), tools.as_ref());
                if !e.leaves_governance_open() {
                    let completion = self
                        .engine
                        .governance
                        .complete_run(
                            &handle,
                            RunOutcome {
                                success: false,
                                summary: projected_summary.clone(),
                                turns: session.turns,
                                termination: e.termination().as_str().into(),
                                workspace: ws.path.display().to_string(),
                            },
                        )
                        .await;
                    if completion.is_err() {
                        let _ = session.save_recoverable(pending_park.clone(), tools.as_ref());
                    } else {
                        // `complete_run` has forgotten the in-memory receipt
                        // state; persist that finalized boundary so a later
                        // resume cannot reuse a terminal plane receipt.
                        let _ = session.save_recoverable(pending_park.clone(), tools.as_ref());
                    }
                }
                // Root cancel/timeout keeps the workspace for resume. Nested
                // git-worktree children still reap: they are not a resume host.
                self.engine.emit(
                    &session.run_id,
                    HarnessEvent::RunFinished {
                        run_id: session.run_id.clone(),
                        success: false,
                        summary: projected_summary,
                    },
                );
                // Reap after all error-path bookkeeping and immediately
                // before inventory so descendants cannot keep mutating the
                // workspace after the failed run has been recorded.
                super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
                    .finalize(&session.run_id, &ws.path, tools.as_ref())
                    .await;
                self.cleanup_workspace(
                    &ws,
                    &session,
                    &request,
                    false,
                    session.keeps_parked_workspace(pending_park.is_some()),
                );
                self.export_spans(&mut session, false).await;
                return Err(e);
            }
        };
        let cost = CostEstimate::from_usage_and_rates(
            usage,
            self.engine.config.model.input_usd_micros_per_mtok,
            self.engine.config.model.output_usd_micros_per_mtok,
        );
        if session.is_content() && termination != RunTermination::Parked {
            session.mark_content_terminal(success, termination, &final_summary, usage)?;
            session.save(tools.as_ref())?;
        }

        if termination == RunTermination::Parked {
            // Session hosts keep wait=false children across park. Unattended
            // `run` prints the park and exits, so cancel and join now:
            // in-flight child tools drop on cancel (park stays off the
            // child's task clock) and registry rows become terminal
            // before process exit.
            if !request.session_wait {
                let _ = self
                    .finish_nested_children(&mut session, &request, started, timeout, true, true)
                    .await;
            }
        } else {
            let over_bounds = check_bounds(
                self.engine,
                &session.run_id,
                &request,
                started,
                timeout,
                &session.parent_run_id,
            )
            .is_err();
            if let Err(error) = self
                .finish_nested_children(&mut session, &request, started, timeout, true, over_bounds)
                .await
            {
                tools.kill_background_jobs().await;
                let _ = session.save_recoverable(None, tools.as_ref());
                self.engine.emit(
                    &session.run_id,
                    HarnessEvent::RunFinished {
                        run_id: session.run_id.clone(),
                        success: false,
                        summary: projected_summary(session.is_content(), &error.to_string()),
                    },
                );
                super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
                    .finalize(&session.run_id, &ws.path, tools.as_ref())
                    .await;
                self.cleanup_workspace(&ws, &session, &request, false, false);
                self.export_spans(&mut session, false).await;
                return Err(error);
            }
            let governance_summary = projected_summary(session.is_content(), &final_summary);
            let completion = self
                .engine
                .governance
                .complete_run(
                    &handle,
                    RunOutcome {
                        success,
                        summary: governance_summary,
                        turns: session.turns,
                        termination: termination.as_str().into(),
                        workspace: ws.path.display().to_string(),
                    },
                )
                .await;
            if let Err(error) = completion {
                let _ = session.save_recoverable(None, tools.as_ref());
                super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
                    .finalize(&session.run_id, &ws.path, tools.as_ref())
                    .await;
                self.export_spans(&mut session, false).await;
                let _ = self
                    .finish_nested_children(&mut session, &request, started, timeout, true, false)
                    .await;
                self.cleanup_workspace(&ws, &session, &request, false, false);
                return Err(error.into());
            }
            // Successful completion clears adapter-owned receipt correlation
            // from the durable checkpoint. Parked runs intentionally retain
            // it for their governed continuation.
            session.mark_replay_terminal(ReplayTerminalCheckpoint {
                success,
                termination,
                summary: final_summary.clone(),
                prompt_id: prompt_id.clone(),
                usage,
                cost: cost.clone(),
                finalized: false,
                artifact_dir: None,
            });
            if let Err(error) = session.save(tools.as_ref()) {
                super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
                    .finalize(&session.run_id, &ws.path, tools.as_ref())
                    .await;
                self.export_spans(&mut session, false).await;
                let _ = self
                    .finish_nested_children(&mut session, &request, started, timeout, true, false)
                    .await;
                self.cleanup_workspace(&ws, &session, &request, false, false);
                return Err(error);
            }
        }

        // Always reap background shells before taking the final inventory.
        let artifact_dir = super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
            .finalize(&session.run_id, &ws.path, tools.as_ref())
            .await;

        // Keep workspace on park. Isolated git-worktree *children* honor the
        // run/resume `keep_workspace` flag, not the park-forced checkpoint
        // bit, so `--resume` of a parked plan worktree still `git worktree
        // remove` unless the operator asked to keep it. Nested worktree
        // children also reap on cancel/fail. Parent git-worktree runs keep
        // the freeze-core park-forced keep.
        self.cleanup_workspace(
            &ws,
            &session,
            &request,
            success,
            termination == RunTermination::Parked,
        );

        self.engine.emit(
            &session.run_id,
            HarnessEvent::RunFinished {
                run_id: session.run_id.clone(),
                success,
                summary: projected_summary(session.is_content(), &final_summary),
            },
        );
        if termination != RunTermination::Parked {
            session.mark_replay_finalized(artifact_dir.as_deref());
            session.mark_content_finalized(artifact_dir.as_deref());
            if let Err(error) = session.save(tools.as_ref()) {
                self.export_spans(&mut session, success).await;
                let _ = self
                    .finish_nested_children(&mut session, &request, started, timeout, true, false)
                    .await;
                self.cleanup_workspace(&ws, &session, &request, false, false);
                return Err(error);
            }
        }

        let _ = hooks::run_hooks(
            &self.engine.config.hooks,
            HookEvent::PostRun,
            json!({
                "run_id": session.run_id,
                "success": success,
                "termination": termination.as_str(),
                "summary": projected_summary(session.is_content(), &final_summary),
            }),
        )
        .await;

        self.export_spans(&mut session, success).await;

        Ok(RunResult {
            run_id: session.run_id.clone(),
            success,
            summary: final_summary,
            turns: session.turns,
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

    /// Root cancel/timeout keeps the tree for resume. Nested git-worktree
    /// children with `keep_workspace=false` reap on any non-park terminal.
    fn cleanup_workspace(
        &self,
        ws: &crate::workspace::MaterializedWorkspace,
        session: &super::session::RunSession,
        request: &RunRequest,
        success: bool,
        parked: bool,
    ) {
        if parked {
            return;
        }
        let nested_worktree = ws.adapter == "git-worktree" && !session.parent_run_id.is_empty();
        let keep = if nested_worktree {
            request.keep_workspace
        } else if success {
            session.keep_workspace
        } else {
            true
        };
        if keep {
            return;
        }
        if !nested_worktree {
            let shared_child_open = session.children.iter().any(|child| {
                let Ok(checkpoint) = Checkpoint::load(&self.engine.state_runs, &child.run_id)
                else {
                    return false;
                };
                checkpoint.workspace == session.workspace
                    && (checkpoint.park.is_some()
                        || self
                            .engine
                            .registry
                            .run_is_active(&child.run_id)
                            .unwrap_or(false))
            });
            if shared_child_open {
                return;
            }
        }
        let _ = crate::workspace::apply_cleanup(ws);
    }

    async fn finish_nested_children(
        &self,
        session: &mut super::session::RunSession,
        request: &RunRequest,
        started: tokio::time::Instant,
        timeout: Option<Duration>,
        join: bool,
        cancel: bool,
    ) -> Result<(), RunError> {
        if cancel {
            for child in &session.children {
                let _ = self.engine.registry.request_cancel(&child.run_id);
            }
        }
        if !join {
            return Ok(());
        }
        let mut bounds_error = None;
        let joins = session.take_background_joins();
        if !joins.is_empty() {
            let join_fut = async move {
                for handle in joins {
                    let _ = handle.await;
                }
            };
            tokio::pin!(join_fut);
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = &mut join_fut => break,
                    _ = interval.tick() => {
                        self.note_nested_bounds(
                            session,
                            request,
                            started,
                            timeout,
                            &mut bounds_error,
                        );
                    }
                }
            }
        }
        // Park detaches JoinHandles. Resume cannot restore them. Cancel any
        // still-running recorded child so it cannot keep writing the shared
        // workspace after the parent completes, then wait until those
        // children leave `running` (in-flight bash observes cancel). Do not
        // return while a child is still active: a 2s grace would let
        // `sleep N && write` finish after parent cancel/timeout.
        // Same-process finish notifies the interned idle watch. JSON
        // membership is rechecked when entering the wait, after a notify,
        // and if the owner-lease TTL elapses without one. Children with no
        // interned watch (another process owns them) fall back to JSON on
        // the bounds tick. Bounds ticks do not load JSON for interned waits.
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut cancelled = std::collections::HashSet::new();
        loop {
            self.note_nested_bounds(session, request, started, timeout, &mut bounds_error);
            let mut idle_watches = Vec::new();
            let mut external = false;
            for child in &session.children {
                if !self
                    .engine
                    .registry
                    .run_is_active(&child.run_id)
                    .unwrap_or(false)
                {
                    continue;
                }
                let (rx, interned) =
                    self.engine
                        .registry
                        .watch_idle(&child.run_id)
                        .map_err(|error| {
                            RunError::Message(format!("nested child idle watch: {error}"))
                        })?;
                let already_idle = *rx.borrow();
                if cancelled.insert(child.run_id.clone()) {
                    let _ = self.engine.registry.request_cancel(&child.run_id);
                }
                if already_idle
                    && !self
                        .engine
                        .registry
                        .run_is_active(&child.run_id)
                        .unwrap_or(false)
                {
                    // Finish landed between the membership check and
                    // subscribe; the new watch will not see another notify.
                    continue;
                }
                external |= !interned;
                idle_watches.push((rx, already_idle));
            }
            if idle_watches.is_empty() {
                break;
            }
            let mut waiting = tokio::task::JoinSet::new();
            for (rx, already_idle) in idle_watches {
                waiting.spawn(wait_for_child_idle(rx, already_idle));
            }
            let lease = tokio::time::sleep(Duration::from_millis(
                crate::registry::ACTIVE_HEARTBEAT_TTL_MS,
            ));
            tokio::pin!(lease);
            loop {
                self.note_nested_bounds(session, request, started, timeout, &mut bounds_error);
                if waiting.is_empty() {
                    break;
                }
                tokio::select! {
                    _ = interval.tick() => {
                        if external {
                            break;
                        }
                    }
                    _ = waiting.join_next() => {}
                    () = &mut lease => break,
                }
            }
        }
        match bounds_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn note_nested_bounds(
        &self,
        session: &super::session::RunSession,
        request: &RunRequest,
        started: tokio::time::Instant,
        timeout: Option<Duration>,
        bounds_error: &mut Option<RunError>,
    ) {
        if bounds_error.is_some() {
            return;
        }
        if let Err(error) = check_bounds(
            self.engine,
            &session.run_id,
            request,
            started,
            timeout,
            &session.parent_run_id,
        ) {
            for child in &session.children {
                let _ = self.engine.registry.request_cancel(&child.run_id);
            }
            *bounds_error = Some(error);
        }
    }

    async fn export_spans(&self, session: &mut super::session::RunSession, success: bool) {
        if let Err(error) = session.spans.finish(success).await {
            self.engine.emit(
                &session.run_id,
                HarnessEvent::Message {
                    level: "warn".into(),
                    text: format!("span export failed: {error}"),
                },
            );
        }
    }
}

async fn wait_for_child_idle(mut rx: watch::Receiver<bool>, already_idle: bool) {
    if already_idle {
        // Stale `true` with a live registry row: wait for the next finish
        // notify (`send_replace` wakes even when the value stays true).
        let _ = rx.changed().await;
        return;
    }
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

fn recovered_text_report(messages: &[ChatMessage]) -> Option<(bool, String)> {
    let assistant_idx = messages
        .iter()
        .rposition(|message| message.role == "assistant")?;
    let assistant = &messages[assistant_idx];
    if assistant.tool_calls.len() != 1 || assistant.tool_calls[0].name != "report" {
        return None;
    }
    let call = &assistant.tool_calls[0];
    if call.id.is_empty() {
        return None;
    }
    let tool = messages.get(assistant_idx + 1)?;
    if tool.role != "tool" || tool.tool_call_id != call.id || assistant_idx + 2 != messages.len() {
        return None;
    }
    let report: crate::tools::Report = serde_json::from_str(&call.args_json).ok()?;
    // A matching tool-call id is not enough: denied, hook-blocked, and failed
    // executions also append a tool message. Successful `report` results use
    // this prefix; anything else must resume through the normal turn loop.
    if tool.content != format!("report: {}", report.summary) {
        return None;
    }
    Some((report.success, report.summary))
}

fn projected_summary(content_run: bool, summary: &str) -> String {
    crate::content::project_bounded_text(content_run, "bounded_content_result", summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::registry::RunRegistry;
    use crate::state::StateRoot;
    use crate::{events, governance, workspace};
    use tempfile::tempdir;

    #[tokio::test]
    async fn transaction_persists_a_terminal_model_turn() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        config.model.adapter = "scripted".into();
        config.model.script_json = Some(r#"[{"content":"done"}]"#.into());
        let registry = Arc::new(RunRegistry::new(state.path()).unwrap());
        let engine = Engine::new(
            config.clone(),
            Arc::from(governance::from_config(&config).unwrap()),
            Arc::from(workspace::from_config(&config).unwrap()),
            Arc::from(crate::model::from_config(&config).unwrap()),
            Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            state.runs_dir(),
            Arc::clone(&registry),
        );
        let run_id = "transaction-test";
        registry.start(run_id, "task", None, None).unwrap();

        let result = RunTransaction::new(&engine)
            .execute(
                RunRequest {
                    task: "task".into(),
                    keep_workspace: true,
                    ..RunRequest::new("")
                },
                run_id.into(),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.summary, "done");
        assert_eq!(result.turns, 1);
        assert!(result.artifact_dir.is_some());
        assert_eq!(
            registry.load(run_id).unwrap().artifact_dir,
            result
                .artifact_dir
                .as_ref()
                .map(|path| path.display().to_string())
        );
        let checkpoint = Checkpoint::load(&state.runs_dir(), run_id).unwrap();
        assert_eq!(checkpoint.completed_turns, 1);
        assert_eq!(checkpoint.messages.last().unwrap().content, "done");
    }

    fn nested_idle_fixture(
        dir: &tempfile::TempDir,
        parent_id: &str,
        child_id: &str,
    ) -> (Engine, super::super::session::RunSession, Arc<RunRegistry>) {
        let state = StateRoot::new(dir.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        config.model.adapter = "scripted".into();
        config.model.script_json = Some(r#"[{"content":"done"}]"#.into());
        let registry = Arc::new(RunRegistry::new(state.path()).unwrap());
        let engine = Engine::new(
            config.clone(),
            Arc::from(governance::from_config(&config).unwrap()),
            Arc::from(workspace::from_config(&config).unwrap()),
            Arc::from(crate::model::from_config(&config).unwrap()),
            Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            state.runs_dir(),
            Arc::clone(&registry),
        );
        let mut session = super::super::session::RunSession::new(
            state.runs_dir(),
            Arc::from(governance::from_config(&config).unwrap()),
            parent_id,
            "delegate",
            dir.path().join("ws"),
            "inplace",
            true,
            vec![],
            0,
        );
        registry.start(child_id, "scout", None, None).unwrap();
        session.children.push(crate::checkpoint::ChildRunRecord {
            run_id: child_id.into(),
            profile: "explore".into(),
            task: "scout".into(),
        });
        (engine, session, registry)
    }

    #[tokio::test]
    async fn nested_parent_finish_idle_wait_is_notify_driven() {
        let dir = tempdir().unwrap();
        let (engine, mut session, registry) =
            nested_idle_fixture(&dir, "parent-idle", "child-idle");
        let delay = Duration::from_millis(200);
        let finisher = {
            let registry = Arc::clone(&registry);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                registry
                    .finish_error("child-idle", &RunError::Cancelled)
                    .unwrap();
            })
        };
        let before = crate::registry::run_is_active_count_for("child-idle");
        let started = tokio::time::Instant::now();
        tokio::time::timeout(
            Duration::from_secs(1),
            RunTransaction::new(&engine).finish_nested_children(
                &mut session,
                &RunRequest::new("delegate"),
                started,
                None,
                true,
                true,
            ),
        )
        .await
        .expect("idle wait must resolve on registry notify")
        .unwrap();
        finisher.await.unwrap();
        assert!(
            started.elapsed() >= delay,
            "parent finish must wait for the child idle notify"
        );
        let extra = crate::registry::run_is_active_count_for("child-idle") - before;
        assert!(
            extra <= 3,
            "parent finish must not poll run_is_active at 50ms; extra={extra}"
        );
        assert!(
            !registry.run_is_active("child-idle").unwrap(),
            "cancelled child must be observed idle"
        );
    }

    #[tokio::test]
    async fn nested_parent_finish_sees_already_finished_child_without_poll() {
        let dir = tempdir().unwrap();
        let (engine, mut session, registry) =
            nested_idle_fixture(&dir, "parent-already-idle", "child-already-idle");
        registry
            .finish_error("child-already-idle", &RunError::Cancelled)
            .unwrap();
        let before = crate::registry::run_is_active_count_for("child-already-idle");
        tokio::time::timeout(
            Duration::from_millis(200),
            RunTransaction::new(&engine).finish_nested_children(
                &mut session,
                &RunRequest::new("delegate"),
                tokio::time::Instant::now(),
                None,
                true,
                true,
            ),
        )
        .await
        .expect("already-finished children must not hang parent finish")
        .unwrap();
        assert_eq!(
            crate::registry::run_is_active_count_for("child-already-idle") - before,
            1,
            "already-idle children confirm once via run_is_active, then skip the wait"
        );
    }

    #[tokio::test]
    async fn nested_parent_finish_cancels_still_active_child_and_observes_idle() {
        let dir = tempdir().unwrap();
        let (engine, mut session, registry) =
            nested_idle_fixture(&dir, "parent-cancel-idle", "child-still-active");
        let finisher = {
            let registry = Arc::clone(&registry);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                assert!(
                    registry.cancel_requested("child-still-active"),
                    "still-active children must be cancelled on parent finish"
                );
                registry
                    .finish_error("child-still-active", &RunError::Cancelled)
                    .unwrap();
            })
        };
        tokio::time::timeout(
            Duration::from_secs(1),
            RunTransaction::new(&engine).finish_nested_children(
                &mut session,
                &RunRequest::new("delegate"),
                tokio::time::Instant::now(),
                None,
                true,
                false,
            ),
        )
        .await
        .expect("idle wait must resolve after cancel")
        .unwrap();
        finisher.await.unwrap();
        assert!(!registry.run_is_active("child-still-active").unwrap());
    }

    #[tokio::test]
    async fn nested_parent_finish_reports_timeout_when_children_already_idle() {
        let dir = tempdir().unwrap();
        let (engine, mut session, registry) =
            nested_idle_fixture(&dir, "parent-bounds-idle", "child-bounds-idle");
        registry
            .finish_error("child-bounds-idle", &RunError::Cancelled)
            .unwrap();
        let err = tokio::time::timeout(
            Duration::from_millis(200),
            RunTransaction::new(&engine).finish_nested_children(
                &mut session,
                &RunRequest::new("delegate"),
                tokio::time::Instant::now() - Duration::from_secs(5),
                Some(Duration::from_millis(1)),
                true,
                true,
            ),
        )
        .await
        .expect("already-idle finish must not hang while reporting bounds")
        .expect_err("parent timeout must surface even when children are idle");
        assert!(
            matches!(err, RunError::TimedOut(_)),
            "expected TimedOut, got {err:?}"
        );
    }

    #[test]
    fn recovered_text_report_reads_a_terminal_report_tool_result() {
        let messages = vec![
            ChatMessage {
                role: "user".into(),
                content: "task".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![crate::model::ToolCall {
                    id: "call_0".into(),
                    name: "report".into(),
                    args_json: r#"{"summary":"done","success":true}"#.into(),
                }],
            },
            ChatMessage {
                role: "tool".into(),
                content: "report: done".into(),
                tool_call_id: "call_0".into(),
                tool_calls: vec![],
            },
        ];
        assert_eq!(
            recovered_text_report(&messages),
            Some((true, "done".into()))
        );
    }

    #[test]
    fn recovered_text_report_ignores_an_unfinished_report_turn() {
        let messages = vec![ChatMessage {
            role: "assistant".into(),
            content: String::new(),
            tool_call_id: String::new(),
            tool_calls: vec![crate::model::ToolCall {
                id: "call_0".into(),
                name: "report".into(),
                args_json: r#"{"summary":"done","success":true}"#.into(),
            }],
        }];
        assert_eq!(recovered_text_report(&messages), None);
    }

    #[test]
    fn recovered_text_report_ignores_a_failed_or_denied_report_execution() {
        let messages = vec![
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![crate::model::ToolCall {
                    id: "call_0".into(),
                    name: "report".into(),
                    args_json: r#"{"summary":"done","success":true}"#.into(),
                }],
            },
            ChatMessage {
                role: "tool".into(),
                content: "tool batch rejected: report/escalate must be the only call".into(),
                tool_call_id: "call_0".into(),
                tool_calls: vec![],
            },
        ];
        assert_eq!(recovered_text_report(&messages), None);
    }
}
