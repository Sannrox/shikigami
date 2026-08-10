//! Complete durable run transaction behind one internal interface.
//!
//! This module owns workspace preparation, governed receipt recovery, model/tool
//! turn ordering, checkpoint durability, park/failure recovery, completion, and
//! artifact finalization. Engine remains the stable public construction interface.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::checkpoint::{Checkpoint, ParkedState};
use crate::events::HarnessEvent;
use crate::governance::{GovernanceError, RunOutcome};
use crate::hooks::{self, HookEvent};
use crate::model::{ChatMessage, CostEstimate};
use crate::tools::ToolRegistry;
use crate::workspace::{
    MaterializedWorkspace, SnapshotOutcome, SnapshotPlan, WorkspaceCleanup, WorkspaceSnapshots,
};

use super::model_turn::DurableModelTurn;
use super::resume::{configured_workspace_adapter, validate_resumed_workspace};
use super::session::RunSession;
use super::tool_batch::{DurableToolBatch, ToolBatchOutcome};
use super::{Engine, ParkInfo, RunError, RunRequest, RunResult, RunTermination, SYSTEM_PROMPT};

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
    ) -> Result<RunResult, RunError> {
        let started = tokio::time::Instant::now();
        let timeout = request
            .timeout
            .or_else(|| self.engine.config.run.timeout_secs.map(Duration::from_secs));

        let (
            run_id,
            messages,
            turns,
            ws,
            task,
            keep_workspace,
            initial_todos,
            governance_checkpoint,
        ) = if let Some(resume_id) = &request.resume_run_id {
            let cp = resume_checkpoint.ok_or_else(|| {
                RunError::Message(format!("checkpoint snapshot missing for run {resume_id}"))
            })?;
            let resumed_workspace = validate_resumed_workspace(
                &self.engine.config,
                &self.engine.state_runs,
                resume_id,
                &cp,
            )?;
            self.engine.emit(
                resume_id,
                HarnessEvent::Status {
                    status: "resuming".into(),
                },
            );
            let ws = MaterializedWorkspace {
                path: resumed_workspace,
                adapter: if cp.workspace_adapter.is_empty() {
                    configured_workspace_adapter(&self.engine.config).into()
                } else {
                    cp.workspace_adapter.clone()
                },
                cleanup: if cp.keep_workspace || cp.workspace_adapter == "inplace" {
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
                cp.governance,
            )
        } else {
            let run_id = fresh_run_id;
            self.engine.emit(
                &run_id,
                HarnessEvent::Status {
                    status: "starting".into(),
                },
            );
            let ws = self
                .engine
                .workspace
                .materialize(&run_id, &self.engine.state_runs)?;
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
                None,
            )
        };

        self.engine
            .registry
            .update_running(
                &run_id,
                &task,
                request.logical_operation_id.as_deref(),
                &ws.path,
                turns,
            )
            .map_err(|error| RunError::Message(format!("run registry update failed: {error}")))?;

        let snapshot_plan = match request.restore_snapshot.as_deref() {
            Some(name) => SnapshotPlan::Restore(name),
            None if self.engine.config.workspace.snapshot => SnapshotPlan::CaptureInitial,
            None => SnapshotPlan::None,
        };
        match WorkspaceSnapshots::new(&self.engine.state_runs).prepare(
            &ws,
            &run_id,
            snapshot_plan,
        )? {
            SnapshotOutcome::Unchanged => {}
            SnapshotOutcome::Captured { name, path } => self.engine.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("snapshot {name} at {}", path.display()),
                },
            ),
            SnapshotOutcome::Restored { name } => self.engine.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("restored snapshot `{name}`"),
                },
            ),
        }

        if let Err(error) =
            crate::artifacts::capture_run_baseline(&self.engine.state_runs, &run_id, &ws.path)
        {
            self.engine.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "warn".into(),
                    text: format!("artifact baseline capture failed: {error}"),
                },
            );
        }

        let prompt_id = crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT);
        let project_rules =
            crate::context::load_project_rules(&ws.path, &self.engine.config.context);
        let skills = crate::context::load_skills(&ws.path, &self.engine.config.context);
        let system_prompt =
            crate::context::compose_system_prompt(SYSTEM_PROMPT, project_rules.as_ref(), &skills);
        self.engine.emit(
            &run_id,
            HarnessEvent::Prompt {
                prompt_id: prompt_id.clone(),
            },
        );
        if let Some(ref rules) = project_rules {
            self.engine.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("project_rules {} digest={}", rules.filename, rules.digest),
                },
            );
        }
        for s in &skills {
            self.engine.emit(
                &run_id,
                HarnessEvent::Message {
                    level: "info".into(),
                    text: format!("skill {} digest={}", s.id, s.digest),
                },
            );
        }
        self.engine.emit(
            &run_id,
            HarnessEvent::Message {
                level: "info".into(),
                text: format!("workspace {}", ws.path.display()),
            },
        );

        let mut tools = ToolRegistry::from_config(&ws.path, &self.engine.config)?;
        tools.set_todos(initial_todos);
        if !self.engine.config.tools.mcp_servers.is_empty() {
            crate::mcp::attach_mcp_servers(&mut tools, &self.engine.config).await?;
        }
        let tool_defs = tools.definitions();
        let tools = Arc::new(tools);
        let mut session = RunSession::new(
            self.engine.state_runs.clone(),
            Arc::clone(&self.engine.governance),
            run_id,
            task,
            ws.path.clone(),
            ws.adapter.clone(),
            keep_workspace,
            messages,
            turns,
        );

        let mut final_summary = String::from("completed without report");
        let mut success = false;
        let mut termination = RunTermination::Completed;
        let handle = self
            .engine
            .governance
            .begin_run_with_checkpoint(
                &session.run_id,
                &session.task,
                request.logical_operation_id.as_deref(),
                governance_checkpoint.as_ref(),
            )
            .await?;

        // Persist the host receipt correlation before any resumable work. A
        // fresh run (or a legacy deferred checkpoint) has no durable host id
        // yet; if this write fails, compensate the newly created remote
        // receipt so retry cannot orphan it.
        let host_receipt_was_durable = governance_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !checkpoint.operation_id.is_empty());
        if let Err(error) = session.save(tools.as_ref()) {
            if !host_receipt_was_durable
                && let Err(compensation) = self
                    .engine
                    .governance
                    .abort_uncheckpointed_run(
                        &handle,
                        &format!("initial checkpoint failed: {error}"),
                    )
                    .await
            {
                return Err(RunError::Message(format!(
                    "initial checkpoint failed: {error}; governance receipt compensation failed: {compensation}"
                )));
            }
            return Err(error);
        }

        self.engine
            .governance
            .recover_staged_tool_executions(&handle)
            .await?;
        if let Err(error) = self
            .engine
            .governance
            .replay_staged_tool_reports(&handle)
            .await
            && self.engine.config.requires_governance()
        {
            return Err(error.into());
        }

        // Persist any replay cursor changes before the first turn begins.
        session.save(tools.as_ref())?;

        if let Err(error) = hooks::run_hooks(
            &self.engine.config.hooks,
            HookEvent::PreRun,
            json!({
                "run_id": session.run_id,
                "task": session.task,
                "resume": request.resume_run_id.is_some(),
            }),
        )
        .await
        {
            return Err(RunError::Governance(GovernanceError::Message(format!(
                "pre-run hook failed; governed state remains checkpointed for resume: {error}"
            ))));
        }

        // Preserve an escalation park if reporting that park fails after the
        // park has already been durably written.
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
        );

        // Ok(Some(park)) when escalated; Ok(None) when finished normally.
        let result: Result<Option<ParkInfo>, RunError> = async {
            loop {
                let turn = model_turns.next(&mut session).await?;

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

                let outcome = DurableToolBatch::new(
                    self.engine,
                    &request,
                    started,
                    timeout,
                    &handle,
                    Arc::clone(&tools),
                )
                .execute(&turn, &mut session, &mut pending_park)
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
        .await;

        let park_info = match &result {
            Ok(park) => park.clone(),
            Err(_) => None,
        };
        let usage = model_turns.usage();

        let (success, final_summary, termination) = match result {
            Ok(_) => (success, final_summary, termination),
            Err(e) => {
                let summary = e.to_string();
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
                                summary: summary.clone(),
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
                // Do not delete workspace on cancel/timeout/max-turns so resume works.
                self.engine.emit(
                    &session.run_id,
                    HarnessEvent::RunFinished {
                        run_id: session.run_id.clone(),
                        success: false,
                        summary: summary.clone(),
                    },
                );
                // Reap after all error-path bookkeeping and immediately
                // before inventory so descendants cannot keep mutating the
                // workspace after the failed run has been recorded.
                tools.kill_background_jobs().await;
                self.engine.capture_artifacts(&session.run_id, &ws.path);
                return Err(e);
            }
        };

        if termination != RunTermination::Parked {
            let completion = self
                .engine
                .governance
                .complete_run(
                    &handle,
                    RunOutcome {
                        success,
                        summary: final_summary.clone(),
                        turns: session.turns,
                        termination: termination.as_str().into(),
                        workspace: ws.path.display().to_string(),
                    },
                )
                .await;
            if let Err(error) = completion {
                let _ = session.save_recoverable(None, tools.as_ref());
                tools.kill_background_jobs().await;
                self.engine.capture_artifacts(&session.run_id, &ws.path);
                return Err(error.into());
            }
            // Successful completion clears adapter-owned receipt correlation
            // from the durable checkpoint. Parked runs intentionally retain
            // it for their governed continuation.
            if let Err(error) = session.save(tools.as_ref()) {
                tools.kill_background_jobs().await;
                self.engine.capture_artifacts(&session.run_id, &ws.path);
                return Err(error);
            }
        }

        // Always reap background shells before taking the final inventory.
        tools.kill_background_jobs().await;
        let artifact_dir = self.engine.capture_artifacts(&session.run_id, &ws.path);

        // Keep workspace on park; only delete on successful non-park completion.
        if !session.keep_workspace && success && termination != RunTermination::Parked {
            let _ = self.engine.workspace.cleanup(&ws);
        }

        self.engine.emit(
            &session.run_id,
            HarnessEvent::RunFinished {
                run_id: session.run_id.clone(),
                success,
                summary: final_summary.clone(),
            },
        );

        let cost = CostEstimate::from_usage_and_rates(
            usage,
            self.engine.config.model.input_usd_micros_per_mtok,
            self.engine.config.model.output_usd_micros_per_mtok,
        );

        let _ = hooks::run_hooks(
            &self.engine.config.hooks,
            HookEvent::PostRun,
            json!({
                "run_id": session.run_id,
                "success": success,
                "termination": termination.as_str(),
                "summary": final_summary,
            }),
        )
        .await;

        Ok(RunResult {
            run_id: session.run_id,
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
            )
            .await
            .unwrap();

        assert_eq!(result.summary, "done");
        assert_eq!(result.turns, 1);
        let checkpoint = Checkpoint::load(&state.runs_dir(), run_id).unwrap();
        assert_eq!(checkpoint.completed_turns, 1);
        assert_eq!(checkpoint.messages.last().unwrap().content, "done");
    }
}
