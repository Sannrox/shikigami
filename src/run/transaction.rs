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
use crate::governance::RunOutcome;
use crate::hooks::{self, HookEvent};
use crate::model::CostEstimate;

use super::model_turn::DurableModelTurn;
use super::tool_batch::{DurableToolBatch, ToolBatchOutcome};
use super::{Engine, ParkInfo, RunError, RunRequest, RunResult, RunTermination};

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

        let super::preparation::PreparedRun {
            mut session,
            workspace: ws,
            tools,
            tool_defs,
            system_prompt,
            prompt_id,
            handle,
            governance_checkpoint,
        } = super::preparation::prepare(self.engine, &request, fresh_run_id, resume_checkpoint)
            .await?;

        let mut final_summary = String::from("completed without report");
        let mut success = false;
        let mut termination = RunTermination::Completed;

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
                super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
                    .finalize(&session.run_id, &ws.path, tools.as_ref())
                    .await;
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
                super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
                    .finalize(&session.run_id, &ws.path, tools.as_ref())
                    .await;
                return Err(error.into());
            }
            // Successful completion clears adapter-owned receipt correlation
            // from the durable checkpoint. Parked runs intentionally retain
            // it for their governed continuation.
            if let Err(error) = session.save(tools.as_ref()) {
                super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
                    .finalize(&session.run_id, &ws.path, tools.as_ref())
                    .await;
                return Err(error);
            }
        }

        // Always reap background shells before taking the final inventory.
        let artifact_dir = super::artifact_lifecycle::RunArtifactLifecycle::new(self.engine)
            .finalize(&session.run_id, &ws.path, tools.as_ref())
            .await;

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
}
