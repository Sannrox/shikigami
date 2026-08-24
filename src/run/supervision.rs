//! Complete Run admission and supervision behind one private interface.
//!
//! This module owns checkpoint preflight, local run ownership, independent
//! heartbeat publication, Run transaction invocation, and durable terminal
//! finalization. [`Engine`](super::Engine) remains the stable public interface.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::checkpoint::Checkpoint;
use crate::content::{
    ContentRunRequestV1, initial_messages_match, load_sidecar, resolve_accepted,
    resolve_terminal_summary, validate_messages,
};
use crate::model::CostEstimate;
use crate::replay::ReplayExecution;

use super::resume::{configured_workspace_adapter, validate_resumed_workspace};
use super::transaction::RunTransaction;
use super::{ContentExecution, Engine, RunError, RunRequest, RunResult, SYSTEM_PROMPT};

pub(super) fn check_bounds(
    engine: &Engine,
    run_id: &str,
    request: &RunRequest,
    started: tokio::time::Instant,
    timeout: Option<Duration>,
) -> Result<(), RunError> {
    engine
        .registry
        .heartbeat(run_id)
        .map_err(|error| RunError::Message(format!("run registry heartbeat failed: {error}")))?;
    if let Some(rx) = &request.cancel
        && *rx.borrow()
    {
        return Err(RunError::Cancelled);
    }
    if engine.registry.cancel_requested(run_id) {
        return Err(RunError::Cancelled);
    }
    if let Some(limit) = timeout
        && started.elapsed() >= limit
    {
        return Err(RunError::TimedOut(limit));
    }
    Ok(())
}

pub(super) struct RunSupervision<'a> {
    engine: &'a Engine,
}

impl<'a> RunSupervision<'a> {
    pub(super) fn new(engine: &'a Engine) -> Self {
        Self { engine }
    }

    pub(super) async fn execute(
        &self,
        request: RunRequest,
        expected_checkpoint_digest: Option<&str>,
    ) -> Result<RunResult, RunError> {
        self.execute_inner(request, expected_checkpoint_digest, None, None)
            .await
    }

    pub(super) async fn execute_replay(
        &self,
        request: RunRequest,
        replay: ReplayExecution,
    ) -> Result<RunResult, RunError> {
        self.execute_inner(request, None, Some(replay), None).await
    }

    pub(super) async fn execute_content(
        &self,
        request: ContentRunRequestV1,
    ) -> Result<RunResult, RunError> {
        validate_messages(&request.messages, &request.capabilities)
            .map_err(|error| RunError::Message(error.to_string()))?;
        if let Some(result) = self.recover_finalized_content(&request).await? {
            return Ok(result);
        }
        let ContentRunRequestV1 {
            task,
            messages,
            capabilities,
            resolver,
            keep_workspace,
            timeout,
            cancel,
            resume_run_id,
            logical_operation_id,
            restore_snapshot,
        } = request;
        let run_request = RunRequest {
            task,
            keep_workspace,
            timeout,
            cancel,
            resume_run_id,
            logical_operation_id,
            resume_answer: None,
            restore_snapshot,
        };
        self.execute_inner(
            run_request,
            None,
            None,
            Some(ContentExecution {
                messages,
                capabilities,
                resolver,
            }),
        )
        .await
    }

    async fn recover_finalized_content(
        &self,
        request: &ContentRunRequestV1,
    ) -> Result<Option<RunResult>, RunError> {
        let Some(run_id) = request.resume_run_id.as_deref() else {
            return Ok(None);
        };
        let checkpoint = Checkpoint::load(&self.engine.state_runs, run_id)?;
        let Some(binding) = checkpoint.content.as_ref() else {
            return Ok(None);
        };
        let sidecar = load_sidecar(&self.engine.state_runs, run_id, binding)
            .map_err(|error| RunError::Message(error.to_string()))?;
        let Some(terminal) = sidecar
            .terminal
            .as_ref()
            .filter(|terminal| terminal.finalized)
        else {
            return Ok(None);
        };
        if binding.resolver_id != request.resolver.id()
            || sidecar.capabilities != request.capabilities
            || !initial_messages_match(&sidecar, &request.messages)
            || (!request.task.is_empty() && request.task != checkpoint.task)
        {
            return Err(RunError::Message(format!(
                "content recovery binding changed for run {run_id}"
            )));
        }
        resolve_accepted(request.resolver.as_ref(), &sidecar.messages)
            .await
            .map_err(|error| RunError::Message(error.to_string()))?;
        let summary = resolve_terminal_summary(request.resolver.as_ref(), &sidecar)
            .await
            .map_err(|error| RunError::Message(error.to_string()))?
            .ok_or_else(|| RunError::Message("content terminal summary is missing".into()))?;
        let cost = CostEstimate::from_usage_and_rates(
            terminal.usage,
            self.engine.config.model.input_usd_micros_per_mtok,
            self.engine.config.model.output_usd_micros_per_mtok,
        );
        let result = RunResult {
            run_id: run_id.into(),
            success: terminal.success,
            summary,
            turns: sidecar.completed_turns,
            workspace: checkpoint.workspace,
            artifact_dir: terminal
                .artifact_dir
                .as_deref()
                .map(std::path::PathBuf::from),
            termination: terminal.termination,
            park: None,
            prompt_id: checkpoint.prompt_id,
            usage: terminal.usage,
            cost,
            todos: checkpoint.todos,
        };
        let mut projected = result.clone();
        projected.summary = format!(
            "bounded_content_result bytes={} digest={}",
            result.summary.len(),
            crate::content::sha256_digest(result.summary.as_bytes())
        );
        let _ = self.engine.registry.finish_result(&projected);
        Ok(Some(result))
    }

    async fn execute_inner(
        &self,
        request: RunRequest,
        expected_checkpoint_digest: Option<&str>,
        replay: Option<ReplayExecution>,
        content: Option<ContentExecution>,
    ) -> Result<RunResult, RunError> {
        let resume_checkpoint = self.preflight(
            &request,
            expected_checkpoint_digest,
            replay.as_ref(),
            content.as_ref(),
        )?;
        let run_id = request
            .resume_run_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let content_run = content.is_some();
        self.acquire_run(&run_id, &request)?;

        let heartbeat_task = self.spawn_heartbeat(run_id.clone());
        let result = RunTransaction::new(self.engine)
            .execute(request, run_id.clone(), resume_checkpoint, replay, content)
            .await;
        heartbeat_task.abort();
        let _ = heartbeat_task.await;
        self.finish_run(&run_id, &result, content_run);
        result
    }

    fn preflight(
        &self,
        request: &RunRequest,
        expected_checkpoint_digest: Option<&str>,
        replay: Option<&ReplayExecution>,
        content: Option<&ContentExecution>,
    ) -> Result<Option<Checkpoint>, RunError> {
        let Some(resume_id) = &request.resume_run_id else {
            if expected_checkpoint_digest.is_some() {
                return Err(RunError::Message(
                    "checkpoint digest requires a resumed run".into(),
                ));
            }
            return Ok(None);
        };

        let (checkpoint, digest) =
            Checkpoint::load_with_digest(&self.engine.state_runs, resume_id)?;
        if expected_checkpoint_digest.is_some_and(|expected| expected != digest) {
            return Err(RunError::Message(format!(
                "checkpoint digest mismatch for run {resume_id}"
            )));
        }
        match (&checkpoint.replay, replay) {
            (Some(_), None) => {
                return Err(RunError::Message(
                    "replay checkpoints must be resumed through Harness::replay".into(),
                ));
            }
            (None, Some(_)) => {
                return Err(RunError::Message(format!(
                    "run {resume_id} is not a replay attempt"
                )));
            }
            (Some(checkpoint_replay), Some(replay))
                if checkpoint_replay.manifest_digest != replay.manifest_digest =>
            {
                return Err(RunError::Message(format!(
                    "replay manifest digest mismatch for run {resume_id}"
                )));
            }
            (Some(checkpoint_replay), Some(_)) if checkpoint_replay.terminal.is_some() => {
                return Err(RunError::Message(format!(
                    "replay attempt {resume_id} is already terminal and cannot be restarted"
                )));
            }
            (Some(checkpoint_replay), Some(replay))
                if checkpoint_replay.source_run_id != replay.source_run_id =>
            {
                return Err(RunError::Message(format!(
                    "replay source identity mismatch for run {resume_id}"
                )));
            }
            (Some(checkpoint_replay), Some(_))
                if checkpoint_replay.workspace != checkpoint.workspace.display().to_string() =>
            {
                return Err(RunError::Message(format!(
                    "replay workspace binding mismatch for run {resume_id}"
                )));
            }
            _ => {}
        }
        match (&checkpoint.content, content) {
            (Some(_), None) => {
                return Err(RunError::Message(
                    "content checkpoints must be resumed through Harness::run_content".into(),
                ));
            }
            (None, Some(_)) => {
                return Err(RunError::Message(format!(
                    "run {resume_id} is not a bounded content run"
                )));
            }
            (Some(binding), Some(content)) if binding.resolver_id != content.resolver.id() => {
                return Err(RunError::Message(format!(
                    "content resolver binding mismatch for run {resume_id}"
                )));
            }
            _ => {}
        }
        checkpoint.validate_prompt(SYSTEM_PROMPT)?;
        let _ = validate_resumed_workspace(
            &self.engine.config,
            &self.engine.state_runs,
            resume_id,
            &checkpoint,
        )?;
        let workspace_adapter = if checkpoint.workspace_adapter.is_empty() {
            configured_workspace_adapter(&self.engine.config)
        } else {
            checkpoint.workspace_adapter.as_str()
        };
        if request.restore_snapshot.is_some() && workspace_adapter == "inplace" {
            return Err(RunError::Message(
                "restore_snapshot is not supported with workspace adapter `inplace`".into(),
            ));
        }
        if checkpoint.park.is_some() && request.resume_answer.is_none() {
            return Err(RunError::Message(format!(
                "run {resume_id} is parked; supply resume_answer / --answer to continue"
            )));
        }
        if checkpoint.park.is_none() && request.resume_answer.is_some() {
            return Err(RunError::Message(
                "resume_answer provided but run is not parked".into(),
            ));
        }
        Ok(Some(checkpoint))
    }

    fn acquire_run(&self, run_id: &str, request: &RunRequest) -> Result<(), RunError> {
        self.engine
            .registry
            .start(
                run_id,
                &request.task,
                request.logical_operation_id.as_deref(),
                None,
            )
            .map_err(|error| RunError::Message(format!("run registry start failed: {error}")))
    }

    fn spawn_heartbeat(&self, run_id: String) -> tokio::task::JoinHandle<()> {
        let registry = Arc::clone(&self.engine.registry);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                if registry.heartbeat(&run_id).is_err() {
                    break;
                }
            }
        })
    }

    fn finish_run(&self, run_id: &str, result: &Result<RunResult, RunError>, content_run: bool) {
        match result {
            Ok(result) => {
                if content_run {
                    let mut projected = result.clone();
                    projected.summary = format!(
                        "bounded_content_result bytes={} digest={}",
                        result.summary.len(),
                        crate::content::sha256_digest(result.summary.as_bytes())
                    );
                    let _ = self.engine.registry.finish_result(&projected);
                } else {
                    let _ = self.engine.registry.finish_result(result);
                }
            }
            Err(error) => {
                let _ = self.engine.registry.finish_error(run_id, error);
            }
        }
    }
}
