//! Complete Run admission and supervision behind one private interface.
//!
//! This module owns checkpoint preflight, local run ownership, independent
//! heartbeat publication, Run transaction invocation, and durable terminal
//! finalization. [`Engine`](super::Engine) remains the stable public interface.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::checkpoint::Checkpoint;

use super::resume::{configured_workspace_adapter, validate_resumed_workspace};
use super::transaction::RunTransaction;
use super::{Engine, RunError, RunRequest, RunResult, SYSTEM_PROMPT};

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
        let resume_checkpoint = self.preflight(&request, expected_checkpoint_digest)?;
        let run_id = request
            .resume_run_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        self.acquire_run(&run_id, &request)?;

        let heartbeat_task = self.spawn_heartbeat(run_id.clone());
        let result = RunTransaction::new(self.engine)
            .execute(request, run_id.clone(), resume_checkpoint)
            .await;
        heartbeat_task.abort();
        let _ = heartbeat_task.await;
        self.finish_run(&run_id, &result);
        result
    }

    fn preflight(
        &self,
        request: &RunRequest,
        expected_checkpoint_digest: Option<&str>,
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

    fn finish_run(&self, run_id: &str, result: &Result<RunResult, RunError>) {
        match result {
            Ok(result) => {
                let _ = self.engine.registry.finish_result(result);
            }
            Err(error) => {
                let _ = self.engine.registry.finish_error(run_id, error);
            }
        }
    }
}
