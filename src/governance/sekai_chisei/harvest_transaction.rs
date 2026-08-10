//! Durable local projection for one governed run's harvest transaction.
//!
//! Plane receipts remain authoritative. This private module concentrates the
//! checkpoint, causality, and tool-recovery state that lets the adapter resume
//! the plane protocol without exposing another port or adapter seam.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::checkpoint::{
    GovernanceCheckpoint, PendingGovernanceEvent, StagedToolExecution, StagedToolReport,
    ToolExecutionStatus,
};
use crate::governance::{GovernanceError, RunHandle};

#[derive(Default)]
struct HarvestState {
    host_operation_id: Option<String>,
    logical_operation_id: Option<String>,
    model_operation_id: Option<String>,
    model_reported: bool,
    last_event_id: Option<String>,
    pending_event: Option<PendingGovernanceEvent>,
    pending_tool_reports: Vec<StagedToolReport>,
    pending_tool_executions: Vec<StagedToolExecution>,
}

#[derive(Clone, Default)]
pub(super) struct HarvestTransaction {
    runs: Arc<Mutex<HashMap<String, HarvestState>>>,
}

impl HarvestTransaction {
    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, HarvestState>>, GovernanceError> {
        self.runs
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))
    }

    pub(super) fn start(
        &self,
        run_id: &str,
        logical_operation_id: String,
    ) -> Result<(), GovernanceError> {
        self.lock()?.insert(
            run_id.into(),
            HarvestState {
                logical_operation_id: Some(logical_operation_id),
                ..HarvestState::default()
            },
        );
        Ok(())
    }

    pub(super) fn restore(
        &self,
        run_id: &str,
        checkpoint: &GovernanceCheckpoint,
        logical_operation_id: String,
    ) -> Result<(), GovernanceError> {
        self.lock()?.insert(
            run_id.into(),
            HarvestState {
                host_operation_id: (!checkpoint.operation_id.is_empty())
                    .then(|| checkpoint.operation_id.clone()),
                logical_operation_id: Some(logical_operation_id),
                model_operation_id: (!checkpoint.model_operation_id.is_empty())
                    .then(|| checkpoint.model_operation_id.clone()),
                model_reported: checkpoint.model_reported,
                last_event_id: (!checkpoint.last_event_id.is_empty())
                    .then(|| checkpoint.last_event_id.clone()),
                pending_event: checkpoint.pending_event.clone(),
                pending_tool_reports: checkpoint.pending_tool_reports.clone(),
                pending_tool_executions: checkpoint.pending_tool_executions.clone(),
            },
        );
        Ok(())
    }

    pub(super) fn checkpoint(&self, run_id: &str) -> Option<GovernanceCheckpoint> {
        let runs = self.runs.lock().ok()?;
        let state = runs.get(run_id)?;
        let has_state = state.host_operation_id.is_some()
            || state.logical_operation_id.is_some()
            || state.model_operation_id.is_some()
            || state.model_reported
            || state.last_event_id.is_some()
            || state.pending_event.is_some()
            || !state.pending_tool_reports.is_empty()
            || !state.pending_tool_executions.is_empty();
        has_state.then(|| GovernanceCheckpoint {
            operation_id: state.host_operation_id.clone().unwrap_or_default(),
            logical_operation_id: state.logical_operation_id.clone().unwrap_or_default(),
            model_operation_id: state.model_operation_id.clone().unwrap_or_default(),
            model_reported: state.model_reported,
            last_event_id: state.last_event_id.clone().unwrap_or_default(),
            pending_event: state.pending_event.clone(),
            pending_tool_reports: state.pending_tool_reports.clone(),
            pending_tool_executions: state.pending_tool_executions.clone(),
        })
    }

    pub(super) fn set_host_operation(
        &self,
        run_id: &str,
        operation_id: String,
    ) -> Result<(), GovernanceError> {
        let mut runs = self.lock()?;
        let state = runs.entry(run_id.into()).or_default();
        state.host_operation_id = Some(operation_id);
        state.last_event_id = None;
        Ok(())
    }

    pub(super) fn set_model_operation(
        &self,
        run_id: &str,
        operation_id: String,
    ) -> Result<(), GovernanceError> {
        let mut runs = self.lock()?;
        let state = runs.entry(run_id.into()).or_default();
        state.model_operation_id = Some(operation_id);
        state.model_reported = false;
        Ok(())
    }

    pub(super) fn has_host_operation(&self, run_id: &str) -> Result<bool, GovernanceError> {
        Ok(self
            .lock()?
            .get(run_id)
            .is_some_and(|state| state.host_operation_id.is_some()))
    }

    pub(super) fn needs_attempt(&self, run_id: &str) -> Result<bool, GovernanceError> {
        Ok(self.lock()?.get(run_id).is_some_and(|state| {
            state.last_event_id.is_none() && state.host_operation_id.is_some()
        }))
    }

    pub(super) fn host_operation_id(&self, handle: &RunHandle) -> Result<String, GovernanceError> {
        self.lock()?
            .get(&handle.run_id)
            .and_then(|state| state.host_operation_id.clone())
            .ok_or_else(|| {
                GovernanceError::Message(
                    "operation receipt unavailable: host PlanExecution did not establish a receipt"
                        .into(),
                )
            })
    }

    pub(super) fn event_context(
        &self,
        handle: &RunHandle,
        requested_event_id: Option<String>,
    ) -> Result<(String, String, String), GovernanceError> {
        let runs = self.lock()?;
        let state = runs.get(&handle.run_id).ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: run has no harvest state".into(),
            )
        })?;
        let operation_id = state.host_operation_id.clone().ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: host PlanExecution did not establish a receipt"
                    .into(),
            )
        })?;
        let parent_event_id = state
            .last_event_id
            .clone()
            .unwrap_or_else(|| format!("{operation_id}:budget"));
        let event_id = requested_event_id
            .unwrap_or_else(|| format!("report:{operation_id}:{}", uuid::Uuid::new_v4()));
        Ok((operation_id, parent_event_id, event_id))
    }

    pub(super) fn pending_event(
        &self,
        run_id: &str,
    ) -> Result<Option<PendingGovernanceEvent>, GovernanceError> {
        Ok(self
            .lock()?
            .get(run_id)
            .and_then(|state| state.pending_event.clone()))
    }

    pub(super) fn stage_event(
        &self,
        run_id: &str,
        pending: PendingGovernanceEvent,
    ) -> Result<(), GovernanceError> {
        let mut runs = self.lock()?;
        let state = runs.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: run has no harvest state".into(),
            )
        })?;
        if state.pending_event.is_some() {
            return Err(GovernanceError::Message(
                "operation-event reporting has an unacknowledged event pending retry".into(),
            ));
        }
        state.pending_event = Some(pending);
        Ok(())
    }

    pub(super) fn commit_event(&self, run_id: &str, event_id: String, model: bool) {
        if let Ok(mut runs) = self.runs.lock()
            && let Some(state) = runs.get_mut(run_id)
        {
            state.last_event_id = Some(event_id);
            state.pending_event = None;
            state.model_reported |= model;
        }
    }

    pub(super) fn model_operation(
        &self,
        run_id: &str,
    ) -> Result<(Option<String>, bool), GovernanceError> {
        self.lock()?
            .get(run_id)
            .map(|state| (state.model_operation_id.clone(), state.model_reported))
            .ok_or_else(|| {
                GovernanceError::Message(
                    "model event reporting unavailable: model PlanExecution did not establish a receipt"
                        .into(),
                )
            })
    }

    pub(super) fn mark_model_reported(&self, run_id: &str) {
        if let Ok(mut runs) = self.runs.lock()
            && let Some(state) = runs.get_mut(run_id)
        {
            state.model_reported = true;
        }
    }

    pub(super) fn stage_tool_reports(
        &self,
        run_id: &str,
        reports: Vec<StagedToolReport>,
    ) -> Result<(), GovernanceError> {
        let mut runs = self.lock()?;
        let state = runs.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: run has no harvest state".into(),
            )
        })?;
        state.pending_tool_reports.extend(reports);
        Ok(())
    }

    pub(super) fn pending_tool_reports(
        &self,
        run_id: &str,
    ) -> Result<Vec<StagedToolReport>, GovernanceError> {
        Ok(self
            .lock()?
            .get(run_id)
            .map(|state| state.pending_tool_reports.clone())
            .unwrap_or_default())
    }

    pub(super) fn commit_tool_report(&self, run_id: &str, call_id: &str) {
        if let Ok(mut runs) = self.runs.lock()
            && let Some(state) = runs.get_mut(run_id)
            && let Some(index) = state
                .pending_tool_reports
                .iter()
                .position(|report| report.call_id == call_id)
        {
            state.pending_tool_reports.remove(index);
        }
    }

    pub(super) fn stage_tool_execution(
        &self,
        run_id: &str,
        execution: StagedToolExecution,
    ) -> Result<(), GovernanceError> {
        let mut runs = self.lock()?;
        let state = runs.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "tool execution staging unavailable: run has no harvest state".into(),
            )
        })?;
        state.pending_tool_executions.push(execution);
        Ok(())
    }

    pub(super) fn mark_tool_execution_started(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.transition_tool_execution(
            run_id,
            call_id,
            ToolExecutionStatus::Authorizing,
            ToolExecutionStatus::Started,
        )
    }

    pub(super) fn mark_tool_execution_complete(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.transition_tool_execution(
            run_id,
            call_id,
            ToolExecutionStatus::Started,
            ToolExecutionStatus::Completed,
        )
    }

    fn transition_tool_execution(
        &self,
        run_id: &str,
        call_id: &str,
        from: ToolExecutionStatus,
        to: ToolExecutionStatus,
    ) -> Result<(), GovernanceError> {
        let mut runs = self.lock()?;
        let state = runs.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "tool execution staging unavailable: run has no harvest state".into(),
            )
        })?;
        if let Some(execution) = state
            .pending_tool_executions
            .iter_mut()
            .find(|execution| execution.call_id == call_id && execution.status == from)
        {
            execution.status = to;
        }
        Ok(())
    }

    pub(super) fn clear_tool_executions(&self, run_id: &str) {
        if let Ok(mut runs) = self.runs.lock()
            && let Some(state) = runs.get_mut(run_id)
        {
            state.pending_tool_executions.clear();
        }
    }

    pub(super) fn recover_tool_executions(&self, run_id: &str) -> Result<(), GovernanceError> {
        let pending = self
            .lock()?
            .get(run_id)
            .map(|state| state.pending_tool_executions.clone())
            .unwrap_or_default();
        if pending.is_empty() {
            return Ok(());
        }
        if pending
            .iter()
            .all(|execution| execution.status == ToolExecutionStatus::Authorizing)
        {
            self.clear_tool_executions(run_id);
            return Ok(());
        }
        let details = pending
            .iter()
            .map(|execution| format!("{} ({:?})", execution.name, execution.status))
            .collect::<Vec<_>>()
            .join(", ");
        Err(GovernanceError::Message(format!(
            "tool execution state is in-doubt; inspect host effects before resuming: {details}"
        )))
    }

    pub(super) fn forget(&self, run_id: &str) {
        if let Ok(mut runs) = self.runs.lock() {
            runs.remove(run_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_round_trip_preserves_causal_state() {
        let transaction = HarvestTransaction::default();
        transaction.start("run-1", "logical-1".into()).unwrap();
        transaction
            .set_host_operation("run-1", "host-1".into())
            .unwrap();
        transaction
            .set_model_operation("run-1", "model-1".into())
            .unwrap();
        let handle = RunHandle {
            run_id: "run-1".into(),
            operation_id: "logical-1".into(),
            namespace: "default".into(),
        };
        let (_, parent, _) = transaction
            .event_context(&handle, Some("event-1".into()))
            .unwrap();
        assert_eq!(parent, "host-1:budget");
        transaction.commit_event("run-1", "event-1".into(), true);

        let checkpoint = transaction.checkpoint("run-1").unwrap();
        let restored = HarvestTransaction::default();
        restored
            .restore("run-1", &checkpoint, "logical-1".into())
            .unwrap();
        assert_eq!(restored.checkpoint("run-1"), Some(checkpoint));
        let (_, parent, _) = restored
            .event_context(&handle, Some("event-2".into()))
            .unwrap();
        assert_eq!(parent, "event-1");
    }

    #[test]
    fn recovery_clears_only_unredeemed_authorizations() {
        let transaction = HarvestTransaction::default();
        transaction.start("run-1", "logical-1".into()).unwrap();
        transaction
            .stage_tool_execution(
                "run-1",
                StagedToolExecution {
                    call_id: "call-1".into(),
                    name: "write_file".into(),
                    args_json: "{}".into(),
                    status: ToolExecutionStatus::Authorizing,
                },
            )
            .unwrap();
        transaction.recover_tool_executions("run-1").unwrap();
        assert!(
            transaction
                .checkpoint("run-1")
                .unwrap()
                .pending_tool_executions
                .is_empty()
        );
    }
}
