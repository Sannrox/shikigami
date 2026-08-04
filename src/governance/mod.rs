//! Governance ports: none, local, http-callback, sekai-chisei.

mod http_callback;
mod local;
mod none;
use async_trait::async_trait;
use thiserror::Error;

use crate::checkpoint::{GovernanceCheckpoint, StagedToolExecution, StagedToolReport};
use crate::config::Config;
use crate::model::{ChatMessage, ModelTurn};
use crate::tools::ToolDef;

pub use http_callback::HttpCallbackGovernance;
pub use local::LocalGovernance;
pub use none::NoneGovernance;

/// A model exposed by the configured governance/model source.
///
/// Governed model availability is authoritative in sekai-chisei. Local
/// adapters may report only their configured model.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AvailableModel {
    pub provider: String,
    pub upstream_model: String,
    pub canonical_model: String,
    pub lifecycle: String,
}

#[cfg(feature = "governance-sekai-chisei")]
pub mod sekai_chisei;

/// Lineage for one harness attempt correlated to plane operations.
///
/// See [docs/identity.md](../../docs/identity.md) and ADR 0002.
#[derive(Debug, Clone)]
pub struct RunHandle {
    /// Harness attempt id (UUID). Equals plane `attempt_id`.
    pub run_id: String,
    /// Logical operation id for plane receipts / PlanExecution.
    /// Defaults to `run_id` when the caller does not supply a parent op.
    pub operation_id: String,
    /// Plane namespace for policy and harvest.
    pub namespace: String,
}

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub success: bool,
    pub summary: String,
    /// Completed model turns (0 if failed before any turn).
    pub turns: u32,
    /// Termination kind: completed | cancelled | timed_out | max_turns | failed
    pub termination: String,
    /// Host workspace path (non-authoritative; plane truth is events/receipts).
    pub workspace: String,
}

#[derive(Debug, Error)]
pub enum GovernanceError {
    #[error("governance: {0}")]
    Message(String),
    #[error("governance unavailable: {0}")]
    Unavailable(String),
    #[error("tool denied: {0}")]
    Denied(String),
}

#[async_trait]
pub trait GovernancePort: Send + Sync {
    fn id(&self) -> &'static str;
    fn health_detail(&self) -> String;
    fn health_ok(&self) -> bool;

    /// Return the models currently available to this governance adapter.
    ///
    /// The default is intentionally unsupported: model catalogs are an
    /// adapter capability, not part of the turn-loop contract.
    async fn available_models(&self) -> Result<Vec<AvailableModel>, GovernanceError> {
        Err(GovernanceError::Message(format!(
            "available model catalog is not supported by governance adapter `{}`",
            self.id()
        )))
    }

    /// Start a run. `logical_operation_id` maps to plane `operation_id` /
    /// `logical_operation_id` (defaults to `run_id` when `None`).
    async fn begin_run(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
    ) -> Result<RunHandle, GovernanceError>;

    /// Start or restore a run with durable governance correlation state.
    /// Adapters that have no remote receipt state use the ordinary begin path.
    async fn begin_run_with_checkpoint(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        _checkpoint: Option<&GovernanceCheckpoint>,
    ) -> Result<RunHandle, GovernanceError> {
        self.begin_run(run_id, task, logical_operation_id).await
    }

    /// Return adapter-owned correlation/retry state for the next local
    /// checkpoint. The plane remains authoritative for the receipt itself.
    fn checkpoint_state(&self, _run_id: &str) -> Option<GovernanceCheckpoint> {
        None
    }

    /// Whether a tool can apply a host-side effect that must not be replayed
    /// after an interrupted process. Adapters may refine this from policy
    /// risk; the conservative default protects unknown tools.
    fn tool_requires_execution_checkpoint(&self, _name: &str) -> bool {
        true
    }

    /// Produce the next model turn. Local adapters use the provided model port
    /// callback; sekai-chisei uses PlanExecution on the plane.
    async fn plan_turn(
        &self,
        handle: &RunHandle,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
        local_model: &dyn crate::model::ModelPort,
    ) -> Result<ModelTurn, GovernanceError>;

    /// Report the model result after the engine has durably staged it in the
    /// local checkpoint. Adapters without remote harvest treat this as a no-op.
    async fn report_model_turn(
        &self,
        _handle: &RunHandle,
        _ok: bool,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    /// Stage host tool outcomes before their effects are reported. This is
    /// kept separate from `report_tool` so a resume can drain the same list.
    async fn stage_tool_reports(
        &self,
        _handle: &RunHandle,
        _reports: Vec<StagedToolReport>,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    /// Replay staged reports after restoring a checkpoint. Implementations
    /// must use stable call ids and idempotent report keys.
    async fn replay_staged_tool_reports(&self, _handle: &RunHandle) -> Result<(), GovernanceError> {
        Ok(())
    }

    /// Persist an in-doubt marker before invoking a host-side effect.
    async fn stage_tool_execution(
        &self,
        _handle: &RunHandle,
        _execution: StagedToolExecution,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    /// Transition an authorization marker to the point immediately before
    /// the host effect begins. This boundary is what makes a resumed
    /// execution in-doubt rather than safely retryable.
    async fn mark_tool_execution_started(
        &self,
        _handle: &RunHandle,
        _call_id: &str,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    /// Advance an in-doubt marker after the host call returns. The marker is
    /// cleared only after a checkpoint containing the staged report exists.
    async fn mark_tool_execution_complete(
        &self,
        _handle: &RunHandle,
        _call_id: &str,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    async fn clear_staged_tool_executions(
        &self,
        _handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    /// Restored authorization-only markers may be retried with stable permit
    /// identities. Restored started/completed markers are in-doubt and must
    /// not be executed again without an operator decision.
    async fn recover_staged_tool_executions(
        &self,
        _handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    async fn authorize_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError>;

    /// Authorize a tool with the stable conversation/tool identity that will
    /// also be used for durable execution and permit idempotency. Adapters
    /// without identity-sensitive authorization retain the ordinary path.
    async fn authorize_tool_with_id(
        &self,
        handle: &RunHandle,
        _call_id: &str,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError> {
        self.authorize_tool(handle, name, args_json).await
    }

    async fn report_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError>;

    async fn report_tool_with_id(
        &self,
        _handle: &RunHandle,
        _call_id: &str,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        self.report_tool(_handle, name, ok, detail).await
    }

    /// Compensate a remote host receipt when its correlation could not be
    /// durably checkpointed. Adapters without remote receipts do nothing.
    async fn abort_uncheckpointed_run(
        &self,
        _handle: &RunHandle,
        _reason: &str,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    async fn complete_run(
        &self,
        handle: &RunHandle,
        outcome: RunOutcome,
    ) -> Result<(), GovernanceError>;
}

pub fn from_config(config: &Config) -> Result<Box<dyn GovernancePort>, GovernanceError> {
    match config.governance.adapter.as_str() {
        "none" => Ok(Box::new(NoneGovernance::from_config(config))),
        "local" => Ok(Box::new(LocalGovernance::from_config(config))),
        "http-callback" | "host-authz" => {
            Ok(Box::new(HttpCallbackGovernance::from_config(config)?))
        }
        "sekai-chisei" => {
            #[cfg(feature = "governance-sekai-chisei")]
            {
                Ok(Box::new(sekai_chisei::SekaiChiseiGovernance::from_config(
                    config,
                )?))
            }
            #[cfg(not(feature = "governance-sekai-chisei"))]
            {
                Err(GovernanceError::Unavailable(
                    "built without governance-sekai-chisei feature".into(),
                ))
            }
        }
        other => Err(GovernanceError::Message(format!(
            "unknown governance adapter `{other}`"
        ))),
    }
}
