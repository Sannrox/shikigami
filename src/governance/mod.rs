//! Governance ports: none, local, sekai-chisei.

mod local;
mod none;
use async_trait::async_trait;
use thiserror::Error;

use crate::config::Config;
use crate::model::{ChatMessage, ModelTurn};
use crate::tools::ToolDef;

pub use local::LocalGovernance;
pub use none::NoneGovernance;

#[cfg(feature = "governance-sekai-chisei")]
pub mod sekai_chisei;

#[derive(Debug, Clone)]
pub struct RunHandle {
    pub run_id: String,
    pub operation_id: String,
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

    async fn begin_run(&self, run_id: &str, task: &str) -> Result<RunHandle, GovernanceError>;

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

    async fn authorize_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError>;

    async fn report_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError>;

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
