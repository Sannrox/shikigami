use async_trait::async_trait;

use crate::config::Config;
use crate::model::{ChatMessage, ModelTurn};
use crate::tools::ToolDef;

use super::{GovernanceError, GovernancePort, RunHandle, RunOutcome};

/// In-process policy: tool allow-list only (deterministic tests).
pub struct LocalGovernance {
    principal: String,
    enabled_tools: Vec<String>,
}

impl LocalGovernance {
    pub fn from_config(config: &Config) -> Self {
        Self {
            principal: config.governance.principal.clone(),
            enabled_tools: config.tools.effective_enabled(),
        }
    }
}

#[async_trait]
impl GovernancePort for LocalGovernance {
    fn id(&self) -> &'static str {
        "local"
    }

    fn health_detail(&self) -> String {
        format!("in-process allow-list (principal {})", self.principal)
    }

    fn health_ok(&self) -> bool {
        true
    }

    async fn begin_run(&self, run_id: &str, _task: &str) -> Result<RunHandle, GovernanceError> {
        Ok(RunHandle {
            run_id: run_id.into(),
            operation_id: format!("local-{run_id}"),
            namespace: "local".into(),
        })
    }

    async fn plan_turn(
        &self,
        _handle: &RunHandle,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
        local_model: &dyn crate::model::ModelPort,
    ) -> Result<ModelTurn, GovernanceError> {
        local_model
            .next_turn(system, messages, tools)
            .await
            .map_err(|e| GovernanceError::Message(e.to_string()))
    }

    async fn authorize_tool(
        &self,
        _handle: &RunHandle,
        name: &str,
        _args_json: &str,
    ) -> Result<(), GovernanceError> {
        if self.enabled_tools.iter().any(|t| t == name) {
            Ok(())
        } else {
            Err(GovernanceError::Denied(format!(
                "local policy denies tool `{name}`"
            )))
        }
    }

    async fn report_tool(
        &self,
        _handle: &RunHandle,
        _name: &str,
        _ok: bool,
        _detail: &str,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    async fn complete_run(
        &self,
        _handle: &RunHandle,
        _outcome: RunOutcome,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }
}
