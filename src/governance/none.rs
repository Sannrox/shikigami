use async_trait::async_trait;

use crate::checkpoint::{
    GovernanceCheckpoint, StagedToolExecution, StagedToolReport, ToolExecutionStatus,
};
use crate::config::Config;
use crate::content::ContentModelTurnV1;
use crate::model::{ChatMessage, ModelTurn};
use crate::tools::ToolDef;

use super::{
    ContentTurnContext, GovernanceError, GovernancePort, LocalDurability, RunHandle, RunOutcome,
};

pub struct NoneGovernance {
    enabled_tools: Vec<String>,
    durability: LocalDurability,
}

impl NoneGovernance {
    pub fn from_config(config: &Config) -> Self {
        Self {
            enabled_tools: config.tools.effective_enabled(),
            durability: LocalDurability::default(),
        }
    }
}

#[async_trait]
impl GovernancePort for NoneGovernance {
    fn id(&self) -> &'static str {
        "none"
    }

    fn health_detail(&self) -> String {
        "no external governance".into()
    }

    fn health_ok(&self) -> bool {
        true
    }

    async fn begin_run(
        &self,
        run_id: &str,
        _task: &str,
        logical_operation_id: Option<&str>,
    ) -> Result<RunHandle, GovernanceError> {
        let handle = RunHandle {
            run_id: run_id.into(),
            operation_id: logical_operation_id.unwrap_or(run_id).into(),
            namespace: "local".into(),
        };
        self.durability.begin(&handle, None)?;
        Ok(handle)
    }

    async fn begin_run_with_checkpoint(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        checkpoint: Option<&GovernanceCheckpoint>,
    ) -> Result<RunHandle, GovernanceError> {
        let handle = self.begin_run(run_id, task, logical_operation_id).await?;
        self.durability.begin(&handle, checkpoint)?;
        Ok(handle)
    }

    fn checkpoint_state(&self, run_id: &str) -> Option<GovernanceCheckpoint> {
        self.durability.checkpoint(run_id)
    }

    async fn stage_tool_execution(
        &self,
        handle: &RunHandle,
        execution: StagedToolExecution,
    ) -> Result<(), GovernanceError> {
        self.durability.stage(&handle.run_id, execution)
    }

    async fn mark_tool_execution_started(
        &self,
        handle: &RunHandle,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.durability
            .mark(&handle.run_id, call_id, ToolExecutionStatus::Started)
    }

    async fn mark_tool_execution_complete(
        &self,
        handle: &RunHandle,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.durability
            .mark(&handle.run_id, call_id, ToolExecutionStatus::Completed)
    }

    async fn clear_staged_tool_executions(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        self.durability.clear(&handle.run_id)
    }

    async fn recover_staged_tool_executions(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        self.durability.recover(&handle.run_id)
    }

    async fn stage_tool_reports(
        &self,
        handle: &RunHandle,
        reports: Vec<StagedToolReport>,
    ) -> Result<(), GovernanceError> {
        self.durability.stage_reports(&handle.run_id, reports)
    }

    async fn replay_staged_tool_reports(&self, handle: &RunHandle) -> Result<(), GovernanceError> {
        let reports = self.durability.pending_reports(&handle.run_id)?;
        for report in reports {
            self.report_tool_with_id(
                handle,
                &report.call_id,
                &report.name,
                report.ok,
                &report.detail,
            )
            .await?;
        }
        Ok(())
    }

    async fn report_tool_with_id(
        &self,
        handle: &RunHandle,
        call_id: &str,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        self.durability.commit_report(&handle.run_id, call_id)?;
        self.report_tool(handle, name, ok, detail).await
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

    async fn plan_content_turn(
        &self,
        _handle: &RunHandle,
        system: &str,
        context: ContentTurnContext<'_>,
    ) -> Result<ContentModelTurnV1, GovernanceError> {
        super::plan_local_content_turn(
            context.local_model,
            system,
            context.messages,
            context.tools,
            context.capabilities,
            context.resolver,
        )
        .await
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
                "tool `{name}` not enabled"
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
