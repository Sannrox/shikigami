//! Governed Run admission protocol behind the existing governance port seam.

use crate::checkpoint::GovernanceCheckpoint;
use crate::governance::{GovernanceError, RunHandle};

use super::{SekaiChiseiGovernance, harvest, proto};

pub(super) fn host_receipt_input(
    governance: &SekaiChiseiGovernance,
    run_id: &str,
    task: &str,
    logical_operation_id: &str,
) -> proto::chisei::ExecutionInput {
    proto::chisei::ExecutionInput {
        request_id: format!("shikigami-host:{run_id}"),
        namespace: governance.namespace.clone(),
        spec: task.into(),
        preferred_model: governance.preferred_model.clone(),
        preferred_runtime: String::new(),
        task_type: "agent".into(),
        priority: 0,
        user_id: governance.principal.clone(),
        estimated_tokens: 0,
        messages: vec![],
        tools: vec![],
        system: String::new(),
        max_tokens: 0,
        task_class: "shikigami-run".into(),
        logical_operation_id: logical_operation_id.into(),
        attempt_id: run_id.into(),
        route_override: String::new(),
    }
}

async fn create_host_receipt(
    governance: &SekaiChiseiGovernance,
    run_id: &str,
    task: &str,
    logical_operation_id: &str,
) -> Result<String, GovernanceError> {
    let client = governance.connect().await?;
    let plan = client
        .plan_execution(
            host_receipt_input(governance, run_id, task, logical_operation_id),
            governance.sdk_call_options(
                Some(&governance.namespace),
                Some(logical_operation_id),
                Some(&format!("shikigami-host:{run_id}")),
            ),
        )
        .await
        .map_err(|error| SekaiChiseiGovernance::sdk_error("PlanExecution host receipt", error))?;
    if plan.budget.as_ref().is_some_and(|budget| !budget.allowed) {
        return Err(GovernanceError::Denied(
            plan.budget
                .as_ref()
                .map(|budget| budget.reason.clone())
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| "host receipt budget denied".into()),
        ));
    }
    if !plan.executable {
        return Err(GovernanceError::Denied(
            plan.warnings
                .first()
                .cloned()
                .unwrap_or_else(|| "host receipt plan not executable".into()),
        ));
    }
    Ok(plan.plan_id)
}

pub(super) async fn admit(
    governance: &SekaiChiseiGovernance,
    run_id: &str,
    task: &str,
    logical_operation_id: Option<&str>,
    checkpoint: Option<&GovernanceCheckpoint>,
) -> Result<RunHandle, GovernanceError> {
    if governance.endpoint.trim().is_empty() {
        return Err(GovernanceError::Unavailable(
            "sekai-chisei endpoint not set".into(),
        ));
    }
    if let Err(error) = governance.probe().await
        && governance.fail_closed
    {
        return Err(error);
    }
    let checkpoint_lineage = checkpoint
        .filter(|state| !state.logical_operation_id.is_empty())
        .map(|state| state.logical_operation_id.as_str());
    if let (Some(requested), Some(restored)) = (logical_operation_id, checkpoint_lineage)
        && requested != restored
    {
        return Err(GovernanceError::Message(format!(
            "resume logical operation id `{requested}` does not match checkpoint lineage `{restored}`"
        )));
    }
    let handle = RunHandle {
        run_id: run_id.into(),
        operation_id: logical_operation_id
            .or(checkpoint_lineage)
            .unwrap_or(run_id)
            .to_string(),
        namespace: governance.namespace.clone(),
    };

    if let Some(checkpoint) = checkpoint {
        governance
            .harvest
            .restore(run_id, checkpoint, handle.operation_id.clone())?;
        reconcile_resume(governance, &handle, checkpoint).await?;
    } else {
        governance
            .harvest
            .start(run_id, handle.operation_id.clone())?;
    }
    if !governance.harvest.has_host_operation(run_id)? {
        match create_host_receipt(governance, run_id, task, &handle.operation_id).await {
            Ok(operation_id) => governance.update_host_plan(run_id, operation_id)?,
            Err(error) if governance.fail_closed => return Err(error),
            Err(_) => {}
        }
    }
    if governance.harvest.needs_attempt(run_id)? {
        let attributes = harvest::attempt_attributes(run_id, &handle.operation_id);
        if let Err(error) = governance
            .report_harvest_event(&handle, harvest::KIND_ATTEMPT, attributes, vec![])
            .await
            && governance.fail_closed
        {
            return Err(error);
        }
    }
    Ok(handle)
}

async fn reconcile_resume(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    checkpoint: &GovernanceCheckpoint,
) -> Result<(), GovernanceError> {
    if checkpoint.operation_id.is_empty() {
        return Ok(());
    }
    reject_terminal_receipt(governance, handle, checkpoint, "references").await?;
    if let Err(error) = governance.retry_pending_harvest_event(handle).await
        && governance.fail_closed
    {
        return Err(error);
    }
    reject_terminal_receipt(
        governance,
        handle,
        checkpoint,
        "became complete while replaying",
    )
    .await
}

async fn reject_terminal_receipt(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    checkpoint: &GovernanceCheckpoint,
    phase: &str,
) -> Result<(), GovernanceError> {
    match governance.harvest_receipt(handle).await {
        Ok(receipt) if receipt.complete => {
            governance.forget_harvest(&handle.run_id);
            Err(GovernanceError::Message(format!(
                "resume checkpoint {phase} completed host receipt {}; refusing to resume terminal run",
                checkpoint.operation_id
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if governance.fail_closed => Err(error),
        Err(_) => Ok(()),
    }
}
