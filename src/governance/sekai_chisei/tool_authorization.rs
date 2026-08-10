//! Governed external-action authorization for one stable tool call.

use sekai_client::SdkErrorCode;

use super::{GovernanceError, RunHandle, SekaiChiseiGovernance, proto};
use proto::chisei::{
    AuthorizeExternalActionRequest, ExternalActionDecision, ExternalActionRequest,
    RedeemExternalActionPermitRequest,
};

/// Authorize and redeem the permit for one stable tool call before host execution.
pub(super) async fn authorize(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    call_id: &str,
    name: &str,
    args_json: &str,
) -> Result<(), GovernanceError> {
    if !requires_external_action(name) {
        return Ok(());
    }
    let client = match governance.connect().await {
        Ok(client) => client,
        Err(error) if governance.fail_closed => return Err(error),
        Err(_) => return Ok(()),
    };
    let request = match build_request(governance, handle, call_id, name, args_json) {
        Ok(request) => request,
        Err(error) if governance.fail_closed => return Err(error),
        Err(_) => return Ok(()),
    };
    let response: proto::chisei::AuthorizeExternalActionResponse = match client
        .raw()
        .unary(
            "/chisei.ChiseiService/AuthorizeExternalAction",
            AuthorizeExternalActionRequest {
                request: Some(request.clone()),
                offline: false,
            },
            governance.sdk_call_options(
                Some(&handle.namespace),
                Some(&request.operation_id),
                Some(&request.request_id),
            ),
        )
        .await
    {
        Ok(response) => response,
        Err(error) if governance.fail_closed => {
            return Err(SekaiChiseiGovernance::sdk_error(
                "AuthorizeExternalAction",
                error,
            ));
        }
        Err(_) => return Ok(()),
    };
    let decision = response
        .decision
        .ok_or_else(|| GovernanceError::Message("external-action missing decision".into()))?;
    let permit = permit_for_decision(&decision, response.permit)?;
    let redemption_response: proto::chisei::RedeemExternalActionPermitResponse = match client
        .raw()
        .unary(
            "/chisei.ChiseiService/RedeemExternalActionPermit",
            RedeemExternalActionPermitRequest {
                permit: Some(permit.clone()),
                executor: request.intended_executor.clone(),
                requesting_harness: request.requesting_harness.clone(),
                canonical_arguments_digest: request.canonical_arguments_digest.clone(),
                target_selectors: request.target_selectors.clone(),
                observed_preconditions: request.immutable_preconditions.clone(),
                host_capabilities: request.required_host_capabilities.clone(),
                idempotency_key: request.idempotency_key.clone(),
                execution_id: format!(
                    "shikigami-execution:{}",
                    SekaiChiseiGovernance::arguments_digest(&format!(
                        "{}:{call_id}",
                        handle.run_id
                    ))
                ),
                invoked_at_ms: 0,
            },
            governance.sdk_call_options(
                Some(&handle.namespace),
                Some(&request.operation_id),
                Some(&request.request_id),
            ),
        )
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let security_sensitive_failure = matches!(
                error.code,
                SdkErrorCode::PermissionDenied
                    | SdkErrorCode::Unauthenticated
                    | SdkErrorCode::FailedPrecondition
                    | SdkErrorCode::InvalidArgument
            );
            let governance_error = if error.code == SdkErrorCode::Unauthenticated {
                GovernanceError::Unavailable(format!("RedeemExternalActionPermit: {error}"))
            } else if security_sensitive_failure {
                GovernanceError::Message(format!("RedeemExternalActionPermit: {error}"))
            } else {
                SekaiChiseiGovernance::sdk_error("RedeemExternalActionPermit", error)
            };
            if governance.fail_closed || security_sensitive_failure {
                return Err(governance_error);
            }
            return Ok(());
        }
    };
    let redemption = redemption_response
        .redemption
        .ok_or_else(|| GovernanceError::Message("external-action redemption missing".into()))?;
    if redemption.permit_id != permit.permit_id || redemption.executor != request.intended_executor
    {
        return Err(GovernanceError::Message(
            "external-action redemption does not match the requested permit".into(),
        ));
    }
    Ok(())
}

pub(super) fn requires_external_action(name: &str) -> bool {
    !matches!(name, "report" | "escalate" | "todo_write")
}

pub(super) fn risk_class(name: &str) -> &'static str {
    match name {
        "bash" | "bash_background" | "bash_job_status" | "bash_job_logs" => "destructive",
        "write_file" | "edit" | "multi_edit" | "apply_patch" => "write",
        "read_file" | "glob" | "grep" | "web_fetch" => "read",
        _ => "write",
    }
}

pub(super) fn interpret_decision(decision: &ExternalActionDecision) -> Result<(), GovernanceError> {
    match decision.decision.as_str() {
        "permit" => Ok(()),
        "deny" => Err(GovernanceError::Denied(format!(
            "external-action denied: {}",
            if decision.reason.is_empty() {
                "policy denied"
            } else {
                &decision.reason
            }
        ))),
        "require_approval" => Err(GovernanceError::Denied(format!(
            "external-action requires approval (unsupported headless): {}",
            if decision.reason.is_empty() {
                "approval required"
            } else {
                &decision.reason
            }
        ))),
        other => Err(GovernanceError::Message(format!(
            "external-action unexpected decision `{other}`{}",
            if decision.reason.is_empty() {
                String::new()
            } else {
                format!(": {}", decision.reason)
            }
        ))),
    }
}

pub(super) fn permit_for_decision(
    decision: &ExternalActionDecision,
    permit: Option<proto::chisei::ExternalActionPermit>,
) -> Result<proto::chisei::ExternalActionPermit, GovernanceError> {
    interpret_decision(decision)?;
    permit.ok_or_else(|| GovernanceError::Message("external-action permit missing".into()))
}

pub(super) fn build_request(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    call_id: &str,
    name: &str,
    args_json: &str,
) -> Result<ExternalActionRequest, GovernanceError> {
    let action_identity =
        SekaiChiseiGovernance::arguments_digest(&format!("{}:{call_id}", handle.run_id));
    let request_id = format!("shikigami-action:{action_identity}");
    let risk_class = risk_class(name);
    Ok(ExternalActionRequest {
        version: "external-action.request/v1".into(),
        operation_id: governance.host_harvest_operation_id(handle)?,
        parent_operation_id: String::new(),
        attempt_id: handle.run_id.clone(),
        request_id: request_id.clone(),
        actor: governance.principal.clone(),
        namespace: handle.namespace.clone(),
        requesting_harness: "shikigami".into(),
        intended_executor: governance.principal.clone(),
        action_type: format!("shikigami.tool.{name}.{risk_class}/v1"),
        parameter_schema: "application/json".into(),
        canonical_arguments_digest: SekaiChiseiGovernance::arguments_digest(args_json),
        policy_summary: std::collections::HashMap::from([("tool".into(), name.to_string())]),
        target_selectors: vec![format!("project:{}/tool:{name}", handle.namespace)],
        immutable_preconditions: std::collections::HashMap::new(),
        risk_class: risk_class.into(),
        expected_effects: vec![format!("execute_tool:{name}")],
        requested_invocation_count: 1,
        deadline_ms: chrono::Utc::now().timestamp_millis() + 120_000,
        estimated_cost_micros: 0,
        estimated_volume: 0,
        affected_resource_count: 1,
        rollback_capability: String::new(),
        required_host_capabilities: vec!["shikigami.tools".into()],
        idempotency_key: request_id,
        policy_project: handle.namespace.clone(),
    })
}
