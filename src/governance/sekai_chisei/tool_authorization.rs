//! Governed external-action authorization for one stable tool call.

use sekai_client::SdkErrorCode;

use crate::governance::{ApprovalState, resolve_approval_wait};

use super::{GovernanceError, RunHandle, SekaiChiseiGovernance, plane_session, proto};
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
    if governance.harvest.fallback_active(&handle.run_id)?
        && let Some(checkpoint) = governance.harvest.fallback(&handle.run_id)?
    {
        let now_ms = crate::digest::unix_now_ms_i64();
        let view = crate::fallback::view_from_checkpoint(
            &checkpoint,
            now_ms,
            governance.fallback_enabled,
            None,
            governance.fallback_allow_test_signatures,
        );
        crate::fallback::tool_permitted(&checkpoint, &view, name)?;
        return Ok(());
    }
    // Mid-run external-action authz is always fail-closed for the sekai-chisei
    // adapter: transport/build/RPC errors must never permit tool execution
    // (including destructive bash), regardless of governance.fail_closed.
    let client = plane_session::connect(governance).await?;
    if let Some(park) = governance.harvest.approval_park(&handle.run_id)
        && park.call_id == call_id
    {
        let request = build_request(
            governance,
            handle,
            call_id,
            name,
            args_json,
            Some(park.deadline_ms),
        )?;
        return resolve_parked_approval(governance, handle, &park, &request, client).await;
    }
    let request = build_request(governance, handle, call_id, name, args_json, None)?;
    let response: proto::chisei::AuthorizeExternalActionResponse = client
        .raw()
        .unary(
            "/chisei.ChiseiService/AuthorizeExternalAction",
            AuthorizeExternalActionRequest {
                request: Some(request.clone()),
                offline: false,
            },
            plane_session::call_options(
                governance,
                Some(&handle.namespace),
                Some(&request.operation_id),
                Some(&request.request_id),
            ),
        )
        .await
        .map_err(|error| plane_session::map_error("AuthorizeExternalAction", error))?;
    let decision = response
        .decision
        .ok_or_else(|| GovernanceError::Message("external-action missing decision".into()))?;
    let permit = match permit_for_decision(&decision, response.permit) {
        Ok(permit) => permit,
        Err(GovernanceError::RequireApproval {
            approval_id,
            authorization_id,
            request_digest,
            expires_at_ms,
            reason,
            ..
        }) => {
            return Err(GovernanceError::RequireApproval {
                approval_id,
                authorization_id: if authorization_id.is_empty() {
                    request.request_id.clone()
                } else {
                    authorization_id
                },
                request_digest: if request_digest.is_empty() {
                    return Err(GovernanceError::Message(
                        "external-action require_approval is missing request_digest".into(),
                    ));
                } else {
                    request_digest
                },
                expires_at_ms,
                deadline_ms: request.deadline_ms,
                reason,
            });
        }
        Err(error) => return Err(error),
    };
    redeem_permit(governance, handle, call_id, &request, permit, &client).await
}

async fn resolve_parked_approval(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    park: &crate::checkpoint::ApprovalPark,
    request: &ExternalActionRequest,
    client: plane_session::PlaneClient,
) -> Result<(), GovernanceError> {
    // The plane has no separate approval-read RPC. Resume resends the original
    // AuthorizeExternalAction (same deadline, same request_digest). An existing
    // require_approval record is returned as-is. After the operator approves,
    // the stored decision becomes permit; issuing that permit is plane-owned.
    // If current policy still says RequireApproval, a plane that refuses permit
    // replay is a plane-contract defect, not a new harness authorization.
    let response: proto::chisei::AuthorizeExternalActionResponse = client
        .raw()
        .unary(
            "/chisei.ChiseiService/AuthorizeExternalAction",
            AuthorizeExternalActionRequest {
                request: Some(request.clone()),
                offline: false,
            },
            plane_session::call_options(
                governance,
                Some(&handle.namespace),
                Some(&request.operation_id),
                Some(&request.request_id),
            ),
        )
        .await
        .map_err(|error| plane_session::map_error("AuthorizeExternalAction", error))?;
    let decision = response
        .decision
        .ok_or_else(|| GovernanceError::Message("external-action missing decision".into()))?;
    if !park.request_digest.is_empty()
        && !decision.request_digest.is_empty()
        && decision.request_digest != park.request_digest
    {
        return Err(GovernanceError::Denied(format!(
            "approval `{}` request digest does not match the parked authorization",
            park.approval_id
        )));
    }
    resolve_approval_wait(
        park,
        approval_state_from_decision(&decision),
        crate::governance::now_unix_ms(),
    )?;
    let permit = permit_for_decision(&decision, response.permit)?;
    redeem_permit(governance, handle, &park.call_id, request, permit, &client).await
}

async fn redeem_permit(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    call_id: &str,
    request: &ExternalActionRequest,
    permit: proto::chisei::ExternalActionPermit,
    client: &plane_session::PlaneClient,
) -> Result<(), GovernanceError> {
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
            plane_session::call_options(
                governance,
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
            // Always deny on redeem failure (no fail-open for plane adapters).
            return Err(if error.code == SdkErrorCode::Unauthenticated {
                GovernanceError::Unavailable(format!("RedeemExternalActionPermit: {error}"))
            } else if security_sensitive_failure {
                GovernanceError::Message(format!("RedeemExternalActionPermit: {error}"))
            } else {
                plane_session::map_error("RedeemExternalActionPermit", error)
            });
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
        "require_approval" => {
            if decision.approval_id.trim().is_empty() {
                return Err(GovernanceError::Message(
                    "external-action require_approval is missing approval_id".into(),
                ));
            }
            Err(GovernanceError::RequireApproval {
                approval_id: decision.approval_id.clone(),
                authorization_id: decision.authorization_id.clone(),
                request_digest: decision.request_digest.clone(),
                expires_at_ms: decision.expires_at_ms,
                deadline_ms: 0,
                reason: if decision.reason.is_empty() {
                    "approval required".into()
                } else {
                    decision.reason.clone()
                },
            })
        }
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

fn approval_state_from_decision(decision: &ExternalActionDecision) -> ApprovalState {
    match decision.decision.as_str() {
        "permit" => ApprovalState::Approved {
            permit_id: decision
                .permit
                .as_ref()
                .map(|permit| permit.permit_id.clone())
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| decision.authorization_id.clone()),
        },
        "require_approval" => ApprovalState::Pending,
        "deny" if decision.cancelled_at_ms > 0 => ApprovalState::Cancelled,
        "deny" => {
            let reason = decision.reason.to_ascii_lowercase();
            if reason.contains("expir") {
                ApprovalState::Expired
            } else if reason.contains("revok") {
                ApprovalState::Revoked
            } else if reason.contains("cancel") {
                ApprovalState::Cancelled
            } else {
                ApprovalState::Denied {
                    reason: if decision.reason.is_empty() {
                        "policy denied".into()
                    } else {
                        decision.reason.clone()
                    },
                }
            }
        }
        _ => ApprovalState::Denied {
            reason: format!("unexpected decision `{}`", decision.decision),
        },
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
    reuse_deadline_ms: Option<i64>,
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
        deadline_ms: reuse_deadline_ms
            .filter(|deadline| *deadline > 0)
            .unwrap_or_else(|| crate::digest::unix_now_ms_i64() + 120_000),
        estimated_cost_micros: 0,
        estimated_volume: 0,
        affected_resource_count: 1,
        rollback_capability: String::new(),
        required_host_capabilities: vec!["shikigami.tools".into()],
        idempotency_key: request_id,
        policy_project: handle.namespace.clone(),
    })
}
