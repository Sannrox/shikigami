//! Governed harvest event reporting behind the existing governance port seam.
//!
//! Owns stage → ReportOperationEvent → commit/retry, model/tool event digests,
//! and abort-before-model finalization. Local harvest checkpoint state remains
//! in [`super::harvest_transaction::HarvestTransaction`].

use std::collections::{BTreeMap, HashMap};

use crate::checkpoint::PendingGovernanceEvent;
use crate::governance::{GovernanceError, RunHandle, RunOutcome};

use super::{SekaiChiseiGovernance, harvest, proto};
use proto::chisei::{GetOperationReceiptRequest, ReportOperationEventRequest};

pub(super) async fn send_pending(
    governance: &SekaiChiseiGovernance,
    pending: &PendingGovernanceEvent,
) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
    let client = governance.connect().await?;
    client
        .report_operation_event(
            ReportOperationEventRequest {
                operation_id: pending.operation_id.clone(),
                event_id: pending.event_id.clone(),
                parent_event_id: pending.parent_event_id.clone(),
                timestamp_ms: pending.timestamp_ms,
                kind: pending.kind.clone(),
                attributes: pending.attributes.clone().into_iter().collect(),
                references: SekaiChiseiGovernance::proto_event_references(&pending.references),
            },
            governance.sdk_call_options(
                Some(&governance.namespace),
                Some(&pending.operation_id),
                Some(&pending.event_id),
            ),
        )
        .await
        .map_err(|error| SekaiChiseiGovernance::sdk_error("ReportOperationEvent", error))
}

pub(super) async fn retry_pending(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
) -> Result<(), GovernanceError> {
    let pending = governance.harvest.pending_event(&handle.run_id)?;
    let Some(pending) = pending else {
        return Ok(());
    };
    let response = send_pending(governance, &pending).await?;
    if !response.recorded && response.event_id != pending.event_id {
        return Err(GovernanceError::Message(format!(
            "ReportOperationEvent did not record pending event {}",
            pending.event_id
        )));
    }
    let model = pending_marks_model_reported(&pending);
    governance
        .harvest
        .commit_event(&handle.run_id, pending.event_id, model);
    Ok(())
}

pub(super) async fn report_with_id(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    kind: &str,
    attributes: HashMap<String, String>,
    references: Vec<proto::chisei::OperationEvidenceReference>,
    event_id: Option<String>,
) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
    let (operation_id, parent_event_id, event_id) =
        governance.harvest_event_context_with_id(handle, event_id)?;
    let pending = PendingGovernanceEvent {
        operation_id,
        event_id,
        parent_event_id,
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        kind: kind.into(),
        attributes: attributes.into_iter().collect::<BTreeMap<_, _>>(),
        references: SekaiChiseiGovernance::pending_event_references(&references),
    };
    governance
        .harvest
        .stage_event(&handle.run_id, pending.clone())?;
    let response = send_pending(governance, &pending).await?;
    if !response.recorded && response.event_id != pending.event_id {
        return Err(GovernanceError::Message(format!(
            "ReportOperationEvent did not record event {}",
            pending.event_id
        )));
    }
    // Match retry_pending: model events must flip model_reported on commit so
    // completion/backfill paths that use KIND_MODEL stay consistent with the
    // dedicated report_model helper.
    let model = pending_marks_model_reported(&pending);
    governance
        .harvest
        .commit_event(&handle.run_id, pending.event_id, model);
    Ok(response)
}

pub(super) async fn report(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    kind: &str,
    attributes: HashMap<String, String>,
    references: Vec<proto::chisei::OperationEvidenceReference>,
) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
    report_with_id(governance, handle, kind, attributes, references, None).await
}

pub(super) async fn report_model(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    ok: bool,
) -> Result<(), GovernanceError> {
    let (model_operation_id, model_reported) =
        governance.harvest.model_operation(&handle.run_id)?;
    if model_reported {
        return Ok(());
    }
    let model_operation_id = model_operation_id.ok_or_else(|| {
        GovernanceError::Message(
            "model event reporting unavailable: model PlanExecution did not establish a receipt"
                .into(),
        )
    })?;
    let host_operation_id = governance.host_harvest_operation_id(handle)?;
    let event_id = format!(
        "report:{host_operation_id}:model:{}",
        SekaiChiseiGovernance::arguments_digest(&format!(
            "{}:{}",
            handle.run_id, model_operation_id
        ))
    );
    report_with_id(
        governance,
        handle,
        harvest::KIND_MODEL,
        harvest::model_attributes(&model_operation_id, ok),
        vec![],
        Some(event_id),
    )
    .await?;
    governance.harvest.mark_model_reported(&handle.run_id);
    Ok(())
}

pub(super) async fn report_failed_model(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
) -> Result<(), GovernanceError> {
    match report_model(governance, handle, false).await {
        Ok(()) => Ok(()),
        Err(error) if governance.fail_closed => Err(error),
        Err(_) => Ok(()),
    }
}

pub(super) async fn report_tool(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    call_id: Option<&str>,
    name: &str,
    ok: bool,
    detail: &str,
) -> Result<(), GovernanceError> {
    let event_id = match call_id.filter(|call_id| !call_id.is_empty()) {
        Some(call_id) => {
            let operation_id = governance.host_harvest_operation_id(handle)?;
            Some(format!(
                "report:{operation_id}:tool:{}",
                SekaiChiseiGovernance::arguments_digest(&format!("{}:{call_id}", handle.run_id))
            ))
        }
        None => None,
    };
    report_with_id(
        governance,
        handle,
        harvest::KIND_TOOL,
        harvest::tool_attributes(name, ok, detail),
        vec![],
        event_id,
    )
    .await?;
    if let Some(call_id) = call_id {
        governance
            .harvest
            .commit_tool_report(&handle.run_id, call_id);
    }
    Ok(())
}

pub(super) async fn harvest_receipt(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
) -> Result<proto::chisei::GetOperationReceiptResponse, GovernanceError> {
    let operation_id = governance.host_harvest_operation_id(handle)?;
    let client = governance.connect().await?;
    client
        .get_operation_receipt(
            GetOperationReceiptRequest {
                operation_id: operation_id.clone(),
                request_id: String::new(),
                caller_scope: String::new(),
                attempt: 0,
            },
            governance.sdk_call_options(Some(&governance.namespace), Some(&operation_id), None),
        )
        .await
        .map_err(|error| SekaiChiseiGovernance::sdk_error("GetOperationReceipt", error))
}

pub(super) async fn abort_uncheckpointed_receipt(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    reason: &str,
) -> Result<(), GovernanceError> {
    if !governance.harvest.has_host_operation(&handle.run_id)? {
        return Ok(());
    }

    retry_pending(governance, handle).await?;
    let receipt = harvest_receipt(governance, handle).await?;
    if receipt.complete {
        governance.forget_harvest(&handle.run_id);
        return Ok(());
    }
    if receipt
        .missing_surfaces
        .iter()
        .any(|surface| surface == "attempt")
    {
        let attributes = harvest::attempt_attributes(&handle.run_id, &handle.operation_id);
        report(
            governance,
            handle,
            harvest::KIND_ATTEMPT,
            attributes,
            vec![],
        )
        .await?;
    }

    let outcome = RunOutcome {
        success: false,
        summary: reason.chars().take(4000).collect(),
        turns: 0,
        termination: "aborted_before_model".into(),
        workspace: String::new(),
    };
    let response = report(
        governance,
        handle,
        harvest::KIND_COMPLETE,
        harvest::complete_attributes(&outcome),
        harvest::complete_references(handle, &outcome),
    )
    .await?;
    if !response.complete {
        return Err(GovernanceError::Message(format!(
            "uncheckpointed host receipt remains incomplete; missing surfaces: {}",
            response.missing_surfaces.join(", ")
        )));
    }
    governance.forget_harvest(&handle.run_id);
    Ok(())
}

/// Whether committing this pending event should flip local `model_reported`.
pub(super) fn pending_marks_model_reported(pending: &PendingGovernanceEvent) -> bool {
    pending.kind == harvest::KIND_MODEL
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::PendingGovernanceEvent;

    #[test]
    fn model_kind_pending_marks_model_reported_like_retry_path() {
        let pending = PendingGovernanceEvent {
            operation_id: "host-1".into(),
            event_id: "event-1".into(),
            parent_event_id: "host-1:budget".into(),
            timestamp_ms: 0,
            kind: harvest::KIND_MODEL.into(),
            attributes: Default::default(),
            references: vec![],
        };
        assert!(pending_marks_model_reported(&pending));
        let pending = PendingGovernanceEvent {
            kind: harvest::KIND_TOOL.into(),
            ..pending
        };
        assert!(!pending_marks_model_reported(&pending));
    }
}
