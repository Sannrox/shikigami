//! Plane claim acquisition and lease protocol behind the existing plane intake seam.
//!
//! Owns list → claim → continuation validation → instance lookup → pre-run
//! fence renew, plus post-admit heartbeat, ack, and claim-event reporting.

use std::time::{Duration, Instant};

use sekai_client::{SdkError, SdkErrorCode};

use super::{SekaiClaimClient, plane_intake_source, proto};
use proto::sekai::{
    AckActionWorkRequest, ClaimActionWorkRequest, GetActionInstanceRequest,
    HeartbeatActionClaimRequest, ListClaimableActionWorkRequest, ReportActionClaimEventRequest,
};

/// Acquire the next fenced claim, or `Ok(None)` when idle or lost to contention.
pub(super) async fn claim_next(
    client: &SekaiClaimClient,
    runtime_id: &str,
    ttl: Duration,
) -> Result<Option<crate::plane_intake::PlaneClaim>, crate::plane_intake::PlaneIntakeError> {
    let plane = client.inner.connect().await.map_err(plane_intake_source)?;
    let listed: proto::sekai::ListClaimableActionWorkResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/ListClaimableActionWork",
            ListClaimableActionWorkRequest {
                namespace: client.namespace.clone(),
                runtime_id: runtime_id.into(),
                limit: 1,
            },
            client
                .inner
                .sdk_call_options(Some(&client.namespace), None, None),
        )
        .await
        .map_err(|error| {
            crate::plane_intake::PlaneIntakeError::Source(format!(
                "ListClaimableActionWork: {error}"
            ))
        })?;
    let Some(candidate) = listed.effects.into_iter().next() else {
        return Ok(None);
    };
    let claim_request_id = uuid::Uuid::new_v4().to_string();
    let claimed: proto::sekai::ClaimActionWorkResponse = match plane
        .raw()
        .unary(
            "/sekai.SekaiService/ClaimActionWork",
            ClaimActionWorkRequest {
                effect_id: candidate.effect_id,
                runtime_id: runtime_id.into(),
                request_id: claim_request_id.clone(),
                ttl_ms: duration_millis(ttl)?,
            },
            client
                .inner
                .sdk_call_options(Some(&client.namespace), None, Some(&claim_request_id)),
        )
        .await
    {
        Ok(response) => response,
        Err(error) if is_claim_contention(&error) => return Ok(None),
        Err(error) => {
            return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                "ClaimActionWork: {error}"
            )));
        }
    };
    let continuation = project_continuation(claimed.continuation, claimed.park)?;
    let effect = claimed.effect.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source("ClaimActionWork returned no effect".into())
    })?;
    let instance_response: proto::sekai::GetActionInstanceResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/GetActionInstance",
            GetActionInstanceRequest {
                instance_id: effect.instance_id.clone(),
                namespace: String::new(),
                idempotency_key: String::new(),
            },
            client
                .inner
                .sdk_call_options(Some(&client.namespace), None, None),
        )
        .await
        .map_err(|error| {
            crate::plane_intake::PlaneIntakeError::Source(format!("GetActionInstance: {error}"))
        })?;
    let instance = instance_response.instance.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "GetActionInstance returned no instance".into(),
        )
    })?;
    // Parameter lookup happens after claim and may consume most of the
    // initial TTL. Revalidate and renew the same fence before the host is
    // allowed to start the run.
    let renew_started = Instant::now();
    let effect_response: proto::sekai::HeartbeatActionClaimResponse = match plane
        .raw()
        .unary(
            "/sekai.SekaiService/HeartbeatActionClaim",
            HeartbeatActionClaimRequest {
                effect_id: effect.effect_id,
                runtime_id: effect.claim_owner,
                claim_generation: effect.claim_generation,
                fencing_token: effect.claim_fencing_token,
                ttl_ms: duration_millis(ttl)?,
            },
            client
                .inner
                .sdk_call_options(Some(&client.namespace), None, None),
        )
        .await
    {
        Ok(response) => response,
        // ClaimActionWork already succeeded: FailedPrecondition is fence loss,
        // not idle contention. Mapping to Ok(None) would hide FenceLost from the
        // serve loop and keep the worker accepting claims.
        Err(error) => return Err(map_owned_claim_heartbeat_error(&error)),
    };
    let effect = effect_response.effect.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "HeartbeatActionClaim before run returned no effect".into(),
        )
    })?;

    Ok(Some(crate::plane_intake::PlaneClaim {
        work: crate::plane_intake::ClaimedPlaneWork {
            effect_id: effect.effect_id,
            instance_id: effect.instance_id,
            operation_id: effect.operation_id,
            kind: effect.kind,
            status: effect.status,
            payload_json: effect.payload_json,
            parameters_json: instance.parameters_json,
            resolved_task: None,
            continuation,
        },
        lease: crate::plane_intake::PlaneClaimLease {
            runtime_id: effect.claim_owner,
            generation: effect.claim_generation,
            fencing_token: effect.claim_fencing_token,
            expires_at_ms: effect.claim_expires_at_ms,
            valid_until: renew_started + ttl,
        },
    }))
}

/// Renew an owned claim fence and return the updated lease.
pub(super) async fn heartbeat(
    client: &SekaiClaimClient,
    claim: &crate::plane_intake::PlaneClaim,
    ttl: Duration,
) -> Result<crate::plane_intake::PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
    let renew_started = Instant::now();
    let plane = client.inner.connect().await.map_err(plane_intake_source)?;
    let response: proto::sekai::HeartbeatActionClaimResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/HeartbeatActionClaim",
            HeartbeatActionClaimRequest {
                effect_id: claim.work.effect_id.clone(),
                runtime_id: claim.lease.runtime_id.clone(),
                claim_generation: claim.lease.generation,
                fencing_token: claim.lease.fencing_token.clone(),
                ttl_ms: duration_millis(ttl)?,
            },
            client
                .inner
                .sdk_call_options(Some(&client.namespace), None, None),
        )
        .await
        .map_err(|error| map_lease_rpc_error("HeartbeatActionClaim", &error))?;
    let effect = response.effect.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "HeartbeatActionClaim returned no effect".into(),
        )
    })?;
    Ok(crate::plane_intake::PlaneClaimLease {
        runtime_id: effect.claim_owner,
        generation: effect.claim_generation,
        fencing_token: effect.claim_fencing_token,
        expires_at_ms: effect.claim_expires_at_ms,
        valid_until: renew_started + ttl,
    })
}

/// Acknowledge a claimed effect with the plane.
pub(super) async fn ack(
    client: &SekaiClaimClient,
    claim: &crate::plane_intake::PlaneClaim,
    ack: &crate::plane_intake::PlaneAck,
) -> Result<(), crate::plane_intake::PlaneIntakeError> {
    let plane = client.inner.connect().await.map_err(plane_intake_source)?;
    let _: proto::sekai::AckActionWorkResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/AckActionWork",
            AckActionWorkRequest {
                effect_id: claim.work.effect_id.clone(),
                runtime_id: claim.lease.runtime_id.clone(),
                claim_generation: claim.lease.generation,
                fencing_token: claim.lease.fencing_token.clone(),
                outcome: ack.outcome.as_str().into(),
                reason: ack.reason.clone(),
                request_id: ack.request_id.clone(),
                checkpoint_store_id: ack
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.store_id.clone())
                    .unwrap_or_default(),
                checkpoint_ref: ack
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.reference.clone())
                    .unwrap_or_default(),
                checkpoint_digest: ack
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.digest.clone())
                    .unwrap_or_default(),
            },
            client
                .inner
                .sdk_call_options(Some(&client.namespace), None, Some(&ack.request_id)),
        )
        .await
        .map_err(|error| map_lease_rpc_error("AckActionWork", &error))?;
    Ok(())
}

/// Report a claim lifecycle event to the plane.
pub(super) async fn report_claim_event(
    client: &SekaiClaimClient,
    claim: &crate::plane_intake::PlaneClaim,
    kind: crate::plane_intake::PlaneClaimEventKind,
    checkpoint_digest: &str,
    reason_code: &str,
    request_id: &str,
) -> Result<(), crate::plane_intake::PlaneIntakeError> {
    let plane = client.inner.connect().await.map_err(plane_intake_source)?;
    let _: proto::sekai::ReportActionClaimEventResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/ReportActionClaimEvent",
            ReportActionClaimEventRequest {
                effect_id: claim.work.effect_id.clone(),
                runtime_id: claim.lease.runtime_id.clone(),
                claim_generation: claim.lease.generation,
                fencing_token: claim.lease.fencing_token.clone(),
                kind: kind.as_str().into(),
                checkpoint_digest: checkpoint_digest.into(),
                reason_code: reason_code.into(),
                request_id: request_id.into(),
            },
            client
                .inner
                .sdk_call_options(Some(&client.namespace), None, Some(request_id)),
        )
        .await
        .map_err(|error| map_lease_rpc_error("ReportActionClaimEvent", &error))?;
    Ok(())
}

fn project_continuation(
    continuation: Option<proto::sekai::ActionWorkContinuation>,
    park: Option<proto::sekai::ActionWorkPark>,
) -> Result<Option<crate::plane_intake::PlaneWorkContinuation>, crate::plane_intake::PlaneIntakeError>
{
    match (continuation, park) {
        (None, None) => Ok(None),
        (Some(continuation), Some(park))
            if continuation.park_id == park.park_id
                && continuation.effect_id == park.effect_id
                && continuation.operation_id == park.operation_id
                && continuation.park_generation == park.park_generation =>
        {
            let checkpoint = if park.checkpoint_store_id.is_empty()
                && park.checkpoint_ref.is_empty()
                && park.checkpoint_digest.is_empty()
            {
                None
            } else {
                Some(crate::plane_intake::PlaneCheckpoint {
                    store_id: park.checkpoint_store_id,
                    reference: park.checkpoint_ref,
                    digest: park.checkpoint_digest,
                })
            };
            Ok(Some(crate::plane_intake::PlaneWorkContinuation {
                resolution_id: continuation.resolution_id,
                park_id: continuation.park_id,
                effect_id: continuation.effect_id,
                operation_id: continuation.operation_id,
                park_generation: continuation.park_generation,
                input_json: continuation.input_json,
                input_digest: continuation.input_digest,
                checkpoint,
            }))
        }
        (Some(_), Some(_)) => Err(crate::plane_intake::PlaneIntakeError::Source(
            "ClaimActionWork returned mismatched continuation and park snapshots".into(),
        )),
        (Some(_), None) | (None, Some(_)) => Err(crate::plane_intake::PlaneIntakeError::Source(
            "ClaimActionWork returned an incomplete continuation snapshot".into(),
        )),
    }
}

fn duration_millis(duration: Duration) -> Result<i64, crate::plane_intake::PlaneIntakeError> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        crate::plane_intake::PlaneIntakeError::Source("claim TTL exceeds i64 milliseconds".into())
    })
}

fn is_claim_contention(error: &SdkError) -> bool {
    error.code == SdkErrorCode::FailedPrecondition
}

/// Map lease RPC failures after the claim is already owned.
fn map_lease_rpc_error(rpc: &str, error: &SdkError) -> crate::plane_intake::PlaneIntakeError {
    if is_claim_contention(error) {
        crate::plane_intake::PlaneIntakeError::FenceLost(error.to_string())
    } else {
        crate::plane_intake::PlaneIntakeError::Source(format!("{rpc}: {error}"))
    }
}

/// Map pre-run renew failures after the claim is already owned.
fn map_owned_claim_heartbeat_error(error: &SdkError) -> crate::plane_intake::PlaneIntakeError {
    map_lease_rpc_error("HeartbeatActionClaim before run", error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_failed_precondition_is_normal_claim_contention() {
        assert!(is_claim_contention(&SdkError::new(
            SdkErrorCode::FailedPrecondition,
        )));
        assert!(!is_claim_contention(&SdkError::new(
            SdkErrorCode::PermissionDenied,
        )));
        assert!(!is_claim_contention(&SdkError::new(
            SdkErrorCode::Unavailable
        )));
    }

    #[test]
    fn owned_claim_heartbeat_contention_is_fence_lost_not_idle() {
        let error =
            map_owned_claim_heartbeat_error(&SdkError::new(SdkErrorCode::FailedPrecondition));
        assert!(
            matches!(error, crate::plane_intake::PlaneIntakeError::FenceLost(_)),
            "expected FenceLost, got {error}"
        );
    }

    #[test]
    fn owned_claim_heartbeat_transport_errors_stay_source() {
        let error = map_owned_claim_heartbeat_error(&SdkError::new(SdkErrorCode::Unavailable));
        assert!(
            matches!(error, crate::plane_intake::PlaneIntakeError::Source(_)),
            "expected Source, got {error}"
        );
        assert!(
            error
                .to_string()
                .contains("HeartbeatActionClaim before run"),
            "{error}"
        );
    }

    #[test]
    fn lease_rpc_contention_is_fence_lost_for_ack_and_events() {
        for rpc in [
            "HeartbeatActionClaim",
            "AckActionWork",
            "ReportActionClaimEvent",
        ] {
            let error = map_lease_rpc_error(rpc, &SdkError::new(SdkErrorCode::FailedPrecondition));
            assert!(
                matches!(error, crate::plane_intake::PlaneIntakeError::FenceLost(_)),
                "{rpc}: expected FenceLost, got {error}"
            );
        }
        let error = map_lease_rpc_error("AckActionWork", &SdkError::new(SdkErrorCode::Unavailable));
        assert!(
            matches!(error, crate::plane_intake::PlaneIntakeError::Source(_)),
            "expected Source, got {error}"
        );
        assert!(error.to_string().contains("AckActionWork"), "{error}");
    }

    #[test]
    fn incomplete_continuation_is_rejected() {
        let continuation = proto::sekai::ActionWorkContinuation {
            resolution_id: "res-1".into(),
            park_id: "park-1".into(),
            effect_id: "effect-1".into(),
            operation_id: "op-1".into(),
            park_generation: 1,
            input_json: "{}".into(),
            input_digest: "digest".into(),
            ..Default::default()
        };
        let err = project_continuation(Some(continuation), None).unwrap_err();
        assert!(err.to_string().contains("incomplete continuation snapshot"));
    }

    #[test]
    fn mismatched_continuation_and_park_are_rejected() {
        let continuation = proto::sekai::ActionWorkContinuation {
            resolution_id: "res-1".into(),
            park_id: "park-1".into(),
            effect_id: "effect-1".into(),
            operation_id: "op-1".into(),
            park_generation: 1,
            input_json: "{}".into(),
            input_digest: "digest".into(),
            ..Default::default()
        };
        let park = proto::sekai::ActionWorkPark {
            park_id: "park-2".into(),
            effect_id: "effect-1".into(),
            operation_id: "op-1".into(),
            park_generation: 1,
            ..Default::default()
        };
        let err = project_continuation(Some(continuation), Some(park)).unwrap_err();
        assert!(err.to_string().contains("mismatched continuation"));
    }
}
