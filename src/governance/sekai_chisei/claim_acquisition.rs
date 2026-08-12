//! Plane claim acquisition protocol behind the existing plane intake seam.
//!
//! Owns list → claim → continuation validation → instance lookup → pre-run
//! fence renew for one fenced claim. Heartbeat, ack, and claim-event reporting
//! remain on the thin `SekaiClaimClient` adapter.

use std::time::{Duration, Instant};

use sekai_client::{SdkError, SdkErrorCode};

use super::{SekaiClaimClient, plane_intake_source, proto};
use proto::sekai::{
    ClaimActionWorkRequest, GetActionInstanceRequest, HeartbeatActionClaimRequest,
    ListClaimableActionWorkRequest,
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
        Err(error) if is_claim_contention(&error) => return Ok(None),
        Err(error) => {
            return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                "HeartbeatActionClaim before run: {error}"
            )));
        }
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

pub(super) fn duration_millis(
    duration: Duration,
) -> Result<i64, crate::plane_intake::PlaneIntakeError> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        crate::plane_intake::PlaneIntakeError::Source("claim TTL exceeds i64 milliseconds".into())
    })
}

pub(super) fn is_claim_contention(error: &SdkError) -> bool {
    error.code == SdkErrorCode::FailedPrecondition
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
