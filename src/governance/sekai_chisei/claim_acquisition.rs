//! Plane claim acquisition and lease protocol behind the existing plane intake seam.
//!
//! Owns list → claim → continuation validation → instance lookup → pre-run
//! fence renew, plus post-admit heartbeat, ack, and claim-event reporting.
//! Claim and heartbeat grants are bound to the claimed work and requested
//! runtime; `valid_until` never exceeds `min(requested, granted)` remaining.

use std::time::{Duration, Instant};

use sekai_client::{SdkError, SdkErrorCode};

use super::{SekaiClaimClient, plane_session, proto};

macro_rules! cached_unary {
    ($slot:ident, $call:expr) => {{
        let result = $call;
        if let Err(error) = &result {
            invalidate_on_transport(&mut $slot, error);
        }
        result
    }};
}
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
    let mut plane_slot = client.connected_plane().await?;
    let ttl_ms = duration_millis(ttl)?;
    let listed: proto::sekai::ListClaimableActionWorkResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
            .raw()
            .unary(
                "/sekai.SekaiService/ListClaimableActionWork",
                ListClaimableActionWorkRequest {
                    namespace: client.namespace.clone(),
                    runtime_id: runtime_id.into(),
                    limit: 1,
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => {
            return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                "ListClaimableActionWork: {error}"
            )));
        }
    };
    let Some(candidate) = listed.effects.into_iter().next() else {
        return Ok(None);
    };
    let candidate_effect_id = candidate.effect_id.clone();
    let claim_request_id = uuid::Uuid::new_v4().to_string();
    let claimed: proto::sekai::ClaimActionWorkResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
            .raw()
            .unary(
                "/sekai.SekaiService/ClaimActionWork",
                ClaimActionWorkRequest {
                    effect_id: candidate_effect_id.clone(),
                    runtime_id: runtime_id.into(),
                    request_id: claim_request_id.clone(),
                    ttl_ms,
                },
                plane_session::call_options(
                    &client.inner,
                    Some(&client.namespace),
                    None,
                    Some(&claim_request_id),
                ),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) if is_claim_contention(&error) => return Ok(None),
        Err(error) => {
            return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                "ClaimActionWork: {error}"
            )));
        }
    };
    let continuation = project_continuation(claimed.continuation, claimed.park)?;
    let claimed_effect = claimed.effect.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source("ClaimActionWork returned no effect".into())
    })?;
    bind_granted_identity(&candidate_effect_id, runtime_id, 0, None, &claimed_effect)?;
    let instance_response: proto::sekai::GetActionInstanceResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
            .raw()
            .unary(
                "/sekai.SekaiService/GetActionInstance",
                GetActionInstanceRequest {
                    instance_id: claimed_effect.instance_id.clone(),
                    namespace: String::new(),
                    idempotency_key: String::new(),
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => {
            return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                "GetActionInstance: {error}"
            )));
        }
    };
    let instance = instance_response.instance.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "GetActionInstance returned no instance".into(),
        )
    })?;
    // Parameter lookup happens after claim and may consume most of the
    // initial TTL. Revalidate and renew the same fence before the host is
    // allowed to start the run.
    let renew_started = Instant::now();
    let effect_response: proto::sekai::HeartbeatActionClaimResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
            .raw()
            .unary(
                "/sekai.SekaiService/HeartbeatActionClaim",
                HeartbeatActionClaimRequest {
                    effect_id: claimed_effect.effect_id.clone(),
                    runtime_id: runtime_id.into(),
                    claim_generation: claimed_effect.claim_generation,
                    fencing_token: claimed_effect.claim_fencing_token.clone(),
                    ttl_ms,
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        // ClaimActionWork already succeeded: FailedPrecondition is fence loss,
        // not idle contention. Mapping to Ok(None) would hide FenceLost from the
        // serve loop and keep the worker accepting claims.
        Err(error) => return Err(map_owned_claim_heartbeat_error(&error)),
    };
    let heartbeat_effect = effect_response.effect.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "HeartbeatActionClaim before run returned no effect".into(),
        )
    })?;
    let lease = lease_from_granted_effect(
        &LeaseGrant {
            expected_effect_id: &claimed_effect.effect_id,
            requested_runtime_id: runtime_id,
            expected_generation: claimed_effect.claim_generation,
            held_fencing_token: Some(&claimed_effect.claim_fencing_token),
            requested_ttl: ttl,
            renew_started,
            now_ms: wall_now_ms(),
        },
        &heartbeat_effect,
    )?;

    Ok(Some(crate::plane_intake::PlaneClaim {
        work: crate::plane_intake::ClaimedPlaneWork {
            effect_id: claimed_effect.effect_id,
            instance_id: claimed_effect.instance_id,
            operation_id: claimed_effect.operation_id,
            kind: claimed_effect.kind,
            status: claimed_effect.status,
            payload_json: claimed_effect.payload_json,
            parameters_json: instance.parameters_json,
            resolved_task: None,
            continuation,
        },
        lease,
    }))
}

/// Renew an owned claim fence and return the updated lease.
pub(super) async fn heartbeat(
    client: &SekaiClaimClient,
    claim: &crate::plane_intake::PlaneClaim,
    ttl: Duration,
) -> Result<crate::plane_intake::PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
    if claim.work.effect_id.is_empty()
        || claim.lease.runtime_id.is_empty()
        || claim.lease.generation == 0
        || claim.lease.fencing_token.is_empty()
    {
        return Err(fence_lost("held claim fence identity is empty"));
    }
    let renew_started = Instant::now();
    let mut plane_slot = client.connected_plane().await?;
    let response: proto::sekai::HeartbeatActionClaimResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
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
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => return Err(map_lease_rpc_error("HeartbeatActionClaim", &error)),
    };
    let effect = response.effect.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "HeartbeatActionClaim returned no effect".into(),
        )
    })?;
    lease_from_granted_effect(
        &LeaseGrant {
            expected_effect_id: &claim.work.effect_id,
            requested_runtime_id: &claim.lease.runtime_id,
            expected_generation: claim.lease.generation,
            held_fencing_token: Some(&claim.lease.fencing_token),
            requested_ttl: ttl,
            renew_started,
            now_ms: wall_now_ms(),
        },
        &effect,
    )
}

/// Acknowledge a claimed effect with the plane.
pub(super) async fn ack(
    client: &SekaiClaimClient,
    claim: &crate::plane_intake::PlaneClaim,
    ack: &crate::plane_intake::PlaneAck,
) -> Result<(), crate::plane_intake::PlaneIntakeError> {
    let mut plane_slot = client.connected_plane().await?;
    let _: proto::sekai::AckActionWorkResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
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
                plane_session::call_options(
                    &client.inner,
                    Some(&client.namespace),
                    None,
                    Some(&ack.request_id),
                ),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => return Err(map_lease_rpc_error("AckActionWork", &error)),
    };
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
    let mut plane_slot = client.connected_plane().await?;
    let _: proto::sekai::ReportActionClaimEventResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
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
                plane_session::call_options(
                    &client.inner,
                    Some(&client.namespace),
                    None,
                    Some(request_id),
                ),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => return Err(map_lease_rpc_error("ReportActionClaimEvent", &error)),
    };
    Ok(())
}

fn invalidate_on_transport(plane_slot: &mut Option<super::PlaneClient>, error: &SdkError) {
    if matches!(
        error.code,
        SdkErrorCode::Unavailable | SdkErrorCode::DeadlineExceeded
    ) {
        *plane_slot = None;
    }
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

fn fence_lost(message: impl Into<String>) -> crate::plane_intake::PlaneIntakeError {
    crate::plane_intake::PlaneIntakeError::FenceLost(message.into())
}

fn wall_now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Heartbeat/claim generation may stay the same or advance by one renew step.
const MAX_GENERATION_BUMP: u64 = 1;

struct LeaseGrant<'a> {
    expected_effect_id: &'a str,
    requested_runtime_id: &'a str,
    expected_generation: u64,
    held_fencing_token: Option<&'a str>,
    requested_ttl: Duration,
    renew_started: Instant,
    now_ms: i64,
}

fn bind_granted_identity(
    expected_effect_id: &str,
    requested_runtime_id: &str,
    expected_generation: u64,
    held_fencing_token: Option<&str>,
    effect: &proto::sekai::ActionEffect,
) -> Result<(), crate::plane_intake::PlaneIntakeError> {
    if expected_effect_id.is_empty() || requested_runtime_id.is_empty() {
        return Err(fence_lost("held claim fence identity is empty"));
    }
    if held_fencing_token.is_some_and(str::is_empty)
        || (held_fencing_token.is_some() && expected_generation == 0)
    {
        return Err(fence_lost("held claim fence identity is empty"));
    }
    if effect.effect_id.is_empty() || effect.effect_id != expected_effect_id {
        return Err(fence_lost("claim effect_id does not match claimed work"));
    }
    if effect.claim_owner.is_empty() || effect.claim_owner != requested_runtime_id {
        return Err(fence_lost(
            "claim owner does not match requested runtime_id",
        ));
    }
    if effect.claim_fencing_token.is_empty() {
        return Err(fence_lost("claim fencing token is empty"));
    }
    if effect.claim_generation == 0 {
        return Err(fence_lost("claim generation is empty"));
    }
    if expected_generation > 0
        && (effect.claim_generation < expected_generation
            || effect.claim_generation.saturating_sub(expected_generation) > MAX_GENERATION_BUMP)
    {
        return Err(fence_lost(
            "claim generation is neither unchanged nor a single renew step",
        ));
    }
    if expected_generation > 0
        && effect.claim_generation == expected_generation
        && let Some(held) = held_fencing_token
        && effect.claim_fencing_token != held
    {
        return Err(fence_lost(
            "claim fencing token does not match the held fence",
        ));
    }
    Ok(())
}

fn granted_valid_until(
    renew_started: Instant,
    requested: Duration,
    expires_at_ms: i64,
    now_ms: i64,
) -> Result<Instant, crate::plane_intake::PlaneIntakeError> {
    if expires_at_ms <= 0 {
        return Err(fence_lost("claim grant is missing expires_at_ms"));
    }
    if expires_at_ms <= now_ms {
        return Err(fence_lost("claim grant is already expired"));
    }
    let remaining_ms = u64::try_from(expires_at_ms - now_ms)
        .map_err(|_| fence_lost("claim grant remaining TTL is invalid"))?;
    let remaining = Duration::from_millis(remaining_ms);
    // Never overstay the requested TTL. An over-grant is clamped rather than
    // adopted, so a slightly fast host clock cannot extend the local fence.
    Ok(renew_started + remaining.min(requested))
}

fn lease_from_granted_effect(
    grant: &LeaseGrant<'_>,
    effect: &proto::sekai::ActionEffect,
) -> Result<crate::plane_intake::PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
    bind_granted_identity(
        grant.expected_effect_id,
        grant.requested_runtime_id,
        grant.expected_generation,
        grant.held_fencing_token,
        effect,
    )?;
    Ok(crate::plane_intake::PlaneClaimLease {
        runtime_id: grant.requested_runtime_id.to_owned(),
        generation: effect.claim_generation,
        fencing_token: effect.claim_fencing_token.clone(),
        expires_at_ms: effect.claim_expires_at_ms,
        valid_until: granted_valid_until(
            grant.renew_started,
            grant.requested_ttl,
            effect.claim_expires_at_ms,
            grant.now_ms,
        )?,
    })
}

fn duration_millis(duration: Duration) -> Result<i64, crate::plane_intake::PlaneIntakeError> {
    let millis = i64::try_from(duration.as_millis()).map_err(|_| {
        crate::plane_intake::PlaneIntakeError::Source("claim TTL exceeds i64 milliseconds".into())
    })?;
    if millis <= 0 {
        return Err(crate::plane_intake::PlaneIntakeError::Source(
            "claim TTL must be greater than zero".into(),
        ));
    }
    Ok(millis)
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

    fn granted_effect() -> proto::sekai::ActionEffect {
        proto::sekai::ActionEffect {
            effect_id: "effect-1".into(),
            instance_id: "inst-1".into(),
            operation_id: "op-1".into(),
            kind: "runtime_dispatch".into(),
            status: "claimed".into(),
            claim_owner: "runtime-1".into(),
            claim_generation: 1,
            claim_fencing_token: "fence-1".into(),
            claim_expires_at_ms: 1_700_000_060_000,
            ..Default::default()
        }
    }

    fn assert_fence_lost(
        result: Result<crate::plane_intake::PlaneClaimLease, crate::plane_intake::PlaneIntakeError>,
        needle: &str,
    ) {
        let err = result.expect_err("expected FenceLost");
        assert!(
            matches!(err, crate::plane_intake::PlaneIntakeError::FenceLost(_)),
            "expected FenceLost, got {err}"
        );
        assert!(
            err.to_string().contains(needle),
            "expected {needle:?} in {err}"
        );
    }

    fn bind_lease(
        start: Instant,
        now_ms: i64,
        ttl: Duration,
        generation: u64,
        token: Option<&str>,
        effect: &proto::sekai::ActionEffect,
    ) -> Result<crate::plane_intake::PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
        lease_from_granted_effect(
            &LeaseGrant {
                expected_effect_id: "effect-1",
                requested_runtime_id: "runtime-1",
                expected_generation: generation,
                held_fencing_token: token,
                requested_ttl: ttl,
                renew_started: start,
                now_ms,
            },
            effect,
        )
    }

    #[test]
    fn duration_millis_rejects_zero_and_sub_millisecond_ttl() {
        let zero = duration_millis(Duration::ZERO).unwrap_err();
        assert!(
            matches!(zero, crate::plane_intake::PlaneIntakeError::Source(_)),
            "expected Source, got {zero}"
        );
        assert!(zero.to_string().contains("greater than zero"), "{zero}");
        let sub_ms = duration_millis(Duration::from_nanos(1)).unwrap_err();
        assert!(
            matches!(sub_ms, crate::plane_intake::PlaneIntakeError::Source(_)),
            "expected Source, got {sub_ms}"
        );
        assert_eq!(duration_millis(Duration::from_millis(1)).unwrap(), 1);
    }

    #[test]
    fn mismatched_owner_token_generation_or_effect_id_is_fence_lost() {
        let start = Instant::now();
        let now_ms = 1_700_000_000_000;
        let ttl = Duration::from_secs(60);
        let held = Some("fence-1");

        let mut owner = granted_effect();
        owner.claim_owner = "other-runtime".into();
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 1, held, &owner),
            "claim owner",
        );

        let mut token = granted_effect();
        token.claim_fencing_token = "fence-other".into();
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 1, held, &token),
            "fencing token",
        );

        let mut generation = granted_effect();
        generation.claim_generation = 3;
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 1, held, &generation),
            "claim generation",
        );

        let mut effect_id = granted_effect();
        effect_id.effect_id = "effect-other".into();
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 1, held, &effect_id),
            "effect_id",
        );
    }

    #[test]
    fn empty_owner_token_or_generation_is_fence_lost() {
        let start = Instant::now();
        let now_ms = 1_700_000_000_000;
        let ttl = Duration::from_secs(60);

        let mut owner = granted_effect();
        owner.claim_owner.clear();
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 0, None, &owner),
            "claim owner",
        );

        let mut token = granted_effect();
        token.claim_fencing_token.clear();
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 0, None, &token),
            "fencing token",
        );

        let mut generation = granted_effect();
        generation.claim_generation = 0;
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 0, None, &generation),
            "claim generation",
        );
    }

    #[test]
    fn generation_may_stay_or_advance_by_one() {
        let start = Instant::now();
        let now_ms = 1_700_000_000_000;
        let ttl = Duration::from_secs(60);
        let same = granted_effect();
        let lease = bind_lease(start, now_ms, ttl, 1, Some("fence-1"), &same).unwrap();
        assert_eq!(lease.generation, 1);
        assert_eq!(lease.runtime_id, "runtime-1");
        assert_eq!(lease.fencing_token, "fence-1");

        let mut bumped = granted_effect();
        bumped.claim_generation = 2;
        bumped.claim_fencing_token = "fence-2".into();
        let lease = bind_lease(start, now_ms, ttl, 1, Some("fence-1"), &bumped).unwrap();
        assert_eq!(lease.generation, 2);
        assert_eq!(lease.fencing_token, "fence-2");
    }

    #[test]
    fn granted_shorter_ttl_sets_valid_until_from_remaining() {
        let start = Instant::now();
        let now_ms = 1_700_000_000_000;
        let mut effect = granted_effect();
        effect.claim_expires_at_ms = now_ms + 10_000;
        let lease = bind_lease(
            start,
            now_ms,
            Duration::from_secs(60),
            1,
            Some("fence-1"),
            &effect,
        )
        .unwrap();
        assert_eq!(
            lease.valid_until.saturating_duration_since(start),
            Duration::from_millis(10_000)
        );
    }

    #[test]
    fn granted_longer_ttl_is_clamped_to_requested() {
        let start = Instant::now();
        let now_ms = 1_700_000_000_000;
        let requested = Duration::from_secs(10);
        let mut effect = granted_effect();
        effect.claim_expires_at_ms = now_ms + 60_000;
        let lease = bind_lease(start, now_ms, requested, 1, Some("fence-1"), &effect).unwrap();
        assert_eq!(
            lease.valid_until.saturating_duration_since(start),
            requested
        );
        assert!(lease.valid_until < start + Duration::from_secs(60));
    }

    #[test]
    fn missing_or_past_grant_is_fence_lost() {
        let start = Instant::now();
        let now_ms = 1_700_000_000_000;
        let ttl = Duration::from_secs(60);

        let mut missing = granted_effect();
        missing.claim_expires_at_ms = 0;
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 1, Some("fence-1"), &missing),
            "missing expires_at_ms",
        );

        let mut past = granted_effect();
        past.claim_expires_at_ms = now_ms;
        assert_fence_lost(
            bind_lease(start, now_ms, ttl, 1, Some("fence-1"), &past),
            "already expired",
        );
    }
}
