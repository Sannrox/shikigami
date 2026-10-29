//! Agent-turn claim acquisition behind [`crate::plane_intake::AgentTurnPort`].
//!
//! List/claim and instance/definition lookup use the worker credential.
//! Heartbeat and ack authenticate with the claim's runtime token.

use std::time::{Duration, Instant};

use sekai_client::{CoreLoopClient, CoreLoopTransport, SdkError, SdkErrorCode};
use serde_json::Value;

use super::{SekaiClaimClient, plane_session, proto};
use crate::plane_intake::{
    AgentTurnAck, AgentTurnContinuation, ClaimedAgentTurn, PlaneClaimLease,
    PublishedAgentDefinition, RuntimeToken, SiteAgentInstanceView,
};

macro_rules! cached_unary {
    ($slot:ident, $call:expr) => {{
        let result = $call;
        if let Err(error) = &result {
            invalidate_on_transport(&mut $slot, error);
        }
        result
    }};
}

const MEMBER_KIND_AGENT: &str = "agent";
const INSTANCE_KIND: &str = "site_agent_instance";
const MAX_GENERATION_BUMP: u64 = 1;

pub(super) async fn claim_next(
    client: &SekaiClaimClient,
    runtime_id: &str,
    ttl: Duration,
) -> Result<Option<ClaimedAgentTurn>, crate::plane_intake::PlaneIntakeError> {
    let mut plane_slot = client.connected_plane().await?;
    let ttl_ms = duration_millis(ttl)?;
    let listed: proto::sekai::ListClaimableAgentTurnsResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
            .raw()
            .unary(
                "/sekai.SekaiService/ListClaimableAgentTurns",
                proto::sekai::ListClaimableAgentTurnsRequest {
                    namespace: client.namespace.clone(),
                    limit: 1,
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => {
            return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                "ListClaimableAgentTurns: {error}"
            )));
        }
    };
    let Some(candidate) = listed.turns.into_iter().next() else {
        return Ok(None);
    };
    let claimed: proto::sekai::ClaimAgentTurnResponse = match cached_unary!(
        plane_slot,
        plane_slot
            .as_ref()
            .expect("connected plane is inserted")
            .raw()
            .unary(
                "/sekai.SekaiService/ClaimAgentTurn",
                proto::sekai::ClaimAgentTurnRequest {
                    namespace: client.namespace.clone(),
                    runtime_id: runtime_id.into(),
                    turn_id: candidate.turn_id.clone(),
                    ttl_ms,
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) if is_claim_contention(&error) => return Ok(None),
        Err(error) => {
            return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                "ClaimAgentTurn: {error}"
            )));
        }
    };
    let claimed_turn = claimed.turn.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source("ClaimAgentTurn returned no turn".into())
    })?;
    bind_granted_identity(&candidate.turn_id, runtime_id, 0, None, &claimed_turn)?;
    let runtime_token = RuntimeToken::from_grant(
        claimed.runtime_token,
        claimed.runtime_token_expires_at_ms,
        wall_now_ms(),
    )?;
    let continuation = project_continuation(claimed.continuation);
    let instance = load_instance(
        plane_slot.as_ref().expect("connected plane is inserted"),
        client,
        &claimed_turn.instance_id,
    )
    .await?;
    let definition = load_definition(
        plane_slot.as_ref().expect("connected plane is inserted"),
        client,
        &claimed_turn.namespace,
        &instance.definition_revision_digest,
        &instance.agent_member,
    )
    .await?;

    let renew_started = Instant::now();
    let claim_plane = with_runtime_token(
        plane_slot.as_ref().expect("connected plane is inserted"),
        &runtime_token,
    )?;
    let heartbeat: proto::sekai::HeartbeatAgentTurnClaimResponse = match cached_unary!(
        plane_slot,
        claim_plane
            .raw()
            .unary(
                "/sekai.SekaiService/HeartbeatAgentTurnClaim",
                proto::sekai::HeartbeatAgentTurnClaimRequest {
                    turn_id: claimed_turn.turn_id.clone(),
                    runtime_id: runtime_id.into(),
                    claim_generation: claimed_turn.claim_generation,
                    fencing_token: claimed_turn.fencing_token.clone(),
                    ttl_ms,
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => return Err(map_owned_claim_heartbeat_error(&error)),
    };
    let refreshed_token = RuntimeToken::from_grant(
        heartbeat.runtime_token,
        heartbeat.runtime_token_expires_at_ms,
        wall_now_ms(),
    )?;
    let heartbeat_turn = heartbeat.turn.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "HeartbeatAgentTurnClaim before run returned no turn".into(),
        )
    })?;
    let lease = lease_from_granted_turn(
        &LeaseGrant {
            expected_turn_id: &claimed_turn.turn_id,
            requested_runtime_id: runtime_id,
            expected_generation: generation_u64(claimed_turn.claim_generation)?,
            held_fencing_token: Some(&claimed_turn.fencing_token),
            requested_ttl: ttl,
            renew_started,
            now_ms: wall_now_ms(),
        },
        &heartbeat_turn,
        refreshed_token,
    )?;

    Ok(Some(ClaimedAgentTurn {
        turn_id: claimed_turn.turn_id,
        namespace: claimed_turn.namespace,
        instance_id: claimed_turn.instance_id,
        principal_id: claimed_turn.principal_id,
        cause: claimed_turn.cause,
        status: claimed_turn.status,
        parked_action_instance_id: claimed_turn.parked_action_instance_id,
        park_count: claimed_turn.park_count,
        lease,
        instance,
        definition,
        continuation,
    }))
}

pub(super) async fn heartbeat(
    client: &SekaiClaimClient,
    claim: &ClaimedAgentTurn,
    ttl: Duration,
) -> Result<PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
    if claim.turn_id.is_empty()
        || claim.lease.runtime_id.is_empty()
        || claim.lease.generation == 0
        || claim.lease.fencing_token.is_empty()
    {
        return Err(fence_lost("held claim fence identity is empty"));
    }
    let renew_started = Instant::now();
    let mut plane_slot = client.connected_plane().await?;
    let claim_plane = with_runtime_token(
        plane_slot.as_ref().expect("connected plane is inserted"),
        &claim.lease.runtime_token,
    )?;
    let generation = i64::try_from(claim.lease.generation).map_err(|_| {
        crate::plane_intake::PlaneIntakeError::Source("claim generation exceeds i64".into())
    })?;
    let response: proto::sekai::HeartbeatAgentTurnClaimResponse = match cached_unary!(
        plane_slot,
        claim_plane
            .raw()
            .unary(
                "/sekai.SekaiService/HeartbeatAgentTurnClaim",
                proto::sekai::HeartbeatAgentTurnClaimRequest {
                    turn_id: claim.turn_id.clone(),
                    runtime_id: claim.lease.runtime_id.clone(),
                    claim_generation: generation,
                    fencing_token: claim.lease.fencing_token.clone(),
                    ttl_ms: duration_millis(ttl)?,
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => return Err(map_lease_rpc_error("HeartbeatAgentTurnClaim", &error)),
    };
    let refreshed_token = RuntimeToken::from_grant(
        response.runtime_token,
        response.runtime_token_expires_at_ms,
        wall_now_ms(),
    )?;
    let turn = response.turn.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "HeartbeatAgentTurnClaim returned no turn".into(),
        )
    })?;
    lease_from_granted_turn(
        &LeaseGrant {
            expected_turn_id: &claim.turn_id,
            requested_runtime_id: &claim.lease.runtime_id,
            expected_generation: claim.lease.generation,
            held_fencing_token: Some(&claim.lease.fencing_token),
            requested_ttl: ttl,
            renew_started,
            now_ms: wall_now_ms(),
        },
        &turn,
        refreshed_token,
    )
}

pub(super) async fn ack(
    client: &SekaiClaimClient,
    claim: &ClaimedAgentTurn,
    ack: &AgentTurnAck,
) -> Result<(), crate::plane_intake::PlaneIntakeError> {
    let mut plane_slot = client.connected_plane().await?;
    let claim_plane = with_runtime_token(
        plane_slot.as_ref().expect("connected plane is inserted"),
        &claim.lease.runtime_token,
    )?;
    let generation = i64::try_from(claim.lease.generation).map_err(|_| {
        crate::plane_intake::PlaneIntakeError::Source("claim generation exceeds i64".into())
    })?;
    let _: proto::sekai::AckAgentTurnResponse = match cached_unary!(
        plane_slot,
        claim_plane
            .raw()
            .unary(
                "/sekai.SekaiService/AckAgentTurn",
                proto::sekai::AckAgentTurnRequest {
                    turn_id: claim.turn_id.clone(),
                    runtime_id: claim.lease.runtime_id.clone(),
                    claim_generation: generation,
                    fencing_token: claim.lease.fencing_token.clone(),
                    outcome: ack.outcome.as_str().into(),
                    reason: ack.reason.clone(),
                    parked_action_instance_id: ack.parked_action_instance_id.clone(),
                },
                plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
            )
            .await
    ) {
        Ok(response) => response,
        Err(error) => return Err(map_lease_rpc_error("AckAgentTurn", &error)),
    };
    Ok(())
}

async fn load_instance(
    plane: &super::PlaneClient,
    client: &SekaiClaimClient,
    instance_id: &str,
) -> Result<SiteAgentInstanceView, crate::plane_intake::PlaneIntakeError> {
    let response: proto::sekai::GetObjectResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/GetObject",
            proto::sekai::GetObjectRequest {
                id: instance_id.into(),
            },
            plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
        )
        .await
        .map_err(|error| {
            crate::plane_intake::PlaneIntakeError::Source(format!("GetObject: {error}"))
        })?;
    let object = response.object.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source("GetObject returned no object".into())
    })?;
    instance_view(&object)
}

fn instance_view(
    object: &proto::sekai::Object,
) -> Result<SiteAgentInstanceView, crate::plane_intake::PlaneIntakeError> {
    if object.kind != INSTANCE_KIND {
        return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
            "claimed instance kind must be {INSTANCE_KIND}, got {:?}",
            object.kind
        )));
    }
    let property = |name: &'static str| -> Result<String, crate::plane_intake::PlaneIntakeError> {
        object
            .properties
            .get(name)
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .ok_or_else(|| {
                crate::plane_intake::PlaneIntakeError::Source(format!(
                    "site agent instance missing {name}"
                ))
            })
    };
    Ok(SiteAgentInstanceView {
        instance_id: object.id.clone(),
        principal_id: property("principal_id")?,
        frozen: object.properties.get("frozen").map(String::as_str) == Some("true"),
        kill_switched: object.properties.get("kill_switched").map(String::as_str) == Some("true"),
        definition_revision_digest: property("definition_revision_digest")?,
        agent_member: property("agent_member")?,
    })
}

async fn load_definition(
    plane: &super::PlaneClient,
    client: &SekaiClaimClient,
    namespace: &str,
    revision_digest: &str,
    agent_member: &str,
) -> Result<PublishedAgentDefinition, crate::plane_intake::PlaneIntakeError> {
    let response: proto::sekai::GetDefinitionMemberResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/GetDefinitionMember",
            proto::sekai::GetDefinitionMemberRequest {
                namespace: namespace.into(),
                revision_digest: revision_digest.into(),
                member_kind: MEMBER_KIND_AGENT.into(),
                member_id: agent_member.into(),
            },
            plane_session::call_options(&client.inner, Some(&client.namespace), None, None),
        )
        .await
        .map_err(|error| {
            crate::plane_intake::PlaneIntakeError::Source(format!("GetDefinitionMember: {error}"))
        })?;
    let member = response.member.ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "GetDefinitionMember returned no member".into(),
        )
    })?;
    published_definition(&member.definition_json)
}

fn published_definition(
    definition_json: &str,
) -> Result<PublishedAgentDefinition, crate::plane_intake::PlaneIntakeError> {
    let value: Value = serde_json::from_str(definition_json).map_err(|error| {
        crate::plane_intake::PlaneIntakeError::Source(format!(
            "agent definition is not JSON: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        crate::plane_intake::PlaneIntakeError::Source(
            "agent definition must be a JSON object".into(),
        )
    })?;
    let field = |name: &'static str| -> Result<String, crate::plane_intake::PlaneIntakeError> {
        object
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                crate::plane_intake::PlaneIntakeError::Source(format!(
                    "agent definition missing {name}"
                ))
            })
    };
    Ok(PublishedAgentDefinition {
        name: field("name")?,
        task_class: object
            .get("task_class")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        instructions: field("instructions")?,
    })
}

fn project_continuation(
    continuation: Option<proto::sekai::AgentTurnContinuation>,
) -> Option<AgentTurnContinuation> {
    let continuation = continuation?;
    if continuation.action_instance_id.trim().is_empty() {
        return None;
    }
    Some(AgentTurnContinuation {
        action_instance_id: continuation.action_instance_id,
        status: continuation.status,
        approval_decision: continuation.approval_decision,
        deny_reason: continuation.deny_reason,
    })
}

fn invalidate_on_transport(plane_slot: &mut Option<super::PlaneClient>, error: &SdkError) {
    if matches!(
        error.code,
        SdkErrorCode::Unavailable | SdkErrorCode::DeadlineExceeded
    ) {
        *plane_slot = None;
    }
}

fn fence_lost(message: impl Into<String>) -> crate::plane_intake::PlaneIntakeError {
    crate::plane_intake::PlaneIntakeError::FenceLost(message.into())
}

fn wall_now_ms() -> i64 {
    crate::digest::unix_now_ms_i64()
}

struct LeaseGrant<'a> {
    expected_turn_id: &'a str,
    requested_runtime_id: &'a str,
    expected_generation: u64,
    held_fencing_token: Option<&'a str>,
    requested_ttl: Duration,
    renew_started: Instant,
    now_ms: i64,
}

fn generation_u64(generation: i64) -> Result<u64, crate::plane_intake::PlaneIntakeError> {
    u64::try_from(generation).map_err(|_| fence_lost("claim generation is empty"))
}

fn bind_granted_identity(
    expected_turn_id: &str,
    requested_runtime_id: &str,
    expected_generation: u64,
    held_fencing_token: Option<&str>,
    turn: &proto::sekai::AgentTurn,
) -> Result<(), crate::plane_intake::PlaneIntakeError> {
    if expected_turn_id.is_empty() || requested_runtime_id.is_empty() {
        return Err(fence_lost("held claim fence identity is empty"));
    }
    if turn.turn_id.is_empty() || turn.turn_id != expected_turn_id {
        return Err(fence_lost("claim turn_id does not match claimed work"));
    }
    if turn.claim_owner.is_empty() || turn.claim_owner != requested_runtime_id {
        return Err(fence_lost(
            "claim owner does not match requested runtime_id",
        ));
    }
    if turn.fencing_token.is_empty() {
        return Err(fence_lost("claim fencing token is empty"));
    }
    let generation = generation_u64(turn.claim_generation)?;
    if generation == 0 {
        return Err(fence_lost("claim generation is empty"));
    }
    if expected_generation > 0
        && (generation < expected_generation
            || generation.saturating_sub(expected_generation) > MAX_GENERATION_BUMP)
    {
        return Err(fence_lost(
            "claim generation is neither unchanged nor a single renew step",
        ));
    }
    if expected_generation > 0
        && generation == expected_generation
        && let Some(held) = held_fencing_token
        && turn.fencing_token != held
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
    Ok(renew_started + Duration::from_millis(remaining_ms).min(requested))
}

fn lease_from_granted_turn(
    grant: &LeaseGrant<'_>,
    turn: &proto::sekai::AgentTurn,
    runtime_token: RuntimeToken,
) -> Result<PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
    bind_granted_identity(
        grant.expected_turn_id,
        grant.requested_runtime_id,
        grant.expected_generation,
        grant.held_fencing_token,
        turn,
    )?;
    let lease_until = granted_valid_until(
        grant.renew_started,
        grant.requested_ttl,
        turn.claim_expires_at_ms,
        grant.now_ms,
    )?;
    let token_until = granted_valid_until(
        grant.renew_started,
        grant.requested_ttl,
        runtime_token.expires_at_ms(),
        grant.now_ms,
    )?;
    Ok(PlaneClaimLease {
        runtime_id: grant.requested_runtime_id.to_owned(),
        generation: generation_u64(turn.claim_generation)?,
        fencing_token: turn.fencing_token.clone(),
        expires_at_ms: turn.claim_expires_at_ms,
        valid_until: lease_until.min(token_until),
        runtime_token,
    })
}

fn with_runtime_token<T: CoreLoopTransport + Clone>(
    plane: &CoreLoopClient<T>,
    token: &RuntimeToken,
) -> Result<CoreLoopClient<T>, crate::plane_intake::PlaneIntakeError> {
    token.check_live(wall_now_ms())?;
    let source = |error: SdkError| {
        crate::plane_intake::PlaneIntakeError::Source(format!("runtime token client: {error}"))
    };
    plane_session::with_bearer(plane, Some(token.value())).map_err(source)
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
    matches!(
        error.code,
        SdkErrorCode::FailedPrecondition | SdkErrorCode::NotFound
    )
}

fn map_lease_rpc_error(rpc: &str, error: &SdkError) -> crate::plane_intake::PlaneIntakeError {
    if error.code == SdkErrorCode::FailedPrecondition {
        crate::plane_intake::PlaneIntakeError::FenceLost(error.to_string())
    } else {
        crate::plane_intake::PlaneIntakeError::Source(format!("{rpc}: {error}"))
    }
}

fn map_owned_claim_heartbeat_error(error: &SdkError) -> crate::plane_intake::PlaneIntakeError {
    map_lease_rpc_error("HeartbeatAgentTurnClaim before run", error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_runtime_token_refuses_the_claim() {
        let error = RuntimeToken::from_grant(String::new(), 0, 0).unwrap_err();
        assert!(
            matches!(error, crate::plane_intake::PlaneIntakeError::Source(_)),
            "{error}"
        );
        assert!(error.to_string().contains("no runtime token"), "{error}");
    }

    #[test]
    fn frozen_instance_property_is_true_only_for_the_frozen_flag() {
        let mut object = proto::sekai::Object {
            id: "inst-1".into(),
            kind: INSTANCE_KIND.into(),
            properties: [
                ("principal_id".into(), "agent:line-7".into()),
                ("definition_revision_digest".into(), "rev-1".into()),
                ("agent_member".into(), "line-7".into()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let view = instance_view(&object).unwrap();
        assert!(!view.frozen);
        assert!(!view.kill_switched);
        object.properties.insert("frozen".into(), "true".into());
        object
            .properties
            .insert("kill_switched".into(), "true".into());
        let view = instance_view(&object).unwrap();
        assert!(view.frozen);
        assert!(view.kill_switched);
        assert_eq!(view.principal_id, "agent:line-7");
    }

    #[test]
    fn published_definition_requires_instructions() {
        let error = published_definition(r#"{"name":"line-7"}"#).unwrap_err();
        assert!(error.to_string().contains("instructions"), "{error}");
        let definition = published_definition(
            r#"{"name":"line-7","task_class":"site","instructions":"inspect the line"}"#,
        )
        .unwrap();
        assert_eq!(definition.instructions, "inspect the line");
    }

    #[test]
    fn empty_continuation_action_is_absent() {
        assert!(
            project_continuation(Some(proto::sekai::AgentTurnContinuation::default())).is_none()
        );
        let continuation = project_continuation(Some(proto::sekai::AgentTurnContinuation {
            action_instance_id: "action-1".into(),
            status: "admitted".into(),
            approval_decision: "approve".into(),
            deny_reason: String::new(),
        }))
        .unwrap();
        assert_eq!(continuation.action_instance_id, "action-1");
        assert_eq!(continuation.status, "admitted");
    }
}
