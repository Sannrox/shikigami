//! One acquired agent-turn claim's durable execution protocol.

use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::harness::HarnessError;
use crate::plane_intake::{lifecycle_set_draining, wait_for_shutdown};
use crate::run::{JobCredential, JobCredentialHandle, RunRequest, RunTermination};

use super::{
    AgentTurnAck, AgentTurnContinuation, AgentTurnPort, AgentTurnServeOptions, ClaimedAgentTurn,
    Harness, PlaneAckOutcome, PlaneIntakeError,
};

pub(super) enum Execution {
    Continue,
    Shutdown,
    GovernanceUnavailable,
}

pub(super) async fn execute(
    harness: &Harness,
    intake: &dyn AgentTurnPort,
    mut claim: ClaimedAgentTurn,
    options: &AgentTurnServeOptions,
    shutdown: &watch::Receiver<bool>,
) -> Result<Execution, PlaneIntakeError> {
    let claim_id = claim.turn_id.clone();
    if let Some(lc) = &options.lifecycle {
        let _ = lc.begin_claim(&claim_id);
    }

    if let Some(reason) = refusal_reason(&claim) {
        let ack = AgentTurnAck {
            outcome: PlaneAckOutcome::Failed,
            reason: bounded_reason(&reason),
            parked_action_instance_id: String::new(),
        };
        if let Err(ack_err) = ack_with_retry(intake, &mut claim, &ack, options, shutdown).await {
            lifecycle_observe_error(options, Some(&claim_id), &ack_err);
            return Err(ack_err);
        }
        if let Some(lc) = &options.lifecycle {
            let _ =
                lc.end_claim_terminal(&claim_id, crate::worker_lifecycle::TerminalOutcome::Failed);
        }
        return Ok(Execution::Continue);
    }

    let namespace = harness.config.governance.namespace.clone();
    let mut request = match prepare_turn_run(harness, &claim) {
        Ok(mut request) => {
            // Runtime token: SubmitActionInstance and lease RPCs only. Host
            // planning and harvest stay on the worker service principal.
            request.job_credential = JobCredentialHandle::from_live(
                claim.lease.runtime_token.value().to_string(),
                claim.lease.runtime_token.expires_at_ms(),
                String::new(),
                namespace.clone(),
                now_ms(),
            );
            request
        }
        Err(error) => {
            let ack = AgentTurnAck {
                outcome: PlaneAckOutcome::Failed,
                reason: bounded_reason(&error.to_string()),
                parked_action_instance_id: String::new(),
            };
            if let Err(ack_err) = ack_with_retry(intake, &mut claim, &ack, options, shutdown).await
            {
                lifecycle_observe_error(options, Some(&claim_id), &ack_err);
                return Err(ack_err);
            }
            if let Some(lc) = &options.lifecycle {
                let _ = lc.end_claim_terminal(
                    &claim_id,
                    crate::worker_lifecycle::TerminalOutcome::Failed,
                );
            }
            return Ok(Execution::Continue);
        }
    };

    if *shutdown.borrow() {
        drain_claim(options, &claim_id);
        return Ok(Execution::Shutdown);
    }

    let (cancel_tx, cancel_rx) = watch::channel(false);
    request.cancel = Some(cancel_rx);
    let job_handle = request.job_credential.clone();
    let mut run = Box::pin(harness.run(request));
    let run_result = loop {
        tokio::select! {
            result = &mut run => break result,
            _ = tokio::time::sleep(next_heartbeat(&claim, options.heartbeat_interval)) => {
                let call_window = match claim_call_window(&claim, options.heartbeat_interval) {
                    Ok(window) => window,
                    Err(error) => {
                        return fail_closed_after_fence_loss(
                            &cancel_tx,
                            &mut run,
                            options,
                            Some(&claim_id),
                            error,
                        )
                        .await;
                    }
                };
                match tokio::time::timeout(
                    call_window,
                    intake.heartbeat(&claim, options.claim_ttl),
                ).await {
                    Ok(Ok(lease)) => {
                        claim.lease = lease;
                        if let Some(handle) = &job_handle {
                            handle.replace(JobCredential::live(
                                claim.lease.runtime_token.value().to_string(),
                                claim.lease.runtime_token.expires_at_ms(),
                                String::new(),
                                namespace.clone(),
                                now_ms(),
                            ));
                        }
                    }
                    Ok(Err(error)) => {
                        return fail_closed_after_fence_loss(
                            &cancel_tx,
                            &mut run,
                            options,
                            Some(&claim_id),
                            error,
                        )
                        .await;
                    }
                    Err(_) => {
                        return fail_closed_after_fence_loss(
                            &cancel_tx,
                            &mut run,
                            options,
                            Some(&claim_id),
                            PlaneIntakeError::FenceLost(
                                "heartbeat did not complete before the lease safety deadline"
                                    .into(),
                            ),
                        )
                        .await;
                    }
                }
            }
            _ = wait_for_shutdown(shutdown.clone()) => {
                if let Some(lc) = &options.lifecycle {
                    lifecycle_set_draining(lc);
                }
                let grace = claim_call_window(&claim, options.heartbeat_interval)
                    .unwrap_or(Duration::ZERO);
                cancel_and_drain(&cancel_tx, &mut run, grace).await;
                if let Some(lc) = &options.lifecycle {
                    let _ = lc.drop_active_claim(&claim_id);
                }
                return Ok(Execution::Shutdown);
            }
        }
    };

    let (ack, governance_abort) = match run_result {
        Ok(result) if result.termination == RunTermination::Parked => {
            match parked_action_instance_id(harness, &claim, &result) {
                Ok(action_id) => (
                    AgentTurnAck {
                        outcome: PlaneAckOutcome::Parked,
                        reason: bounded_reason(&park_reason(&result)),
                        parked_action_instance_id: action_id,
                    },
                    false,
                ),
                Err(reason) => (
                    AgentTurnAck {
                        outcome: PlaneAckOutcome::Failed,
                        reason: bounded_reason(&reason),
                        parked_action_instance_id: String::new(),
                    },
                    false,
                ),
            }
        }
        Ok(result) if result.success => (
            AgentTurnAck {
                outcome: PlaneAckOutcome::Completed,
                reason: bounded_reason(&result.summary),
                parked_action_instance_id: String::new(),
            },
            false,
        ),
        Ok(result) => (
            AgentTurnAck {
                outcome: PlaneAckOutcome::Failed,
                reason: bounded_reason(&result.summary),
                parked_action_instance_id: String::new(),
            },
            false,
        ),
        Err(error) if local_run_still_active(&error) => {
            // The local 120s run lease can outlive the plane claim. Leave
            // the turn unacked so the plane re-offers after TTL.
            drain_claim(options, &claim_id);
            return Ok(Execution::Continue);
        }
        Err(error) => {
            let governance_fail = harness_error_is_governance(&error);
            if let Some(lc) = &options.lifecycle
                && governance_fail
            {
                let _ = lc.set_governance_ok(false);
            }
            (
                AgentTurnAck {
                    outcome: PlaneAckOutcome::Failed,
                    reason: bounded_reason(&error.to_string()),
                    parked_action_instance_id: String::new(),
                },
                governance_fail,
            )
        }
    };
    ack_with_retry(intake, &mut claim, &ack, options, shutdown)
        .await
        .inspect_err(|error| lifecycle_observe_error(options, Some(&claim_id), error))?;
    if let Some(lc) = &options.lifecycle {
        let terminal = match ack.outcome {
            PlaneAckOutcome::Completed => crate::worker_lifecycle::TerminalOutcome::Completed,
            PlaneAckOutcome::Failed => crate::worker_lifecycle::TerminalOutcome::Failed,
            PlaneAckOutcome::Parked => crate::worker_lifecycle::TerminalOutcome::Parked,
        };
        let _ = lc.end_claim_terminal(&claim_id, terminal);
    }
    Ok(if governance_abort {
        Execution::GovernanceUnavailable
    } else {
        Execution::Continue
    })
}

/// Refuse a frozen, kill-switched, or principal-mismatched instance without running.
fn refusal_reason(claim: &ClaimedAgentTurn) -> Option<String> {
    if claim.instance.frozen {
        return Some(format!(
            "site agent instance {} is frozen; refusal audited, no apply",
            claim.instance.instance_id
        ));
    }
    if claim.instance.kill_switched {
        return Some(format!(
            "site agent instance {} is kill-switched; refusal audited, no apply",
            claim.instance.instance_id
        ));
    }
    if claim.principal_id != claim.instance.principal_id {
        return Some(format!(
            "turn principal {} does not match instance principal {}",
            claim.principal_id, claim.instance.principal_id
        ));
    }
    if claim.definition.instructions.trim().is_empty() {
        return Some("published agent definition has no instructions".into());
    }
    None
}

fn prepare_turn_run(
    harness: &Harness,
    claim: &ClaimedAgentTurn,
) -> Result<RunRequest, PlaneIntakeError> {
    let mut request = RunRequest::new(claim.definition.instructions.clone());
    request.logical_operation_id = Some(claim.turn_id.clone());
    request.assigned_run_id = Some(claim.turn_id.clone());
    if let Some(continuation) = &claim.continuation {
        apply_continuation(&mut request, harness, claim, continuation)?;
    } else {
        recover_unacked_turn(&mut request, harness, claim)?;
    }
    Ok(request)
}

/// A reclaim without continuation still has local scratch from the last
/// generation. Resume an escalate park; otherwise do not start the original
/// instructions again if an Action was already submitted.
fn recover_unacked_turn(
    request: &mut RunRequest,
    harness: &Harness,
    claim: &ClaimedAgentTurn,
) -> Result<(), PlaneIntakeError> {
    let Ok(checkpoint) =
        crate::checkpoint::Checkpoint::load(&harness.state.runs_dir(), &claim.turn_id)
    else {
        return Ok(());
    };
    if checkpoint.is_escalate_park() {
        request.resume_run_id = Some(claim.turn_id.clone());
        request.resume_answer = Some(
            "previous claim ended before acknowledgement; read any submitted Action outcome; do not submit it again"
                .into(),
        );
        return Ok(());
    }
    if let Some(action_id) = action_instance_id_from_checkpoint(harness, &claim.turn_id) {
        request.assigned_run_id = None;
        request.task = format!(
            "{}\n\nAction {action_id} was already submitted under this turn. Read that outcome; do not submit it again.",
            request.task
        );
    }
    Ok(())
}

fn apply_continuation(
    request: &mut RunRequest,
    harness: &Harness,
    claim: &ClaimedAgentTurn,
    continuation: &AgentTurnContinuation,
) -> Result<(), PlaneIntakeError> {
    if continuation.action_instance_id.trim().is_empty() {
        return Err(PlaneIntakeError::Source(
            "agent-turn continuation is missing action_instance_id".into(),
        ));
    }
    let answer = continuation_answer(continuation);
    if crate::checkpoint::Checkpoint::load(&harness.state.runs_dir(), &claim.turn_id)
        .is_ok_and(|checkpoint| checkpoint.is_escalate_park())
    {
        request.resume_run_id = Some(claim.turn_id.clone());
        request.resume_answer = Some(answer);
        return Ok(());
    }
    request.assigned_run_id = None;
    request.task = format!(
        "{}\n\nThe Action {} was already applied by its grant (status={}, approval={}). Read that outcome; do not submit it again.\n{}",
        request.task,
        continuation.action_instance_id,
        continuation.status,
        continuation.approval_decision,
        answer
    );
    Ok(())
}

fn continuation_answer(continuation: &AgentTurnContinuation) -> String {
    format!(
        "Action {} decided: status={} approval={} deny={}",
        continuation.action_instance_id,
        continuation.status,
        continuation.approval_decision,
        continuation.deny_reason
    )
}

fn parked_action_instance_id(
    harness: &Harness,
    claim: &ClaimedAgentTurn,
    result: &crate::run::RunResult,
) -> Result<String, String> {
    if let Some(id) = result
        .park
        .as_ref()
        .and_then(|park| park.approval_id.as_deref())
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        return Ok(id.to_string());
    }
    if let Some(id) = result
        .park
        .as_ref()
        .and_then(|park| park.args_json.as_deref())
        .and_then(action_instance_id_in_json)
    {
        return Ok(id);
    }
    if let Some(id) = action_instance_id_from_checkpoint(harness, &result.run_id) {
        return Ok(id);
    }
    if result.run_id != claim.turn_id
        && let Some(id) = action_instance_id_from_checkpoint(harness, &claim.turn_id)
    {
        return Ok(id);
    }
    if !claim.parked_action_instance_id.trim().is_empty() {
        return Ok(claim.parked_action_instance_id.clone());
    }
    if let Some(continuation) = &claim.continuation
        && !continuation.action_instance_id.trim().is_empty()
    {
        return Ok(continuation.action_instance_id.clone());
    }
    Err("parked turn has no Action submitted under this claim".into())
}

fn action_instance_id_from_checkpoint(harness: &Harness, turn_id: &str) -> Option<String> {
    let checkpoint =
        crate::checkpoint::Checkpoint::load(&harness.state.runs_dir(), turn_id).ok()?;
    checkpoint
        .messages
        .iter()
        .rev()
        .filter(|message| message.role == "tool")
        .find_map(|message| action_instance_id_in_json(&message.content))
}

fn action_instance_id_in_json(text: &str) -> Option<String> {
    action_instance_id_in_value(&serde_json::from_str(text).ok()?)
}

fn action_instance_id_in_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => action_instance_id_in_json(text),
        serde_json::Value::Array(items) => items.iter().rev().find_map(action_instance_id_in_value),
        serde_json::Value::Object(map) => {
            if let Some(id) = nonempty_id(map.get("action_instance_id")) {
                return Some(id);
            }
            // SubmitActionInstance tool output: `{ "instance": { "instance_id": ... } }`.
            if let Some(id) = map
                .get("instance")
                .and_then(|instance| instance.get("instance_id"))
                .and_then(nonempty_id_value)
            {
                return Some(id);
            }
            None
        }
        _ => None,
    }
}

fn nonempty_id(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(nonempty_id_value)
}

fn nonempty_id_value(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn drain_claim(options: &AgentTurnServeOptions, claim_id: &str) {
    if let Some(lc) = &options.lifecycle {
        lifecycle_set_draining(lc);
        let _ = lc.drop_active_claim(claim_id);
    }
}

fn harness_error_is_governance(error: &HarnessError) -> bool {
    match error {
        HarnessError::Governance(_) => true,
        HarnessError::Run(run_error) => {
            matches!(run_error, crate::run::RunError::Governance(_))
        }
        _ => false,
    }
}

fn local_run_still_active(error: &HarnessError) -> bool {
    match error {
        HarnessError::Registry(crate::registry::RegistryError::Active(_)) => true,
        HarnessError::Run(crate::run::RunError::Message(message)) => {
            message.contains("run registry start failed") && message.contains("still active")
        }
        _ => false,
    }
}

fn lifecycle_observe_error(
    options: &AgentTurnServeOptions,
    claim_id: Option<&str>,
    error: &PlaneIntakeError,
) {
    let Some(lifecycle) = &options.lifecycle else {
        return;
    };
    if let Some(id) = claim_id {
        let _ = lifecycle.drop_active_claim(id);
    }
    match error {
        PlaneIntakeError::FenceLost(kind) => {
            let _ = lifecycle.set_fence_lost(kind);
        }
        PlaneIntakeError::Source(message) if message.contains("shutdown requested") => {
            lifecycle_set_draining(lifecycle);
        }
        PlaneIntakeError::Source(_) | PlaneIntakeError::Harness(_) => {
            let _ = lifecycle.set_governance_ok(false);
        }
        PlaneIntakeError::Mapping(_) => {}
    }
}

async fn ack_with_retry(
    intake: &dyn AgentTurnPort,
    claim: &mut ClaimedAgentTurn,
    ack: &AgentTurnAck,
    options: &AgentTurnServeOptions,
    shutdown: &watch::Receiver<bool>,
) -> Result<(), PlaneIntakeError> {
    let retry_interval = options
        .poll_interval
        .clamp(Duration::from_millis(100), Duration::from_millis(250));
    for attempt in 1..=options.ack_retry_limit {
        let call_window = claim_call_window(claim, options.heartbeat_interval)?;
        let error = match tokio::time::timeout(call_window, intake.ack(claim, ack)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error @ PlaneIntakeError::FenceLost(_))) => return Err(error),
            Ok(Err(error)) => error,
            Err(_) => PlaneIntakeError::Source(
                "acknowledgement timed out before the lease safety deadline".into(),
            ),
        };
        if attempt == options.ack_retry_limit {
            return Err(error);
        }
        if *shutdown.borrow() {
            return Err(PlaneIntakeError::Source(
                "shutdown requested while acknowledgement was retrying".into(),
            ));
        }
        // Ack is idempotent once committed. Heartbeat fails after a parked or
        // terminal ack, so a lost response must retry ack without requiring a
        // successful renew.
        if let Ok(call_window) = claim_call_window(claim, options.heartbeat_interval)
            && let Ok(Ok(lease)) =
                tokio::time::timeout(call_window, intake.heartbeat(claim, options.claim_ttl)).await
        {
            claim.lease = lease;
        }
        let sleep_for = fenced_retry_sleep(
            claim.lease.valid_until,
            retry_interval,
            options.heartbeat_interval,
        );
        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            _ = wait_for_shutdown(shutdown.clone()) => {
                return Err(PlaneIntakeError::Source(
                    "shutdown requested while acknowledgement was retrying".into(),
                ));
            }
        }
    }
    unreachable!("positive acknowledgement retry limit validated")
}

fn fenced_retry_sleep(
    valid_until: Instant,
    retry_interval: Duration,
    heartbeat_interval: Duration,
) -> Duration {
    let remaining = valid_until.saturating_duration_since(Instant::now()) / 2;
    retry_interval.min(heartbeat_interval / 2).min(remaining)
}

async fn cancel_and_drain<F>(
    cancel: &watch::Sender<bool>,
    run: &mut std::pin::Pin<Box<F>>,
    grace: Duration,
) where
    F: std::future::Future,
{
    let _ = cancel.send(true);
    let _ = tokio::time::timeout(grace, run).await;
}

async fn fail_closed_after_fence_loss<F>(
    cancel: &watch::Sender<bool>,
    run: &mut std::pin::Pin<Box<F>>,
    options: &AgentTurnServeOptions,
    claim_id: Option<&str>,
    error: PlaneIntakeError,
) -> Result<Execution, PlaneIntakeError>
where
    F: std::future::Future,
{
    let grace = options
        .heartbeat_interval
        .clamp(Duration::from_millis(50), Duration::from_secs(5));
    cancel_and_drain(cancel, run, grace).await;
    lifecycle_observe_error(options, claim_id, &error);
    Err(error)
}

fn bounded_reason(reason: &str) -> String {
    reason.chars().take(512).collect()
}

fn park_reason(result: &crate::run::RunResult) -> String {
    match &result.park {
        Some(park) => format!("{}; question: {}", park.reason, park.question),
        None => result.summary.clone(),
    }
}

fn next_heartbeat(claim: &ClaimedAgentTurn, interval: Duration) -> Duration {
    let remaining = claim
        .lease
        .valid_until
        .saturating_duration_since(Instant::now());
    interval.min(remaining / 2)
}

fn claim_call_window(
    claim: &ClaimedAgentTurn,
    maximum: Duration,
) -> Result<Duration, PlaneIntakeError> {
    let remaining = claim
        .lease
        .valid_until
        .saturating_duration_since(Instant::now());
    if remaining <= Duration::from_millis(20) {
        return Err(PlaneIntakeError::FenceLost(
            "claim lease has no safe time remaining".into(),
        ));
    }
    let safety = (remaining / 10).clamp(Duration::from_millis(10), Duration::from_millis(250));
    Ok(maximum.min(remaining - safety))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::action_instance_id_in_json;

    #[test]
    fn reads_top_level_action_instance_id() {
        assert_eq!(
            action_instance_id_in_json(r#"{"action_instance_id":"action-1"}"#).as_deref(),
            Some("action-1")
        );
    }

    #[test]
    fn reads_submit_output_instance_id() {
        assert_eq!(
            action_instance_id_in_json(
                r#"{"instance":{"instance_id":"action-2","status":"parked"},"replay":false}"#
            )
            .as_deref(),
            Some("action-2")
        );
    }

    #[test]
    fn ignores_bare_instance_id() {
        assert_eq!(
            action_instance_id_in_json(r#"{"instance_id":"inst-line-7"}"#),
            None
        );
    }
}
