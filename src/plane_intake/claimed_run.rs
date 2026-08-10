//! One acquired plane claim's durable execution protocol.

use std::time::Duration;

use tokio::sync::watch;
use uuid::Uuid;

use super::{
    Harness, PlaneAck, PlaneAckOutcome, PlaneClaim, PlaneClaimEventKind, PlaneIntakeAckExt,
    PlaneIntakeError, PlaneIntakePort, PlaneServeOptions, RunTermination, bounded_reason,
    cancel_and_drain, checkpoint_for_park, claim_call_window, harness_error_is_governance,
    lifecycle_observe_error, lifecycle_set_draining, park_reason, prepare_claimed_run,
    report_claim_event_with_retry, wait_for_shutdown,
};

/// Host-loop decision after one acquired claim has reached a durable stopping point.
pub(super) enum Execution {
    Continue,
    Shutdown,
    GovernanceUnavailable,
}

/// Execute one acquired claim from lifecycle admission through terminal acknowledgement.
pub(super) async fn execute(
    harness: &Harness,
    intake: &dyn PlaneIntakePort,
    mut claim: PlaneClaim,
    options: &PlaneServeOptions,
    shutdown: &watch::Receiver<bool>,
) -> Result<Execution, PlaneIntakeError> {
    let claim_id = claim.work.effect_id.clone();
    if let Some(lc) = &options.lifecycle {
        let _ = lc.begin_claim(&claim_id);
    }

    let mut prepared = match prepare_claimed_run(harness, &claim.work, options) {
        Ok(request) => request,
        Err(error) => {
            let ack = PlaneAck {
                outcome: PlaneAckOutcome::Failed,
                reason: bounded_reason(&error.to_string()),
                request_id: Uuid::new_v4().to_string(),
                checkpoint: None,
            };
            if let Err(ack_err) = intake
                .ack_with_retry(&mut claim, &ack, options, shutdown)
                .await
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
    for (kind, digest, reason) in prepared.before_run_events.drain(..) {
        report_claim_event_with_retry(
            intake, &mut claim, kind, &digest, &reason, options, shutdown,
        )
        .await
        .inspect_err(|error| lifecycle_observe_error(options, Some(&claim_id), error))?;
    }

    let call_window = match claim_call_window(&claim, options.heartbeat_interval) {
        Ok(window) => window,
        Err(error) => {
            lifecycle_observe_error(options, Some(&claim_id), &error);
            return Err(error);
        }
    };
    claim.lease = tokio::select! {
        result = tokio::time::timeout(call_window, intake.heartbeat(&claim, options.claim_ttl)) => {
            result
                .map_err(|_| {
                    let error = PlaneIntakeError::FenceLost(
                        "pre-run heartbeat did not complete before the lease safety deadline".into(),
                    );
                    lifecycle_observe_error(options, Some(&claim_id), &error);
                    error
                })?
                .inspect_err(|error| lifecycle_observe_error(options, Some(&claim_id), error))?
        }
        _ = wait_for_shutdown(shutdown.clone()) => {
            drain_claim(options, &claim_id);
            return Ok(Execution::Shutdown);
        }
    };
    if *shutdown.borrow() {
        drain_claim(options, &claim_id);
        return Ok(Execution::Shutdown);
    }

    let (cancel_tx, cancel_rx) = watch::channel(false);
    prepared.request.cancel = Some(cancel_rx);
    let resumed = prepared.resumed;
    let expected_checkpoint_digest = prepared.checkpoint_digest.clone();
    let request = prepared.request;
    let mut run = Box::pin(async move {
        if resumed {
            harness
                .run_with_checkpoint_digest(request, &expected_checkpoint_digest)
                .await
        } else {
            harness.run(request).await
        }
    });
    let run_result = loop {
        tokio::select! {
            result = &mut run => break result,
            _ = tokio::time::sleep(options.heartbeat_interval) => {
                let call_window = match claim_call_window(&claim, options.heartbeat_interval) {
                    Ok(window) => window,
                    Err(error) => {
                        let _ = cancel_tx.send(true);
                        lifecycle_observe_error(options, Some(&claim_id), &error);
                        return Err(error);
                    }
                };
                match tokio::time::timeout(
                    call_window,
                    intake.heartbeat(&claim, options.claim_ttl),
                ).await {
                    Ok(Ok(lease)) => claim.lease = lease,
                    Ok(Err(error)) => {
                        let _ = cancel_tx.send(true);
                        lifecycle_observe_error(options, Some(&claim_id), &error);
                        return Err(error);
                    }
                    Err(_) => {
                        let _ = cancel_tx.send(true);
                        let error = PlaneIntakeError::FenceLost(
                            "heartbeat did not complete before the lease safety deadline".into(),
                        );
                        lifecycle_observe_error(options, Some(&claim_id), &error);
                        return Err(error);
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

    if prepared.resumed && matches!(&run_result, Ok(result) if result.success) {
        report_claim_event_with_retry(
            intake,
            &mut claim,
            PlaneClaimEventKind::ResumeSucceeded,
            &prepared.checkpoint_digest,
            "",
            options,
            shutdown,
        )
        .await
        .inspect_err(|error| lifecycle_observe_error(options, Some(&claim_id), error))?;
    }

    let (ack, governance_abort) = match run_result {
        Ok(result) if result.termination == RunTermination::Parked => {
            let checkpoint = options
                .checkpoint_store_id
                .as_deref()
                .and_then(|store_id| checkpoint_for_park(harness, store_id, &result.run_id).ok());
            (
                PlaneAck {
                    outcome: PlaneAckOutcome::Parked,
                    reason: bounded_reason(&park_reason(&result)),
                    request_id: Uuid::new_v4().to_string(),
                    checkpoint,
                },
                false,
            )
        }
        Ok(result) if result.success => (
            PlaneAck {
                outcome: PlaneAckOutcome::Completed,
                reason: bounded_reason(&result.summary),
                request_id: Uuid::new_v4().to_string(),
                checkpoint: None,
            },
            false,
        ),
        Ok(result) => (
            PlaneAck {
                outcome: PlaneAckOutcome::Failed,
                reason: bounded_reason(&result.summary),
                request_id: Uuid::new_v4().to_string(),
                checkpoint: None,
            },
            false,
        ),
        Err(error) => {
            let governance_fail = harness_error_is_governance(&error);
            if let Some(lc) = &options.lifecycle
                && governance_fail
            {
                let _ = lc.set_governance_ok(false);
            }
            (
                PlaneAck {
                    outcome: PlaneAckOutcome::Failed,
                    reason: bounded_reason(&error.to_string()),
                    request_id: Uuid::new_v4().to_string(),
                    checkpoint: None,
                },
                governance_fail,
            )
        }
    };
    intake
        .ack_with_retry(&mut claim, &ack, options, shutdown)
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

fn drain_claim(options: &PlaneServeOptions, claim_id: &str) {
    if let Some(lc) = &options.lifecycle {
        lifecycle_set_draining(lc);
        let _ = lc.drop_active_claim(claim_id);
    }
}
