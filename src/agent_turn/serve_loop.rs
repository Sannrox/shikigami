//! Agent-turn serve admission / poll loop behind the thin host entry point.

use tokio::sync::watch;

use super::{AgentTurnPort, AgentTurnServeOptions, Harness, PlaneIntakeError, claimed_run};
use crate::plane_intake::{
    lifecycle_copy_delayed_evidence, lifecycle_set_draining, wait_for_shutdown,
};

pub(super) async fn run_until_shutdown(
    harness: &Harness,
    intake: &dyn AgentTurnPort,
    options: &AgentTurnServeOptions,
    shutdown: watch::Receiver<bool>,
) -> Result<u64, PlaneIntakeError> {
    if harness.reconcile_delayed_evidence().await.is_err()
        && let Some(lc) = &options.lifecycle
    {
        let _ = lc.set_governance_ok(false);
    }
    if let Some(lc) = &options.lifecycle {
        if !harness.governance_ok() {
            let _ = lc.set_governance_ok(false);
        }
        lifecycle_copy_delayed_evidence(lc, harness);
        let _ = lc.publish();
    }

    let mut completed = 0u64;
    loop {
        if *shutdown.borrow() {
            if let Some(lc) = &options.lifecycle {
                lifecycle_set_draining(lc);
            }
            return Ok(completed);
        }
        if options.max_jobs.is_some_and(|max| completed >= max) {
            if let Some(lc) = &options.lifecycle {
                lifecycle_set_draining(lc);
            }
            return Ok(completed);
        }

        if harness.reconcile_delayed_evidence().await.is_err()
            && let Some(lc) = &options.lifecycle
        {
            let _ = lc.set_governance_ok(false);
        }
        if let Some(lc) = &options.lifecycle {
            if !harness.governance_ok() {
                let _ = lc.set_governance_ok(false);
            }
            lifecycle_copy_delayed_evidence(lc, harness);
            if !lc.accepting_claims() {
                if lc.snapshot().state == crate::worker_lifecycle::WorkerLifecycleState::Draining
                    || *shutdown.borrow()
                {
                    return Ok(completed);
                }
                tokio::select! {
                    _ = tokio::time::sleep(options.poll_interval) => {}
                    _ = wait_for_shutdown(shutdown.clone()) => {
                        lifecycle_set_draining(lc);
                        return Ok(completed);
                    }
                }
                continue;
            }
        }

        let claim_result = tokio::select! {
            result = intake.claim_next(&options.runtime_id, options.claim_ttl) => result,
            _ = wait_for_shutdown(shutdown.clone()) => {
                if let Some(lc) = &options.lifecycle {
                    lifecycle_set_draining(lc);
                }
                return Ok(completed);
            }
        };
        let Some(claim) = claim_result.inspect_err(|error| {
            observe_claim_error(options, error);
        })?
        else {
            tokio::select! {
                _ = tokio::time::sleep(options.poll_interval) => {}
                _ = wait_for_shutdown(shutdown.clone()) => {
                    if let Some(lc) = &options.lifecycle {
                        lifecycle_set_draining(lc);
                    }
                    return Ok(completed);
                }
            }
            continue;
        };
        if *shutdown.borrow() {
            if let Some(lc) = &options.lifecycle {
                lifecycle_set_draining(lc);
            }
            return Ok(completed);
        }
        completed += 1;
        match claimed_run::execute(harness, intake, claim, options, &shutdown).await? {
            claimed_run::Execution::Continue => {
                if let Some(lc) = &options.lifecycle {
                    lifecycle_copy_delayed_evidence(lc, harness);
                }
            }
            claimed_run::Execution::Shutdown => return Ok(completed),
            claimed_run::Execution::GovernanceUnavailable => {
                return Err(PlaneIntakeError::Source(
                    "governance unavailable during run; agent-turn serve exiting for replacement"
                        .into(),
                ));
            }
        }
    }
}

fn observe_claim_error(options: &AgentTurnServeOptions, error: &PlaneIntakeError) {
    let Some(lifecycle) = &options.lifecycle else {
        return;
    };
    match error {
        PlaneIntakeError::FenceLost(kind) => {
            let _ = lifecycle.set_fence_lost(kind);
        }
        PlaneIntakeError::Source(_) | PlaneIntakeError::Harness(_) => {
            let _ = lifecycle.set_governance_ok(false);
        }
        PlaneIntakeError::Mapping(_) => {}
    }
}
