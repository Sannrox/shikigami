//! Plane serve admission / poll loop behind the thin host entry point.
//!
//! Owns shutdown races, lifecycle accepting/draining gates, poll sleep,
//! max_jobs, claim select, and error observation. Per-claim execution stays
//! in [`super::claimed_run`].

use tokio::sync::watch;

use super::{
    Harness, PlaneIntakeError, PlaneIntakePort, PlaneServeOptions, claimed_run,
    lifecycle_set_draining, wait_for_shutdown,
};

/// Pull, claim, and drive claimed runs until shutdown, max_jobs, or fail-closed exit.
pub(super) async fn run_until_shutdown(
    harness: &Harness,
    intake: &dyn PlaneIntakePort,
    options: &PlaneServeOptions,
    shutdown: watch::Receiver<bool>,
) -> Result<u64, PlaneIntakeError> {
    if let Some(lc) = &options.lifecycle {
        // Demote only: never auto-clear a runtime governance failure from a
        // static adapter health check alone.
        if !harness.governance_ok() {
            let _ = lc.set_governance_ok(false);
        }
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

        if let Some(lc) = &options.lifecycle {
            if !harness.governance_ok() {
                let _ = lc.set_governance_ok(false);
            }
            if !lc.accepting_claims() {
                // Drain or governance/fence/unhealthy: do not start new claims.
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
            result = intake.claim_next(&options.policy.expected_runtime, options.claim_ttl) => result,
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
        // select! may pick claim_next even when shutdown is also ready; recheck
        // so SIGTERM never starts side effects for a newly acquired claim.
        if *shutdown.borrow() {
            if let Some(lc) = &options.lifecycle {
                lifecycle_set_draining(lc);
            }
            // Leave the plane claim unacked so lease expiry can reclaim it.
            return Ok(completed);
        }
        completed += 1;
        match claimed_run::execute(harness, intake, claim, options, &shutdown).await? {
            claimed_run::Execution::Continue => {}
            claimed_run::Execution::Shutdown => return Ok(completed),
            claimed_run::Execution::GovernanceUnavailable => {
                return Err(PlaneIntakeError::Source(
                    "governance unavailable during run; plane serve exiting for replacement".into(),
                ));
            }
        }
    }
}

fn observe_claim_error(options: &PlaneServeOptions, error: &PlaneIntakeError) {
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
