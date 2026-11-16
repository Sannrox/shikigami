//! Agent-turn serve admission / poll loop behind the thin host entry point.

use std::time::Duration;

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
    let mut idle = IdleBackoff::new(options.poll_interval);
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
                _ = tokio::time::sleep(idle.next_delay()) => {}
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
        idle.reset();
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

/// Longest idle wait between empty polls, unless `poll_interval` is longer.
const IDLE_BACKOFF_CAP: Duration = Duration::from_secs(5);

/// Delay between polls that found no claimable turn: doubles from
/// `poll_interval` up to [`IDLE_BACKOFF_CAP`], with ±20% jitter so idle
/// workers do not poll in step. A claimed turn resets it.
struct IdleBackoff {
    base: Duration,
    next: Duration,
}

impl IdleBackoff {
    fn new(base: Duration) -> Self {
        Self { base, next: base }
    }

    fn reset(&mut self) {
        self.next = self.base;
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = (self.next * 2).min(IDLE_BACKOFF_CAP.max(self.base));
        // 0.8..1.2 of the delay.
        let jitter = 0.8 + 0.4 * (uuid::Uuid::new_v4().as_u128() % 1_000) as f64 / 1_000.0;
        delay.mul_f64(jitter)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// #494: idle workers polled every 200 ms forever. The delay grows to the
    /// cap and a claimed turn brings it back to `poll_interval`.
    #[test]
    fn idle_polls_back_off_to_the_cap_and_reset_on_work() {
        let base = Duration::from_millis(200);
        let mut idle = IdleBackoff::new(base);
        let delays: Vec<_> = (0..10).map(|_| idle.next_delay()).collect();
        assert!(delays[0] >= base.mul_f64(0.8) && delays[0] <= base.mul_f64(1.2));
        let last = *delays.last().unwrap();
        assert!(last >= IDLE_BACKOFF_CAP.mul_f64(0.8) && last <= IDLE_BACKOFF_CAP.mul_f64(1.2));

        idle.reset();
        assert!(idle.next_delay() <= base.mul_f64(1.2));
    }
}
