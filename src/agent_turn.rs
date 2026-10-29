//! Agent-turn intake: claim a site-agent turn and run it as the instance principal.
//!
//! [`AgentTurnPort`] owns claim / heartbeat / ack. Mapping the published
//! definition onto a harness request and driving the loop stay in this host
//! crate. Action-work intake remains [`crate::plane_intake`].

use std::time::Duration;

use tokio::sync::watch;

use crate::harness::Harness;

mod claimed_run;
mod serve_loop;

pub use shikigami_plane_intake::{
    AgentTurnAck, AgentTurnContinuation, AgentTurnPort, ClaimedAgentTurn, PlaneAckOutcome,
    PlaneClaimLease, PlaneIntakeError, PublishedAgentDefinition, RuntimeToken,
    SiteAgentInstanceView,
};

#[derive(Debug, Clone)]
pub struct AgentTurnServeOptions {
    pub runtime_id: String,
    pub poll_interval: Duration,
    pub max_jobs: Option<u64>,
    pub claim_ttl: Duration,
    pub heartbeat_interval: Duration,
    pub ack_retry_limit: u32,
    /// Optional fleet worker lifecycle publisher (agent-turn intake).
    pub lifecycle: Option<crate::worker_lifecycle::WorkerLifecycle>,
}

impl Default for AgentTurnServeOptions {
    fn default() -> Self {
        let claim_ttl = Duration::from_secs(60);
        Self {
            runtime_id: "shikigami".into(),
            poll_interval: Duration::from_millis(200),
            max_jobs: None,
            claim_ttl,
            heartbeat_interval: claim_ttl / 3,
            ack_retry_limit: 5,
            lifecycle: None,
        }
    }
}

/// Pull, claim, execute, heartbeat, and acknowledge agent-turn work until
/// shutdown or `max_jobs`.
pub async fn run_agent_turn_serve(
    harness: &Harness,
    intake: &dyn AgentTurnPort,
    options: AgentTurnServeOptions,
    shutdown: watch::Receiver<bool>,
) -> Result<u64, PlaneIntakeError> {
    if options.claim_ttl.is_zero() {
        return Err(PlaneIntakeError::Source(
            "claim_ttl must be greater than zero".into(),
        ));
    }
    if options.heartbeat_interval.is_zero() || options.heartbeat_interval >= options.claim_ttl {
        return Err(PlaneIntakeError::Source(
            "heartbeat_interval must be positive and shorter than claim_ttl".into(),
        ));
    }
    if options.ack_retry_limit == 0 {
        return Err(PlaneIntakeError::Source(
            "ack_retry_limit must be greater than zero".into(),
        ));
    }
    serve_loop::run_until_shutdown(harness, intake, &options, shutdown).await
}
