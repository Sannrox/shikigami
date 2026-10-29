//! Plane-claim port and the claim values adapters and hosts share.
//!
//! Mapping claimed work onto a harness run request and driving the plane
//! serve loop stay in the host crate.

use std::time::{Duration, Instant};

use thiserror::Error;

/// Plane data required to map one claimed Action effect into a harness run.
///
/// `parameters_json` comes from the effect's parent ActionInstance. When those
/// parameters contain `artifact_refs` instead of inline `task`, the intake
/// adapter must resolve them under its own authorization and supply the
/// resulting task in `resolved_task`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedPlaneWork {
    /// Top-level `ActionEffect.effect_id` used for heartbeat/ack. The plane's
    /// v1 `payload_json` does not duplicate this field.
    pub effect_id: String,
    pub instance_id: String,
    pub operation_id: String,
    pub kind: String,
    pub status: String,
    pub payload_json: String,
    pub parameters_json: String,
    pub resolved_task: Option<String>,
    /// Immutable plane-owned continuation returned only after a governed
    /// parked-work resolution has made the same effect ready again.
    pub continuation: Option<PlaneWorkContinuation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneCheckpoint {
    pub store_id: String,
    pub reference: String,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneWorkContinuation {
    pub resolution_id: String,
    pub park_id: String,
    pub effect_id: String,
    pub operation_id: String,
    pub park_generation: u64,
    pub input_json: String,
    pub input_digest: String,
    pub checkpoint: Option<PlaneCheckpoint>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ClaimedWorkMappingError {
    #[error("claimed work {field} is required")]
    MissingField { field: &'static str },
    #[error("claimed effect kind must be runtime_dispatch, got {0:?}")]
    InvalidKind(String),
    #[error("claimed effect status must be claimed, got {0:?}")]
    InvalidStatus(String),
    #[error("claimed effect payload must be a JSON object: {0}")]
    InvalidPayload(String),
    #[error("claimed Action parameters must be a JSON object: {0}")]
    InvalidParameters(String),
    #[error("claimed effect payload {field} does not match the claim envelope")]
    CorrelationMismatch { field: &'static str },
    #[error("claimed effect runtime {actual:?} does not match host runtime {expected:?}")]
    RuntimeMismatch { expected: String, actual: String },
    #[error("claimed Action parameters digest does not match the effect payload")]
    ParametersDigestMismatch,
    #[error("claimed Action task is required")]
    MissingTask,
    #[error("artifact_refs require an authorized host resolution result")]
    ArtifactResolutionRequired,
    #[error("claimed Action task is {actual} bytes; maximum is {maximum}")]
    TaskTooLarge { actual: usize, maximum: usize },
    #[error("claimed Action timeout_secs must be a positive integer")]
    InvalidTimeout,
    #[error("claimed Action keep_workspace must be a boolean")]
    InvalidKeepWorkspace,
    #[error("claimed Action artifact_refs must be an array of non-empty strings")]
    InvalidArtifactRefs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneClaimLease {
    pub runtime_id: String,
    pub generation: u64,
    pub fencing_token: String,
    pub expires_at_ms: i64,
    /// Local monotonic deadline: `min(requested TTL from RPC start, granted remaining)`.
    /// Granted remaining comes from plane `expires_at_ms` versus the local wall
    /// clock at grant interpretation. Missing or already-expired grants are
    /// fence loss. After that, local fencing uses this Instant only.
    pub valid_until: Instant,
    /// Per-claim credential for heartbeat, ack, and claim events.
    pub runtime_token: RuntimeToken,
}

/// Short-lived plane credential granted with one claim. It authenticates only
/// the follow-up calls for that claim; `Debug` never prints the value.
#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeToken {
    value: String,
    expires_at_ms: i64,
}

impl RuntimeToken {
    /// Accept a token from a claim or heartbeat grant. A missing token refuses
    /// the claimed work (the plane needs a signing key and a tenant-scoped
    /// worker credential); an expired one is fence loss.
    pub fn from_grant(
        value: String,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<Self, PlaneIntakeError> {
        if value.trim().is_empty() || expires_at_ms <= 0 {
            return Err(PlaneIntakeError::Source(
                "claim grant has no runtime token; the plane must sign runtime tokens and the worker credential must carry a tenant".into(),
            ));
        }
        let token = Self {
            value,
            expires_at_ms,
        };
        token.check_live(now_ms)?;
        Ok(token)
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }

    /// Refuse use at or after expiry (wall clock, milliseconds since epoch).
    pub fn check_live(&self, now_ms: i64) -> Result<(), PlaneIntakeError> {
        if now_ms >= self.expires_at_ms {
            return Err(PlaneIntakeError::FenceLost("runtime token expired".into()));
        }
        Ok(())
    }
}

impl std::fmt::Debug for RuntimeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeToken")
            .field("value", &"[REDACTED]")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneClaim {
    pub work: ClaimedPlaneWork,
    pub lease: PlaneClaimLease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaneAckOutcome {
    Completed,
    Failed,
    Parked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneAck {
    pub outcome: PlaneAckOutcome,
    pub reason: String,
    pub request_id: String,
    pub checkpoint: Option<PlaneCheckpoint>,
    /// Credential-free retained-artifact manifest JSON. Empty means omitted.
    pub artifact_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaneClaimEventKind {
    ResumeStarted,
    ResumeSucceeded,
    CheckpointUnavailable,
    ReplacementStarted,
}

impl PlaneClaimEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResumeStarted => "resume_started",
            Self::ResumeSucceeded => "resume_succeeded",
            Self::CheckpointUnavailable => "checkpoint_unavailable",
            Self::ReplacementStarted => "replacement_started",
        }
    }
}

impl PlaneAckOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Parked => "parked",
        }
    }
}

#[derive(Debug, Error)]
pub enum PlaneIntakeError {
    #[error("plane intake: {0}")]
    Source(String),
    #[error("plane intake fence lost: {0}")]
    FenceLost(String),
    #[error(transparent)]
    Mapping(#[from] ClaimedWorkMappingError),
    /// Host run failure while executing claimed work (`HarnessError` display).
    #[error("{0}")]
    Harness(String),
}

#[async_trait::async_trait]
pub trait PlaneIntakePort: Send + Sync {
    async fn claim_next(
        &self,
        runtime_id: &str,
        ttl: Duration,
    ) -> Result<Option<PlaneClaim>, PlaneIntakeError>;

    async fn heartbeat(
        &self,
        claim: &PlaneClaim,
        ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError>;

    async fn ack(&self, claim: &PlaneClaim, ack: &PlaneAck) -> Result<(), PlaneIntakeError>;

    async fn report_claim_event(
        &self,
        claim: &PlaneClaim,
        kind: PlaneClaimEventKind,
        checkpoint_digest: &str,
        reason_code: &str,
        request_id: &str,
    ) -> Result<(), PlaneIntakeError>;
}

/// Instance facts a claimed agent turn must load before running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteAgentInstanceView {
    pub instance_id: String,
    pub principal_id: String,
    pub frozen: bool,
    pub kill_switched: bool,
    pub definition_revision_digest: String,
    pub agent_member: String,
}

/// Published agent-definition member used as the turn's task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedAgentDefinition {
    pub name: String,
    pub task_class: String,
    pub instructions: String,
}

/// Plane-owned continuation after a parked Action is decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnContinuation {
    pub action_instance_id: String,
    pub status: String,
    pub approval_decision: String,
    pub deny_reason: String,
}

/// One fenced agent-turn claim plus the instance and definition it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedAgentTurn {
    pub turn_id: String,
    pub namespace: String,
    pub instance_id: String,
    pub principal_id: String,
    pub cause: String,
    pub status: String,
    pub parked_action_instance_id: String,
    pub park_count: i64,
    pub lease: PlaneClaimLease,
    pub instance: SiteAgentInstanceView,
    pub definition: PublishedAgentDefinition,
    pub continuation: Option<AgentTurnContinuation>,
}

/// Terminal or park acknowledgement for an agent turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnAck {
    pub outcome: PlaneAckOutcome,
    pub reason: String,
    /// Required when `outcome` is `Parked`: the Action this claim submitted.
    pub parked_action_instance_id: String,
}

/// Claim / heartbeat / ack for agent-turn work. Separate from Action
/// [`PlaneIntakePort`]: turns carry an instance principal and a parked Action,
/// not a `runtime_dispatch` effect.
#[async_trait::async_trait]
pub trait AgentTurnPort: Send + Sync {
    async fn claim_next(
        &self,
        runtime_id: &str,
        ttl: Duration,
    ) -> Result<Option<ClaimedAgentTurn>, PlaneIntakeError>;

    async fn heartbeat(
        &self,
        claim: &ClaimedAgentTurn,
        ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError>;

    async fn ack(
        &self,
        claim: &ClaimedAgentTurn,
        ack: &AgentTurnAck,
    ) -> Result<(), PlaneIntakeError>;
}
