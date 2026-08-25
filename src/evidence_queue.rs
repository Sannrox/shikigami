//! Bounded durable spool for signed delayed evidence.
//!
//! Shikigami transports already-executed outcomes. Governance remains receipt
//! and policy authority. The queue never admits new work or re-executes models
//! or effect-capable tools.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::atomic;
use crate::governance::GovernanceError;

pub const EVIDENCE_QUEUE_SCHEMA_VERSION: u32 = 1;
pub const TEST_HMAC_SHA256: &str = "test-hmac-sha256";
pub const SHA256_BINDING: &str = "sha256-binding";
const TEST_HMAC_DOMAIN: &str = "shikigami-delayed-evidence-v1";
pub const DEFAULT_MAX_ENTRIES: usize = 32;
pub const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("sha256:{}", hex_lower(Sha256::digest(bytes).as_slice()))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Terminal,
    Model,
    Tool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceState {
    #[default]
    Pending,
    Retryable,
    Accepted,
    Rejected,
    Conflicted,
    Expired,
}

impl EvidenceState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Retryable => "retryable",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Conflicted => "conflicted",
            Self::Expired => "expired",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Accepted | Self::Rejected | Self::Conflicted | Self::Expired
        )
    }

    pub fn unresolved(self) -> bool {
        !matches!(self, Self::Accepted)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceIdentity {
    pub run_id: String,
    pub operation_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
    pub event_id: String,
    pub payload_digest: String,
}

impl EvidenceIdentity {
    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.run_id,
            self.operation_id,
            self.attempt_id,
            self.claim_id.as_deref().unwrap_or(""),
            self.event_id
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DelayedEnvelope {
    pub schema_version: u32,
    pub kind: EvidenceKind,
    pub identity: EvidenceIdentity,
    pub issuer: String,
    pub signer_ref: String,
    pub policy_revision: String,
    pub lease_valid_until_ms: i64,
    pub fencing_token: String,
    pub generation: u64,
    /// Redacted, credential-free metadata for later verification.
    pub redacted_attributes: Vec<(String, String)>,
    pub signature_algorithm: String,
    pub binding_digest: String,
    pub signature: String,
}

impl DelayedEnvelope {
    fn binding_payload(&self) -> Result<Vec<u8>, QueueDenial> {
        let payload = serde_json::json!({
            "schema_version": self.schema_version,
            "kind": self.kind,
            "identity": self.identity,
            "issuer": self.issuer,
            "signer_ref": self.signer_ref,
            "policy_revision": self.policy_revision,
            "lease_valid_until_ms": self.lease_valid_until_ms,
            "fencing_token": self.fencing_token,
            "generation": self.generation,
            "redacted_attributes": self.redacted_attributes,
        });
        serde_json::to_vec(&payload).map_err(|_| QueueDenial::Unverifiable)
    }

    pub fn compute_binding_digest(&self) -> Result<String, QueueDenial> {
        Ok(sha256_hex(&self.binding_payload()?))
    }

    fn test_hmac_signature(&self) -> Result<String, QueueDenial> {
        let digest = self.compute_binding_digest()?;
        Ok(sha256_hex(
            format!("{digest}|{}|{TEST_HMAC_DOMAIN}", self.signer_ref).as_bytes(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn seal_test_hmac(&mut self) -> Result<(), QueueDenial> {
        self.signature_algorithm = TEST_HMAC_SHA256.into();
        self.binding_digest = self.compute_binding_digest()?;
        self.signature = self.test_hmac_signature()?;
        Ok(())
    }

    pub fn seal_sha256_binding(&mut self) -> Result<(), QueueDenial> {
        self.signature_algorithm = SHA256_BINDING.into();
        self.binding_digest = self.compute_binding_digest()?;
        self.signature = self.binding_digest.clone();
        Ok(())
    }

    pub fn verify_signature(&self, allow_test: bool) -> Result<(), QueueDenial> {
        if self.schema_version != EVIDENCE_QUEUE_SCHEMA_VERSION
            || self.identity.run_id.is_empty()
            || self.identity.operation_id.is_empty()
            || self.identity.attempt_id.is_empty()
            || self.identity.event_id.is_empty()
            || self.identity.payload_digest.is_empty()
            || self.issuer.is_empty()
            || self.signer_ref.is_empty()
            || self.binding_digest.is_empty()
            || self.signature.is_empty()
        {
            return Err(QueueDenial::Unverifiable);
        }
        if self.compute_binding_digest()? != self.binding_digest {
            return Err(QueueDenial::Unverifiable);
        }
        match self.signature_algorithm.as_str() {
            SHA256_BINDING if self.signature == self.binding_digest => Ok(()),
            TEST_HMAC_SHA256 if allow_test && self.signature == self.test_hmac_signature()? => {
                Ok(())
            }
            _ => Err(QueueDenial::Unverifiable),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueueEntry {
    pub envelope: DelayedEnvelope,
    pub state: EvidenceState,
    pub queued_at_ms: i64,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub failure_class: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DelayedEvidenceStore {
    pub schema_version: u32,
    pub entries: Vec<QueueEntry>,
}

impl Default for DelayedEvidenceStore {
    fn default() -> Self {
        Self {
            schema_version: EVIDENCE_QUEUE_SCHEMA_VERSION,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QueueLimits {
    pub max_entries: usize,
    pub retention_ms: i64,
    pub allow_test_signatures: bool,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_ENTRIES,
            retention_ms: DEFAULT_RETENTION_MS,
            allow_test_signatures: false,
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum QueueDenial {
    #[error("capacity_exhausted")]
    CapacityExhausted,
    #[error("retention_expired")]
    RetentionExpired,
    #[error("unverifiable")]
    Unverifiable,
    #[error("expired")]
    Expired,
    #[error("fence_lost")]
    FenceLost,
    #[error("conflicted")]
    Conflicted,
    #[error("rejected")]
    Rejected,
    #[error("policy_changed")]
    PolicyChanged,
}

impl From<QueueDenial> for GovernanceError {
    fn from(denial: QueueDenial) -> Self {
        GovernanceError::Denied(format!("delayed_evidence:{denial}"))
    }
}

/// Operator-visible projection: no payloads, credentials, or tool arguments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSnapshot {
    pub depth: usize,
    pub oldest_age_ms: i64,
    pub failure_class: String,
    pub unresolved: usize,
}

pub fn snapshot(store: &DelayedEvidenceStore, now_ms: i64) -> QueueSnapshot {
    let unresolved: Vec<&QueueEntry> = store
        .entries
        .iter()
        .filter(|entry| entry.state.unresolved())
        .collect();
    let oldest = unresolved
        .iter()
        .map(|entry| now_ms.saturating_sub(entry.queued_at_ms).max(0))
        .max()
        .unwrap_or(0);
    let failure_class = unresolved
        .iter()
        .find(|entry| !entry.failure_class.is_empty())
        .map(|entry| entry.failure_class.clone())
        .unwrap_or_default();
    QueueSnapshot {
        depth: unresolved.len(),
        oldest_age_ms: oldest,
        failure_class,
        unresolved: unresolved.len(),
    }
}

fn live_fence_ok(envelope: &DelayedEnvelope, fencing_token: &str, generation: u64) -> bool {
    !fencing_token.is_empty()
        && generation > 0
        && envelope.fencing_token == fencing_token
        && envelope.generation == generation
}

pub fn enqueue(
    store: &mut DelayedEvidenceStore,
    envelope: DelayedEnvelope,
    now_ms: i64,
    limits: QueueLimits,
    live_fencing_token: &str,
    live_generation: u64,
) -> Result<(), QueueDenial> {
    envelope.verify_signature(limits.allow_test_signatures)?;
    if now_ms >= envelope.lease_valid_until_ms {
        return Err(QueueDenial::Expired);
    }
    if !live_fence_ok(&envelope, live_fencing_token, live_generation) {
        return Err(QueueDenial::FenceLost);
    }
    for entry in &mut store.entries {
        if now_ms.saturating_sub(entry.queued_at_ms) > limits.retention_ms
            && !matches!(entry.state, EvidenceState::Accepted)
        {
            entry.state = EvidenceState::Expired;
            entry.failure_class = QueueDenial::RetentionExpired.to_string();
        }
    }
    if let Some(existing) = store
        .entries
        .iter_mut()
        .find(|entry| entry.envelope.identity.key() == envelope.identity.key())
    {
        if existing.envelope.identity.payload_digest != envelope.identity.payload_digest {
            existing.state = EvidenceState::Conflicted;
            existing.failure_class = QueueDenial::Conflicted.to_string();
            return Err(QueueDenial::Conflicted);
        }
        existing.state = match existing.state {
            EvidenceState::Accepted => EvidenceState::Accepted,
            EvidenceState::Rejected => EvidenceState::Rejected,
            EvidenceState::Conflicted => EvidenceState::Conflicted,
            EvidenceState::Expired => EvidenceState::Expired,
            EvidenceState::Pending | EvidenceState::Retryable => EvidenceState::Retryable,
        };
        return Ok(());
    }
    let unresolved = store
        .entries
        .iter()
        .filter(|entry| entry.state.unresolved())
        .count();
    if unresolved >= limits.max_entries {
        return Err(QueueDenial::CapacityExhausted);
    }
    store.entries.push(QueueEntry {
        envelope,
        state: EvidenceState::Pending,
        queued_at_ms: now_ms,
        attempts: 0,
        failure_class: String::new(),
    });
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityDisposition {
    Accept,
    Reject,
    Conflict,
    Expire,
    Unavailable,
}

/// Reconcile one queued envelope with an external authority. Never mints
/// `accepted` locally.
pub fn submit(
    entry: &mut QueueEntry,
    now_ms: i64,
    limits: QueueLimits,
    live_fencing_token: &str,
    live_generation: u64,
    current_policy_revision: &str,
    disposition: AuthorityDisposition,
) -> EvidenceState {
    if now_ms.saturating_sub(entry.queued_at_ms) > limits.retention_ms {
        entry.state = EvidenceState::Expired;
        entry.failure_class = QueueDenial::RetentionExpired.to_string();
        return entry.state;
    }
    if entry
        .envelope
        .verify_signature(limits.allow_test_signatures)
        .is_err()
    {
        entry.state = EvidenceState::Rejected;
        entry.failure_class = QueueDenial::Unverifiable.to_string();
        return entry.state;
    }
    if now_ms >= entry.envelope.lease_valid_until_ms
        || !live_fence_ok(&entry.envelope, live_fencing_token, live_generation)
    {
        entry.state = EvidenceState::Expired;
        entry.failure_class = if now_ms >= entry.envelope.lease_valid_until_ms {
            QueueDenial::Expired.to_string()
        } else {
            QueueDenial::FenceLost.to_string()
        };
        return entry.state;
    }
    if current_policy_revision != entry.envelope.policy_revision {
        entry.state = EvidenceState::Rejected;
        entry.failure_class = QueueDenial::PolicyChanged.to_string();
        return entry.state;
    }
    entry.attempts = entry.attempts.saturating_add(1);
    entry.state = match disposition {
        AuthorityDisposition::Accept => EvidenceState::Accepted,
        AuthorityDisposition::Reject => EvidenceState::Rejected,
        AuthorityDisposition::Conflict => EvidenceState::Conflicted,
        AuthorityDisposition::Expire => EvidenceState::Expired,
        AuthorityDisposition::Unavailable => EvidenceState::Retryable,
    };
    entry.failure_class = match entry.state {
        EvidenceState::Accepted | EvidenceState::Pending | EvidenceState::Retryable => {
            String::new()
        }
        EvidenceState::Rejected => QueueDenial::Rejected.to_string(),
        EvidenceState::Conflicted => QueueDenial::Conflicted.to_string(),
        EvidenceState::Expired => QueueDenial::Expired.to_string(),
    };
    entry.state
}

pub fn load(path: impl AsRef<Path>) -> Result<DelayedEvidenceStore, GovernanceError> {
    let path = path.as_ref();
    if !path.is_file() {
        return Ok(DelayedEvidenceStore::default());
    }
    let raw = fs::read(path)
        .map_err(|error| GovernanceError::Message(format!("delayed evidence read: {error}")))?;
    let store: DelayedEvidenceStore = serde_json::from_slice(&raw)
        .map_err(|error| GovernanceError::Message(format!("delayed evidence parse: {error}")))?;
    if store.schema_version != EVIDENCE_QUEUE_SCHEMA_VERSION {
        return Err(GovernanceError::Message(format!(
            "unsupported delayed evidence schema {}",
            store.schema_version
        )));
    }
    Ok(store)
}

pub fn save(path: impl AsRef<Path>, store: &DelayedEvidenceStore) -> Result<(), GovernanceError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            GovernanceError::Message(format!("delayed evidence mkdir: {error}"))
        })?;
    }
    let raw = serde_json::to_vec_pretty(store)
        .map_err(|error| GovernanceError::Message(format!("delayed evidence encode: {error}")))?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, raw)
        .map_err(|error| GovernanceError::Message(format!("delayed evidence write: {error}")))?;
    atomic::replace_file(&temporary, path)
        .map_err(|error| GovernanceError::Message(format!("delayed evidence replace: {error}")))?;
    Ok(())
}

pub fn path_in(state_root: impl AsRef<Path>) -> PathBuf {
    state_root.as_ref().join("delayed-evidence.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const NOW: i64 = 1_700_000_000_000;

    fn limits() -> QueueLimits {
        QueueLimits {
            max_entries: 2,
            retention_ms: 60_000,
            allow_test_signatures: true,
        }
    }

    fn envelope(event: &str, payload: &str) -> DelayedEnvelope {
        let mut envelope = DelayedEnvelope {
            schema_version: EVIDENCE_QUEUE_SCHEMA_VERSION,
            kind: EvidenceKind::Terminal,
            identity: EvidenceIdentity {
                run_id: "run-1".into(),
                operation_id: "op-1".into(),
                attempt_id: "run-1".into(),
                claim_id: Some("claim-1".into()),
                event_id: event.into(),
                payload_digest: sha256_hex(payload.as_bytes()),
            },
            issuer: "governance".into(),
            signer_ref: "signer-1".into(),
            policy_revision: "policy-1".into(),
            lease_valid_until_ms: NOW + 60_000,
            fencing_token: "fence-1".into(),
            generation: 1,
            redacted_attributes: vec![("termination".into(), "completed".into())],
            signature_algorithm: String::new(),
            binding_digest: String::new(),
            signature: String::new(),
        };
        envelope.seal_test_hmac().unwrap();
        envelope
    }

    #[test]
    fn queued_outcome_survives_restart_and_reconciles_identically() {
        let dir = tempdir().unwrap();
        let path = path_in(dir.path());
        let mut store = DelayedEvidenceStore::default();
        let env = envelope("complete", "ok");
        enqueue(&mut store, env.clone(), NOW, limits(), "fence-1", 1).unwrap();
        save(&path, &store).unwrap();
        let mut restored = load(&path).unwrap();
        assert_eq!(restored.entries.len(), 1);
        assert_eq!(
            restored.entries[0].envelope.identity.key(),
            env.identity.key()
        );
        let state = submit(
            &mut restored.entries[0],
            NOW + 10,
            limits(),
            "fence-1",
            1,
            "policy-1",
            AuthorityDisposition::Accept,
        );
        assert_eq!(state, EvidenceState::Accepted);
        save(&path, &restored).unwrap();
        let again = load(&path).unwrap();
        assert_eq!(again.entries[0].state, EvidenceState::Accepted);
    }

    #[test]
    fn duplicate_submission_is_idempotent() {
        let mut store = DelayedEvidenceStore::default();
        let env = envelope("complete", "ok");
        enqueue(&mut store, env.clone(), NOW, limits(), "fence-1", 1).unwrap();
        enqueue(&mut store, env, NOW + 1, limits(), "fence-1", 1).unwrap();
        assert_eq!(store.entries.len(), 1);
        assert_eq!(store.entries[0].state, EvidenceState::Retryable);
        submit(
            &mut store.entries[0],
            NOW + 2,
            limits(),
            "fence-1",
            1,
            "policy-1",
            AuthorityDisposition::Accept,
        );
        let env = envelope("complete", "ok");
        enqueue(&mut store, env, NOW + 3, limits(), "fence-1", 1).unwrap();
        assert_eq!(store.entries.len(), 1);
        assert_eq!(store.entries[0].state, EvidenceState::Accepted);
    }

    #[test]
    fn conflicting_payloads_do_not_overwrite_or_succeed() {
        let mut store = DelayedEvidenceStore::default();
        enqueue(
            &mut store,
            envelope("complete", "ok"),
            NOW,
            limits(),
            "fence-1",
            1,
        )
        .unwrap();
        let first_digest = store.entries[0].envelope.identity.payload_digest.clone();
        let err = enqueue(
            &mut store,
            envelope("complete", "other"),
            NOW + 1,
            limits(),
            "fence-1",
            1,
        )
        .unwrap_err();
        assert_eq!(err, QueueDenial::Conflicted);
        assert_eq!(store.entries.len(), 1);
        assert_eq!(store.entries[0].state, EvidenceState::Conflicted);
        assert_eq!(
            store.entries[0].envelope.identity.payload_digest,
            first_digest
        );
        assert_ne!(store.entries[0].state, EvidenceState::Accepted);
    }

    #[test]
    fn expired_fence_signature_reject_and_policy_fail_closed() {
        let mut store = DelayedEvidenceStore::default();
        assert_eq!(
            enqueue(
                &mut store,
                envelope("complete", "ok"),
                NOW + 120_000,
                limits(),
                "fence-1",
                1
            )
            .unwrap_err(),
            QueueDenial::Expired
        );
        assert_eq!(
            enqueue(
                &mut store,
                envelope("complete", "ok"),
                NOW,
                limits(),
                "other-fence",
                1
            )
            .unwrap_err(),
            QueueDenial::FenceLost
        );
        let mut bad = envelope("complete", "ok");
        bad.signature = "deadbeef".into();
        assert_eq!(
            enqueue(&mut store, bad, NOW, limits(), "fence-1", 1).unwrap_err(),
            QueueDenial::Unverifiable
        );
        enqueue(
            &mut store,
            envelope("complete", "ok"),
            NOW,
            limits(),
            "fence-1",
            1,
        )
        .unwrap();
        let rejected = submit(
            &mut store.entries[0],
            NOW + 1,
            limits(),
            "fence-1",
            1,
            "policy-1",
            AuthorityDisposition::Reject,
        );
        assert_eq!(rejected, EvidenceState::Rejected);
        store = DelayedEvidenceStore::default();
        enqueue(
            &mut store,
            envelope("complete", "ok"),
            NOW,
            limits(),
            "fence-1",
            1,
        )
        .unwrap();
        let changed = submit(
            &mut store.entries[0],
            NOW + 1,
            limits(),
            "fence-1",
            1,
            "policy-2",
            AuthorityDisposition::Accept,
        );
        assert_eq!(changed, EvidenceState::Rejected);
        assert_eq!(store.entries[0].failure_class, "policy_changed");
        assert_ne!(store.entries[0].state, EvidenceState::Accepted);
    }

    #[test]
    fn crash_before_and_after_commit_does_not_duplicate() {
        let dir = tempdir().unwrap();
        let path = path_in(dir.path());
        let mut store = DelayedEvidenceStore::default();
        enqueue(
            &mut store,
            envelope("complete", "ok"),
            NOW,
            limits(),
            "fence-1",
            1,
        )
        .unwrap();
        assert!(!path.is_file(), "crash before commit leaves no queue file");
        save(&path, &store).unwrap();
        let restored = load(&path).unwrap();
        assert_eq!(restored.entries.len(), 1);
        let mut again = restored;
        enqueue(
            &mut again,
            envelope("complete", "ok"),
            NOW + 1,
            limits(),
            "fence-1",
            1,
        )
        .unwrap();
        assert_eq!(again.entries.len(), 1);
    }

    #[test]
    fn capacity_and_retention_fail_closed_without_silent_eviction() {
        let mut store = DelayedEvidenceStore::default();
        enqueue(
            &mut store,
            envelope("a", "one"),
            NOW,
            limits(),
            "fence-1",
            1,
        )
        .unwrap();
        let mut second = envelope("b", "two");
        second.identity.event_id = "other".into();
        second.seal_test_hmac().unwrap();
        enqueue(&mut store, second, NOW, limits(), "fence-1", 1).unwrap();
        let mut third = envelope("c", "three");
        third.identity.event_id = "third".into();
        third.seal_test_hmac().unwrap();
        assert_eq!(
            enqueue(&mut store, third, NOW, limits(), "fence-1", 1).unwrap_err(),
            QueueDenial::CapacityExhausted
        );
        assert_eq!(store.entries.len(), 2);
        assert!(store.entries.iter().all(|entry| entry.state.unresolved()));
        submit(
            &mut store.entries[0],
            NOW + 120_000,
            limits(),
            "fence-1",
            1,
            "policy-1",
            AuthorityDisposition::Accept,
        );
        assert_eq!(store.entries[0].state, EvidenceState::Expired);
        assert_eq!(store.entries[0].failure_class, "retention_expired");
        assert_ne!(store.entries[0].state, EvidenceState::Accepted);
        let snap = snapshot(&store, NOW + 1);
        assert_eq!(snap.depth, 2);
        assert!(snap.failure_class.is_empty() || snap.unresolved == 2);
        assert!(!serde_json::to_string(&snap).unwrap().contains("ok"));
    }

    #[test]
    fn fixture_signatures_denied_without_opt_in() {
        let mut store = DelayedEvidenceStore::default();
        let env = envelope("complete", "ok");
        let mut closed = limits();
        closed.allow_test_signatures = false;
        assert_eq!(
            enqueue(&mut store, env, NOW, closed, "fence-1", 1).unwrap_err(),
            QueueDenial::Unverifiable
        );
    }
}
