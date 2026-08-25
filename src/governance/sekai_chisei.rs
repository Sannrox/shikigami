//! First-party sekai-chisei governance adapter.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;

use crate::checkpoint::{
    GovernanceCheckpoint, GovernanceEvidenceReference, StagedToolExecution, StagedToolReport,
};
use crate::config::Config;
use crate::content::ContentModelTurnV1;
use crate::model::{ChatMessage, ModelTurn};
use crate::tools::ToolDef;

use super::{
    AvailableModel, ContentTurnContext, GovernanceError, GovernancePort, RunHandle, RunOutcome,
};

pub use sekai_client::protocol as proto;
use sha2::{Digest, Sha256};

use proto::chisei::GetEffectivePolicySummaryRequest;

mod claim_acquisition;
mod governed_content_turn;
mod governed_model_turn;
mod governed_run_admission;
mod governed_run_completion;
mod harvest_event_reporting;
mod harvest_transaction;
mod plane_session;
mod tool_authorization;

use harvest_transaction::HarvestTransaction;
pub(super) type PlaneClient = plane_session::PlaneClient;

fn available_model(model: proto::chisei::AvailableModelRecord) -> AvailableModel {
    AvailableModel {
        provider: model.provider,
        upstream_model: model.upstream_model,
        canonical_model: model.canonical_model,
        lifecycle: model.lifecycle,
    }
}

fn available_models_from_summary(
    response: proto::chisei::GetEffectivePolicySummaryResponse,
) -> Vec<AvailableModel> {
    if response.models.is_empty() {
        return Vec::new();
    }
    response.models.into_iter().map(available_model).collect()
}

pub struct SekaiChiseiGovernance {
    endpoint: String,
    principal: String,
    namespace: String,
    fail_closed: bool,
    token_env: Option<String>,
    max_tokens: i32,
    preferred_model: String,
    harvest: HarvestTransaction,
    fallback_enabled: bool,
    fallback_allow_test_signatures: bool,
    delayed: std::sync::Mutex<DelayedEvidenceRuntime>,
}

struct DelayedEvidenceRuntime {
    enabled: bool,
    limits: crate::evidence_queue::QueueLimits,
    root: Option<std::path::PathBuf>,
}

/// Runtime-claim client used by the explicit plane intake mode of `serve`.
///
/// This client only claims and acknowledges already-admitted work. Governance
/// planning, tool authorization, and harvest remain on [`SekaiChiseiGovernance`].
/// One connected plane client is reused for list/claim/lease RPCs and is
/// dropped only after a transport error.
pub struct SekaiClaimClient {
    inner: SekaiChiseiGovernance,
    namespace: String,
    plane: CachedClient<PlaneClient>,
}

impl SekaiClaimClient {
    pub fn from_config(config: &Config) -> Result<Self, GovernanceError> {
        Ok(Self {
            inner: SekaiChiseiGovernance::from_config(config)?,
            namespace: config.governance.namespace.clone(),
            plane: CachedClient::new(),
        })
    }

    pub(super) async fn connected_plane(
        &self,
    ) -> Result<
        tokio::sync::MutexGuard<'_, Option<PlaneClient>>,
        crate::plane_intake::PlaneIntakeError,
    > {
        self.plane
            .get_or_try_insert_with(|| plane_session::connect(&self.inner))
            .await
            .map_err(plane_intake_source)
    }
}

/// Reuses one connected value until a caller invalidates it after a transport error.
struct CachedClient<T> {
    slot: tokio::sync::Mutex<Option<T>>,
}

impl<T> CachedClient<T> {
    fn new() -> Self {
        Self {
            slot: tokio::sync::Mutex::new(None),
        }
    }

    async fn get_or_try_insert_with<F, Fut, E>(
        &self,
        connect: F,
    ) -> Result<tokio::sync::MutexGuard<'_, Option<T>>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut guard = self.slot.lock().await;
        if guard.is_none() {
            *guard = Some(connect().await?);
        }
        Ok(guard)
    }
}

impl SekaiChiseiGovernance {
    pub fn from_config(config: &Config) -> Result<Self, GovernanceError> {
        // Allow construction without endpoint so `doctor` can report the gap.
        let endpoint = config.governance.endpoint.clone().unwrap_or_default();
        Ok(Self {
            endpoint,
            principal: config.governance.principal.clone(),
            namespace: config.governance.namespace.clone(),
            fail_closed: config.requires_governance(),
            token_env: config.governance.token_env.clone(),
            max_tokens: 4096,
            preferred_model: config.model.model.clone(),
            harvest: HarvestTransaction::default(),
            fallback_enabled: config.model.fallback.enabled,
            fallback_allow_test_signatures: config.model.fallback.allow_test_signatures,
            delayed: std::sync::Mutex::new(DelayedEvidenceRuntime {
                enabled: config.governance.delayed_evidence.enabled,
                limits: crate::evidence_queue::QueueLimits {
                    max_entries: config.governance.delayed_evidence.max_entries,
                    retention_ms: i64::try_from(config.governance.delayed_evidence.retention_ms)
                        .unwrap_or(i64::MAX),
                    allow_test_signatures: config.governance.delayed_evidence.allow_test_signatures,
                },
                root: None,
            }),
        })
    }

    pub fn persist_delayed_envelope(
        &self,
        envelope: crate::evidence_queue::DelayedEnvelope,
        now_ms: i64,
        fencing_token: &str,
        generation: u64,
    ) -> Result<(), GovernanceError> {
        self.persist_delayed_envelope_with_grant(envelope, now_ms, fencing_token, generation, None)
    }

    fn persist_delayed_envelope_with_grant(
        &self,
        envelope: crate::evidence_queue::DelayedEnvelope,
        now_ms: i64,
        fencing_token: &str,
        generation: u64,
        reconnect_grant: Option<crate::fallback::FallbackCheckpoint>,
    ) -> Result<(), GovernanceError> {
        let runtime = self
            .delayed
            .lock()
            .map_err(|_| GovernanceError::Message("delayed evidence lock poisoned".into()))?;
        if !runtime.enabled {
            return Err(GovernanceError::Denied("delayed_evidence:disabled".into()));
        }
        let root = runtime.root.clone().ok_or_else(|| {
            GovernanceError::Message("delayed evidence state root unbound".into())
        })?;
        let path = crate::evidence_queue::path_in(root);
        let mut store = crate::evidence_queue::load(&path)?;
        let identity = envelope.identity.key();
        let result = crate::evidence_queue::enqueue(
            &mut store,
            envelope,
            now_ms,
            runtime.limits,
            fencing_token,
            generation,
        );
        if result.is_ok()
            && let Some(grant) = reconnect_grant
            && let Some(entry) = store
                .entries
                .iter_mut()
                .find(|entry| entry.envelope.identity.key() == identity)
        {
            entry.reconnect_grant = Some(grant);
        }
        crate::evidence_queue::save(&path, &store)?;
        result.map_err(Into::into)
    }

    fn delayed_evidence_enabled(&self) -> Result<bool, GovernanceError> {
        Ok(self
            .delayed
            .lock()
            .map_err(|_| GovernanceError::Message("delayed evidence lock poisoned".into()))?
            .enabled)
    }

    fn queue_or_preserve_unavailable(
        &self,
        handle: &RunHandle,
        outcome: &RunOutcome,
        message: String,
    ) -> Result<(), GovernanceError> {
        if !self.delayed_evidence_enabled()? {
            return Err(GovernanceError::Unavailable(message));
        }
        match self.queue_terminal_outcome(handle, outcome) {
            Ok(()) => Err(GovernanceError::Unavailable(
                "delayed evidence queued; receipt authority unavailable".into(),
            )),
            Err(GovernanceError::Denied(denied))
                if denied.starts_with("delayed_evidence:missing_")
                    || denied == "delayed_evidence:identity_mismatch"
                    || denied == "delayed_evidence:disabled" =>
            {
                Err(GovernanceError::Unavailable(message))
            }
            Err(error) => Err(error),
        }
    }

    fn queue_terminal_outcome(
        &self,
        handle: &RunHandle,
        outcome: &RunOutcome,
    ) -> Result<(), GovernanceError> {
        let fallback = self
            .harvest
            .fallback(&handle.run_id)?
            .ok_or_else(|| GovernanceError::Denied("delayed_evidence:missing_grant".into()))?;
        let fence = &fallback.held_fence;
        let auth = &fallback.authorization;
        if fence.fencing_token.is_empty() || fence.generation == 0 {
            return Err(GovernanceError::Denied(
                "delayed_evidence:missing_fence".into(),
            ));
        }
        if auth.run_id != handle.run_id || auth.operation_id != handle.operation_id {
            return Err(GovernanceError::Denied(
                "delayed_evidence:identity_mismatch".into(),
            ));
        }
        let receipt_operation_id = self
            .harvest
            .host_operation_id(handle)
            .map_err(|_| GovernanceError::Denied("delayed_evidence:missing_receipt".into()))?;
        if receipt_operation_id.is_empty() {
            return Err(GovernanceError::Denied(
                "delayed_evidence:missing_receipt".into(),
            ));
        }
        let pending = self
            .harvest
            .pending_event(&handle.run_id)?
            .filter(|event| event.kind == harvest::KIND_COMPLETE);
        let parent_event_id = pending
            .as_ref()
            .map(|event| event.parent_event_id.clone())
            .filter(|parent| !parent.is_empty())
            .or_else(|| {
                self.harvest
                    .checkpoint(&handle.run_id)
                    .map(|checkpoint| checkpoint.last_event_id)
                    .filter(|event_id| !event_id.is_empty())
            })
            .unwrap_or_else(|| format!("{receipt_operation_id}:budget"));
        let event_id = pending
            .as_ref()
            .map(|event| event.event_id.clone())
            .filter(|event_id| !event_id.is_empty())
            .unwrap_or_else(|| format!("complete:{}", handle.run_id));
        let operation_id = pending
            .as_ref()
            .map(|event| event.operation_id.clone())
            .filter(|operation_id| !operation_id.is_empty())
            .unwrap_or(receipt_operation_id);
        let redacted_attributes = vec![
            ("termination".into(), outcome.termination.clone()),
            ("success".into(), outcome.success.to_string()),
            ("turns".into(), outcome.turns.to_string()),
        ];
        let report_attributes = pending
            .as_ref()
            .map(|event| {
                event
                    .attributes
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_else(|| harvest::complete_attributes(outcome).into_iter().collect());
        let now_ms = unix_now_ms();
        let payload = serde_json::json!({
            "success": outcome.success,
            "summary": outcome.summary,
            "turns": outcome.turns,
            "termination": outcome.termination,
            "event_id": event_id,
            "parent_event_id": parent_event_id,
            "operation_id": operation_id,
        });
        let payload = serde_json::to_string(&payload).unwrap_or_else(|_| outcome.summary.clone());
        let mut envelope = crate::evidence_queue::DelayedEnvelope {
            schema_version: crate::evidence_queue::EVIDENCE_QUEUE_SCHEMA_VERSION,
            kind: crate::evidence_queue::EvidenceKind::Terminal,
            identity: crate::evidence_queue::EvidenceIdentity {
                run_id: auth.run_id.clone(),
                operation_id,
                attempt_id: auth.attempt_id.clone(),
                claim_id: auth.claim_id.clone(),
                event_id,
                payload_digest: crate::evidence_queue::sha256_hex(payload.as_bytes()),
            },
            issuer: auth.issuer.clone(),
            signer_ref: auth.signer_ref.clone(),
            policy_revision: auth.policy_revision.clone(),
            lease_valid_until_ms: fence.valid_until_ms,
            fencing_token: fence.fencing_token.clone(),
            generation: fence.generation,
            parent_event_id,
            redacted_attributes,
            report_attributes,
            signature_algorithm: String::new(),
            binding_digest: String::new(),
            signature: String::new(),
        };
        envelope.seal_sha256_binding()?;
        self.persist_delayed_envelope_with_grant(
            envelope,
            now_ms,
            &fence.fencing_token,
            fence.generation,
            Some(fallback.clone()),
        )
    }

    fn has_unresolved_queued(&self, run_id: &str) -> Result<bool, GovernanceError> {
        let runtime = self
            .delayed
            .lock()
            .map_err(|_| GovernanceError::Message("delayed evidence lock poisoned".into()))?;
        if !runtime.enabled {
            return Ok(false);
        }
        let Some(root) = runtime.root.clone() else {
            return Ok(false);
        };
        let store = crate::evidence_queue::load(crate::evidence_queue::path_in(root))?;
        Ok(store.entries.iter().any(|entry| {
            entry.envelope.identity.run_id == run_id
                && matches!(
                    entry.state,
                    crate::evidence_queue::EvidenceState::Pending
                        | crate::evidence_queue::EvidenceState::Retryable
                )
        }))
    }

    fn complete_run_from_queue(&self, run_id: &str) -> Result<(), GovernanceError> {
        let runtime = self
            .delayed
            .lock()
            .map_err(|_| GovernanceError::Message("delayed evidence lock poisoned".into()))?;
        let Some(root) = runtime.root.clone() else {
            return Err(GovernanceError::Unavailable(
                "delayed evidence queued; receipt authority unavailable".into(),
            ));
        };
        let store = crate::evidence_queue::load(crate::evidence_queue::path_in(root))?;
        let mine: Vec<_> = store
            .entries
            .iter()
            .filter(|entry| entry.envelope.identity.run_id == run_id)
            .collect();
        if mine.is_empty() {
            return Ok(());
        }
        if mine.iter().any(|entry| {
            matches!(
                entry.state,
                crate::evidence_queue::EvidenceState::Pending
                    | crate::evidence_queue::EvidenceState::Retryable
            )
        }) {
            return Err(GovernanceError::Unavailable(
                "delayed evidence queued; receipt authority unavailable".into(),
            ));
        }
        if let Some(accepted) = mine
            .iter()
            .find(|entry| entry.state == crate::evidence_queue::EvidenceState::Accepted)
        {
            self.harvest
                .commit_event(run_id, accepted.envelope.identity.event_id.clone(), false);
            self.forget_harvest(run_id);
            return Ok(());
        }
        if let Some(failed) = mine.iter().find(|entry| {
            matches!(
                entry.state,
                crate::evidence_queue::EvidenceState::Rejected
                    | crate::evidence_queue::EvidenceState::Conflicted
                    | crate::evidence_queue::EvidenceState::Expired
            )
        }) {
            return Err(GovernanceError::Denied(format!(
                "delayed_evidence:{}",
                failed.failure_class
            )));
        }
        Ok(())
    }

    fn live_reconnect_context(
        &self,
        entry: &crate::evidence_queue::QueueEntry,
    ) -> Option<(String, u64, String, bool)> {
        let allow_test = self.fallback_allow_test_signatures
            || self
                .delayed
                .lock()
                .ok()
                .is_some_and(|runtime| runtime.limits.allow_test_signatures);
        let fallback = self
            .harvest
            .fallback(&entry.envelope.identity.run_id)
            .ok()
            .flatten()
            .or_else(|| entry.reconnect_grant.clone())?;
        fallback.authorization.verify_signature(allow_test).ok()?;
        // Envelope identity.operation_id is the receipt PlanExecution id;
        // the grant's operation_id is the logical handle. Do not equate them.
        if fallback.authorization.revoked
            || !crate::fallback::fence_matches(&fallback.authorization.lease, &fallback.held_fence)
            || fallback.authorization.run_id != entry.envelope.identity.run_id
            || fallback.authorization.attempt_id != entry.envelope.identity.attempt_id
            || fallback.authorization.claim_id != entry.envelope.identity.claim_id
            || fallback.authorization.operation_id.is_empty()
            || fallback.held_fence.fencing_token != entry.envelope.fencing_token
            || fallback.held_fence.generation != entry.envelope.generation
            || fallback.authorization.policy_revision != entry.envelope.policy_revision
        {
            return None;
        }
        Some((
            fallback.held_fence.fencing_token,
            fallback.held_fence.generation,
            fallback.authorization.policy_revision,
            true,
        ))
    }

    async fn submit_envelope_to_authority(
        &self,
        envelope: &crate::evidence_queue::DelayedEnvelope,
    ) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
        let parent_event_id = if envelope.parent_event_id.is_empty() {
            format!("{}:budget", envelope.identity.operation_id)
        } else {
            envelope.parent_event_id.clone()
        };
        let pending = crate::checkpoint::PendingGovernanceEvent {
            operation_id: envelope.identity.operation_id.clone(),
            event_id: envelope.identity.event_id.clone(),
            parent_event_id,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            kind: harvest::KIND_COMPLETE.into(),
            attributes: if envelope.report_attributes.is_empty() {
                envelope.redacted_attributes.iter().cloned().collect()
            } else {
                envelope.report_attributes.iter().cloned().collect()
            },
            references: vec![crate::checkpoint::GovernanceEvidenceReference {
                kind: "payload_digest".into(),
                reference: envelope.identity.payload_digest.clone(),
                content_hash: envelope.identity.payload_digest.clone(),
                disclosed_fields: vec!["payload_digest".into()],
                omitted: false,
                omission_reason: String::new(),
            }],
        };
        harvest_event_reporting::send_pending(self, &pending).await
    }

    /// Store a governance-issued fallback grant for later fail-closed admission.
    /// This does not issue the grant.
    pub fn install_fallback_grant(
        &self,
        run_id: &str,
        authorization: crate::fallback::FallbackAuthorization,
        fence: crate::fallback::LiveFence,
    ) -> Result<(), GovernanceError> {
        let selection = crate::fallback::FallbackSelection {
            authorization_id: authorization.authorization_id.clone(),
            model_content_digest: authorization.model_content_digest.clone(),
            degraded_guarantees: true,
            selected_at_ms: 0,
        };
        self.harvest.set_fallback(
            run_id,
            crate::fallback::checkpoint_after_admit(authorization, selection, fence),
        )
    }

    fn update_host_plan(&self, run_id: &str, operation_id: String) -> Result<(), GovernanceError> {
        self.harvest.set_host_operation(run_id, operation_id)
    }

    fn update_harvest_plan(
        &self,
        run_id: &str,
        operation_id: String,
    ) -> Result<(), GovernanceError> {
        self.harvest.set_model_operation(run_id, operation_id)
    }

    fn harvest_checkpoint_state(&self, run_id: &str) -> Option<GovernanceCheckpoint> {
        self.harvest.checkpoint(run_id)
    }

    #[cfg(test)]
    fn harvest_event_context(
        &self,
        handle: &RunHandle,
    ) -> Result<(String, String, String), GovernanceError> {
        self.harvest_event_context_with_id(handle, None)
    }

    pub(super) fn harvest_event_context_with_id(
        &self,
        handle: &RunHandle,
        requested_event_id: Option<String>,
    ) -> Result<(String, String, String), GovernanceError> {
        self.harvest.event_context(handle, requested_event_id)
    }

    pub(super) fn forget_harvest(&self, run_id: &str) {
        self.harvest.forget(run_id);
    }

    fn stage_tool_reports_for_run(
        &self,
        run_id: &str,
        reports: Vec<StagedToolReport>,
    ) -> Result<(), GovernanceError> {
        self.harvest.stage_tool_reports(run_id, reports)
    }

    fn stage_tool_execution_for_run(
        &self,
        run_id: &str,
        execution: StagedToolExecution,
    ) -> Result<(), GovernanceError> {
        self.harvest.stage_tool_execution(run_id, execution)
    }

    fn mark_tool_execution_complete_for_run(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.harvest.mark_tool_execution_complete(run_id, call_id)
    }

    fn mark_tool_execution_started_for_run(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        self.harvest.mark_tool_execution_started(run_id, call_id)
    }

    fn clear_staged_tool_executions_for_run(&self, run_id: &str) {
        self.harvest.clear_tool_executions(run_id);
    }

    fn recover_staged_tool_executions_for_run(&self, run_id: &str) -> Result<(), GovernanceError> {
        self.harvest.recover_tool_executions(run_id)
    }

    pub(super) fn host_harvest_operation_id(
        &self,
        handle: &RunHandle,
    ) -> Result<String, GovernanceError> {
        self.harvest.host_operation_id(handle)
    }

    pub(super) fn pending_event_references(
        references: &[proto::chisei::OperationEvidenceReference],
    ) -> Vec<GovernanceEvidenceReference> {
        references
            .iter()
            .map(|reference| GovernanceEvidenceReference {
                kind: reference.kind.clone(),
                reference: reference.reference.clone(),
                content_hash: reference.content_hash.clone(),
                disclosed_fields: reference.disclosed_fields.clone(),
                omitted: reference.omitted,
                omission_reason: reference.omission_reason.clone(),
            })
            .collect()
    }

    pub(super) fn proto_event_references(
        references: &[GovernanceEvidenceReference],
    ) -> Vec<proto::chisei::OperationEvidenceReference> {
        references
            .iter()
            .map(|reference| proto::chisei::OperationEvidenceReference {
                kind: reference.kind.clone(),
                reference: reference.reference.clone(),
                content_hash: reference.content_hash.clone(),
                disclosed_fields: reference.disclosed_fields.clone(),
                omitted: reference.omitted,
                omission_reason: reference.omission_reason.clone(),
            })
            .collect()
    }

    pub(super) async fn retry_pending_harvest_event(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        harvest_event_reporting::retry_pending(self, handle).await
    }

    pub(super) async fn report_harvest_event(
        &self,
        handle: &RunHandle,
        kind: &str,
        attributes: HashMap<String, String>,
        references: Vec<proto::chisei::OperationEvidenceReference>,
    ) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
        harvest_event_reporting::report(self, handle, kind, attributes, references).await
    }

    pub(super) async fn report_model_event(
        &self,
        handle: &RunHandle,
        ok: bool,
    ) -> Result<(), GovernanceError> {
        harvest_event_reporting::report_model(self, handle, ok).await
    }

    pub(super) async fn report_failed_model_event(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        harvest_event_reporting::report_failed_model(self, handle).await
    }

    pub(super) async fn report_tool_event(
        &self,
        handle: &RunHandle,
        call_id: Option<&str>,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        harvest_event_reporting::report_tool(self, handle, call_id, name, ok, detail).await
    }

    pub(super) async fn harvest_receipt(
        &self,
        handle: &RunHandle,
    ) -> Result<proto::chisei::GetOperationReceiptResponse, GovernanceError> {
        harvest_event_reporting::harvest_receipt(self, handle).await
    }

    pub(super) fn arguments_digest(args_json: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(args_json.as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    #[cfg(test)]
    fn host_receipt_input(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: &str,
    ) -> proto::chisei::ExecutionInput {
        governed_run_admission::host_receipt_input(self, run_id, task, logical_operation_id)
    }

    pub(super) async fn abort_uncheckpointed_receipt(
        &self,
        handle: &RunHandle,
        reason: &str,
    ) -> Result<(), GovernanceError> {
        harvest_event_reporting::abort_uncheckpointed_receipt(self, handle, reason).await
    }
}

#[async_trait]
impl crate::plane_intake::PlaneIntakePort for SekaiClaimClient {
    async fn claim_next(
        &self,
        runtime_id: &str,
        ttl: Duration,
    ) -> Result<Option<crate::plane_intake::PlaneClaim>, crate::plane_intake::PlaneIntakeError>
    {
        claim_acquisition::claim_next(self, runtime_id, ttl).await
    }

    async fn heartbeat(
        &self,
        claim: &crate::plane_intake::PlaneClaim,
        ttl: Duration,
    ) -> Result<crate::plane_intake::PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
        claim_acquisition::heartbeat(self, claim, ttl).await
    }

    async fn ack(
        &self,
        claim: &crate::plane_intake::PlaneClaim,
        ack: &crate::plane_intake::PlaneAck,
    ) -> Result<(), crate::plane_intake::PlaneIntakeError> {
        claim_acquisition::ack(self, claim, ack).await
    }

    async fn report_claim_event(
        &self,
        claim: &crate::plane_intake::PlaneClaim,
        kind: crate::plane_intake::PlaneClaimEventKind,
        checkpoint_digest: &str,
        reason_code: &str,
        request_id: &str,
    ) -> Result<(), crate::plane_intake::PlaneIntakeError> {
        claim_acquisition::report_claim_event(
            self,
            claim,
            kind,
            checkpoint_digest,
            reason_code,
            request_id,
        )
        .await
    }
}

fn plane_intake_source(error: GovernanceError) -> crate::plane_intake::PlaneIntakeError {
    crate::plane_intake::PlaneIntakeError::Source(error.to_string())
}

#[async_trait]
impl GovernancePort for SekaiChiseiGovernance {
    fn id(&self) -> &'static str {
        "sekai-chisei"
    }

    fn health_detail(&self) -> String {
        if self.endpoint.trim().is_empty() {
            return "endpoint not set (governance.endpoint or SHIKIGAMI_CONTROL_PLANE)".into();
        }
        format!(
            "endpoint={} principal={} namespace={} fail_closed={}",
            self.endpoint, self.principal, self.namespace, self.fail_closed
        )
    }

    fn health_ok(&self) -> bool {
        !self.endpoint.trim().is_empty()
    }

    async fn available_models(&self) -> Result<Vec<AvailableModel>, GovernanceError> {
        let client = plane_session::connect(self).await?;
        let response: proto::chisei::GetEffectivePolicySummaryResponse = client
            .raw()
            .unary(
                "/chisei.ChiseiService/GetEffectivePolicySummary",
                GetEffectivePolicySummaryRequest {
                    namespace: self.namespace.clone(),
                    provider: String::new(),
                },
                plane_session::call_options(self, Some(&self.namespace), None, None),
            )
            .await
            .map_err(|error| plane_session::map_error("GetEffectivePolicySummary", error))?;
        Ok(available_models_from_summary(response))
    }

    async fn begin_run(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
    ) -> Result<RunHandle, GovernanceError> {
        governed_run_admission::admit(self, run_id, task, logical_operation_id, None).await
    }

    async fn begin_run_with_checkpoint(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        checkpoint: Option<&GovernanceCheckpoint>,
    ) -> Result<RunHandle, GovernanceError> {
        governed_run_admission::admit(self, run_id, task, logical_operation_id, checkpoint).await
    }

    fn checkpoint_state(&self, run_id: &str) -> Option<GovernanceCheckpoint> {
        self.harvest_checkpoint_state(run_id)
    }

    fn tool_requires_execution_checkpoint(&self, name: &str) -> bool {
        // Read actions still consume an external-action permit. Persist the
        // stable call identity before authorization so a crash cannot redeem
        // that permit twice on resume, even when the host effect is read-only.
        tool_authorization::requires_external_action(name)
    }

    async fn plan_turn(
        &self,
        handle: &RunHandle,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
        local_model: &dyn crate::model::ModelPort,
    ) -> Result<ModelTurn, GovernanceError> {
        governed_model_turn::execute(self, handle, system, messages, tools, local_model).await
    }

    async fn plan_content_turn(
        &self,
        handle: &RunHandle,
        system: &str,
        context: ContentTurnContext<'_>,
    ) -> Result<ContentModelTurnV1, GovernanceError> {
        governed_content_turn::execute(
            self,
            handle,
            system,
            context.messages,
            context.tools,
            context.capabilities,
            context.resolver,
        )
        .await
    }

    async fn authorize_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError> {
        let fallback_call_id = format!("{name}:{}", Self::arguments_digest(args_json));
        self.authorize_tool_with_id(handle, &fallback_call_id, name, args_json)
            .await
    }

    async fn authorize_tool_with_id(
        &self,
        handle: &RunHandle,
        call_id: &str,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError> {
        tool_authorization::authorize(self, handle, call_id, name, args_json).await
    }

    async fn report_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        if self.harvest.fallback_active(&handle.run_id)? {
            return Ok(());
        }
        self.report_tool_event(handle, None, name, ok, detail).await
    }

    async fn report_model_turn(&self, handle: &RunHandle, ok: bool) -> Result<(), GovernanceError> {
        if self.harvest.fallback_active(&handle.run_id)? {
            let _ = ok;
            self.harvest.mark_model_reported(&handle.run_id);
            return Ok(());
        }
        self.report_model_event(handle, ok).await
    }

    async fn stage_tool_reports(
        &self,
        handle: &RunHandle,
        reports: Vec<StagedToolReport>,
    ) -> Result<(), GovernanceError> {
        Self::stage_tool_reports_for_run(self, &handle.run_id, reports)
    }

    async fn replay_staged_tool_reports(&self, handle: &RunHandle) -> Result<(), GovernanceError> {
        let reports = self.harvest.pending_tool_reports(&handle.run_id)?;
        for report in reports {
            self.report_tool_event(
                handle,
                Some(&report.call_id),
                &report.name,
                report.ok,
                &report.detail,
            )
            .await?;
        }
        Ok(())
    }

    async fn stage_tool_execution(
        &self,
        handle: &RunHandle,
        execution: StagedToolExecution,
    ) -> Result<(), GovernanceError> {
        Self::stage_tool_execution_for_run(self, &handle.run_id, execution)
    }

    async fn mark_tool_execution_complete(
        &self,
        handle: &RunHandle,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        Self::mark_tool_execution_complete_for_run(self, &handle.run_id, call_id)
    }

    async fn mark_tool_execution_started(
        &self,
        handle: &RunHandle,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        Self::mark_tool_execution_started_for_run(self, &handle.run_id, call_id)
    }

    async fn clear_staged_tool_executions(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        self.clear_staged_tool_executions_for_run(&handle.run_id);
        Ok(())
    }

    async fn recover_staged_tool_executions(
        &self,
        handle: &RunHandle,
    ) -> Result<(), GovernanceError> {
        self.recover_staged_tool_executions_for_run(&handle.run_id)
    }

    async fn report_tool_with_id(
        &self,
        handle: &RunHandle,
        call_id: &str,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        self.report_tool_event(handle, Some(call_id), name, ok, detail)
            .await
    }

    async fn abort_uncheckpointed_run(
        &self,
        handle: &RunHandle,
        reason: &str,
    ) -> Result<(), GovernanceError> {
        self.abort_uncheckpointed_receipt(handle, reason).await
    }

    async fn complete_run(
        &self,
        handle: &RunHandle,
        outcome: RunOutcome,
    ) -> Result<(), GovernanceError> {
        if let Err(error) = self.retry_pending_harvest_event(handle).await {
            match error {
                GovernanceError::Unavailable(message) => {
                    let pending_kind = self
                        .harvest
                        .pending_event(&handle.run_id)?
                        .map(|event| event.kind);
                    if pending_kind
                        .as_deref()
                        .is_some_and(|kind| kind != harvest::KIND_COMPLETE)
                    {
                        return Err(GovernanceError::Unavailable(message));
                    }
                    if self.has_unresolved_queued(&handle.run_id)? {
                        self.reconcile_delayed_evidence().await?;
                        return self.complete_run_from_queue(&handle.run_id);
                    }
                    return self.queue_or_preserve_unavailable(handle, &outcome, message);
                }
                other if self.fail_closed => return Err(other),
                _ => {
                    self.forget_harvest(&handle.run_id);
                    return Ok(());
                }
            }
        }
        if self.has_unresolved_queued(&handle.run_id)? {
            self.reconcile_delayed_evidence().await?;
            return self.complete_run_from_queue(&handle.run_id);
        }
        match governed_run_completion::complete(self, handle, outcome.clone()).await {
            Ok(()) => Ok(()),
            Err(GovernanceError::Unavailable(message)) => {
                self.queue_or_preserve_unavailable(handle, &outcome, message)
            }
            Err(error) => Err(error),
        }
    }

    fn bind_state_root(&self, root: &std::path::Path) {
        if let Ok(mut runtime) = self.delayed.lock() {
            runtime.root = Some(root.to_path_buf());
        }
    }

    fn delayed_evidence_snapshot(&self) -> Option<crate::evidence_queue::QueueSnapshot> {
        let runtime = self.delayed.lock().ok()?;
        let root = runtime.root.clone()?;
        let store = crate::evidence_queue::load(crate::evidence_queue::path_in(root)).ok()?;
        Some(crate::evidence_queue::snapshot(&store, unix_now_ms()))
    }

    async fn reconcile_delayed_evidence(
        &self,
    ) -> Result<Option<crate::evidence_queue::QueueSnapshot>, GovernanceError> {
        let (path, limits, mut store) = {
            let runtime = self
                .delayed
                .lock()
                .map_err(|_| GovernanceError::Message("delayed evidence lock poisoned".into()))?;
            if !runtime.enabled {
                return Ok(None);
            }
            let root = runtime.root.clone().ok_or_else(|| {
                GovernanceError::Message("delayed evidence state root unbound".into())
            })?;
            let path = crate::evidence_queue::path_in(root);
            let store = crate::evidence_queue::load(&path)?;
            (path, runtime.limits, store)
        };
        let now_ms = unix_now_ms();
        let mut outcomes: Vec<(String, crate::evidence_queue::EvidenceState, String, u32)> =
            Vec::new();
        for entry in &mut store.entries {
            if !matches!(
                entry.state,
                crate::evidence_queue::EvidenceState::Pending
                    | crate::evidence_queue::EvidenceState::Retryable
            ) {
                continue;
            }
            let Some((token, generation, policy, check_live_fence)) =
                self.live_reconnect_context(entry)
            else {
                continue;
            };
            if crate::evidence_queue::apply_local_gate(
                entry,
                now_ms,
                limits,
                &token,
                generation,
                &policy,
                check_live_fence,
            )
            .is_some()
            {
                outcomes.push((
                    entry.envelope.identity.key(),
                    entry.state,
                    entry.failure_class.clone(),
                    entry.attempts,
                ));
                continue;
            }
            let result = self.submit_envelope_to_authority(&entry.envelope).await;
            let disposition = disposition_from_authority(&entry.envelope, result.as_ref());
            crate::evidence_queue::submit_with_live_check(
                entry,
                now_ms,
                limits,
                &token,
                generation,
                &policy,
                disposition,
                check_live_fence,
            );
            outcomes.push((
                entry.envelope.identity.key(),
                entry.state,
                entry.failure_class.clone(),
                entry.attempts,
            ));
        }
        {
            let _runtime = self
                .delayed
                .lock()
                .map_err(|_| GovernanceError::Message("delayed evidence lock poisoned".into()))?;
            let mut latest = crate::evidence_queue::load(&path)?;
            for (key, state, failure_class, attempts) in outcomes {
                if let Some(entry) = latest
                    .entries
                    .iter_mut()
                    .find(|entry| entry.envelope.identity.key() == key)
                {
                    if entry.state.is_terminal() {
                        continue;
                    }
                    entry.state = state;
                    entry.failure_class = failure_class;
                    entry.attempts = attempts;
                }
            }
            crate::evidence_queue::save(&path, &latest)?;
            Ok(Some(crate::evidence_queue::snapshot(&latest, now_ms)))
        }
    }
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn disposition_from_authority(
    envelope: &crate::evidence_queue::DelayedEnvelope,
    result: Result<&proto::chisei::ReportOperationEventResponse, &GovernanceError>,
) -> crate::evidence_queue::AuthorityDisposition {
    match result {
        Ok(response)
            if (response.recorded || response.event_id == envelope.identity.event_id)
                && response.complete =>
        {
            crate::evidence_queue::AuthorityDisposition::Accept
        }
        Ok(response) if response.recorded || response.event_id == envelope.identity.event_id => {
            crate::evidence_queue::AuthorityDisposition::Unavailable
        }
        Ok(_) => crate::evidence_queue::AuthorityDisposition::Conflict,
        Err(GovernanceError::Unavailable(_)) => {
            crate::evidence_queue::AuthorityDisposition::Unavailable
        }
        Err(GovernanceError::Denied(_)) => crate::evidence_queue::AuthorityDisposition::Reject,
        Err(GovernanceError::Message(message)) if message.contains("conflict") => {
            crate::evidence_queue::AuthorityDisposition::Conflict
        }
        Err(_) => crate::evidence_queue::AuthorityDisposition::Reject,
    }
}

/// Pure harvest mapping: run lifecycle → plane operation events.
/// Local checkpoint/state is never authoritative for governed truth.
pub mod harvest {
    use std::collections::HashMap;

    use super::proto::chisei::OperationEvidenceReference;
    use crate::governance::{RunHandle, RunOutcome};

    /// `PlanExecution` records the intent/policy/routing/budget spine.
    pub const KIND_BEGIN: &str = "intent_recorded";
    pub const KIND_ATTEMPT: &str = "attempt_started";
    pub const KIND_MODEL: &str = "model_called";
    pub const KIND_TOOL: &str = "action_performed";
    pub const KIND_COMPLETE: &str = "outcome_recorded";

    pub fn attempt_attributes(run_id: &str, logical_operation_id: &str) -> HashMap<String, String> {
        HashMap::from([
            ("run_id".into(), run_id.into()),
            ("attempt_id".into(), run_id.into()),
            ("logical_operation_id".into(), logical_operation_id.into()),
            ("harness".into(), "shikigami".into()),
        ])
    }

    pub fn model_attributes(plan_operation_id: &str, ok: bool) -> HashMap<String, String> {
        HashMap::from([
            ("plan_operation_id".into(), plan_operation_id.into()),
            ("ok".into(), ok.to_string()),
            ("harness".into(), "shikigami".into()),
        ])
    }

    pub fn begin_attributes(
        run_id: &str,
        logical_operation_id: &str,
        task: &str,
        principal: &str,
    ) -> HashMap<String, String> {
        let mut attributes = HashMap::new();
        attributes.insert("run_id".into(), run_id.into());
        attributes.insert("attempt_id".into(), run_id.into());
        attributes.insert("logical_operation_id".into(), logical_operation_id.into());
        attributes.insert("operation_id".into(), logical_operation_id.into());
        attributes.insert("task".into(), task.chars().take(4000).collect());
        attributes.insert("principal".into(), principal.into());
        attributes.insert("harness".into(), "shikigami".into());
        attributes.insert("product".into(), "shikigami".into());
        attributes.insert(
            "prompt_id".into(),
            crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT),
        );
        attributes
    }

    pub fn tool_attributes(name: &str, ok: bool, detail: &str) -> HashMap<String, String> {
        let mut attributes = HashMap::new();
        attributes.insert("tool".into(), name.into());
        attributes.insert("ok".into(), ok.to_string());
        attributes.insert("detail".into(), detail.chars().take(2000).collect());
        attributes.insert("harness".into(), "shikigami".into());
        attributes
    }

    pub fn complete_attributes(outcome: &RunOutcome) -> HashMap<String, String> {
        let mut attributes = HashMap::new();
        attributes.insert(
            "status".into(),
            if outcome.success {
                "succeeded".into()
            } else {
                "failed".into()
            },
        );
        attributes.insert("completion_reason".into(), outcome.termination.clone());
        attributes.insert("success".into(), outcome.success.to_string());
        attributes.insert(
            "summary".into(),
            outcome.summary.chars().take(4000).collect(),
        );
        attributes.insert("turns".into(), outcome.turns.to_string());
        attributes.insert("termination".into(), outcome.termination.clone());
        attributes.insert(
            "workspace".into(),
            outcome.workspace.chars().take(2000).collect(),
        );
        attributes.insert("harness".into(), "shikigami".into());
        attributes.insert(
            "authoritative".into(),
            "plane".into(), // local state is non-authoritative for governed truth
        );
        attributes.insert(
            "prompt_id".into(),
            crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT),
        );
        attributes
    }

    pub fn complete_references(
        handle: &RunHandle,
        outcome: &RunOutcome,
    ) -> Vec<OperationEvidenceReference> {
        let mut refs = vec![OperationEvidenceReference {
            kind: "run_id".into(),
            reference: handle.run_id.clone(),
            content_hash: String::new(),
            disclosed_fields: vec!["run_id".into()],
            omitted: false,
            omission_reason: String::new(),
        }];
        if !outcome.workspace.is_empty() {
            refs.push(OperationEvidenceReference {
                kind: "workspace_path".into(),
                reference: outcome.workspace.chars().take(2000).collect(),
                content_hash: String::new(),
                disclosed_fields: vec!["path".into()],
                omitted: false,
                omission_reason: String::new(),
            });
        }
        refs
    }
}

/// Async live probe for doctor.
pub async fn live_probe(config: &Config) -> Result<String, GovernanceError> {
    let g = SekaiChiseiGovernance::from_config(config)?;
    if g.endpoint.trim().is_empty() {
        return Err(GovernanceError::Unavailable(
            "sekai-chisei endpoint not set".into(),
        ));
    }
    plane_session::probe(&g).await?;
    Ok(format!("reachable at {}", g.endpoint))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision(kind: &str, reason: &str) -> proto::chisei::ExternalActionDecision {
        proto::chisei::ExternalActionDecision {
            version: "external-action.decision/v1".into(),
            authorization_id: "auth-1".into(),
            request_digest: "digest".into(),
            decision: kind.into(),
            reason: reason.into(),
            approval_id: String::new(),
            policy_scope: String::new(),
            policy_version: String::new(),
            created_at_ms: 0,
            expires_at_ms: 0,
            cancelled_at_ms: 0,
            assurance: None,
            permit: None,
        }
    }

    #[test]
    fn available_model_projection_preserves_v1_identity_fields() {
        let projected = available_model(proto::chisei::AvailableModelRecord {
            provider: "openai".into(),
            upstream_model: "gpt-5.5".into(),
            canonical_model: "openai/gpt-5.5".into(),
            lifecycle: "enabled".into(),
            ..Default::default()
        });
        assert_eq!(
            projected,
            AvailableModel {
                provider: "openai".into(),
                upstream_model: "gpt-5.5".into(),
                canonical_model: "openai/gpt-5.5".into(),
                lifecycle: "enabled".into(),
            }
        );
    }

    #[test]
    fn empty_policy_summary_projects_no_available_models() {
        let projected = available_models_from_summary(
            proto::chisei::GetEffectivePolicySummaryResponse::default(),
        );
        assert!(projected.is_empty());
    }

    #[test]
    fn harvest_event_context_is_receipt_scoped_and_causal() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        governance
            .harvest
            .start("run-1", "logical-op-1".into())
            .unwrap();
        governance
            .update_host_plan("run-1", "host-plan-1".into())
            .unwrap();
        let handle = RunHandle {
            run_id: "run-1".into(),
            operation_id: "logical-op-1".into(),
            namespace: "default".into(),
        };
        let (operation_id, parent, event_id) = governance.harvest_event_context(&handle).unwrap();
        assert_eq!(operation_id, "host-plan-1");
        assert_eq!(parent, "host-plan-1:budget");
        assert!(event_id.starts_with("report:host-plan-1:"), "{event_id}");
        governance
            .harvest
            .commit_event("run-1", event_id.clone(), false);
        let (_, next_parent, _) = governance.harvest_event_context(&handle).unwrap();
        assert_eq!(next_parent, event_id);
    }

    #[test]
    fn harvest_checkpoint_keeps_host_model_and_logical_correlation() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        governance
            .harvest
            .start("run-1", "logical-op-1".into())
            .unwrap();
        governance
            .update_host_plan("run-1", "host-plan-1".into())
            .unwrap();
        governance
            .update_harvest_plan("run-1", "model-plan-1".into())
            .unwrap();

        let checkpoint = governance.harvest_checkpoint_state("run-1").unwrap();
        assert_eq!(checkpoint.operation_id, "host-plan-1");
        assert_eq!(checkpoint.logical_operation_id, "logical-op-1");
        assert_eq!(checkpoint.model_operation_id, "model-plan-1");
    }

    #[test]
    fn host_receipt_plan_has_no_model_budget_payload() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        let input = governance.host_receipt_input("run-1", "ship the change", "logical-1");

        assert_eq!(input.estimated_tokens, 0);
        assert_eq!(input.max_tokens, 0);
        assert!(input.messages.is_empty());
        assert!(input.tools.is_empty());
        assert!(input.system.is_empty());
    }

    #[test]
    fn report_skips_external_action() {
        assert!(!tool_authorization::requires_external_action("report"));
        assert!(!tool_authorization::requires_external_action("escalate"));
        assert!(!tool_authorization::requires_external_action("todo_write"));
        assert!(tool_authorization::requires_external_action("bash"));
        assert!(tool_authorization::requires_external_action("write_file"));
        assert!(tool_authorization::requires_external_action("edit"));
        assert!(tool_authorization::requires_external_action("read_file"));
    }

    #[test]
    fn risk_classes_match_tool_consequences() {
        assert_eq!(tool_authorization::risk_class("bash"), "destructive");
        assert_eq!(tool_authorization::risk_class("write_file"), "write");
        assert_eq!(tool_authorization::risk_class("edit"), "write");
        assert_eq!(tool_authorization::risk_class("read_file"), "read");
    }

    #[test]
    fn permit_allows_execution() {
        assert!(tool_authorization::interpret_decision(&decision("permit", "")).is_ok());
    }

    #[test]
    fn permit_decision_requires_signed_permit() {
        let err =
            tool_authorization::permit_for_decision(&decision("permit", ""), None).unwrap_err();
        assert!(matches!(err, GovernanceError::Message(message) if message.contains("permit")));
    }

    #[test]
    fn external_action_request_uses_v1_risk_and_project_binding() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        governance
            .harvest
            .start("run-1", "logical-op-1".into())
            .unwrap();
        governance
            .update_host_plan("run-1", "host-plan-1".into())
            .unwrap();
        governance
            .update_harvest_plan("run-1", "plane-plan-1".into())
            .unwrap();
        let handle = RunHandle {
            run_id: "run-1".into(),
            operation_id: "logical-op-1".into(),
            namespace: "default".into(),
        };
        let request = tool_authorization::build_request(
            &governance,
            &handle,
            "tool-1-0-provider-call",
            "write_file",
            "{}",
        )
        .unwrap();
        assert_eq!(request.intended_executor, "shikigami");
        assert_eq!(request.action_type, "shikigami.tool.write_file.write/v1");
        assert_eq!(
            request.target_selectors,
            vec!["project:default/tool:write_file"]
        );
        assert_eq!(request.operation_id, "host-plan-1");
        assert_eq!(
            request.request_id,
            tool_authorization::build_request(
                &governance,
                &handle,
                "tool-1-0-provider-call",
                "write_file",
                "{}"
            )
            .unwrap()
            .request_id
        );
    }

    #[test]
    fn deny_blocks_execution() {
        let err = tool_authorization::interpret_decision(&decision("deny", "budget exhausted"))
            .unwrap_err();
        match err {
            GovernanceError::Denied(msg) => {
                assert!(msg.contains("budget exhausted"), "{msg}");
            }
            other => panic!("expected Denied, got {other}"),
        }
    }

    #[test]
    fn require_approval_blocks_headless() {
        let err =
            tool_authorization::interpret_decision(&decision("require_approval", "needs human"))
                .unwrap_err();
        match err {
            GovernanceError::Denied(msg) => {
                assert!(msg.contains("approval"), "{msg}");
                assert!(msg.contains("needs human"), "{msg}");
            }
            other => panic!("expected Denied, got {other}"),
        }
    }

    #[test]
    fn unknown_decision_fails_closed() {
        let err = tool_authorization::interpret_decision(&decision("maybe", "")).unwrap_err();
        assert!(matches!(err, GovernanceError::Message(_)));
    }

    #[tokio::test]
    async fn authorize_transport_errors_deny_even_when_not_fail_closed() {
        // Empty endpoint → connect Unavailable. Mid-run authz must deny tools
        // even when the adapter was constructed with fail_closed=false.
        let governance = SekaiChiseiGovernance {
            endpoint: String::new(),
            principal: "test".into(),
            namespace: "default".into(),
            fail_closed: false,
            token_env: None,
            max_tokens: 4096,
            preferred_model: "auto".into(),
            harvest: HarvestTransaction::default(),
            fallback_enabled: false,
            fallback_allow_test_signatures: false,
            delayed: std::sync::Mutex::new(DelayedEvidenceRuntime {
                enabled: false,
                limits: crate::evidence_queue::QueueLimits::default(),
                root: None,
            }),
        };
        let handle = RunHandle {
            run_id: "run-authz-fail".into(),
            operation_id: "op-authz-fail".into(),
            namespace: "default".into(),
        };
        let err = tool_authorization::authorize(
            &governance,
            &handle,
            "tool-1-0-provider-call",
            "bash",
            r#"{"command":"true"}"#,
        )
        .await
        .expect_err("transport failure must deny, not Ok(())");
        assert!(
            matches!(err, GovernanceError::Unavailable(_)),
            "expected Unavailable, got {err}"
        );
    }

    #[test]
    fn arguments_digest_is_stable() {
        let a = SekaiChiseiGovernance::arguments_digest(r#"{"path":"a.txt"}"#);
        let b = SekaiChiseiGovernance::arguments_digest(r#"{"path":"a.txt"}"#);
        let c = SekaiChiseiGovernance::arguments_digest(r#"{"path":"b.txt"}"#);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn harvest_complete_attributes_include_termination() {
        let outcome = RunOutcome {
            success: false,
            summary: "timed out".into(),
            turns: 3,
            termination: "timed_out".into(),
            workspace: "/tmp/ws".into(),
        };
        let attrs = harvest::complete_attributes(&outcome);
        assert_eq!(attrs.get("status").map(String::as_str), Some("failed"));
        assert_eq!(
            attrs.get("completion_reason").map(String::as_str),
            Some("timed_out")
        );
        assert_eq!(attrs.get("success").map(String::as_str), Some("false"));
        assert_eq!(attrs.get("turns").map(String::as_str), Some("3"));
        assert_eq!(
            attrs.get("termination").map(String::as_str),
            Some("timed_out")
        );
        assert_eq!(
            attrs.get("authoritative").map(String::as_str),
            Some("plane")
        );
        let handle = RunHandle {
            run_id: "r1".into(),
            operation_id: "r1".into(),
            namespace: "ns".into(),
        };
        let refs = harvest::complete_references(&handle, &outcome);
        assert!(
            refs.iter()
                .any(|r| r.kind == "run_id" && r.reference == "r1")
        );
        assert!(
            refs.iter()
                .any(|r| r.kind == "workspace_path" && r.reference == "/tmp/ws")
        );
    }

    #[test]
    fn harvest_begin_attributes_capture_task() {
        let attrs = harvest::begin_attributes("run-1", "op-9", "do the thing", "alice");
        assert_eq!(attrs.get("run_id").map(String::as_str), Some("run-1"));
        assert_eq!(attrs.get("attempt_id").map(String::as_str), Some("run-1"));
        assert_eq!(
            attrs.get("logical_operation_id").map(String::as_str),
            Some("op-9")
        );
        assert_eq!(attrs.get("task").map(String::as_str), Some("do the thing"));
        assert_eq!(attrs.get("principal").map(String::as_str), Some("alice"));
    }

    #[tokio::test]
    async fn lease_rpcs_reuse_one_cached_connect() {
        let cache = CachedClient::new();
        let connects = std::sync::atomic::AtomicU32::new(0);
        let connect = || async {
            connects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, &'static str>("channel")
        };
        for _ in 0..3 {
            let guard = cache.get_or_try_insert_with(&connect).await.unwrap();
            assert_eq!(guard.as_deref(), Some("channel"));
        }
        assert_eq!(connects.load(std::sync::atomic::Ordering::SeqCst), 1);
        {
            let mut guard = cache.get_or_try_insert_with(&connect).await.unwrap();
            *guard = None;
        }
        let guard = cache.get_or_try_insert_with(&connect).await.unwrap();
        assert_eq!(guard.as_deref(), Some("channel"));
        assert_eq!(connects.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn harvest_uses_v1_receipt_event_kinds() {
        assert_eq!(harvest::KIND_BEGIN, "intent_recorded");
        assert_eq!(harvest::KIND_ATTEMPT, "attempt_started");
        assert_eq!(harvest::KIND_MODEL, "model_called");
        assert_eq!(harvest::KIND_TOOL, "action_performed");
        assert_eq!(harvest::KIND_COMPLETE, "outcome_recorded");
        assert_eq!(
            harvest::model_attributes("model-plan", true)
                .get("ok")
                .map(String::as_str),
            Some("true")
        );
    }

    fn fallback_config() -> Config {
        let mut config = Config::default();
        config.governance.adapter = "sekai-chisei".into();
        config.governance.endpoint = Some("http://127.0.0.1:1".into());
        config.governance.fail_closed = true;
        config.model.adapter = "plane".into();
        config.model.fallback.enabled = true;
        config.model.fallback.allow_test_signatures = true;
        config.model.fallback.adapter = Some("scripted".into());
        config.model.fallback.script_json = Some(r#"[{"content":"fallback-ok"}]"#.into());
        config
    }

    fn matching_grant(
        model: &dyn crate::model::ModelPort,
        handle: &RunHandle,
        system: &str,
        tools: &[ToolDef],
    ) -> (
        crate::fallback::FallbackAuthorization,
        crate::fallback::LiveFence,
    ) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(0))
            .unwrap_or(0);
        let fence = crate::fallback::LiveFence {
            effect_id: "effect-1".into(),
            owner: "runtime-1".into(),
            fencing_token: "fence-1".into(),
            generation: 1,
            valid_until_ms: now + 3_600_000,
        };
        let allowed = crate::fallback::tool_names(tools);
        let mut auth = crate::fallback::FallbackAuthorization {
            schema_version: crate::fallback::FALLBACK_SCHEMA_VERSION,
            authorization_id: "auth-1".into(),
            issuer: "governance".into(),
            signer_ref: "signer-1".into(),
            run_id: handle.run_id.clone(),
            operation_id: handle.operation_id.clone(),
            attempt_id: handle.run_id.clone(),
            claim_id: Some("claim-1".into()),
            lease: crate::fallback::FallbackLease {
                effect_id: fence.effect_id.clone(),
                owner: fence.owner.clone(),
                fencing_token: fence.fencing_token.clone(),
                generation: fence.generation,
                valid_until_ms: fence.valid_until_ms,
            },
            policy_revision: "policy-1".into(),
            model_content_digest: model.content_digest(),
            prompt_context_digest: crate::fallback::prompt_context_digest(system, &[]),
            tool_surface_digest: crate::fallback::tool_surface_digest(tools),
            allowed_tools: allowed,
            valid_from_ms: now - 1,
            valid_until_ms: fence.valid_until_ms,
            revoked: false,
            reduced_guarantees: true,
            signature_algorithm: String::new(),
            binding_digest: String::new(),
            signature: String::new(),
        };
        auth.seal_test_hmac().unwrap();
        (auth, fence)
    }

    #[tokio::test]
    async fn fallback_plan_turn_uses_local_model_when_plane_unavailable() {
        let config = fallback_config();
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        let model = crate::model::from_config(&config).unwrap();
        let handle = RunHandle {
            run_id: "run-fb".into(),
            operation_id: "op-fb".into(),
            namespace: "default".into(),
        };
        let tools = vec![ToolDef {
            name: "report".into(),
            description: "done".into(),
            schema: "{}".into(),
        }];
        let (auth, fence) = matching_grant(model.as_ref(), &handle, "system", &tools);
        governance
            .install_fallback_grant(&handle.run_id, auth, fence)
            .unwrap();
        let turn = governance
            .plan_turn(&handle, "system", &[], &tools, model.as_ref())
            .await
            .unwrap();
        assert_eq!(turn.content, "fallback-ok");
        let checkpoint = governance.harvest_checkpoint_state(&handle.run_id).unwrap();
        let fallback = checkpoint.fallback.expect("fallback scratch");
        assert_eq!(
            fallback.selection.model_content_digest,
            model.content_digest()
        );
        assert!(fallback.selection.degraded_guarantees);
        assert!(!fallback.evidence.is_empty());
        assert_eq!(
            fallback.reconciliation,
            crate::fallback::FallbackReconciliation::Pending
        );
    }

    #[tokio::test]
    async fn fallback_denial_does_not_invoke_exhausted_script() {
        let mut config = fallback_config();
        config.model.fallback.script_json = Some(r#"[]"#.into());
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        let model = crate::model::from_config(&config).unwrap();
        let handle = RunHandle {
            run_id: "run-deny".into(),
            operation_id: "op-deny".into(),
            namespace: "default".into(),
        };
        let err = governance
            .plan_turn(&handle, "system", &[], &[], model.as_ref())
            .await
            .unwrap_err();
        match err {
            GovernanceError::Denied(message) => {
                assert!(message.contains("fallback:"), "{message}");
            }
            other => panic!("expected fallback denial, got {other}"),
        }
    }

    #[tokio::test]
    async fn stored_grant_does_not_bypass_connected_tool_authorization() {
        let config = fallback_config();
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        let model = crate::model::from_config(&config).unwrap();
        let handle = RunHandle {
            run_id: "run-online".into(),
            operation_id: "op-online".into(),
            namespace: "default".into(),
        };
        let tools = vec![ToolDef {
            name: "read_file".into(),
            description: "read".into(),
            schema: "{}".into(),
        }];
        let (auth, fence) = matching_grant(model.as_ref(), &handle, "system", &tools);
        governance
            .install_fallback_grant(&handle.run_id, auth, fence)
            .unwrap();
        let err = governance
            .authorize_tool(&handle, "bash", r#"{"command":"true"}"#)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GovernanceError::Unavailable(_)),
            "expected plane authz, got {err}"
        );
    }

    #[tokio::test]
    async fn fallback_tool_authz_uses_bound_surface() {
        let config = fallback_config();
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        let model = crate::model::from_config(&config).unwrap();
        let handle = RunHandle {
            run_id: "run-tool".into(),
            operation_id: "op-tool".into(),
            namespace: "default".into(),
        };
        let tools = vec![ToolDef {
            name: "read_file".into(),
            description: "read".into(),
            schema: "{}".into(),
        }];
        let (auth, fence) = matching_grant(model.as_ref(), &handle, "system", &tools);
        governance
            .install_fallback_grant(&handle.run_id, auth, fence)
            .unwrap();
        let turn = governance
            .plan_turn(&handle, "system", &[], &tools, model.as_ref())
            .await
            .unwrap();
        assert_eq!(turn.content, "fallback-ok");
        governance
            .authorize_tool(&handle, "read_file", "{}")
            .await
            .unwrap();
        let err = governance
            .authorize_tool(&handle, "bash", r#"{"command":"true"}"#)
            .await
            .unwrap_err();
        match err {
            GovernanceError::Denied(message) => {
                assert!(message.contains("mismatched_tool_surface"), "{message}");
            }
            other => panic!("expected tool-surface denial, got {other}"),
        }
    }

    #[test]
    fn delayed_envelope_survives_bind_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.governance.adapter = "sekai-chisei".into();
        config.governance.endpoint = Some("http://127.0.0.1:1".into());
        config.governance.delayed_evidence.enabled = true;
        config.governance.delayed_evidence.allow_test_signatures = true;
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        governance.bind_state_root(dir.path());
        let mut envelope = crate::evidence_queue::DelayedEnvelope {
            schema_version: crate::evidence_queue::EVIDENCE_QUEUE_SCHEMA_VERSION,
            kind: crate::evidence_queue::EvidenceKind::Terminal,
            identity: crate::evidence_queue::EvidenceIdentity {
                run_id: "run-q".into(),
                operation_id: "op-q".into(),
                attempt_id: "run-q".into(),
                claim_id: None,
                event_id: "complete".into(),
                payload_digest: crate::evidence_queue::sha256_hex(b"done"),
            },
            issuer: "governance".into(),
            signer_ref: "signer-1".into(),
            policy_revision: "policy-1".into(),
            lease_valid_until_ms: 4_000_000_000_000,
            fencing_token: "fence-1".into(),
            generation: 1,
            parent_event_id: "op-q:budget".into(),
            redacted_attributes: vec![("termination".into(), "completed".into())],
            report_attributes: vec![("termination".into(), "completed".into())],
            signature_algorithm: String::new(),
            binding_digest: String::new(),
            signature: String::new(),
        };
        envelope.seal_test_hmac().unwrap();
        governance
            .persist_delayed_envelope(envelope, 1_700_000_000_000, "fence-1", 1)
            .unwrap();
        let snap = governance.delayed_evidence_snapshot().unwrap();
        assert_eq!(snap.depth, 1);
        assert!(snap.failure_class.is_empty());
        let store =
            crate::evidence_queue::load(crate::evidence_queue::path_in(dir.path())).unwrap();
        assert_eq!(store.entries.len(), 1);
        assert_eq!(
            store.entries[0].state,
            crate::evidence_queue::EvidenceState::Pending
        );
    }

    fn delayed_complete_config(enabled: bool) -> Config {
        let mut config = fallback_config();
        config.governance.endpoint = Some(String::new());
        config.governance.delayed_evidence.enabled = enabled;
        config
    }

    fn complete_handle() -> RunHandle {
        RunHandle {
            run_id: "run-complete".into(),
            operation_id: "op-complete".into(),
            namespace: "default".into(),
        }
    }

    fn complete_outcome() -> RunOutcome {
        RunOutcome {
            success: true,
            summary: "done".into(),
            turns: 1,
            termination: "completed".into(),
            workspace: String::new(),
        }
    }

    #[tokio::test]
    async fn complete_run_unavailable_queues_then_reloads_and_reconciles() {
        let dir = tempfile::tempdir().unwrap();
        let config = delayed_complete_config(true);
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        governance.bind_state_root(dir.path());
        let model = crate::model::from_config(&config).unwrap();
        let handle = complete_handle();
        let (auth, fence) = matching_grant(model.as_ref(), &handle, "system", &[]);
        let live_token = fence.fencing_token.clone();
        let live_generation = fence.generation;
        let policy = auth.policy_revision.clone();
        governance
            .install_fallback_grant(&handle.run_id, auth, fence)
            .unwrap();
        governance
            .update_host_plan(&handle.run_id, "host-receipt-op".into())
            .unwrap();
        governance
            .harvest
            .stage_event(
                &handle.run_id,
                crate::checkpoint::PendingGovernanceEvent {
                    operation_id: "host-receipt-op".into(),
                    event_id: "report:host-receipt-op:complete".into(),
                    parent_event_id: "report:host-receipt-op:attempt".into(),
                    timestamp_ms: 1,
                    kind: harvest::KIND_COMPLETE.into(),
                    attributes: [
                        ("termination".into(), "completed".into()),
                        ("success".into(), "true".into()),
                    ]
                    .into_iter()
                    .collect(),
                    references: vec![],
                },
            )
            .unwrap();

        let err = GovernancePort::complete_run(&governance, &handle, complete_outcome())
            .await
            .unwrap_err();
        match err {
            GovernanceError::Unavailable(message) => {
                assert!(message.contains("delayed evidence queued"), "{message}");
            }
            other => panic!("expected queued Unavailable, got {other}"),
        }

        let path = crate::evidence_queue::path_in(dir.path());
        let stored = crate::evidence_queue::load(&path).unwrap();
        assert_eq!(stored.entries.len(), 1);
        assert_eq!(stored.entries[0].envelope.fencing_token, live_token);
        assert_eq!(stored.entries[0].envelope.generation, live_generation);
        assert_eq!(stored.entries[0].envelope.policy_revision, policy);
        assert_eq!(
            stored.entries[0].envelope.identity.operation_id,
            "host-receipt-op"
        );
        assert_eq!(
            stored.entries[0].envelope.identity.event_id,
            "report:host-receipt-op:complete"
        );
        assert_eq!(
            stored.entries[0].envelope.parent_event_id,
            "report:host-receipt-op:attempt"
        );
        assert_ne!(stored.entries[0].envelope.fencing_token, "queued");
        assert_ne!(
            stored.entries[0].envelope.policy_revision,
            "local-integrity"
        );
        assert_eq!(
            stored.entries[0].state,
            crate::evidence_queue::EvidenceState::Pending
        );

        let reloaded = SekaiChiseiGovernance::from_config(&config).unwrap();
        reloaded.bind_state_root(dir.path());
        assert!(
            reloaded
                .harvest_checkpoint_state(&handle.run_id)
                .and_then(|checkpoint| checkpoint.fallback)
                .is_none(),
            "reload must not restore harvest; persist reconnect_grant only"
        );
        let snap = reloaded.delayed_evidence_snapshot().unwrap();
        assert_eq!(snap.depth, 1);
        assert!(
            crate::evidence_queue::load(&path).unwrap().entries[0]
                .reconnect_grant
                .is_some()
        );
        let after = reloaded
            .reconcile_delayed_evidence()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.depth, 1);
        let retried = crate::evidence_queue::load(&path).unwrap();
        assert_eq!(
            retried.entries[0].state,
            crate::evidence_queue::EvidenceState::Retryable
        );
        assert!(
            retried.entries[0].attempts >= 1,
            "reload must submit using persisted reconnect_grant"
        );
        assert_ne!(
            retried.entries[0].state,
            crate::evidence_queue::EvidenceState::Accepted
        );
        assert_eq!(
            retried.entries[0].envelope.identity.key(),
            stored.entries[0].envelope.identity.key()
        );
        assert_eq!(retried.entries[0].envelope.fencing_token, live_token);
        assert_eq!(retried.entries[0].envelope.generation, live_generation);
        assert_eq!(retried.entries[0].envelope.policy_revision, policy);

        let again = GovernancePort::complete_run(&governance, &handle, complete_outcome())
            .await
            .unwrap_err();
        match again {
            GovernanceError::Unavailable(message) => {
                assert!(message.contains("delayed evidence queued"), "{message}");
            }
            other => panic!("expected queued Unavailable on retry, got {other}"),
        }
        let still = crate::evidence_queue::load(&path).unwrap();
        assert_ne!(
            still.entries[0].state,
            crate::evidence_queue::EvidenceState::Accepted
        );
    }

    #[test]
    fn delayed_evidence_disposition_comes_from_authority_response() {
        let envelope = crate::evidence_queue::DelayedEnvelope {
            schema_version: crate::evidence_queue::EVIDENCE_QUEUE_SCHEMA_VERSION,
            kind: crate::evidence_queue::EvidenceKind::Terminal,
            identity: crate::evidence_queue::EvidenceIdentity {
                run_id: "run-complete".into(),
                operation_id: "op-complete".into(),
                attempt_id: "run-complete".into(),
                claim_id: None,
                event_id: "complete:run-complete".into(),
                payload_digest: crate::evidence_queue::sha256_hex(b"done"),
            },
            issuer: "governance".into(),
            signer_ref: "signer-1".into(),
            policy_revision: "policy-1".into(),
            lease_valid_until_ms: 4_000_000_000_000,
            fencing_token: "fence-1".into(),
            generation: 1,
            parent_event_id: "op-complete:budget".into(),
            redacted_attributes: vec![],
            report_attributes: vec![],
            signature_algorithm: String::new(),
            binding_digest: String::new(),
            signature: String::new(),
        };
        let recorded = proto::chisei::ReportOperationEventResponse {
            event_id: envelope.identity.event_id.clone(),
            recorded: true,
            complete: true,
            missing_surfaces: vec![],
        };
        assert_eq!(
            disposition_from_authority(&envelope, Ok(&recorded)),
            crate::evidence_queue::AuthorityDisposition::Accept
        );
        let idempotent = proto::chisei::ReportOperationEventResponse {
            event_id: envelope.identity.event_id.clone(),
            recorded: false,
            complete: true,
            missing_surfaces: vec![],
        };
        assert_eq!(
            disposition_from_authority(&envelope, Ok(&idempotent)),
            crate::evidence_queue::AuthorityDisposition::Accept
        );
        let incomplete = proto::chisei::ReportOperationEventResponse {
            event_id: envelope.identity.event_id.clone(),
            recorded: true,
            complete: false,
            missing_surfaces: vec!["attempt".into()],
        };
        assert_eq!(
            disposition_from_authority(&envelope, Ok(&incomplete)),
            crate::evidence_queue::AuthorityDisposition::Unavailable
        );
        let conflicted = proto::chisei::ReportOperationEventResponse {
            event_id: "other-event".into(),
            recorded: false,
            complete: false,
            missing_surfaces: vec![],
        };
        assert_eq!(
            disposition_from_authority(&envelope, Ok(&conflicted)),
            crate::evidence_queue::AuthorityDisposition::Conflict
        );
        let unavailable = GovernanceError::Unavailable("plane down".into());
        assert_eq!(
            disposition_from_authority(&envelope, Err(&unavailable)),
            crate::evidence_queue::AuthorityDisposition::Unavailable
        );
        let denied = GovernanceError::Denied("rejected".into());
        assert_eq!(
            disposition_from_authority(&envelope, Err(&denied)),
            crate::evidence_queue::AuthorityDisposition::Reject
        );
    }

    #[tokio::test]
    async fn complete_run_unavailable_preserves_when_queue_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let config = delayed_complete_config(false);
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        governance.bind_state_root(dir.path());
        let handle = complete_handle();
        governance
            .update_host_plan(&handle.run_id, handle.operation_id.clone())
            .unwrap();

        let err = GovernancePort::complete_run(&governance, &handle, complete_outcome())
            .await
            .unwrap_err();
        match err {
            GovernanceError::Unavailable(message) => {
                assert!(!message.contains("delayed evidence queued"), "{message}");
                assert!(!message.contains("delayed_evidence:disabled"), "{message}");
            }
            other => panic!("expected preserved Unavailable, got {other}"),
        }
        assert!(!crate::evidence_queue::path_in(dir.path()).is_file());
    }

    #[tokio::test]
    async fn complete_run_unavailable_does_not_mint_fence_without_grant() {
        let dir = tempfile::tempdir().unwrap();
        let config = delayed_complete_config(true);
        let governance = SekaiChiseiGovernance::from_config(&config).unwrap();
        governance.bind_state_root(dir.path());
        let handle = complete_handle();
        governance
            .update_host_plan(&handle.run_id, handle.operation_id.clone())
            .unwrap();

        let err = GovernancePort::complete_run(&governance, &handle, complete_outcome())
            .await
            .unwrap_err();
        match err {
            GovernanceError::Unavailable(message) => {
                assert!(!message.contains("delayed evidence queued"), "{message}");
            }
            other => panic!("expected preserved Unavailable, got {other}"),
        }
        assert!(!crate::evidence_queue::path_in(dir.path()).is_file());
    }
}
