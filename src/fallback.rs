//! Fail-closed admission for one authorized local-model fallback interval.
//!
//! Governance issues the grant. Shikigami only verifies and executes. Delivery
//! placing bytes is not a grant. Local scratch is never a receipt.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::governance::GovernanceError;
use crate::tools::ToolDef;

pub const FALLBACK_SCHEMA_VERSION: u32 = 1;
pub const TEST_HMAC_SHA256: &str = "test-hmac-sha256";
const TEST_HMAC_DOMAIN: &str = "shikigami-fallback-v1";

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("sha256:{}", hex_lower(Sha256::digest(bytes).as_slice()))
}

/// Bind the authorized task, not the evolving tool transcript.
/// Subsequent assistant/tool messages remain governed by the lease, fence,
/// and tool-surface checks for the same interval.
pub fn prompt_context_digest(system: &str, messages: &[crate::model::ChatMessage]) -> String {
    let task = messages
        .iter()
        .find(|message| message.role == "user")
        .map(|message| message.content.replace("\r\n", "\n"))
        .unwrap_or_default();
    let payload = serde_json::json!({
        "system": system.replace("\r\n", "\n"),
        "task": task,
    });
    sha256_hex(&serde_json::to_vec(&payload).unwrap_or_default())
}

pub fn tool_surface_digest(tools: &[ToolDef]) -> String {
    let mut rows: Vec<(String, String)> = tools
        .iter()
        .map(|tool| (tool.name.clone(), tool.schema.clone()))
        .collect();
    rows.sort();
    rows.dedup();
    let payload = rows
        .into_iter()
        .map(|(name, schema)| format!("{name}\n{schema}"))
        .collect::<Vec<_>>()
        .join("\n");
    sha256_hex(payload.as_bytes())
}

pub fn tool_names(tools: &[ToolDef]) -> Vec<String> {
    tools.iter().map(|tool| tool.name.clone()).collect()
}

/// Governance-issued grant for one disconnected interval.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FallbackAuthorization {
    pub schema_version: u32,
    pub authorization_id: String,
    pub issuer: String,
    pub signer_ref: String,
    pub run_id: String,
    pub operation_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
    pub lease: FallbackLease,
    pub policy_revision: String,
    pub model_content_digest: String,
    pub prompt_context_digest: String,
    pub tool_surface_digest: String,
    pub allowed_tools: Vec<String>,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    #[serde(default)]
    pub revoked: bool,
    #[serde(default)]
    pub reduced_guarantees: bool,
    pub signature_algorithm: String,
    pub binding_digest: String,
    pub signature: String,
}

/// Lease identity copied from the issuing authority (not minted here).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FallbackLease {
    pub effect_id: String,
    pub owner: String,
    pub fencing_token: String,
    pub generation: u64,
    pub valid_until_ms: i64,
}

/// Currently held claim fence. Must match the authorization lease.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveFence {
    pub effect_id: String,
    pub owner: String,
    pub fencing_token: String,
    pub generation: u64,
    pub valid_until_ms: i64,
}

/// Harness scratch after admission. Not a receipt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FallbackSelection {
    pub authorization_id: String,
    pub model_content_digest: String,
    pub degraded_guarantees: bool,
    pub selected_at_ms: i64,
}

/// Stable identities used for idempotent reconnect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FallbackEvidenceIdentity {
    pub run_id: String,
    pub operation_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
    pub authorization_id: String,
    pub model_event_id: String,
    pub payload_digest: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum FallbackReconciliation {
    #[default]
    Pending,
    Retryable,
    Accepted,
    Rejected,
    Conflicted,
    Expired,
}

/// Durable fallback projection stored on the governance checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FallbackCheckpoint {
    pub authorization: FallbackAuthorization,
    pub selection: FallbackSelection,
    pub held_fence: LiveFence,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<FallbackEvidenceIdentity>,
    pub reconciliation: FallbackReconciliation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelSource {
    Plane,
    Local,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FallbackDenial {
    #[error("fallback_disabled")]
    Disabled,
    #[error("missing_authorization")]
    MissingAuthorization,
    #[error("unverifiable")]
    Unverifiable,
    #[error("revoked")]
    Revoked,
    #[error("stale")]
    Stale,
    #[error("expired")]
    Expired,
    #[error("mismatched_identity")]
    MismatchedIdentity,
    #[error("fence_lost")]
    FenceLost,
    #[error("mismatched_model")]
    MismatchedModel,
    #[error("mismatched_prompt")]
    MismatchedPrompt,
    #[error("mismatched_tool_surface")]
    MismatchedToolSurface,
    #[error("conflicted")]
    Conflicted,
}

impl From<FallbackDenial> for GovernanceError {
    fn from(denial: FallbackDenial) -> Self {
        GovernanceError::Denied(format!("fallback:{denial}"))
    }
}

/// Observed executor state used for fail-closed admission.
#[derive(Debug, Clone)]
pub struct FallbackView {
    pub now_ms: i64,
    pub fallback_enabled: bool,
    pub run_id: String,
    pub operation_id: String,
    pub attempt_id: String,
    pub claim_id: Option<String>,
    pub local_model_digest: String,
    pub prompt_context_digest: String,
    pub tool_surface_digest: String,
    pub tool_names: Vec<String>,
    pub live_fence: Option<LiveFence>,
    /// When false, `test-hmac-sha256` is unverifiable (fixture-only).
    pub allow_test_signatures: bool,
}

impl FallbackAuthorization {
    fn binding_payload(&self) -> Result<Vec<u8>, FallbackDenial> {
        let mut lease = BTreeMap::new();
        lease.insert("effect_id", self.lease.effect_id.clone());
        lease.insert("owner", self.lease.owner.clone());
        lease.insert("fencing_token", self.lease.fencing_token.clone());
        lease.insert("generation", self.lease.generation.to_string());
        lease.insert("valid_until_ms", self.lease.valid_until_ms.to_string());
        let mut fields = BTreeMap::new();
        fields.insert(
            "schema_version",
            serde_json::Value::from(self.schema_version),
        );
        fields.insert(
            "authorization_id",
            serde_json::Value::from(self.authorization_id.clone()),
        );
        fields.insert("issuer", serde_json::Value::from(self.issuer.clone()));
        fields.insert(
            "signer_ref",
            serde_json::Value::from(self.signer_ref.clone()),
        );
        fields.insert("run_id", serde_json::Value::from(self.run_id.clone()));
        fields.insert(
            "operation_id",
            serde_json::Value::from(self.operation_id.clone()),
        );
        fields.insert(
            "attempt_id",
            serde_json::Value::from(self.attempt_id.clone()),
        );
        fields.insert(
            "claim_id",
            match &self.claim_id {
                Some(id) => serde_json::Value::from(id.clone()),
                None => serde_json::Value::Null,
            },
        );
        fields.insert("lease", serde_json::to_value(lease).expect("lease map"));
        fields.insert(
            "policy_revision",
            serde_json::Value::from(self.policy_revision.clone()),
        );
        fields.insert(
            "model_content_digest",
            serde_json::Value::from(self.model_content_digest.clone()),
        );
        fields.insert(
            "prompt_context_digest",
            serde_json::Value::from(self.prompt_context_digest.clone()),
        );
        fields.insert(
            "tool_surface_digest",
            serde_json::Value::from(self.tool_surface_digest.clone()),
        );
        fields.insert(
            "allowed_tools",
            serde_json::Value::from(self.allowed_tools.clone()),
        );
        fields.insert("valid_from_ms", serde_json::Value::from(self.valid_from_ms));
        fields.insert(
            "valid_until_ms",
            serde_json::Value::from(self.valid_until_ms),
        );
        fields.insert("revoked", serde_json::Value::from(self.revoked));
        fields.insert(
            "reduced_guarantees",
            serde_json::Value::from(self.reduced_guarantees),
        );
        serde_json::to_vec(&fields).map_err(|_| FallbackDenial::Unverifiable)
    }

    pub fn compute_binding_digest(&self) -> Result<String, FallbackDenial> {
        Ok(sha256_hex(&self.binding_payload()?))
    }

    fn test_hmac_signature(&self) -> Result<String, FallbackDenial> {
        let digest = self.compute_binding_digest()?;
        Ok(sha256_hex(
            format!("{digest}|{}|{TEST_HMAC_DOMAIN}", self.signer_ref).as_bytes(),
        ))
    }

    /// Seal a fixture envelope with the supported test MAC. Not a production issuer.
    #[cfg(test)]
    pub(crate) fn seal_test_hmac(&mut self) -> Result<(), FallbackDenial> {
        self.signature_algorithm = TEST_HMAC_SHA256.into();
        self.binding_digest = self.compute_binding_digest()?;
        self.signature = self.test_hmac_signature()?;
        Ok(())
    }

    fn required_ids_present(&self) -> bool {
        self.schema_version == FALLBACK_SCHEMA_VERSION
            && !self.authorization_id.is_empty()
            && !self.issuer.is_empty()
            && !self.signer_ref.is_empty()
            && !self.run_id.is_empty()
            && !self.operation_id.is_empty()
            && !self.attempt_id.is_empty()
            && !self.policy_revision.is_empty()
            && !self.model_content_digest.is_empty()
            && !self.prompt_context_digest.is_empty()
            && !self.tool_surface_digest.is_empty()
            && !self.lease.effect_id.is_empty()
            && !self.lease.owner.is_empty()
            && !self.lease.fencing_token.is_empty()
            && self.lease.generation > 0
    }

    fn verify_signature(&self, allow_test: bool) -> Result<(), FallbackDenial> {
        if !self.required_ids_present()
            || self.binding_digest.is_empty()
            || self.signature.is_empty()
        {
            return Err(FallbackDenial::Unverifiable);
        }
        let expected_digest = self.compute_binding_digest()?;
        if expected_digest != self.binding_digest {
            return Err(FallbackDenial::Unverifiable);
        }
        if self.signature_algorithm != TEST_HMAC_SHA256 || !allow_test {
            return Err(FallbackDenial::Unverifiable);
        }
        if self.signature != self.test_hmac_signature()? {
            return Err(FallbackDenial::Unverifiable);
        }
        Ok(())
    }
}

fn fence_matches(lease: &FallbackLease, fence: &LiveFence) -> bool {
    lease.effect_id == fence.effect_id
        && lease.owner == fence.owner
        && lease.fencing_token == fence.fencing_token
        && lease.generation == fence.generation
}

/// Choose plane planning when reachable; otherwise admit local fallback.
pub fn select_model_source(
    plane_reachable: bool,
    authorization: Option<&FallbackAuthorization>,
    view: &FallbackView,
) -> Result<(ModelSource, Option<FallbackSelection>), FallbackDenial> {
    if plane_reachable {
        return Ok((ModelSource::Plane, None));
    }
    let selection = admit(authorization, view)?;
    Ok((ModelSource::Local, Some(selection)))
}

/// Fail-closed local-model admission. Does not invoke a model.
pub fn admit(
    authorization: Option<&FallbackAuthorization>,
    view: &FallbackView,
) -> Result<FallbackSelection, FallbackDenial> {
    if !view.fallback_enabled {
        return Err(FallbackDenial::Disabled);
    }
    let auth = authorization.ok_or(FallbackDenial::MissingAuthorization)?;
    auth.verify_signature(view.allow_test_signatures)?;
    if auth.revoked {
        return Err(FallbackDenial::Revoked);
    }
    if view.now_ms < auth.valid_from_ms {
        return Err(FallbackDenial::Stale);
    }
    if view.now_ms >= auth.valid_until_ms || view.now_ms >= auth.lease.valid_until_ms {
        return Err(FallbackDenial::Expired);
    }
    if auth.run_id != view.run_id
        || auth.operation_id != view.operation_id
        || auth.attempt_id != view.attempt_id
        || auth.claim_id != view.claim_id
    {
        return Err(FallbackDenial::MismatchedIdentity);
    }
    let fence = view.live_fence.as_ref().ok_or(FallbackDenial::FenceLost)?;
    if fence.effect_id.is_empty()
        || fence.owner.is_empty()
        || fence.fencing_token.is_empty()
        || fence.generation == 0
    {
        return Err(FallbackDenial::FenceLost);
    }
    if !fence_matches(&auth.lease, fence) {
        return Err(FallbackDenial::FenceLost);
    }
    if view.now_ms >= fence.valid_until_ms || fence.generation < auth.lease.generation {
        return Err(FallbackDenial::Stale);
    }
    if view.local_model_digest != auth.model_content_digest {
        return Err(FallbackDenial::MismatchedModel);
    }
    if view.prompt_context_digest != auth.prompt_context_digest {
        return Err(FallbackDenial::MismatchedPrompt);
    }
    if view.tool_surface_digest != auth.tool_surface_digest {
        return Err(FallbackDenial::MismatchedToolSurface);
    }
    let mut allowed = auth.allowed_tools.clone();
    let mut advertised = view.tool_names.clone();
    allowed.sort();
    advertised.sort();
    if allowed != advertised {
        return Err(FallbackDenial::MismatchedToolSurface);
    }
    Ok(FallbackSelection {
        authorization_id: auth.authorization_id.clone(),
        model_content_digest: auth.model_content_digest.clone(),
        degraded_guarantees: true,
        selected_at_ms: view.now_ms,
    })
}

/// Re-validate a checkpointed grant. Changed or expired evidence blocks resume.
pub fn resume(
    checkpoint: &FallbackCheckpoint,
    view: &FallbackView,
) -> Result<FallbackSelection, FallbackDenial> {
    let mut view = view.clone();
    if view.live_fence.is_none() {
        view.live_fence = Some(checkpoint.held_fence.clone());
    }
    let selection = admit(Some(&checkpoint.authorization), &view)?;
    if selection.authorization_id != checkpoint.selection.authorization_id
        || selection.model_content_digest != checkpoint.selection.model_content_digest
    {
        return Err(FallbackDenial::Stale);
    }
    Ok(selection)
}

pub fn still_valid(
    checkpoint: &FallbackCheckpoint,
    view: &FallbackView,
) -> Result<(), FallbackDenial> {
    resume(checkpoint, view).map(|_| ())
}

pub fn tool_permitted(
    checkpoint: &FallbackCheckpoint,
    view: &FallbackView,
    name: &str,
) -> Result<(), FallbackDenial> {
    still_valid(checkpoint, view)?;
    if checkpoint
        .authorization
        .allowed_tools
        .iter()
        .any(|allowed| allowed == name)
    {
        Ok(())
    } else {
        Err(FallbackDenial::MismatchedToolSurface)
    }
}

fn same_identity_keys(left: &FallbackEvidenceIdentity, right: &FallbackEvidenceIdentity) -> bool {
    left.run_id == right.run_id
        && left.operation_id == right.operation_id
        && left.attempt_id == right.attempt_id
        && left.claim_id == right.claim_id
        && left.authorization_id == right.authorization_id
        && left.model_event_id == right.model_event_id
}

/// Idempotent reconnect. Shikigami never invents `accepted`.
pub fn reconcile(
    prior: Option<&FallbackCheckpoint>,
    incoming: &FallbackEvidenceIdentity,
) -> FallbackReconciliation {
    let Some(prior) = prior else {
        return FallbackReconciliation::Pending;
    };
    let Some(prior_evidence) = prior
        .evidence
        .iter()
        .find(|evidence| same_identity_keys(evidence, incoming))
    else {
        return FallbackReconciliation::Pending;
    };
    if prior_evidence.payload_digest != incoming.payload_digest {
        return FallbackReconciliation::Conflicted;
    }
    match prior.reconciliation {
        FallbackReconciliation::Accepted
        | FallbackReconciliation::Rejected
        | FallbackReconciliation::Conflicted
        | FallbackReconciliation::Expired => prior.reconciliation,
        FallbackReconciliation::Pending | FallbackReconciliation::Retryable => {
            FallbackReconciliation::Retryable
        }
    }
}

/// Apply an external authority disposition. Never used to mint success locally.
pub fn apply_authority_disposition(
    current: FallbackReconciliation,
    accepted: Option<bool>,
) -> FallbackReconciliation {
    match accepted {
        Some(true) => FallbackReconciliation::Accepted,
        Some(false) => FallbackReconciliation::Rejected,
        None => current,
    }
}

pub fn evidence_identity(
    auth: &FallbackAuthorization,
    model_event_id: &str,
    payload: &str,
) -> FallbackEvidenceIdentity {
    FallbackEvidenceIdentity {
        run_id: auth.run_id.clone(),
        operation_id: auth.operation_id.clone(),
        attempt_id: auth.attempt_id.clone(),
        claim_id: auth.claim_id.clone(),
        authorization_id: auth.authorization_id.clone(),
        model_event_id: model_event_id.into(),
        payload_digest: sha256_hex(payload.as_bytes()),
    }
}

pub fn view_from_checkpoint(
    checkpoint: &FallbackCheckpoint,
    now_ms: i64,
    enabled: bool,
    live_fence: Option<LiveFence>,
    allow_test_signatures: bool,
) -> FallbackView {
    FallbackView {
        now_ms,
        fallback_enabled: enabled,
        run_id: checkpoint.authorization.run_id.clone(),
        operation_id: checkpoint.authorization.operation_id.clone(),
        attempt_id: checkpoint.authorization.attempt_id.clone(),
        claim_id: checkpoint.authorization.claim_id.clone(),
        local_model_digest: checkpoint.authorization.model_content_digest.clone(),
        prompt_context_digest: checkpoint.authorization.prompt_context_digest.clone(),
        tool_surface_digest: checkpoint.authorization.tool_surface_digest.clone(),
        tool_names: checkpoint.authorization.allowed_tools.clone(),
        live_fence: live_fence.or_else(|| Some(checkpoint.held_fence.clone())),
        allow_test_signatures,
    }
}

pub fn checkpoint_after_admit(
    authorization: FallbackAuthorization,
    selection: FallbackSelection,
    held_fence: LiveFence,
) -> FallbackCheckpoint {
    FallbackCheckpoint {
        authorization,
        selection,
        held_fence,
        evidence: Vec::new(),
        reconciliation: FallbackReconciliation::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChatMessage, ModelError, ModelPort, ModelTurn};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const NOW: i64 = 1_000_000;

    struct CountingModel {
        digest: String,
        calls: AtomicUsize,
        turn: ModelTurn,
    }

    #[async_trait]
    impl ModelPort for CountingModel {
        fn id(&self) -> &'static str {
            "scripted"
        }
        fn content_digest(&self) -> String {
            self.digest.clone()
        }
        async fn next_turn(
            &self,
            _system: &str,
            _messages: &[ChatMessage],
            _tools: &[ToolDef],
        ) -> Result<ModelTurn, ModelError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.turn.clone())
        }
    }

    fn fence() -> LiveFence {
        LiveFence {
            effect_id: "effect-1".into(),
            owner: "runtime-1".into(),
            fencing_token: "fence-1".into(),
            generation: 3,
            valid_until_ms: NOW + 60_000,
        }
    }

    fn defs(names: &[&str]) -> Vec<ToolDef> {
        names
            .iter()
            .map(|name| ToolDef {
                name: (*name).into(),
                description: String::new(),
                schema: "{}".into(),
            })
            .collect()
    }

    fn view() -> FallbackView {
        let tools = defs(&["read_file", "report"]);
        FallbackView {
            now_ms: NOW,
            fallback_enabled: true,
            run_id: "run-1".into(),
            operation_id: "op-1".into(),
            attempt_id: "run-1".into(),
            claim_id: Some("claim-1".into()),
            local_model_digest: "sha256:model".into(),
            prompt_context_digest: prompt_context_digest("system", &[]),
            tool_surface_digest: tool_surface_digest(&tools),
            tool_names: tools.iter().map(|tool| tool.name.clone()).collect(),
            live_fence: Some(fence()),
            allow_test_signatures: true,
        }
    }

    fn authorization_for(view: &FallbackView) -> FallbackAuthorization {
        let mut auth = FallbackAuthorization {
            schema_version: FALLBACK_SCHEMA_VERSION,
            authorization_id: "auth-1".into(),
            issuer: "governance".into(),
            signer_ref: "signer-1".into(),
            run_id: view.run_id.clone(),
            operation_id: view.operation_id.clone(),
            attempt_id: view.attempt_id.clone(),
            claim_id: view.claim_id.clone(),
            lease: FallbackLease {
                effect_id: "effect-1".into(),
                owner: "runtime-1".into(),
                fencing_token: "fence-1".into(),
                generation: 3,
                valid_until_ms: NOW + 60_000,
            },
            policy_revision: "policy-1".into(),
            model_content_digest: view.local_model_digest.clone(),
            prompt_context_digest: view.prompt_context_digest.clone(),
            tool_surface_digest: view.tool_surface_digest.clone(),
            allowed_tools: vec!["read_file".into(), "report".into()],
            valid_from_ms: NOW - 1,
            valid_until_ms: NOW + 60_000,
            revoked: false,
            reduced_guarantees: true,
            signature_algorithm: String::new(),
            binding_digest: String::new(),
            signature: String::new(),
        };
        auth.seal_test_hmac().unwrap();
        auth
    }

    async fn invoke_if_admitted(
        auth: Option<&FallbackAuthorization>,
        view: &FallbackView,
        model: &CountingModel,
    ) -> Result<ModelTurn, FallbackDenial> {
        let (source, selection) = select_model_source(false, auth, view)?;
        assert_eq!(source, ModelSource::Local);
        assert!(selection.is_some());
        model
            .next_turn("system", &[], &[])
            .await
            .map_err(|_| FallbackDenial::Unverifiable)
    }

    #[test]
    fn connected_path_ignores_fallback() {
        let view = view();
        let auth = authorization_for(&view);
        let (source, selection) =
            select_model_source(true, Some(&auth), &view).expect("connected path");
        assert_eq!(source, ModelSource::Plane);
        assert!(selection.is_none());
    }

    #[tokio::test]
    async fn matching_authorization_selects_local_digest() {
        let view = view();
        let auth = authorization_for(&view);
        let model = CountingModel {
            digest: view.local_model_digest.clone(),
            calls: AtomicUsize::new(0),
            turn: ModelTurn {
                content: "fallback-turn".into(),
                tool_calls: vec![],
                usage: None,
            },
        };
        let turn = invoke_if_admitted(Some(&auth), &view, &model)
            .await
            .unwrap();
        assert_eq!(turn.content, "fallback-turn");
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        let selection = admit(Some(&auth), &view).unwrap();
        assert_eq!(selection.model_content_digest, view.local_model_digest);
        assert!(selection.degraded_guarantees);
    }

    #[derive(Clone, Copy, Debug)]
    enum DenialCase {
        Disabled,
        Missing,
        Revoked,
        StaleWindow,
        Expired,
        MismatchedRun,
        FenceLost,
        MismatchedModel,
        MismatchedPrompt,
        MismatchedTools,
        UnverifiableSig,
        UnknownAlg,
    }

    fn apply_denial_case(
        case: DenialCase,
        auth: &mut FallbackAuthorization,
        view: &mut FallbackView,
    ) -> Option<()> {
        match case {
            DenialCase::Disabled => view.fallback_enabled = false,
            DenialCase::Missing => return None,
            DenialCase::Revoked => {
                auth.revoked = true;
                auth.seal_test_hmac().unwrap();
            }
            DenialCase::StaleWindow => view.now_ms = 0,
            DenialCase::Expired => view.now_ms = NOW + 120_000,
            DenialCase::MismatchedRun => view.run_id = "other".into(),
            DenialCase::FenceLost => view.live_fence = None,
            DenialCase::MismatchedModel => view.local_model_digest = "sha256:other".into(),
            DenialCase::MismatchedPrompt => {
                view.prompt_context_digest = prompt_context_digest("other", &[]);
            }
            DenialCase::MismatchedTools => {
                let tools = defs(&["bash"]);
                view.tool_surface_digest = tool_surface_digest(&tools);
                view.tool_names = vec!["bash".into()];
            }
            DenialCase::UnverifiableSig => auth.signature = "deadbeef".into(),
            DenialCase::UnknownAlg => auth.signature_algorithm = "ed25519".into(),
        }
        Some(())
    }

    fn expected_denial(case: DenialCase) -> FallbackDenial {
        match case {
            DenialCase::Disabled => FallbackDenial::Disabled,
            DenialCase::Missing => FallbackDenial::MissingAuthorization,
            DenialCase::Revoked => FallbackDenial::Revoked,
            DenialCase::StaleWindow => FallbackDenial::Stale,
            DenialCase::Expired => FallbackDenial::Expired,
            DenialCase::MismatchedRun => FallbackDenial::MismatchedIdentity,
            DenialCase::FenceLost => FallbackDenial::FenceLost,
            DenialCase::MismatchedModel => FallbackDenial::MismatchedModel,
            DenialCase::MismatchedPrompt => FallbackDenial::MismatchedPrompt,
            DenialCase::MismatchedTools => FallbackDenial::MismatchedToolSurface,
            DenialCase::UnverifiableSig | DenialCase::UnknownAlg => FallbackDenial::Unverifiable,
        }
    }

    #[tokio::test]
    async fn denial_does_not_invoke_model() {
        let cases = [
            DenialCase::Disabled,
            DenialCase::Missing,
            DenialCase::Revoked,
            DenialCase::StaleWindow,
            DenialCase::Expired,
            DenialCase::MismatchedRun,
            DenialCase::FenceLost,
            DenialCase::MismatchedModel,
            DenialCase::MismatchedPrompt,
            DenialCase::MismatchedTools,
            DenialCase::UnverifiableSig,
            DenialCase::UnknownAlg,
        ];
        for case in cases {
            let mut view = view();
            let mut auth = authorization_for(&view);
            let present = apply_denial_case(case, &mut auth, &mut view);
            let model = CountingModel {
                digest: view.local_model_digest.clone(),
                calls: AtomicUsize::new(0),
                turn: ModelTurn {
                    content: "should-not-run".into(),
                    tool_calls: vec![],
                    usage: None,
                },
            };
            let auth_ref = present.and(Some(&auth));
            let err = invoke_if_admitted(auth_ref, &view, &model)
                .await
                .expect_err("denial class must fail closed");
            assert_eq!(
                model.calls.load(Ordering::SeqCst),
                0,
                "{case:?} invoked model"
            );
            assert_eq!(err, expected_denial(case), "{case:?}");
        }
    }

    #[test]
    fn fence_mismatch_and_expiry_during_execution() {
        let view = view();
        let auth = authorization_for(&view);
        let selection = admit(Some(&auth), &view).unwrap();
        let mut checkpoint = checkpoint_after_admit(auth, selection, fence());
        let mut later = view.clone();
        later.now_ms = NOW + 120_000;
        assert_eq!(
            still_valid(&checkpoint, &later).unwrap_err(),
            FallbackDenial::Expired
        );
        later = view.clone();
        later.live_fence = Some(LiveFence {
            effect_id: "other".into(),
            owner: "runtime-1".into(),
            fencing_token: "fence-1".into(),
            generation: 3,
            valid_until_ms: NOW + 60_000,
        });
        assert_eq!(
            still_valid(&checkpoint, &later).unwrap_err(),
            FallbackDenial::FenceLost
        );
        checkpoint.authorization.revoked = true;
        checkpoint.authorization.seal_test_hmac().unwrap();
        assert_eq!(
            still_valid(&checkpoint, &view).unwrap_err(),
            FallbackDenial::Revoked
        );
    }

    #[test]
    fn resume_restores_bound_decision_and_blocks_changed_evidence() {
        let view = view();
        let auth = authorization_for(&view);
        let selection = admit(Some(&auth), &view).unwrap();
        let checkpoint = checkpoint_after_admit(auth.clone(), selection.clone(), fence());
        let mut resumed = view.clone();
        resumed.live_fence = None;
        let restored = resume(&checkpoint, &resumed).unwrap();
        assert_eq!(restored.authorization_id, selection.authorization_id);
        let mut changed = view.clone();
        changed.local_model_digest = "sha256:changed".into();
        assert_eq!(
            resume(&checkpoint, &changed).unwrap_err(),
            FallbackDenial::MismatchedModel
        );
    }

    #[test]
    fn reconnect_is_idempotent_and_conflicts_stay_unresolved() {
        let view = view();
        let auth = authorization_for(&view);
        let selection = admit(Some(&auth), &view).unwrap();
        let mut checkpoint = checkpoint_after_admit(auth.clone(), selection, fence());
        let first = evidence_identity(&auth, "model-event-1", "payload-a");
        assert_eq!(
            reconcile(Some(&checkpoint), &first),
            FallbackReconciliation::Pending
        );
        checkpoint.evidence = vec![first.clone()];
        let retry = evidence_identity(&auth, "model-event-1", "payload-a");
        checkpoint.reconciliation = reconcile(Some(&checkpoint), &retry);
        assert_eq!(checkpoint.reconciliation, FallbackReconciliation::Retryable);
        let conflict = evidence_identity(&auth, "model-event-1", "payload-b");
        let state = reconcile(Some(&checkpoint), &conflict);
        assert_eq!(state, FallbackReconciliation::Conflicted);
        checkpoint.reconciliation = state;
        assert_eq!(
            apply_authority_disposition(checkpoint.reconciliation, Some(false)),
            FallbackReconciliation::Rejected
        );
        assert_ne!(
            apply_authority_disposition(FallbackReconciliation::Retryable, None),
            FallbackReconciliation::Accepted
        );
    }

    #[test]
    fn tool_outside_surface_is_denied() {
        let view = view();
        let auth = authorization_for(&view);
        let selection = admit(Some(&auth), &view).unwrap();
        let checkpoint = checkpoint_after_admit(auth, selection, fence());
        tool_permitted(&checkpoint, &view, "read_file").unwrap();
        assert_eq!(
            tool_permitted(&checkpoint, &view, "bash").unwrap_err(),
            FallbackDenial::MismatchedToolSurface
        );
    }

    #[test]
    fn fixture_signatures_fail_closed_without_opt_in() {
        let mut view = view();
        view.allow_test_signatures = false;
        let auth = authorization_for(&view);
        assert_eq!(
            admit(Some(&auth), &view).unwrap_err(),
            FallbackDenial::Unverifiable
        );
    }

    #[test]
    fn prompt_digest_includes_message_history() {
        let empty = prompt_context_digest("system", &[]);
        let with_user = prompt_context_digest(
            "system",
            &[ChatMessage {
                role: "user".into(),
                content: "task".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            }],
        );
        let later = prompt_context_digest(
            "system",
            &[
                ChatMessage {
                    role: "user".into(),
                    content: "task".into(),
                    tool_call_id: String::new(),
                    tool_calls: vec![],
                },
                ChatMessage {
                    role: "assistant".into(),
                    content: "working".into(),
                    tool_call_id: String::new(),
                    tool_calls: vec![],
                },
            ],
        );
        assert_ne!(empty, with_user);
        assert_eq!(with_user, later);
    }

    #[test]
    fn unknown_authorization_fields_fail_closed() {
        let err = serde_json::from_str::<FallbackAuthorization>(
            r#"{"schema_version":1,"authorization_id":"a","issuer":"i","signer_ref":"s","run_id":"r","operation_id":"o","attempt_id":"r","lease":{"effect_id":"e","owner":"o","fencing_token":"f","generation":1,"valid_until_ms":1},"policy_revision":"p","model_content_digest":"m","prompt_context_digest":"p","tool_surface_digest":"t","allowed_tools":[],"valid_from_ms":0,"valid_until_ms":1,"signature_algorithm":"x","binding_digest":"b","signature":"s","extra":true}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }
}
