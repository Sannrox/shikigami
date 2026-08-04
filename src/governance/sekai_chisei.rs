//! First-party sekai-chisei governance adapter.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::checkpoint::{
    GovernanceCheckpoint, GovernanceEvidenceReference, PendingGovernanceEvent, StagedToolExecution,
    StagedToolReport, ToolExecutionStatus,
};
use crate::config::Config;
use crate::model::{ChatMessage, ModelTurn, ToolCall};
use crate::tools::ToolDef;

use super::{AvailableModel, GovernanceError, GovernancePort, RunHandle, RunOutcome};

pub mod proto {
    pub mod chisei {
        tonic::include_proto!("chisei");
    }
    pub mod sekai {
        tonic::include_proto!("sekai");
    }
}

use sha2::{Digest, Sha256};

use proto::chisei::chisei_service_client::ChiseiServiceClient;
use proto::chisei::{
    AuthorizeExternalActionRequest, ChatMessage as ProtoChatMessage, ExecutePlanRequest,
    ExecutionInput, ExternalActionDecision, ExternalActionRequest,
    GetEffectivePolicySummaryRequest, GetOperationReceiptRequest, PlanExecutionRequest,
    RedeemExternalActionPermitRequest, ReportOperationEventRequest, ToolCall as ProtoToolCall,
    ToolDef as ProtoToolDef,
};
use proto::sekai::ListSchemaTypesRequest;
use proto::sekai::sekai_service_client::SekaiServiceClient;
use proto::sekai::{
    AckActionWorkRequest, ClaimActionWorkRequest, GetActionInstanceRequest,
    HeartbeatActionClaimRequest, ListClaimableActionWorkRequest, ReportActionClaimEventRequest,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const RPC_TIMEOUT: Duration = Duration::from_secs(120);

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

#[derive(Clone)]
struct AuthInterceptor {
    principal: MetadataValue<Ascii>,
    token: Option<MetadataValue<Ascii>>,
}

#[derive(Default)]
struct HarvestState {
    host_operation_id: Option<String>,
    logical_operation_id: Option<String>,
    model_operation_id: Option<String>,
    model_reported: bool,
    last_event_id: Option<String>,
    pending_event: Option<PendingGovernanceEvent>,
    pending_tool_reports: Vec<StagedToolReport>,
    pending_tool_executions: Vec<StagedToolExecution>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("x-principal", self.principal.clone());
        req.metadata_mut().insert(
            "x-sekai-auth-source",
            if self.token.is_some() {
                MetadataValue::from_static("token")
            } else {
                MetadataValue::from_static("local")
            },
        );
        if let Some(token) = &self.token {
            req.metadata_mut().insert("authorization", token.clone());
        }
        Ok(req)
    }
}

type Chisei = ChiseiServiceClient<InterceptedService<Channel, AuthInterceptor>>;
type Sekai = SekaiServiceClient<InterceptedService<Channel, AuthInterceptor>>;

pub struct SekaiChiseiGovernance {
    endpoint: String,
    principal: String,
    namespace: String,
    fail_closed: bool,
    token_env: Option<String>,
    max_tokens: i32,
    preferred_model: String,
    harvest: Arc<Mutex<HashMap<String, HarvestState>>>,
}

/// Runtime-claim client used by the explicit plane intake mode of `serve`.
///
/// This client only claims and acknowledges already-admitted work. Governance
/// planning, tool authorization, and harvest remain on [`SekaiChiseiGovernance`].
pub struct SekaiClaimClient {
    inner: SekaiChiseiGovernance,
    namespace: String,
}

impl SekaiClaimClient {
    pub fn from_config(config: &Config) -> Result<Self, GovernanceError> {
        Ok(Self {
            inner: SekaiChiseiGovernance::from_config(config)?,
            namespace: config.governance.namespace.clone(),
        })
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
            harvest: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn update_host_plan(&self, run_id: &str, operation_id: String) -> Result<(), GovernanceError> {
        let mut harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.entry(run_id.into()).or_default();
        state.host_operation_id = Some(operation_id);
        state.last_event_id = None;
        Ok(())
    }

    fn update_harvest_plan(
        &self,
        run_id: &str,
        operation_id: String,
    ) -> Result<(), GovernanceError> {
        let mut harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.entry(run_id.into()).or_default();
        state.model_operation_id = Some(operation_id);
        state.model_reported = false;
        Ok(())
    }

    fn restore_harvest_state(
        &self,
        run_id: &str,
        checkpoint: &GovernanceCheckpoint,
    ) -> Result<(), GovernanceError> {
        self.harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .insert(
                run_id.into(),
                HarvestState {
                    host_operation_id: (!checkpoint.operation_id.is_empty())
                        .then(|| checkpoint.operation_id.clone()),
                    logical_operation_id: (!checkpoint.logical_operation_id.is_empty())
                        .then(|| checkpoint.logical_operation_id.clone()),
                    model_operation_id: (!checkpoint.model_operation_id.is_empty())
                        .then(|| checkpoint.model_operation_id.clone()),
                    model_reported: checkpoint.model_reported,
                    last_event_id: (!checkpoint.last_event_id.is_empty())
                        .then(|| checkpoint.last_event_id.clone()),
                    pending_event: checkpoint.pending_event.clone(),
                    pending_tool_reports: checkpoint.pending_tool_reports.clone(),
                    pending_tool_executions: checkpoint.pending_tool_executions.clone(),
                },
            );
        Ok(())
    }

    fn harvest_checkpoint_state(&self, run_id: &str) -> Option<GovernanceCheckpoint> {
        let harvest = self.harvest.lock().ok()?;
        let state = harvest.get(run_id)?;
        let has_state = state.host_operation_id.is_some()
            || state.logical_operation_id.is_some()
            || state.model_operation_id.is_some()
            || state.model_reported
            || state.last_event_id.is_some()
            || state.pending_event.is_some()
            || !state.pending_tool_reports.is_empty()
            || !state.pending_tool_executions.is_empty();
        if !has_state {
            return None;
        }
        Some(GovernanceCheckpoint {
            operation_id: state.host_operation_id.clone().unwrap_or_default(),
            logical_operation_id: state.logical_operation_id.clone().unwrap_or_default(),
            model_operation_id: state.model_operation_id.clone().unwrap_or_default(),
            model_reported: state.model_reported,
            last_event_id: state.last_event_id.clone().unwrap_or_default(),
            pending_event: state.pending_event.clone(),
            pending_tool_reports: state.pending_tool_reports.clone(),
            pending_tool_executions: state.pending_tool_executions.clone(),
        })
    }

    #[cfg(test)]
    fn harvest_event_context(
        &self,
        handle: &RunHandle,
    ) -> Result<(String, String, String), GovernanceError> {
        self.harvest_event_context_with_id(handle, None)
    }

    fn harvest_event_context_with_id(
        &self,
        handle: &RunHandle,
        requested_event_id: Option<String>,
    ) -> Result<(String, String, String), GovernanceError> {
        let harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.get(&handle.run_id).ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: run has no harvest state".into(),
            )
        })?;
        let operation_id = state.host_operation_id.clone().ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: host PlanExecution did not establish a receipt".into(),
            )
        })?;
        let parent_event_id = state
            .last_event_id
            .clone()
            .unwrap_or_else(|| format!("{operation_id}:budget"));
        let event_id = requested_event_id
            .unwrap_or_else(|| format!("report:{operation_id}:{}", uuid::Uuid::new_v4()));
        Ok((operation_id, parent_event_id, event_id))
    }

    fn remember_harvest_event(&self, run_id: &str, event_id: String) {
        if let Ok(mut harvest) = self.harvest.lock()
            && let Some(state) = harvest.get_mut(run_id)
        {
            state.last_event_id = Some(event_id);
        }
    }

    fn set_pending_harvest_event(
        &self,
        run_id: &str,
        pending: PendingGovernanceEvent,
    ) -> Result<(), GovernanceError> {
        let mut harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: run has no harvest state".into(),
            )
        })?;
        if state.pending_event.is_some() {
            return Err(GovernanceError::Message(
                "operation-event reporting has an unacknowledged event pending retry".into(),
            ));
        }
        state.pending_event = Some(pending);
        Ok(())
    }

    fn clear_pending_harvest_event(&self, run_id: &str) {
        if let Ok(mut harvest) = self.harvest.lock()
            && let Some(state) = harvest.get_mut(run_id)
        {
            state.pending_event = None;
        }
    }

    fn forget_harvest(&self, run_id: &str) {
        if let Ok(mut harvest) = self.harvest.lock() {
            harvest.remove(run_id);
        }
    }

    fn stage_tool_reports_for_run(
        &self,
        run_id: &str,
        reports: Vec<StagedToolReport>,
    ) -> Result<(), GovernanceError> {
        let mut harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "operation-event reporting unavailable: run has no harvest state".into(),
            )
        })?;
        state.pending_tool_reports.extend(reports);
        Ok(())
    }

    fn remove_staged_tool_report(&self, run_id: &str, call_id: &str) {
        if let Ok(mut harvest) = self.harvest.lock()
            && let Some(state) = harvest.get_mut(run_id)
            && let Some(index) = state
                .pending_tool_reports
                .iter()
                .position(|report| report.call_id == call_id)
        {
            state.pending_tool_reports.remove(index);
        }
    }

    fn stage_tool_execution_for_run(
        &self,
        run_id: &str,
        execution: StagedToolExecution,
    ) -> Result<(), GovernanceError> {
        let mut harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "tool execution staging unavailable: run has no harvest state".into(),
            )
        })?;
        state.pending_tool_executions.push(execution);
        Ok(())
    }

    fn mark_tool_execution_complete_for_run(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        let mut harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "tool execution staging unavailable: run has no harvest state".into(),
            )
        })?;
        if let Some(execution) = state
            .pending_tool_executions
            .iter_mut()
            .find(|execution| execution.call_id == call_id)
            .filter(|execution| matches!(execution.status, ToolExecutionStatus::Started))
        {
            execution.status = ToolExecutionStatus::Completed;
        }
        Ok(())
    }

    fn mark_tool_execution_started_for_run(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<(), GovernanceError> {
        let mut harvest = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?;
        let state = harvest.get_mut(run_id).ok_or_else(|| {
            GovernanceError::Message(
                "tool execution staging unavailable: run has no harvest state".into(),
            )
        })?;
        if let Some(execution) = state
            .pending_tool_executions
            .iter_mut()
            .find(|execution| execution.call_id == call_id)
            .filter(|execution| matches!(execution.status, ToolExecutionStatus::Authorizing))
        {
            execution.status = ToolExecutionStatus::Started;
        }
        Ok(())
    }

    fn clear_staged_tool_executions_for_run(&self, run_id: &str) {
        if let Ok(mut harvest) = self.harvest.lock()
            && let Some(state) = harvest.get_mut(run_id)
        {
            state.pending_tool_executions.clear();
        }
    }

    fn recover_staged_tool_executions_for_run(&self, run_id: &str) -> Result<(), GovernanceError> {
        let pending = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(run_id)
            .map(|state| state.pending_tool_executions.clone())
            .unwrap_or_default();
        if pending.is_empty() {
            return Ok(());
        }
        if pending
            .iter()
            .all(|execution| matches!(execution.status, ToolExecutionStatus::Authorizing))
        {
            self.clear_staged_tool_executions_for_run(run_id);
            return Ok(());
        }
        let details = pending
            .iter()
            .map(|execution| format!("{} ({:?})", execution.name, execution.status))
            .collect::<Vec<_>>()
            .join(", ");
        Err(GovernanceError::Message(format!(
            "tool execution state is in-doubt; inspect host effects before resuming: {details}"
        )))
    }

    fn host_harvest_operation_id(&self, handle: &RunHandle) -> Result<String, GovernanceError> {
        self.harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(&handle.run_id)
            .and_then(|state| state.host_operation_id.clone())
            .ok_or_else(|| {
                GovernanceError::Message(
                    "operation receipt unavailable: host PlanExecution did not establish a receipt"
                        .into(),
                )
            })
    }

    fn pending_event_references(
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

    fn proto_event_references(
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

    async fn send_pending_harvest_event(
        &self,
        pending: &PendingGovernanceEvent,
    ) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
        let (mut chisei, _) = self.connect().await?;
        chisei
            .report_operation_event(ReportOperationEventRequest {
                operation_id: pending.operation_id.clone(),
                event_id: pending.event_id.clone(),
                parent_event_id: pending.parent_event_id.clone(),
                timestamp_ms: pending.timestamp_ms,
                kind: pending.kind.clone(),
                attributes: pending.attributes.clone().into_iter().collect(),
                references: Self::proto_event_references(&pending.references),
            })
            .await
            .map_err(|error| match error.code() {
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                    GovernanceError::Message(format!(
                        "operation-event reporting authorization failed: {error}"
                    ))
                }
                _ => GovernanceError::Message(format!("ReportOperationEvent: {error}")),
            })
            .map(tonic::Response::into_inner)
    }

    async fn retry_pending_harvest_event(&self, handle: &RunHandle) -> Result<(), GovernanceError> {
        let pending = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(&handle.run_id)
            .and_then(|state| state.pending_event.clone());
        let Some(pending) = pending else {
            return Ok(());
        };
        let response = self.send_pending_harvest_event(&pending).await?;
        if !response.recorded && response.event_id != pending.event_id {
            return Err(GovernanceError::Message(format!(
                "ReportOperationEvent did not record pending event {}",
                pending.event_id
            )));
        }
        self.remember_harvest_event(&handle.run_id, pending.event_id);
        if pending.kind == harvest::KIND_MODEL
            && let Ok(mut harvest) = self.harvest.lock()
            && let Some(state) = harvest.get_mut(&handle.run_id)
        {
            state.model_reported = true;
        }
        self.clear_pending_harvest_event(&handle.run_id);
        Ok(())
    }

    async fn report_harvest_event_with_id(
        &self,
        handle: &RunHandle,
        kind: &str,
        attributes: HashMap<String, String>,
        references: Vec<proto::chisei::OperationEvidenceReference>,
        event_id: Option<String>,
    ) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
        let (operation_id, parent_event_id, event_id) =
            self.harvest_event_context_with_id(handle, event_id)?;
        let pending = PendingGovernanceEvent {
            operation_id,
            event_id,
            parent_event_id,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            kind: kind.into(),
            attributes: attributes.into_iter().collect::<BTreeMap<_, _>>(),
            references: Self::pending_event_references(&references),
        };
        self.set_pending_harvest_event(&handle.run_id, pending.clone())?;
        let response = self.send_pending_harvest_event(&pending).await?;
        if !response.recorded && response.event_id != pending.event_id {
            return Err(GovernanceError::Message(format!(
                "ReportOperationEvent did not record event {}",
                pending.event_id
            )));
        }
        self.remember_harvest_event(&handle.run_id, pending.event_id);
        self.clear_pending_harvest_event(&handle.run_id);
        Ok(response)
    }

    async fn report_harvest_event(
        &self,
        handle: &RunHandle,
        kind: &str,
        attributes: HashMap<String, String>,
        references: Vec<proto::chisei::OperationEvidenceReference>,
    ) -> Result<proto::chisei::ReportOperationEventResponse, GovernanceError> {
        self.report_harvest_event_with_id(handle, kind, attributes, references, None)
            .await
    }

    async fn report_model_event(
        &self,
        handle: &RunHandle,
        ok: bool,
    ) -> Result<(), GovernanceError> {
        let (model_operation_id, model_reported) = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(&handle.run_id)
            .map(|state| {
                (
                    state.model_operation_id.clone(),
                    state.model_reported,
                )
            })
            .ok_or_else(|| {
                GovernanceError::Message(
                    "model event reporting unavailable: model PlanExecution did not establish a receipt".into(),
                )
            })?;
        if model_reported {
            return Ok(());
        }
        let model_operation_id = model_operation_id.ok_or_else(|| {
            GovernanceError::Message(
                "model event reporting unavailable: model PlanExecution did not establish a receipt".into(),
            )
        })?;
        let host_operation_id = self.host_harvest_operation_id(handle)?;
        let event_id = format!(
            "report:{host_operation_id}:model:{}",
            Self::arguments_digest(&format!("{}:{}", handle.run_id, model_operation_id))
        );
        self.report_harvest_event_with_id(
            handle,
            harvest::KIND_MODEL,
            harvest::model_attributes(&model_operation_id, ok),
            vec![],
            Some(event_id),
        )
        .await?;
        if let Ok(mut harvest) = self.harvest.lock()
            && let Some(state) = harvest.get_mut(&handle.run_id)
        {
            state.model_reported = true;
        }
        Ok(())
    }

    async fn report_failed_model_event(&self, handle: &RunHandle) -> Result<(), GovernanceError> {
        match self.report_model_event(handle, false).await {
            Ok(()) => Ok(()),
            Err(error) if self.fail_closed => Err(error),
            Err(_) => Ok(()),
        }
    }

    async fn report_tool_event(
        &self,
        handle: &RunHandle,
        call_id: Option<&str>,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        let event_id = match call_id.filter(|call_id| !call_id.is_empty()) {
            Some(call_id) => {
                let operation_id = self.host_harvest_operation_id(handle)?;
                Some(format!(
                    "report:{operation_id}:tool:{}",
                    Self::arguments_digest(&format!("{}:{call_id}", handle.run_id))
                ))
            }
            None => None,
        };
        self.report_harvest_event_with_id(
            handle,
            harvest::KIND_TOOL,
            harvest::tool_attributes(name, ok, detail),
            vec![],
            event_id,
        )
        .await?;
        if let Some(call_id) = call_id {
            self.remove_staged_tool_report(&handle.run_id, call_id);
        }
        Ok(())
    }

    async fn harvest_receipt(
        &self,
        handle: &RunHandle,
    ) -> Result<proto::chisei::GetOperationReceiptResponse, GovernanceError> {
        let operation_id = self.host_harvest_operation_id(handle)?;
        let (mut chisei, _) = self.connect().await?;
        chisei
            .get_operation_receipt(GetOperationReceiptRequest {
                operation_id,
                request_id: String::new(),
                caller_scope: String::new(),
                attempt: 0,
            })
            .await
            .map_err(|error| match error.code() {
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                    GovernanceError::Message(format!(
                        "operation receipt inspection authorization failed: {error}"
                    ))
                }
                _ => GovernanceError::Message(format!("GetOperationReceipt: {error}")),
            })
            .map(tonic::Response::into_inner)
    }

    async fn connect(&self) -> Result<(Chisei, Sekai), GovernanceError> {
        if self.endpoint.trim().is_empty() {
            return Err(GovernanceError::Unavailable(
                "sekai-chisei endpoint not set (governance.endpoint or SHIKIGAMI_CONTROL_PLANE)"
                    .into(),
            ));
        }
        let channel = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| GovernanceError::Unavailable(e.to_string()))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect()
            .await
            .map_err(|e| GovernanceError::Unavailable(format!("connect {e}")))?;

        let principal = MetadataValue::try_from(self.principal.as_str())
            .map_err(|e| GovernanceError::Message(e.to_string()))?;
        let token = self
            .token_env
            .as_ref()
            .and_then(|k| std::env::var(k).ok())
            .filter(|t| !t.is_empty())
            .and_then(|t| {
                let value = if t.starts_with("Bearer ") {
                    t
                } else {
                    format!("Bearer {t}")
                };
                MetadataValue::try_from(value.as_str()).ok()
            });
        let interceptor = AuthInterceptor { principal, token };
        Ok((
            ChiseiServiceClient::with_interceptor(channel.clone(), interceptor.clone()),
            SekaiServiceClient::with_interceptor(channel, interceptor),
        ))
    }

    async fn probe(&self) -> Result<(), GovernanceError> {
        let (_chisei, mut sekai) = self.connect().await?;
        sekai
            .list_schema_types(ListSchemaTypesRequest {})
            .await
            .map_err(|e| GovernanceError::Unavailable(format!("probe: {e}")))?;
        Ok(())
    }

    /// Whether a tool must request plane external-action authorization before
    /// host execution. `report` / `escalate` / `todo_write` are harness-internal.
    pub(crate) fn tool_requires_external_action(name: &str) -> bool {
        !matches!(name, "report" | "escalate" | "todo_write")
    }

    /// Map a shikigami tool to the external-action risk class contract.
    pub(crate) fn tool_risk_class(name: &str) -> &'static str {
        match name {
            "bash" | "bash_background" | "bash_job_status" | "bash_job_logs" => "destructive",
            "write_file" | "edit" | "multi_edit" | "apply_patch" => "write",
            "read_file" | "glob" | "grep" | "web_fetch" => "read",
            _ => "write",
        }
    }

    fn arguments_digest(args_json: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(args_json.as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    /// Interpret an ExternalActionDecision for headless execution.
    /// Only `permit` allows the tool to proceed; the caller must still redeem
    /// the signed permit before executing the tool.
    pub(crate) fn interpret_external_action_decision(
        decision: &ExternalActionDecision,
    ) -> Result<(), GovernanceError> {
        match decision.decision.as_str() {
            "permit" => Ok(()),
            "deny" => Err(GovernanceError::Denied(format!(
                "external-action denied: {}",
                if decision.reason.is_empty() {
                    "policy denied"
                } else {
                    &decision.reason
                }
            ))),
            "require_approval" => Err(GovernanceError::Denied(format!(
                "external-action requires approval (unsupported headless): {}",
                if decision.reason.is_empty() {
                    "approval required"
                } else {
                    &decision.reason
                }
            ))),
            other => Err(GovernanceError::Message(format!(
                "external-action unexpected decision `{other}`{}",
                if decision.reason.is_empty() {
                    String::new()
                } else {
                    format!(": {}", decision.reason)
                }
            ))),
        }
    }

    fn permit_for_decision(
        decision: &ExternalActionDecision,
        permit: Option<proto::chisei::ExternalActionPermit>,
    ) -> Result<proto::chisei::ExternalActionPermit, GovernanceError> {
        Self::interpret_external_action_decision(decision)?;
        permit.ok_or_else(|| GovernanceError::Message("external-action permit missing".into()))
    }

    fn build_external_action_request(
        &self,
        handle: &RunHandle,
        call_id: &str,
        name: &str,
        args_json: &str,
    ) -> Result<ExternalActionRequest, GovernanceError> {
        let action_identity = Self::arguments_digest(&format!("{}:{call_id}", handle.run_id));
        let request_id = format!("shikigami-action:{action_identity}");
        let risk_class = Self::tool_risk_class(name);
        Ok(ExternalActionRequest {
            version: "external-action.request/v1".into(),
            operation_id: self.host_harvest_operation_id(handle)?,
            parent_operation_id: String::new(),
            attempt_id: handle.run_id.clone(),
            request_id: request_id.clone(),
            actor: self.principal.clone(),
            namespace: handle.namespace.clone(),
            requesting_harness: "shikigami".into(),
            // RedeemExternalActionPermit authenticates the bound executor
            // through x-principal. This adapter has one configured
            // authenticated identity, so it must bind actor and executor to
            // the same principal; `requesting_harness` remains the stable
            // shikigami host identity.
            intended_executor: self.principal.clone(),
            action_type: format!("shikigami.tool.{name}.{risk_class}/v1"),
            parameter_schema: "application/json".into(),
            canonical_arguments_digest: Self::arguments_digest(args_json),
            policy_summary: std::collections::HashMap::from([("tool".into(), name.to_string())]),
            target_selectors: vec![format!("project:{}/tool:{name}", handle.namespace)],
            immutable_preconditions: std::collections::HashMap::new(),
            risk_class: Self::tool_risk_class(name).into(),
            expected_effects: vec![format!("execute_tool:{name}")],
            requested_invocation_count: 1,
            deadline_ms: chrono::Utc::now().timestamp_millis() + 120_000,
            estimated_cost_micros: 0,
            estimated_volume: 0,
            affected_resource_count: 1,
            rollback_capability: String::new(),
            required_host_capabilities: vec!["shikigami.tools".into()],
            idempotency_key: request_id,
            policy_project: handle.namespace.clone(),
        })
    }

    fn host_receipt_input(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: &str,
    ) -> ExecutionInput {
        // Sekai/Chisei 1.0 exposes PlanExecution as the receipt-creation
        // surface. Keep this host-only plan non-model: the empty prompt/tool
        // payload and zero output bound make its budget admission zero, while
        // each actual model turn receives its own normal execution plan.
        ExecutionInput {
            request_id: format!("shikigami-host:{run_id}"),
            namespace: self.namespace.clone(),
            spec: task.into(),
            preferred_model: self.preferred_model.clone(),
            preferred_runtime: String::new(),
            task_type: "agent".into(),
            priority: 0,
            user_id: self.principal.clone(),
            estimated_tokens: 0,
            messages: vec![],
            tools: vec![],
            system: String::new(),
            max_tokens: 0,
            task_class: "shikigami-run".into(),
            logical_operation_id: logical_operation_id.into(),
            attempt_id: run_id.into(),
            route_override: String::new(),
        }
    }

    async fn create_host_receipt(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: &str,
    ) -> Result<String, GovernanceError> {
        // This plan allocates the host receipt's planning spine only. The
        // host plan is intentionally never sent to ExecutePlanStream; the
        // host lifecycle is filled by authenticated ReportOperationEvent
        // events below, while each model turn owns its own executed plan.
        let (mut chisei, _) = self.connect().await?;
        let plan = chisei
            .plan_execution(PlanExecutionRequest {
                input: Some(self.host_receipt_input(run_id, task, logical_operation_id)),
                gunshi_allocation: None,
            })
            .await
            .map_err(|error| {
                GovernanceError::Message(format!("PlanExecution host receipt: {error}"))
            })?
            .into_inner()
            .plan
            .ok_or_else(|| GovernanceError::Message("missing host receipt plan".into()))?;
        if plan.budget.as_ref().is_some_and(|budget| !budget.allowed) {
            return Err(GovernanceError::Denied(
                plan.budget
                    .as_ref()
                    .map(|budget| budget.reason.clone())
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or_else(|| "host receipt budget denied".into()),
            ));
        }
        if !plan.executable {
            return Err(GovernanceError::Denied(
                plan.warnings
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "host receipt plan not executable".into()),
            ));
        }
        Ok(plan.plan_id)
    }

    async fn abort_uncheckpointed_receipt(
        &self,
        handle: &RunHandle,
        reason: &str,
    ) -> Result<(), GovernanceError> {
        if self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(&handle.run_id)
            .and_then(|state| state.host_operation_id.as_ref())
            .is_none()
        {
            return Ok(());
        }

        self.retry_pending_harvest_event(handle).await?;
        let receipt = self.harvest_receipt(handle).await?;
        if receipt.complete {
            self.forget_harvest(&handle.run_id);
            return Ok(());
        }
        if receipt
            .missing_surfaces
            .iter()
            .any(|surface| surface == "attempt")
        {
            let attributes = harvest::attempt_attributes(&handle.run_id, &handle.operation_id);
            self.report_harvest_event(handle, harvest::KIND_ATTEMPT, attributes, vec![])
                .await?;
        }

        let outcome = RunOutcome {
            success: false,
            summary: reason.chars().take(4000).collect(),
            turns: 0,
            termination: "aborted_before_model".into(),
            workspace: String::new(),
        };
        let response = self
            .report_harvest_event(
                handle,
                harvest::KIND_COMPLETE,
                harvest::complete_attributes(&outcome),
                harvest::complete_references(handle, &outcome),
            )
            .await?;
        if !response.complete {
            return Err(GovernanceError::Message(format!(
                "uncheckpointed host receipt remains incomplete; missing surfaces: {}",
                response.missing_surfaces.join(", ")
            )));
        }
        self.forget_harvest(&handle.run_id);
        Ok(())
    }

    async fn begin_governed_run(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        checkpoint: Option<&GovernanceCheckpoint>,
    ) -> Result<RunHandle, GovernanceError> {
        if self.endpoint.trim().is_empty() {
            return Err(GovernanceError::Unavailable(
                "sekai-chisei endpoint not set".into(),
            ));
        }
        if let Err(error) = self.probe().await
            && self.fail_closed
        {
            return Err(error);
        }
        let checkpoint_logical_operation_id = checkpoint
            .filter(|state| !state.logical_operation_id.is_empty())
            .map(|state| state.logical_operation_id.as_str())
            .filter(|operation_id| !operation_id.is_empty());
        if let (Some(requested), Some(checkpoint_logical)) =
            (logical_operation_id, checkpoint_logical_operation_id)
            && requested != checkpoint_logical
        {
            return Err(GovernanceError::Message(format!(
                "resume logical operation id `{requested}` does not match checkpoint lineage `{checkpoint_logical}`"
            )));
        }
        let operation_id = logical_operation_id
            .or(checkpoint_logical_operation_id)
            .unwrap_or(run_id)
            .to_string();
        let handle = RunHandle {
            run_id: run_id.into(),
            operation_id,
            namespace: self.namespace.clone(),
        };

        if let Some(checkpoint) = checkpoint {
            self.restore_harvest_state(run_id, checkpoint)?;
            if let Ok(mut harvest) = self.harvest.lock()
                && let Some(state) = harvest.get_mut(run_id)
            {
                state.logical_operation_id = Some(handle.operation_id.clone());
            }
            if !checkpoint.operation_id.is_empty() {
                match self.harvest_receipt(&handle).await {
                    Ok(receipt) if receipt.complete => {
                        self.forget_harvest(run_id);
                        return Err(GovernanceError::Message(format!(
                            "resume checkpoint references completed host receipt {}; refusing to resume terminal run",
                            checkpoint.operation_id
                        )));
                    }
                    Ok(_) => {}
                    Err(error) if self.fail_closed => return Err(error),
                    Err(_) => {}
                }
                if let Err(error) = self.retry_pending_harvest_event(&handle).await
                    && self.fail_closed
                {
                    return Err(error);
                }
                match self.harvest_receipt(&handle).await {
                    Ok(receipt) if receipt.complete => {
                        self.forget_harvest(run_id);
                        return Err(GovernanceError::Message(format!(
                            "resume checkpoint became complete while replaying host receipt {}; refusing to resume terminal run",
                            checkpoint.operation_id
                        )));
                    }
                    Ok(_) => {}
                    Err(error) if self.fail_closed => return Err(error),
                    Err(_) => {}
                }
            }
        } else {
            self.harvest
                .lock()
                .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
                .insert(
                    run_id.into(),
                    HarvestState {
                        logical_operation_id: Some(handle.operation_id.clone()),
                        ..HarvestState::default()
                    },
                );
        }

        let has_host_receipt = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(run_id)
            .is_some_and(|state| state.host_operation_id.is_some());
        if !has_host_receipt {
            match self
                .create_host_receipt(run_id, task, &handle.operation_id)
                .await
            {
                Ok(operation_id) => self.update_host_plan(run_id, operation_id)?,
                Err(error) if self.fail_closed => return Err(error),
                Err(_) => {}
            }
        }

        let needs_attempt = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(run_id)
            .is_some_and(|state| {
                state.last_event_id.is_none() && state.host_operation_id.is_some()
            });
        if needs_attempt {
            let attributes = harvest::attempt_attributes(run_id, &handle.operation_id);
            if let Err(error) = self
                .report_harvest_event(&handle, harvest::KIND_ATTEMPT, attributes, vec![])
                .await
                && self.fail_closed
            {
                return Err(error);
            }
        }

        Ok(handle)
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
        let (_chisei, mut sekai) = self.inner.connect().await.map_err(plane_intake_source)?;
        let listed = sekai
            .list_claimable_action_work(ListClaimableActionWorkRequest {
                namespace: self.namespace.clone(),
                runtime_id: runtime_id.into(),
                limit: 1,
            })
            .await
            .map_err(|error| {
                crate::plane_intake::PlaneIntakeError::Source(format!(
                    "ListClaimableActionWork: {error}"
                ))
            })?
            .into_inner();
        let Some(candidate) = listed.effects.into_iter().next() else {
            return Ok(None);
        };
        let claimed = match sekai
            .claim_action_work(ClaimActionWorkRequest {
                effect_id: candidate.effect_id,
                runtime_id: runtime_id.into(),
                request_id: uuid::Uuid::new_v4().to_string(),
                ttl_ms: duration_millis(ttl)?,
            })
            .await
        {
            Ok(response) => response,
            Err(status) if is_claim_contention(&status) => return Ok(None),
            Err(error) => {
                return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                    "ClaimActionWork: {error}"
                )));
            }
        }
        .into_inner();
        let continuation = match (claimed.continuation, claimed.park) {
            (None, None) => None,
            (Some(continuation), Some(park))
                if continuation.park_id == park.park_id
                    && continuation.effect_id == park.effect_id
                    && continuation.operation_id == park.operation_id
                    && continuation.park_generation == park.park_generation =>
            {
                let checkpoint = if park.checkpoint_store_id.is_empty()
                    && park.checkpoint_ref.is_empty()
                    && park.checkpoint_digest.is_empty()
                {
                    None
                } else {
                    Some(crate::plane_intake::PlaneCheckpoint {
                        store_id: park.checkpoint_store_id,
                        reference: park.checkpoint_ref,
                        digest: park.checkpoint_digest,
                    })
                };
                Some(crate::plane_intake::PlaneWorkContinuation {
                    resolution_id: continuation.resolution_id,
                    park_id: continuation.park_id,
                    effect_id: continuation.effect_id,
                    operation_id: continuation.operation_id,
                    park_generation: continuation.park_generation,
                    input_json: continuation.input_json,
                    input_digest: continuation.input_digest,
                    checkpoint,
                })
            }
            (Some(_), Some(_)) => {
                return Err(crate::plane_intake::PlaneIntakeError::Source(
                    "ClaimActionWork returned mismatched continuation and park snapshots".into(),
                ));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(crate::plane_intake::PlaneIntakeError::Source(
                    "ClaimActionWork returned an incomplete continuation snapshot".into(),
                ));
            }
        };
        let effect = claimed.effect.ok_or_else(|| {
            crate::plane_intake::PlaneIntakeError::Source(
                "ClaimActionWork returned no effect".into(),
            )
        })?;
        let instance = sekai
            .get_action_instance(GetActionInstanceRequest {
                instance_id: effect.instance_id.clone(),
                namespace: String::new(),
                idempotency_key: String::new(),
            })
            .await
            .map_err(|error| {
                crate::plane_intake::PlaneIntakeError::Source(format!("GetActionInstance: {error}"))
            })?
            .into_inner()
            .instance
            .ok_or_else(|| {
                crate::plane_intake::PlaneIntakeError::Source(
                    "GetActionInstance returned no instance".into(),
                )
            })?;
        // Parameter lookup happens after claim and may consume most of the
        // initial TTL. Revalidate and renew the same fence before the host is
        // allowed to start the run.
        let renew_started = Instant::now();
        let effect = match sekai
            .heartbeat_action_claim(HeartbeatActionClaimRequest {
                effect_id: effect.effect_id,
                runtime_id: effect.claim_owner,
                claim_generation: effect.claim_generation,
                fencing_token: effect.claim_fencing_token,
                ttl_ms: duration_millis(ttl)?,
            })
            .await
        {
            Ok(response) => response,
            Err(status) if is_claim_contention(&status) => return Ok(None),
            Err(error) => {
                return Err(crate::plane_intake::PlaneIntakeError::Source(format!(
                    "HeartbeatActionClaim before run: {error}"
                )));
            }
        }
        .into_inner()
        .effect
        .ok_or_else(|| {
            crate::plane_intake::PlaneIntakeError::Source(
                "HeartbeatActionClaim before run returned no effect".into(),
            )
        })?;

        Ok(Some(crate::plane_intake::PlaneClaim {
            work: crate::plane_intake::ClaimedPlaneWork {
                effect_id: effect.effect_id,
                instance_id: effect.instance_id,
                operation_id: effect.operation_id,
                kind: effect.kind,
                status: effect.status,
                payload_json: effect.payload_json,
                parameters_json: instance.parameters_json,
                resolved_task: None,
                continuation,
            },
            lease: crate::plane_intake::PlaneClaimLease {
                runtime_id: effect.claim_owner,
                generation: effect.claim_generation,
                fencing_token: effect.claim_fencing_token,
                expires_at_ms: effect.claim_expires_at_ms,
                valid_until: renew_started + ttl,
            },
        }))
    }

    async fn heartbeat(
        &self,
        claim: &crate::plane_intake::PlaneClaim,
        ttl: Duration,
    ) -> Result<crate::plane_intake::PlaneClaimLease, crate::plane_intake::PlaneIntakeError> {
        let renew_started = Instant::now();
        let (_chisei, mut sekai) = self.inner.connect().await.map_err(plane_intake_source)?;
        let effect = sekai
            .heartbeat_action_claim(HeartbeatActionClaimRequest {
                effect_id: claim.work.effect_id.clone(),
                runtime_id: claim.lease.runtime_id.clone(),
                claim_generation: claim.lease.generation,
                fencing_token: claim.lease.fencing_token.clone(),
                ttl_ms: duration_millis(ttl)?,
            })
            .await
            .map_err(|error| {
                if is_claim_contention(&error) {
                    crate::plane_intake::PlaneIntakeError::FenceLost(error.to_string())
                } else {
                    crate::plane_intake::PlaneIntakeError::Source(format!(
                        "HeartbeatActionClaim: {error}"
                    ))
                }
            })?
            .into_inner()
            .effect
            .ok_or_else(|| {
                crate::plane_intake::PlaneIntakeError::Source(
                    "HeartbeatActionClaim returned no effect".into(),
                )
            })?;
        Ok(crate::plane_intake::PlaneClaimLease {
            runtime_id: effect.claim_owner,
            generation: effect.claim_generation,
            fencing_token: effect.claim_fencing_token,
            expires_at_ms: effect.claim_expires_at_ms,
            valid_until: renew_started + ttl,
        })
    }

    async fn ack(
        &self,
        claim: &crate::plane_intake::PlaneClaim,
        ack: &crate::plane_intake::PlaneAck,
    ) -> Result<(), crate::plane_intake::PlaneIntakeError> {
        let (_chisei, mut sekai) = self.inner.connect().await.map_err(plane_intake_source)?;
        sekai
            .ack_action_work(AckActionWorkRequest {
                effect_id: claim.work.effect_id.clone(),
                runtime_id: claim.lease.runtime_id.clone(),
                claim_generation: claim.lease.generation,
                fencing_token: claim.lease.fencing_token.clone(),
                outcome: ack.outcome.as_str().into(),
                reason: ack.reason.clone(),
                request_id: ack.request_id.clone(),
                checkpoint_store_id: ack
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.store_id.clone())
                    .unwrap_or_default(),
                checkpoint_ref: ack
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.reference.clone())
                    .unwrap_or_default(),
                checkpoint_digest: ack
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.digest.clone())
                    .unwrap_or_default(),
            })
            .await
            .map_err(|error| {
                if is_claim_contention(&error) {
                    crate::plane_intake::PlaneIntakeError::FenceLost(error.to_string())
                } else {
                    crate::plane_intake::PlaneIntakeError::Source(format!("AckActionWork: {error}"))
                }
            })?;
        Ok(())
    }

    async fn report_claim_event(
        &self,
        claim: &crate::plane_intake::PlaneClaim,
        kind: crate::plane_intake::PlaneClaimEventKind,
        checkpoint_digest: &str,
        reason_code: &str,
        request_id: &str,
    ) -> Result<(), crate::plane_intake::PlaneIntakeError> {
        let (_chisei, mut sekai) = self.inner.connect().await.map_err(plane_intake_source)?;
        sekai
            .report_action_claim_event(ReportActionClaimEventRequest {
                effect_id: claim.work.effect_id.clone(),
                runtime_id: claim.lease.runtime_id.clone(),
                claim_generation: claim.lease.generation,
                fencing_token: claim.lease.fencing_token.clone(),
                kind: kind.as_str().into(),
                checkpoint_digest: checkpoint_digest.into(),
                reason_code: reason_code.into(),
                request_id: request_id.into(),
            })
            .await
            .map_err(|error| {
                if is_claim_contention(&error) {
                    crate::plane_intake::PlaneIntakeError::FenceLost(error.to_string())
                } else {
                    crate::plane_intake::PlaneIntakeError::Source(format!(
                        "ReportActionClaimEvent: {error}"
                    ))
                }
            })?;
        Ok(())
    }
}

fn duration_millis(duration: Duration) -> Result<i64, crate::plane_intake::PlaneIntakeError> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        crate::plane_intake::PlaneIntakeError::Source("claim TTL exceeds i64 milliseconds".into())
    })
}

fn plane_intake_source(error: GovernanceError) -> crate::plane_intake::PlaneIntakeError {
    crate::plane_intake::PlaneIntakeError::Source(error.to_string())
}

fn is_claim_contention(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
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
        let (mut chisei, _sekai) = self.connect().await?;
        let response = chisei
            .get_effective_policy_summary(GetEffectivePolicySummaryRequest {
                namespace: self.namespace.clone(),
                provider: String::new(),
            })
            .await
            .map_err(|e| GovernanceError::Message(format!("GetEffectivePolicySummary: {e}")))?
            .into_inner();
        Ok(available_models_from_summary(response))
    }

    async fn begin_run(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
    ) -> Result<RunHandle, GovernanceError> {
        self.begin_governed_run(run_id, task, logical_operation_id, None)
            .await
    }

    async fn begin_run_with_checkpoint(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        checkpoint: Option<&GovernanceCheckpoint>,
    ) -> Result<RunHandle, GovernanceError> {
        self.begin_governed_run(run_id, task, logical_operation_id, checkpoint)
            .await
    }

    fn checkpoint_state(&self, run_id: &str) -> Option<GovernanceCheckpoint> {
        self.harvest_checkpoint_state(run_id)
    }

    fn tool_requires_execution_checkpoint(&self, name: &str) -> bool {
        // Read actions still consume an external-action permit. Persist the
        // stable call identity before authorization so a crash cannot redeem
        // that permit twice on resume, even when the host effect is read-only.
        Self::tool_requires_external_action(name)
    }

    async fn plan_turn(
        &self,
        handle: &RunHandle,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
        _local_model: &dyn crate::model::ModelPort,
    ) -> Result<ModelTurn, GovernanceError> {
        let (mut chisei, _sekai) = self.connect().await?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let input = ExecutionInput {
            request_id: request_id.clone(),
            namespace: handle.namespace.clone(),
            spec: messages
                .iter()
                .find(|m| m.role == "user")
                .map(|m| m.content.clone())
                .unwrap_or_default(),
            preferred_model: self.preferred_model.clone(),
            preferred_runtime: String::new(),
            task_type: "agent".into(),
            priority: 0,
            user_id: self.principal.clone(),
            estimated_tokens: self.max_tokens,
            messages: messages
                .iter()
                .map(|m| ProtoChatMessage {
                    role: m.role.clone(),
                    content: m.content.clone(),
                    tool_call_id: m.tool_call_id.clone(),
                    tool_calls: m
                        .tool_calls
                        .iter()
                        .map(|c| ProtoToolCall {
                            id: c.id.clone(),
                            name: c.name.clone(),
                            args_json: c.args_json.clone(),
                        })
                        .collect(),
                })
                .collect(),
            tools: tools
                .iter()
                .map(|t| ProtoToolDef {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    input_schema_json: t.schema.clone(),
                })
                .collect(),
            system: system.into(),
            max_tokens: self.max_tokens,
            task_class: "shikigami-run".into(),
            logical_operation_id: handle.operation_id.clone(),
            attempt_id: handle.run_id.clone(),
            route_override: String::new(),
        };

        let plan_resp = chisei
            .plan_execution(PlanExecutionRequest {
                input: Some(input),
                gunshi_allocation: None,
            })
            .await
            .map_err(|e| GovernanceError::Message(format!("PlanExecution: {e}")))?
            .into_inner();

        let plan = plan_resp
            .plan
            .ok_or_else(|| GovernanceError::Message("missing plan".into()))?;
        let plan_id = plan.plan_id.clone();
        self.update_harvest_plan(&handle.run_id, plan_id.clone())?;

        if plan.budget.as_ref().is_some_and(|b| !b.allowed) {
            let reason = plan
                .budget
                .as_ref()
                .map(|b| b.reason.clone())
                .unwrap_or_else(|| "budget denied".into());
            self.report_failed_model_event(handle).await?;
            return Err(GovernanceError::Denied(reason));
        }
        if !plan.executable {
            let reason = if !plan.eval_regression_reason.is_empty() {
                plan.eval_regression_reason
            } else {
                plan.warnings
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "plan not executable".into())
            };
            self.report_failed_model_event(handle).await?;
            return Err(GovernanceError::Denied(reason));
        }

        let mut stream = match chisei
            .execute_plan_stream(ExecutePlanRequest { plan: Some(plan) })
            .await
        {
            Ok(stream) => stream.into_inner(),
            Err(error) => {
                self.report_failed_model_event(handle).await?;
                return Err(GovernanceError::Message(format!(
                    "ExecutePlanStream: {error}"
                )));
            }
        };

        let mut final_response = None;
        loop {
            let event = match stream.message().await {
                Ok(event) => event,
                Err(error) => {
                    self.report_failed_model_event(handle).await?;
                    return Err(GovernanceError::Message(format!("stream: {error}")));
                }
            };
            let Some(event) = event else {
                break;
            };
            if event.response.is_some() {
                final_response = event.response;
            }
            if event.done {
                break;
            }
        }
        let response = match final_response {
            Some(response) => response,
            None => {
                self.report_failed_model_event(handle).await?;
                return Err(GovernanceError::Message("missing model response".into()));
            }
        };

        Ok(ModelTurn {
            content: response.content,
            tool_calls: response
                .tool_calls
                .into_iter()
                .map(|c| ToolCall {
                    id: c.id,
                    name: c.name,
                    args_json: c.args_json,
                })
                .collect(),
            usage: None, // plane usage surfaces via harvest when available
        })
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
        if !Self::tool_requires_external_action(name) {
            return Ok(());
        }

        let (mut chisei, _) = match self.connect().await {
            Ok(c) => c,
            Err(e) => {
                if self.fail_closed {
                    return Err(e);
                }
                return Ok(());
            }
        };

        let request = match self.build_external_action_request(handle, call_id, name, args_json) {
            Ok(request) => request,
            Err(error) => {
                if self.fail_closed {
                    return Err(error);
                }
                return Ok(());
            }
        };
        let response = match chisei
            .authorize_external_action(AuthorizeExternalActionRequest {
                request: Some(request.clone()),
                offline: false,
            })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if self.fail_closed {
                    return Err(GovernanceError::Unavailable(format!(
                        "AuthorizeExternalAction: {e}"
                    )));
                }
                return Ok(());
            }
        };

        let response = response.into_inner();
        let decision = response
            .decision
            .ok_or_else(|| GovernanceError::Message("external-action missing decision".into()))?;
        let permit = Self::permit_for_decision(&decision, response.permit)?;
        let redemption_response = match chisei
            .redeem_external_action_permit(RedeemExternalActionPermitRequest {
                permit: Some(permit.clone()),
                executor: request.intended_executor.clone(),
                requesting_harness: request.requesting_harness.clone(),
                canonical_arguments_digest: request.canonical_arguments_digest.clone(),
                target_selectors: request.target_selectors.clone(),
                observed_preconditions: request.immutable_preconditions.clone(),
                host_capabilities: request.required_host_capabilities.clone(),
                idempotency_key: request.idempotency_key.clone(),
                execution_id: format!(
                    "shikigami-execution:{}",
                    Self::arguments_digest(&format!("{}:{call_id}", handle.run_id))
                ),
                invoked_at_ms: 0,
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let security_sensitive_failure = matches!(
                    error.code(),
                    tonic::Code::PermissionDenied
                        | tonic::Code::Unauthenticated
                        | tonic::Code::FailedPrecondition
                        | tonic::Code::InvalidArgument
                );
                let governance_error = match error.code() {
                    tonic::Code::Unauthenticated => {
                        GovernanceError::Unavailable(format!("RedeemExternalActionPermit: {error}"))
                    }
                    _ if security_sensitive_failure => {
                        GovernanceError::Message(format!("RedeemExternalActionPermit: {error}"))
                    }
                    _ => {
                        GovernanceError::Unavailable(format!("RedeemExternalActionPermit: {error}"))
                    }
                };
                if self.fail_closed || security_sensitive_failure {
                    return Err(governance_error);
                }
                return Ok(());
            }
        };
        let redemption = redemption_response
            .into_inner()
            .redemption
            .ok_or_else(|| GovernanceError::Message("external-action redemption missing".into()))?;
        if redemption.permit_id != permit.permit_id
            || redemption.executor != request.intended_executor
        {
            return Err(GovernanceError::Message(
                "external-action redemption does not match the requested permit".into(),
            ));
        }
        Ok(())
    }

    async fn report_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        self.report_tool_event(handle, None, name, ok, detail).await
    }

    async fn report_model_turn(&self, handle: &RunHandle, ok: bool) -> Result<(), GovernanceError> {
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
        let reports = self
            .harvest
            .lock()
            .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
            .get(&handle.run_id)
            .map(|state| state.pending_tool_reports.clone())
            .unwrap_or_default();
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
            if self.fail_closed {
                return Err(error);
            }
            self.forget_harvest(&handle.run_id);
            return Ok(());
        }
        let receipt = match self.harvest_receipt(handle).await {
            Ok(receipt) => receipt,
            Err(error) => {
                if self.fail_closed {
                    return Err(error);
                }
                self.forget_harvest(&handle.run_id);
                return Ok(());
            }
        };
        if receipt.complete {
            self.forget_harvest(&handle.run_id);
            return Ok(());
        }
        if receipt
            .missing_surfaces
            .iter()
            .any(|surface| surface == "attempt")
        {
            let attributes = harvest::attempt_attributes(&handle.run_id, &handle.operation_id);
            if let Err(error) = self
                .report_harvest_event(handle, harvest::KIND_ATTEMPT, attributes, vec![])
                .await
            {
                if self.fail_closed {
                    return Err(error);
                }
                self.forget_harvest(&handle.run_id);
                return Ok(());
            }
        }
        let receipt = match self.harvest_receipt(handle).await {
            Ok(receipt) => receipt,
            Err(error) => {
                if self.fail_closed {
                    return Err(error);
                }
                self.forget_harvest(&handle.run_id);
                return Ok(());
            }
        };
        if receipt
            .missing_surfaces
            .iter()
            .any(|surface| surface == "model_call")
        {
            let model_operation_id = self
                .harvest
                .lock()
                .map_err(|_| GovernanceError::Message("harvest state lock poisoned".into()))?
                .get(&handle.run_id)
                .and_then(|state| state.model_operation_id.clone())
                .filter(|operation_id| !operation_id.is_empty());
            let Some(model_operation_id) = model_operation_id else {
                match self
                    .abort_uncheckpointed_receipt(handle, &outcome.summary)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(error) if self.fail_closed => return Err(error),
                    Err(_) => {
                        self.forget_harvest(&handle.run_id);
                        return Ok(());
                    }
                }
            };
            if let Err(error) = self
                .report_harvest_event(
                    handle,
                    harvest::KIND_MODEL,
                    harvest::model_attributes(&model_operation_id, false),
                    vec![],
                )
                .await
            {
                if self.fail_closed {
                    return Err(error);
                }
                self.forget_harvest(&handle.run_id);
                return Ok(());
            }
        }
        let attributes = harvest::complete_attributes(&outcome);
        let references = harvest::complete_references(handle, &outcome);
        let response = match self
            .report_harvest_event(handle, harvest::KIND_COMPLETE, attributes, references)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if self.fail_closed {
                    return Err(error);
                }
                self.forget_harvest(&handle.run_id);
                return Ok(());
            }
        };
        if !response.complete {
            let error = GovernanceError::Message(format!(
                "operation receipt remains incomplete; missing surfaces: {}",
                response.missing_surfaces.join(", ")
            ));
            if self.fail_closed {
                return Err(error);
            }
        }
        self.forget_harvest(&handle.run_id);
        Ok(())
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
    g.probe().await?;
    Ok(format!("reachable at {}", g.endpoint))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision(kind: &str, reason: &str) -> ExternalActionDecision {
        ExternalActionDecision {
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
    fn auth_interceptor_uses_v1_principal_and_token_metadata() {
        let mut interceptor = AuthInterceptor {
            principal: MetadataValue::try_from("agent:test").unwrap(),
            token: Some(MetadataValue::try_from("Bearer synthetic").unwrap()),
        };
        let request = interceptor.call(Request::new(())).unwrap();
        assert_eq!(
            request
                .metadata()
                .get("x-principal")
                .unwrap()
                .to_str()
                .unwrap(),
            "agent:test"
        );
        assert_eq!(
            request
                .metadata()
                .get("x-sekai-auth-source")
                .unwrap()
                .to_str()
                .unwrap(),
            "token"
        );
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer synthetic"
        );
    }

    #[test]
    fn harvest_event_context_is_receipt_scoped_and_causal() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        governance
            .harvest
            .lock()
            .unwrap()
            .insert("run-1".into(), HarvestState::default());
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
        governance.remember_harvest_event("run-1", event_id.clone());
        let (_, next_parent, _) = governance.harvest_event_context(&handle).unwrap();
        assert_eq!(next_parent, event_id);
    }

    #[test]
    fn harvest_checkpoint_keeps_host_model_and_logical_correlation() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        governance
            .harvest
            .lock()
            .unwrap()
            .insert("run-1".into(), HarvestState::default());
        governance
            .harvest
            .lock()
            .unwrap()
            .get_mut("run-1")
            .unwrap()
            .logical_operation_id = Some("logical-op-1".into());
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
    fn only_failed_precondition_is_normal_claim_contention() {
        assert!(is_claim_contention(&Status::failed_precondition(
            "already claimed"
        )));
        assert!(!is_claim_contention(&Status::permission_denied("denied")));
        assert!(!is_claim_contention(&Status::unavailable("offline")));
    }

    #[test]
    fn report_skips_external_action() {
        assert!(!SekaiChiseiGovernance::tool_requires_external_action(
            "report"
        ));
        assert!(!SekaiChiseiGovernance::tool_requires_external_action(
            "escalate"
        ));
        assert!(!SekaiChiseiGovernance::tool_requires_external_action(
            "todo_write"
        ));
        assert!(SekaiChiseiGovernance::tool_requires_external_action("bash"));
        assert!(SekaiChiseiGovernance::tool_requires_external_action(
            "write_file"
        ));
        assert!(SekaiChiseiGovernance::tool_requires_external_action("edit"));
        assert!(SekaiChiseiGovernance::tool_requires_external_action(
            "read_file"
        ));
    }

    #[test]
    fn risk_classes_match_tool_consequences() {
        assert_eq!(
            SekaiChiseiGovernance::tool_risk_class("bash"),
            "destructive"
        );
        assert_eq!(
            SekaiChiseiGovernance::tool_risk_class("write_file"),
            "write"
        );
        assert_eq!(SekaiChiseiGovernance::tool_risk_class("edit"), "write");
        assert_eq!(SekaiChiseiGovernance::tool_risk_class("read_file"), "read");
    }

    #[test]
    fn permit_allows_execution() {
        assert!(
            SekaiChiseiGovernance::interpret_external_action_decision(&decision("permit", ""))
                .is_ok()
        );
    }

    #[test]
    fn permit_decision_requires_signed_permit() {
        let err =
            SekaiChiseiGovernance::permit_for_decision(&decision("permit", ""), None).unwrap_err();
        assert!(matches!(err, GovernanceError::Message(message) if message.contains("permit")));
    }

    #[test]
    fn external_action_request_uses_v1_risk_and_project_binding() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        governance
            .harvest
            .lock()
            .unwrap()
            .insert("run-1".into(), HarvestState::default());
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
        let request = governance
            .build_external_action_request(&handle, "tool-1-0-provider-call", "write_file", "{}")
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
            governance
                .build_external_action_request(
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
        let err = SekaiChiseiGovernance::interpret_external_action_decision(&decision(
            "deny",
            "budget exhausted",
        ))
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
        let err = SekaiChiseiGovernance::interpret_external_action_decision(&decision(
            "require_approval",
            "needs human",
        ))
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
        let err = SekaiChiseiGovernance::interpret_external_action_decision(&decision("maybe", ""))
            .unwrap_err();
        assert!(matches!(err, GovernanceError::Message(_)));
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
}
