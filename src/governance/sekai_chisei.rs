//! First-party sekai-chisei governance adapter.

use std::time::Duration;

use async_trait::async_trait;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::config::Config;
use crate::model::{ChatMessage, ModelTurn, ToolCall};
use crate::tools::ToolDef;

use super::{GovernanceError, GovernancePort, RunHandle, RunOutcome};

pub mod proto {
    pub mod chisei {
        tonic::include_proto!("chisei");
    }
    pub mod llm {
        tonic::include_proto!("llm");
    }
    pub mod sekai {
        tonic::include_proto!("sekai");
    }
}

use sha2::{Digest, Sha256};

use proto::chisei::chisei_service_client::ChiseiServiceClient;
use proto::chisei::{
    AuthorizeExternalActionRequest, AuthorizeOperationReporterRequest,
    ChatMessage as ProtoChatMessage, ExecutePlanRequest, ExecutionInput, ExternalActionDecision,
    ExternalActionRequest, PlanExecutionRequest, ReportOperationEventRequest,
    ToolCall as ProtoToolCall, ToolDef as ProtoToolDef,
};
use proto::sekai::ListSchemaTypesRequest;
use proto::sekai::sekai_service_client::SekaiServiceClient;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const RPC_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
struct AuthInterceptor {
    principal: MetadataValue<Ascii>,
    token: Option<MetadataValue<Ascii>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("x-sekai-principal", self.principal.clone());
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
        })
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
    /// host execution. `report` is harness-internal completion signaling.
    pub(crate) fn tool_requires_external_action(name: &str) -> bool {
        !matches!(name, "report" | "escalate")
    }

    /// Map a shikigami tool to the external-action risk class contract.
    pub(crate) fn tool_risk_class(name: &str) -> &'static str {
        match name {
            "bash" => "destructive",
            "write_file" | "edit" => "write",
            "read_file" => "read",
            _ => "write",
        }
    }

    fn arguments_digest(args_json: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(args_json.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Interpret an ExternalActionDecision for headless execution.
    /// Only `permit` allows the tool to run; `require_approval` is denied
    /// because the headless path cannot wait for interactive approval.
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
            other => Err(GovernanceError::Denied(format!(
                "external-action unexpected decision `{other}`{}",
                if decision.reason.is_empty() {
                    String::new()
                } else {
                    format!(": {}", decision.reason)
                }
            ))),
        }
    }

    fn build_external_action_request(
        &self,
        handle: &RunHandle,
        name: &str,
        args_json: &str,
    ) -> ExternalActionRequest {
        let request_id = uuid::Uuid::new_v4().to_string();
        ExternalActionRequest {
            version: "external-action.request/v1".into(),
            operation_id: handle.operation_id.clone(),
            parent_operation_id: String::new(),
            attempt_id: handle.run_id.clone(),
            request_id: request_id.clone(),
            actor: self.principal.clone(),
            namespace: handle.namespace.clone(),
            requesting_harness: "shikigami".into(),
            intended_executor: "shikigami".into(),
            action_type: format!("shikigami.tool.{name}"),
            parameter_schema: "application/json".into(),
            canonical_arguments_digest: Self::arguments_digest(args_json),
            policy_summary: std::collections::HashMap::from([("tool".into(), name.to_string())]),
            target_selectors: vec![format!("tool:{name}")],
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
        }
    }
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

    async fn begin_run(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
    ) -> Result<RunHandle, GovernanceError> {
        if self.endpoint.trim().is_empty() {
            return Err(GovernanceError::Unavailable(
                "sekai-chisei endpoint not set".into(),
            ));
        }
        // Connectivity check; operation id is harness-owned lineage for PlanExecution.
        if let Err(e) = self.probe().await
            && self.fail_closed
        {
            return Err(e);
        }
        let operation_id = logical_operation_id.unwrap_or(run_id).to_string();
        let handle = RunHandle {
            run_id: run_id.into(),
            operation_id,
            namespace: self.namespace.clone(),
        };

        // Best-effort harvest: authorize reporter + run.begin event.
        if let Ok((mut chisei, _)) = self.connect().await {
            let _ = chisei
                .authorize_operation_reporter(AuthorizeOperationReporterRequest {
                    operation_id: handle.operation_id.clone(),
                    principal: self.principal.clone(),
                    event_kinds: vec![
                        "shikigami.run.begin".into(),
                        "shikigami.tool".into(),
                        "shikigami.run.complete".into(),
                    ],
                })
                .await;
            let mut attributes =
                harvest::begin_attributes(run_id, &handle.operation_id, task, &self.principal);
            attributes.insert("namespace".into(), handle.namespace.clone());
            let _ = chisei
                .report_operation_event(ReportOperationEventRequest {
                    operation_id: handle.operation_id.clone(),
                    event_id: uuid::Uuid::new_v4().to_string(),
                    parent_event_id: String::new(),
                    timestamp_ms: chrono::Utc::now().timestamp_millis(),
                    kind: "shikigami.run.begin".into(),
                    attributes,
                    references: vec![],
                })
                .await;
        } else if self.fail_closed {
            // connect failed after successful probe is unexpected; still allow
            // the run to proceed — complete_run will surface fail-closed errors.
        }

        Ok(handle)
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
                    name: t.name.into(),
                    description: t.description.into(),
                    input_schema_json: t.schema.into(),
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
            .plan_execution(PlanExecutionRequest { input: Some(input) })
            .await
            .map_err(|e| GovernanceError::Message(format!("PlanExecution: {e}")))?
            .into_inner();

        let plan = plan_resp
            .plan
            .ok_or_else(|| GovernanceError::Message("missing plan".into()))?;

        if plan.budget.as_ref().is_some_and(|b| !b.allowed) {
            let reason = plan
                .budget
                .as_ref()
                .map(|b| b.reason.clone())
                .unwrap_or_else(|| "budget denied".into());
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
            return Err(GovernanceError::Denied(reason));
        }

        let mut stream = chisei
            .execute_plan_stream(ExecutePlanRequest { plan: Some(plan) })
            .await
            .map_err(|e| GovernanceError::Message(format!("ExecutePlanStream: {e}")))?
            .into_inner();

        let mut final_response = None;
        while let Some(event) = stream
            .message()
            .await
            .map_err(|e| GovernanceError::Message(format!("stream: {e}")))?
        {
            if event.response.is_some() {
                final_response = event.response;
            }
            if event.done {
                break;
            }
        }
        let response = final_response
            .ok_or_else(|| GovernanceError::Message("missing model response".into()))?;

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
        })
    }

    async fn authorize_tool(
        &self,
        handle: &RunHandle,
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

        let request = self.build_external_action_request(handle, name, args_json);
        let response = match chisei
            .authorize_external_action(AuthorizeExternalActionRequest {
                request: Some(request),
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

        let decision = response
            .into_inner()
            .decision
            .ok_or_else(|| GovernanceError::Denied("external-action missing decision".into()))?;

        Self::interpret_external_action_decision(&decision)
    }

    async fn report_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), GovernanceError> {
        let (mut chisei, _) = match self.connect().await {
            Ok(c) => c,
            Err(e) => {
                if self.fail_closed {
                    return Err(e);
                }
                return Ok(());
            }
        };
        let attributes = harvest::tool_attributes(name, ok, detail);
        let _ = chisei
            .report_operation_event(ReportOperationEventRequest {
                operation_id: handle.operation_id.clone(),
                event_id: uuid::Uuid::new_v4().to_string(),
                parent_event_id: String::new(),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                kind: "shikigami.tool".into(),
                attributes,
                references: vec![],
            })
            .await;
        Ok(())
    }

    async fn complete_run(
        &self,
        handle: &RunHandle,
        outcome: RunOutcome,
    ) -> Result<(), GovernanceError> {
        let (mut chisei, _) = match self.connect().await {
            Ok(c) => c,
            Err(e) => {
                if self.fail_closed {
                    return Err(e);
                }
                return Ok(());
            }
        };
        let attributes = harvest::complete_attributes(&outcome);
        let references = harvest::complete_references(handle, &outcome);
        let _ = chisei
            .report_operation_event(ReportOperationEventRequest {
                operation_id: handle.operation_id.clone(),
                event_id: uuid::Uuid::new_v4().to_string(),
                parent_event_id: String::new(),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                kind: "shikigami.run.complete".into(),
                attributes,
                references,
            })
            .await;
        Ok(())
    }
}

/// Pure harvest mapping: run lifecycle → plane operation events.
/// Local checkpoint/state is never authoritative for governed truth.
pub mod harvest {
    use std::collections::HashMap;

    use super::proto::chisei::OperationEvidenceReference;
    use crate::governance::{RunHandle, RunOutcome};

    pub const KIND_BEGIN: &str = "shikigami.run.begin";
    pub const KIND_TOOL: &str = "shikigami.tool";
    pub const KIND_COMPLETE: &str = "shikigami.run.complete";

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
    fn report_skips_external_action() {
        assert!(!SekaiChiseiGovernance::tool_requires_external_action(
            "report"
        ));
        assert!(!SekaiChiseiGovernance::tool_requires_external_action(
            "escalate"
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
        assert!(matches!(err, GovernanceError::Denied(_)));
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
}
