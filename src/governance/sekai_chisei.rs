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

use proto::chisei::chisei_service_client::ChiseiServiceClient;
use proto::chisei::{
    ChatMessage as ProtoChatMessage, ExecutePlanRequest, ExecutionInput, PlanExecutionRequest,
    ReportOperationEventRequest, ToolCall as ProtoToolCall, ToolDef as ProtoToolDef,
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
        let endpoint = config
            .governance
            .endpoint
            .clone()
            .unwrap_or_default();
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

    async fn begin_run(&self, run_id: &str, task: &str) -> Result<RunHandle, GovernanceError> {
        if self.endpoint.trim().is_empty() {
            return Err(GovernanceError::Unavailable(
                "sekai-chisei endpoint not set".into(),
            ));
        }
        // Connectivity check; operation id is harness-owned lineage for PlanExecution.
        if let Err(e) = self.probe().await {
            if self.fail_closed {
                return Err(e);
            }
        }
        let _ = task;
        Ok(RunHandle {
            run_id: run_id.into(),
            operation_id: run_id.into(),
            namespace: self.namespace.clone(),
        })
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
        _handle: &RunHandle,
        _name: &str,
        _args_json: &str,
    ) -> Result<(), GovernanceError> {
        // Tool allow-list is enforced by the plane via prepared tools; host still
        // re-checks enabled tools in the engine. Future: external-action authz.
        Ok(())
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
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("tool".into(), name.into());
        attributes.insert("ok".into(), ok.to_string());
        attributes.insert("detail".into(), detail.chars().take(2000).collect());
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
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("success".into(), outcome.success.to_string());
        attributes.insert(
            "summary".into(),
            outcome.summary.chars().take(4000).collect(),
        );
        let _ = chisei
            .report_operation_event(ReportOperationEventRequest {
                operation_id: handle.operation_id.clone(),
                event_id: uuid::Uuid::new_v4().to_string(),
                parent_event_id: String::new(),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                kind: "shikigami.run.complete".into(),
                attributes,
                references: vec![],
            })
            .await;
        Ok(())
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
