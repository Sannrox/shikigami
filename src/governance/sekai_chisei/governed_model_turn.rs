//! One sekai-chisei governed model turn's planning and execution protocol.

use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;

use super::proto::chisei::{
    ChatMessage as ProtoChatMessage, ExecutionInput, ToolCall as ProtoToolCall,
    ToolDef as ProtoToolDef,
};
use super::{GovernanceError, RunHandle, SekaiChiseiGovernance, plane_session};
use crate::fallback::{
    self, FallbackDenial, FallbackView, ModelSource, evidence_identity, prompt_context_digest,
    tool_names, tool_surface_digest,
};
use crate::model::{ChatMessage, ModelPort, ModelTurn, ToolCall};
use crate::tools::ToolDef;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Plan and execute one governed model turn, including durable failure reporting.
pub(super) async fn execute(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    system: &str,
    messages: &[ChatMessage],
    tools: &[ToolDef],
    local_model: &dyn ModelPort,
) -> Result<ModelTurn, GovernanceError> {
    match execute_plane(governance, handle, system, messages, tools).await {
        Ok(turn) => {
            let _ = governance
                .harvest
                .set_fallback_active(&handle.run_id, false);
            Ok(turn)
        }
        Err(GovernanceError::Unavailable(_)) => {
            execute_fallback(governance, handle, system, messages, tools, local_model).await
        }
        Err(error) => Err(error),
    }
}

async fn execute_plane(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    system: &str,
    messages: &[ChatMessage],
    tools: &[ToolDef],
) -> Result<ModelTurn, GovernanceError> {
    let client = plane_session::connect(governance).await?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let input = execution_input(governance, handle, system, messages, tools, &request_id);
    let call_options = || {
        plane_session::call_options(
            governance,
            Some(&handle.namespace),
            Some(&handle.operation_id),
            Some(&request_id),
        )
    };

    let plan = client
        .plan_execution(input, call_options())
        .await
        .map_err(|error| plane_session::map_error("PlanExecution", error))?;
    governance.update_harvest_plan(&handle.run_id, plan.plan_id.clone())?;

    if plan.budget.as_ref().is_some_and(|budget| !budget.allowed) {
        let reason = plan
            .budget
            .as_ref()
            .map(|budget| budget.reason.clone())
            .unwrap_or_else(|| "budget denied".into());
        governance.report_failed_model_event(handle).await?;
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
        governance.report_failed_model_event(handle).await?;
        return Err(GovernanceError::Denied(reason));
    }

    let mut stream = match client.execute_plan_stream(plan, call_options()).await {
        Ok(stream) => stream,
        Err(error) => {
            governance.report_failed_model_event(handle).await?;
            return Err(plane_session::map_error("ExecutePlanStream", error));
        }
    };

    let mut final_response = None;
    while let Some(event) = stream.next().await {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                governance.report_failed_model_event(handle).await?;
                return Err(plane_session::map_error("ExecutePlanStream", error));
            }
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
            governance.report_failed_model_event(handle).await?;
            return Err(GovernanceError::Message("missing model response".into()));
        }
    };

    Ok(ModelTurn {
        content: response.content,
        tool_calls: response
            .tool_calls
            .into_iter()
            .map(|call| ToolCall {
                id: call.id,
                name: call.name,
                args_json: call.args_json,
            })
            .collect(),
        usage: None, // plane usage surfaces via harvest when available
    })
}

async fn execute_fallback(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    system: &str,
    messages: &[ChatMessage],
    tools: &[ToolDef],
    local_model: &dyn ModelPort,
) -> Result<ModelTurn, GovernanceError> {
    let stored = governance.harvest.fallback(&handle.run_id)?;
    let view = match stored.as_ref() {
        Some(checkpoint) => FallbackView {
            now_ms: now_ms(),
            fallback_enabled: governance.fallback_enabled,
            run_id: handle.run_id.clone(),
            operation_id: handle.operation_id.clone(),
            attempt_id: handle.run_id.clone(),
            claim_id: checkpoint.authorization.claim_id.clone(),
            local_model_digest: local_model.content_digest(),
            prompt_context_digest: prompt_context_digest(system, messages),
            tool_surface_digest: tool_surface_digest(tools),
            tool_names: tool_names(tools),
            live_fence: Some(checkpoint.held_fence.clone()),
            allow_test_signatures: governance.fallback_allow_test_signatures,
        },
        None => FallbackView {
            now_ms: now_ms(),
            fallback_enabled: governance.fallback_enabled,
            run_id: handle.run_id.clone(),
            operation_id: handle.operation_id.clone(),
            attempt_id: handle.run_id.clone(),
            claim_id: None,
            local_model_digest: local_model.content_digest(),
            prompt_context_digest: prompt_context_digest(system, messages),
            tool_surface_digest: tool_surface_digest(tools),
            tool_names: tool_names(tools),
            live_fence: None,
            allow_test_signatures: governance.fallback_allow_test_signatures,
        },
    };
    let authorization = stored.as_ref().map(|checkpoint| &checkpoint.authorization);
    let (source, selection) = fallback::select_model_source(false, authorization, &view)?;
    if source != ModelSource::Local {
        return Err(GovernanceError::Unavailable(
            "fallback selected plane source while the plane is unavailable".into(),
        ));
    }
    let selection = selection
        .ok_or_else(|| GovernanceError::Denied("fallback:missing_authorization".into()))?;
    let Some(mut checkpoint) = stored else {
        return Err(FallbackDenial::MissingAuthorization.into());
    };
    checkpoint.selection = selection;
    let turn = local_model
        .next_turn(system, messages, tools)
        .await
        .map_err(|error| GovernanceError::Message(error.to_string()))?;
    let mut view = view;
    view.now_ms = now_ms();
    fallback::still_valid(&checkpoint, &view)?;
    let payload = serde_json::json!({
        "content": turn.content,
        "tool_calls": turn.tool_calls.iter().map(|call| {
            serde_json::json!({
                "id": call.id,
                "name": call.name,
                "args_json": call.args_json,
            })
        }).collect::<Vec<_>>(),
    });
    let payload = serde_json::to_string(&payload).unwrap_or_else(|_| turn.content.clone());
    let payload_digest = fallback::sha256_hex(payload.as_bytes());
    let incoming = evidence_identity(
        &checkpoint.authorization,
        &format!("fallback-model:{}:{payload_digest}", handle.run_id),
        &payload,
    );
    checkpoint.reconciliation = fallback::reconcile(Some(&checkpoint), &incoming);
    if let Some(existing) = checkpoint.evidence.iter_mut().find(|evidence| {
        evidence.run_id == incoming.run_id
            && evidence.operation_id == incoming.operation_id
            && evidence.attempt_id == incoming.attempt_id
            && evidence.claim_id == incoming.claim_id
            && evidence.authorization_id == incoming.authorization_id
            && evidence.model_event_id == incoming.model_event_id
    }) {
        *existing = incoming;
    } else {
        checkpoint.evidence.push(incoming);
    }
    governance
        .harvest
        .set_fallback(&handle.run_id, checkpoint)?;
    governance
        .harvest
        .set_fallback_active(&handle.run_id, true)?;
    let _ = governance
        .harvest
        .set_model_operation(&handle.run_id, format!("fallback-model:{}", handle.run_id));
    Ok(turn)
}

fn execution_input(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    system: &str,
    messages: &[ChatMessage],
    tools: &[ToolDef],
    request_id: &str,
) -> ExecutionInput {
    ExecutionInput {
        request_id: request_id.into(),
        namespace: handle.namespace.clone(),
        spec: messages
            .iter()
            .find(|message| message.role == "user")
            .map(|message| message.content.clone())
            .unwrap_or_default(),
        preferred_model: governance.preferred_model.clone(),
        preferred_runtime: String::new(),
        task_type: "agent".into(),
        priority: 0,
        user_id: governance.principal.clone(),
        estimated_tokens: governance.max_tokens,
        messages: messages
            .iter()
            .map(|message| ProtoChatMessage {
                role: message.role.clone(),
                content: message.content.clone(),
                tool_call_id: message.tool_call_id.clone(),
                tool_calls: message
                    .tool_calls
                    .iter()
                    .map(|call| ProtoToolCall {
                        id: call.id.clone(),
                        name: call.name.clone(),
                        args_json: call.args_json.clone(),
                    })
                    .collect(),
            })
            .collect(),
        tools: tools
            .iter()
            .map(|tool| ProtoToolDef {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema_json: tool.schema.clone(),
            })
            .collect(),
        system: system.into(),
        max_tokens: governance.max_tokens,
        task_class: "shikigami-run".into(),
        logical_operation_id: handle.operation_id.clone(),
        attempt_id: handle.run_id.clone(),
        route_override: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::model::ToolCall as ModelToolCall;

    #[test]
    fn projects_one_governed_turn_without_losing_correlation_or_tool_context() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        let handle = RunHandle {
            run_id: "attempt-1".into(),
            operation_id: "operation-1".into(),
            namespace: "namespace-1".into(),
        };
        let messages = vec![
            ChatMessage {
                role: "user".into(),
                content: "first task".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
            ChatMessage {
                role: "assistant".into(),
                content: "calling".into(),
                tool_call_id: String::new(),
                tool_calls: vec![ModelToolCall {
                    id: "call-1".into(),
                    name: "read_file".into(),
                    args_json: r#"{"path":"README.md"}"#.into(),
                }],
            },
        ];
        let tools = vec![ToolDef {
            name: "read_file".into(),
            description: "read a file".into(),
            schema: r#"{"type":"object"}"#.into(),
        }];

        let input = execution_input(
            &governance,
            &handle,
            "system prompt",
            &messages,
            &tools,
            "request-1",
        );

        assert_eq!(input.request_id, "request-1");
        assert_eq!(input.namespace, "namespace-1");
        assert_eq!(input.logical_operation_id, "operation-1");
        assert_eq!(input.attempt_id, "attempt-1");
        assert_eq!(input.spec, "first task");
        assert_eq!(input.messages[1].tool_calls[0].id, "call-1");
        assert_eq!(input.tools[0].name, "read_file");
        assert_eq!(input.system, "system prompt");
    }
}
