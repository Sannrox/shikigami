//! One sekai-chisei governed model turn's planning and execution protocol.

use futures_util::StreamExt;

use super::proto::chisei::{
    ChatMessage as ProtoChatMessage, ExecutionInput, ToolCall as ProtoToolCall,
    ToolDef as ProtoToolDef,
};
use super::{GovernanceError, RunHandle, SekaiChiseiGovernance};
use crate::model::{ChatMessage, ModelTurn, ToolCall};
use crate::tools::ToolDef;

/// Plan and execute one governed model turn, including durable failure reporting.
pub(super) async fn execute(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    system: &str,
    messages: &[ChatMessage],
    tools: &[ToolDef],
) -> Result<ModelTurn, GovernanceError> {
    let client = governance.connect().await?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let input = execution_input(governance, handle, system, messages, tools, &request_id);
    let call_options = || {
        governance.sdk_call_options(
            Some(&handle.namespace),
            Some(&handle.operation_id),
            Some(&request_id),
        )
    };

    let plan = client
        .plan_execution(input, call_options())
        .await
        .map_err(|error| SekaiChiseiGovernance::sdk_error("PlanExecution", error))?;
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
            return Err(SekaiChiseiGovernance::sdk_error("ExecutePlanStream", error));
        }
    };

    let mut final_response = None;
    while let Some(event) = stream.next().await {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                governance.report_failed_model_event(handle).await?;
                return Err(SekaiChiseiGovernance::sdk_error("ExecutePlanStream", error));
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
