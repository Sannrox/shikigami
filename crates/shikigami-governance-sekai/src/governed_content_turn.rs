//! One fail-closed governed content planning and execution call.

use futures_util::StreamExt;

use super::proto::chisei::{
    ContentCapabilitiesV1 as ProtoCapabilities, ContentExecutionInputV1,
    ContentMessageV1 as ProtoMessage, ContentPartDescriptorV1 as ProtoDescriptor,
    ContentProvenanceV1 as ProtoProvenance, ExecutionInput, ResolvedContentPartV1,
    ToolCall as ProtoToolCall, ToolDef as ProtoToolDef, resolved_content_part_v1,
};
use super::{GovernanceError, RunHandle, SekaiChiseiGovernance, plane_session};
use crate::content::{
    CONTENT_CONTRACT_VERSION, ContentCapabilitiesV1, ContentMessageV1, ContentModelTurnV1,
    ContentPartDescriptor, ContentResolver, ResolvedContent, resolve_accepted, validate_messages,
};
use crate::model::ToolCall;
use crate::tools::ToolDef;

pub(super) async fn execute(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    system: &str,
    messages: &[ContentMessageV1],
    tools: &[ToolDef],
    capabilities: &ContentCapabilitiesV1,
    resolver: &dyn ContentResolver,
) -> Result<ContentModelTurnV1, GovernanceError> {
    let client = plane_session::connect(governance).await?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let input = content_execution_input(
        governance,
        handle,
        system,
        messages,
        tools,
        capabilities,
        &request_id,
    );
    let call_options = || {
        plane_session::call_options(
            governance,
            Some(&handle.namespace),
            Some(&handle.operation_id),
            Some(&request_id),
        )
    };
    let plan = client
        .plan_content_execution(input, call_options())
        .await
        .map_err(|error| plane_session::map_error("PlanContentExecution", error))?;
    let execution = plan
        .execution
        .as_ref()
        .ok_or_else(|| GovernanceError::Message("content plan missing execution".into()))?;
    governance.update_harvest_plan(&handle.run_id, execution.plan_id.clone())?;
    if execution
        .budget
        .as_ref()
        .is_some_and(|budget| !budget.allowed)
    {
        governance.report_failed_model_event(handle).await?;
        return Err(GovernanceError::Denied(
            execution
                .budget
                .as_ref()
                .map(|budget| budget.reason.clone())
                .unwrap_or_else(|| "content budget denied".into()),
        ));
    }
    if !execution.executable {
        governance.report_failed_model_event(handle).await?;
        return Err(GovernanceError::Denied(
            execution
                .warnings
                .first()
                .cloned()
                .unwrap_or_else(|| "content plan is not executable".into()),
        ));
    }

    let planned_messages = plan
        .content_messages
        .iter()
        .map(local_message)
        .collect::<Result<Vec<_>, _>>()?;
    validate_messages(&planned_messages, capabilities)
        .map_err(|error| GovernanceError::Denied(error.to_string()))?;
    validate_planned_binding(messages, &planned_messages)?;
    let resolved = resolve_accepted(resolver, &planned_messages)
        .await
        .map_err(|error| GovernanceError::Denied(error.to_string()))?
        .into_iter()
        .map(|part| ResolvedContentPartV1 {
            descriptor: Some(proto_descriptor(&part.descriptor)),
            payload: Some(match part.payload {
                ResolvedContent::Text(text) => resolved_content_part_v1::Payload::Text(text),
                ResolvedContent::Bytes(bytes) => resolved_content_part_v1::Payload::Bytes(bytes),
            }),
        })
        .collect();

    let mut stream = match client
        .execute_content_plan_stream(plan, resolved, call_options())
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            governance.report_failed_model_event(handle).await?;
            return Err(plane_session::map_error("ExecuteContentPlanStream", error));
        }
    };
    let mut final_response = None;
    while let Some(event) = stream.next().await {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                governance.report_failed_model_event(handle).await?;
                return Err(plane_session::map_error("ExecuteContentPlanStream", error));
            }
        };
        if event.response.is_some() {
            final_response = event.response;
        }
        if event.done {
            break;
        }
    }
    let response = final_response
        .ok_or_else(|| GovernanceError::Message("missing content model response".into()))?;
    let chat = response
        .response
        .ok_or_else(|| GovernanceError::Message("missing content chat response".into()))?;
    Ok(ContentModelTurnV1 {
        text: chat.content,
        output_parts: response
            .output_parts
            .iter()
            .map(local_descriptor)
            .collect::<Result<Vec<_>, _>>()?,
        tool_calls: chat
            .tool_calls
            .into_iter()
            .map(|call| ToolCall {
                id: call.id,
                name: call.name,
                args_json: call.args_json,
            })
            .collect(),
        usage: None,
    })
}

fn validate_planned_binding(
    submitted: &[ContentMessageV1],
    planned: &[ContentMessageV1],
) -> Result<(), GovernanceError> {
    if submitted.len() != planned.len() {
        return Err(GovernanceError::Denied(
            "content plan changed the submitted message topology".into(),
        ));
    }
    for (submitted_message, planned_message) in submitted.iter().zip(planned) {
        if submitted_message.role != planned_message.role
            || submitted_message.tool_call_id != planned_message.tool_call_id
            || submitted_message.tool_calls != planned_message.tool_calls
            || submitted_message.parts.len() != planned_message.parts.len()
        {
            return Err(GovernanceError::Denied(
                "content plan changed submitted message metadata".into(),
            ));
        }
        for (submitted_part, planned_part) in
            submitted_message.parts.iter().zip(&planned_message.parts)
        {
            if submitted_part.part_id != planned_part.part_id
                || submitted_part.kind != planned_part.kind
                || submitted_part.media_type != planned_part.media_type
                || submitted_part.byte_length != planned_part.byte_length
                || submitted_part.sha256_digest != planned_part.sha256_digest
                || submitted_part.reference != planned_part.reference
                || submitted_part.provenance != planned_part.provenance
                || !valid_disclosure_transition(
                    submitted_part.disclosure_state,
                    planned_part.disclosure_state,
                )
            {
                return Err(GovernanceError::Denied(format!(
                    "content plan changed immutable binding for part `{}`",
                    submitted_part.part_id
                )));
            }
        }
    }
    Ok(())
}

fn valid_disclosure_transition(
    submitted: crate::content::ContentDisclosureState,
    planned: crate::content::ContentDisclosureState,
) -> bool {
    use crate::content::ContentDisclosureState::{Accepted, Omitted, Redacted};
    matches!(
        (submitted, planned),
        (Accepted, Accepted | Redacted | Omitted)
            | (Redacted, Redacted | Omitted)
            | (Omitted, Omitted)
    )
}

#[allow(clippy::too_many_arguments)]
fn content_execution_input(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    system: &str,
    messages: &[ContentMessageV1],
    tools: &[ToolDef],
    capabilities: &ContentCapabilitiesV1,
    request_id: &str,
) -> ContentExecutionInputV1 {
    ContentExecutionInputV1 {
        execution: Some(ExecutionInput {
            request_id: request_id.into(),
            namespace: handle.namespace.clone(),
            spec: "bounded content run".into(),
            preferred_model: governance.preferred_model.clone(),
            preferred_runtime: String::new(),
            task_type: "agent".into(),
            priority: 0,
            user_id: governance.principal.clone(),
            estimated_tokens: governance.max_tokens,
            messages: Vec::new(),
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
            task_class: "shikigami-content-run".into(),
            logical_operation_id: handle.operation_id.clone(),
            attempt_id: handle.run_id.clone(),
            route_override: String::new(),
        }),
        content_messages: messages.iter().map(proto_message).collect(),
        requested_capabilities: Some(ProtoCapabilities {
            contract_version: CONTENT_CONTRACT_VERSION.into(),
            input_kinds: capabilities
                .input_kinds
                .iter()
                .map(|kind| kind.proto_value())
                .collect(),
            output_kinds: capabilities
                .output_kinds
                .iter()
                .map(|kind| kind.proto_value())
                .collect(),
            media_types: capabilities.media_types.clone(),
            reference_modes: capabilities.reference_modes.clone(),
            max_parts: capabilities.max_parts,
            max_part_bytes: capabilities.max_part_bytes,
            max_aggregate_bytes: capabilities.max_aggregate_bytes,
            streaming: capabilities.streaming,
        }),
        disclosure_authority: sekai_client::CONTENT_DISCLOSURE_AUTHORITY.into(),
    }
}

fn proto_message(message: &ContentMessageV1) -> ProtoMessage {
    ProtoMessage {
        role: message.role.clone(),
        parts: message.parts.iter().map(proto_descriptor).collect(),
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
    }
}

fn proto_descriptor(descriptor: &ContentPartDescriptor) -> ProtoDescriptor {
    ProtoDescriptor {
        part_id: descriptor.part_id.clone(),
        kind: descriptor.kind.proto_value(),
        media_type: descriptor.media_type.clone(),
        byte_length: descriptor.byte_length,
        sha256_digest: descriptor.sha256_digest.clone(),
        reference: descriptor.reference.clone(),
        provenance: Some(ProtoProvenance {
            source: descriptor.provenance.source.clone(),
            source_id: descriptor.provenance.source_id.clone(),
            source_version: descriptor.provenance.source_version.clone(),
            observed_at_ms: descriptor.provenance.observed_at_ms,
        }),
        disclosure_state: descriptor.disclosure_state.proto_value(),
        disclosure_reason: descriptor.disclosure_reason.clone(),
    }
}

fn local_message(message: &ProtoMessage) -> Result<ContentMessageV1, GovernanceError> {
    Ok(ContentMessageV1 {
        role: message.role.clone(),
        parts: message
            .parts
            .iter()
            .map(local_descriptor)
            .collect::<Result<Vec<_>, _>>()?,
        tool_call_id: message.tool_call_id.clone(),
        tool_calls: message
            .tool_calls
            .iter()
            .map(|call| ToolCall {
                id: call.id.clone(),
                name: call.name.clone(),
                args_json: call.args_json.clone(),
            })
            .collect(),
    })
}

fn local_descriptor(
    descriptor: &ProtoDescriptor,
) -> Result<ContentPartDescriptor, GovernanceError> {
    let provenance = descriptor
        .provenance
        .as_ref()
        .ok_or_else(|| GovernanceError::Message("content provenance is missing".into()))?;
    Ok(ContentPartDescriptor {
        part_id: descriptor.part_id.clone(),
        kind: crate::content::ContentPartKind::from_proto(descriptor.kind)
            .map_err(|error| GovernanceError::Message(error.to_string()))?,
        media_type: descriptor.media_type.clone(),
        byte_length: descriptor.byte_length,
        sha256_digest: descriptor.sha256_digest.clone(),
        reference: descriptor.reference.clone(),
        provenance: crate::content::ContentProvenanceV1 {
            source: provenance.source.clone(),
            source_id: provenance.source_id.clone(),
            source_version: provenance.source_version.clone(),
            observed_at_ms: provenance.observed_at_ms,
        },
        disclosure_state: crate::content::ContentDisclosureState::from_proto(
            descriptor.disclosure_state,
        )
        .map_err(|error| GovernanceError::Message(error.to_string()))?,
        disclosure_reason: descriptor.disclosure_reason.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{
        ContentDisclosureState, ContentPartKind, ContentProvenanceV1, sha256_digest,
    };

    fn messages() -> Vec<ContentMessageV1> {
        vec![ContentMessageV1 {
            role: "user".into(),
            parts: vec![ContentPartDescriptor {
                part_id: "part-1".into(),
                kind: ContentPartKind::Text,
                media_type: "text/plain".into(),
                byte_length: 7,
                sha256_digest: sha256_digest(b"payload"),
                reference: "memory:part-1".into(),
                provenance: ContentProvenanceV1 {
                    source: "test".into(),
                    source_id: "part-1".into(),
                    source_version: "1".into(),
                    observed_at_ms: 1,
                },
                disclosure_state: ContentDisclosureState::Accepted,
                disclosure_reason: String::new(),
            }],
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }]
    }

    #[test]
    fn planned_content_may_only_reduce_disclosure() {
        let submitted = messages();
        let mut planned = submitted.clone();
        planned[0].parts[0].disclosure_state = ContentDisclosureState::Redacted;
        planned[0].parts[0].disclosure_reason = "policy".into();

        validate_planned_binding(&submitted, &planned).unwrap();

        let mut substituted = planned;
        substituted[0].parts[0].reference = "memory:part-2".into();
        assert!(validate_planned_binding(&submitted, &substituted).is_err());
    }
}
