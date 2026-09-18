//! Durable planning and reporting for one model-produced turn.

use std::sync::Arc;
use std::time::Duration;

use crate::checkpoint::GovernanceCheckpoint;
use crate::content::{
    ContentDisclosureState, ContentMessageV1, ContentModelTurnV1, ContentPartKind, ResolvedContent,
    tool_arguments_part_id,
};
use crate::events::HarnessEvent;
use crate::governance::{ContentTurnContext, RunHandle};
use crate::model::{ChatMessage, ModelTurn, TokenUsage};
use crate::tools::{ToolDef, ToolRegistry};

use super::session::RunSession;
use super::supervision::check_bounds;
use super::{Engine, RunError, RunRequest};

/// Compact middle of the message list when over `threshold`.
/// Keeps the first message (task) and the last `keep_tail` messages.
/// Returns `(before, after)` when compaction ran.
pub fn compact_messages(
    messages: &mut Vec<ChatMessage>,
    threshold: usize,
    keep_tail: usize,
) -> Option<(usize, usize)> {
    let before = messages.len();
    if before <= threshold || before <= keep_tail + 1 {
        return None;
    }
    let head = messages.first().cloned()?;
    let tail_start = before.saturating_sub(keep_tail);
    let tail: Vec<ChatMessage> = messages[tail_start..].to_vec();
    let dropped = before.saturating_sub(1 + tail.len());
    let summary = ChatMessage {
        role: "user".into(),
        content: format!(
            "[context compacted: {dropped} earlier messages omitted; continue the original task]"
        ),
        tool_call_id: String::new(),
        tool_calls: vec![],
    };
    *messages = std::iter::once(head)
        .chain(std::iter::once(summary))
        .chain(tail)
        .collect();
    Some((before, messages.len()))
}

/// Deep private module that owns the durable protocol around model planning.
pub(super) struct DurableModelTurn<'a> {
    engine: &'a Engine,
    request: &'a RunRequest,
    started: tokio::time::Instant,
    timeout: Option<Duration>,
    handle: &'a RunHandle,
    system_prompt: &'a str,
    tool_defs: &'a [ToolDef],
    tools: Arc<ToolRegistry>,
    staged_turn: Option<ModelTurn>,
    staged_content_turn: Option<ContentModelTurnV1>,
    usage: TokenUsage,
}

impl<'a> DurableModelTurn<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        engine: &'a Engine,
        request: &'a RunRequest,
        started: tokio::time::Instant,
        timeout: Option<Duration>,
        handle: &'a RunHandle,
        system_prompt: &'a str,
        tool_defs: &'a [ToolDef],
        tools: Arc<ToolRegistry>,
        _governance_checkpoint: Option<&GovernanceCheckpoint>,
        session: &RunSession,
        replay: bool,
    ) -> Self {
        // A stopped run resumes from the durable assistant message. Local
        // checkpoints are not plane receipts; they still prevent repeating a
        // paid or already-planned model turn after abrupt termination.
        let staged_turn = staged_model_turn(
            request.resume_run_id.is_some(),
            &session.messages,
            session.turns,
        );
        let staged_content_checkpoint =
            request.resume_run_id.is_some() && session.has_staged_content_model_turn();
        let staged_content_turn = session
            .content_messages()
            .as_deref()
            .and_then(|messages| staged_content_model_turn(staged_content_checkpoint, messages));
        Self {
            engine,
            request,
            started,
            timeout,
            handle,
            system_prompt,
            tool_defs,
            tools,
            staged_turn,
            staged_content_turn,
            usage: if session.is_content() {
                session.content_usage()
            } else if replay {
                session.replay_usage()
            } else {
                TokenUsage::default()
            },
        }
    }

    /// Return one model turn only after its result and report cursor are durable.
    pub(super) async fn next(&mut self, session: &mut RunSession) -> Result<ModelTurn, RunError> {
        check_bounds(
            self.engine,
            &session.run_id,
            self.request,
            self.started,
            self.timeout,
        )?;

        if self.staged_turn.is_none()
            && self.staged_content_turn.is_none()
            && session.turns >= self.engine.config.run.max_turns
        {
            return Err(RunError::MaxTurns(self.engine.config.run.max_turns));
        }
        if self.staged_turn.is_none()
            && self.staged_content_turn.is_none()
            && !session.is_content()
            && let Some(threshold) = self.engine.config.run.compact_after_messages
        {
            let keep = self.engine.config.run.compact_keep_tail.max(2) as usize;
            if let Some((before, after)) =
                compact_messages(&mut session.messages, threshold as usize, keep)
            {
                self.engine.emit(
                    &session.run_id,
                    HarnessEvent::ContextCompacted { before, after },
                );
            }
        }
        self.engine.emit(
            &session.run_id,
            HarnessEvent::Status {
                status: "planning".into(),
            },
        );
        session.spans.start_turn(session.turns.saturating_add(1));

        let turn = if let Some(turn) = self.staged_content_turn.take() {
            let resolver = session
                .content_context()
                .map(|(_, _, resolver)| resolver)
                .ok_or_else(|| RunError::Message("content resolver is unavailable".into()))?;
            self.engine.report_governance_model(self.handle).await?;
            session.save(self.tools.as_ref())?;
            content_to_model_turn(turn, resolver.as_ref()).await?
        } else if session.is_content() {
            let (messages, capabilities, resolver) = session
                .content_context()
                .ok_or_else(|| RunError::Message("content session is unavailable".into()))?;
            let turn = self
                .engine
                .governance
                .plan_content_turn(
                    self.handle,
                    self.system_prompt,
                    ContentTurnContext {
                        messages: &messages,
                        tools: self.tool_defs,
                        capabilities: &capabilities,
                        resolver: resolver.as_ref(),
                        local_model: self.engine.model.as_ref(),
                    },
                )
                .await?;
            session.turns += 1;
            if let Some(usage) = turn.usage {
                self.usage.input_tokens =
                    self.usage.input_tokens.saturating_add(usage.input_tokens);
                self.usage.output_tokens =
                    self.usage.output_tokens.saturating_add(usage.output_tokens);
            }
            session.set_content_usage(self.usage);
            let model_turn = ModelTurn {
                content: turn.text.clone(),
                tool_calls: turn.tool_calls.clone(),
                usage: turn.usage,
            };
            session.append_content_model_turn(&turn).await?;
            let content_message = session
                .content_messages()
                .and_then(|messages| messages.last().cloned())
                .ok_or_else(|| RunError::Message("content model result was not staged".into()))?;
            self.engine.emit(
                &session.run_id,
                HarnessEvent::ContentTurn {
                    turn: session.turns,
                    part_count: content_message.parts.len(),
                    kinds: content_message
                        .parts
                        .iter()
                        .map(|part| format!("{:?}", part.kind).to_ascii_lowercase())
                        .collect(),
                    digests: content_message
                        .parts
                        .iter()
                        .map(|part| part.sha256_digest.clone())
                        .collect(),
                },
            );
            session.messages.push(ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: content_message.tool_calls,
            });
            session.save(self.tools.as_ref())?;
            self.engine.report_governance_model(self.handle).await?;
            session.save(self.tools.as_ref())?;
            model_turn
        } else if let Some(turn) = self.staged_turn.take() {
            self.engine.report_governance_model(self.handle).await?;
            session.save(self.tools.as_ref())?;
            turn
        } else {
            let turn = self
                .engine
                .governance
                .plan_turn(
                    self.handle,
                    self.system_prompt,
                    &session.messages,
                    self.tool_defs,
                    self.engine.model.as_ref(),
                )
                .await?;
            session.turns += 1;
            if let Some(usage) = turn.usage {
                self.usage.input_tokens =
                    self.usage.input_tokens.saturating_add(usage.input_tokens);
                self.usage.output_tokens =
                    self.usage.output_tokens.saturating_add(usage.output_tokens);
            }
            session.set_replay_usage(self.usage);
            self.engine.emit(
                &session.run_id,
                HarnessEvent::ModelTurn {
                    turn: session.turns,
                    content_preview: turn.content.chars().take(200).collect(),
                },
            );
            session.messages.push(ChatMessage {
                role: "assistant".into(),
                content: turn.content.clone(),
                tool_call_id: String::new(),
                tool_calls: turn.tool_calls.clone(),
            });
            session.save(self.tools.as_ref())?;
            self.engine.report_governance_model(self.handle).await?;
            session.save(self.tools.as_ref())?;
            turn
        };

        // A stopped run resumes from the durable assistant result instead of
        // repeating a paid or governed model call.
        check_bounds(
            self.engine,
            &session.run_id,
            self.request,
            self.started,
            self.timeout,
        )?;
        session.spans.end_turn();
        Ok(turn)
    }

    pub(super) fn usage(&self) -> TokenUsage {
        self.usage
    }
}

async fn content_to_model_turn(
    turn: ContentModelTurnV1,
    resolver: &dyn crate::content::ContentResolver,
) -> Result<ModelTurn, RunError> {
    let mut text = turn.text;
    if text.is_empty() {
        for descriptor in &turn.output_parts {
            if descriptor.kind == ContentPartKind::Text
                && descriptor.disclosure_state == ContentDisclosureState::Accepted
                && descriptor.provenance.source != "model-tool-arguments"
            {
                match resolver.resolve(descriptor).await.map_err(|_| {
                    RunError::Message(format!(
                        "content resolver failed for staged part `{}`",
                        descriptor.part_id
                    ))
                })? {
                    ResolvedContent::Text(value) => {
                        text = value;
                        break;
                    }
                    ResolvedContent::Bytes(_) => {
                        return Err(RunError::Message(
                            "text descriptor resolved to bytes".into(),
                        ));
                    }
                }
            }
        }
    }
    let mut tool_calls = turn.tool_calls;
    for call in &mut tool_calls {
        let part_id = tool_arguments_part_id(&call.args_json).ok_or_else(|| {
            RunError::Message("content tool arguments are not externalized".into())
        })?;
        let descriptor = turn
            .output_parts
            .iter()
            .find(|descriptor| {
                descriptor.part_id == part_id
                    && descriptor.kind == ContentPartKind::Text
                    && descriptor.provenance.source == "model-tool-arguments"
                    && descriptor.disclosure_state == ContentDisclosureState::Accepted
            })
            .ok_or_else(|| {
                RunError::Message("content tool argument descriptor is missing".into())
            })?;
        let payload = resolver.resolve(descriptor).await.map_err(|_| {
            RunError::Message(format!(
                "content resolver failed for staged part `{}`",
                descriptor.part_id
            ))
        })?;
        crate::content::validate_resolved(descriptor, &payload)
            .map_err(|error| RunError::Message(error.to_string()))?;
        call.args_json = match payload {
            ResolvedContent::Text(value) => value,
            ResolvedContent::Bytes(_) => {
                return Err(RunError::Message(
                    "content tool arguments resolved to bytes".into(),
                ));
            }
        };
    }
    Ok(ModelTurn {
        content: text,
        tool_calls,
        usage: turn.usage,
    })
}

fn staged_model_turn(
    governed_model_checkpoint: bool,
    messages: &[ChatMessage],
    turn: u32,
) -> Option<ModelTurn> {
    if !governed_model_checkpoint {
        return None;
    }
    if messages
        .last()
        .is_some_and(|message| message.role == "assistant")
    {
        let message = messages.last()?;
        return Some(ModelTurn {
            content: message.content.clone(),
            tool_calls: message.tool_calls.clone(),
            usage: None,
        });
    }
    let assistant_idx = messages
        .iter()
        .rposition(|message| message.role == "assistant")?;
    let message = &messages[assistant_idx];
    let results: Vec<&str> = messages[assistant_idx + 1..]
        .iter()
        .filter(|message| message.role == "tool")
        .map(|message| message.tool_call_id.as_str())
        .collect();
    let has_unanswered = message.tool_calls.iter().enumerate().any(|(index, call)| {
        !super::tool_batch::tool_result_answers_call(
            &results,
            call,
            turn,
            index,
            &message.tool_calls,
        )
    });
    if !has_unanswered {
        return None;
    }
    Some(ModelTurn {
        content: message.content.clone(),
        tool_calls: message.tool_calls.clone(),
        usage: None,
    })
}

fn staged_content_model_turn(
    governed_model_checkpoint: bool,
    messages: &[ContentMessageV1],
) -> Option<ContentModelTurnV1> {
    if !governed_model_checkpoint {
        return None;
    }
    if messages
        .last()
        .is_some_and(|message| message.role == "assistant")
    {
        let message = messages.last()?;
        return Some(ContentModelTurnV1 {
            text: String::new(),
            output_parts: message.parts.clone(),
            tool_calls: message.tool_calls.clone(),
            usage: None,
        });
    }
    let assistant_idx = messages
        .iter()
        .rposition(|message| message.role == "assistant")?;
    let message = &messages[assistant_idx];
    let answered: std::collections::HashSet<&str> = messages[assistant_idx + 1..]
        .iter()
        .filter(|message| message.role == "tool")
        .map(|message| message.tool_call_id.as_str())
        .collect();
    let has_unanswered = message
        .tool_calls
        .iter()
        .any(|call| !answered.contains(call.id.as_str()));
    if !has_unanswered {
        return None;
    }
    Some(ContentModelTurnV1 {
        text: String::new(),
        output_parts: message.parts.clone(),
        tool_calls: message.tool_calls.clone(),
        usage: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ToolCall;

    #[test]
    fn resume_reuses_a_checkpointed_assistant_turn_without_a_plane_model_id() {
        let messages = vec![ChatMessage {
            role: "assistant".into(),
            content: String::new(),
            tool_call_id: String::new(),
            tool_calls: vec![ToolCall {
                id: "call-1".into(),
                name: "write_file".into(),
                args_json: r#"{"path":"once.txt","content":"once"}"#.into(),
            }],
        }];

        let turn = staged_model_turn(true, &messages, 1).unwrap();
        assert_eq!(turn.tool_calls, messages[0].tool_calls);
        assert!(staged_model_turn(false, &messages, 1).is_none());
    }

    #[test]
    fn staged_replay_reconstructs_the_durable_assistant_turn() {
        let messages = vec![ChatMessage {
            role: "assistant".into(),
            content: "continue".into(),
            tool_call_id: String::new(),
            tool_calls: vec![ToolCall {
                id: "call-1".into(),
                name: "read_file".into(),
                args_json: r#"{"path":"README.md"}"#.into(),
            }],
        }];

        let turn = staged_model_turn(true, &messages, 1).unwrap();

        assert_eq!(turn.content, "continue");
        assert_eq!(turn.tool_calls, messages[0].tool_calls);
        assert_eq!(turn.usage, None);
    }

    #[test]
    fn unfinished_batch_after_legacy_empty_ids_restages_remaining_calls() {
        let messages = vec![
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![
                    ToolCall {
                        id: String::new(),
                        name: "read_file".into(),
                        args_json: r#"{"path":"missing.txt"}"#.into(),
                    },
                    ToolCall {
                        id: String::new(),
                        name: "write_file".into(),
                        args_json: r#"{"path":"hello.txt","content":"approved"}"#.into(),
                    },
                ],
            },
            ChatMessage {
                role: "tool".into(),
                content: "missing".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
        ];
        let turn = staged_model_turn(true, &messages, 1).unwrap();
        assert_eq!(turn.tool_calls.len(), 2);
        assert_eq!(turn.tool_calls[1].name, "write_file");
    }

    #[test]
    fn completed_empty_id_batch_does_not_restage() {
        let messages = vec![
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![ToolCall {
                    id: String::new(),
                    name: "read_file".into(),
                    args_json: r#"{"path":"missing.txt"}"#.into(),
                }],
            },
            ChatMessage {
                role: "tool".into(),
                content: "missing".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
        ];
        assert!(staged_model_turn(true, &messages, 1).is_none());
    }

    #[test]
    fn reused_provider_ids_do_not_mark_later_calls_answered() {
        let messages = vec![
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![
                    ToolCall {
                        id: "write-1".into(),
                        name: "read_file".into(),
                        args_json: r#"{"path":"missing.txt"}"#.into(),
                    },
                    ToolCall {
                        id: "write-1".into(),
                        name: "write_file".into(),
                        args_json: r#"{"path":"hello.txt","content":"approved"}"#.into(),
                    },
                ],
            },
            ChatMessage {
                role: "tool".into(),
                content: "missing".into(),
                tool_call_id: "write-1".into(),
                tool_calls: vec![],
            },
        ];
        let turn = staged_model_turn(true, &messages, 1).unwrap();
        assert_eq!(turn.tool_calls.len(), 2);
        assert_eq!(turn.tool_calls[1].name, "write_file");
    }

    #[test]
    fn unfinished_batch_after_prefix_results_restages_remaining_calls() {
        let messages = vec![
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![
                    ToolCall {
                        id: "read-1".into(),
                        name: "read_file".into(),
                        args_json: r#"{"path":"missing.txt"}"#.into(),
                    },
                    ToolCall {
                        id: "write-1".into(),
                        name: "write_file".into(),
                        args_json: r#"{"path":"hello.txt","content":"approved"}"#.into(),
                    },
                ],
            },
            ChatMessage {
                role: "tool".into(),
                content: "missing".into(),
                tool_call_id: "read-1".into(),
                tool_calls: vec![],
            },
        ];
        let turn = staged_model_turn(true, &messages, 1).unwrap();
        assert_eq!(turn.tool_calls.len(), 2);
        assert_eq!(turn.tool_calls[1].id, "write-1");
    }

    #[test]
    fn last_assistant_without_tools_is_restaged() {
        let messages = vec![ChatMessage {
            role: "assistant".into(),
            content: "done".into(),
            tool_call_id: String::new(),
            tool_calls: vec![],
        }];
        let turn = staged_model_turn(true, &messages, 1).unwrap();
        assert_eq!(turn.content, "done");
        assert!(turn.tool_calls.is_empty());
    }

    #[test]
    fn post_tool_checkpoint_continues_with_a_fresh_model_turn() {
        let messages = vec![ChatMessage {
            role: "tool".into(),
            content: "result".into(),
            tool_call_id: "call-1".into(),
            tool_calls: vec![],
        }];

        assert!(staged_model_turn(true, &messages, 1).is_none());
    }

    #[test]
    fn compact_messages_shrinks_list() {
        let mut msgs: Vec<ChatMessage> = (0..20)
            .map(|i| ChatMessage {
                role: if i == 0 { "user" } else { "assistant" }.into(),
                content: format!("m{i}"),
                tool_call_id: String::new(),
                tool_calls: vec![],
            })
            .collect();
        let (before, after) = compact_messages(&mut msgs, 10, 4).unwrap();
        assert_eq!(before, 20);
        assert!(after < before);
        assert_eq!(msgs[0].content, "m0");
        assert!(msgs[1].content.contains("compacted"));
        assert_eq!(msgs.last().unwrap().content, "m19");
    }
}
