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

/// Compact middle of the message list when over `threshold`, dropping it
/// behind a marker. Keeps the first message (task) and the last `keep_tail`
/// messages. If the tail would start on a tool result, it expands to the
/// assistant that issued those calls so providers still see a complete
/// exchange. Returns `(before, after)` when compaction ran.
pub fn compact_messages(
    messages: &mut Vec<ChatMessage>,
    threshold: usize,
    keep_tail: usize,
) -> Option<(usize, usize)> {
    let tail_start = compaction_tail_start(messages, threshold, keep_tail)?;
    let marker = dropped_marker(tail_start - 1);
    Some(replace_compacted_span(messages, tail_start, marker))
}

/// Where compaction cuts `messages`: `messages[1..tail_start]` is the
/// compacted span and `messages[tail_start..]` the verbatim tail.
fn compaction_tail_start(
    messages: &[ChatMessage],
    threshold: usize,
    keep_tail: usize,
) -> Option<usize> {
    let before = messages.len();
    if before <= threshold || before <= keep_tail + 1 {
        return None;
    }
    let tail_start = tool_exchange_tail_start(messages, before.saturating_sub(keep_tail).max(1));
    (tail_start > 1).then_some(tail_start)
}

/// Replace the compacted span with `marker`; returns `(before, after)`.
fn replace_compacted_span(
    messages: &mut Vec<ChatMessage>,
    tail_start: usize,
    marker: ChatMessage,
) -> (usize, usize) {
    let before = messages.len();
    messages.splice(1..tail_start, std::iter::once(marker));
    (before, messages.len())
}

/// Every compaction marker starts with `[context compacted:`; replay and
/// export detect compacted history by that prefix.
fn dropped_marker(dropped: usize) -> ChatMessage {
    user_message(format!(
        "[context compacted: {dropped} earlier messages omitted; continue the original task]"
    ))
}

fn summary_marker(summarized: usize, through_turn: u32, summary: &str) -> ChatMessage {
    user_message(format!(
        "[context compacted: {summarized} earlier messages through turn {through_turn} summarized; \
         the run checkpoint keeps the full record. Continue the original task.]\n\n{summary}"
    ))
}

fn user_message(content: String) -> ChatMessage {
    ChatMessage {
        role: "user".into(),
        content,
        tool_call_id: String::new(),
        tool_calls: vec![],
    }
}

/// Instructions for the compaction summarizer. The last sentence matters:
/// without it a model sometimes calls a tool instead of writing the summary.
const SUMMARY_PROMPT: &str = "You summarize part of an agent run so the agent can continue without it. \
Write the summary as plain text with these sections, each complete even at the cost of length:\n\
1. Task and constraints the user stated, kept exact.\n\
2. Decisions made and options ruled out, with the reasons.\n\
3. Problems hit and how each was resolved.\n\
4. Where the work stands now.\n\
5. What is still open.\n\
6. Details that are hard to reconstruct: paths, names, numbers, identifiers, exact values, kept exactly.\n\
Keep the user's words close to verbatim; condense the agent's own reasoning to what it concluded or produced. \
Do not call any tools; respond with text only.";

/// Longest text kept per message when rendering the compacted span for the
/// summarizer. The tail of a long message (a tool result, a log) is usually
/// the part that matters, so the head is cut.
const SUMMARY_MESSAGE_CHARS: usize = 8_000;

/// Consecutive summary failures after which a run stops asking for summaries
/// and only drops, so a model that cannot summarize is not asked every turn.
const SUMMARY_MISS_LIMIT: u32 = 3;

/// Render messages as one plain-text transcript for the summarizer. Tool
/// exchanges become text, so the request carries no tool definitions.
fn render_transcript(messages: &[ChatMessage]) -> String {
    let mut out = String::new();
    for message in messages {
        let text = tail_chars(&message.content, SUMMARY_MESSAGE_CHARS);
        match message.role.as_str() {
            "tool" => out.push_str(&format!(
                "[tool result {}]\n{text}\n\n",
                message.tool_call_id
            )),
            role => {
                out.push_str(&format!("[{role}]\n{text}\n"));
                for call in &message.tool_calls {
                    out.push_str(&format!(
                        "[tool call {} {}] {}\n",
                        call.id,
                        call.name,
                        tail_chars(&call.args_json, SUMMARY_MESSAGE_CHARS)
                    ));
                }
                out.push('\n');
            }
        }
    }
    out
}

fn tail_chars(text: &str, limit: usize) -> std::borrow::Cow<'_, str> {
    let count = text.chars().count();
    if count <= limit {
        return text.into();
    }
    let skip = count - limit;
    format!(
        "[... {skip} characters cut ...]{}",
        text.chars().skip(skip).collect::<String>()
    )
    .into()
}

fn is_tool_result(msg: &ChatMessage) -> bool {
    msg.role == "tool"
}

fn assistant_has_tool_calls(msg: &ChatMessage) -> bool {
    msg.role == "assistant" && !msg.tool_calls.is_empty()
}

fn tool_exchange_tail_start(messages: &[ChatMessage], proposed: usize) -> usize {
    if proposed >= messages.len() || !is_tool_result(&messages[proposed]) {
        return proposed;
    }
    let mut start = proposed;
    while start > 1 && is_tool_result(&messages[start]) {
        start -= 1;
    }
    if assistant_has_tool_calls(&messages[start]) {
        return start;
    }
    let mut start = proposed;
    while start < messages.len() && is_tool_result(&messages[start]) {
        start += 1;
    }
    start
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
    /// `session.turns` when this prompt's loop started. Session hosts apply
    /// `max_turns` to the delta; unattended runs keep the lifetime cap.
    prompt_start_turns: u32,
    /// Consecutive compactions whose summary failed, in this process. A
    /// restart resets it, which costs at most [`SUMMARY_MISS_LIMIT`] more
    /// summary attempts.
    summary_misses: u32,
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
            prompt_start_turns: if request.session_wait {
                session.prompt_start_turns.unwrap_or(session.turns)
            } else {
                0
            },
            summary_misses: 0,
        }
    }

    /// Surface each transient-failure retry the local model made for this turn,
    /// whether the turn then succeeded or failed.
    fn emit_model_retries(&self, run_id: &str, notes: Vec<String>) {
        for status in notes {
            self.engine.emit(run_id, HarnessEvent::Status { status });
        }
    }

    /// Replace `session.messages[1..tail_start]` with a model-written summary,
    /// or drop it behind a marker when no summary is available. The new list
    /// becomes durable with the next checkpoint save; a crash before that
    /// resumes from the full list and compacts again.
    async fn compact(
        &mut self,
        session: &mut RunSession,
        tail_start: usize,
    ) -> Result<(), RunError> {
        let span = &session.messages[1..tail_start];
        let summary = self.summarize_span(session, span).await?;
        let summarized = tail_start - 1;
        let event = match summary {
            Some(summary) => {
                let tail_turns = session.messages[tail_start..]
                    .iter()
                    .filter(|message| message.role == "assistant")
                    .count();
                let through_turn = session
                    .turns
                    .saturating_sub(u32::try_from(tail_turns).unwrap_or(u32::MAX));
                let marker = summary_marker(summarized, through_turn, &summary);
                let (before, after) =
                    replace_compacted_span(&mut session.messages, tail_start, marker);
                HarnessEvent::ContextSummarized {
                    before,
                    after,
                    through_turn,
                }
            }
            None => {
                let (before, after) = replace_compacted_span(
                    &mut session.messages,
                    tail_start,
                    dropped_marker(summarized),
                );
                HarnessEvent::ContextCompacted { before, after }
            }
        };
        self.engine.emit(&session.run_id, event);
        Ok(())
    }

    /// Ask the local model to summarize `span`. `None` means compact without
    /// a summary: the governance adapter plans remotely, recent summaries kept
    /// failing, or this one failed. A cancelled or timed-out run still stops.
    async fn summarize_span(
        &mut self,
        session: &RunSession,
        span: &[ChatMessage],
    ) -> Result<Option<String>, RunError> {
        if !self.engine.governance.plans_with_local_model()
            || self.summary_misses >= SUMMARY_MISS_LIMIT
        {
            return Ok(None);
        }
        let transcript = render_transcript(span);
        let result = super::supervision::run_until_cancelled(
            self.engine,
            &session.run_id,
            self.request,
            self.started,
            self.timeout,
            &session.parent_run_id,
            self.engine.model.summarize(SUMMARY_PROMPT, &transcript),
        )
        .await?;
        let failure = match result {
            Ok(turn) => {
                if let Some(usage) = turn.usage {
                    self.usage.input_tokens =
                        self.usage.input_tokens.saturating_add(usage.input_tokens);
                    self.usage.output_tokens =
                        self.usage.output_tokens.saturating_add(usage.output_tokens);
                }
                let summary = turn.content.trim();
                if !turn.tool_calls.is_empty() {
                    "the summarizer called a tool".to_owned()
                } else if summary.is_empty() {
                    "the summary was empty".to_owned()
                } else if summary.len() >= transcript.len() {
                    "the summary was not shorter than the messages it replaces".to_owned()
                } else {
                    self.summary_misses = 0;
                    return Ok(Some(summary.to_owned()));
                }
            }
            Err(error) => error.to_string(),
        };
        self.summary_misses += 1;
        self.engine.emit(
            &session.run_id,
            HarnessEvent::Status {
                status: format!("context summary unavailable ({failure}); compacting without it"),
            },
        );
        Ok(None)
    }

    /// Return one model turn only after its result and report cursor are durable.
    pub(super) async fn next(&mut self, session: &mut RunSession) -> Result<ModelTurn, RunError> {
        check_bounds(
            self.engine,
            &session.run_id,
            self.request,
            self.started,
            self.timeout,
            &session.parent_run_id,
        )?;

        if self.staged_turn.is_none() && self.staged_content_turn.is_none() {
            let used = if self.request.session_wait {
                session.turns.saturating_sub(self.prompt_start_turns)
            } else {
                session.turns
            };
            if used >= self.engine.config.run.max_turns {
                return Err(RunError::MaxTurns(self.engine.config.run.max_turns));
            }
        }
        if self.staged_turn.is_none()
            && self.staged_content_turn.is_none()
            && !session.is_content()
            && let Some(threshold) = self.engine.config.run.compact_after_messages
        {
            let keep = self.engine.config.run.compact_keep_tail.max(2) as usize;
            if let Some(tail_start) =
                compaction_tail_start(&session.messages, threshold as usize, keep)
            {
                self.compact(session, tail_start).await?;
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
            let (planned, retries) =
                crate::model::collect_retry_notes(self.engine.governance.plan_content_turn(
                    self.handle,
                    self.system_prompt,
                    ContentTurnContext {
                        messages: &messages,
                        tools: self.tool_defs,
                        capabilities: &capabilities,
                        resolver: resolver.as_ref(),
                        local_model: self.engine.model.as_ref(),
                    },
                ))
                .await;
            self.emit_model_retries(&session.run_id, retries);
            let turn = planned?;
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
            let plan = crate::model::collect_retry_notes(self.engine.governance.plan_turn(
                self.handle,
                self.system_prompt,
                &session.messages,
                self.tool_defs,
                self.engine.model.as_ref(),
            ));
            let (planned, retries) = if self.engine.events.wants_text_deltas() {
                // A streamed turn shows text early, so it can also be cancelled
                // early. Dropping it leaves the checkpoint at the last durable
                // turn, which a resume plans again.
                let events = std::sync::Arc::clone(&self.engine.events);
                let turn = session.turns + 1;
                let run_id = session.run_id.clone();
                let sink = std::sync::Arc::new(move |text: &str| {
                    events.emit(HarnessEvent::AssistantDelta {
                        run_id: run_id.clone(),
                        turn,
                        text: text.to_owned(),
                    });
                });
                super::supervision::run_until_cancelled(
                    self.engine,
                    &session.run_id,
                    self.request,
                    self.started,
                    self.timeout,
                    &session.parent_run_id,
                    crate::model::stream_text_deltas(sink, plan),
                )
                .await?
            } else {
                plan.await
            };
            self.emit_model_retries(&session.run_id, retries);
            let turn = planned?;
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
            &session.parent_run_id,
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

    #[test]
    fn compact_messages_keeps_complete_tool_exchange() {
        let mut msgs = vec![
            ChatMessage {
                role: "user".into(),
                content: "task".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
            ChatMessage {
                role: "assistant".into(),
                content: "a1".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
            ChatMessage {
                role: "user".into(),
                content: "u2".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "bash".into(),
                    args_json: r#"{"command":"echo hi"}"#.into(),
                }],
            },
            ChatMessage {
                role: "tool".into(),
                content: "hi".into(),
                tool_call_id: "c1".into(),
                tool_calls: vec![],
            },
            ChatMessage {
                role: "assistant".into(),
                content: "done".into(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            },
        ];
        let (before, after) = compact_messages(&mut msgs, 0, 2).unwrap();
        assert_eq!(before, 6);
        assert!(after < before);
        let tool_idx = msgs.iter().position(|m| m.role == "tool").unwrap();
        assert!(
            tool_idx > 0 && !msgs[tool_idx - 1].tool_calls.is_empty(),
            "tail must keep the assistant call with its tool result: {msgs:?}"
        );
        assert_eq!(msgs[tool_idx - 1].tool_calls[0].id, "c1");
    }
}
