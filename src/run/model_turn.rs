//! Durable planning and reporting for one model-produced turn.

use std::sync::Arc;
use std::time::Duration;

use crate::checkpoint::GovernanceCheckpoint;
use crate::events::HarnessEvent;
use crate::governance::RunHandle;
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
        governance_checkpoint: Option<&GovernanceCheckpoint>,
        session: &RunSession,
    ) -> Self {
        let governed_model_checkpoint = request.resume_run_id.is_some()
            && governance_checkpoint
                .is_some_and(|checkpoint| !checkpoint.model_operation_id.is_empty());
        let staged_turn = staged_model_turn(governed_model_checkpoint, &session.messages);
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
            usage: TokenUsage::default(),
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

        if self.staged_turn.is_none() && session.turns >= self.engine.config.run.max_turns {
            return Err(RunError::MaxTurns(self.engine.config.run.max_turns));
        }
        if self.staged_turn.is_none()
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

        let turn = if let Some(turn) = self.staged_turn.take() {
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
        Ok(turn)
    }

    pub(super) fn usage(&self) -> TokenUsage {
        self.usage
    }
}

fn staged_model_turn(
    governed_model_checkpoint: bool,
    messages: &[ChatMessage],
) -> Option<ModelTurn> {
    if !governed_model_checkpoint {
        return None;
    }
    let message = messages
        .last()
        .filter(|message| message.role == "assistant")?;
    Some(ModelTurn {
        content: message.content.clone(),
        tool_calls: message.tool_calls.clone(),
        usage: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ToolCall;

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

        let turn = staged_model_turn(true, &messages).unwrap();

        assert_eq!(turn.content, "continue");
        assert_eq!(turn.tool_calls, messages[0].tool_calls);
        assert_eq!(turn.usage, None);
    }

    #[test]
    fn post_tool_checkpoint_continues_with_a_fresh_model_turn() {
        let messages = vec![ChatMessage {
            role: "tool".into(),
            content: "result".into(),
            tool_call_id: "call-1".into(),
            tool_calls: vec![],
        }];

        assert!(staged_model_turn(true, &messages).is_none());
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
