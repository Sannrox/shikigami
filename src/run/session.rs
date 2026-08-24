//! Durable attempt state behind a deep checkpoint interface.
//!
//! Callers mutate conversation progress on this session and ask it to persist.
//! They do not restate messages, turns, todos, workspace, or retention at each
//! durability point.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use crate::checkpoint::{self, Checkpoint, ParkedState};
use crate::content::{
    ContentCapabilitiesV1, ContentCheckpointBinding, ContentCheckpointV1, ContentDisclosureState,
    ContentError, ContentMessageV1, ContentModelTurnV1, ContentResolver, ContentTerminalCheckpoint,
    load_sidecar, resolve_accepted, resolve_terminal_summary, save_sidecar, store_text,
    tool_arguments_pointer, validate_descriptor, validate_messages,
};
use crate::governance::GovernancePort;
use crate::model::{ChatMessage, TokenUsage};
use crate::replay::{ReplayCheckpoint, ReplayTerminalCheckpoint};
use crate::tools::ToolRegistry;

use super::{RunError, RunTermination, SYSTEM_PROMPT};

/// Owned run progress + checkpoint retention policy for one engine attempt.
///
/// Deepens the earlier borrow-based `CheckpointSession` by also owning the
/// conversation fields that every save had to restate.
pub(super) struct RunSession {
    state_runs: PathBuf,
    governance: Arc<dyn GovernancePort>,
    pub run_id: String,
    pub task: String,
    pub workspace: PathBuf,
    workspace_adapter: String,
    pub keep_workspace: bool,
    pub messages: Vec<ChatMessage>,
    pub turns: u32,
    replay: Option<ReplayCheckpoint>,
    content: Option<ContentSession>,
}

struct ContentSession {
    resolver: Arc<dyn ContentResolver>,
    capabilities: ContentCapabilitiesV1,
    messages: Vec<ContentMessageV1>,
    initial_message_count: u32,
    binding: Option<ContentCheckpointBinding>,
    terminal: Option<ContentTerminalCheckpoint>,
    usage: TokenUsage,
}

pub(super) struct ContentResumeState {
    pub initial_message_count: u32,
    pub binding: Option<ContentCheckpointBinding>,
    pub terminal: Option<ContentTerminalCheckpoint>,
    pub usage: TokenUsage,
}

impl RunSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state_runs: impl Into<PathBuf>,
        governance: Arc<dyn GovernancePort>,
        run_id: impl Into<String>,
        task: impl Into<String>,
        workspace: impl Into<PathBuf>,
        workspace_adapter: impl Into<String>,
        keep_workspace: bool,
        messages: Vec<ChatMessage>,
        turns: u32,
    ) -> Self {
        Self {
            state_runs: state_runs.into(),
            governance,
            run_id: run_id.into(),
            task: task.into(),
            workspace: workspace.into(),
            workspace_adapter: workspace_adapter.into(),
            keep_workspace,
            messages,
            turns,
            replay: None,
            content: None,
        }
    }

    pub fn set_content(
        &mut self,
        resolver: Arc<dyn ContentResolver>,
        capabilities: ContentCapabilitiesV1,
        messages: Vec<ContentMessageV1>,
        state: ContentResumeState,
    ) -> Result<(), RunError> {
        validate_messages(&messages, &capabilities)
            .map_err(|error| RunError::Message(error.to_string()))?;
        if resolver.id().trim().is_empty() || resolver.id().len() > 256 {
            return Err(RunError::Message(
                "content resolver id must be bounded non-empty text".into(),
            ));
        }
        if state
            .binding
            .as_ref()
            .is_some_and(|binding| binding.resolver_id != resolver.id())
        {
            return Err(RunError::Message(
                "content resolver binding changed on resume".into(),
            ));
        }
        if state.initial_message_count == 0 || state.initial_message_count as usize > messages.len()
        {
            return Err(RunError::Message(
                "content initial message boundary is invalid".into(),
            ));
        }
        self.content = Some(ContentSession {
            resolver,
            capabilities,
            messages,
            initial_message_count: state.initial_message_count,
            binding: state.binding,
            terminal: state.terminal,
            usage: state.usage,
        });
        Ok(())
    }

    pub fn is_content(&self) -> bool {
        self.content.is_some()
    }

    pub fn content_context(
        &self,
    ) -> Option<(
        Vec<ContentMessageV1>,
        ContentCapabilitiesV1,
        Arc<dyn ContentResolver>,
    )> {
        self.content.as_ref().map(|content| {
            (
                content.messages.clone(),
                content.capabilities.clone(),
                Arc::clone(&content.resolver),
            )
        })
    }

    pub fn content_messages(&self) -> Option<Vec<ContentMessageV1>> {
        self.content
            .as_ref()
            .map(|content| content.messages.clone())
    }

    pub fn has_staged_content_model_turn(&self) -> bool {
        self.content.as_ref().is_some_and(|content| {
            has_staged_content_model_turn(
                self.turns,
                content.initial_message_count,
                &content.messages,
            )
        })
    }

    pub fn durable_content_tool_arguments(&self, index: usize) -> Option<String> {
        self.content
            .as_ref()?
            .messages
            .last()?
            .tool_calls
            .get(index)
            .map(|call| call.args_json.clone())
    }

    pub async fn recover_content_terminal(
        &self,
    ) -> Result<Option<(ContentTerminalCheckpoint, String)>, RunError> {
        let Some(content) = &self.content else {
            return Ok(None);
        };
        let Some(terminal) = content.terminal.clone() else {
            return Ok(None);
        };
        let sidecar = ContentCheckpointV1 {
            schema_version: crate::content::CONTENT_SCHEMA_VERSION,
            run_id: self.run_id.clone(),
            generation: content
                .binding
                .as_ref()
                .map_or(0, |binding| binding.generation),
            resolver_id: content.resolver.id().into(),
            capabilities: content.capabilities.clone(),
            messages: content.messages.clone(),
            initial_message_count: content.initial_message_count,
            completed_turns: self.turns,
            usage: content.usage,
            terminal: Some(terminal.clone()),
        };
        let summary = resolve_terminal_summary(content.resolver.as_ref(), &sidecar)
            .await
            .map_err(content_run_error)?
            .ok_or_else(|| RunError::Message("content terminal summary is missing".into()))?;
        Ok(Some((terminal, summary)))
    }

    pub fn mark_content_terminal(
        &mut self,
        success: bool,
        termination: RunTermination,
        summary: &str,
        usage: TokenUsage,
    ) -> Result<(), RunError> {
        let content = self
            .content
            .as_mut()
            .ok_or_else(|| RunError::Message("content session is unavailable".into()))?;
        let (summary_part_id, summary_digest) =
            terminal_summary_binding(&content.messages, summary).ok_or_else(|| {
                RunError::Message("content terminal summary descriptor is missing".into())
            })?;
        content.terminal = Some(ContentTerminalCheckpoint {
            success,
            termination,
            summary_part_id,
            summary_digest,
            usage,
            finalized: false,
            artifact_dir: None,
        });
        Ok(())
    }

    pub fn mark_content_finalized(&mut self, artifact_dir: Option<&std::path::Path>) {
        if let Some(terminal) = self
            .content
            .as_mut()
            .and_then(|content| content.terminal.as_mut())
        {
            terminal.finalized = true;
            terminal.artifact_dir = artifact_dir.map(|path| path.display().to_string());
        }
    }

    pub async fn revalidate_content(&self) -> Result<(), RunError> {
        let Some(content) = &self.content else {
            return Ok(());
        };
        resolve_accepted(content.resolver.as_ref(), &content.messages)
            .await
            .map_err(|error| RunError::Message(error.to_string()))?;
        Ok(())
    }

    pub fn content_usage(&self) -> TokenUsage {
        self.content
            .as_ref()
            .map(|content| content.usage)
            .unwrap_or_default()
    }

    pub fn set_content_usage(&mut self, usage: TokenUsage) {
        if let Some(content) = &mut self.content {
            content.usage = usage;
        }
    }

    pub async fn append_content_model_turn(
        &mut self,
        turn: &ContentModelTurnV1,
    ) -> Result<(), RunError> {
        let content = self
            .content
            .as_mut()
            .ok_or_else(|| RunError::Message("content session is unavailable".into()))?;
        let references = content
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .map(|descriptor| descriptor.reference.as_str())
            .collect::<Vec<_>>();
        if turn
            .tool_calls
            .iter()
            .any(|call| tool_arguments_disclose_reference(&call.args_json, &references))
        {
            return Err(RunError::Message(
                "content tool arguments must not disclose raw resolver references".into(),
            ));
        }
        let mut used_ids = content
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .map(|descriptor| descriptor.part_id.clone())
            .collect::<HashSet<_>>();
        for descriptor in &turn.output_parts {
            validate_descriptor(descriptor).map_err(content_run_error)?;
            if !used_ids.insert(descriptor.part_id.clone()) {
                return Err(RunError::Message(
                    "model returned a duplicate content part id".into(),
                ));
            }
            if !content.capabilities.output_kinds.contains(&descriptor.kind) {
                return Err(RunError::Message(
                    "model returned an unsupported content output kind".into(),
                ));
            }
            if !content.capabilities.input_kinds.contains(&descriptor.kind)
                || !content
                    .capabilities
                    .media_types
                    .contains(&descriptor.media_type)
                || descriptor.byte_length > content.capabilities.max_part_bytes
            {
                return Err(RunError::Message(
                    "model returned content outside the configured capability bounds".into(),
                ));
            }
            if descriptor.disclosure_state == ContentDisclosureState::Accepted {
                let payload = content.resolver.resolve(descriptor).await.map_err(|_| {
                    RunError::Message(format!(
                        "content resolver failed for output part `{}`",
                        descriptor.part_id
                    ))
                })?;
                crate::content::validate_resolved(descriptor, &payload)
                    .map_err(content_run_error)?;
            }
        }
        let generated_lengths = std::iter::once(turn.text.as_bytes())
            .filter(|bytes| !bytes.is_empty())
            .chain(turn.tool_calls.iter().map(|call| call.args_json.as_bytes()))
            .collect::<Vec<_>>();
        if !generated_lengths.is_empty()
            && (!content
                .capabilities
                .input_kinds
                .contains(&crate::content::ContentPartKind::Text)
                || !content
                    .capabilities
                    .media_types
                    .iter()
                    .any(|media_type| media_type == "text/plain"))
        {
            return Err(RunError::Message(
                "content capabilities do not permit externalized text".into(),
            ));
        }
        if generated_lengths.iter().any(|bytes| {
            bytes.is_empty() || bytes.len() as u64 > content.capabilities.max_part_bytes
        }) {
            return Err(RunError::Message(
                "content model text or tool arguments exceed the configured part bound".into(),
            ));
        }
        let existing_count = content
            .messages
            .iter()
            .map(|message| message.parts.len())
            .sum::<usize>();
        let next_count = existing_count
            .saturating_add(turn.output_parts.len())
            .saturating_add(generated_lengths.len());
        let existing_bytes = content
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .map(|descriptor| descriptor.byte_length)
            .sum::<u64>();
        let next_bytes = turn
            .output_parts
            .iter()
            .map(|descriptor| descriptor.byte_length)
            .chain(generated_lengths.iter().map(|bytes| bytes.len() as u64))
            .try_fold(existing_bytes, u64::checked_add)
            .ok_or_else(|| RunError::Message("content size overflow".into()))?;
        if next_count > content.capabilities.max_parts as usize
            || next_bytes > content.capabilities.max_aggregate_bytes
        {
            return Err(RunError::Message(
                "content model output exceeds the configured aggregate bound".into(),
            ));
        }
        let mut parts = Vec::new();
        if !turn.text.is_empty() {
            let part_id = reserve_part_id(
                &mut used_ids,
                format!("shikigami-model-{}-text", self.turns),
            );
            parts.push(
                store_text(
                    content.resolver.as_ref(),
                    part_id,
                    turn.text.clone(),
                    "model",
                    format!("turn-{}", self.turns),
                )
                .await
                .map_err(content_run_error)?,
            );
        }
        parts.extend(turn.output_parts.iter().cloned());
        let mut persisted_tool_calls = Vec::with_capacity(turn.tool_calls.len());
        for (index, call) in turn.tool_calls.iter().enumerate() {
            let part_id = reserve_part_id(
                &mut used_ids,
                format!("shikigami-model-{}-tool-{index}-arguments", self.turns),
            );
            let descriptor = store_text(
                content.resolver.as_ref(),
                part_id.clone(),
                call.args_json.clone(),
                "model-tool-arguments",
                format!("turn-{}-call-{index}", self.turns),
            )
            .await
            .map_err(content_run_error)?;
            parts.push(descriptor);
            let mut persisted = call.clone();
            if persisted.id.is_empty() {
                persisted.id = format!("tool-{}-{index}", self.turns);
            }
            persisted.args_json = tool_arguments_pointer(&part_id);
            persisted_tool_calls.push(persisted);
        }
        if parts.is_empty() && turn.tool_calls.is_empty() {
            return Err(RunError::Message(
                "content model turn returned no output or tool calls".into(),
            ));
        }
        content.messages.push(ContentMessageV1 {
            role: "assistant".into(),
            parts,
            tool_call_id: String::new(),
            tool_calls: persisted_tool_calls,
        });
        validate_messages(&content.messages, &content.capabilities).map_err(content_run_error)?;
        Ok(())
    }

    pub async fn append_content_tool_text(
        &mut self,
        tool_call_id: String,
        text: String,
    ) -> Result<(), RunError> {
        let content = self
            .content
            .as_mut()
            .ok_or_else(|| RunError::Message("content session is unavailable".into()))?;
        let used_ids = content
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .map(|descriptor| descriptor.part_id.clone())
            .collect::<HashSet<_>>();
        let mut used_ids = used_ids;
        let part_id = reserve_part_id(
            &mut used_ids,
            format!("shikigami-tool-{}-{}", self.turns, content.messages.len()),
        );
        let descriptor = store_text(
            content.resolver.as_ref(),
            part_id,
            text,
            "tool",
            tool_call_id.clone(),
        )
        .await
        .map_err(content_run_error)?;
        content.messages.push(ContentMessageV1 {
            role: "tool".into(),
            parts: vec![descriptor],
            tool_call_id,
            tool_calls: Vec::new(),
        });
        validate_messages(&content.messages, &content.capabilities).map_err(content_run_error)?;
        Ok(())
    }

    pub fn load_content_sidecar(
        state_runs: &std::path::Path,
        run_id: &str,
        binding: &ContentCheckpointBinding,
    ) -> Result<ContentCheckpointV1, RunError> {
        load_sidecar(state_runs, run_id, binding).map_err(content_run_error)
    }

    pub fn set_replay(&mut self, replay: Option<ReplayCheckpoint>) {
        self.replay = replay;
    }

    pub fn replay_usage(&self) -> TokenUsage {
        self.replay
            .as_ref()
            .map(|replay| replay.usage)
            .unwrap_or_default()
    }

    pub fn set_replay_usage(&mut self, usage: TokenUsage) {
        if let Some(replay) = &mut self.replay {
            replay.usage = usage;
        }
    }

    pub fn mark_replay_terminal(&mut self, terminal: ReplayTerminalCheckpoint) {
        if let Some(replay) = &mut self.replay {
            replay.terminal = Some(terminal);
        }
    }

    pub fn mark_replay_finalized(&mut self, artifact_dir: Option<&std::path::Path>) {
        if let Some(terminal) = self
            .replay
            .as_mut()
            .and_then(|replay| replay.terminal.as_mut())
        {
            terminal.finalized = true;
            terminal.artifact_dir = artifact_dir.map(|path| path.display().to_string());
        }
    }

    /// Persist using the session's configured keep-workspace policy.
    pub fn save(&mut self, tools: &ToolRegistry) -> Result<(), RunError> {
        self.save_with_retention(self.keep_workspace, None, tools)
    }

    /// Persist with forced keep-workspace (failure / park recovery paths).
    pub fn save_recoverable(
        &mut self,
        park: Option<ParkedState>,
        tools: &ToolRegistry,
    ) -> Result<(), RunError> {
        self.save_with_retention(true, park, tools)
    }

    fn save_with_retention(
        &mut self,
        keep_workspace: bool,
        park: Option<ParkedState>,
        tools: &ToolRegistry,
    ) -> Result<(), RunError> {
        let mut replay = self.replay.clone();
        if let Some(replay) = &mut replay {
            replay.workspace = self.workspace.display().to_string();
            replay.comparison_cursor = self
                .messages
                .iter()
                .filter(|message| matches!(message.role.as_str(), "assistant" | "tool"))
                .count();
        }
        let next_content_binding = if let Some(content) = &self.content {
            Some(
                save_sidecar(
                    &self.state_runs,
                    &ContentCheckpointV1 {
                        schema_version: crate::content::CONTENT_SCHEMA_VERSION,
                        run_id: self.run_id.clone(),
                        generation: 0,
                        resolver_id: content.resolver.id().into(),
                        capabilities: content.capabilities.clone(),
                        messages: content.messages.clone(),
                        initial_message_count: content.initial_message_count,
                        completed_turns: self.turns,
                        usage: content.usage,
                        terminal: content.terminal.clone(),
                    },
                    content.binding.as_ref(),
                )
                .map_err(content_run_error)?,
            )
        } else {
            None
        };
        Checkpoint {
            version: checkpoint::CHECKPOINT_VERSION,
            run_id: self.run_id.clone(),
            task: self.task.clone(),
            prompt_id: checkpoint::prompt_id(SYSTEM_PROMPT),
            messages: self.messages.clone(),
            completed_turns: self.turns,
            workspace: self.workspace.clone(),
            keep_workspace,
            workspace_adapter: self.workspace_adapter.clone(),
            park,
            todos: tools.todos(),
            governance: self.governance.checkpoint_state(&self.run_id),
            replay,
            content: next_content_binding.clone(),
        }
        .save(&self.state_runs)?;
        if let (Some(content), Some(binding)) = (&mut self.content, next_content_binding) {
            content.binding = Some(binding);
        }
        Ok(())
    }
}

fn content_run_error(error: ContentError) -> RunError {
    RunError::Message(error.to_string())
}

fn reserve_part_id(used_ids: &mut HashSet<String>, base: String) -> String {
    if used_ids.insert(base.clone()) {
        return base;
    }
    for suffix in 1_u32.. {
        let candidate = format!("{base}-{suffix}");
        if used_ids.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("u32 part id suffixes are inexhaustible under content part bounds")
}

fn tool_arguments_disclose_reference(args_json: &str, references: &[&str]) -> bool {
    fn value_discloses(value: &serde_json::Value, references: &[&str]) -> bool {
        match value {
            serde_json::Value::String(value) => {
                references.iter().any(|reference| value.contains(reference))
            }
            serde_json::Value::Array(values) => values
                .iter()
                .any(|value| value_discloses(value, references)),
            serde_json::Value::Object(values) => values
                .values()
                .any(|value| value_discloses(value, references)),
            _ => false,
        }
    }

    serde_json::from_str(args_json)
        .ok()
        .is_some_and(|value| value_discloses(&value, references))
}

fn has_staged_content_model_turn(
    turns: u32,
    initial_message_count: u32,
    messages: &[ContentMessageV1],
) -> bool {
    turns > 0
        && messages.len() > initial_message_count as usize
        && messages
            .last()
            .is_some_and(|message| message.role == "assistant")
}

fn terminal_summary_binding(
    messages: &[ContentMessageV1],
    summary: &str,
) -> Option<(String, String)> {
    let summary_digest = crate::content::sha256_digest(summary.as_bytes());
    messages
        .iter()
        .rev()
        .flat_map(|message| message.parts.iter().rev())
        .find(|descriptor| {
            descriptor.kind == crate::content::ContentPartKind::Text
                && descriptor.disclosure_state == ContentDisclosureState::Accepted
                && descriptor.sha256_digest == summary_digest
        })
        .map(|descriptor| (descriptor.part_id.clone(), descriptor.sha256_digest.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{
        ContentPartDescriptor, ContentPartKind, ContentProvenanceV1, sha256_digest,
    };

    fn descriptor(part_id: &str, kind: ContentPartKind, payload: &[u8]) -> ContentPartDescriptor {
        ContentPartDescriptor {
            part_id: part_id.into(),
            kind,
            media_type: if kind == ContentPartKind::Text {
                "text/plain"
            } else {
                "image/png"
            }
            .into(),
            byte_length: payload.len() as u64,
            sha256_digest: sha256_digest(payload),
            reference: format!("memory:{part_id}"),
            provenance: ContentProvenanceV1 {
                source: "test".into(),
                source_id: part_id.into(),
                source_version: "1".into(),
                observed_at_ms: 1,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        }
    }

    #[test]
    fn terminal_summary_selects_matching_text_before_later_output_parts() {
        let messages = vec![ContentMessageV1 {
            role: "assistant".into(),
            parts: vec![
                descriptor("summary", ContentPartKind::Text, b"finished"),
                descriptor("image", ContentPartKind::Image, b"png"),
            ],
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }];

        let binding = terminal_summary_binding(&messages, "finished").unwrap();

        assert_eq!(binding.0, "summary");
        assert_eq!(binding.1, sha256_digest(b"finished"));
    }

    #[test]
    fn reference_disclosure_check_uses_complete_json_string_tokens() {
        assert!(!tool_arguments_disclose_reference(
            r#"{"path":"result.txt"}"#,
            &["memory:a"]
        ));
        assert!(tool_arguments_disclose_reference(
            r#"{"reference":"memory:part-1"}"#,
            &["memory:part-1"]
        ));
        assert!(tool_arguments_disclose_reference(
            r#"{"command":"open memory:part-1"}"#,
            &["memory:part-1"]
        ));
        assert!(tool_arguments_disclose_reference(
            r#"{"command":"open(memory:part-1)"}"#,
            &["memory:part-1"]
        ));
        assert!(tool_arguments_disclose_reference(
            r#"{"value":"prefix-memory:part-1"}"#,
            &["memory:part-1"]
        ));
    }

    #[test]
    fn initial_assistant_message_is_not_a_staged_model_turn() {
        let assistant = ContentMessageV1 {
            role: "assistant".into(),
            parts: Vec::new(),
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        };
        assert!(!has_staged_content_model_turn(
            0,
            1,
            std::slice::from_ref(&assistant)
        ));
        assert!(!has_staged_content_model_turn(
            1,
            1,
            std::slice::from_ref(&assistant)
        ));
        assert!(has_staged_content_model_turn(
            1,
            1,
            &[assistant.clone(), assistant]
        ));
    }
}
