//! Durable attempt state behind a deep checkpoint interface.
//!
//! Callers mutate conversation progress on this session and ask it to persist.
//! They do not restate messages, turns, todos, workspace, or retention at each
//! durability point.

use std::path::PathBuf;
use std::sync::Arc;

use crate::checkpoint::{self, Checkpoint, ParkedState};
use crate::governance::GovernancePort;
use crate::model::{ChatMessage, TokenUsage};
use crate::replay::{ReplayCheckpoint, ReplayTerminalCheckpoint};
use crate::tools::ToolRegistry;

use super::{RunError, SYSTEM_PROMPT};

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
        }
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
    pub fn save(&self, tools: &ToolRegistry) -> Result<(), RunError> {
        self.save_with_retention(self.keep_workspace, None, tools)
    }

    /// Persist with forced keep-workspace (failure / park recovery paths).
    pub fn save_recoverable(
        &self,
        park: Option<ParkedState>,
        tools: &ToolRegistry,
    ) -> Result<(), RunError> {
        self.save_with_retention(true, park, tools)
    }

    fn save_with_retention(
        &self,
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
        }
        .save(&self.state_runs)?;
        Ok(())
    }
}
