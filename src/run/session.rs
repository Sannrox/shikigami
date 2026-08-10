//! Durable attempt state behind a deep checkpoint interface.
//!
//! Callers mutate conversation progress on this session and ask it to persist.
//! They do not restate messages, turns, todos, workspace, or retention at each
//! durability point.

use std::path::PathBuf;
use std::sync::Arc;

use crate::checkpoint::{self, Checkpoint, ParkedState};
use crate::governance::GovernancePort;
use crate::model::ChatMessage;
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
        }
        .save(&self.state_runs)?;
        Ok(())
    }
}
