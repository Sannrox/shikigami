//! Local run checkpoints (harness scratch, not plane truth).

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
use thiserror::Error;

use crate::model::ChatMessage;
use crate::tools::TodoItem;

pub const CHECKPOINT_VERSION: u32 = 1;
pub const CHECKPOINT_FILENAME: &str = "checkpoint.json";

/// Structured park state when a run awaits an operator answer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParkedState {
    pub reason: String,
    pub question: String,
    /// Tool call id that must receive the operator answer as a tool result.
    pub tool_call_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub version: u32,
    pub run_id: String,
    pub task: String,
    pub prompt_id: String,
    pub messages: Vec<ChatMessage>,
    pub completed_turns: u32,
    pub workspace: PathBuf,
    pub keep_workspace: bool,
    /// Workspace adapter id at materialization (`directory`, `inplace`, …).
    /// Older checkpoints omit this field (default empty).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub workspace_adapter: String,
    /// Present when the run is parked for operator input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub park: Option<ParkedState>,
    /// Run-scoped todo checklist (from `todo_write`); empty on older checkpoints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub todos: Vec<TodoItem>,
}

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("checkpoint I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("checkpoint parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("unsupported checkpoint version {found}; expected {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("checkpoint not found for run {0}")]
    Missing(String),
    #[error("checkpoint prompt id mismatch")]
    PromptMismatch,
}

/// Versioned prompt id for a body (defaults to the `harness-v1` name prefix
/// for backward-compatible checkpoints written before `prompts` module).
pub fn prompt_id(prompt: &str) -> String {
    // Prefer the canonical asset id when body matches the shipped default.
    if prompt == crate::prompts::HARNESS_V1.body
        || prompt.replace("\r\n", "\n") == crate::prompts::HARNESS_V1.body.replace("\r\n", "\n")
    {
        return crate::prompts::versioned_id(&crate::prompts::HARNESS_V1);
    }
    let digest = Sha256::digest(prompt.replace("\r\n", "\n").as_bytes());
    format!("custom:{}", hex_lower(digest.as_slice()))
}

pub fn path_for(state_runs: &Path, run_id: &str) -> PathBuf {
    state_runs.join(run_id).join(CHECKPOINT_FILENAME)
}

impl Checkpoint {
    pub fn save(&self, state_runs: &Path) -> Result<PathBuf, CheckpointError> {
        let path = path_for(state_runs, &self.run_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = serde_json::to_string_pretty(self)?;
        fs::write(&path, raw)?;
        Ok(path)
    }

    pub fn load(state_runs: &Path, run_id: &str) -> Result<Self, CheckpointError> {
        let path = path_for(state_runs, run_id);
        if !path.is_file() {
            return Err(CheckpointError::Missing(run_id.into()));
        }
        let raw = fs::read_to_string(path)?;
        let cp: Self = serde_json::from_str(&raw)?;
        if cp.version != CHECKPOINT_VERSION {
            return Err(CheckpointError::UnsupportedVersion {
                found: cp.version,
                expected: CHECKPOINT_VERSION,
            });
        }
        Ok(cp)
    }

    pub fn validate_prompt(&self, prompt: &str) -> Result<(), CheckpointError> {
        if self.prompt_id != prompt_id(prompt) {
            return Err(CheckpointError::PromptMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn round_trip() {
        let dir = tempdir().unwrap();
        let runs = dir.path().join("runs");
        let cp = Checkpoint {
            version: CHECKPOINT_VERSION,
            run_id: "abc".into(),
            task: "t".into(),
            prompt_id: prompt_id("p"),
            messages: vec![],
            completed_turns: 1,
            workspace: runs.join("abc/workspace"),
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: None,
            todos: vec![],
        };
        cp.save(&runs).unwrap();
        let loaded = Checkpoint::load(&runs, "abc").unwrap();
        assert_eq!(loaded, cp);
        loaded.validate_prompt("p").unwrap();
        assert!(loaded.validate_prompt("other").is_err());
    }
}
