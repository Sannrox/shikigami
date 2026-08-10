//! Local run checkpoints (harness scratch, not plane truth).

use std::collections::BTreeMap;
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

/// Governance correlation that must survive a local resume. This is a
/// transport/retry aid only; the plane receipt remains authoritative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct GovernanceCheckpoint {
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub logical_operation_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model_operation_id: String,
    #[serde(default)]
    pub model_reported: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_event_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_event: Option<PendingGovernanceEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_tool_reports: Vec<StagedToolReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_tool_executions: Vec<StagedToolExecution>,
}

/// Host-side tool outcome staged before authenticated reporting. The host may
/// already have applied the effect when a report transport fails, so resume
/// replays this intent with its stable tool-call id instead of executing it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedToolReport {
    pub call_id: String,
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionStatus {
    Authorizing,
    Started,
    Completed,
}

/// Durable host-effect state. An `authorizing` record is safe to retry with
/// the same stable permit identity. A `started` record is deliberately
/// treated as in-doubt on resume; shikigami never blindly replays an effect
/// whose process may have exited after applying it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedToolExecution {
    pub call_id: String,
    pub name: String,
    pub args_json: String,
    pub status: ToolExecutionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingGovernanceEvent {
    pub operation_id: String,
    pub event_id: String,
    pub parent_event_id: String,
    pub timestamp_ms: i64,
    pub kind: String,
    pub attributes: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<GovernanceEvidenceReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovernanceEvidenceReference {
    pub kind: String,
    pub reference: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disclosed_fields: Vec<String>,
    #[serde(default)]
    pub omitted: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub omission_reason: String,
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
    /// Governed receipt/event retry state; absent on older checkpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance: Option<GovernanceCheckpoint>,
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
    #[error("checkpoint run id must be a non-empty opaque ASCII identifier")]
    InvalidRunId,
    #[error("checkpoint run id does not match requested run")]
    RunIdMismatch,
    #[error("checkpoint is not parked")]
    NotParked,
    #[error("checkpoint workspace is unavailable")]
    WorkspaceUnavailable,
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

pub fn is_safe_run_id(run_id: &str) -> bool {
    !run_id.is_empty()
        && run_id.len() <= 128
        && run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

impl Checkpoint {
    pub fn save(&self, state_runs: &Path) -> Result<PathBuf, CheckpointError> {
        if !is_safe_run_id(&self.run_id) {
            return Err(CheckpointError::InvalidRunId);
        }
        let path = path_for(state_runs, &self.run_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = serde_json::to_string_pretty(self)?;
        fs::write(&path, raw)?;
        Ok(path)
    }

    pub fn load(state_runs: &Path, run_id: &str) -> Result<Self, CheckpointError> {
        Ok(Self::load_with_digest(state_runs, run_id)?.0)
    }

    /// Load a resumable parked checkpoint and bind its digest to the exact
    /// bytes that passed validation.
    pub fn load_parked_digest(
        state_runs: &Path,
        run_id: &str,
        prompt: &str,
    ) -> Result<String, CheckpointError> {
        let (checkpoint, digest) = Self::load_with_digest(state_runs, run_id)?;
        if checkpoint.park.is_none() {
            return Err(CheckpointError::NotParked);
        }
        if !checkpoint.workspace.is_dir() {
            return Err(CheckpointError::WorkspaceUnavailable);
        }
        checkpoint.validate_prompt(prompt)?;
        Ok(digest)
    }

    pub(crate) fn load_with_digest(
        state_runs: &Path,
        run_id: &str,
    ) -> Result<(Self, String), CheckpointError> {
        let raw = Self::read(state_runs, run_id)?;
        let checkpoint = Self::parse(&raw, run_id)?;
        let digest = format!("sha256:{}", hex_lower(Sha256::digest(&raw).as_slice()));
        Ok((checkpoint, digest))
    }

    fn read(state_runs: &Path, run_id: &str) -> Result<Vec<u8>, CheckpointError> {
        if !is_safe_run_id(run_id) {
            return Err(CheckpointError::InvalidRunId);
        }
        let path = path_for(state_runs, run_id);
        if !path.is_file() {
            return Err(CheckpointError::Missing(run_id.into()));
        }
        Ok(fs::read(path)?)
    }

    fn parse(raw: &[u8], run_id: &str) -> Result<Self, CheckpointError> {
        let cp: Self = serde_json::from_slice(raw)?;
        if cp.version != CHECKPOINT_VERSION {
            return Err(CheckpointError::UnsupportedVersion {
                found: cp.version,
                expected: CHECKPOINT_VERSION,
            });
        }
        if cp.run_id != run_id {
            return Err(CheckpointError::RunIdMismatch);
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
            governance: Some(GovernanceCheckpoint {
                operation_id: "host-plan".into(),
                logical_operation_id: "logical-op".into(),
                model_operation_id: "model-plan".into(),
                model_reported: true,
                last_event_id: "report:host-plan:event".into(),
                pending_event: Some(PendingGovernanceEvent {
                    operation_id: "host-plan".into(),
                    event_id: "report:host-plan:pending".into(),
                    parent_event_id: "report:host-plan:event".into(),
                    timestamp_ms: 42,
                    kind: "action_performed".into(),
                    attributes: BTreeMap::from([(String::from("tool"), String::from("bash"))]),
                    references: vec![],
                }),
                pending_tool_reports: vec![],
                pending_tool_executions: vec![],
            }),
        };
        cp.save(&runs).unwrap();
        let loaded = Checkpoint::load(&runs, "abc").unwrap();
        assert_eq!(loaded, cp);
        loaded.validate_prompt("p").unwrap();
        assert!(loaded.validate_prompt("other").is_err());
    }

    #[test]
    fn load_rejects_path_like_and_mismatched_run_ids() {
        let dir = tempdir().unwrap();
        let runs = dir.path().join("runs");
        assert!(matches!(
            Checkpoint::load(&runs, "../other"),
            Err(CheckpointError::InvalidRunId)
        ));
        assert!(matches!(
            Checkpoint::load(&runs, "/tmp/other"),
            Err(CheckpointError::InvalidRunId)
        ));

        let requested = "requested";
        let path = path_for(&runs, requested);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let cp = Checkpoint {
            version: CHECKPOINT_VERSION,
            run_id: "different".into(),
            task: "t".into(),
            prompt_id: prompt_id("p"),
            messages: vec![],
            completed_turns: 0,
            workspace: runs.join(requested).join("workspace"),
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: None,
            todos: vec![],
            governance: None,
        };
        std::fs::write(path, serde_json::to_vec(&cp).unwrap()).unwrap();
        assert!(matches!(
            Checkpoint::load(&runs, requested),
            Err(CheckpointError::RunIdMismatch)
        ));
    }

    #[test]
    fn load_parked_validates_state_and_digests_validated_bytes() {
        let dir = tempdir().unwrap();
        let runs = dir.path().join("runs");
        let workspace = runs.join("parked/workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut cp = Checkpoint {
            version: CHECKPOINT_VERSION,
            run_id: "parked".into(),
            task: "t".into(),
            prompt_id: prompt_id("p"),
            messages: vec![],
            completed_turns: 1,
            workspace,
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: Some(ParkedState {
                reason: "approval required".into(),
                question: "continue?".into(),
                tool_call_id: "tool-1".into(),
            }),
            todos: vec![],
            governance: None,
        };
        let path = cp.save(&runs).unwrap();
        let raw = std::fs::read(&path).unwrap();

        let digest = Checkpoint::load_parked_digest(&runs, "parked", "p").unwrap();
        assert_eq!(
            digest,
            format!("sha256:{}", hex_lower(Sha256::digest(raw).as_slice()))
        );

        cp.park = None;
        cp.save(&runs).unwrap();
        assert!(matches!(
            Checkpoint::load_parked_digest(&runs, "parked", "p"),
            Err(CheckpointError::NotParked)
        ));
    }
}
