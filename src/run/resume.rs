//! Resume-path workspace validation.
//!
//! Isolates "is this checkpoint workspace still the one this config owns?"
//! from the turn loop so resume safety is one deep seam.

use std::path::{Path, PathBuf};

use crate::checkpoint::Checkpoint;
use crate::config::Config;

use super::RunError;

pub(super) fn configured_workspace_adapter(config: &Config) -> &str {
    match config.workspace.adapter.as_str() {
        "directory-inplace" => "inplace",
        other => other,
    }
}

fn canonical_workspace_below(root: &Path, suffix: &[&str]) -> Result<PathBuf, RunError> {
    let trusted_root = root.canonicalize().map_err(|error| {
        RunError::Message(format!(
            "configured workspace root cannot be resolved: {}: {error}",
            root.display()
        ))
    })?;
    let mut candidate = root.to_path_buf();
    for component in suffix {
        candidate.push(component);
        let metadata = std::fs::symlink_metadata(&candidate).map_err(|error| {
            RunError::Message(format!(
                "expected checkpoint workspace cannot be inspected: {}: {error}",
                candidate.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RunError::Message(format!(
                "checkpoint workspace path must not contain symlinks: {}",
                candidate.display()
            )));
        }
    }
    let expected = candidate.canonicalize().map_err(|error| {
        RunError::Message(format!(
            "expected checkpoint workspace cannot be resolved: {}: {error}",
            candidate.display()
        ))
    })?;
    if !expected.starts_with(&trusted_root) {
        return Err(RunError::Message(format!(
            "checkpoint workspace {} escapes configured root {}",
            expected.display(),
            trusted_root.display()
        )));
    }
    Ok(expected)
}

/// Ensure a resumed checkpoint still points at the workspace this config owns.
pub(super) fn validate_resumed_workspace(
    config: &Config,
    state_runs: &Path,
    resume_id: &str,
    checkpoint: &Checkpoint,
) -> Result<PathBuf, RunError> {
    let actual = checkpoint.workspace.canonicalize().map_err(|error| {
        RunError::Message(format!(
            "checkpoint workspace cannot be resolved: {}: {error}",
            checkpoint.workspace.display()
        ))
    })?;
    let configured_adapter = configured_workspace_adapter(config);
    let checkpoint_adapter = if checkpoint.workspace_adapter.is_empty() {
        configured_adapter
    } else {
        checkpoint.workspace_adapter.as_str()
    };
    if checkpoint_adapter != configured_adapter {
        return Err(RunError::Message(format!(
            "checkpoint workspace adapter `{checkpoint_adapter}` does not match configured adapter `{configured_adapter}`"
        )));
    }
    let expected = match configured_adapter {
        "directory" => {
            let root = PathBuf::from(&config.workspace.root);
            if root.as_os_str() == "." {
                canonical_workspace_below(state_runs, &[resume_id, "workspace"])?
            } else {
                canonical_workspace_below(&root, &["shikigami-runs", resume_id])?
            }
        }
        "git-worktree" => canonical_workspace_below(state_runs, &[resume_id, "worktree"])?,
        "inplace" => PathBuf::from(&config.workspace.root)
            .canonicalize()
            .map_err(|error| {
                RunError::Message(format!(
                    "configured workspace root cannot be resolved: {}: {error}",
                    config.workspace.root
                ))
            })?,
        other => {
            return Err(RunError::Message(format!(
                "cannot validate checkpoint workspace for adapter `{other}`"
            )));
        }
    };
    if actual != expected {
        return Err(RunError::Message(format!(
            "checkpoint workspace {} does not match configured workspace {}",
            actual.display(),
            expected.display()
        )));
    }
    Ok(actual)
}
