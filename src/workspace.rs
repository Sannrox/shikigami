//! Workspace materialization ports.

use std::path::{Path, PathBuf};
use std::process::Command;

use thiserror::Error;

use crate::config::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedWorkspace {
    pub path: PathBuf,
    pub adapter: String,
    pub cleanup: WorkspaceCleanup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceCleanup {
    None,
    /// Remove directory on drop of run (directory sandbox).
    RemoveDir,
    /// Remove git worktree.
    RemoveGitWorktree {
        repo: PathBuf,
        branch: String,
    },
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("unknown workspace adapter `{0}`")]
    Unknown(String),
    #[error("workspace I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("git worktree failed: {0}")]
    Git(String),
    #[error("snapshot not found: {0}")]
    SnapshotMissing(PathBuf),
}

/// Copy directory tree without following symlinks (workspace → snapshot).
pub fn copy_tree(src: &Path, dst: &Path) -> Result<(), WorkspaceError> {
    std::fs::create_dir_all(dst)?;
    let mut stack = vec![src.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_symlink() {
                continue;
            }
            let from = entry.path();
            let rel = from.strip_prefix(src).unwrap_or(from.as_path());
            let to = dst.join(rel);
            if meta.is_dir() {
                std::fs::create_dir_all(&to)?;
                stack.push(from);
            } else if meta.is_file() {
                if let Some(parent) = to.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::copy(&from, &to)?;
            }
        }
    }
    Ok(())
}

/// Snapshot workspace into `state_runs/<run_id>/snapshots/<name>/`.
pub fn take_snapshot(
    workspace: &Path,
    state_runs: &Path,
    run_id: &str,
    name: &str,
) -> Result<PathBuf, WorkspaceError> {
    let dest = state_runs.join(run_id).join("snapshots").join(name);
    if dest.exists() {
        std::fs::remove_dir_all(&dest)?;
    }
    copy_tree(workspace, &dest)?;
    Ok(dest)
}

/// Restore workspace files from a named snapshot (overwrites files).
pub fn restore_snapshot(
    workspace: &Path,
    state_runs: &Path,
    run_id: &str,
    name: &str,
) -> Result<(), WorkspaceError> {
    let src = state_runs.join(run_id).join("snapshots").join(name);
    if !src.is_dir() {
        return Err(WorkspaceError::SnapshotMissing(src));
    }
    // Clear files under workspace then copy back.
    if workspace.exists() {
        for entry in std::fs::read_dir(workspace)? {
            let entry = entry?;
            let p = entry.path();
            if entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(&p)?;
            } else {
                std::fs::remove_file(&p)?;
            }
        }
    } else {
        std::fs::create_dir_all(workspace)?;
    }
    copy_tree(&src, workspace)?;
    Ok(())
}

pub trait WorkspacePort: Send + Sync {
    fn id(&self) -> &'static str;
    fn health_detail(&self) -> String;
    fn materialize(
        &self,
        run_id: &str,
        state_runs: &Path,
    ) -> Result<MaterializedWorkspace, WorkspaceError>;
    fn cleanup(&self, ws: &MaterializedWorkspace) -> Result<(), WorkspaceError>;
}

pub fn from_config(config: &Config) -> Result<Box<dyn WorkspacePort>, WorkspaceError> {
    match config.workspace.adapter.as_str() {
        "directory" => Ok(Box::new(DirectoryWorkspace {
            root: PathBuf::from(&config.workspace.root),
        })),
        "git-worktree" => Ok(Box::new(GitWorktreeWorkspace {
            repo: PathBuf::from(&config.workspace.root),
            branch_prefix: config.workspace.branch_prefix.clone(),
        })),
        other => Err(WorkspaceError::Unknown(other.into())),
    }
}

struct DirectoryWorkspace {
    root: PathBuf,
}

impl WorkspacePort for DirectoryWorkspace {
    fn id(&self) -> &'static str {
        "directory"
    }

    fn health_detail(&self) -> String {
        format!("root={}", self.root.display())
    }

    fn materialize(
        &self,
        run_id: &str,
        state_runs: &Path,
    ) -> Result<MaterializedWorkspace, WorkspaceError> {
        let base = if self.root.as_os_str() == "." {
            state_runs.join(run_id).join("workspace")
        } else {
            self.root.join("shikigami-runs").join(run_id)
        };
        std::fs::create_dir_all(&base)?;
        let path = std::fs::canonicalize(&base)?;
        Ok(MaterializedWorkspace {
            path,
            adapter: "directory".into(),
            cleanup: WorkspaceCleanup::RemoveDir,
        })
    }

    fn cleanup(&self, ws: &MaterializedWorkspace) -> Result<(), WorkspaceError> {
        if matches!(ws.cleanup, WorkspaceCleanup::RemoveDir) && ws.path.exists() {
            let _ = std::fs::remove_dir_all(&ws.path);
        }
        Ok(())
    }
}

struct GitWorktreeWorkspace {
    repo: PathBuf,
    branch_prefix: String,
}

impl WorkspacePort for GitWorktreeWorkspace {
    fn id(&self) -> &'static str {
        "git-worktree"
    }

    fn health_detail(&self) -> String {
        let git_ok = Command::new("git")
            .args([
                "-C",
                &self.repo.to_string_lossy(),
                "rev-parse",
                "--is-inside-work-tree",
            ])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if git_ok {
            format!("repo={} ok", self.repo.display())
        } else {
            format!("repo={} (not a git work tree yet)", self.repo.display())
        }
    }

    fn materialize(
        &self,
        run_id: &str,
        state_runs: &Path,
    ) -> Result<MaterializedWorkspace, WorkspaceError> {
        let branch = format!("{}{run_id}", self.branch_prefix);
        let path = state_runs.join(run_id).join("worktree");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Create branch from HEAD and add worktree.
        let repo = std::fs::canonicalize(&self.repo).unwrap_or_else(|_| self.repo.clone());
        let status = Command::new("git")
            .args([
                "-C",
                &repo.to_string_lossy(),
                "worktree",
                "add",
                "-b",
                &branch,
            ])
            .arg(&path)
            .output()?;
        if !status.status.success() {
            // Retry without -b if branch exists: worktree add path branch
            let status2 = Command::new("git")
                .args(["-C", &repo.to_string_lossy(), "worktree", "add"])
                .arg(&path)
                .arg(&branch)
                .output()?;
            if !status2.status.success() {
                return Err(WorkspaceError::Git(format!(
                    "{}{}",
                    String::from_utf8_lossy(&status.stderr),
                    String::from_utf8_lossy(&status2.stderr)
                )));
            }
        }
        let path = std::fs::canonicalize(&path)?;
        Ok(MaterializedWorkspace {
            path,
            adapter: "git-worktree".into(),
            cleanup: WorkspaceCleanup::RemoveGitWorktree { repo, branch },
        })
    }

    fn cleanup(&self, ws: &MaterializedWorkspace) -> Result<(), WorkspaceError> {
        if let WorkspaceCleanup::RemoveGitWorktree { repo, branch } = &ws.cleanup {
            let _ = Command::new("git")
                .args([
                    "-C",
                    &repo.to_string_lossy(),
                    "worktree",
                    "remove",
                    "--force",
                ])
                .arg(&ws.path)
                .output();
            let _ = Command::new("git")
                .args(["-C", &repo.to_string_lossy(), "branch", "-D", branch])
                .output();
        }
        Ok(())
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn snapshot_and_restore() {
        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let runs = dir.path().join("runs");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("a.txt"), "v1").unwrap();
        take_snapshot(&ws, &runs, "r1", "initial").unwrap();
        std::fs::write(ws.join("a.txt"), "v2").unwrap();
        restore_snapshot(&ws, &runs, "r1", "initial").unwrap();
        assert_eq!(std::fs::read_to_string(ws.join("a.txt")).unwrap(), "v1");
    }
}
