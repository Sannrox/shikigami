//! Stable local run artifact manifests.
//!
//! Manifests contain file metadata and hashes, not file contents. A git patch
//! is captured separately when the workspace is a git worktree and remains an
//! explicit export choice for callers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::checkpoint;

pub const ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const ARTIFACT_DIRNAME: &str = "artifacts";
const BASELINE_FILENAME: &str = "baseline.json";
const MAX_FILES: usize = 10_000;
const MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("artifact: invalid run id")]
    InvalidRunId,
    #[error("artifact: run {0} has no captured manifest")]
    Missing(String),
    #[error("artifact: {0}")]
    Message(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactChange {
    pub path: String,
    /// `added` | `modified` | `deleted`.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactManifest {
    pub schema_version: u32,
    pub run_id: String,
    pub captured_at_ms: u64,
    pub workspace: String,
    pub workspace_present: bool,
    pub files_truncated: bool,
    pub files: Vec<ArtifactFile>,
    pub changes: Vec<ArtifactChange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactBaseline {
    schema_version: u32,
    captured_at_ms: u64,
    files_truncated: bool,
    #[serde(default)]
    patch_safe: bool,
    #[serde(default)]
    excluded_paths: Vec<String>,
    files: Vec<ArtifactFile>,
}

pub fn artifact_dir(state_runs: &Path, run_id: &str) -> Result<PathBuf, ArtifactError> {
    if !checkpoint::is_safe_run_id(run_id) {
        return Err(ArtifactError::InvalidRunId);
    }
    Ok(state_runs.join(run_id).join(ARTIFACT_DIRNAME))
}

pub fn manifest_path(state_runs: &Path, run_id: &str) -> Result<PathBuf, ArtifactError> {
    Ok(artifact_dir(state_runs, run_id)?.join("manifest.json"))
}

pub fn capture_run_artifacts(
    state_runs: &Path,
    run_id: &str,
    workspace: &Path,
) -> Result<PathBuf, ArtifactError> {
    let dir = artifact_dir(state_runs, run_id)?;
    fs::create_dir_all(&dir)?;
    let (files, files_truncated) = inventory(workspace)?;
    let baseline = load_baseline(&dir)?;
    let before = baseline
        .as_ref()
        .map(|baseline| {
            baseline
                .files
                .iter()
                .cloned()
                .map(|file| (file.path.clone(), file))
                .collect()
        })
        .unwrap_or(inventory_dir(
            &state_runs.join(run_id).join("snapshots").join("initial"),
        )?);
    let after: BTreeMap<String, ArtifactFile> = files
        .iter()
        .cloned()
        .map(|file| (file.path.clone(), file))
        .collect();
    let changes = changes(&before, &after);

    let patch_changes: Vec<ArtifactChange> = baseline
        .as_ref()
        .filter(|baseline| baseline.patch_safe && !baseline.files_truncated)
        .map(|baseline| {
            changes
                .iter()
                .filter(|change| !baseline.excluded_paths.contains(&change.path))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let diff_path = if patch_changes.is_empty() {
        None
    } else {
        capture_git_diff(workspace, &dir, &files, &patch_changes)?
    };
    let manifest = ArtifactManifest {
        schema_version: ARTIFACT_SCHEMA_VERSION,
        run_id: run_id.into(),
        captured_at_ms: now_ms(),
        workspace: workspace.display().to_string(),
        workspace_present: workspace.is_dir(),
        files_truncated,
        files,
        changes,
        diff_path,
    };
    let path = dir.join("manifest.json");
    atomic_write_json(&path, &manifest)?;
    Ok(dir)
}

/// Capture a hash-only workspace baseline before a run mutates the workspace.
/// Existing baselines are preserved across parked/error resume attempts.
pub fn capture_run_baseline(
    state_runs: &Path,
    run_id: &str,
    workspace: &Path,
) -> Result<(), ArtifactError> {
    let dir = artifact_dir(state_runs, run_id)?;
    fs::create_dir_all(&dir)?;
    let path = dir.join(BASELINE_FILENAME);
    if path.is_file() {
        return Ok(());
    }
    let (files, files_truncated) = inventory(workspace)?;
    let (patch_safe, excluded_paths) = git_baseline_exclusions(workspace);
    atomic_write_json(
        &path,
        &ArtifactBaseline {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            captured_at_ms: now_ms(),
            files_truncated,
            patch_safe,
            excluded_paths,
            files,
        },
    )
}

pub fn load_manifest(state_runs: &Path, run_id: &str) -> Result<ArtifactManifest, ArtifactError> {
    let path = manifest_path(state_runs, run_id)?;
    if !path.is_file() {
        return Err(ArtifactError::Missing(run_id.into()));
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub fn export_run_artifacts(
    state_runs: &Path,
    run_id: &str,
    include_patch: bool,
) -> Result<String, ArtifactError> {
    let manifest = load_manifest(state_runs, run_id)?;
    if include_patch {
        let Some(relative) = manifest.diff_path else {
            return Err(ArtifactError::Message(
                "no git patch was captured for this run".into(),
            ));
        };
        return Ok(fs::read_to_string(
            artifact_dir(state_runs, run_id)?.join(relative),
        )?);
    }
    Ok(serde_json::to_string_pretty(&manifest)? + "\n")
}

fn inventory(root: &Path) -> Result<(Vec<ArtifactFile>, bool), ArtifactError> {
    if !root.is_dir() {
        return Ok((Vec::new(), false));
    }
    let mut files = Vec::new();
    let mut total_bytes = 0u64;
    let mut truncated = false;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            let rel = path
                .strip_prefix(root)
                .map_err(|error| ArtifactError::Message(error.to_string()))?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                if rel.components().any(|component| {
                    matches!(component, std::path::Component::Normal(name) if name == ".git" || name == ".shikigami")
                }) {
                    continue;
                }
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            if files.len() >= MAX_FILES || total_bytes >= MAX_TOTAL_BYTES {
                truncated = true;
                break;
            }
            let metadata = entry.metadata()?;
            let bytes = metadata.len();
            if total_bytes.saturating_add(bytes) > MAX_TOTAL_BYTES {
                truncated = true;
                break;
            }
            files.push(ArtifactFile {
                path: rel.to_string_lossy().replace('\\', "/"),
                bytes,
                sha256: hash_file(&path)?,
            });
            total_bytes = total_bytes.saturating_add(bytes);
        }
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok((files, truncated))
}

fn inventory_dir(root: &Path) -> Result<BTreeMap<String, ArtifactFile>, ArtifactError> {
    let (files, _) = inventory(root)?;
    Ok(files
        .into_iter()
        .map(|file| (file.path.clone(), file))
        .collect())
}

fn load_baseline(dir: &Path) -> Result<Option<ArtifactBaseline>, ArtifactError> {
    let path = dir.join(BASELINE_FILENAME);
    if !path.is_file() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
}

fn git_baseline_exclusions(workspace: &Path) -> (bool, Vec<String>) {
    if !is_git_workspace(workspace) {
        return (true, Vec::new());
    }
    let mut tracked_command = Command::new("git");
    tracked_command
        .arg("-C")
        .arg(workspace)
        .args(["diff", "--name-only", "-z", "HEAD", "--"])
        .arg(".");
    let tracked = match bounded_git_output(tracked_command, MAX_PATCH_BYTES) {
        Ok(Some(output)) => output,
        _ => return (false, Vec::new()),
    };
    let has_head = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", "--verify", "HEAD"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if !tracked.status.success() && has_head {
        return (false, Vec::new());
    }

    let mut untracked_command = Command::new("git");
    untracked_command
        .arg("-C")
        .arg(workspace)
        .args(["ls-files", "--others", "--exclude-standard", "-z", "--"])
        .arg(".");
    let untracked = match bounded_git_output(untracked_command, MAX_PATCH_BYTES) {
        Ok(Some(output)) => output,
        _ => return (false, Vec::new()),
    };
    if !untracked.status.success() {
        return (false, Vec::new());
    }
    let mut excluded = BTreeSet::new();
    excluded.extend(parse_git_paths(&tracked.stdout));
    excluded.extend(parse_git_paths(&untracked.stdout));
    (true, excluded.into_iter().collect())
}

fn parse_git_paths(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8_lossy(path).replace('\\', "/"))
        .collect()
}

fn changes(
    before: &BTreeMap<String, ArtifactFile>,
    after: &BTreeMap<String, ArtifactFile>,
) -> Vec<ArtifactChange> {
    let mut paths = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .filter_map(|path| match (before.get(&path), after.get(&path)) {
            (None, Some(file)) => Some(ArtifactChange {
                path,
                status: "added".into(),
                before_sha256: None,
                after_sha256: Some(file.sha256.clone()),
            }),
            (Some(file), None) => Some(ArtifactChange {
                path,
                status: "deleted".into(),
                before_sha256: Some(file.sha256.clone()),
                after_sha256: None,
            }),
            (Some(before), Some(after)) if before.sha256 != after.sha256 => Some(ArtifactChange {
                path,
                status: "modified".into(),
                before_sha256: Some(before.sha256.clone()),
                after_sha256: Some(after.sha256.clone()),
            }),
            _ => None,
        })
        .collect()
}

fn capture_git_diff(
    workspace: &Path,
    artifact_dir: &Path,
    files: &[ArtifactFile],
    changes: &[ArtifactChange],
) -> Result<Option<String>, ArtifactError> {
    if changes.is_empty() {
        return Ok(None);
    }
    if !is_git_workspace(workspace) {
        return Ok(None);
    }
    let mut tracked_command = Command::new("git");
    tracked_command
        .arg("-C")
        .arg(workspace)
        .args(["diff", "--binary", "HEAD", "--"])
        // Every pathspec comes from the hash baseline comparison, so a
        // pre-existing dirty file is included only when the run changed it.
        ;
    for change in changes {
        tracked_command.arg(&change.path);
    }
    let Some(tracked) = bounded_git_output(tracked_command, MAX_PATCH_BYTES)? else {
        return Ok(None);
    };
    let mut patch = if tracked.status.success() {
        tracked.stdout
    } else {
        Vec::new()
    };
    let changed_paths: BTreeMap<&str, &ArtifactFile> = files
        .iter()
        .filter_map(|file| {
            changes
                .iter()
                .any(|change| change.path == file.path && change.status != "deleted")
                .then_some((file.path.as_str(), file))
        })
        .collect();
    for path in changed_paths.keys() {
        let ignored = Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["check-ignore", "--quiet", "--no-index", "--"])
            .arg(path)
            .stderr(Stdio::null())
            .status();
        if ignored.is_ok_and(|status| status.success()) {
            continue;
        }
        let tracked = Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["ls-files", "--error-unmatch", "--"])
            .arg(path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if tracked.is_ok_and(|status| status.success()) {
            continue;
        }
        let mut untracked_command = Command::new("git");
        untracked_command
            .arg("-C")
            .arg(workspace)
            .args(["diff", "--no-index", "--binary", null_device(), "--"])
            .arg(path);
        let Some(untracked) = bounded_git_output(
            untracked_command,
            MAX_PATCH_BYTES.saturating_sub(patch.len()),
        )?
        else {
            return Ok(None);
        };
        patch.extend_from_slice(&untracked.stdout);
    }
    if patch.is_empty() {
        return Ok(None);
    }
    let path = artifact_dir.join("diff.patch");
    fs::write(&path, &patch)?;
    Ok(Some("diff.patch".into()))
}

fn is_git_workspace(workspace: &Path) -> bool {
    let root = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", "--show-toplevel"])
        .stderr(Stdio::null())
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|root| PathBuf::from(root.trim()));
    let Some(root) = root.and_then(|root| root.canonicalize().ok()) else {
        return false;
    };
    let Some(workspace) = workspace.canonicalize().ok() else {
        return false;
    };
    workspace.starts_with(root)
}

struct GitOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
}

/// Read Git output without allowing a pathological diff to grow without
/// bound. `None` means Git could not be spawned or the bounded capture was
/// exceeded, either of which disables patch retention for this capture.
fn bounded_git_output(
    mut command: Command,
    limit: usize,
) -> Result<Option<GitOutput>, ArtifactError> {
    command.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let Some(mut stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(ArtifactError::Message(
            "git patch command did not provide stdout".into(),
        ));
    };
    let mut output = Vec::with_capacity(limit.min(64 * 1024));
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = match stdout.read(&mut buffer) {
            Ok(read) => read,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
        };
        if read == 0 {
            break;
        }
        if output.len().saturating_add(read) > limit {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(None);
        }
        output.extend_from_slice(&buffer[..read]);
    }
    Ok(Some(GitOutput {
        status: child.wait()?,
        stdout: output,
    }))
}

fn null_device() -> &'static str {
    if cfg!(windows) { "NUL" } else { "/dev/null" }
}

fn hash_file(path: &Path) -> Result<String, ArtifactError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok(format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), ArtifactError> {
    let temp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(value)?)?;
    crate::atomic::replace_file(&temp, path)?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn captures_inventory_and_changes_against_initial_snapshot() {
        let dir = tempdir().unwrap();
        let state_runs = dir.path().join("runs");
        let workspace = dir.path().join("workspace");
        let initial = state_runs.join("run-1/snapshots/initial");
        fs::create_dir_all(&initial).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(initial.join("same.txt"), "same").unwrap();
        fs::write(initial.join("old.txt"), "old").unwrap();
        fs::write(workspace.join("same.txt"), "changed").unwrap();
        fs::write(workspace.join("new.txt"), "new").unwrap();

        let dir = capture_run_artifacts(&state_runs, "run-1", &workspace).unwrap();
        let manifest: ArtifactManifest =
            serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap();
        assert!(manifest.workspace_present);
        assert_eq!(manifest.changes.len(), 3);
        assert!(
            manifest
                .changes
                .iter()
                .any(|change| change.status == "deleted")
        );
    }

    #[test]
    fn rejects_path_like_run_ids() {
        let dir = tempdir().unwrap();
        let err = capture_run_artifacts(dir.path(), "../bad", dir.path()).unwrap_err();
        assert!(matches!(err, ArtifactError::InvalidRunId));
    }

    #[test]
    fn git_patch_includes_untracked_files() {
        let dir = tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let state_runs = dir.path().join("runs");
        fs::create_dir_all(&workspace).unwrap();
        Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(&workspace)
            .status()
            .unwrap();
        fs::write(workspace.join("preexisting.txt"), "leave me alone\n").unwrap();
        capture_run_baseline(&state_runs, "run-1", &workspace).unwrap();
        fs::write(workspace.join("new.txt"), "new content\n").unwrap();
        let artifacts = capture_run_artifacts(&state_runs, "run-1", &workspace).unwrap();
        let patch = fs::read_to_string(artifacts.join("diff.patch")).unwrap();
        assert!(patch.contains("new.txt"));
        assert!(patch.contains("new content"));
        assert!(!patch.contains("preexisting.txt"));
    }

    #[test]
    fn git_patch_is_scoped_to_nested_workspace() {
        let dir = tempdir().unwrap();
        let repo = dir.path().join("repo");
        let workspace = repo.join("nested");
        let state_runs = dir.path().join("runs");
        fs::create_dir_all(&workspace).unwrap();
        Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(&repo)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@example.invalid"])
            .current_dir(&repo)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&repo)
            .status()
            .unwrap();
        fs::write(repo.join("outside.txt"), "before outside\n").unwrap();
        fs::write(workspace.join("inside.txt"), "before inside\n").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(&repo)
            .status()
            .unwrap();
        Command::new("git")
            .args(["commit", "--quiet", "-m", "initial"])
            .current_dir(&repo)
            .status()
            .unwrap();
        capture_run_baseline(&state_runs, "run-1", &workspace).unwrap();
        fs::write(repo.join("outside.txt"), "after outside\n").unwrap();
        fs::write(workspace.join("inside.txt"), "after inside\n").unwrap();

        let artifacts = capture_run_artifacts(&state_runs, "run-1", &workspace).unwrap();
        let patch = fs::read_to_string(artifacts.join("diff.patch")).unwrap();
        assert!(patch.contains("inside.txt"));
        assert!(patch.contains("after inside"));
        assert!(!patch.contains("outside.txt"));
        assert!(!patch.contains("after outside"));
    }
}
