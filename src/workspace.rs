//! Workspace materialization ports.

use std::fs::OpenOptions;
use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
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
    #[error("invalid snapshot {field}: `{value}` must be one safe path segment")]
    InvalidSnapshotId { field: &'static str, value: String },
    #[error("snapshot storage contains a symbolic link")]
    UnsafeSnapshotPath,
    #[error("workspace adapter `{0}` does not support snapshots")]
    SnapshotUnsupported(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotPlan<'a> {
    None,
    CaptureInitial,
    /// Keep an existing `initial` snapshot; never capture from a resumed workspace.
    KeepInitial,
    Restore(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotOutcome {
    Unchanged,
    Captured { name: String, path: PathBuf },
    Restored { name: String },
}

/// Run-scoped snapshot module.
///
/// This is the only interface the turn loop uses for snapshot policy. Storage
/// layout, identifier validation, and destructive restore ordering stay behind
/// this seam.
pub(crate) struct WorkspaceSnapshots<'a> {
    state_runs: &'a Path,
}

impl<'a> WorkspaceSnapshots<'a> {
    pub(crate) fn new(state_runs: &'a Path) -> Self {
        Self { state_runs }
    }

    pub(crate) fn prepare(
        &self,
        workspace: &MaterializedWorkspace,
        run_id: &str,
        plan: SnapshotPlan<'_>,
    ) -> Result<SnapshotOutcome, WorkspaceError> {
        if matches!(plan, SnapshotPlan::None) {
            return Ok(SnapshotOutcome::Unchanged);
        }
        if workspace.adapter == "inplace" {
            return Err(WorkspaceError::SnapshotUnsupported(
                workspace.adapter.clone(),
            ));
        }
        match plan {
            SnapshotPlan::None => Ok(SnapshotOutcome::Unchanged),
            SnapshotPlan::CaptureInitial => {
                let name = "initial";
                match existing_initial_snapshot(self.state_runs, run_id)? {
                    Some(_) => Ok(SnapshotOutcome::Unchanged),
                    None => {
                        let path = take_snapshot(&workspace.path, self.state_runs, run_id, name)?;
                        Ok(SnapshotOutcome::Captured {
                            name: name.into(),
                            path,
                        })
                    }
                }
            }
            SnapshotPlan::KeepInitial => {
                existing_initial_snapshot(self.state_runs, run_id)?;
                Ok(SnapshotOutcome::Unchanged)
            }
            SnapshotPlan::Restore(name) => {
                restore_snapshot(&workspace.path, self.state_runs, run_id, name)?;
                Ok(SnapshotOutcome::Restored { name: name.into() })
            }
        }
    }
}

fn validate_snapshot_id(field: &'static str, value: &str) -> Result<(), WorkspaceError> {
    let mut components = Path::new(value).components();
    let valid = !value.is_empty()
        && value.len() <= 64
        && matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
        && !value.contains('\\');
    if valid {
        Ok(())
    } else {
        Err(WorkspaceError::InvalidSnapshotId {
            field,
            value: value.into(),
        })
    }
}

fn existing_initial_snapshot(
    state_runs: &Path,
    run_id: &str,
) -> Result<Option<PathBuf>, WorkspaceError> {
    let dest = snapshot_path(state_runs, run_id, "initial")?;
    match std::fs::symlink_metadata(&dest) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(WorkspaceError::UnsafeSnapshotPath)
        }
        Ok(_) => Ok(Some(dest)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn snapshot_path(state_runs: &Path, run_id: &str, name: &str) -> Result<PathBuf, WorkspaceError> {
    if !crate::checkpoint::is_safe_run_id(run_id) {
        return Err(WorkspaceError::InvalidSnapshotId {
            field: "run id",
            value: run_id.into(),
        });
    }
    validate_snapshot_id("name", name)?;
    let run_root = state_runs.join(run_id);
    let snapshots = run_root.join("snapshots");
    for ancestor in [&run_root, &snapshots] {
        if std::fs::symlink_metadata(ancestor)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Err(WorkspaceError::UnsafeSnapshotPath);
        }
    }
    Ok(snapshots.join(name))
}

/// Copy directory tree without following symlinks (workspace → snapshot).
pub fn copy_tree(src: &Path, dst: &Path) -> Result<(), WorkspaceError> {
    std::fs::create_dir_all(dst)?;
    copy_tree_nofollow(src, dst)
}

#[cfg(unix)]
struct DirFrame {
    fd: OwnedFd,
    dest: PathBuf,
    pending: std::vec::IntoIter<DirentNofollow>,
}

#[cfg(unix)]
fn copy_tree_nofollow(src: &Path, dst: &Path) -> Result<(), WorkspaceError> {
    let root = open_dir_nofollow(src)?;
    let pending = read_dir_nofollow(root.as_raw_fd())?;
    let mut stack = vec![DirFrame {
        fd: root,
        dest: dst.to_path_buf(),
        pending: pending.into_iter(),
    }];
    loop {
        let item = {
            let Some(frame) = stack.last_mut() else {
                break;
            };
            frame
                .pending
                .next()
                .map(|entry| (frame.fd.as_raw_fd(), frame.dest.clone(), entry))
        };
        let Some((dir_fd, dest_dir, entry)) = item else {
            stack.pop();
            continue;
        };
        if !dirent_name_allowed(&entry.name)? {
            continue;
        }
        match entry.kind {
            DirentKind::Symlink | DirentKind::Other => continue,
            DirentKind::Dir => {
                let to = dest_dir.join(&entry.name);
                std::fs::create_dir_all(&to)?;
                let child = openat_dir_nofollow(dir_fd, &entry.name)?;
                let pending = read_dir_nofollow(child.as_raw_fd())?;
                stack.push(DirFrame {
                    fd: child,
                    dest: to,
                    pending: pending.into_iter(),
                });
            }
            DirentKind::File => {
                let to = dest_dir.join(&entry.name);
                copy_file_at_nofollow(dir_fd, &entry.name, &to)?;
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn copy_tree_nofollow(src: &Path, dst: &Path) -> Result<(), WorkspaceError> {
    let mut stack = vec![src.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let metadata = std::fs::symlink_metadata(&dir)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(follow_refused(&dir));
        }
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                continue;
            }
            let from = entry.path();
            let rel = from.strip_prefix(src).unwrap_or(from.as_path());
            let to = dst.join(rel);
            if file_type.is_dir() {
                std::fs::create_dir_all(&to)?;
                stack.push(from);
            } else if file_type.is_file() {
                if let Some(parent) = to.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                copy_file_nofollow(&from, &to)?;
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn follow_refused(path: &Path) -> WorkspaceError {
    WorkspaceError::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("snapshot copy refused to follow `{}`", path.display()),
    ))
}

fn cannot_open(path: impl std::fmt::Display, error: io::Error) -> WorkspaceError {
    WorkspaceError::Io(io::Error::new(
        error.kind(),
        format!("snapshot copy cannot open `{path}`: {error}"),
    ))
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum DirentKind {
    Dir,
    File,
    Symlink,
    Other,
}

#[cfg(unix)]
struct DirentNofollow {
    name: std::ffi::OsString,
    kind: DirentKind,
}

#[cfg(unix)]
fn dirent_name_allowed(name: &std::ffi::OsStr) -> Result<bool, WorkspaceError> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes == b"." || bytes == b".." {
        return Ok(false);
    }
    if bytes.contains(&b'/') || bytes.contains(&0) {
        return Err(WorkspaceError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "snapshot copy refused an unsafe directory entry `{}`",
                name.to_string_lossy()
            ),
        )));
    }
    Ok(true)
}

#[cfg(unix)]
fn cstring_name(name: &std::ffi::OsStr) -> Result<std::ffi::CString, WorkspaceError> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(name.as_bytes()).map_err(|_| {
        WorkspaceError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "snapshot copy refused an interior NUL in `{}`",
                name.to_string_lossy()
            ),
        ))
    })
}

#[cfg(unix)]
fn open_dir_nofollow(path: &Path) -> Result<OwnedFd, WorkspaceError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|error| cannot_open(path.display(), error))?;
    Ok(OwnedFd::from(file))
}

#[cfg(unix)]
fn openat_dir_nofollow(dir_fd: i32, name: &std::ffi::OsStr) -> Result<OwnedFd, WorkspaceError> {
    let c_name = cstring_name(name)?;
    let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY;
    // SAFETY: `c_name` is a NUL-terminated file name; `dir_fd` is a live
    // directory descriptor opened with O_DIRECTORY.
    let fd = unsafe { libc::openat(dir_fd, c_name.as_ptr(), flags) };
    if fd < 0 {
        return Err(cannot_open(
            name.to_string_lossy(),
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `openat` returned a new owned descriptor on success.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(unix)]
fn copy_file_at_nofollow(
    dir_fd: i32,
    name: &std::ffi::OsStr,
    to: &Path,
) -> Result<(), WorkspaceError> {
    use std::fs::File;
    let c_name = cstring_name(name)?;
    let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
    // SAFETY: `c_name` is a NUL-terminated file name; `dir_fd` is a live
    // directory descriptor opened with O_DIRECTORY.
    let fd = unsafe { libc::openat(dir_fd, c_name.as_ptr(), flags) };
    if fd < 0 {
        return Err(cannot_open(
            name.to_string_lossy(),
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `openat` returned a new owned descriptor on success.
    let mut src = File::from(unsafe { OwnedFd::from_raw_fd(fd) });
    let metadata = src.metadata()?;
    if !metadata.is_file() {
        return Err(WorkspaceError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "snapshot copy requires a regular file `{}`",
                name.to_string_lossy()
            ),
        )));
    }
    let mut dst = OpenOptions::new().write(true).create_new(true).open(to)?;
    io::copy(&mut src, &mut dst)?;
    dst.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn zero_errno() {
    // SAFETY: errno is a thread-local slot; we only write the calling thread.
    unsafe {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            *libc::__errno_location() = 0;
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            *libc::__error() = 0;
        }
    }
}

#[cfg(unix)]
fn read_dir_nofollow(dir_fd: i32) -> Result<Vec<DirentNofollow>, WorkspaceError> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;

    // SAFETY: `dir_fd` is a live directory descriptor; `dup` clones it.
    let dup = unsafe { libc::dup(dir_fd) };
    if dup < 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: `dup` is an open directory fd; `fdopendir` takes ownership on success.
    let dirp = unsafe { libc::fdopendir(dup) };
    if dirp.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: `fdopendir` failed, so we still own `dup`.
        unsafe {
            libc::close(dup);
        }
        return Err(error.into());
    }
    struct CloseDir(*mut libc::DIR);
    impl Drop for CloseDir {
        fn drop(&mut self) {
            // SAFETY: `fdopendir` succeeded; `closedir` owns and closes the stream.
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let _dir = CloseDir(dirp);
    let mut entries = Vec::new();
    loop {
        zero_errno();
        // SAFETY: `dirp` is a live `DIR*` owned by `_dir` for this loop.
        let ent = unsafe { libc::readdir(dirp) };
        if ent.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(0) {
                break;
            }
            return Err(error.into());
        }
        // SAFETY: `readdir` returned a dirent whose `d_name` is NUL-terminated.
        let c_name = unsafe { CStr::from_ptr((*ent).d_name.as_ptr()) };
        let name = std::ffi::OsStr::from_bytes(c_name.to_bytes()).to_os_string();
        if name == "." || name == ".." {
            continue;
        }
        let c_name = cstring_name(&name)?;
        // SAFETY: `libc::stat` is a C POD struct; zero is a valid bit pattern
        // before `fstatat` fills it.
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        // SAFETY: `dir_fd` is a live directory descriptor; `c_name` is NUL-terminated.
        let rc = unsafe {
            libc::fstatat(
                dir_fd,
                c_name.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc != 0 {
            return Err(cannot_open(
                name.to_string_lossy(),
                io::Error::last_os_error(),
            ));
        }
        let mode = stat.st_mode & libc::S_IFMT;
        let kind = if mode == libc::S_IFLNK {
            DirentKind::Symlink
        } else if mode == libc::S_IFDIR {
            DirentKind::Dir
        } else if mode == libc::S_IFREG {
            DirentKind::File
        } else {
            DirentKind::Other
        };
        entries.push(DirentNofollow { name, kind });
    }
    Ok(entries)
}

#[cfg(any(test, not(unix)))]
fn copy_file_nofollow(from: &Path, to: &Path) -> Result<(), WorkspaceError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(not(unix))]
    {
        let metadata = std::fs::symlink_metadata(from)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(follow_refused(from));
        }
    }
    let mut src = options
        .open(from)
        .map_err(|error| cannot_open(from.display(), error))?;
    let metadata = src.metadata()?;
    if !metadata.is_file() {
        return Err(WorkspaceError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("snapshot copy requires a regular file `{}`", from.display()),
        )));
    }
    let mut dst = OpenOptions::new().write(true).create_new(true).open(to)?;
    io::copy(&mut src, &mut dst)?;
    dst.sync_all()?;
    Ok(())
}

/// Snapshot workspace into `state_runs/<run_id>/snapshots/<name>/`.
pub fn take_snapshot(
    workspace: &Path,
    state_runs: &Path,
    run_id: &str,
    name: &str,
) -> Result<PathBuf, WorkspaceError> {
    let dest = snapshot_path(state_runs, run_id, name)?;
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
    let src = snapshot_path(state_runs, run_id, name)?;
    if std::fs::symlink_metadata(&src).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(WorkspaceError::UnsafeSnapshotPath);
    }
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
        // Use the configured root itself (no nested shikigami-runs/<id>).
        // Hosts such as Aldunis Code pass a selected worktree path.
        "inplace" | "directory-inplace" => {
            if config.workspace.snapshot {
                return Err(WorkspaceError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "workspace.snapshot cannot be used with adapter `inplace`",
                )));
            }
            Ok(Box::new(InPlaceDirectoryWorkspace {
                root: PathBuf::from(&config.workspace.root),
            }))
        }
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

/// Workspace is exactly `root` (must already exist). No per-run subdirectory.
///
/// Safety: the harness state root must **not** live under `root` (otherwise
/// tools can read/write checkpoints). Snapshots are rejected for this adapter.
/// Hosts must serialize concurrent runs against the same root (no OS lock).
struct InPlaceDirectoryWorkspace {
    root: PathBuf,
}

fn path_is_within(parent: &Path, child: &Path) -> bool {
    let Ok(parent) = parent.canonicalize() else {
        return false;
    };
    let Ok(child) = child.canonicalize() else {
        return child.starts_with(&parent);
    };
    child.starts_with(&parent)
}

impl WorkspacePort for InPlaceDirectoryWorkspace {
    fn id(&self) -> &'static str {
        "inplace"
    }

    fn health_detail(&self) -> String {
        format!("inplace root={}", self.root.display())
    }

    fn materialize(
        &self,
        _run_id: &str,
        state_runs: &Path,
    ) -> Result<MaterializedWorkspace, WorkspaceError> {
        if !self.root.is_dir() {
            return Err(WorkspaceError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("inplace workspace root missing: {}", self.root.display()),
            )));
        }
        let path = std::fs::canonicalize(&self.root)?;
        if path_is_within(&path, state_runs) {
            return Err(WorkspaceError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "inplace workspace must not contain the harness state directory; \
                 set SHIKIGAMI_STATE / --state outside the workspace root",
            )));
        }
        Ok(MaterializedWorkspace {
            path,
            adapter: "inplace".into(),
            cleanup: WorkspaceCleanup::None,
        })
    }

    fn cleanup(&self, _ws: &MaterializedWorkspace) -> Result<(), WorkspaceError> {
        Ok(())
    }
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

    #[cfg(unix)]
    #[test]
    fn snapshot_does_not_follow_symlinks_outside_workspace() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let outside = dir.path().join("outside");
        let runs = dir.path().join("runs");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret.txt"), "outside").unwrap();
        symlink(outside.join("secret.txt"), ws.join("linked-file")).unwrap();
        symlink(&outside, ws.join("linked-directory")).unwrap();

        let snapshot = take_snapshot(&ws, &runs, "r1", "initial").unwrap();

        assert!(!snapshot.join("linked-file").exists());
        assert!(!snapshot.join("linked-directory").exists());
        assert!(!snapshot.join("linked-directory/secret.txt").exists());
    }

    #[test]
    fn restore_rejects_unsafe_names_before_mutating_workspace() {
        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let runs = dir.path().join("runs");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("keep.txt"), "keep").unwrap();

        for name in ["", ".", "..", "../outside", "/tmp/outside", "a/b", "a\\b"] {
            let error = restore_snapshot(&ws, &runs, "r1", name).unwrap_err();
            assert!(matches!(error, WorkspaceError::InvalidSnapshotId { .. }));
            assert_eq!(
                std::fs::read_to_string(ws.join("keep.txt")).unwrap(),
                "keep"
            );
        }
    }

    #[test]
    fn snapshot_accepts_canonical_maximum_length_run_id() {
        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let runs = dir.path().join("runs");
        let run_id = "a".repeat(128);
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("file.txt"), "content").unwrap();

        let path = take_snapshot(&ws, &runs, &run_id, "initial").unwrap();

        assert!(path.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn restore_rejects_symlinked_snapshot_before_mutating_workspace() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let runs = dir.path().join("runs");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(ws.join("keep.txt"), "keep").unwrap();
        std::fs::write(outside.join("secret.txt"), "secret").unwrap();
        std::fs::create_dir_all(runs.join("r1/snapshots")).unwrap();
        symlink(&outside, runs.join("r1/snapshots/initial")).unwrap();

        let error = restore_snapshot(&ws, &runs, "r1", "initial").unwrap_err();

        assert!(matches!(error, WorkspaceError::UnsafeSnapshotPath));
        assert_eq!(
            std::fs::read_to_string(ws.join("keep.txt")).unwrap(),
            "keep"
        );
    }

    #[test]
    fn snapshot_module_restores_before_callers_observe_workspace() {
        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let runs = dir.path().join("runs");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("AGENTS.md"), "restored rules").unwrap();
        take_snapshot(&ws, &runs, "r1", "initial").unwrap();
        std::fs::write(ws.join("AGENTS.md"), "current rules").unwrap();
        let materialized = MaterializedWorkspace {
            path: ws.clone(),
            adapter: "directory".into(),
            cleanup: WorkspaceCleanup::None,
        };

        let outcome = WorkspaceSnapshots::new(&runs)
            .prepare(&materialized, "r1", SnapshotPlan::Restore("initial"))
            .unwrap();

        assert_eq!(
            outcome,
            SnapshotOutcome::Restored {
                name: "initial".into()
            }
        );
        assert_eq!(
            std::fs::read_to_string(ws.join("AGENTS.md")).unwrap(),
            "restored rules"
        );
    }

    #[test]
    fn capture_initial_preserves_the_first_snapshot_across_later_prepares() {
        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let runs = dir.path().join("runs");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("a.txt"), "original").unwrap();
        let materialized = MaterializedWorkspace {
            path: ws.clone(),
            adapter: "directory".into(),
            cleanup: WorkspaceCleanup::None,
        };
        let snapshots = WorkspaceSnapshots::new(&runs);

        let first = snapshots
            .prepare(&materialized, "r1", SnapshotPlan::CaptureInitial)
            .unwrap();
        assert!(matches!(first, SnapshotOutcome::Captured { .. }));
        std::fs::write(ws.join("a.txt"), "mutated").unwrap();
        let second = snapshots
            .prepare(&materialized, "r1", SnapshotPlan::CaptureInitial)
            .unwrap();
        assert_eq!(second, SnapshotOutcome::Unchanged);
        assert_eq!(
            std::fs::read_to_string(runs.join("r1/snapshots/initial/a.txt")).unwrap(),
            "original"
        );
    }

    #[test]
    fn keep_initial_does_not_capture_from_a_mutated_workspace() {
        let dir = tempdir().unwrap();
        let ws = dir.path().join("ws");
        let runs = dir.path().join("runs");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("a.txt"), "mutated").unwrap();
        let materialized = MaterializedWorkspace {
            path: ws,
            adapter: "directory".into(),
            cleanup: WorkspaceCleanup::None,
        };

        let outcome = WorkspaceSnapshots::new(&runs)
            .prepare(&materialized, "r1", SnapshotPlan::KeepInitial)
            .unwrap();

        assert_eq!(outcome, SnapshotOutcome::Unchanged);
        assert!(!runs.join("r1/snapshots/initial").exists());
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_copy_does_not_follow_a_swapped_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let from = dir.path().join("from.txt");
        let to = dir.path().join("to.txt");
        let outside = dir.path().join("outside.txt");
        std::fs::write(&outside, "secret-outside").unwrap();
        symlink(&outside, &from).unwrap();

        let error = copy_file_nofollow(&from, &to).unwrap_err();
        assert!(
            error.to_string().contains("cannot open")
                || error.to_string().contains("refused")
                || error.to_string().contains("regular file"),
            "{error}"
        );
        assert!(!to.exists());
    }

    fn dest_contains_bytes(root: &Path, needle: &[u8]) -> bool {
        let Ok(metadata) = std::fs::symlink_metadata(root) else {
            return false;
        };
        if metadata.file_type().is_symlink() {
            return false;
        }
        if metadata.is_file() {
            return std::fs::read(root)
                .is_ok_and(|bytes| bytes.windows(needle.len()).any(|w| w == needle));
        }
        if !metadata.is_dir() {
            return false;
        }
        let Ok(entries) = std::fs::read_dir(root) else {
            return false;
        };
        entries
            .flatten()
            .any(|entry| dest_contains_bytes(&entry.path(), needle))
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_copy_does_not_follow_a_swapped_directory() {
        use std::os::unix::fs::symlink;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;

        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        let nested = src.join("nested");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(nested.join("inside.txt"), "inside").unwrap();
        std::fs::write(outside.join("secret.txt"), "secret-outside").unwrap();
        for i in 0..64 {
            std::fs::write(src.join(format!("pad-{i}.txt")), "pad").unwrap();
        }

        let stop = Arc::new(AtomicBool::new(false));
        let nested_for_thread = nested.clone();
        let outside_for_thread = outside.clone();
        let stop_for_thread = stop.clone();
        let swapper = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::Relaxed) {
                let _ = std::fs::remove_dir_all(&nested_for_thread);
                let _ = std::fs::remove_file(&nested_for_thread);
                let _ = symlink(&outside_for_thread, &nested_for_thread);
            }
        });

        let _ = copy_tree(&src, &dst);
        stop.store(true, Ordering::Relaxed);
        swapper.join().unwrap();

        assert!(
            !dest_contains_bytes(&dst, b"secret-outside"),
            "copy_tree followed a directory swapped to a symlink"
        );
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_copy_preserves_unix_backslash_filenames() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("weird\\name.txt"), "keep").unwrap();
        copy_tree(&src, &dst).unwrap();
        assert_eq!(
            std::fs::read_to_string(dst.join("weird\\name.txt")).unwrap(),
            "keep"
        );
    }
}
