//! OS-level child process isolation, mapped onto the pinned `kekkai` library.
//!
//! Settings names, CLI, and doctor lines stay here. Enforcement lives in
//! kekkai (ADR 0020). Backends:
//! - `none`: no isolation (local development; doctor warns).
//! - `rlimit`: Unix resource limits and a process group (doctor: `limits`).
//! - `linux_native`: Landlock filesystem rules plus a seccomp socket gate
//!   applied in `pre_exec` (ADR 0013).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;
use tokio::process::Command;

use crate::config::{SandboxBackend, SandboxSettings};

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox backend `{0:?}` is unavailable on this platform")]
    Unsupported(SandboxBackend),
    #[error("sandbox unavailable: {0}")]
    Unavailable(String),
    #[error("sandbox configuration: {0}")]
    Invalid(String),
}

/// Doctor-facing health of the configured sandbox backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxHealth {
    Ok(String),
    Warning(String),
    Error(String),
}

/// Prepared kekkai backend for one spawned child.
///
/// Keep this value until that child exits. kekkai records seccomp denials
/// with a per-`apply` supervisor; dropping this releases those sockets
/// instead of accumulating them on a reused sandbox.
#[derive(Debug)]
#[must_use]
pub struct AppliedSandbox {
    inner: Option<Arc<kekkai::Sandbox>>,
}

impl Drop for AppliedSandbox {
    fn drop(&mut self) {
        // Dropping the kekkai sandbox closes per-apply seccomp notif sockets
        // and supervisor threads. Callers keep this value until the child exits.
        drop(self.inner.take());
    }
}

#[derive(Debug)]
struct RunScratch(PathBuf);

impl Drop for RunScratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Clone)]
pub struct Sandbox {
    settings: SandboxSettings,
    workspace: Option<PathBuf>,
    host: Option<kekkai::Host>,
    scratch: Option<Arc<RunScratch>>,
    frozen_read_only: Vec<PathBuf>,
}

impl Sandbox {
    pub fn new(settings: SandboxSettings) -> Result<Self, SandboxError> {
        Self::for_workspace(settings, None)
    }

    pub fn for_workspace(
        settings: SandboxSettings,
        workspace: Option<&Path>,
    ) -> Result<Self, SandboxError> {
        match health(&settings) {
            SandboxHealth::Error(message) => {
                let unsupported = matches!(
                    settings.backend,
                    SandboxBackend::LinuxNative | SandboxBackend::Rlimit
                ) && !cfg!(target_os = "linux")
                    && !(cfg!(unix) && matches!(settings.backend, SandboxBackend::Rlimit));
                return Err(if unsupported {
                    SandboxError::Unsupported(settings.backend)
                } else {
                    SandboxError::Unavailable(message)
                });
            }
            SandboxHealth::Ok(_) | SandboxHealth::Warning(_) => {}
        }
        if matches!(settings.backend, SandboxBackend::LinuxNative) && workspace.is_none() {
            return Err(SandboxError::Invalid(
                "linux_native requires a workspace root to build Landlock rules".into(),
            ));
        }
        let host = match settings.backend {
            SandboxBackend::None => None,
            SandboxBackend::Rlimit | SandboxBackend::LinuxNative => Some(kekkai::Host::probe()),
        };
        let (workspace, frozen_read_only, scratch) =
            if matches!(settings.backend, SandboxBackend::LinuxNative) {
                let workspace = workspace.ok_or_else(|| {
                    SandboxError::Invalid(
                        "linux_native requires a workspace root to build Landlock rules".into(),
                    )
                })?;
                let workspace = fs::canonicalize(workspace).map_err(|error| {
                    SandboxError::Invalid(format!(
                        "linux_native workspace {}: {error}",
                        workspace.display()
                    ))
                })?;
                let frozen_read_only = freeze_read_only_paths(&settings.read_only_paths)?;
                (
                    Some(workspace),
                    frozen_read_only,
                    Some(Arc::new(create_run_scratch()?)),
                )
            } else {
                (workspace.map(Path::to_path_buf), Vec::new(), None)
            };
        Ok(Self {
            settings,
            workspace,
            host,
            scratch,
            frozen_read_only,
        })
    }

    pub fn apply(&self, command: &mut Command) -> Result<AppliedSandbox, SandboxError> {
        match self.settings.backend {
            SandboxBackend::None => Ok(AppliedSandbox { inner: None }),
            SandboxBackend::Rlimit | SandboxBackend::LinuxNative => {
                let root = self.workspace.as_deref().unwrap_or_else(|| Path::new("."));
                let scratch = self.scratch.as_ref().map(|s| s.0.as_path());
                let mapped = mapped_settings(
                    &self.settings,
                    scratch,
                    &grantable_read_only(
                        &self.frozen_read_only,
                        self.workspace.as_deref(),
                        scratch,
                    ),
                );
                let prepared = match &self.host {
                    Some(host) => kekkai::Sandbox::prepare_with_host(host, &mapped, root),
                    None => kekkai::Sandbox::prepare(&mapped, root),
                }
                .map_err(map_error)?;
                prepared.apply(command.as_std_mut()).map_err(map_error)?;
                if let Some(scratch) = &self.scratch {
                    command.env("TMPDIR", &scratch.0);
                    command.env("TMP", &scratch.0);
                    command.env("TEMP", &scratch.0);
                }
                Ok(AppliedSandbox {
                    inner: Some(Arc::new(prepared)),
                })
            }
        }
    }

    /// Kill the process group created by the sandbox backend. This is a
    /// best-effort cleanup path used for timeout and run shutdown.
    pub fn kill_process_group(&self, pid: Option<u32>) {
        #[cfg(unix)]
        if matches!(
            self.settings.backend,
            SandboxBackend::Rlimit | SandboxBackend::LinuxNative
        ) && let Some(pid) = pid
            && pid > 1
        {
            // Negative pid targets the process group. Ignore ESRCH: the child
            // may have already exited between the timeout and this call.
            unsafe {
                let _ = libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
            }
        }
    }

    pub fn scratch_dir(&self) -> Option<&Path> {
        self.scratch.as_ref().map(|scratch| scratch.0.as_path())
    }
}

/// Probe the configured backend without constructing a run-scoped ruleset.
pub fn health(settings: &SandboxSettings) -> SandboxHealth {
    match settings.backend {
        SandboxBackend::None => SandboxHealth::Warning(
            "sandbox:   backend=none — no OS isolation for tool children".into(),
        ),
        SandboxBackend::Rlimit => {
            if cfg!(unix) {
                SandboxHealth::Ok(format!(
                    "sandbox:   backend=limits cpu={:?} memory_mb={:?} user_processes={:?} (no OS isolation)",
                    settings.cpu_time_secs, settings.memory_mb, settings.user_processes
                ))
            } else {
                SandboxHealth::Error("sandbox.backend=rlimit is supported only on Unix".into())
            }
        }
        SandboxBackend::LinuxNative => linux_native_health(settings),
    }
}

fn linux_native_health(settings: &SandboxSettings) -> SandboxHealth {
    if !cfg!(target_os = "linux") {
        return SandboxHealth::Error(
            "sandbox.backend=linux_native is supported only on Linux".into(),
        );
    }
    match kekkai::health(&mapped_settings(settings, None, &[])) {
        kekkai::Health::Ok(message) => {
            let abi = linux_native_abi(&message);
            let signal = if abi >= 6 { "landlock" } else { "none" };
            SandboxHealth::Ok(format!(
                "sandbox:   backend=linux_native abi={abi} tool_sockets=deny signal_scoping={signal} writable=workspace+scratch cpu={:?} memory_mb={:?} user_processes={:?} read_only_paths={}",
                settings.cpu_time_secs,
                settings.memory_mb,
                settings.user_processes,
                settings.read_only_paths.len()
            ))
        }
        kekkai::Health::Warning(message) | kekkai::Health::Error(message) => {
            SandboxHealth::Error(message)
        }
        _ => SandboxHealth::Error("sandbox backend linux_native is unavailable".into()),
    }
}

fn linux_native_abi(health_ok: &str) -> u32 {
    health_ok
        .split_once("abi=")
        .and_then(|(_, rest)| rest.split([' ', ';']).next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn mapped_settings(
    settings: &SandboxSettings,
    run_scratch: Option<&Path>,
    frozen_read_only: &[PathBuf],
) -> kekkai::Settings {
    let backend = match settings.backend {
        SandboxBackend::None => kekkai::Backend::None,
        SandboxBackend::Rlimit => kekkai::Backend::Rlimit,
        SandboxBackend::LinuxNative => kekkai::Backend::LinuxNative,
    };
    let mut builder = kekkai::Settings::builder()
        .backend(backend)
        .limits(kekkai::Limits {
            cpu_time_secs: settings.cpu_time_secs,
            memory_mb: settings.memory_mb,
            user_processes: settings.user_processes,
            file_size_mb: settings.file_size_mb,
            open_files: settings.open_files,
        })
        .network(kekkai::Network::Deny);
    if matches!(settings.backend, SandboxBackend::LinuxNative) {
        builder = builder.readable_roots(frozen_read_only.to_vec());
        if let Some(scratch) = run_scratch {
            builder = builder.writable_roots(vec![scratch.to_path_buf()]);
        }
    }
    builder.build()
}

/// Resolve `read_only_paths` once at construction.
///
/// kekkai canonicalizes readable roots on every `prepare`. Extra grants
/// must be the original targets, not live workspace paths that a later
/// command can replace.
fn freeze_read_only_paths(paths: &[String]) -> Result<Vec<PathBuf>, SandboxError> {
    paths
        .iter()
        .map(|path| {
            fs::canonicalize(path).map_err(|error| {
                SandboxError::Invalid(format!("sandbox.read_only_paths `{path}`: {error}"))
            })
        })
        .collect()
}

/// Drop extra grants whose path can be replaced through a writable root.
///
/// The workspace and scratch are already writable. Passing them (or a
/// child) as `readable_roots` lets the next `prepare` follow a replacement
/// symlink and widen Landlock read/execute. Outside those trees the child
/// cannot replace the path, so re-canonicalizing the frozen target is safe.
fn grantable_read_only(
    frozen: &[PathBuf],
    workspace: Option<&Path>,
    scratch: Option<&Path>,
) -> Vec<PathBuf> {
    frozen
        .iter()
        .filter(|path| !writable_covers(path, workspace) && !writable_covers(path, scratch))
        .cloned()
        .collect()
}

fn writable_covers(path: &Path, root: Option<&Path>) -> bool {
    root.is_some_and(|root| path.strip_prefix(root).is_ok())
}

fn create_run_scratch() -> Result<RunScratch, SandboxError> {
    let path = std::env::temp_dir().join(format!("shikigami-scratch-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&path)
        .map_err(|error| SandboxError::Invalid(format!("scratch directory: {error}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .map_err(|error| SandboxError::Invalid(format!("scratch directory: {error}")))?;
    }
    let path = fs::canonicalize(&path)
        .map_err(|error| SandboxError::Invalid(format!("scratch directory: {error}")))?;
    Ok(RunScratch(path))
}

fn map_error(error: kekkai::Error) -> SandboxError {
    match error {
        kekkai::Error::Unsupported { backend, .. } => {
            SandboxError::Unsupported(map_backend_back(backend))
        }
        kekkai::Error::Unavailable { message, .. } => SandboxError::Unavailable(message),
        kekkai::Error::Invalid { message } => SandboxError::Invalid(message),
        other => SandboxError::Unavailable(other.to_string()),
    }
}

fn map_backend_back(backend: kekkai::Backend) -> SandboxBackend {
    match backend {
        kekkai::Backend::Rlimit => SandboxBackend::Rlimit,
        kekkai::Backend::LinuxNative => SandboxBackend::LinuxNative,
        _ => SandboxBackend::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sandbox_is_a_noop() {
        let sandbox = Sandbox::new(SandboxSettings::default()).unwrap();
        let mut command = Command::new("true");
        let _applied = sandbox.apply(&mut command).unwrap();
    }

    #[test]
    fn none_backend_is_a_doctor_warning() {
        assert!(matches!(
            health(&SandboxSettings::default()),
            SandboxHealth::Warning(message) if message.contains("backend=none")
        ));
    }

    #[test]
    fn doctor_lines_stay_stable_for_none_and_rlimit() {
        assert_eq!(
            health(&SandboxSettings::default()),
            SandboxHealth::Warning(
                "sandbox:   backend=none — no OS isolation for tool children".into()
            )
        );
        let limits = SandboxSettings {
            backend: SandboxBackend::Rlimit,
            cpu_time_secs: Some(1),
            ..SandboxSettings::default()
        };
        if cfg!(unix) {
            assert_eq!(
                health(&limits),
                SandboxHealth::Ok(
                    "sandbox:   backend=limits cpu=Some(1) memory_mb=None user_processes=None (no OS isolation)"
                        .into()
                )
            );
        } else {
            assert_eq!(
                health(&limits),
                SandboxHealth::Error("sandbox.backend=rlimit is supported only on Unix".into())
            );
        }
        let native = SandboxSettings {
            backend: SandboxBackend::LinuxNative,
            ..SandboxSettings::default()
        };
        if !cfg!(target_os = "linux") {
            assert_eq!(
                health(&native),
                SandboxHealth::Error(
                    "sandbox.backend=linux_native is supported only on Linux".into()
                )
            );
        }
    }

    #[test]
    fn linux_native_health_matches_the_host() {
        let settings = SandboxSettings {
            backend: SandboxBackend::LinuxNative,
            ..SandboxSettings::default()
        };
        let report = health(&settings);
        if cfg!(target_os = "linux") {
            assert!(
                matches!(
                    report,
                    SandboxHealth::Ok(ref message) if message.contains("linux_native")
                        && message.contains("tool_sockets=deny")
                        && message.contains("writable=workspace+scratch")
                ) || matches!(report, SandboxHealth::Error(_)),
                "{report:?}"
            );
        } else {
            assert!(
                matches!(report, SandboxHealth::Error(ref message) if message.contains("Linux")),
                "{report:?}"
            );
        }
    }

    #[test]
    fn freeze_read_only_paths_keeps_the_original_symlink_target() {
        let root = tempfile::tempdir().unwrap();
        let granted = root.path().join("granted");
        let decoy = root.path().join("decoy");
        let link = root.path().join("link");
        fs::create_dir(&granted).unwrap();
        fs::create_dir(&decoy).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&granted, &link).unwrap();
        #[cfg(not(unix))]
        std::os::windows::fs::symlink_dir(&granted, &link).unwrap();

        let frozen = freeze_read_only_paths(&[link.to_string_lossy().into_owned()]).unwrap();
        assert_eq!(frozen, vec![fs::canonicalize(&granted).unwrap()]);

        fs::remove_file(&link).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&decoy, &link).unwrap();
        #[cfg(not(unix))]
        std::os::windows::fs::symlink_dir(&decoy, &link).unwrap();

        assert_eq!(frozen, vec![fs::canonicalize(&granted).unwrap()]);
        assert_ne!(
            fs::canonicalize(&link).unwrap(),
            fs::canonicalize(&granted).unwrap()
        );
    }

    #[test]
    fn grantable_read_only_drops_paths_under_writable_roots() {
        let root = tempfile::tempdir().unwrap();
        let workspace = fs::canonicalize(root.path()).unwrap();
        let inside = workspace.join("allowed");
        fs::create_dir(&inside).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside = fs::canonicalize(outside.path()).unwrap();
        let scratch = workspace.join("scratch");
        fs::create_dir(&scratch).unwrap();
        let under_scratch = scratch.join("tmp");
        fs::create_dir(&under_scratch).unwrap();

        let frozen = vec![inside.clone(), outside.clone(), under_scratch.clone()];
        let grantable = grantable_read_only(&frozen, Some(&workspace), Some(&scratch));
        assert_eq!(grantable, vec![outside]);
    }

    #[test]
    fn linux_native_construction_without_workspace_fails() {
        let settings = SandboxSettings {
            backend: SandboxBackend::LinuxNative,
            ..SandboxSettings::default()
        };
        if cfg!(target_os = "linux") {
            let error = Sandbox::new(settings).unwrap_err();
            assert!(
                error.to_string().contains("workspace")
                    || error.to_string().contains("unavailable")
                    || error.to_string().contains("Landlock"),
                "{error}"
            );
        } else {
            let error = Sandbox::new(settings).unwrap_err();
            assert!(
                matches!(
                    error,
                    SandboxError::Unsupported(SandboxBackend::LinuxNative)
                ),
                "{error}"
            );
        }
    }
}
