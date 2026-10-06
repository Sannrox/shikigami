//! OS-level child process isolation.
//!
//! Backends:
//! - `none`: no isolation (local development; doctor warns).
//! - `rlimit`: Unix resource limits and a process group (doctor: `limits`).
//! - `linux_native`: Landlock filesystem rules plus a seccomp socket gate
//!   applied in `pre_exec` (ADR 0013).

use std::io;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::sync::Arc;

use thiserror::Error;
use tokio::process::Command;

use crate::config::{SandboxBackend, SandboxSettings};

#[cfg(target_os = "linux")]
mod linux;

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

#[derive(Debug, Clone)]
pub struct Sandbox {
    settings: SandboxSettings,
    #[cfg(target_os = "linux")]
    linux: Option<Arc<linux::LinuxPrepared>>,
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
        #[cfg(target_os = "linux")]
        let linux = if matches!(settings.backend, SandboxBackend::LinuxNative) {
            let workspace = workspace.ok_or_else(|| {
                SandboxError::Invalid(
                    "linux_native requires a workspace root to build Landlock rules".into(),
                )
            })?;
            Some(linux::prepare(&settings, workspace)?)
        } else {
            None
        };
        let _ = workspace;
        Ok(Self {
            settings,
            #[cfg(target_os = "linux")]
            linux,
        })
    }

    pub fn apply(&self, command: &mut Command) -> Result<(), SandboxError> {
        match self.settings.backend {
            SandboxBackend::None => Ok(()),
            SandboxBackend::Rlimit => self.apply_rlimit(command),
            SandboxBackend::LinuxNative => self.apply_linux_native(command),
        }
    }

    #[cfg(unix)]
    fn apply_rlimit(&self, command: &mut Command) -> Result<(), SandboxError> {
        use std::os::unix::process::CommandExt;

        let settings = self.settings.clone();
        // `pre_exec` runs in the forked child, before exec. The closure only
        // performs async-signal-safe libc calls and does not allocate.
        unsafe {
            command.as_std_mut().pre_exec(move || {
                if libc::setpgid(0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                apply_rlimit_values(&settings)
            });
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn apply_rlimit(&self, _command: &mut Command) -> Result<(), SandboxError> {
        Err(SandboxError::Unsupported(SandboxBackend::Rlimit))
    }

    #[cfg(target_os = "linux")]
    fn apply_linux_native(&self, command: &mut Command) -> Result<(), SandboxError> {
        let prepared = self
            .linux
            .clone()
            .ok_or_else(|| SandboxError::Invalid("linux_native ruleset was not prepared".into()))?;
        linux::apply(command, &self.settings, prepared)
    }

    #[cfg(not(target_os = "linux"))]
    fn apply_linux_native(&self, _command: &mut Command) -> Result<(), SandboxError> {
        Err(SandboxError::Unsupported(SandboxBackend::LinuxNative))
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
        #[cfg(target_os = "linux")]
        {
            self.linux
                .as_ref()
                .map(|prepared| prepared.scratch.as_path())
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
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

#[cfg(target_os = "linux")]
fn linux_native_health(settings: &SandboxSettings) -> SandboxHealth {
    match linux::probe_enforcement() {
        Ok(abi) => {
            let signal = if abi >= 6 { "landlock" } else { "none" };
            SandboxHealth::Ok(format!(
                "sandbox:   backend=linux_native abi={abi} tool_sockets=deny signal_scoping={signal} writable=workspace+scratch cpu={:?} memory_mb={:?} user_processes={:?} read_only_paths={}",
                settings.cpu_time_secs,
                settings.memory_mb,
                settings.user_processes,
                settings.read_only_paths.len()
            ))
        }
        Err(error) => SandboxHealth::Error(error.to_string()),
    }
}

#[cfg(not(target_os = "linux"))]
fn linux_native_health(_settings: &SandboxSettings) -> SandboxHealth {
    SandboxHealth::Error("sandbox.backend=linux_native is supported only on Linux".into())
}

#[cfg(unix)]
fn apply_rlimit_values(settings: &SandboxSettings) -> io::Result<()> {
    set_limit(libc::RLIMIT_CPU, settings.cpu_time_secs)?;
    set_limit(
        libc::RLIMIT_AS,
        settings
            .memory_mb
            .map(|value| value.saturating_mul(1024 * 1024)),
    )?;
    set_limit(libc::RLIMIT_NPROC, settings.user_processes)?;
    set_limit(
        libc::RLIMIT_FSIZE,
        settings
            .file_size_mb
            .map(|value| value.saturating_mul(1024 * 1024)),
    )?;
    set_limit(libc::RLIMIT_NOFILE, settings.open_files)?;
    Ok(())
}

#[cfg(unix)]
fn set_limit(resource: RlimitResource, limit: Option<u64>) -> io::Result<()> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let value = libc::rlimit {
        rlim_cur: limit as libc::rlim_t,
        rlim_max: limit as libc::rlim_t,
    };
    // SAFETY: `value` is initialized and the resource constants are supplied
    // by libc for this target.
    if unsafe { libc::setrlimit(resource as _, &value) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
type RlimitResource = libc::c_uint;

#[cfg(all(unix, not(target_os = "linux")))]
type RlimitResource = libc::c_int;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sandbox_is_a_noop() {
        let sandbox = Sandbox::new(SandboxSettings::default()).unwrap();
        let mut command = Command::new("true");
        sandbox.apply(&mut command).unwrap();
    }

    #[test]
    fn none_backend_is_a_doctor_warning() {
        assert!(matches!(
            health(&SandboxSettings::default()),
            SandboxHealth::Warning(message) if message.contains("backend=none")
        ));
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
