//! OS-level child process limits.
//!
//! This is deliberately a small, explicit backend. Workspace path-jailing and
//! network policy remain separate controls; selecting `rlimit` only limits the
//! child process and its process group on Unix.

use std::io;

use thiserror::Error;
use tokio::process::Command;

use crate::config::{SandboxBackend, SandboxSettings};

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox backend `{0:?}` is unavailable on this platform")]
    Unsupported(SandboxBackend),
    #[error("sandbox configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct Sandbox {
    settings: SandboxSettings,
}

impl Sandbox {
    pub fn new(settings: SandboxSettings) -> Result<Self, SandboxError> {
        if matches!(settings.backend, SandboxBackend::Rlimit) && !cfg!(unix) {
            return Err(SandboxError::Unsupported(settings.backend));
        }
        Ok(Self { settings })
    }

    pub fn apply(&self, command: &mut Command) -> Result<(), SandboxError> {
        match self.settings.backend {
            SandboxBackend::None => Ok(()),
            SandboxBackend::Rlimit => self.apply_rlimit(command),
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
            });
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn apply_rlimit(&self, _command: &mut Command) -> Result<(), SandboxError> {
        Err(SandboxError::Unsupported(SandboxBackend::Rlimit))
    }

    /// Kill the process group created by the sandbox backend. This is a
    /// best-effort cleanup path used for timeout and run shutdown.
    pub fn kill_process_group(&self, pid: Option<u32>) {
        #[cfg(unix)]
        if matches!(self.settings.backend, SandboxBackend::Rlimit)
            && let Some(pid) = pid
            && pid > 1
        {
            // Negative pid targets the process group. Ignore ESRCH: the child
            // may have already exited between the timeout and this call.
            unsafe {
                let _ = libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
            }
        }
    }
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
}
