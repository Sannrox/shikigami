//! Background bash job bookkeeping and bash output caps.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub(crate) const MAX_BASH_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_BG_JOBS: usize = 4;
/// Exited jobs kept for `bash_job_status` / `bash_job_logs` in a long session.
const MAX_BG_JOB_RECORDS: usize = 32;
pub(crate) const MAX_BG_LOG_BYTES: usize = 256 * 1024;

pub(crate) fn create_job_log(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).read(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Background jobs of one run. A session host moves the table into the run
/// registry across a park ([`crate::registry::RunRegistry`]); dropping it
/// kills every job and removes its host-owned log directory.
#[derive(Debug)]
pub(crate) struct BackgroundJobs {
    pub(crate) next_id: u64,
    pub(crate) jobs: HashMap<String, BgJob>,
    /// Host scratch for job stdout/stderr. Created on the first job, parked
    /// with the table, never under the workspace (skills live in `.shikigami/`).
    log_dir: Option<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct BgJob {
    pub(crate) child: tokio::process::Child,
    pub(crate) log_path: PathBuf,
    pub(crate) _applied: crate::sandbox::AppliedSandbox,
    /// Process group the sandbox started the job in. Cleared once the shell
    /// is reaped (see [`BgJob::try_wait`]).
    pub(crate) process_group: Option<u32>,
}

impl BgJob {
    /// Poll the shell. A job runs until its shell exits. The first poll that
    /// sees the exit reaps the shell and kills what is left of its group in
    /// the same step: only then is the group id certain to still be ours,
    /// since a reaped, empty group's id can be recycled.
    pub(crate) fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let status = self.child.try_wait()?;
        if status.is_some() {
            crate::sandbox::kill_process_group(self.process_group.take());
        }
        Ok(status)
    }

    fn is_running(&mut self) -> bool {
        !matches!(self.try_wait(), Ok(Some(_)))
    }

    /// Best-effort SIGKILL of the job and, when sandboxed, its descendants.
    pub(crate) fn kill(&mut self) {
        if self.is_running() {
            crate::sandbox::kill_process_group(self.process_group.take());
            let _ = self.child.start_kill();
        }
    }
}

impl Drop for BgJob {
    fn drop(&mut self) {
        self.kill();
    }
}

impl Drop for BackgroundJobs {
    fn drop(&mut self) {
        self.jobs.clear();
        if let Some(dir) = self.log_dir.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

impl BackgroundJobs {
    pub(crate) fn new() -> Self {
        Self {
            next_id: 1,
            jobs: HashMap::new(),
            log_dir: None,
        }
    }

    /// Host-owned directory for this table's log files. Outside the workspace
    /// so an inplace `git add -A` cannot commit harness output.
    pub(crate) fn ensure_log_dir(&mut self) -> std::io::Result<&Path> {
        if self.log_dir.is_none() {
            let path =
                std::env::temp_dir().join(format!("shikigami-jobs-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
            }
            self.log_dir = Some(path);
        }
        self.log_dir
            .as_deref()
            .ok_or_else(|| std::io::Error::other("background job log directory missing"))
    }

    /// Jobs whose shell has not exited. Only these count against
    /// [`MAX_BG_JOBS`]: a session keeps exited jobs readable across prompts.
    pub(crate) fn running(&mut self) -> usize {
        self.jobs
            .values_mut()
            .filter_map(|job| job.is_running().then_some(()))
            .count()
    }

    /// Make room for one more record by dropping the oldest exited job once
    /// the table holds [`MAX_BG_JOB_RECORDS`].
    pub(crate) fn forget_oldest_exited(&mut self) {
        if self.jobs.len() < MAX_BG_JOB_RECORDS {
            return;
        }
        let oldest = self
            .jobs
            .iter_mut()
            .filter_map(|(id, job)| (!job.is_running()).then_some(id))
            .filter_map(|id| Some((id.strip_prefix("job-")?.parse::<u64>().ok()?, id.clone())))
            .min();
        if let Some((_, id)) = oldest {
            self.jobs.remove(&id);
        }
    }
}
