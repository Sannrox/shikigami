//! Background bash job bookkeeping and bash output caps.

use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) const MAX_BASH_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_BG_JOBS: usize = 4;
/// Exited jobs kept for `bash_job_status` / `bash_job_logs` in a long session.
const MAX_BG_JOB_RECORDS: usize = 32;
pub(crate) const MAX_BG_LOG_BYTES: usize = 256 * 1024;

/// Background jobs of one run. A session host moves the table into the run
/// registry across a park ([`crate::registry::RunRegistry`]); dropping it
/// kills every job.
#[derive(Debug)]
pub(crate) struct BackgroundJobs {
    pub(crate) next_id: u64,
    pub(crate) jobs: HashMap<String, BgJob>,
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

impl BackgroundJobs {
    pub(crate) fn new() -> Self {
        Self {
            next_id: 1,
            jobs: HashMap::new(),
        }
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
