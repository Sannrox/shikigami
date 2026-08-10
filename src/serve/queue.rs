use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use uuid::Uuid;

use super::{HealthStatus, QueueJob, QueueLayout, QueueResult, ServeError, ServeRuntimeOptions};
use crate::harness::Harness;
use crate::run::{RunRequest, RunResult, RunTermination};

/// Private deep module for the filesystem queue lifecycle.
///
/// The serve host and control adapter use this interface instead of learning
/// atomic admission, claim naming, retry, archival, and observation invariants.
#[derive(Clone)]
pub(super) struct FilesystemQueue {
    layout: QueueLayout,
}

pub(super) enum Admission {
    Accepted(QueueJob),
    MissingTask,
    Full,
}

impl FilesystemQueue {
    pub(super) fn new(layout: &QueueLayout) -> Self {
        Self {
            layout: layout.clone(),
        }
    }

    pub(super) fn ensure(&self) -> Result<(), ServeError> {
        self.layout.ensure()
    }

    pub(super) fn admit(
        &self,
        mut job: QueueJob,
        capacity: usize,
    ) -> Result<Admission, ServeError> {
        let _admission = self
            .layout
            .admission_lock
            .lock()
            .map_err(|_| ServeError::Message("queue admission lock poisoned".into()))?;
        if job.task.trim().is_empty() {
            return Ok(Admission::MissingTask);
        }
        if self.depth_unlocked()? >= capacity.max(1) {
            return Ok(Admission::Full);
        }
        if job.job_id.is_none() {
            job.job_id = Some(Uuid::new_v4().to_string());
        }
        let path = self
            .layout
            .inbox
            .join(format!("job-{}.json", Uuid::new_v4()));
        let temp = path.with_extension("json.tmp");
        std::fs::write(&temp, serde_json::to_vec_pretty(&job)?)?;
        std::fs::rename(temp, path)?;
        Ok(Admission::Accepted(job))
    }

    pub(super) fn claim_next(&self) -> Result<Option<PathBuf>, ServeError> {
        let entries: Vec<_> = std::fs::read_dir(&self.layout.inbox)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|extension| extension.to_str())
                    == Some("json")
            })
            .collect();
        let mut prioritized = Vec::with_capacity(entries.len());
        for entry in entries {
            let priority = std::fs::read_to_string(entry.path())
                .ok()
                .and_then(|raw| serde_json::from_str::<QueueJob>(&raw).ok())
                .map(|job| job.priority)
                .unwrap_or(0);
            prioritized.push((priority, entry.file_name(), entry.path()));
        }
        prioritized.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let Some((_, _, source)) = prioritized.into_iter().next() else {
            return Ok(None);
        };
        let original_name = source
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("job.json");
        let destination = self.layout.processing.join(format!(
            "{original_name}.processing-{}.json",
            Uuid::new_v4()
        ));
        std::fs::rename(&source, &destination)?;
        Ok(Some(destination))
    }

    pub(super) async fn run_claimed(
        &self,
        harness: &Harness,
        job_path: &Path,
        retry_limit: u32,
    ) -> Result<Option<RunResult>, ServeError> {
        let raw = std::fs::read_to_string(job_path)?;
        let job: QueueJob = match serde_json::from_str(&raw) {
            Ok(job) => job,
            Err(source) => {
                let destination = self.archive(job_path, &self.layout.failed)?;
                let _ = std::fs::write(destination.with_extension("error.txt"), source.to_string());
                return Err(ServeError::Job {
                    path: job_path.to_path_buf(),
                    source,
                });
            }
        };

        let mut request = RunRequest::new(job.task.clone());
        request.keep_workspace = job.keep_workspace;
        request.logical_operation_id = job.logical_operation_id.clone();
        request.timeout = job.timeout_secs.map(Duration::from_secs);

        let result = match harness.run(request).await {
            Ok(result) => result,
            Err(error) => {
                if job.attempt < retry_limit {
                    let mut retry = job.clone();
                    retry.attempt = retry.attempt.saturating_add(1);
                    let destination = self
                        .layout
                        .inbox
                        .join(format!("retry-{}.json", Uuid::new_v4()));
                    let temp = destination.with_extension("json.tmp");
                    std::fs::write(&temp, serde_json::to_vec_pretty(&retry)?)?;
                    std::fs::rename(&temp, &destination)?;
                    std::fs::remove_file(job_path)?;
                    return Ok(None);
                }
                let destination = self.archive(job_path, &self.layout.failed)?;
                let _ = std::fs::write(destination.with_extension("error.txt"), error.to_string());
                return Err(error.into());
            }
        };

        let queue_result = QueueResult {
            job_path: job_path.display().to_string(),
            job_id: job.job_id.clone(),
            attempt: job.attempt,
            run_id: result.run_id.clone(),
            success: result.success,
            termination: result.termination.as_str().into(),
            summary: result.summary.clone(),
            turns: result.turns,
            artifact_dir: result
                .artifact_dir
                .as_ref()
                .map(|path| path.display().to_string()),
        };
        let destination_dir = if result.success && result.termination != RunTermination::Parked {
            &self.layout.done
        } else {
            &self.layout.failed
        };
        let stem = original_job_stem(job_path);
        let serialized = serde_json::to_string_pretty(&queue_result)?;
        let _archive = self
            .layout
            .admission_lock
            .lock()
            .map_err(|_| ServeError::Message("queue archive lock poisoned".into()))?;
        let suffix = Uuid::new_v4().to_string();
        let preferred_result = destination_dir.join(format!("{stem}.result.json"));
        let result_path = if preferred_result.exists() {
            destination_dir.join(format!("{stem}-{suffix}.result.json"))
        } else {
            preferred_result
        };
        std::fs::write(result_path, serialized)?;
        archive_unlocked(job_path, destination_dir, &suffix)?;
        Ok(Some(result))
    }

    pub(super) fn write_health(
        &self,
        running: &AtomicBool,
        running_jobs: usize,
        last_run_id: Option<String>,
        runtime: &ServeRuntimeOptions,
    ) -> Result<(), ServeError> {
        let status = HealthStatus {
            ok: true,
            product: crate::PRODUCT.into(),
            version: crate::VERSION.into(),
            queue_inbox: count_entries(&self.layout.inbox)?,
            running: running.load(Ordering::SeqCst),
            last_run_id,
            running_jobs,
            queue_capacity: runtime.queue_capacity,
            queue_over_capacity: self.depth()? > runtime.queue_capacity,
        };
        std::fs::write(&self.layout.health, serde_json::to_string_pretty(&status)?)?;
        Ok(())
    }

    pub(super) fn health(&self) -> Vec<u8> {
        std::fs::read(&self.layout.health).unwrap_or_else(|_| {
            serde_json::to_vec(&serde_json::json!({
                "ok": true,
                "product": crate::PRODUCT
            }))
            .expect("static health response serializes")
        })
    }

    pub(super) fn depth(&self) -> Result<usize, ServeError> {
        self.depth_unlocked()
    }

    fn depth_unlocked(&self) -> Result<usize, ServeError> {
        Ok(count_json(&self.layout.inbox)? + count_json(&self.layout.processing)?)
    }

    fn archive(&self, job_path: &Path, destination_dir: &Path) -> Result<PathBuf, ServeError> {
        let _archive = self
            .layout
            .admission_lock
            .lock()
            .map_err(|_| ServeError::Message("queue archive lock poisoned".into()))?;
        archive_unlocked(job_path, destination_dir, &Uuid::new_v4().to_string())
    }
}

fn original_job_stem(job_path: &Path) -> String {
    Path::new(&original_job_filename(job_path))
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("job")
        .to_string()
}

fn original_job_filename(job_path: &Path) -> String {
    let name = job_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("job.json");
    name.rsplit_once(".processing-")
        .map(|(original, _)| original)
        .unwrap_or(name)
        .to_string()
}

fn archive_unlocked(
    job_path: &Path,
    destination_dir: &Path,
    suffix: &str,
) -> Result<PathBuf, ServeError> {
    let preferred = destination_dir.join(original_job_filename(job_path));
    let destination = if preferred.exists() {
        destination_dir.join(format!("{}-{suffix}.json", original_job_stem(job_path)))
    } else {
        preferred
    };
    std::fs::rename(job_path, &destination)?;
    Ok(destination)
}

fn count_json(path: &Path) -> Result<usize, ServeError> {
    Ok(std::fs::read_dir(path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
        .count())
}

fn count_entries(path: &Path) -> Result<usize, ServeError> {
    Ok(std::fs::read_dir(path)?.filter_map(Result::ok).count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn job(task: &str, priority: i32) -> QueueJob {
        QueueJob {
            job_id: None,
            task: task.into(),
            priority,
            attempt: 0,
            keep_workspace: false,
            logical_operation_id: None,
            timeout_secs: None,
        }
    }

    #[test]
    fn admission_is_bounded_and_claims_highest_priority() {
        let directory = tempdir().unwrap();
        let layout = QueueLayout::under_state(directory.path());
        let queue = FilesystemQueue::new(&layout);
        queue.ensure().unwrap();

        assert!(matches!(
            queue.admit(job("low", 1), 2).unwrap(),
            Admission::Accepted(_)
        ));
        assert!(matches!(
            queue.admit(job("high", 9), 2).unwrap(),
            Admission::Accepted(_)
        ));
        assert!(matches!(
            queue.admit(job("full", 10), 2).unwrap(),
            Admission::Full
        ));

        let claimed = queue.claim_next().unwrap().unwrap();
        let claimed_job: QueueJob =
            serde_json::from_str(&std::fs::read_to_string(claimed).unwrap()).unwrap();
        assert_eq!(claimed_job.task, "high");
        assert_eq!(queue.depth().unwrap(), 2);
    }

    #[test]
    fn admission_rejects_blank_tasks_without_consuming_capacity() {
        let directory = tempdir().unwrap();
        let layout = QueueLayout::under_state(directory.path());
        let queue = FilesystemQueue::new(&layout);
        queue.ensure().unwrap();

        assert!(matches!(
            queue.admit(job("  ", 0), 1).unwrap(),
            Admission::MissingTask
        ));
        assert_eq!(queue.depth().unwrap(), 0);
    }
}
