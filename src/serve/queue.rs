use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

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
    cache: Arc<Mutex<QueueCache>>,
}

#[derive(Default)]
struct QueueCache {
    inbox_mtime: Option<SystemTime>,
    inbox_idle: bool,
    last_health: Option<String>,
    counts: Option<QueueCounts>,
    counts_inbox_mtime: Option<SystemTime>,
    counts_processing_mtime: Option<SystemTime>,
    #[cfg(test)]
    inbox_file_reads: u64,
    #[cfg(test)]
    health_writes: u64,
}

#[derive(Clone, Copy)]
struct QueueCounts {
    inbox_entries: usize,
    inbox_json: usize,
    processing_json: usize,
}

pub(super) struct ClaimedJob {
    pub path: PathBuf,
    pub job: Result<QueueJob, serde_json::Error>,
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
            cache: Arc::new(Mutex::new(QueueCache::default())),
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
        let path = self.layout.inbox.join(inbox_filename(job.priority));
        let temp = path.with_extension("json.tmp");
        std::fs::write(&temp, serde_json::to_vec_pretty(&job)?)?;
        std::fs::rename(temp, path)?;
        self.invalidate_inbox_cache();
        Ok(Admission::Accepted(job))
    }

    pub(super) fn claim_next(&self) -> Result<Option<ClaimedJob>, ServeError> {
        if self.inbox_is_idle()? {
            return Ok(None);
        }
        let mut prioritized = Vec::new();
        for entry in std::fs::read_dir(&self.layout.inbox)? {
            let Ok(entry) = entry else {
                continue;
            };
            if entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                != Some("json")
            {
                continue;
            }
            let name = entry.file_name();
            let priority = priority_from_inbox_name(&name);
            prioritized.push((priority, name, entry.path()));
        }
        prioritized.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let Some((_, _, source)) = prioritized.into_iter().next() else {
            self.mark_inbox_idle()?;
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
        self.invalidate_inbox_cache();
        let raw = std::fs::read_to_string(&destination)?;
        self.record_inbox_read();
        Ok(Some(ClaimedJob {
            path: destination,
            job: serde_json::from_str(&raw),
        }))
    }

    pub(super) async fn run_claimed(
        &self,
        harness: &Harness,
        claimed: ClaimedJob,
        retry_limit: u32,
    ) -> Result<Option<RunResult>, ServeError> {
        let job_path = claimed.path;
        let job = match claimed.job {
            Ok(job) => job,
            Err(source) => {
                let destination = self.archive(&job_path, &self.layout.failed)?;
                let _ = std::fs::write(destination.with_extension("error.txt"), source.to_string());
                return Err(ServeError::Job {
                    path: job_path,
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
                    let destination = self.layout.inbox.join(inbox_filename(retry.priority));
                    let temp = destination.with_extension("json.tmp");
                    std::fs::write(&temp, serde_json::to_vec_pretty(&retry)?)?;
                    std::fs::rename(&temp, &destination)?;
                    std::fs::remove_file(&job_path)?;
                    self.invalidate_inbox_cache();
                    return Ok(None);
                }
                let destination = self.archive(&job_path, &self.layout.failed)?;
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
        let stem = original_job_stem(&job_path);
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
        archive_unlocked(&job_path, destination_dir, &suffix)?;
        Ok(Some(result))
    }

    pub(super) fn write_health(
        &self,
        running: &AtomicBool,
        running_jobs: usize,
        last_run_id: Option<String>,
        runtime: &ServeRuntimeOptions,
    ) -> Result<(), ServeError> {
        let counts = self.queue_counts()?;
        let status = HealthStatus {
            ok: true,
            product: crate::PRODUCT.into(),
            version: crate::VERSION.into(),
            queue_inbox: counts.inbox_entries,
            running: running.load(Ordering::SeqCst),
            last_run_id,
            running_jobs,
            queue_capacity: runtime.queue_capacity,
            queue_over_capacity: counts.inbox_json + counts.processing_json
                > runtime.queue_capacity,
        };
        let serialized = serde_json::to_string(&status)?;
        {
            let mut cache = self.cache.lock().expect("queue cache lock");
            if cache.last_health.as_deref() == Some(serialized.as_str()) {
                return Ok(());
            }
            cache.last_health = Some(serialized.clone());
            #[cfg(test)]
            {
                cache.health_writes += 1;
            }
        }
        std::fs::write(&self.layout.health, serialized)?;
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

    #[cfg(test)]
    pub(super) fn depth(&self) -> Result<usize, ServeError> {
        self.depth_unlocked()
    }

    fn depth_unlocked(&self) -> Result<usize, ServeError> {
        let counts = self.queue_counts()?;
        Ok(counts.inbox_json + counts.processing_json)
    }

    fn queue_counts(&self) -> Result<QueueCounts, ServeError> {
        let inbox_mtime = dir_mtime(&self.layout.inbox);
        let processing_mtime = dir_mtime(&self.layout.processing);
        {
            let cache = self.cache.lock().expect("queue cache lock");
            if let Some(counts) = cache.counts
                && cache.counts_inbox_mtime == inbox_mtime
                && cache.counts_processing_mtime == processing_mtime
            {
                return Ok(counts);
            }
        }
        let (inbox_entries, inbox_json) = count_inbox(&self.layout.inbox)?;
        let processing_json = count_json(&self.layout.processing)?;
        let counts = QueueCounts {
            inbox_entries,
            inbox_json,
            processing_json,
        };
        let mut cache = self.cache.lock().expect("queue cache lock");
        cache.counts = Some(counts);
        cache.counts_inbox_mtime = inbox_mtime;
        cache.counts_processing_mtime = processing_mtime;
        Ok(counts)
    }

    fn inbox_is_idle(&self) -> Result<bool, ServeError> {
        let mtime = dir_mtime(&self.layout.inbox);
        let cache = self.cache.lock().expect("queue cache lock");
        Ok(cache.inbox_idle && cache.inbox_mtime == mtime && mtime.is_some())
    }

    fn mark_inbox_idle(&self) -> Result<(), ServeError> {
        let mtime = dir_mtime(&self.layout.inbox);
        let mut cache = self.cache.lock().expect("queue cache lock");
        cache.inbox_idle = true;
        cache.inbox_mtime = mtime;
        Ok(())
    }

    fn invalidate_inbox_cache(&self) {
        let mut cache = self.cache.lock().expect("queue cache lock");
        cache.inbox_idle = false;
        cache.inbox_mtime = None;
        cache.counts = None;
        cache.last_health = None;
    }

    fn record_inbox_read(&self) {
        #[cfg(test)]
        {
            self.cache
                .lock()
                .expect("queue cache lock")
                .inbox_file_reads += 1;
        }
    }

    #[cfg(test)]
    fn inbox_file_reads(&self) -> u64 {
        self.cache
            .lock()
            .expect("queue cache lock")
            .inbox_file_reads
    }

    #[cfg(test)]
    fn health_writes(&self) -> u64 {
        self.cache.lock().expect("queue cache lock").health_writes
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
    Ok(count_inbox(path)?.1)
}

fn count_inbox(path: &Path) -> Result<(usize, usize), ServeError> {
    let mut entries = 0usize;
    let mut json = 0usize;
    for entry in std::fs::read_dir(path)? {
        let Ok(entry) = entry else {
            continue;
        };
        entries += 1;
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("json") {
            json += 1;
        }
    }
    Ok((entries, json))
}

fn dir_mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
}

fn inbox_priority_key(priority: i32) -> u32 {
    u32::try_from(i64::from(i32::MAX) - i64::from(priority))
        .expect("i32 priority maps into a u32 sort key")
}

fn inbox_filename(priority: i32) -> String {
    format!(
        "p{:010}-{}.json",
        inbox_priority_key(priority),
        Uuid::new_v4()
    )
}

fn priority_from_inbox_name(name: &OsString) -> i32 {
    let Some(name) = name.to_str() else {
        return 0;
    };
    let Some(rest) = name.strip_prefix('p') else {
        return 0;
    };
    let Some((key, _)) = rest.split_once('-') else {
        return 0;
    };
    if key.len() != 10 {
        return 0;
    }
    let Ok(key) = key.parse::<u32>() else {
        return 0;
    };
    (i64::from(i32::MAX) - i64::from(key)) as i32
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
        assert_eq!(queue.inbox_file_reads(), 1);
        let claimed_job = claimed.job.expect("winner parses once");
        assert_eq!(claimed_job.task, "high");
        assert_eq!(queue.depth().unwrap(), 2);
    }

    #[test]
    fn claim_next_uses_filename_priority_without_parsing_every_file() {
        let directory = tempdir().unwrap();
        let layout = QueueLayout::under_state(directory.path());
        let queue = FilesystemQueue::new(&layout);
        queue.ensure().unwrap();

        let sneaky = job("sneaky-high-json", 99);
        std::fs::write(
            layout.inbox.join("zzz-sneaky.json"),
            serde_json::to_vec_pretty(&sneaky).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            queue.admit(job("filename-high", 9), 8).unwrap(),
            Admission::Accepted(_)
        ));
        assert!(matches!(
            queue.admit(job("filename-low", 1), 8).unwrap(),
            Admission::Accepted(_)
        ));

        let claimed = queue.claim_next().unwrap().unwrap();
        assert_eq!(queue.inbox_file_reads(), 1);
        assert_eq!(claimed.job.unwrap().task, "filename-high");
        assert!(queue.claim_next().unwrap().is_some());
        assert_eq!(queue.inbox_file_reads(), 2);
    }

    #[test]
    fn write_health_skips_unchanged_compact_snapshot() {
        let directory = tempdir().unwrap();
        let layout = QueueLayout::under_state(directory.path());
        let queue = FilesystemQueue::new(&layout);
        queue.ensure().unwrap();
        let running = AtomicBool::new(true);
        let runtime = ServeRuntimeOptions::default();

        queue.write_health(&running, 0, None, &runtime).unwrap();
        queue.write_health(&running, 0, None, &runtime).unwrap();
        assert_eq!(queue.health_writes(), 1);
        let raw = std::fs::read_to_string(&layout.health).unwrap();
        assert!(
            !raw.contains('\n'),
            "health snapshot should be compact JSON"
        );

        queue.write_health(&running, 1, None, &runtime).unwrap();
        assert_eq!(queue.health_writes(), 2);
    }

    #[test]
    fn inbox_filename_priority_round_trips() {
        for priority in [i32::MIN, -3, 0, 1, 9, i32::MAX] {
            let name = OsString::from(inbox_filename(priority));
            assert_eq!(priority_from_inbox_name(&name), priority);
        }
        assert_eq!(priority_from_inbox_name(&OsString::from("001.json")), 0);
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
