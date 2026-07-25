//! Local-queue daemon host (`shikigami serve`).
//!
//! See [docs/decisions/0003-serve-daemon.md](../../docs/decisions/0003-serve-daemon.md).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

use crate::harness::{Harness, HarnessError};
use crate::run::{RunRequest, RunResult, RunTermination};

/// Job file dropped into the inbox for the daemon.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueJob {
    /// Human task text for the harness run.
    pub task: String,
    #[serde(default)]
    pub keep_workspace: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueResult {
    pub job_path: String,
    pub run_id: String,
    pub success: bool,
    pub termination: String,
    pub summary: String,
    pub turns: u32,
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error(transparent)]
    Harness(Box<HarnessError>),
    #[error("serve I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid job {path}: {source}")]
    Job {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("serve: {0}")]
    Message(String),
}

impl From<HarnessError> for ServeError {
    fn from(value: HarnessError) -> Self {
        Self::Harness(Box::new(value))
    }
}

/// Layout under the state root for the local queue.
#[derive(Debug, Clone)]
pub struct QueueLayout {
    pub root: PathBuf,
    pub inbox: PathBuf,
    pub processing: PathBuf,
    pub done: PathBuf,
    pub failed: PathBuf,
    pub health: PathBuf,
}

impl QueueLayout {
    pub fn under_state(state_root: &Path) -> Self {
        let root = state_root.join("queue");
        Self {
            inbox: root.join("inbox"),
            processing: root.join("processing"),
            done: root.join("done"),
            failed: root.join("failed"),
            health: root.join("health.json"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), ServeError> {
        for d in [&self.inbox, &self.processing, &self.done, &self.failed] {
            std::fs::create_dir_all(d)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub ok: bool,
    pub product: String,
    pub version: String,
    pub queue_inbox: usize,
    pub running: bool,
    pub last_run_id: Option<String>,
}

pub struct ServeOptions {
    pub poll_interval: Duration,
    pub max_jobs: Option<u64>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(200),
            max_jobs: None,
        }
    }
}

/// Run the local-queue serve loop until shutdown or `max_jobs` completed.
pub async fn run_serve(
    harness: &Harness,
    layout: &QueueLayout,
    options: ServeOptions,
    shutdown: watch::Receiver<bool>,
) -> Result<u64, ServeError> {
    layout.ensure()?;
    let running = Arc::new(AtomicBool::new(true));
    write_health(layout, harness, &running, 0, None)?;

    let mut completed = 0u64;
    loop {
        if *shutdown.borrow() {
            break;
        }
        if let Some(max) = options.max_jobs
            && completed >= max
        {
            break;
        }

        match take_next_job(layout)? {
            Some(job_path) => {
                let outcome = process_job(harness, layout, &job_path).await;
                completed += 1;
                let last_id = match &outcome {
                    Ok(r) => Some(r.run_id.clone()),
                    Err(_) => None,
                };
                write_health(layout, harness, &running, completed, last_id)?;
            }
            None => {
                write_health(layout, harness, &running, completed, None)?;
                tokio::select! {
                    _ = tokio::time::sleep(options.poll_interval) => {}
                    _ = wait_shutdown(shutdown.clone()) => break,
                }
            }
        }
    }

    running.store(false, Ordering::SeqCst);
    write_health(layout, harness, &running, completed, None)?;
    Ok(completed)
}

async fn wait_shutdown(mut rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

fn take_next_job(layout: &QueueLayout) -> Result<Option<PathBuf>, ServeError> {
    let mut entries: Vec<_> = std::fs::read_dir(&layout.inbox)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                .is_some_and(|x| x == "json")
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());
    let Some(entry) = entries.into_iter().next() else {
        return Ok(None);
    };
    let src = entry.path();
    let dest = layout.processing.join(entry.file_name());
    std::fs::rename(&src, &dest)?;
    Ok(Some(dest))
}

async fn process_job(
    harness: &Harness,
    layout: &QueueLayout,
    job_path: &Path,
) -> Result<RunResult, ServeError> {
    let raw = std::fs::read_to_string(job_path)?;
    let job: QueueJob = serde_json::from_str(&raw).map_err(|source| ServeError::Job {
        path: job_path.to_path_buf(),
        source,
    })?;

    let mut request = RunRequest::new(job.task);
    request.keep_workspace = job.keep_workspace;
    request.logical_operation_id = job.logical_operation_id;
    request.timeout = job.timeout_secs.map(Duration::from_secs);

    let result = match harness.run(request).await {
        Ok(r) => r,
        Err(e) => {
            let fail_name = job_path.file_name().unwrap_or_default();
            let dest = layout.failed.join(fail_name);
            let _ = std::fs::rename(job_path, &dest);
            let err_path = dest.with_extension("error.txt");
            let _ = std::fs::write(err_path, e.to_string());
            return Err(e.into());
        }
    };

    let qr = QueueResult {
        job_path: job_path.display().to_string(),
        run_id: result.run_id.clone(),
        success: result.success,
        termination: result.termination.as_str().into(),
        summary: result.summary.clone(),
        turns: result.turns,
    };
    let dest_dir = if result.success && result.termination != RunTermination::Parked {
        &layout.done
    } else {
        &layout.failed
    };
    let stem = job_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("job");
    let result_path = dest_dir.join(format!("{stem}.result.json"));
    std::fs::write(&result_path, serde_json::to_string_pretty(&qr).unwrap())?;
    let dest = dest_dir.join(job_path.file_name().unwrap_or_default());
    let _ = std::fs::rename(job_path, dest);

    Ok(result)
}

fn write_health(
    layout: &QueueLayout,
    harness: &Harness,
    running: &AtomicBool,
    _completed: u64,
    last_run_id: Option<String>,
) -> Result<(), ServeError> {
    let inbox = std::fs::read_dir(&layout.inbox)
        .map(|rd| rd.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    let status = HealthStatus {
        ok: true,
        product: crate::PRODUCT.into(),
        version: crate::VERSION.into(),
        queue_inbox: inbox,
        running: running.load(Ordering::SeqCst),
        last_run_id,
    };
    let _ = harness; // reserved for future doctor embedding
    std::fs::write(
        &layout.health,
        serde_json::to_string_pretty(&status).unwrap(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::state::StateRoot;
    use tempfile::tempdir;

    #[tokio::test]
    async fn serve_processes_one_inbox_job() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").to_string_lossy().into();
        let harness = Harness::from_config(config, state.clone()).unwrap();
        let layout = QueueLayout::under_state(state.path());
        layout.ensure().unwrap();

        let job = QueueJob {
            task: "demo".into(),
            keep_workspace: true,
            logical_operation_id: None,
            timeout_secs: None,
        };
        std::fs::write(
            layout.inbox.join("001.json"),
            serde_json::to_string_pretty(&job).unwrap(),
        )
        .unwrap();

        let (tx, rx) = watch::channel(false);
        let options = ServeOptions {
            poll_interval: Duration::from_millis(50),
            max_jobs: Some(1),
        };
        let n = run_serve(&harness, &layout, options, rx).await.unwrap();
        assert_eq!(n, 1);
        assert!(layout.done.join("001.result.json").is_file());
        let health: HealthStatus =
            serde_json::from_str(&std::fs::read_to_string(&layout.health).unwrap()).unwrap();
        assert!(health.ok);
        drop(tx);
    }
}
