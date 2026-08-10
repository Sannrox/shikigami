//! Local-queue daemon host (`shikigami serve`).
//!
//! See [docs/decisions/0003-serve-daemon.md](../../docs/decisions/0003-serve-daemon.md).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::harness::{Harness, HarnessError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

mod control;
mod queue;

use queue::FilesystemQueue;

/// Job file dropped into the inbox for the daemon.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueJob {
    /// Optional caller correlation id. It is not used as a filesystem path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// Human task text for the harness run.
    pub task: String,
    /// Higher values run first within the local queue.
    #[serde(default)]
    pub priority: i32,
    /// Number of local attempts already made.
    #[serde(default)]
    pub attempt: u32,
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
    pub job_id: Option<String>,
    pub attempt: u32,
    pub run_id: String,
    pub success: bool,
    pub termination: String,
    pub summary: String,
    pub turns: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error(transparent)]
    Harness(Box<HarnessError>),
    #[error("serve I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Registry(#[from] crate::registry::RegistryError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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
    admission_lock: Arc<std::sync::Mutex<()>>,
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
            admission_lock: Arc::new(std::sync::Mutex::new(())),
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
    #[serde(default)]
    pub running_jobs: usize,
    #[serde(default)]
    pub queue_capacity: usize,
    #[serde(default)]
    pub queue_over_capacity: bool,
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

#[derive(Debug, Clone)]
pub struct ServeRuntimeOptions {
    pub concurrency: usize,
    /// Maximum number of queued and processing filesystem jobs accepted by
    /// the HTTP intake surface.
    pub queue_capacity: usize,
    /// Number of local retries after a harness error. Governance-plane retry
    /// semantics remain plane-owned.
    pub retry_limit: u32,
}

impl Default for ServeRuntimeOptions {
    fn default() -> Self {
        Self {
            concurrency: 1,
            queue_capacity: 256,
            retry_limit: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ControlOptions {
    pub bind: SocketAddr,
    pub auth_token: Option<String>,
    pub queue_capacity: usize,
    pub max_body_bytes: usize,
}

impl Default for ControlOptions {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            auth_token: None,
            queue_capacity: 256,
            max_body_bytes: 256 * 1024,
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
    run_serve_with_options(
        harness,
        layout,
        options,
        ServeRuntimeOptions::default(),
        None,
        shutdown,
    )
    .await
}

/// Run the local queue with bounded parallelism and optional authenticated
/// HTTP control/intake.
pub async fn run_serve_with_options(
    harness: &Harness,
    layout: &QueueLayout,
    options: ServeOptions,
    runtime: ServeRuntimeOptions,
    control: Option<ControlOptions>,
    shutdown: watch::Receiver<bool>,
) -> Result<u64, ServeError> {
    let queue = FilesystemQueue::new(layout);
    queue.ensure()?;
    let runtime = ServeRuntimeOptions {
        concurrency: runtime.concurrency.max(1),
        queue_capacity: runtime.queue_capacity.max(1),
        ..runtime
    };
    if runtime.concurrency > 1
        && matches!(
            harness.config.workspace.adapter.as_str(),
            "inplace" | "directory-inplace"
        )
    {
        return Err(ServeError::Message(
            "serve concurrency must be 1 with the inplace workspace adapter".into(),
        ));
    }
    if let Some(control) = &control {
        control::validate_options(control, runtime.queue_capacity)?;
    }
    let running = Arc::new(AtomicBool::new(true));
    queue.write_health(&running, 0, None, &runtime)?;

    let control_task = if let Some(control) = control {
        let listener = TcpListener::bind(control.bind).await?;
        let control_harness = harness.clone();
        let control_queue = queue.clone();
        let control_shutdown = shutdown.clone();
        Some(tokio::spawn(async move {
            control::listen(
                listener,
                control_harness,
                control_queue,
                control,
                control_shutdown,
            )
            .await
        }))
    } else {
        None
    };

    let mut completed = 0u64;
    let mut active = 0usize;
    let mut last_run_id = None;
    let mut jobs = JoinSet::new();
    let mut stopping = false;
    loop {
        if *shutdown.borrow() {
            stopping = true;
            break;
        }
        if let Some(max) = options.max_jobs
            && completed + active as u64 >= max
            && active == 0
        {
            break;
        }

        while active < runtime.concurrency
            && options
                .max_jobs
                .map(|max| completed + (active as u64) < max)
                .unwrap_or(true)
        {
            let Some(job_path) = queue.claim_next()? else {
                break;
            };
            let worker = harness.clone();
            let worker_queue = queue.clone();
            let retry_limit = runtime.retry_limit;
            jobs.spawn(async move {
                worker_queue
                    .run_claimed(&worker, &job_path, retry_limit)
                    .await
            });
            active += 1;
        }

        queue.write_health(&running, active, last_run_id.clone(), &runtime)?;
        if let Some(max) = options.max_jobs
            && completed >= max
            && active == 0
        {
            break;
        }
        if active == 0 {
            tokio::select! {
                _ = tokio::time::sleep(options.poll_interval) => {}
                _ = wait_shutdown(shutdown.clone()) => { stopping = true; break; }
            }
        } else {
            tokio::select! {
                joined = jobs.join_next() => {
                    active = active.saturating_sub(1);
                    if let Some(joined) = joined {
                        match joined {
                            Ok(Ok(Some(result))) => {
                                completed += 1;
                                last_run_id = Some(result.run_id);
                                queue.write_health(&running, active, last_run_id.clone(), &runtime)?;
                            }
                            Ok(Ok(None)) => {}
                            Ok(Err(_)) | Err(_) => {
                                completed += 1;
                                queue.write_health(&running, active, last_run_id.clone(), &runtime)?;
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(options.poll_interval) => {}
                _ = wait_shutdown(shutdown.clone()) => { stopping = true; break; }
            }
        }
    }

    // Drain already claimed jobs on graceful shutdown; no new jobs are taken.
    if stopping {
        while let Some(joined) = jobs.join_next().await {
            active = active.saturating_sub(1);
            match joined {
                Ok(Ok(Some(result))) => {
                    completed += 1;
                    last_run_id = Some(result.run_id);
                }
                Ok(Ok(None)) => {}
                Ok(Err(_)) | Err(_) => {
                    completed += 1;
                }
            }
        }
    }

    running.store(false, Ordering::SeqCst);
    queue.write_health(&running, 0, last_run_id, &runtime)?;
    if let Some(task) = control_task {
        task.abort();
        let _ = task.await;
    }
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
            job_id: None,
            task: "demo".into(),
            priority: 0,
            attempt: 0,
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

    #[tokio::test]
    async fn serve_rejects_parallel_inplace_jobs() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.workspace.adapter = "inplace".into();
        config.workspace.root = dir.path().join("workspace").display().to_string();
        std::fs::create_dir_all(&config.workspace.root).unwrap();
        let harness = Harness::from_config(config, state.clone()).unwrap();
        let layout = QueueLayout::under_state(state.path());
        let runtime = ServeRuntimeOptions {
            concurrency: 2,
            ..ServeRuntimeOptions::default()
        };
        let (_, shutdown) = watch::channel(false);
        let error = run_serve_with_options(
            &harness,
            &layout,
            ServeOptions::default(),
            runtime,
            None,
            shutdown,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("inplace"));
    }
}
