//! Local-queue daemon host (`shikigami serve`).
//!
//! See [docs/decisions/0003-serve-daemon.md](../../docs/decisions/0003-serve-daemon.md).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::harness::{Harness, HarnessError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

mod control;
mod queue;
mod serve_loop;

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
    serve_loop::run_until_shutdown(harness, queue, options, runtime, control, shutdown).await
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
    async fn serve_stops_at_max_jobs_without_claiming_the_rest() {
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
        let payload = serde_json::to_string_pretty(&job).unwrap();
        std::fs::write(layout.inbox.join("001.json"), &payload).unwrap();
        std::fs::write(layout.inbox.join("002.json"), &payload).unwrap();

        let (tx, rx) = watch::channel(false);
        let options = ServeOptions {
            poll_interval: Duration::from_millis(50),
            max_jobs: Some(1),
        };
        let n = run_serve(&harness, &layout, options, rx).await.unwrap();
        drop(tx);
        assert_eq!(n, 1);
        let inbox = std::fs::read_dir(&layout.inbox)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .ok()
                    .and_then(|e| e.path().extension().map(|ext| ext == "json"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(inbox, 1);
        let results = ["001.result.json", "002.result.json"]
            .iter()
            .filter(|name| layout.done.join(name).is_file())
            .count();
        assert_eq!(results, 1);
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
