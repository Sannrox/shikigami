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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

mod queue;

use queue::{Admission, FilesystemQueue};

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
    if let Some(control) = &control
        && control.queue_capacity.max(1) != runtime.queue_capacity
    {
        return Err(ServeError::Message(
            "control queue capacity must match serve runtime queue capacity".into(),
        ));
    }
    if let Some(control) = &control {
        if control
            .auth_token
            .as_deref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(ServeError::Message(
                "control auth token must not be empty".into(),
            ));
        }
        if control.auth_token.is_none() {
            return Err(ServeError::Message(
                "control binds require an auth token, including loopback".into(),
            ));
        }
    }
    let running = Arc::new(AtomicBool::new(true));
    queue.write_health(&running, 0, None, &runtime)?;

    let control_task = if let Some(control) = control {
        let listener = TcpListener::bind(control.bind).await?;
        let control_harness = harness.clone();
        let control_queue = queue.clone();
        let control_shutdown = shutdown.clone();
        Some(tokio::spawn(async move {
            run_control_listener(
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

async fn run_control_listener(
    listener: TcpListener,
    harness: Harness,
    queue: FilesystemQueue,
    control: ControlOptions,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServeError> {
    const MAX_CONTROL_CONNECTIONS: usize = 64;
    let permits = Arc::new(Semaphore::new(MAX_CONTROL_CONNECTIONS));
    loop {
        tokio::select! {
            _ = wait_shutdown(shutdown.clone()) => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let connection_harness = harness.clone();
                let connection_queue = queue.clone();
                let connection_control = control.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = handle_control_connection(
                        stream,
                        connection_harness,
                        connection_queue,
                        connection_control,
                    ).await;
                });
            }
        }
        if *shutdown.borrow() {
            return Ok(());
        }
        if shutdown.has_changed().unwrap_or(false) {
            let _ = shutdown.changed().await;
        }
    }
}

async fn handle_control_connection(
    mut stream: TcpStream,
    harness: Harness,
    queue: FilesystemQueue,
    control: ControlOptions,
) -> Result<(), ServeError> {
    let request = tokio::time::timeout(
        Duration::from_secs(10),
        read_http_request(&mut stream, control.max_body_bytes),
    )
    .await
    .map_err(|_| ServeError::Message("control request timed out".into()))?;
    let (status, content_type, body) = match request {
        Err(error) => control_error_response(error),
        Ok(request) if !authorized(&request, control.auth_token.as_deref()) => (
            "401 Unauthorized",
            "application/json",
            serde_json::json!({"error":"unauthorized"})
                .to_string()
                .into_bytes(),
        ),
        Ok(request) => match handle_control_request(&request, &harness, &queue, &control) {
            Ok(response) => response,
            Err(error) => control_error_response(error),
        },
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

fn control_error_response(error: ServeError) -> (&'static str, &'static str, Vec<u8>) {
    let detail = error.to_string();
    let status = if detail.contains("size limit") {
        "413 Payload Too Large"
    } else {
        "400 Bad Request"
    };
    (
        status,
        "application/json",
        serde_json::json!({"error": detail})
            .to_string()
            .into_bytes(),
    )
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    body: Vec<u8>,
}

async fn read_http_request(
    stream: &mut TcpStream,
    max_body_bytes: usize,
) -> Result<HttpRequest, ServeError> {
    const MAX_HEADER_BYTES: usize = 16 * 1024;
    let mut raw = Vec::with_capacity(4096);
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(ServeError::Message(
                "control request ended before headers".into(),
            ));
        }
        raw.extend_from_slice(&chunk[..read]);
        if raw.len() > MAX_HEADER_BYTES + max_body_bytes {
            return Err(ServeError::Message(
                "control request exceeds size limit".into(),
            ));
        }
        if let Some(index) = find_bytes(&raw, b"\r\n\r\n") {
            break index;
        }
        if raw.len() > MAX_HEADER_BYTES {
            return Err(ServeError::Message(
                "control headers exceed size limit".into(),
            ));
        }
    };
    let header_text = std::str::from_utf8(&raw[..header_end])
        .map_err(|_| ServeError::Message("control headers are not UTF-8".into()))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| ServeError::Message("missing control request line".into()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let path = request_parts.next().unwrap_or_default().to_string();
    if method.is_empty() || path.is_empty() {
        return Err(ServeError::Message("invalid control request line".into()));
    }
    let mut content_length = 0usize;
    let mut authorization = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                content_length = value
                    .trim()
                    .parse()
                    .map_err(|_| ServeError::Message("invalid content length".into()))?;
            }
            "authorization" => authorization = Some(value.trim().to_string()),
            _ => {}
        }
    }
    if content_length > max_body_bytes {
        return Err(ServeError::Message(
            "control body exceeds size limit".into(),
        ));
    }
    let body_start = header_end + 4;
    while raw.len() < body_start + content_length {
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(ServeError::Message("control body ended early".into()));
        }
        raw.extend_from_slice(&chunk[..read]);
    }
    Ok(HttpRequest {
        method,
        path,
        authorization,
        body: raw[body_start..body_start + content_length].to_vec(),
    })
}

fn authorized(request: &HttpRequest, token: Option<&str>) -> bool {
    let Some(token) = token else {
        return true;
    };
    request.authorization.as_deref() == Some(&format!("Bearer {token}"))
}

fn handle_control_request(
    request: &HttpRequest,
    harness: &Harness,
    queue: &FilesystemQueue,
    control: &ControlOptions,
) -> Result<(&'static str, &'static str, Vec<u8>), ServeError> {
    let (path, query) = request
        .path
        .split_once('?')
        .map_or((request.path.as_str(), ""), |(path, query)| (path, query));
    if request.method == "GET" && path == "/healthz" {
        let body = queue.health();
        return Ok(("200 OK", "application/json", body));
    }
    if request.method == "GET" && path == "/metrics" {
        let snapshot = crate::metrics::Metrics::aggregate(harness.state.path())
            .unwrap_or_else(|_| harness.metrics.snapshot());
        let body = snapshot.to_prometheus().into_bytes();
        return Ok(("200 OK", "text/plain; version=0.0.4", body));
    }
    if request.method == "GET" && path == "/runs" {
        return json_response("200 OK", &harness.registry.list()?);
    }
    if request.method == "POST" && path == "/runs" {
        if request.body.len() > control.max_body_bytes {
            return Ok((
                "413 Payload Too Large",
                "application/json",
                b"{\"error\":\"body too large\"}".to_vec(),
            ));
        }
        let job: QueueJob = match serde_json::from_slice(&request.body) {
            Ok(job) => job,
            Err(error) => {
                return Ok((
                    "400 Bad Request",
                    "application/json",
                    serde_json::json!({"error":format!("invalid job JSON: {error}")})
                        .to_string()
                        .into_bytes(),
                ));
            }
        };
        return match queue.admit(job, control.queue_capacity)? {
            Admission::Accepted(job) => Ok((
                "202 Accepted",
                "application/json",
                serde_json::to_vec(&job)?,
            )),
            Admission::MissingTask => Ok((
                "400 Bad Request",
                "application/json",
                b"{\"error\":\"task is required\"}".to_vec(),
            )),
            Admission::Full => Ok((
                "429 Too Many Requests",
                "application/json",
                b"{\"error\":\"queue capacity reached\"}".to_vec(),
            )),
        };
    }

    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    if parts.len() >= 2 && parts[0] == "runs" {
        let run_id = parts[1];
        if !crate::checkpoint::is_safe_run_id(run_id) {
            return Ok((
                "400 Bad Request",
                "application/json",
                b"{\"error\":\"invalid run id\"}".to_vec(),
            ));
        }
        if request.method == "GET" && parts.len() == 2 {
            return match harness.registry.load(run_id) {
                Ok(record) => json_response("200 OK", &record),
                Err(crate::registry::RegistryError::Missing(_)) => Ok((
                    "404 Not Found",
                    "application/json",
                    b"{\"error\":\"run not found\"}".to_vec(),
                )),
                Err(crate::registry::RegistryError::NotActive(_)) => Ok((
                    "409 Conflict",
                    "application/json",
                    b"{\"error\":\"run is not active\"}".to_vec(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        if request.method == "GET" && parts.len() == 3 && parts[2] == "events" {
            return match harness.registry.event_log(run_id) {
                Ok(log) => Ok(("200 OK", "application/x-ndjson", log.into_bytes())),
                Err(crate::registry::RegistryError::Missing(_)) => Ok((
                    "404 Not Found",
                    "application/json",
                    b"{\"error\":\"run not found\"}".to_vec(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        if request.method == "POST" && parts.len() == 3 && parts[2] == "cancel" {
            return match harness.registry.cancel(run_id) {
                Ok(()) => Ok((
                    "202 Accepted",
                    "application/json",
                    b"{\"cancel_requested\":true}".to_vec(),
                )),
                Err(crate::registry::RegistryError::Missing(_)) => Ok((
                    "404 Not Found",
                    "application/json",
                    b"{\"error\":\"run not found\"}".to_vec(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        if request.method == "POST" && parts.len() == 3 && parts[2] == "cleanup" {
            let force = query
                .split('&')
                .any(|part| part == "force=1" || part == "force=true");
            return match harness.registry.clean(run_id, force) {
                Ok(()) => Ok(("204 No Content", "application/json", Vec::new())),
                Err(crate::registry::RegistryError::Missing(_)) => Ok((
                    "404 Not Found",
                    "application/json",
                    b"{\"error\":\"run not found\"}".to_vec(),
                )),
                Err(crate::registry::RegistryError::Active(_)) => Ok((
                    "409 Conflict",
                    "application/json",
                    b"{\"error\":\"run is active\"}".to_vec(),
                )),
                Err(error) => Err(error.into()),
            };
        }
    }
    Ok((
        "404 Not Found",
        "application/json",
        b"{\"error\":\"not found\"}".to_vec(),
    ))
}

fn json_response<T: Serialize>(
    status: &'static str,
    value: &T,
) -> Result<(&'static str, &'static str, Vec<u8>), ServeError> {
    Ok((
        status,
        "application/json",
        serde_json::to_vec_pretty(value)?,
    ))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
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

    #[test]
    fn control_auth_and_priority_admission_are_bounded() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.path().join("ws").display().to_string();
        let harness = Harness::from_config(config, state.clone()).unwrap();
        let layout = QueueLayout::under_state(state.path());
        layout.ensure().unwrap();
        let queue = FilesystemQueue::new(&layout);
        let runtime = ServeRuntimeOptions {
            queue_capacity: 1,
            ..ServeRuntimeOptions::default()
        };
        let running = AtomicBool::new(true);
        queue.write_health(&running, 0, None, &runtime).unwrap();
        let control = ControlOptions {
            auth_token: Some("secret".into()),
            queue_capacity: 1,
            ..ControlOptions::default()
        };
        let body = serde_json::to_vec(&QueueJob {
            job_id: None,
            task: "queued".into(),
            priority: 7,
            attempt: 0,
            keep_workspace: false,
            logical_operation_id: None,
            timeout_secs: None,
        })
        .unwrap();
        let request = HttpRequest {
            method: "POST".into(),
            path: "/runs".into(),
            authorization: Some("Bearer secret".into()),
            body,
        };
        assert!(authorized(&request, control.auth_token.as_deref()));
        let (status, _, _) = handle_control_request(&request, &harness, &queue, &control).unwrap();
        assert_eq!(status, "202 Accepted");
        assert_eq!(queue.depth().unwrap(), 1);

        let unauthorized = HttpRequest {
            authorization: Some("Bearer wrong".into()),
            ..request
        };
        assert!(!authorized(&unauthorized, control.auth_token.as_deref()));
        let second = HttpRequest {
            method: "POST".into(),
            path: "/runs".into(),
            authorization: Some("Bearer secret".into()),
            body: serde_json::to_vec(&QueueJob {
                job_id: None,
                task: "full".into(),
                priority: 0,
                attempt: 0,
                keep_workspace: false,
                logical_operation_id: None,
                timeout_secs: None,
            })
            .unwrap(),
        };
        let (status, _, _) = handle_control_request(&second, &harness, &queue, &control).unwrap();
        assert_eq!(status, "429 Too Many Requests");

        std::fs::write(layout.processing.join("active.json"), b"{}").unwrap();
        queue.write_health(&running, 0, None, &runtime).unwrap();
        let health: HealthStatus =
            serde_json::from_str(&std::fs::read_to_string(&layout.health).unwrap()).unwrap();
        assert!(health.queue_over_capacity);
    }
}
