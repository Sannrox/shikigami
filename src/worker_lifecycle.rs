//! Versioned worker-host lifecycle contract for fleet-managed plane workers.
//!
//! Canonical snapshot: `$SHIKIGAMI_STATE/worker/lifecycle.json`.
//! Optional loopback HTTP mirrors the same document for probes.
//!
//! Authority: issue #155 / Tenkai ADR 0011. Managed contract applies to
//! `serve --intake plane` only.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};

use crate::identity::{PRODUCT, VERSION};

/// Protocol name for the worker lifecycle document.
pub const WORKER_LIFECYCLE_PROTOCOL: &str = "shikigami.worker_lifecycle";
/// First published schema version.
pub const WORKER_LIFECYCLE_SCHEMA_VERSION: u32 = 1;
/// v1 plane workers run one claim at a time.
pub const WORKER_LIFECYCLE_CONCURRENCY_V1: u32 = 1;

/// Terminal claim outcome for lifecycle counters (plane-agnostic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalOutcome {
    Completed,
    Failed,
    Parked,
}

/// Primary lifecycle state for plane workers.
///
/// Precedence when multiple conditions apply (highest first):
/// `unhealthy` > `governance_unavailable` > `fence_lost` > `draining` >
/// `active` > `ready`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerLifecycleState {
    Ready,
    Active,
    Draining,
    GovernanceUnavailable,
    FenceLost,
    Unhealthy,
}

impl WorkerLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::GovernanceUnavailable => "governance_unavailable",
            Self::FenceLost => "fence_lost",
            Self::Unhealthy => "unhealthy",
        }
    }

    /// Whether a fleet readiness probe should succeed.
    pub fn ready_for_fleet(self) -> bool {
        matches!(self, Self::Ready | Self::Active)
    }
}

/// Versioned operational snapshot. Contains no task payloads or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLifecycleSnapshot {
    pub schema_version: u32,
    pub protocol: String,
    pub product: String,
    pub version: String,
    pub worker_id: String,
    pub namespace: String,
    pub runtime_id: String,
    /// Always `plane` for the managed fleet contract.
    pub intake: String,
    pub state: WorkerLifecycleState,
    pub accepting_claims: bool,
    pub active_claims: u32,
    pub active_runs: u32,
    /// Opaque operational claim handles only (e.g. effect ids).
    pub active_claim_ids: Vec<String>,
    pub configured_concurrency: u32,
    pub governance_ok: bool,
    pub fencing_ok: bool,
    pub terminal_completed: u64,
    pub terminal_failed: u64,
    pub terminal_parked: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_kind: Option<String>,
}

#[derive(Debug, Error)]
pub enum WorkerLifecycleError {
    #[error("worker lifecycle I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("worker lifecycle: {0}")]
    Message(String),
}

#[derive(Debug, Clone)]
pub struct WorkerLifecycleIdentity {
    pub worker_id: String,
    pub namespace: String,
    pub runtime_id: String,
}

#[derive(Debug)]
struct Inner {
    identity: WorkerLifecycleIdentity,
    path: PathBuf,
    draining: bool,
    unhealthy: bool,
    governance_ok: bool,
    fencing_ok: bool,
    active_claim_ids: Vec<String>,
    terminal_completed: u64,
    terminal_failed: u64,
    terminal_parked: u64,
    last_error_kind: Option<String>,
}

impl Inner {
    fn snapshot(&self) -> WorkerLifecycleSnapshot {
        let active = self.active_claim_ids.len() as u32;
        let state = resolve_state(
            self.unhealthy,
            self.governance_ok,
            self.fencing_ok,
            self.draining,
            active > 0,
        );
        let capacity = WORKER_LIFECYCLE_CONCURRENCY_V1;
        let accepting_claims = matches!(state, WorkerLifecycleState::Ready)
            || (matches!(state, WorkerLifecycleState::Active) && active < capacity);
        WorkerLifecycleSnapshot {
            schema_version: WORKER_LIFECYCLE_SCHEMA_VERSION,
            protocol: WORKER_LIFECYCLE_PROTOCOL.into(),
            product: PRODUCT.into(),
            version: VERSION.into(),
            worker_id: self.identity.worker_id.clone(),
            namespace: self.identity.namespace.clone(),
            runtime_id: self.identity.runtime_id.clone(),
            intake: "plane".into(),
            state,
            accepting_claims,
            active_claims: active,
            active_runs: active,
            active_claim_ids: self.active_claim_ids.clone(),
            configured_concurrency: capacity,
            governance_ok: self.governance_ok,
            fencing_ok: self.fencing_ok,
            terminal_completed: self.terminal_completed,
            terminal_failed: self.terminal_failed,
            terminal_parked: self.terminal_parked,
            last_error_kind: self.last_error_kind.clone(),
        }
    }

    fn write_snapshot(&mut self) -> Result<WorkerLifecycleSnapshot, WorkerLifecycleError> {
        let snap = self.snapshot();
        if let Some(parent) = self.path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            return Err(self.fail_closed_publish(WorkerLifecycleError::Io(e)));
        }
        let body = match serde_json::to_string_pretty(&snap) {
            Ok(body) => body,
            Err(e) => {
                return Err(self.fail_closed_publish(WorkerLifecycleError::Message(e.to_string())));
            }
        };
        let tmp = self.path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, &body) {
            return Err(self.fail_closed_publish(WorkerLifecycleError::Io(e)));
        }
        // Unix rename over an existing destination is atomic. Windows rename
        // cannot overwrite, so replace there only after a best-effort remove.
        #[cfg(windows)]
        if self.path.exists()
            && let Err(e) = std::fs::remove_file(&self.path)
        {
            let _ = std::fs::remove_file(&tmp);
            return Err(self.fail_closed_publish(WorkerLifecycleError::Io(e)));
        }
        if let Err(e) = std::fs::rename(&tmp, &self.path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(self.fail_closed_publish(WorkerLifecycleError::Io(e)));
        }
        Ok(self.snapshot())
    }

    /// Mark unhealthy and remove any stale on-disk snapshot so fleets fail closed.
    fn fail_closed_publish(&mut self, error: WorkerLifecycleError) -> WorkerLifecycleError {
        self.unhealthy = true;
        self.last_error_kind = Some("lifecycle_publish_failed".into());
        let _ = std::fs::remove_file(&self.path);
        error
    }
}

/// Resolve primary state from independent condition flags.
pub fn resolve_state(
    unhealthy: bool,
    governance_ok: bool,
    fencing_ok: bool,
    draining: bool,
    has_active: bool,
) -> WorkerLifecycleState {
    if unhealthy {
        return WorkerLifecycleState::Unhealthy;
    }
    if !governance_ok {
        return WorkerLifecycleState::GovernanceUnavailable;
    }
    if !fencing_ok {
        return WorkerLifecycleState::FenceLost;
    }
    if draining {
        return WorkerLifecycleState::Draining;
    }
    if has_active {
        return WorkerLifecycleState::Active;
    }
    WorkerLifecycleState::Ready
}

/// Shared publisher for the canonical lifecycle file (and optional HTTP).
#[derive(Clone)]
pub struct WorkerLifecycle {
    inner: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for WorkerLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path = self
            .inner
            .lock()
            .map(|g| g.path.display().to_string())
            .unwrap_or_else(|_| "<locked>".into());
        f.debug_struct("WorkerLifecycle")
            .field("path", &path)
            .finish()
    }
}

impl WorkerLifecycle {
    /// Open under `$STATE/worker/lifecycle.json`.
    pub fn open(
        state_root: &Path,
        identity: WorkerLifecycleIdentity,
    ) -> Result<Self, WorkerLifecycleError> {
        if identity.worker_id.trim().is_empty() {
            return Err(WorkerLifecycleError::Message(
                "worker_id must not be empty".into(),
            ));
        }
        if identity.runtime_id.trim().is_empty() {
            return Err(WorkerLifecycleError::Message(
                "runtime_id must not be empty".into(),
            ));
        }
        let path = lifecycle_path(state_root);
        let lc = Self {
            inner: Arc::new(Mutex::new(Inner {
                identity,
                path,
                draining: false,
                // Start non-ready until the host finishes plane startup.
                unhealthy: true,
                governance_ok: true,
                fencing_ok: true,
                active_claim_ids: Vec::new(),
                terminal_completed: 0,
                terminal_failed: 0,
                terminal_parked: 0,
                last_error_kind: Some("starting".into()),
            })),
        };
        lc.publish()?;
        Ok(lc)
    }

    /// Clear startup unhealth after doctor/client/listen succeed.
    pub fn mark_serving(&self) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.unhealthy = false;
        g.draining = false;
        g.fencing_ok = true;
        if g.last_error_kind.as_deref() == Some("starting")
            || g.last_error_kind.as_deref() == Some("lifecycle_publish_failed")
        {
            g.last_error_kind = None;
        }
        g.write_snapshot()?;
        Ok(())
    }

    pub fn path(&self) -> PathBuf {
        self.inner.lock().expect("lifecycle lock").path.clone()
    }

    pub fn snapshot(&self) -> WorkerLifecycleSnapshot {
        self.inner.lock().expect("lifecycle lock").snapshot()
    }

    pub fn publish(&self) -> Result<WorkerLifecycleSnapshot, WorkerLifecycleError> {
        self.inner.lock().expect("lifecycle lock").write_snapshot()
    }

    pub fn accepting_claims(&self) -> bool {
        self.snapshot().accepting_claims
    }

    pub fn set_draining(&self) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.draining = true;
        g.write_snapshot()?;
        Ok(())
    }

    pub fn set_unhealthy(&self, kind: impl Into<String>) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.unhealthy = true;
        g.last_error_kind = Some(kind.into());
        g.write_snapshot()?;
        Ok(())
    }

    pub fn set_governance_ok(&self, ok: bool) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.governance_ok = ok;
        if !ok {
            g.last_error_kind = Some("governance_unavailable".into());
        }
        g.write_snapshot()?;
        Ok(())
    }

    pub fn set_fence_lost(&self, kind: impl Into<String>) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.fencing_ok = false;
        g.last_error_kind = Some(kind.into());
        g.write_snapshot()?;
        Ok(())
    }

    pub fn begin_claim(&self, claim_id: impl Into<String>) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.active_claim_ids.push(claim_id.into());
        g.fencing_ok = true;
        g.write_snapshot()?;
        Ok(())
    }

    pub fn end_claim_terminal(
        &self,
        claim_id: &str,
        outcome: TerminalOutcome,
    ) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.active_claim_ids.retain(|id| id != claim_id);
        match outcome {
            TerminalOutcome::Completed => g.terminal_completed += 1,
            TerminalOutcome::Failed => g.terminal_failed += 1,
            TerminalOutcome::Parked => g.terminal_parked += 1,
        }
        g.write_snapshot()?;
        Ok(())
    }

    /// Drop active claim without a terminal ack (cancel / fence loss / force).
    pub fn drop_active_claim(&self, claim_id: &str) -> Result<(), WorkerLifecycleError> {
        let mut g = self.inner.lock().expect("lifecycle lock");
        g.active_claim_ids.retain(|id| id != claim_id);
        g.write_snapshot()?;
        Ok(())
    }
}

pub fn lifecycle_path(state_root: &Path) -> PathBuf {
    state_root.join("worker").join("lifecycle.json")
}

const LIFECYCLE_HTTP_MAX_CONNS: usize = 32;
const LIFECYCLE_HTTP_READ_TIMEOUT: Duration = Duration::from_secs(2);
const LIFECYCLE_HTTP_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const LIFECYCLE_HTTP_MAX_REQUEST_BYTES: usize = 2048;

/// Read until the end of HTTP headers (or the max size) so probes that send a
/// full request are not reset mid-write, and so the request line is complete.
async fn read_http_request_prefix(
    stream: &mut tokio::net::TcpStream,
) -> Result<String, std::io::Error> {
    let mut buf = Vec::with_capacity(256);
    let mut chunk = [0u8; 256];
    while buf.len() < LIFECYCLE_HTTP_MAX_REQUEST_BYTES {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.windows(2).any(|w| w == b"\n\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Bind an HTTP probe server for fleet readiness/liveness.
///
/// Routes:
/// - `GET /readyz` — 200 when fleet-ready (`ready`|`active`), else 503
/// - `GET /livez` — 200 unless `unhealthy`
/// - `GET /lifecycle` — full JSON **only** when bound to loopback; otherwise 404
///
/// Cluster binds (`0.0.0.0`) intentionally omit detailed claim metadata so
/// unauthenticated pod-network peers cannot scrape operational identifiers.
/// Connections are capped and read/write timed out.
pub async fn serve_lifecycle_http(
    bind: SocketAddr,
    lifecycle: WorkerLifecycle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<SocketAddr, WorkerLifecycleError> {
    let listener = TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    let detail_routes = local.ip().is_loopback();
    let permits = Arc::new(Semaphore::new(LIFECYCLE_HTTP_MAX_CONNS));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    match accept {
                        Ok((mut stream, _)) => {
                            let Ok(permit) = permits.clone().try_acquire_owned() else {
                                let body = r#"{"error":"busy"}"#;
                                let resp = format!(
                                    "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len()
                                );
                                let _ = tokio::time::timeout(
                                    LIFECYCLE_HTTP_WRITE_TIMEOUT,
                                    stream.write_all(resp.as_bytes()),
                                )
                                .await;
                                continue;
                            };
                            let lc = lifecycle.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let Ok(req) = tokio::time::timeout(
                                    LIFECYCLE_HTTP_READ_TIMEOUT,
                                    read_http_request_prefix(&mut stream),
                                )
                                .await
                                else {
                                    return;
                                };
                                let Ok(req) = req else {
                                    return;
                                };
                                let line = req.lines().next().unwrap_or("");
                                let mut parts = line.split_whitespace();
                                let method = parts.next().unwrap_or("");
                                let path = parts.next().unwrap_or("/");
                                let snap = lc.snapshot();
                                let (status, body) = if !method.eq_ignore_ascii_case("GET") {
                                    (405, r#"{"error":"method_not_allowed"}"#.to_string())
                                } else {
                                    match path {
                                        "/livez" | "/livez/" => {
                                            if matches!(snap.state, WorkerLifecycleState::Unhealthy)
                                            {
                                                (503, r#"{"ok":false}"#.to_string())
                                            } else {
                                                (200, r#"{"ok":true}"#.to_string())
                                            }
                                        }
                                        "/readyz" | "/readyz/" => {
                                            if snap.state.ready_for_fleet() {
                                                (200, r#"{"ok":true}"#.to_string())
                                            } else {
                                                (
                                                    503,
                                                    format!(
                                                        r#"{{"ok":false,"state":"{}"}}"#,
                                                        snap.state.as_str()
                                                    ),
                                                )
                                            }
                                        }
                                        "/lifecycle" | "/lifecycle/" | "/" if detail_routes => {
                                            match serde_json::to_string(&snap) {
                                                Ok(j) => (200, j),
                                                Err(_) => {
                                                    (500, r#"{"error":"serialize"}"#.into())
                                                }
                                            }
                                        }
                                        _ => (404, r#"{"error":"not_found"}"#.into()),
                                    }
                                };
                                let resp = format!(
                                    "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    match status {
                                        200 => "OK",
                                        404 => "Not Found",
                                        405 => "Method Not Allowed",
                                        503 => "Service Unavailable",
                                        _ => "Error",
                                    },
                                    body.len()
                                );
                                let _ = tokio::time::timeout(
                                    LIFECYCLE_HTTP_WRITE_TIMEOUT,
                                    stream.write_all(resp.as_bytes()),
                                )
                                .await;
                            });
                        }
                        Err(_) => break,
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });
    // Tiny settle so callers can connect immediately in tests.
    tokio::time::sleep(Duration::from_millis(5)).await;
    Ok(local)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn identity() -> WorkerLifecycleIdentity {
        WorkerLifecycleIdentity {
            worker_id: "worker-1".into(),
            namespace: "team-a".into(),
            runtime_id: "shikigami".into(),
        }
    }

    #[test]
    fn resolve_state_precedence() {
        assert_eq!(
            resolve_state(true, false, false, true, true),
            WorkerLifecycleState::Unhealthy
        );
        assert_eq!(
            resolve_state(false, false, false, true, true),
            WorkerLifecycleState::GovernanceUnavailable
        );
        assert_eq!(
            resolve_state(false, true, false, true, true),
            WorkerLifecycleState::FenceLost
        );
        assert_eq!(
            resolve_state(false, true, true, true, true),
            WorkerLifecycleState::Draining
        );
        assert_eq!(
            resolve_state(false, true, true, false, true),
            WorkerLifecycleState::Active
        );
        assert_eq!(
            resolve_state(false, true, true, false, false),
            WorkerLifecycleState::Ready
        );
    }

    #[test]
    fn snapshot_ready_and_draining() {
        let dir = tempdir().unwrap();
        let lc = WorkerLifecycle::open(dir.path(), identity()).unwrap();
        assert_eq!(lc.snapshot().state, WorkerLifecycleState::Unhealthy);
        lc.mark_serving().unwrap();
        let s = lc.snapshot();
        assert_eq!(s.schema_version, 1);
        assert_eq!(s.protocol, WORKER_LIFECYCLE_PROTOCOL);
        assert_eq!(s.state, WorkerLifecycleState::Ready);
        assert!(s.accepting_claims);
        assert_eq!(s.intake, "plane");
        assert!(s.active_claim_ids.is_empty());

        lc.begin_claim("effect-1").unwrap();
        let s = lc.snapshot();
        assert_eq!(s.state, WorkerLifecycleState::Active);
        assert!(!s.accepting_claims); // concurrency 1
        assert_eq!(s.active_claim_ids, vec!["effect-1".to_string()]);

        lc.set_draining().unwrap();
        let s = lc.snapshot();
        assert_eq!(s.state, WorkerLifecycleState::Draining);
        assert!(!s.accepting_claims);

        let on_disk: WorkerLifecycleSnapshot =
            serde_json::from_str(&std::fs::read_to_string(lc.path()).unwrap()).unwrap();
        assert_eq!(on_disk.state, WorkerLifecycleState::Draining);
    }

    #[test]
    fn no_task_payload_fields_in_json() {
        let dir = tempdir().unwrap();
        let lc = WorkerLifecycle::open(dir.path(), identity()).unwrap();
        lc.begin_claim("opaque-id").unwrap();
        let raw = std::fs::read_to_string(lc.path()).unwrap();
        for banned in ["task", "prompt", "credential", "token", "password"] {
            // field names only — ensure we did not serialize those keys
            assert!(
                !raw.contains(&format!("\"{banned}\"")),
                "lifecycle json must not contain key {banned}: {raw}"
            );
        }
    }

    #[test]
    fn governance_and_fence_and_unhealthy() {
        let dir = tempdir().unwrap();
        let lc = WorkerLifecycle::open(dir.path(), identity()).unwrap();
        lc.mark_serving().unwrap();
        lc.set_governance_ok(false).unwrap();
        assert_eq!(
            lc.snapshot().state,
            WorkerLifecycleState::GovernanceUnavailable
        );
        assert!(!lc.accepting_claims());

        let lc = WorkerLifecycle::open(dir.path(), identity()).unwrap();
        lc.mark_serving().unwrap();
        lc.set_fence_lost("heartbeat_timeout").unwrap();
        assert_eq!(lc.snapshot().state, WorkerLifecycleState::FenceLost);
        assert!(!lc.accepting_claims());

        let lc = WorkerLifecycle::open(dir.path(), identity()).unwrap();
        // open() starts unhealthy until mark_serving
        assert_eq!(lc.snapshot().state, WorkerLifecycleState::Unhealthy);
        assert!(!lc.accepting_claims());
        lc.mark_serving().unwrap();
        lc.set_unhealthy("doctor_failed").unwrap();
        assert_eq!(lc.snapshot().state, WorkerLifecycleState::Unhealthy);
        assert!(!lc.accepting_claims());
    }

    #[tokio::test]
    async fn http_readyz_reflects_state() {
        let dir = tempdir().unwrap();
        let lc = WorkerLifecycle::open(dir.path(), identity()).unwrap();
        lc.mark_serving().unwrap();
        let (tx, rx) = watch::channel(false);
        let addr = serve_lifecycle_http("127.0.0.1:0".parse().unwrap(), lc.clone(), rx)
            .await
            .unwrap();

        let body = http_get(addr, "/readyz").await;
        assert!(body.starts_with("HTTP/1.1 200"), "{body}");

        lc.set_draining().unwrap();
        let body = http_get(addr, "/readyz").await;
        assert!(body.starts_with("HTTP/1.1 503"), "{body}");

        let body = http_get(addr, "/lifecycle").await;
        assert!(
            body.contains("\"draining\"") || body.contains("draining"),
            "{body}"
        );

        let _ = tx.send(true);
    }

    async fn http_get(addr: SocketAddr, path: &str) -> String {
        use tokio::net::TcpStream;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }
}
