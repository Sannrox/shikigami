//! Fleet HTTP probe protocol behind the existing `serve_lifecycle_http` interface.
//!
//! Owns connection caps, bounded header reads, `/readyz` `/livez` mapping,
//! loopback-only `/lifecycle` detail, and shutdown. Snapshot publishing stays
//! on [`WorkerLifecycle`](super::WorkerLifecycle). This is not a new public port.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};

use super::{WorkerLifecycle, WorkerLifecycleError, WorkerLifecycleSnapshot, WorkerLifecycleState};

const LIFECYCLE_HTTP_MAX_CONNS: usize = 32;
const LIFECYCLE_HTTP_READ_TIMEOUT: Duration = Duration::from_secs(2);
const LIFECYCLE_HTTP_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const LIFECYCLE_HTTP_MAX_REQUEST_BYTES: usize = 2048;

struct ProbeReply {
    status: u16,
    body: String,
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
                                let resp = format_http(
                                    503,
                                    r#"{"error":"busy"}"#,
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
                                let reply = probe_reply(
                                    method,
                                    path,
                                    &lc.snapshot(),
                                    detail_routes,
                                );
                                let resp = format_http(reply.status, &reply.body);
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

fn probe_reply(
    method: &str,
    path: &str,
    snap: &WorkerLifecycleSnapshot,
    detail_routes: bool,
) -> ProbeReply {
    if !method.eq_ignore_ascii_case("GET") {
        return ProbeReply {
            status: 405,
            body: r#"{"error":"method_not_allowed"}"#.to_string(),
        };
    }
    match path {
        "/livez" | "/livez/" => {
            if matches!(snap.state, WorkerLifecycleState::Unhealthy) {
                ProbeReply {
                    status: 503,
                    body: r#"{"ok":false}"#.to_string(),
                }
            } else {
                ProbeReply {
                    status: 200,
                    body: r#"{"ok":true}"#.to_string(),
                }
            }
        }
        "/readyz" | "/readyz/" => {
            if snap.state.ready_for_fleet() {
                ProbeReply {
                    status: 200,
                    body: r#"{"ok":true}"#.to_string(),
                }
            } else {
                ProbeReply {
                    status: 503,
                    body: format!(r#"{{"ok":false,"state":"{}"}}"#, snap.state.as_str()),
                }
            }
        }
        "/lifecycle" | "/lifecycle/" | "/" if detail_routes => match serde_json::to_string(snap) {
            Ok(body) => ProbeReply { status: 200, body },
            Err(_) => ProbeReply {
                status: 500,
                body: r#"{"error":"serialize"}"#.into(),
            },
        },
        _ => ProbeReply {
            status: 404,
            body: r#"{"error":"not_found"}"#.into(),
        },
    }
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Error",
    }
}

fn format_http(status: u16, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        reason_phrase(status),
        body.len()
    )
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PRODUCT, VERSION};
    use crate::worker_lifecycle::{
        WORKER_LIFECYCLE_CONCURRENCY_V1, WORKER_LIFECYCLE_PROTOCOL,
        WORKER_LIFECYCLE_SCHEMA_VERSION, WorkerLifecycleIdentity,
    };
    use tempfile::tempdir;

    fn snapshot(state: WorkerLifecycleState) -> WorkerLifecycleSnapshot {
        WorkerLifecycleSnapshot {
            schema_version: WORKER_LIFECYCLE_SCHEMA_VERSION,
            protocol: WORKER_LIFECYCLE_PROTOCOL.into(),
            product: PRODUCT.into(),
            version: VERSION.into(),
            worker_id: "worker-1".into(),
            namespace: "team-a".into(),
            runtime_id: "shikigami".into(),
            intake: "plane".into(),
            state,
            accepting_claims: matches!(state, WorkerLifecycleState::Ready),
            active_claims: 0,
            active_runs: 0,
            active_claim_ids: Vec::new(),
            configured_concurrency: WORKER_LIFECYCLE_CONCURRENCY_V1,
            governance_ok: true,
            fencing_ok: true,
            terminal_completed: 0,
            terminal_failed: 0,
            terminal_parked: 0,
            last_error_kind: None,
        }
    }

    fn identity() -> WorkerLifecycleIdentity {
        WorkerLifecycleIdentity {
            worker_id: "worker-1".into(),
            namespace: "team-a".into(),
            runtime_id: "shikigami".into(),
        }
    }

    #[test]
    fn livez_fails_only_when_unhealthy() {
        let live = probe_reply(
            "GET",
            "/livez",
            &snapshot(WorkerLifecycleState::Draining),
            false,
        );
        assert_eq!(live.status, 200);
        assert_eq!(live.body, r#"{"ok":true}"#);
        let dead = probe_reply(
            "GET",
            "/livez/",
            &snapshot(WorkerLifecycleState::Unhealthy),
            false,
        );
        assert_eq!(dead.status, 503);
        assert_eq!(dead.body, r#"{"ok":false}"#);
    }

    #[test]
    fn readyz_follows_fleet_readiness() {
        let ready = probe_reply(
            "GET",
            "/readyz",
            &snapshot(WorkerLifecycleState::Ready),
            false,
        );
        assert_eq!(ready.status, 200);
        let active = probe_reply(
            "GET",
            "/readyz/",
            &snapshot(WorkerLifecycleState::Active),
            false,
        );
        assert_eq!(active.status, 200);
        let draining = probe_reply(
            "GET",
            "/readyz",
            &snapshot(WorkerLifecycleState::Draining),
            false,
        );
        assert_eq!(draining.status, 503);
        assert!(draining.body.contains("draining"));
    }

    #[test]
    fn lifecycle_detail_is_loopback_only() {
        let snap = snapshot(WorkerLifecycleState::Ready);
        let loopback = probe_reply("GET", "/lifecycle", &snap, true);
        assert_eq!(loopback.status, 200);
        assert!(loopback.body.contains("\"ready\""));
        let root = probe_reply("GET", "/", &snap, true);
        assert_eq!(root.status, 200);
        let cluster = probe_reply("GET", "/lifecycle", &snap, false);
        assert_eq!(cluster.status, 404);
        assert_eq!(cluster.body, r#"{"error":"not_found"}"#);
    }

    #[test]
    fn non_get_and_unknown_paths_fail_closed() {
        let method = probe_reply(
            "POST",
            "/readyz",
            &snapshot(WorkerLifecycleState::Ready),
            true,
        );
        assert_eq!(method.status, 405);
        let missing = probe_reply(
            "GET",
            "/secret",
            &snapshot(WorkerLifecycleState::Ready),
            true,
        );
        assert_eq!(missing.status, 404);
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
