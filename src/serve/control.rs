use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};

use super::queue::{Admission, FilesystemQueue};
use super::{ControlOptions, QueueJob, ServeError, wait_shutdown};
use crate::harness::Harness;

const MAX_CONTROL_CONNECTIONS: usize = 64;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Private deep module for the authenticated local Run Control protocol.
///
/// The serve host delegates option validation and listening here instead of
/// learning transport, framing, authentication, routing, and response rules.
pub(super) fn validate_options(
    options: &ControlOptions,
    runtime_queue_capacity: usize,
) -> Result<(), ServeError> {
    if options.queue_capacity.max(1) != runtime_queue_capacity {
        return Err(ServeError::Message(
            "control queue capacity must match serve runtime queue capacity".into(),
        ));
    }
    if options
        .auth_token
        .as_deref()
        .is_some_and(|token| token.trim().is_empty())
    {
        return Err(ServeError::Message(
            "control auth token must not be empty".into(),
        ));
    }
    if options.auth_token.is_none() {
        return Err(ServeError::Message(
            "control binds require an auth token, including loopback".into(),
        ));
    }
    Ok(())
}

pub(super) async fn listen(
    listener: TcpListener,
    harness: Harness,
    queue: FilesystemQueue,
    options: ControlOptions,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServeError> {
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
                let connection_options = options.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = handle_connection(
                        stream,
                        connection_harness,
                        connection_queue,
                        connection_options,
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

async fn handle_connection(
    mut stream: TcpStream,
    harness: Harness,
    queue: FilesystemQueue,
    options: ControlOptions,
) -> Result<(), ServeError> {
    let request = tokio::time::timeout(
        REQUEST_TIMEOUT,
        read_request(&mut stream, options.max_body_bytes),
    )
    .await
    .map_err(|_| ServeError::Message("control request timed out".into()))?;
    let response = match request {
        Err(error) => error_response(error),
        Ok(request) if !authorized(&request, options.auth_token.as_deref()) => Response::json(
            "401 Unauthorized",
            serde_json::json!({"error":"unauthorized"})
                .to_string()
                .into_bytes(),
        ),
        Ok(request) => route(&request, &harness, &queue, &options).unwrap_or_else(error_response),
    };
    response.write_to(&mut stream).await
}

struct Response {
    status: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
}

impl Response {
    fn json(status: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: "application/json",
            body,
        }
    }

    async fn write_to(self, stream: &mut TcpStream) -> Result<(), ServeError> {
        let head = format!(
            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.status,
            self.content_type,
            self.body.len()
        );
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(&self.body).await?;
        Ok(())
    }
}

fn error_response(error: ServeError) -> Response {
    let detail = error.to_string();
    let status = if detail.contains("size limit") {
        "413 Payload Too Large"
    } else {
        "400 Bad Request"
    };
    Response::json(
        status,
        serde_json::json!({"error": detail})
            .to_string()
            .into_bytes(),
    )
}

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    authorization: Option<String>,
    body: Vec<u8>,
}

async fn read_request(
    stream: &mut TcpStream,
    max_body_bytes: usize,
) -> Result<Request, ServeError> {
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
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_ascii_uppercase();
    let path = parts.next().unwrap_or_default().to_string();
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
                    .map_err(|_| ServeError::Message("invalid content length".into()))?
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
    Ok(Request {
        method,
        path,
        authorization,
        body: raw[body_start..body_start + content_length].to_vec(),
    })
}

fn authorized(request: &Request, token: Option<&str>) -> bool {
    token.is_none_or(|token| request.authorization.as_deref() == Some(&format!("Bearer {token}")))
}

fn route(
    request: &Request,
    harness: &Harness,
    queue: &FilesystemQueue,
    options: &ControlOptions,
) -> Result<Response, ServeError> {
    let (path, query) = request
        .path
        .split_once('?')
        .map_or((request.path.as_str(), ""), |parts| parts);
    if request.method == "GET" && path == "/healthz" {
        return Ok(Response {
            status: "200 OK",
            content_type: "application/json",
            body: queue.health(),
        });
    }
    if request.method == "GET" && path == "/metrics" {
        let snapshot = crate::metrics::Metrics::aggregate(harness.state.path())
            .unwrap_or_else(|_| harness.metrics.snapshot());
        return Ok(Response {
            status: "200 OK",
            content_type: "text/plain; version=0.0.4",
            body: snapshot.to_prometheus().into_bytes(),
        });
    }
    if request.method == "GET" && path == "/runs" {
        return json_response("200 OK", &harness.registry.list()?);
    }
    if request.method == "POST" && path == "/runs" {
        if request.body.len() > options.max_body_bytes {
            return Ok(Response::json(
                "413 Payload Too Large",
                b"{\"error\":\"body too large\"}".to_vec(),
            ));
        }
        let job: QueueJob = match serde_json::from_slice(&request.body) {
            Ok(job) => job,
            Err(error) => {
                return Ok(Response::json(
                    "400 Bad Request",
                    serde_json::json!({"error":format!("invalid job JSON: {error}")})
                        .to_string()
                        .into_bytes(),
                ));
            }
        };
        return Ok(match queue.admit(job, options.queue_capacity)? {
            Admission::Accepted(job) => Response::json("202 Accepted", serde_json::to_vec(&job)?),
            Admission::MissingTask => Response::json(
                "400 Bad Request",
                b"{\"error\":\"task is required\"}".to_vec(),
            ),
            Admission::Full => Response::json(
                "429 Too Many Requests",
                b"{\"error\":\"queue capacity reached\"}".to_vec(),
            ),
        });
    }
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    if parts.len() >= 2 && parts[0] == "runs" {
        let run_id = parts[1];
        if !crate::checkpoint::is_safe_run_id(run_id) {
            return Ok(Response::json(
                "400 Bad Request",
                b"{\"error\":\"invalid run id\"}".to_vec(),
            ));
        }
        if request.method == "GET" && parts.len() == 2 {
            return match harness.registry.load(run_id) {
                Ok(record) => json_response("200 OK", &record),
                Err(crate::registry::RegistryError::Missing(_)) => Ok(Response::json(
                    "404 Not Found",
                    b"{\"error\":\"run not found\"}".to_vec(),
                )),
                Err(crate::registry::RegistryError::NotActive(_)) => Ok(Response::json(
                    "409 Conflict",
                    b"{\"error\":\"run is not active\"}".to_vec(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        if request.method == "GET" && parts.len() == 3 && parts[2] == "events" {
            return match harness.registry.event_log(run_id) {
                Ok(log) => Ok(Response {
                    status: "200 OK",
                    content_type: "application/x-ndjson",
                    body: log.into_bytes(),
                }),
                Err(crate::registry::RegistryError::Missing(_)) => Ok(Response::json(
                    "404 Not Found",
                    b"{\"error\":\"run not found\"}".to_vec(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        if request.method == "POST" && parts.len() == 3 && parts[2] == "cancel" {
            return match harness.registry.cancel(run_id) {
                Ok(()) => Ok(Response::json(
                    "202 Accepted",
                    b"{\"cancel_requested\":true}".to_vec(),
                )),
                Err(crate::registry::RegistryError::Missing(_)) => Ok(Response::json(
                    "404 Not Found",
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
                Ok(()) => Ok(Response::json("204 No Content", Vec::new())),
                Err(crate::registry::RegistryError::Missing(_)) => Ok(Response::json(
                    "404 Not Found",
                    b"{\"error\":\"run not found\"}".to_vec(),
                )),
                Err(crate::registry::RegistryError::Active(_)) => Ok(Response::json(
                    "409 Conflict",
                    b"{\"error\":\"run is active\"}".to_vec(),
                )),
                Err(error) => Err(error.into()),
            };
        }
    }
    Ok(Response::json(
        "404 Not Found",
        b"{\"error\":\"not found\"}".to_vec(),
    ))
}

fn json_response<T: Serialize>(status: &'static str, value: &T) -> Result<Response, ServeError> {
    Ok(Response::json(status, serde_json::to_vec_pretty(value)?))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use tempfile::tempdir;

    use super::*;
    use crate::config::Config;
    use crate::serve::{HealthStatus, QueueLayout, ServeRuntimeOptions};
    use crate::state::StateRoot;

    fn request(method: &str, path: &str, body: Vec<u8>) -> Request {
        Request {
            method: method.into(),
            path: path.into(),
            authorization: Some("Bearer secret".into()),
            body,
        }
    }

    #[test]
    fn control_interface_owns_auth_and_bounded_admission() {
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
        let options = ControlOptions {
            auth_token: Some("secret".into()),
            queue_capacity: 1,
            ..ControlOptions::default()
        };
        validate_options(&options, runtime.queue_capacity).unwrap();
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
        let accepted = request("POST", "/runs", body);
        assert!(authorized(&accepted, options.auth_token.as_deref()));
        assert_eq!(
            route(&accepted, &harness, &queue, &options).unwrap().status,
            "202 Accepted"
        );
        assert_eq!(queue.depth().unwrap(), 1);
        let unauthorized = Request {
            authorization: Some("Bearer wrong".into()),
            ..accepted
        };
        assert!(!authorized(&unauthorized, options.auth_token.as_deref()));
        let full = request(
            "POST",
            "/runs",
            serde_json::to_vec(&QueueJob {
                job_id: None,
                task: "full".into(),
                priority: 0,
                attempt: 0,
                keep_workspace: false,
                logical_operation_id: None,
                timeout_secs: None,
            })
            .unwrap(),
        );
        assert_eq!(
            route(&full, &harness, &queue, &options).unwrap().status,
            "429 Too Many Requests"
        );
        std::fs::write(layout.processing.join("active.json"), b"{}").unwrap();
        queue.write_health(&running, 0, None, &runtime).unwrap();
        let health: HealthStatus =
            serde_json::from_str(&std::fs::read_to_string(&layout.health).unwrap()).unwrap();
        assert!(health.queue_over_capacity);
    }

    #[test]
    fn control_options_fail_closed() {
        let missing = ControlOptions {
            auth_token: None,
            ..ControlOptions::default()
        };
        assert!(validate_options(&missing, missing.queue_capacity).is_err());
        let mismatched = ControlOptions {
            auth_token: Some("secret".into()),
            queue_capacity: 2,
            ..ControlOptions::default()
        };
        assert!(validate_options(&mismatched, 1).is_err());
    }
}
